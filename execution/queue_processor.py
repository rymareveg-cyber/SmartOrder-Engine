#!/usr/bin/env python3
"""
Queue Processor - обработчик очереди заказов из Redis.

Обрабатывает сообщения из Redis Queue, маршрутизирует по каналам
и вызывает AI Parser для каждого сообщения.
"""

import os
import json
import logging
import signal
import asyncio
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

from dotenv import load_dotenv
import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/queue_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Переменные окружения
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
AI_PARSER_URL = os.getenv('AI_PARSER_URL', 'http://localhost:8001')
WORKER_CONCURRENCY = int(os.getenv('WORKER_CONCURRENCY', '5'))
HEALTH_CHECK_PORT = int(os.getenv('HEALTH_CHECK_PORT', '8027'))
HEALTH_CHECK_HOST = os.getenv('HEALTH_CHECK_HOST', '0.0.0.0')

# Redis Queue ключи
QUEUE_KEY = "orders:queue"
DEAD_LETTER_QUEUE_KEY = "orders:dead_letter"

# Максимальное количество попыток обработки сообщения
MAX_RETRIES = 3

# Флаг для graceful shutdown
shutdown_flag = False

# Метрики
metrics = {
    "processed": 0,
    "errors": 0,
    "dead_letter": 0,
    "by_channel": {
        "telegram": 0,
        "yandex_mail": 0,
        "yandex_forms": 0
    },
    "start_time": None
}

# Redis connection pool
redis_pool: Optional[aioredis.ConnectionPool] = None


def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown."""
    global shutdown_flag
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_flag = True


async def get_redis_client() -> aioredis.Redis:
    """Получить Redis клиент из connection pool."""
    global redis_pool
    if not redis_pool:
        redis_pool = aioredis.ConnectionPool.from_url(
            REDIS_URL,
            decode_responses=False,
            max_connections=WORKER_CONCURRENCY + 2
        )
    return aioredis.Redis(connection_pool=redis_pool)


async def send_to_dead_letter(message_data: dict, error: str):
    """Отправить сообщение в dead letter queue."""
    try:
        redis_client = await get_redis_client()
        dead_letter_data = {
            "original_message": message_data,
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "retries": MAX_RETRIES
        }
        dead_letter_json = json.dumps(dead_letter_data, ensure_ascii=False)
        await redis_client.lpush(DEAD_LETTER_QUEUE_KEY, dead_letter_json)
        metrics["dead_letter"] += 1
        logger.error(f"Message sent to dead letter queue: {error}")
    except Exception as e:
        logger.error(f"Failed to send to dead letter queue: {e}")


async def call_ai_parser(message_data: dict) -> Optional[Dict[str, Any]]:
    """
    Вызов AI Parser для обработки сообщения.
    
    Args:
        message_data: Данные сообщения из очереди
        
    Returns:
        Результат парсинга или None в случае ошибки
    """
    try:
        import sys
        from pathlib import Path
        
        # Добавление корневой директории проекта в sys.path для импорта
        project_root = Path(__file__).parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from execution.ai_order_parser import process_order_message
        
        channel = message_data.get("channel", "unknown")
        logger.info(f"Calling AI Parser for channel: {channel}, message_id: {message_data.get('message_id') or message_data.get('submission_id')}")
        
        # Вызов AI Parser
        result = await process_order_message(message_data)
        
        if result:
            logger.info(f"AI Parser successfully processed message from {channel}")
            return result
        else:
            logger.warning(f"AI Parser returned None for message from {channel}")
            return None
    
    except Exception as e:
        logger.error(f"Error calling AI Parser: {e}", exc_info=True)
        return None


async def process_message(message_data: dict) -> bool:
    """
    Обработка одного сообщения из очереди.
    
    Args:
        message_data: Данные сообщения
        
    Returns:
        True если успешно обработано, False в противном случае
    """
    try:
        channel = message_data.get("channel", "unknown")
        
        # Маршрутизация по каналам
        if channel not in ["telegram", "yandex_mail", "yandex_forms"]:
            logger.warning(f"Unknown channel: {channel}")
            return False
        
        # Обновление метрик
        metrics["by_channel"][channel] = metrics["by_channel"].get(channel, 0) + 1
        
        # Вызов AI Parser
        result = await call_ai_parser(message_data)
        
        if result:
            # Сохранение заказа в БД, если парсинг успешен и статус validated
            if result.get("status") == "validated":
                try:
                    import sys
                    from pathlib import Path
                    project_root = Path(__file__).parent.parent
                    if str(project_root) not in sys.path:
                        sys.path.insert(0, str(project_root))
                    
                    from execution.crm_service import OrderService
                    
                    # Формирование данных для создания заказа
                    order_data = {
                        "channel": channel,
                        "customer_name": result.get("customer", {}).get("name"),
                        "customer_phone": result.get("customer", {}).get("phone"),
                        "customer_address": result.get("customer", {}).get("address"),
                        "items": [
                            {
                                "product_articul": item["articul"],
                                "product_name": item["name"],
                                "quantity": item["quantity"],
                                "price_at_order": item["price_at_order"]
                            }
                            for item in result.get("products", [])
                        ],
                        "delivery_cost": 0,  # Будет автоматически рассчитано в OrderService.create_order
                        "status": "validated"
                    }
                    
                    # Создание заказа
                    order = OrderService.create_order(order_data)
                    logger.info(f"Order saved to database: {order.order_number} (ID: {order.id})")
                    
                    # Генерация счёта (PDF)
                    try:
                        from execution.invoice_generator import InvoiceGenerator
                        
                        invoice_result = InvoiceGenerator.generate_invoice(order.id)
                        logger.info(f"Invoice generated: {invoice_result['invoice_number']} for order {order.order_number}")
                    except Exception as e:
                        logger.error(f"Failed to generate invoice: {e}", exc_info=True)
                        # Не считаем это критической ошибкой, продолжаем обработку
                except Exception as e:
                    logger.error(f"Failed to save order to database: {e}", exc_info=True)
                    # Не считаем это критической ошибкой, продолжаем обработку
            
            logger.info(f"Successfully processed message from {channel}")
            metrics["processed"] += 1
            return True
        else:
            logger.error(f"Failed to process message from {channel}")
            metrics["errors"] += 1
            return False
    
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)
        metrics["errors"] += 1
        return False


async def worker(worker_id: int):
    """
    Worker для обработки сообщений из очереди.
    
    Args:
        worker_id: ID воркера
    """
    logger.info(f"Worker {worker_id} started")
    
    redis_client = await get_redis_client()
    retry_count = {}
    
    try:
        while not shutdown_flag:
            try:
                # Блокирующий pop из очереди (таймаут 5 секунд)
                result = await redis_client.brpop(QUEUE_KEY, timeout=5)
                
                if not result:
                    # Таймаут - продолжаем цикл
                    continue
                
                queue_name, message_bytes = result
                message_json = message_bytes.decode('utf-8')
                message_data = json.loads(message_json)
                
                # Получение уникального идентификатора сообщения
                message_id = (
                    message_data.get("message_id") or
                    message_data.get("submission_id") or
                    message_data.get("email") or
                    f"unknown_{int(time.time())}"
                )
                
                # Обработка сообщения
                success = await process_message(message_data)
                
                if not success:
                    # Увеличение счётчика попыток
                    retry_key = f"retry:{message_id}"
                    retry_count[retry_key] = retry_count.get(retry_key, 0) + 1
                    
                    if retry_count[retry_key] >= MAX_RETRIES:
                        # Отправка в dead letter queue
                        await send_to_dead_letter(
                            message_data,
                            f"Failed after {MAX_RETRIES} retries"
                        )
                        del retry_count[retry_key]
                    else:
                        # Возврат сообщения в очередь для повторной попытки
                        await redis_client.lpush(QUEUE_KEY, message_json)
                        logger.warning(f"Message {message_id} returned to queue (retry {retry_count[retry_key]}/{MAX_RETRIES})")
                else:
                    # Успешная обработка - удаляем из retry_count
                    retry_key = f"retry:{message_id}"
                    if retry_key in retry_count:
                        del retry_count[retry_key]
            
            except asyncio.CancelledError:
                logger.info(f"Worker {worker_id} cancelled")
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}", exc_info=True)
                await asyncio.sleep(1)  # Небольшая задержка перед повтором
    
    finally:
        await redis_client.close()
        logger.info(f"Worker {worker_id} stopped")


async def start_workers():
    """Запуск всех воркеров."""
    logger.info(f"Starting {WORKER_CONCURRENCY} workers...")
    metrics["start_time"] = datetime.now(timezone.utc).isoformat()
    
    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Создание задач для воркеров
    tasks = [
        asyncio.create_task(worker(i + 1))
        for i in range(WORKER_CONCURRENCY)
    ]
    
    try:
        # Ожидание завершения всех задач
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Workers cancelled")
    finally:
        # Отмена всех задач
        for task in tasks:
            task.cancel()
        
        # Ожидание завершения
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Закрытие connection pool
        if redis_pool:
            await redis_pool.disconnect()
        
        logger.info("All workers stopped")


# FastAPI для health check
app = FastAPI(
    title="SmartOrder Engine - Queue Processor",
    description="Обработчик очереди заказов из Redis. Маршрутизирует сообщения по каналам и вызывает AI Parser.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        redis_client = await get_redis_client()
        await redis_client.ping()
        redis_status = "ok"
        await redis_client.close()
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        redis_status = "error"
    
    # Вычисление uptime
    uptime_seconds = 0
    if metrics["start_time"]:
        start_dt = datetime.fromisoformat(metrics["start_time"].replace('Z', '+00:00'))
        uptime_seconds = int((datetime.now(timezone.utc) - start_dt).total_seconds())
    
    return {
        "status": "ok" if redis_status == "ok" else "degraded",
        "redis": redis_status,
        "workers": WORKER_CONCURRENCY,
        "shutdown": shutdown_flag,
        "metrics": {
            "processed": metrics["processed"],
            "errors": metrics["errors"],
            "dead_letter": metrics["dead_letter"],
            "by_channel": metrics["by_channel"],
            "uptime_seconds": uptime_seconds
        }
    }


@app.get("/metrics")
async def get_metrics():
    """Endpoint для получения метрик."""
    return JSONResponse(content=metrics)


async def run_health_check_server():
    """Запуск FastAPI сервера для health check."""
    config = uvicorn.Config(
        app,
        host=HEALTH_CHECK_HOST,
        port=HEALTH_CHECK_PORT,
        log_level="info"
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    """Главная функция."""
    # Создание директории для логов если её нет
    os.makedirs("logs", exist_ok=True)
    
    logger.info("Starting Queue Processor...")
    logger.info(f"Worker concurrency: {WORKER_CONCURRENCY}")
    logger.info(f"Redis URL: {REDIS_URL}")
    logger.info(f"AI Parser URL: {AI_PARSER_URL}")
    logger.info(f"Health check: http://{HEALTH_CHECK_HOST}:{HEALTH_CHECK_PORT}/health")
    
    # Запуск health check сервера в фоне
    health_check_task = asyncio.create_task(run_health_check_server())
    
    try:
        # Запуск воркеров
        await start_workers()
    finally:
        # Остановка health check сервера
        health_check_task.cancel()
        try:
            await health_check_task
        except asyncio.CancelledError:
            pass
        
        logger.info("Queue Processor stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Queue Processor stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise
