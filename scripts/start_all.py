#!/usr/bin/env python3
"""
Скрипт запуска всех сервисов SmartOrder Engine.

Запускает все компоненты системы в отдельных процессах с логированием.
"""

import os
import sys
import time
import signal
import subprocess
import logging
from pathlib import Path
from typing import List, Dict, Optional
from multiprocessing import Process

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/start_all.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Список сервисов для запуска
SERVICES = [
    {
        "name": "sync_1c_catalog",
        "script": "execution/sync_1c_catalog.py",
        "description": "Синхронизация каталога 1С (с планировщиком)",
        "required": True
    },
    {
        "name": "api_catalog_server",
        "script": "execution/api_catalog_server.py",
        "description": "API сервер каталога",
        "required": True,
        "port": os.getenv('API_PORT', '8025')
    },
    {
        "name": "telegram_bot",
        "script": "execution/telegram_bot.py",
        "description": "Telegram бот",
        "required": False
    },
    {
        "name": "yandex_mail_parser",
        "script": "execution/yandex_mail_parser.py",
        "description": "Парсер Яндекс.Почты",
        "required": False
    },
    {
        "name": "yandex_forms_webhook",
        "script": "execution/yandex_forms_webhook.py",
        "description": "Webhook для Яндекс.Форм",
        "required": False,
        "port": os.getenv('WEBHOOK_PORT', '8026')
    },
    {
        "name": "queue_processor",
        "script": "execution/queue_processor.py",
        "description": "Обработчик очереди Redis",
        "required": True
    },
    {
        "name": "dashboard_api",
        "script": "execution/dashboard_api.py",
        "description": "Dashboard API",
        "required": False,
        "port": os.getenv('DASHBOARD_PORT', '8028')  # Отдельный порт для dashboard
    }
]

# Хранилище процессов
processes: List[Dict[str, any]] = []


def is_port_in_use(port: int, host: str = '0.0.0.0') -> bool:
    """
    Проверка, занят ли порт.
    
    Args:
        port: Номер порта
        host: Хост для проверки
        
    Returns:
        True если порт занят, False если свободен
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            result = s.connect_ex((host if host != '0.0.0.0' else '127.0.0.1', port))
            return result == 0
    except Exception:
        return False


def start_service(service: Dict[str, any]) -> Optional[Process]:
    """
    Запуск сервиса в отдельном процессе.
    
    Args:
        service: Конфигурация сервиса
        
    Returns:
        Процесс сервиса или None в случае ошибки
    """
    script_path = project_root / service["script"]
    
    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        if service.get("required", False):
            raise FileNotFoundError(f"Required script not found: {script_path}")
        return None
    
    logger.info(f"Starting {service['name']}: {service['description']}")
    
    # Проверка порта, если указан
    if "port" in service:
        port = int(service["port"])
        if is_port_in_use(port):
            logger.warning(f"Port {port} is already in use for {service['name']}. Skipping...")
            if service.get("required", False):
                logger.error(f"Required service {service['name']} cannot start on port {port}")
                raise RuntimeError(f"Port {port} is already in use")
            return None
    
    try:
        # Запуск процесса
        process = subprocess.Popen(
            [sys.executable, str(script_path)],
            cwd=str(project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        
        # Небольшая задержка для проверки запуска
        time.sleep(2)  # Увеличиваем задержку для dashboard_api
        
        # Проверка, что процесс запущен
        if process.poll() is None:
            logger.info(f"[OK] {service['name']} started (PID: {process.pid})")
            return process
        else:
            # Процесс завершился сразу
            stdout, stderr = process.communicate()
            logger.error(f"[FAIL] {service['name']} failed to start")
            if stdout:
                logger.error(f"  stdout: {stdout[:500]}")  # Ограничиваем длину вывода
            if stderr:
                logger.error(f"  stderr: {stderr[:500]}")  # Ограничиваем длину вывода
            return None
            
    except Exception as e:
        logger.error(f"[ERROR] Error starting {service['name']}: {e}")
        if service.get("required", False):
            raise
        return None


def stop_all_services():
    """Остановка всех запущенных сервисов."""
    logger.info("Stopping all services...")
    
    for proc_info in processes:
        process = proc_info.get("process")
        service_name = proc_info.get("name")
        
        if process and process.poll() is None:
            logger.info(f"Stopping {service_name} (PID: {process.pid})...")
            try:
                process.terminate()
                # Ждём завершения (максимум 5 секунд)
                process.wait(timeout=5)
                logger.info(f"[OK] {service_name} stopped")
            except subprocess.TimeoutExpired:
                logger.warning(f"Force killing {service_name}...")
                process.kill()
                process.wait()
                logger.info(f"[OK] {service_name} force stopped")
            except Exception as e:
                logger.error(f"Error stopping {service_name}: {e}")


def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown."""
    logger.info(f"Received signal {signum}, shutting down...")
    stop_all_services()
    sys.exit(0)


def monitor_services():
    """Мониторинг запущенных сервисов."""
    while True:
        time.sleep(10)  # Проверка каждые 10 секунд
        
        for proc_info in processes:
            process = proc_info.get("process")
            service_name = proc_info.get("name")
            required = proc_info.get("required", False)
            
            if process:
                if process.poll() is not None:
                    # Процесс завершился
                    logger.error(f"[ERROR] {service_name} has stopped unexpectedly (exit code: {process.returncode})")
                    
                    if required:
                        logger.error(f"Required service {service_name} stopped, shutting down...")
                        stop_all_services()
                        sys.exit(1)
                    else:
                        # Опциональный сервис - можно продолжить
                        logger.warning(f"Optional service {service_name} stopped, continuing...")


def main():
    """Главная функция запуска всех сервисов."""
    logger.info("=" * 60)
    logger.info("SmartOrder Engine - Starting all services")
    logger.info("=" * 60)
    
    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Создание директории для логов
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Запуск всех сервисов
    for service in SERVICES:
        # Проверка, нужно ли запускать сервис
        # Можно добавить проверку переменных окружения
        if not service.get("required", False):
            # Проверяем наличие необходимых переменных окружения
            if service["name"] == "telegram_bot" and not os.getenv("TELEGRAM_BOT_TOKEN"):
                logger.info(f"Skipping {service['name']} (TELEGRAM_BOT_TOKEN not set)")
                continue
            elif service["name"] == "yandex_mail_parser" and not os.getenv("YANDEX_MAIL_EMAIL"):
                logger.info(f"Skipping {service['name']} (YANDEX_MAIL_EMAIL not set)")
                continue
        
        process = start_service(service)
        if process:
            processes.append({
                "name": service["name"],
                "process": process,
                "service": service,
                "required": service.get("required", False)
            })
        else:
            if service.get("required", False):
                logger.error(f"Failed to start required service {service['name']}, aborting...")
                stop_all_services()
                sys.exit(1)
    
    logger.info("=" * 60)
    logger.info(f"All services started. Total: {len(processes)}")
    logger.info("=" * 60)
    logger.info("Press Ctrl+C to stop all services")
    logger.info("")
    
    # Мониторинг сервисов
    try:
        monitor_services()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        stop_all_services()


if __name__ == "__main__":
    main()
