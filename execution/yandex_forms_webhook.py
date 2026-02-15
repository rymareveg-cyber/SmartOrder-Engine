#!/usr/bin/env python3
"""
Webhook endpoint для Яндекс.Форм.

Принимает данные от Яндекс.Форм и отправляет их в Redis Queue
для дальнейшей обработки AI-парсером.
"""

import os
import json
import logging
import hashlib
import hmac
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

from dotenv import load_dotenv
import redis
from fastapi import FastAPI, Request, HTTPException, Depends, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from starlette.middleware.base import BaseHTTPMiddleware

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/yandex_forms_webhook.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Переменные окружения
YANDEX_FORMS_SECRET = os.getenv('YANDEX_FORMS_SECRET', '')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
WEBHOOK_PORT = int(os.getenv('WEBHOOK_PORT', '8026'))
WEBHOOK_HOST = os.getenv('WEBHOOK_HOST', '0.0.0.0')
RATE_LIMIT_REQUESTS = int(os.getenv('RATE_LIMIT_REQUESTS', '100'))  # запросов в минуту
RATE_LIMIT_WINDOW = int(os.getenv('RATE_LIMIT_WINDOW', '60'))  # секунд

# Redis клиент
redis_client: Optional[redis.Redis] = None

# Redis Queue ключ
QUEUE_KEY = "orders:queue"

# Ключ для хранения обработанных submission_id (idempotency)
PROCESSED_SUBMISSIONS_KEY = "yandex_forms:processed"

# Максимальное количество попыток отправки в Redis
MAX_RETRIES = 3
RETRY_DELAYS = [1, 2, 4]  # секунды


# Pydantic модели для валидации
class YandexFormSubmission(BaseModel):
    """Модель данных от Яндекс.Форм."""
    form_id: str = Field(..., description="ID формы")
    form_name: Optional[str] = Field(None, description="Название формы")
    submission_id: str = Field(..., description="Уникальный ID отправки")
    data: Dict[str, Any] = Field(..., description="Данные формы (все поля)")
    timestamp: Optional[str] = Field(None, description="Временная метка отправки")
    signature: Optional[str] = Field(None, description="Подпись запроса (если поддерживается)")
    
    @field_validator('data')
    @classmethod
    def validate_data(cls, v):
        """Проверка, что data не пустой."""
        if not v:
            raise ValueError("data cannot be empty")
        return v


class WebhookResponse(BaseModel):
    """Модель ответа webhook."""
    status: str = "ok"
    message: str = "Submission received"


# Rate Limiting Middleware
class RateLimitMiddleware(BaseHTTPMiddleware):
    """Middleware для rate limiting по IP адресу."""
    
    async def dispatch(self, request: Request, call_next):
        # Получение IP адреса
        client_ip = request.client.host if request.client else "unknown"
        
        # Пропуск rate limiting для health check
        if request.url.path == "/health":
            return await call_next(request)
        
        # Проверка rate limit
        if not self.check_rate_limit(client_ip):
            logger.warning(f"Rate limit exceeded for IP: {client_ip}")
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "error": "Too Many Requests",
                    "message": f"Rate limit exceeded: {RATE_LIMIT_REQUESTS} requests per {RATE_LIMIT_WINDOW} seconds"
                },
                headers={
                    "X-RateLimit-Limit": str(RATE_LIMIT_REQUESTS),
                    "X-RateLimit-Remaining": "0",
                    "Retry-After": str(RATE_LIMIT_WINDOW)
                }
            )
        
        response = await call_next(request)
        
        # Добавление заголовков rate limit
        remaining = self.get_remaining_requests(client_ip)
        response.headers["X-RateLimit-Limit"] = str(RATE_LIMIT_REQUESTS)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(int(time.time()) + RATE_LIMIT_WINDOW)
        
        return response
    
    def check_rate_limit(self, ip: str) -> bool:
        """Проверка rate limit для IP адреса."""
        if not redis_client:
            return True  # Если Redis недоступен, пропускаем
        
        try:
            key = f"rate_limit:yandex_forms:{ip}"
            current = redis_client.get(key)
            
            if current is None:
                # Первый запрос в окне
                redis_client.setex(key, RATE_LIMIT_WINDOW, 1)
                return True
            
            current_count = int(current)
            if current_count >= RATE_LIMIT_REQUESTS:
                return False
            
            # Увеличение счётчика
            redis_client.incr(key)
            return True
        except Exception as e:
            logger.error(f"Error checking rate limit: {e}")
            return True  # В случае ошибки пропускаем
    
    def get_remaining_requests(self, ip: str) -> int:
        """Получение оставшегося количества запросов."""
        if not redis_client:
            return RATE_LIMIT_REQUESTS
        
        try:
            key = f"rate_limit:yandex_forms:{ip}"
            current = redis_client.get(key)
            if current is None:
                return RATE_LIMIT_REQUESTS
            current_count = int(current)
            return max(0, RATE_LIMIT_REQUESTS - current_count)
        except Exception:
            return RATE_LIMIT_REQUESTS


def init_redis():
    """Инициализация Redis клиента."""
    global redis_client
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=False)
        redis_client.ping()
        logger.info("Redis client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Redis: {e}")
        raise


def verify_signature(payload: bytes, signature: Optional[str]) -> bool:
    """
    Проверка подписи запроса от Яндекс.Форм.
    
    Если Яндекс.Формы поддерживают подпись, она передаётся в заголовке или в payload.
    Здесь реализована базовая проверка через HMAC-SHA256.
    """
    if not YANDEX_FORMS_SECRET:
        # Если секрет не настроен, пропускаем проверку
        return True
    
    if not signature:
        logger.warning("Signature provided but secret is configured")
        return False
    
    try:
        # Вычисление ожидаемой подписи
        expected_signature = hmac.new(
            YANDEX_FORMS_SECRET.encode('utf-8'),
            payload,
            hashlib.sha256
        ).hexdigest()
        
        # Сравнение подписей (constant-time comparison)
        return hmac.compare_digest(expected_signature, signature)
    except Exception as e:
        logger.error(f"Error verifying signature: {e}")
        return False


def is_duplicate_submission(submission_id: str) -> bool:
    """Проверка, не обрабатывалось ли уже это submission."""
    if not redis_client:
        return False
    
    try:
        key = f"{PROCESSED_SUBMISSIONS_KEY}:{submission_id}"
        exists = redis_client.exists(key)
        
        if not exists:
            # Сохраняем submission_id на 24 часа
            redis_client.setex(key, 86400, "1")
            return False
        
        return True
    except Exception as e:
        logger.error(f"Error checking duplicate submission: {e}")
        return False


def send_to_queue(message_data: dict) -> bool:
    """Отправить сообщение в Redis Queue с retry логикой."""
    if not redis_client:
        logger.error("Redis client not initialized")
        return False
    
    message_json = json.dumps(message_data, ensure_ascii=False)
    
    for attempt in range(MAX_RETRIES):
        try:
            redis_client.lpush(QUEUE_KEY, message_json)
            logger.info(f"Message sent to queue: {message_data.get('submission_id')}")
            return True
        except Exception as e:
            delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)] if attempt < MAX_RETRIES - 1 else 0
            logger.warning(f"Failed to send to queue (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            
            if attempt < MAX_RETRIES - 1:
                time.sleep(delay)
            else:
                logger.error(f"Failed to send message to queue after {MAX_RETRIES} attempts")
    
    return False


# Lifespan events для FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events для FastAPI."""
    # Startup
    init_redis()
    logger.info("Yandex Forms Webhook server started")
    yield
    # Shutdown
    if redis_client:
        redis_client.close()
    logger.info("Yandex Forms Webhook server stopped")


# Инициализация FastAPI с lifespan
app = FastAPI(
    title="SmartOrder Engine - Yandex Forms Webhook",
    description="Webhook endpoint для приёма данных от Яндекс.Форм",
    version="1.0.0",
    lifespan=lifespan
)

# Добавление middleware
app.add_middleware(RateLimitMiddleware)

# CORS middleware (если нужно принимать запросы с других доменов)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В production указать конкретные домены
    allow_credentials=True,
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


@app.get(
    "/health",
    summary="Health Check",
    description="Проверка состояния сервиса и подключения к Redis",
    responses={
        200: {
            "description": "Статус сервиса",
            "content": {
                "application/json": {
                    "example": {
                        "status": "ok",
                        "redis": "ok"
                    }
                }
            }
        }
    }
)
async def health_check():
    """
    Health check endpoint.
    
    Проверяет подключение к Redis.
    """
    try:
        if redis_client:
            redis_client.ping()
            redis_status = "ok"
        else:
            redis_status = "not_configured"
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        redis_status = "error"
    
    return {
        "status": "ok" if redis_status == "ok" else "degraded",
        "redis": redis_status
    }


@app.get("/favicon.ico")
async def favicon():
    """Обработчик для favicon.ico."""
    from fastapi.responses import Response
    return Response(status_code=204)


@app.get("/webhook/yandex-forms")
async def yandex_forms_webhook_info():
    """
    Информационный endpoint для проверки доступности webhook.
    
    Возвращает информацию о webhook и инструкции по использованию.
    """
    return {
        "status": "ok",
        "message": "Yandex Forms Webhook is running",
        "endpoint": "/webhook/yandex-forms",
        "method": "POST",
        "description": "Send POST requests with Yandex Form submission data",
        "example": {
            "form_id": "form_123",
            "form_name": "Заказ товаров",
            "submission_id": "sub_456",
            "data": {
                "name": "Иван Иванов",
                "phone": "+79001234567",
                "products": "2 варочные панели"
            }
        }
    }


@app.post("/webhook/yandex-forms", response_model=WebhookResponse)
async def yandex_forms_webhook(
    submission: YandexFormSubmission,
    request: Request
):
    """
    Webhook endpoint для приёма данных от Яндекс.Форм.
    
    Args:
        submission: Данные формы от Яндекс.Форм
        request: FastAPI Request объект для доступа к заголовкам
    
    Returns:
        WebhookResponse с подтверждением получения данных
    """
    # Логирование запроса
    client_ip = request.client.host if request.client else "unknown"
    logger.info(f"Received submission from {client_ip}: form_id={submission.form_id}, submission_id={submission.submission_id}")
    
    # Проверка подписи (если настроена)
    if YANDEX_FORMS_SECRET:
        # Получение подписи из заголовка или payload
        signature_header = request.headers.get("X-Yandex-Forms-Signature", "")
        signature = submission.signature or signature_header
        
        if not verify_signature(
            json.dumps(submission.model_dump(), sort_keys=True).encode('utf-8'),
            signature
        ):
            logger.warning(f"Invalid signature for submission {submission.submission_id}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid signature"
            )
    
    # Проверка на дубликаты (idempotency)
    if is_duplicate_submission(submission.submission_id):
        logger.info(f"Duplicate submission {submission.submission_id}, ignoring")
        return WebhookResponse(
            status="ok",
            message="Duplicate submission ignored"
        )
    
    # Формирование timestamp если не указан
    timestamp = submission.timestamp or datetime.now(timezone.utc).isoformat()
    
    # Формирование сообщения для очереди
    message_data = {
        "channel": "yandex_forms",
        "form_id": submission.form_id,
        "form_name": submission.form_name,
        "submission_id": submission.submission_id,
        "data": submission.data,
        "timestamp": timestamp
    }
    
    # Отправка в очередь
    if send_to_queue(message_data):
        logger.info(f"Successfully processed submission {submission.submission_id}")
        return WebhookResponse(
            status="ok",
            message="Submission received and queued"
        )
    else:
        logger.error(f"Failed to queue submission {submission.submission_id}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process submission"
        )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Обработчик HTTP исключений."""
    logger.error(f"HTTP {exc.status_code}: {exc.detail} from {request.client.host if request.client else 'unknown'}")
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Обработчик общих исключений."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "Internal server error"}
    )


if __name__ == "__main__":
    import uvicorn
    
    # Создание директории для логов если её нет
    os.makedirs("logs", exist_ok=True)
    
    uvicorn.run(
        app,
        host=WEBHOOK_HOST,
        port=WEBHOOK_PORT,
        log_level="info"
    )
