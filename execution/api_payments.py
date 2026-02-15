#!/usr/bin/env python3
"""
API Payments - FastAPI endpoints для обработки оплаты заказов.

Предоставляет REST API для обработки оплаты заказов (fake карта).
"""

import os
import logging
from typing import Optional
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status, Path as FastAPIPath
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, field_validator

# Импорты с поддержкой как относительных, так и абсолютных
try:
    from .payment_processor import PaymentProcessor, PaymentValidationError, PaymentProcessingError
except ImportError:
    import sys
    from pathlib import Path as PathLib
    project_root = PathLib(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from execution.payment_processor import PaymentProcessor, PaymentValidationError, PaymentProcessingError

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/api_payments.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Переменные окружения
API_PORT = int(os.getenv('API_PORT', '8025'))
API_HOST = os.getenv('API_HOST', '0.0.0.0')


# Pydantic модели
class CardData(BaseModel):
    """Модель данных карты."""
    number: str = Field(..., description="Номер карты (16 цифр)")
    cvv: str = Field(..., description="CVV код (3 цифры)")
    expiry: str = Field(..., description="Срок действия (MM/YY)")
    holder_name: str = Field(..., description="Имя держателя карты")
    
    @field_validator('number')
    @classmethod
    def validate_number(cls, v):
        """Валидация номера карты."""
        import re
        cleaned = re.sub(r'[\s-]', '', v)
        if not re.match(r'^\d{16}$', cleaned):
            raise ValueError("Card number must be 16 digits")
        return cleaned
    
    @field_validator('cvv')
    @classmethod
    def validate_cvv(cls, v):
        """Валидация CVV."""
        import re
        if not re.match(r'^\d{3}$', v):
            raise ValueError("CVV must be 3 digits")
        return v
    
    @field_validator('expiry')
    @classmethod
    def validate_expiry(cls, v):
        """Валидация срока действия."""
        import re
        from datetime import datetime
        if not re.match(r'^\d{2}/\d{2}$', v):
            raise ValueError("Expiry must be in MM/YY format")
        try:
            month, year = v.split('/')
            month_int = int(month)
            year_int = int(year)
            if month_int < 1 or month_int > 12:
                raise ValueError("Month must be between 01 and 12")
            current_year = datetime.now().year % 100
            if year_int < current_year:
                raise ValueError("Expiry year must be current or future")
        except ValueError as e:
            raise ValueError(f"Invalid expiry date: {e}")
        return v


class PaymentRequest(BaseModel):
    """Модель запроса на оплату."""
    card: CardData = Field(..., description="Данные карты")


class PaymentResponse(BaseModel):
    """Модель ответа на оплату."""
    transaction_id: str
    order_id: str
    order_number: str
    status: str
    amount: float
    paid_at: str
    card_last4: str


# Lifespan events для FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events для FastAPI."""
    # Startup
    logger.info("Starting API Payments server...")
    logger.info(f"Server will run on {API_HOST}:{API_PORT}")
    yield
    # Shutdown
    logger.info("Shutting down API Payments server...")


# Создание FastAPI приложения
app = FastAPI(
    title="SmartOrder Engine - Payments API",
    description="API для обработки оплаты заказов (fake карта). Валидация данных карты, симуляция оплаты, генерация transaction_id.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)


# Exception handlers
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc: RequestValidationError):
    """Обработчик ошибок валидации Pydantic."""
    errors = []
    for error in exc.errors():
        field = " -> ".join(str(loc) for loc in error["loc"])
        errors.append(f"{field}: {error['msg']}")
    
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Validation error",
                "details": errors
            }
        }
    )


@app.exception_handler(PaymentValidationError)
async def payment_validation_exception_handler(request, exc: PaymentValidationError):
    """Обработчик ошибок валидации данных карты."""
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "error": {
                "code": "INVALID_CARD_DATA",
                "message": str(exc)
            }
        }
    )


@app.exception_handler(PaymentProcessingError)
async def payment_processing_exception_handler(request, exc: PaymentProcessingError):
    """Обработчик ошибок обработки оплаты."""
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "error": {
                "code": "PAYMENT_PROCESSING_ERROR",
                "message": str(exc)
            }
        }
    )


# Endpoints
@app.get("/")
async def root():
    """Корневой endpoint."""
    return {
        "service": "SmartOrder Engine - Payments API",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/favicon.ico")
async def favicon():
    """Обработчик favicon."""
    from fastapi.responses import Response
    return Response(status_code=204)


@app.get(
    "/health",
    summary="Health Check",
    description="Проверка состояния сервиса",
    responses={
        200: {
            "description": "Статус сервиса",
            "content": {
                "application/json": {
                    "example": {
                        "status": "ok",
                        "service": "payments_api"
                    }
                }
            }
        }
    }
)
async def health_check():
    """
    Health check endpoint.
    
    Проверяет состояние сервиса обработки оплаты.
    """
    return {
        "status": "ok",
        "service": "payments_api"
    }


@app.post(
    "/api/payments/process/{order_id}",
    response_model=PaymentResponse,
    summary="Обработка оплаты заказа",
    description="Обработка оплаты заказа с использованием fake карты. Всегда успешная оплата для тестирования.",
    responses={
        200: {
            "description": "Оплата успешно обработана",
            "content": {
                "application/json": {
                    "example": {
                        "transaction_id": "TXN-20260213-123456",
                        "order_id": "123e4567-e89b-12d3-a456-426614174000",
                        "order_number": "ORD-2026-0001",
                        "status": "paid",
                        "amount": 245168.0,
                        "paid_at": "2026-02-13T10:30:00Z",
                        "card_last4": "1234"
                    }
                }
            }
        },
        400: {"description": "Ошибка валидации данных карты или обработки оплаты"},
        404: {"description": "Заказ не найден"},
        500: {"description": "Внутренняя ошибка сервера"}
    }
)
async def process_payment(
    order_id: str = FastAPIPath(..., description="UUID заказа"),
    payment_request: PaymentRequest = ...
):
    """
    Обработка оплаты заказа (fake карта).
    
    Процесс:
    1. Валидация данных карты (формат, срок действия)
    2. Симуляция оплаты (всегда успешная)
    3. Генерация transaction_id
    4. Обновление статуса заказа на "paid"
    5. Автоматическая генерация трек-номера
    6. Автоматический экспорт счёта в 1С
    
    ⚠️ ВАЖНО: Это fake система, не использовать реальные платёжные данные!
    """
    try:
        # Преобразование данных карты в словарь
        card_data = payment_request.card.model_dump()
        
        # Обработка оплаты
        result = PaymentProcessor.process_payment(order_id, card_data)
        
        logger.info(
            f"Payment processed successfully for order {order_id}",
            extra={
                "order_id": order_id,
                "transaction_id": result["transaction_id"],
                "amount": result["amount"]
            }
        )
        
        return PaymentResponse(**result)
        
    except PaymentValidationError as e:
        logger.warning(
            f"Payment validation error for order {order_id}: {e}",
            extra={"order_id": order_id}
        )
        raise
    except PaymentProcessingError as e:
        logger.error(
            f"Payment processing error for order {order_id}: {e}",
            extra={"order_id": order_id}
        )
        raise
    except Exception as e:
        logger.error(
            f"Unexpected error processing payment for order {order_id}: {e}",
            exc_info=True,
            extra={"order_id": order_id}
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process payment"
        )


# Для прямого запуска
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=API_HOST, port=API_PORT)
