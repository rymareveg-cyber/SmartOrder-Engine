#!/usr/bin/env python3
"""
FastAPI сервер для работы с заказами.

Предоставляет REST API endpoints для создания, получения и обновления заказов.
"""

import os
from src.config import APIConfig
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Path, status, Request, APIRouter
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from src.services.order_service import OrderService, OrderCreate, Order, OrderItem
from src.utils.logger import get_logger
logger = get_logger(__name__)

API_PORT = APIConfig.CATALOG_PORT
API_HOST = APIConfig.HOST


class OrderListResponse(BaseModel):
    """Модель ответа со списком заказов."""
    items: List[Order]
    total: int
    page: int
    page_size: int
    pages: int


class OrderStatusUpdate(BaseModel):
    """Модель для обновления статуса заказа."""
    status: str
    paid_at: Optional[str] = None
    shipped_at: Optional[str] = None
    
    @field_validator('status')
    @classmethod
    def validate_status(cls, v):
        allowed_statuses = ['new', 'validated', 'invoice_created', 'paid', 'shipped', 'cancelled']
        if v not in allowed_statuses:
            raise ValueError(f"Status must be one of {allowed_statuses}")
        return v


# Lifespan events для FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events для FastAPI."""
    # Startup
    logger.info("Orders API server starting")
    yield
    # Shutdown
    logger.info("Orders API server stopped")


# Создаём отдельный router для включения в другие приложения
router = APIRouter()

# Инициализация FastAPI с lifespan (для запуска отдельно)
app = FastAPI(
    title="SmartOrder Engine - Orders API",
    description="API для работы с заказами. Поддерживает создание, получение, обновление статусов и поиск заказов по телефону.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",  # Swagger документация
    redoc_url="/redoc"  # ReDoc документация
)

# Включаем router в app для обратной совместимости (если запускается отдельно)
app.include_router(router)


@router.get("/favicon.ico")
async def favicon():
    """Обработчик для favicon.ico."""
    from fastapi.responses import Response
    return Response(status_code=204)


@router.post("/api/orders", response_model=Order, status_code=status.HTTP_201_CREATED)
async def create_order(order_data: OrderCreate):
    """
    Создание нового заказа.
    
    Процесс:
    1. Валидация данных заказа
    2. Генерация уникального номера заказа (ORD-YYYY-NNNN)
    3. Автоматический расчёт стоимости доставки (если не указана)
    4. Нормализация номера телефона
    5. Создание заказа и позиций в БД
    6. Возврат созданного заказа
    
    Args:
        order_data: Данные заказа (канал, товары, контакты клиента)
        
    Returns:
        Созданный заказ с присвоенным номером
    """
    try:
        order_dict = order_data.model_dump()
        order = OrderService.create_order(order_dict)
        logger.info(f"Order created: {order.order_number}")
        return order
    except ValueError as e:
        logger.error(f"Validation error creating order: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating order: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create order")


@router.get(
    "/api/orders/by-phone",
    response_model=Dict[str, Any],
    summary="Получение заказов по телефону",
    description="Получение всех заказов пользователя по номеру телефона. Используется для Mini App.",
    responses={
        200: {
            "description": "Список заказов найден",
            "content": {
                "application/json": {
                    "example": {
                        "phone": "+79991234567",
                        "normalized_phone": "+79991234567",
                        "orders": [],
                        "total": 0
                    }
                }
            }
        },
        500: {"description": "Внутренняя ошибка сервера"}
    }
)
async def get_orders_by_phone(
    phone: str = Query(..., description="Номер телефона (любой формат, будет нормализован)"),
    telegram_user_id: Optional[int] = Query(None, description="Telegram user ID для проверки безопасности (опционально)")
):
    """
    Получение всех заказов пользователя по номеру телефона.
    
    Используется для Mini App для отображения истории заказов из всех каналов.
    
    Особенности:
    - Телефон нормализуется к формату +7XXXXXXXXXX
    - Возвращает заказы из всех каналов (Telegram, Яндекс.Почта, Яндекс.Формы)
    - Безопасность: проверка telegram_user_id для заказов из Telegram
    
    Args:
        phone: Номер телефона (любой формат, будет нормализован)
        telegram_user_id: Опционально, Telegram user ID для проверки безопасности
        
    Returns:
        Словарь с телефоном, нормализованным телефоном, списком заказов и общим количеством
    """
    try:
        orders = OrderService.get_orders_by_phone(phone, telegram_user_id)
        
        # Нормализация телефона для ответа
        from src.services.order_service import normalize_phone_number
        
        normalized_phone = normalize_phone_number(phone)
        
        return {
            "phone": phone,
            "normalized_phone": normalized_phone,
            "orders": [order.model_dump() for order in orders],
            "total": len(orders)
        }
    except Exception as e:
        logger.error(f"Error getting orders by phone: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get orders by phone"
        )


@router.get(
    "/api/orders/health",
    summary="Health Check",
    description="Проверка состояния сервиса и подключения к БД",
    responses={
        200: {
            "description": "Статус сервиса",
            "content": {
                "application/json": {
                    "example": {
                        "status": "ok",
                        "database": "ok"
                    }
                }
            }
        }
    }
)
async def orders_health_check():
    """
    Health check endpoint для Orders API.
    
    Проверяет подключение к PostgreSQL.
    """
    try:
        # Проверка подключения к БД
        from src.services.order_service import get_db_connection, return_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        return_db_connection(conn)
        db_status = "ok"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        db_status = "error"
    
    return {
        "status": "ok" if db_status == "ok" else "degraded",
        "database": db_status,
        "service": "orders_api"
    }


@router.get(
    "/api/orders/docs",
    summary="Swagger Documentation Redirect",
    description="Перенаправление на Swagger документацию",
    include_in_schema=False
)
async def orders_docs_redirect():
    """
    Перенаправление на корневую Swagger документацию.
    
    Swagger документация доступна по адресу /docs на корневом уровне.
    """
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/docs", status_code=307)


@router.get(
    "/api/orders/{order_id}",
    response_model=Order,
    summary="Получение заказа",
    description="Получение заказа по UUID с полной информацией о товарах и статусе",
    responses={
        200: {"description": "Заказ найден"},
        404: {"description": "Заказ не найден"}
    }
)
async def get_order(
    order_id: str = Path(..., description="UUID заказа")
):
    """
    Получение заказа по ID.
    
    Args:
        order_id: UUID заказа
        
    Returns:
        Заказ с полной информацией (товары, статус, контакты, даты, telegram_user_id)
    """
    try:
        order = OrderService.get_order(order_id)
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order with id '{order_id}' not found"
            )
        return order
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting order {order_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to get order")


@router.patch(
    "/api/orders/{order_id}/status",
    response_model=Order,
    summary="Обновление статуса заказа",
    description="Обновление статуса заказа с валидацией переходов между статусами",
    responses={
        200: {"description": "Статус успешно обновлён"},
        400: {"description": "Некорректный переход статуса"},
        404: {"description": "Заказ не найден"}
    }
)
async def update_order_status(
    order_id: str = Path(..., description="UUID заказа"),
    status_update: OrderStatusUpdate = ...
):
    """
    Обновление статуса заказа.
    
    Валидация переходов:
    - new → validated
    - validated → invoice_created
    - invoice_created → paid
    - paid → shipped
    - Любой → cancelled
    
    Args:
        order_id: UUID заказа
        status_update: Новый статус и опциональные поля (paid_at, shipped_at)
        
    Returns:
        Обновлённый заказ
    """
    try:
        # Получаем текущий статус заказа перед обновлением
        current_order = OrderService.get_order(order_id)
        if not current_order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order with id '{order_id}' not found"
            )
        old_status = current_order.status
        
        update_data = status_update.model_dump(exclude_none=True)
        order = OrderService.update_order_status(order_id, update_data["status"], **update_data)
        
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order with id '{order_id}' not found"
            )
        
        logger.info(f"Order {order_id} status updated to {update_data['status']}")
        
        # Отправка уведомления в Telegram (если есть telegram_user_id и статус изменился)
        if order.telegram_user_id and old_status != update_data["status"]:
            try:
                from src.services.telegram_bot import send_status_change_notification
                
                # Вызываем асинхронную функцию напрямую через await (мы уже в async контексте FastAPI)
                await send_status_change_notification(
                    telegram_user_id=order.telegram_user_id,
                    order_number=order.order_number,
                    old_status=old_status,
                    new_status=update_data["status"]
                )
            except Exception as e:
                logger.warning(f"Failed to send status change notification: {e}")
        
        return order
    except ValueError as e:
        logger.error(f"Validation error updating order status: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating order status: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update order status")


@router.get("/api/orders", response_model=OrderListResponse)
async def list_orders(
    status: Optional[str] = Query(None, description="Фильтр по статусу"),
    channel: Optional[str] = Query(None, description="Фильтр по каналу"),
    customer_phone: Optional[str] = Query(None, description="Фильтр по телефону"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    page_size: int = Query(20, ge=1, le=100, description="Размер страницы")
):
    """
    Получение списка заказов с фильтрацией и пагинацией.
    
    Args:
        status: Фильтр по статусу
        channel: Фильтр по каналу
        customer_phone: Фильтр по телефону
        page: Номер страницы
        page_size: Размер страницы
        
    Returns:
        Список заказов с метаданными пагинации
    """
    try:
        result = OrderService.list_orders(
            status=status,
            channel=channel,
            customer_phone=customer_phone,
            page=page,
            page_size=page_size
        )
        return result
    except Exception as e:
        logger.error(f"Error listing orders: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to list orders")


@router.get("/api/orders/{order_id}/items", response_model=List[OrderItem])
async def get_order_items(
    order_id: str = Path(..., description="UUID заказа")
):
    """
    Получение позиций заказа.
    
    Args:
        order_id: UUID заказа
        
    Returns:
        Список позиций заказа
    """
    try:
        # Проверка существования заказа
        order = OrderService.get_order(order_id)
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order with id '{order_id}' not found"
            )
        
        items = OrderService.get_order_items(order_id)
        return items
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting order items: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to get order items")


@router.post(
    "/api/orders/{order_id}/generate-tracking",
    summary="Генерация трек-номера",
    description="Генерация трек-номера для заказа и обновление статуса на 'shipped'",
    responses={
        200: {"description": "Трек-номер успешно сгенерирован"},
        400: {"description": "Ошибка генерации трек-номера"},
        404: {"description": "Заказ не найден"}
    }
)
async def generate_tracking(
    order_id: str = Path(..., description="UUID заказа")
):
    """
    Генерация трек-номера для заказа.
    
    Процесс:
    1. Генерация уникального трек-номера (TRACK-YYYYMMDD-XXXXXX)
    2. Обновление поля tracking_number в заказе
    3. Обновление статуса на "shipped"
    4. Установка shipped_at
    
    Args:
        order_id: UUID заказа
        
    Returns:
        Результат генерации трек-номера
    """
    try:
        from src.services.tracking_generator import TrackingGenerator, TrackingGenerationError
        
        result = TrackingGenerator.generate_and_update(order_id)
        
        logger.info(
            f"Tracking number generated for order {order_id}",
            extra={
                "order_id": order_id,
                "tracking_number": result["tracking_number"]
            }
        )
        
        return result
        
    except TrackingGenerationError as e:
        logger.warning(
            f"Tracking generation error for order {order_id}: {e}",
            extra={"order_id": order_id}
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"Unexpected error generating tracking for order {order_id}: {e}",
            exc_info=True,
            extra={"order_id": order_id}
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate tracking number"
        )


@router.get(
    "/health",
    summary="Health Check",
    description="Проверка состояния сервиса и подключения к БД",
    responses={
        200: {
            "description": "Статус сервиса",
            "content": {
                "application/json": {
                    "example": {
                        "status": "ok",
                        "database": "ok"
                    }
                }
            }
        }
    }
)
async def health_check():
    """
    Health check endpoint.
    
    Проверяет подключение к PostgreSQL.
    """
    return {
        "status": "ok",
        "service": "orders_api"
    }


# Exception handlers для app (если запускается отдельно)
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Обработчик ошибок валидации Pydantic."""
    logger.error(f"Validation error: {exc.errors()}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "Validation error",
            "details": exc.errors()
        }
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Обработчик HTTP исключений."""
    logger.error(f"HTTP {exc.status_code}: {exc.detail}")
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
    import sys
    import signal
    
    # Создание директории для логов если её нет
    os.makedirs("logs", exist_ok=True)

    # Раздельные логи uvicorn (access/error) в файлы logs/api_orders.uvicorn.*.log
    try:
        from src.utils.logger import setup_uvicorn_logging
        setup_uvicorn_logging("api_orders")
    except Exception as e:
        logger.warning(f"Failed to setup uvicorn logging: {e}")
    
    def signal_handler(signum, frame):
        """Обработчик сигналов для graceful shutdown."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        sys.exit(0)
    
    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        uvicorn.run(
            app,
            host=API_HOST,
            port=API_PORT,
            log_level="info",
            log_config=None
        )
    except KeyboardInterrupt:
        logger.info("Orders API stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in Orders API: {e}", exc_info=True)
        sys.exit(1)
