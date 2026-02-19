#!/usr/bin/env python3
"""
Dashboard API - FastAPI сервер для веб-интерфейса менеджера.

Предоставляет REST API endpoints для просмотра статистики, управления заказами и мониторинга системы.
"""

import os
import time
from src.config import DatabaseConfig, RedisConfig, APIConfig
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
from pathlib import Path as PathLib

from fastapi import FastAPI, HTTPException, Query, Path, status, Request, BackgroundTasks
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src.services.order_service import OrderService, Order, OrderItem
from src.services.data_exporter import DataExporter
from src.utils.logger import get_logger
logger = get_logger(__name__)

DASHBOARD_PORT = APIConfig.DASHBOARD_PORT
DASHBOARD_HOST = APIConfig.HOST
DATABASE_URL = DatabaseConfig.URL
CACHE_TTL = int(os.getenv('DASHBOARD_CACHE_TTL', '60'))

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional
import json
import hashlib

from src.utils.redis_client import init_redis_client
from src.database.pool import DatabasePool, init_db_pool as init_db_pool_util, get_db_connection as get_db_connection_util, return_db_connection as return_db_connection_util

dashboard_db_pool: Optional[DatabasePool] = None
redis_client: Optional[Any] = None

dashboard_db_available = True
dashboard_db_failures = 0
DASHBOARD_DB_MAX_FAILURES = 3
dashboard_db_circuit_breaker_reset_time = None
DASHBOARD_DB_CIRCUIT_BREAKER_RESET_INTERVAL = 60
def init_dashboard_db_pool():
    """Инициализация отдельного connection pool для Dashboard API."""
    global dashboard_db_pool
    # Отдельный пул для dashboard
    # Увеличено до 30 соединений для высокой нагрузки
    max_connections = int(os.getenv('DASHBOARD_DB_POOL_MAX_CONNECTIONS', '30'))
    min_connections = int(os.getenv('DASHBOARD_DB_POOL_MIN_CONNECTIONS', '10'))
    
    dashboard_db_pool = init_db_pool_util(
        minconn=min_connections,
        maxconn=max_connections,
        dsn=DATABASE_URL
    )


def init_redis():
    """Инициализация Redis клиента для кэширования."""
    global redis_client
    redis_client = init_redis_client(decode_responses=True, raise_on_error=False)


def get_cache_key(prefix: str, **params) -> str:
    """Генерация ключа кэша из параметров."""
    # Создаём строку из параметров для хеширования
    param_str = json.dumps(params, sort_keys=True, ensure_ascii=False)
    # Хешируем для короткого ключа
    param_hash = hashlib.md5(param_str.encode('utf-8')).hexdigest()[:12]
    return f"dashboard:{prefix}:{param_hash}"


def get_from_cache(key: str) -> Optional[dict]:
    """Получить данные из кэша."""
    if not redis_client:
        return None
    try:
        cached = redis_client.get(key)
        if cached:
            return json.loads(cached)
    except Exception as e:
        logger.debug(f"Cache get error: {e}")
    return None


def set_to_cache(key: str, value: dict, ttl: int = CACHE_TTL):
    """Сохранить данные в кэш."""
    if not redis_client:
        return
    try:
        redis_client.setex(key, ttl, json.dumps(value, ensure_ascii=False))
    except Exception as e:
        logger.debug(f"Cache set error: {e}")


def invalidate_cache(pattern: str = "dashboard:*"):
    """Инвалидация кэша по паттерну."""
    if not redis_client:
        return
    try:
        keys = redis_client.keys(pattern)
        if keys:
            redis_client.delete(*keys)
            logger.info(f"Invalidated {len(keys)} cache keys matching '{pattern}'")
    except Exception as e:
        logger.warning(f"Cache invalidation error: {e}")


def get_dashboard_db_connection():
    """Получить соединение с БД из dashboard pool с таймаутом и circuit breaker."""
    global dashboard_db_available, dashboard_db_failures, dashboard_db_pool, dashboard_db_circuit_breaker_reset_time
    
    # Проверка автоматического сброса circuit breaker
    if not dashboard_db_available and dashboard_db_circuit_breaker_reset_time:
        elapsed = time.time() - dashboard_db_circuit_breaker_reset_time
        if elapsed >= DASHBOARD_DB_CIRCUIT_BREAKER_RESET_INTERVAL:
            logger.info("Dashboard DB circuit breaker auto-reset after timeout")
            dashboard_db_available = True
            dashboard_db_failures = 0
            dashboard_db_circuit_breaker_reset_time = None
    
    if not dashboard_db_available:
        # Circuit breaker открыт - сразу возвращаем ошибку
        raise TimeoutError("Dashboard DB circuit breaker is open")
    
    if dashboard_db_pool is None:
        init_dashboard_db_pool()
    
    timeout = 3  # Увеличено до 3 секунд для большей надежности
    
    try:
        # Используем общий timeout для get_connection
        conn = dashboard_db_pool.get_connection(timeout=timeout, retry_interval=0.1)
        # Успешно получили соединение - сбрасываем счётчик ошибок
        dashboard_db_failures = 0
        dashboard_db_available = True
        dashboard_db_circuit_breaker_reset_time = None
        return conn
    except (TimeoutError, Exception) as e:
        dashboard_db_failures += 1
        if dashboard_db_failures >= DASHBOARD_DB_MAX_FAILURES:
            dashboard_db_available = False
            dashboard_db_circuit_breaker_reset_time = time.time()
            logger.warning(f"Dashboard DB circuit breaker opened after {dashboard_db_failures} failures: {e}")
        if isinstance(e, TimeoutError):
            raise
        else:
            # Для других ошибок тоже поднимаем TimeoutError для единообразия
            raise TimeoutError(f"Failed to get dashboard DB connection: {e}")


def return_dashboard_db_connection(conn):
    """Вернуть соединение в dashboard pool."""
    global dashboard_db_pool
    if dashboard_db_pool and conn:
        try:
            dashboard_db_pool.return_connection(conn)
        except Exception as e:
            logger.warning(f"Error returning dashboard DB connection: {e}")


class OrderStatusUpdate(BaseModel):
    """Модель для обновления статуса заказа."""
    status: str = Field(..., description="Новый статус заказа")


class StatsResponse(BaseModel):
    """Модель ответа со статистикой."""
    revenue_today: float
    revenue_week: float
    revenue_month: float
    orders_today: int
    orders_week: int
    orders_month: int
    conversion_rate: float
    average_check: float
    top_products: List[Dict[str, Any]]
    # Сравнение с предыдущим периодом
    period_comparison: Optional[Dict[str, Any]] = None
    # Дополнительные метрики
    new_orders_today: int = 0
    new_orders_week: int = 0
    new_orders_month: int = 0
    paid_orders_today: int = 0
    paid_orders_week: int = 0
    paid_orders_month: int = 0
    cancelled_orders_today: int = 0
    cancelled_orders_week: int = 0
    cancelled_orders_month: int = 0
    average_basket_size: float = 0.0  # Среднее количество товаров в заказе
    repeat_customers_count: int = 0  # Количество клиентов с повторными покупками
    revenue_forecast: Optional[float] = None  # Прогноз выручки на следующий период


class SyncStatusResponse(BaseModel):
    """Модель ответа со статусом синхронизации."""
    last_sync: Optional[str]
    products_count: int
    status: str


class AnalyticsResponse(BaseModel):
    """Модель ответа с детальной аналитикой."""
    # Динамика выручки по дням
    revenue_by_days: List[Dict[str, Any]]
    # Анализ по каналам
    channel_analysis: Dict[str, Dict[str, Any]]
    # Воронка продаж
    sales_funnel: Dict[str, int]
    # Распределение по статусам
    status_distribution: Dict[str, int]
    # Дополнительные метрики
    metrics: Dict[str, Any]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events для FastAPI с graceful shutdown."""
    logger.info("Dashboard API server starting")
    try:
        init_dashboard_db_pool()
        logger.info("Dashboard database connection pool initialized")
    except Exception as e:
        logger.error(f"Failed to initialize dashboard database pool: {e}", exc_info=True)
    
    try:
        init_redis()
    except Exception as e:
        logger.warning(f"Failed to initialize Redis: {e}. Caching disabled.")
    
    yield
    
    logger.info("Dashboard API server shutting down gracefully")
    
    global dashboard_db_pool
    if dashboard_db_pool:
        try:
            dashboard_db_pool.close_all()
            logger.info("Dashboard database connection pool closed")
        except Exception as e:
            logger.warning(f"Error closing dashboard database pool: {e}")
    
    global redis_client
    if redis_client:
        try:
            redis_client.close()
            logger.info("Redis client closed")
        except Exception as e:
            logger.warning(f"Error closing Redis client: {e}")


app = FastAPI(
    title="SmartOrder Engine - Dashboard API",
    description="API для веб-интерфейса менеджера. Статистика, аналитика, управление заказами и экспорт данных.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    
    # Отключаем автоматическое чтение .env через переменную окружения
    import os
    original_env_file = os.environ.get('ENV_FILE')
    if original_env_file:
        os.environ.pop('ENV_FILE')
    
    # Инициализируем limiter с явным указанием storage (memory) для избежания проблем с кодировкой .env
    try:
        limiter = Limiter(
            key_func=get_remote_address,
            default_limits=["1000/hour"],
            storage_uri="memory://",
            headers_enabled=True
        )
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        RATE_LIMITING_ENABLED = True
        logger.info("Rate limiting enabled")
    except UnicodeDecodeError as e:
        logger.warning(f"Failed to initialize rate limiter due to encoding issue: {e}. Rate limiting disabled.")
        limiter = None
        RATE_LIMITING_ENABLED = False
    except Exception as e:
        logger.warning(f"Failed to initialize rate limiter: {e}. Rate limiting disabled.")
        limiter = None
        RATE_LIMITING_ENABLED = False
    finally:
        if original_env_file:
            os.environ['ENV_FILE'] = original_env_file
    
    def rate_limit(limit_str):
        """Декоратор для применения rate limiting."""
        if limiter is None:
            return lambda func: func
        def decorator(func):
            return limiter.limit(limit_str)(func)
        return decorator
except ImportError:
    limiter = None
    RATE_LIMITING_ENABLED = False
    logger.warning("slowapi not installed, rate limiting disabled")
    
    def rate_limit(limit_str):
        """Пустой декоратор если rate limiting отключен."""
        def decorator(func):
            return func
        return decorator
except Exception as e:
    logger.warning(f"Failed to initialize rate limiter: {e}. Rate limiting disabled.")
    limiter = None
    RATE_LIMITING_ENABLED = False
    
    def rate_limit(limit_str):
        """Пустой декоратор если rate limiting отключен."""
        def decorator(func):
            return func
        return decorator

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене указать конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

frontend_path = PathLib(__file__).parent / "dashboard_frontend"
if frontend_path.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_path)), name="static")


@app.get("/")
async def serve_dashboard():
    """Главная страница dashboard."""
    index_path = frontend_path / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    return {"message": "Dashboard frontend not found"}


@app.get("/favicon.ico")
async def favicon():
    """Обработчик для favicon.ico."""
    from fastapi.responses import Response
    return Response(status_code=204)


def get_stats_from_db(period: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Получение статистики из БД.
    
    Args:
        period: Период ('today', 'week', 'month', 'quarter', 'year', 'custom')
        start_date: Начальная дата для произвольного периода (ISO format)
        end_date: Конечная дата для произвольного периода (ISO format)
    
    Returns:
        Словарь со статистикой
    """
    conn = None
    try:
        try:
            conn = get_dashboard_db_connection()
        except (TimeoutError, Exception) as e:
            logger.error(f"Failed to get dashboard database connection (pool may be exhausted): {e}", exc_info=True)
            # Возвращаем пустую статистику вместо падения сервиса
            return {
                'revenue_today': 0.0,
                'revenue_week': 0.0,
                'revenue_month': 0.0,
                'orders_today': 0,
                'orders_week': 0,
                'orders_month': 0,
                'conversion_rate': 0.0,
                'average_check': 0.0,
                'top_products': [],
                'new_orders_today': 0,
                'new_orders_week': 0,
                'new_orders_month': 0,
                'paid_orders_today': 0,
                'paid_orders_week': 0,
                'paid_orders_month': 0,
                'cancelled_orders_today': 0,
                'cancelled_orders_week': 0,
                'cancelled_orders_month': 0,
                'average_basket_size': 0.0,
                'repeat_customers_count': 0
            }
        
        if not conn:
            logger.error("Failed to get database connection")
            # Возвращаем пустую статистику вместо падения
            return {
                'revenue_today': 0.0,
                'revenue_week': 0.0,
                'revenue_month': 0.0,
                'orders_today': 0,
                'orders_week': 0,
                'orders_month': 0,
                'conversion_rate': 0.0,
                'average_check': 0.0,
                'top_products': [],
                'new_orders_today': 0,
                'new_orders_week': 0,
                'new_orders_month': 0,
                'paid_orders_today': 0,
                'paid_orders_week': 0,
                'paid_orders_month': 0,
                'cancelled_orders_today': 0,
                'cancelled_orders_week': 0,
                'cancelled_orders_month': 0,
                'average_basket_size': 0.0,
                'repeat_customers_count': 0
            }
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Проверка доступности БД
        cursor.execute("SELECT 1")
        cursor.fetchone()
        
        now = datetime.now(timezone.utc)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.debug(f"Getting stats for period: {period}, today_start: {today_start}")
        
        # Определение периода для сравнения
        period_start = None
        period_end = now
        previous_period_start = None
        previous_period_end = None
        
        if period == 'custom' and start_date and end_date:
            # Произвольный период
            period_start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            period_end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            period_duration = (period_end - period_start).days
            previous_period_end = period_start
            previous_period_start = period_start - timedelta(days=period_duration)
        elif period == 'today':
            period_start = today_start
            previous_period_start = today_start - timedelta(days=1)
            previous_period_end = today_start
        elif period == 'week':
            period_start = now - timedelta(days=7)
            previous_period_start = period_start - timedelta(days=7)
            previous_period_end = period_start
        elif period == 'month':
            period_start = now - timedelta(days=30)
            previous_period_start = period_start - timedelta(days=30)
            previous_period_end = period_start
        elif period == 'quarter':
            period_start = now - timedelta(days=90)
            previous_period_start = period_start - timedelta(days=90)
            previous_period_end = period_start
        elif period == 'year':
            period_start = now - timedelta(days=365)
            previous_period_start = period_start - timedelta(days=365)
            previous_period_end = period_start
        
        week_start = now - timedelta(days=7)
        month_start = now - timedelta(days=30)
        
        # Выручка и количество заказов
        stats = {}
        
        # Сегодня
        # created_at уже TIMESTAMP WITH TIME ZONE, PostgreSQL автоматически конвертирует при сравнении
        cursor.execute("""
            SELECT 
                COUNT(*) as orders_count,
                COALESCE(SUM(total_amount), 0) as revenue
            FROM orders
            WHERE created_at >= %s AND status != 'cancelled'
        """, (today_start,))
        today = cursor.fetchone()
        stats['revenue_today'] = float(today['revenue']) if today and today['revenue'] is not None else 0.0
        stats['orders_today'] = today['orders_count'] if today and today['orders_count'] is not None else 0
        logger.debug(f"Today stats: orders={stats['orders_today']}, revenue={stats['revenue_today']}")
        
        # Неделя
        # created_at уже TIMESTAMP WITH TIME ZONE, PostgreSQL автоматически конвертирует при сравнении
        cursor.execute("""
            SELECT 
                COUNT(*) as orders_count,
                COALESCE(SUM(total_amount), 0) as revenue
            FROM orders
            WHERE created_at >= %s AND status != 'cancelled'
        """, (week_start,))
        week = cursor.fetchone()
        stats['revenue_week'] = float(week['revenue']) if week and week['revenue'] is not None else 0.0
        stats['orders_week'] = week['orders_count'] if week and week['orders_count'] is not None else 0
        
        # Месяц
        # created_at уже TIMESTAMP WITH TIME ZONE, PostgreSQL автоматически конвертирует при сравнении
        cursor.execute("""
            SELECT 
                COUNT(*) as orders_count,
                COALESCE(SUM(total_amount), 0) as revenue
            FROM orders
            WHERE created_at >= %s AND status != 'cancelled'
        """, (month_start,))
        month = cursor.fetchone()
        stats['revenue_month'] = float(month['revenue']) if month and month['revenue'] is not None else 0.0
        stats['orders_month'] = month['orders_count'] if month and month['orders_count'] is not None else 0
        
        # Конверсия (новые → оплаченные)
        cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE status = 'new') as new_orders,
                COUNT(*) FILTER (WHERE status = 'paid') as paid_orders
            FROM orders
            WHERE created_at >= %s
        """, (month_start,))
        conversion = cursor.fetchone()
        new_orders = conversion['new_orders'] or 0
        paid_orders = conversion['paid_orders'] or 0
        stats['conversion_rate'] = (paid_orders / new_orders * 100) if new_orders > 0 else 0.0
        
        # Средний чек
        cursor.execute("""
            SELECT 
                COALESCE(AVG(total_amount), 0) as avg_check
            FROM orders
            WHERE created_at >= %s AND status = 'paid'
        """, (month_start,))
        avg = cursor.fetchone()
        stats['average_check'] = float(avg['avg_check']) if avg else 0.0
        
        # Топ товаров
        cursor.execute("""
            SELECT 
                oi.product_articul,
                oi.product_name,
                SUM(oi.quantity) as total_quantity,
                SUM(oi.total) as total_revenue
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.id
            WHERE o.created_at >= %s AND o.status != 'cancelled'
            GROUP BY oi.product_articul, oi.product_name
            ORDER BY total_quantity DESC
            LIMIT 10
        """, (month_start,))
        top_products = cursor.fetchall()
        stats['top_products'] = [
            {
                "articul": row['product_articul'],
                "name": row['product_name'],
                "quantity": row['total_quantity'],
                "revenue": float(row['total_revenue'])
            }
            for row in top_products
        ]
        
        # Дополнительные метрики: объединяем все запросы по статусам в один для оптимизации
        # Используем один запрос с FILTER для подсчёта по разным периодам и статусам
        cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE created_at >= %s AND status = 'new') as new_today,
                COUNT(*) FILTER (WHERE created_at >= %s AND status = 'new') as new_week,
                COUNT(*) FILTER (WHERE created_at >= %s AND status = 'new') as new_month,
                COUNT(*) FILTER (WHERE created_at >= %s AND status = 'paid') as paid_today,
                COUNT(*) FILTER (WHERE created_at >= %s AND status = 'paid') as paid_week,
                COUNT(*) FILTER (WHERE created_at >= %s AND status = 'paid') as paid_month,
                COUNT(*) FILTER (WHERE created_at >= %s AND status = 'cancelled') as cancelled_today,
                COUNT(*) FILTER (WHERE created_at >= %s AND status = 'cancelled') as cancelled_week,
                COUNT(*) FILTER (WHERE created_at >= %s AND status = 'cancelled') as cancelled_month
            FROM orders
            WHERE created_at >= %s
        """, (today_start, week_start, month_start, today_start, week_start, month_start, 
              today_start, week_start, month_start, month_start))
        status_counts = cursor.fetchone()
        stats['new_orders_today'] = status_counts['new_today'] or 0
        stats['new_orders_week'] = status_counts['new_week'] or 0
        stats['new_orders_month'] = status_counts['new_month'] or 0
        stats['paid_orders_today'] = status_counts['paid_today'] or 0
        stats['paid_orders_week'] = status_counts['paid_week'] or 0
        stats['paid_orders_month'] = status_counts['paid_month'] or 0
        stats['cancelled_orders_today'] = status_counts['cancelled_today'] or 0
        stats['cancelled_orders_week'] = status_counts['cancelled_week'] or 0
        stats['cancelled_orders_month'] = status_counts['cancelled_month'] or 0
        
        # Средний размер корзины (количество товаров в заказе)
        cursor.execute("""
            SELECT 
                AVG(item_count) as avg_basket_size
            FROM (
                SELECT 
                    o.id,
                    COUNT(oi.id) as item_count
                FROM orders o
                LEFT JOIN order_items oi ON o.id = oi.order_id
                WHERE o.created_at >= %s AND o.status != 'cancelled'
                GROUP BY o.id
            ) as basket_sizes
        """, (month_start,))
        basket_size = cursor.fetchone()
        stats['average_basket_size'] = round(float(basket_size['avg_basket_size']) if basket_size and basket_size['avg_basket_size'] else 0.0, 2)
        
        # Повторные покупки (клиенты с >1 заказом)
        cursor.execute("""
            SELECT COUNT(*) as repeat_customers
            FROM (
                SELECT customer_phone
                FROM orders
                WHERE created_at >= %s 
                    AND customer_phone IS NOT NULL 
                    AND customer_phone != ''
                    AND status != 'cancelled'
                GROUP BY customer_phone
                HAVING COUNT(*) > 1
            ) as repeat_customers
        """, (month_start,))
        repeat_customers = cursor.fetchone()
        stats['repeat_customers_count'] = repeat_customers['repeat_customers'] if repeat_customers else 0
        
        # Прогноз выручки (на основе тренда за последние 7 дней)
        cursor.execute("""
            SELECT 
                DATE(created_at) as date,
                COALESCE(SUM(total_amount), 0) as daily_revenue
            FROM orders
            WHERE created_at >= %s 
                AND created_at < %s
                AND status != 'cancelled'
            GROUP BY DATE(created_at)
            ORDER BY date DESC
            LIMIT 7
        """, (now - timedelta(days=7), now))
        recent_revenue = cursor.fetchall()
        
        if recent_revenue and len(recent_revenue) >= 3:
            # Простой прогноз: средняя выручка за последние дни
            daily_revenues = [float(row['daily_revenue']) for row in recent_revenue]
            avg_daily_revenue = sum(daily_revenues) / len(daily_revenues)
            # Прогноз на следующий период (7 дней)
            stats['revenue_forecast'] = round(avg_daily_revenue * 7, 2)
        else:
            stats['revenue_forecast'] = None
        
        # Сравнение с предыдущим периодом (если указан период)
        if period and period_start and previous_period_start:
            # Статистика текущего периода
            cursor.execute("""
                SELECT 
                    COUNT(*) as orders_count,
                    COALESCE(SUM(total_amount), 0) as revenue
                FROM orders
                WHERE created_at >= %s AND created_at < %s AND status != 'cancelled'
            """, (period_start, period_end))
            current_period = cursor.fetchone()
            current_revenue = float(current_period['revenue']) if current_period else 0.0
            current_orders = current_period['orders_count'] if current_period else 0
            
            # Статистика предыдущего периода
            cursor.execute("""
                SELECT 
                    COUNT(*) as orders_count,
                    COALESCE(SUM(total_amount), 0) as revenue
                FROM orders
                WHERE created_at >= %s AND created_at < %s AND status != 'cancelled'
            """, (previous_period_start, previous_period_end))
            previous_period = cursor.fetchone()
            previous_revenue = float(previous_period['revenue']) if previous_period else 0.0
            previous_orders = previous_period['orders_count'] if previous_period else 0
            
            # Расчет изменений в процентах
            revenue_change = ((current_revenue - previous_revenue) / previous_revenue * 100) if previous_revenue > 0 else 0.0
            orders_change = ((current_orders - previous_orders) / previous_orders * 100) if previous_orders > 0 else 0.0
            
            stats['period_comparison'] = {
                "current_revenue": current_revenue,
                "previous_revenue": previous_revenue,
                "revenue_change": round(revenue_change, 2),
                "current_orders": current_orders,
                "previous_orders": previous_orders,
                "orders_change": round(orders_change, 2),
                "period": period,
                "period_start": period_start.isoformat() if period_start else None,
                "period_end": period_end.isoformat() if period_end else None
            }
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}", exc_info=True)
        # Возвращаем пустую статистику вместо проброса ошибки
        # Это позволяет dashboard работать даже если БД недоступна
        return {
            'revenue_today': 0.0,
            'revenue_week': 0.0,
            'revenue_month': 0.0,
            'orders_today': 0,
            'orders_week': 0,
            'orders_month': 0,
            'conversion_rate': 0.0,
            'average_check': 0.0,
            'top_products': [],
            'new_orders_today': 0,
            'new_orders_week': 0,
            'new_orders_month': 0,
            'paid_orders_today': 0,
            'paid_orders_week': 0,
            'paid_orders_month': 0,
            'cancelled_orders_today': 0,
            'cancelled_orders_week': 0,
            'cancelled_orders_month': 0,
            'average_basket_size': 0.0,
            'repeat_customers_count': 0
        }
    finally:
        if conn:
            try:
                cursor.close()
                return_dashboard_db_connection(conn)
            except Exception as e:
                logger.warning(f"Error closing database connection: {e}")


@app.get("/api/dashboard/stats", response_model=StatsResponse)
@rate_limit("30/minute")
async def get_stats(
    request: Request,
    period: Optional[str] = Query(None, description="Период: today, week, month, quarter, year, custom"),
    start_date: Optional[str] = Query(None, description="Начальная дата для произвольного периода (ISO format)"),
    end_date: Optional[str] = Query(None, description="Конечная дата для произвольного периода (ISO format)")
):
    """
    Получение статистики для dashboard.
    
    Rate limit: 30 запросов в минуту на IP.
    Кэширование: 60 секунд (TTL настраивается через DASHBOARD_CACHE_TTL).
    
    Args:
        period: Период для анализа (today, week, month, quarter, year, custom)
        start_date: Начальная дата для произвольного периода
        end_date: Конечная дата для произвольного периода
    
    Returns:
        Статистика: выручка, количество заказов, конверсия, средний чек, топ товаров, сравнение с предыдущим периодом
    """
    # Проверка кэша
    cache_key = get_cache_key("stats", period=period, start_date=start_date, end_date=end_date)
    cached = get_from_cache(cache_key)
    if cached:
        logger.debug(f"Cache hit for stats: {cache_key}")
        return StatsResponse(**cached)
    
    try:
        # Получаем статистику (в отдельном потоке, чтобы не блокировать)
        import asyncio
        stats = await asyncio.to_thread(get_stats_from_db, period=period, start_date=start_date, end_date=end_date)
        
        # Сохранение в кэш
        set_to_cache(cache_key, stats)
        
        return StatsResponse(**stats)
    except Exception as e:
        logger.error(f"Error getting stats: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get statistics"
        )


@app.get("/api/dashboard/orders")
@rate_limit("60/minute")
async def get_orders(
    request: Request,
    status: Optional[str] = Query(None, description="Фильтр по статусу"),
    channel: Optional[str] = Query(None, description="Фильтр по каналу"),
    search: Optional[str] = Query(None, description="Поиск по номеру, ФИО, телефону"),
    sort_by: Optional[str] = Query("created_at", description="Сортировка: created_at, total_amount"),
    sort_order: Optional[str] = Query("desc", description="Порядок сортировки: asc, desc"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    page_size: int = Query(20, ge=1, le=100, description="Размер страницы")
):
    """
    Получение списка заказов с фильтрацией, поиском и пагинацией.
    
    Returns:
        Список заказов с метаданными пагинации
    """
    try:
        # Используем существующий метод из OrderService (в отдельном потоке, чтобы не блокировать)
        import asyncio
        result = await asyncio.to_thread(
            OrderService.list_orders,
            status=status,
            channel=channel,
            customer_phone=search,  # Поиск по телефону
            page=page,
            page_size=page_size
        )
        
        # Дополнительная фильтрация по поиску (номер, ФИО)
        if search:
            filtered_items = []
            search_lower = search.lower()
            for order in result['items']:
                if (search_lower in order.order_number.lower() or
                    (order.customer_name and search_lower in order.customer_name.lower()) or
                    (order.customer_phone and search_lower in order.customer_phone.lower())):
                    filtered_items.append(order)
            result['items'] = filtered_items
            result['total'] = len(filtered_items)
        
        # Сортировка
        if sort_by == "total_amount":
            result['items'].sort(
                key=lambda x: x.total_amount,
                reverse=(sort_order == "desc")
            )
        elif sort_by == "created_at":
            result['items'].sort(
                key=lambda x: x.created_at or "",
                reverse=(sort_order == "desc")
            )
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting orders: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get orders"
        )


@app.get("/api/dashboard/orders/{order_id}")
async def get_order_details(order_id: str = Path(..., description="UUID заказа")):
    """
    Получение детальной информации о заказе.
    
    Returns:
        Детальная информация о заказе
    """
    try:
        # Получаем заказ (в отдельном потоке, чтобы не блокировать)
        import asyncio
        order = await asyncio.to_thread(OrderService.get_order, order_id)
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order {order_id} not found"
            )
        return order
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting order details: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get order details"
        )


@app.patch("/api/dashboard/orders/{order_id}/status")
async def update_order_status(
    background_tasks: BackgroundTasks,
    order_id: str = Path(..., description="UUID заказа"),
    status_update: OrderStatusUpdate = ...
):
    """
    Изменение статуса заказа.
    
    При изменении статуса на "paid" автоматически (в фоне):
    - Экспортируется счет в 1С (если еще не экспортирован)
    - Генерируется трек-номер (если еще не сгенерирован)
    - Отправляется уведомление в Telegram (если есть telegram_user_id)
    
    Returns:
        Обновлённый заказ (ответ возвращается сразу, тяжелые операции выполняются в фоне)
    """
    
    try:
        # Получаем текущий статус заказа перед обновлением (в отдельном потоке, чтобы не блокировать)
        import asyncio
        current_order = await asyncio.to_thread(OrderService.get_order, order_id)
        if not current_order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order {order_id} not found"
            )
        old_status = current_order.status
        
        # Обновляем статус заказа (в отдельном потоке, чтобы не блокировать)
        updated_order = await asyncio.to_thread(
            OrderService.update_order_status,
            order_id,
            status_update.status
        )
        if not updated_order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order {order_id} not found"
            )
        
        # Инвалидация кэша статистики и аналитики при обновлении статуса заказа
        try:
            invalidate_cache("dashboard:stats:*")
            invalidate_cache("dashboard:analytics:*")
        except Exception as e:
            logger.warning(f"Failed to invalidate cache: {e}")
        
        # Последовательная фоновая обработка: 1C → tracking
        # Правильная цепочка: paid → order_created_1c → tracking_issued
        async def post_payment_pipeline():
            """
            Последовательная обработка после оплаты заказа:
            1. Экспорт в 1С (paid → order_created_1c)
            2. Генерация трек-номера (order_created_1c → tracking_issued)
            """
            current_order = None
            try:
                import asyncio
                current_order = await asyncio.to_thread(OrderService.get_order, order_id)
                if not current_order:
                    logger.warning(f"Order {order_id} not found for post-payment pipeline")
                    return

                # Шаг 1: Экспорт в 1С (если ещё не экспортировали)
                if current_order.invoice_exported_to_1c:
                    logger.info(
                        f"Invoice for order {order_id} already exported to 1C, skipping export step",
                        extra={"order_id": order_id}
                    )
                else:
                    try:
                        def _do_1c_export():
                            from src.services.onec_exporter import OneCExporter
                            return OneCExporter.export_invoice(order_id)

                        export_result = await asyncio.to_thread(_do_1c_export)
                        logger.info(
                            f"Invoice exported to 1C for order {order_id}",
                            extra={"order_id": order_id, "invoice_number": export_result.get("invoice_number")}
                        )
                        # Перечитываем заказ после экспорта
                        current_order = await asyncio.to_thread(OrderService.get_order, order_id) or current_order
                    except Exception as e:
                        logger.error(
                            f"Failed to export invoice to 1C for order {order_id}: {e}",
                            exc_info=True,
                            extra={"order_id": order_id}
                        )
                        # Уведомляем администратора
                        try:
                            from src.services.telegram_bot import send_admin_notification
                            order_num = current_order.order_number if current_order else order_id
                            await send_admin_notification(
                                f"⚠️ Ошибка экспорта в 1С\n\n"
                                f"Заказ: {order_num}\n"
                                f"Ошибка: {str(e)}\n\n"
                                f"Требуется ручной экспорт."
                            )
                        except Exception as notify_error:
                            logger.warning(f"Failed to notify admin about 1C export error: {notify_error}")
                        # Не продолжаем пайплайн если 1C экспорт провалился
                        return

                # Шаг 2: Генерация трек-номера (если ещё нет)
                if not current_order.tracking_number:
                    try:
                        from src.services.tracking_generator import TrackingGenerator
                        tracking_result = await asyncio.to_thread(TrackingGenerator.generate_and_update, order_id)
                        logger.info(
                            f"Tracking number generated for order {order_id}: {tracking_result['tracking_number']}",
                            extra={"order_id": order_id, "tracking_number": tracking_result["tracking_number"]}
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to generate tracking number for order {order_id}: {e}",
                            extra={"order_id": order_id}
                        )
            except Exception as e:
                order_num = current_order.order_number if current_order else order_id
                logger.error(
                    f"Unexpected error in post-payment pipeline for order {order_num}: {e}",
                    exc_info=True
                )

        # Автоматические действия при изменении статуса на "paid" - последовательный пайплайн
        if status_update.status == "paid" and old_status != "paid":
            background_tasks.add_task(post_payment_pipeline)
        
        # При переводе в order_created_1c — запускаем генерацию трек-номера
        if status_update.status == "order_created_1c" and old_status != "order_created_1c":
            if not updated_order.tracking_number:
                async def generate_tracking_for_1c():
                    try:
                        import asyncio
                        from src.services.tracking_generator import TrackingGenerator
                        tracking_result = await asyncio.to_thread(TrackingGenerator.generate_and_update, order_id)
                        logger.info(
                            f"Tracking number generated after 1C export for order {order_id}: {tracking_result['tracking_number']}",
                            extra={"order_id": order_id, "tracking_number": tracking_result["tracking_number"]}
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to generate tracking number after 1C export for order {order_id}: {e}",
                            extra={"order_id": order_id}
                        )

                background_tasks.add_task(generate_tracking_for_1c)
        
        # 3. Отправка уведомления в Telegram (только для заказов из Telegram, если есть telegram_user_id и статус изменился) - в фоне
        # Проверяем канал заказа, чтобы не отправлять Telegram уведомления для заказов из почты
        if (updated_order.channel == "telegram" and 
            updated_order.telegram_user_id and 
            old_status != status_update.status):
            async def send_telegram_notification_background():
                try:
                    from src.services.telegram_bot import send_status_change_notification
                    await send_status_change_notification(
                        telegram_user_id=updated_order.telegram_user_id,
                        order_number=updated_order.order_number,
                        old_status=old_status,
                        new_status=status_update.status
                    )
                except Exception as e:
                    logger.warning(f"Failed to send status change notification: {e}")
            
            background_tasks.add_task(send_telegram_notification_background)
        
        # Возвращаем ответ сразу, фоновые задачи выполнятся после ответа
        return updated_order
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating order status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update order status"
        )


@app.get("/api/dashboard/catalog")
async def get_catalog(
    q: Optional[str] = Query(None, description="Поисковый запрос"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100)
):
    """
    Получение каталога товаров с остатками.
    
    Returns:
        Список товаров с пагинацией
    """
    conn = None
    try:
        try:
            conn = get_dashboard_db_connection()
        except (TimeoutError, Exception) as e:
            logger.error(f"Failed to get dashboard database connection (pool may be exhausted): {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database temporarily unavailable"
            )
        
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection failed"
            )
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Построение запроса с поиском
        where_conditions = []
        params = []
        
        if q:
            where_conditions.append("(name ILIKE %s OR articul ILIKE %s)")
            search_pattern = f"%{q}%"
            params.extend([search_pattern, search_pattern])
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        # Подсчёт общего количества
        cursor.execute(f"""
            SELECT COUNT(*) as total
            FROM products
            WHERE {where_clause}
        """, params)
        total = cursor.fetchone()["total"]
        
        # Получение товаров с пагинацией
        offset = (page - 1) * page_size
        cursor.execute(f"""
            SELECT id, articul, name, price, stock, 
                   updated_at, synced_at
            FROM products
            WHERE {where_clause}
            ORDER BY name
            LIMIT %s OFFSET %s
        """, params + [page_size, offset])
        
        products_rows = cursor.fetchall()
        products = [
            {
                "id": str(row["id"]),
                "articul": row["articul"],
                "name": row["name"],
                "price": float(row["price"]),
                "stock": row["stock"],
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                "synced_at": row["synced_at"].isoformat() if row["synced_at"] else None
            }
            for row in products_rows
        ]
        
        pages = (total + page_size - 1) // page_size if total > 0 else 0
        
        return {
            "items": products,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages
        }
        
    except Exception as e:
        logger.error(f"Error getting catalog: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get catalog"
        )
    finally:
        if conn:
            cursor.close()
            return_dashboard_db_connection(conn)


@app.get("/api/dashboard/sync-status", response_model=SyncStatusResponse)
async def get_sync_status():
    """
    Получение статуса синхронизации с 1С.
    
    Returns:
        Статус синхронизации: последняя синхронизация, количество товаров
    """
    # Создаем синхронную функцию для выполнения в отдельном потоке
    def _get_sync_status_sync():
        conn = None
        try:
            try:
                conn = get_dashboard_db_connection()
            except (TimeoutError, Exception) as e:
                logger.error(f"Failed to get dashboard database connection (pool may be exhausted): {e}", exc_info=True)
                return SyncStatusResponse(
                    last_sync=None,
                    products_count=0,
                    status="error"
                )
            
            if not conn:
                return SyncStatusResponse(
                    last_sync=None,
                    products_count=0,
                    status="error"
                )
            
            cursor = None
            try:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # Получение последней синхронизации
                cursor.execute("""
                    SELECT MAX(synced_at) as last_sync
                    FROM products
                """)
                last_sync_row = cursor.fetchone()
                last_sync = last_sync_row['last_sync'].isoformat() if last_sync_row and last_sync_row['last_sync'] else None
                
                # Количество товаров
                cursor.execute("SELECT COUNT(*) as count FROM products")
                count_row = cursor.fetchone()
                products_count = count_row['count'] if count_row else 0
                
                # Определение статуса
                if products_count == 0:
                    # Нет товаров - это ошибка
                    sync_status = "error"
                elif last_sync:
                    # Есть товары и есть информация о синхронизации
                    last_sync_dt = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
                    hours_ago = (datetime.now(timezone.utc) - last_sync_dt).total_seconds() / 3600
                    if hours_ago < 2:
                        sync_status = "ok"
                    elif hours_ago < 24:
                        sync_status = "warning"
                    else:
                        # Даже если синхронизация была давно, но товары есть - это warning, не error
                        sync_status = "warning"
                else:
                    # Есть товары, но нет информации о последней синхронизации
                    # Это может быть нормально (первая синхронизация), поэтому warning
                    sync_status = "warning"
                
                return SyncStatusResponse(
                    last_sync=last_sync,
                    products_count=products_count,
                    status=sync_status
                )
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except Exception as e:
                        logger.warning(f"Error closing cursor: {e}")
            
        except Exception as e:
            logger.error(f"Error getting sync status: {e}", exc_info=True)
            return SyncStatusResponse(
                last_sync=None,
                products_count=0,
                status="error"
            )
        finally:
            if conn:
                try:
                    if 'cursor' in locals() and cursor:
                        cursor.close()
                except Exception as e:
                    logger.warning(f"Error closing cursor: {e}")
                return_dashboard_db_connection(conn)
    
    # Выполняем синхронную функцию в отдельном потоке
    import asyncio
    return await asyncio.to_thread(_get_sync_status_sync)


def get_analytics_from_db(days: int = 30) -> Dict[str, Any]:
    """
    Получение детальной аналитики из БД.
    
    Args:
        days: Количество дней для анализа (по умолчанию 30)
        
    Returns:
        Словарь с детальной аналитикой
    """
    conn = None
    try:
        try:
            conn = get_dashboard_db_connection()
        except (TimeoutError, Exception) as e:
            logger.error(f"Failed to get dashboard database connection (pool may be exhausted): {e}", exc_info=True)
            # Возвращаем пустую аналитику вместо падения
            return {
                'revenue_by_days': [],
                'channel_analysis': {},
                'sales_funnel': {
                    'new': 0,
                    'validated': 0,
                    'invoice_created': 0,
                    'paid': 0,
                    'shipped': 0,
                    'cancelled': 0
                },
                'status_distribution': {
                    'new': 0,
                    'validated': 0,
                    'invoice_created': 0,
                    'paid': 0,
                    'shipped': 0,
                    'cancelled': 0
                },
                'metrics': {
                    'avg_processing_hours': 0.0,
                    'avg_delivery_hours': 0.0,
                    'avg_delivery_cost': 0.0,
                    'orders_with_delivery': 0,
                    'top_cities': []
                }
            }
        
        if not conn:
            # Возвращаем пустую аналитику вместо падения
            return {
                'revenue_by_days': [],
                'channel_analysis': {},
                'sales_funnel': {
                    'new': 0,
                    'validated': 0,
                    'invoice_created': 0,
                    'paid': 0,
                    'shipped': 0,
                    'cancelled': 0
                },
                'status_distribution': {
                    'new': 0,
                    'validated': 0,
                    'invoice_created': 0,
                    'paid': 0,
                    'shipped': 0,
                    'cancelled': 0
                },
                'metrics': {
                    'avg_processing_hours': 0.0,
                    'avg_delivery_hours': 0.0,
                    'avg_delivery_cost': 0.0,
                    'orders_with_delivery': 0,
                    'top_cities': []
                }
            }
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        now = datetime.now(timezone.utc)
        period_start = now - timedelta(days=days)
        
        analytics = {}
        
        # 1. Динамика выручки по дням
        cursor.execute("""
            SELECT 
                DATE(created_at AT TIME ZONE 'UTC') as date,
                COUNT(*) as orders_count,
                COALESCE(SUM(total_amount), 0) as revenue
            FROM orders
            WHERE created_at >= %s AND status != 'cancelled'
            GROUP BY DATE(created_at AT TIME ZONE 'UTC')
            ORDER BY date ASC
        """, (period_start,))
        revenue_by_days = cursor.fetchall()
        analytics['revenue_by_days'] = [
            {
                "date": row['date'].isoformat() if row['date'] else None,
                "orders_count": row['orders_count'],
                "revenue": float(row['revenue'])
            }
            for row in revenue_by_days
        ]
        
        # 2. Анализ по каналам
        cursor.execute("""
            SELECT 
                channel,
                COUNT(*) as orders_count,
                COALESCE(SUM(total_amount), 0) as revenue,
                COALESCE(AVG(total_amount), 0) as avg_check,
                COUNT(*) FILTER (WHERE status = 'paid') as paid_orders,
                COUNT(*) FILTER (WHERE status = 'new') as new_orders
            FROM orders
            WHERE created_at >= %s AND status != 'cancelled'
            GROUP BY channel
        """, (period_start,))
        channel_data = cursor.fetchall()
        channel_analysis = {}
        for row in channel_data:
            total_orders = row['orders_count'] or 0
            paid_orders = row['paid_orders'] or 0
            conversion = (paid_orders / total_orders * 100) if total_orders > 0 else 0.0
            
            channel_analysis[row['channel']] = {
                "orders_count": total_orders,
                "revenue": float(row['revenue']),
                "avg_check": float(row['avg_check']),
                "paid_orders": paid_orders,
                "new_orders": row['new_orders'] or 0,
                "conversion_rate": round(conversion, 2)
            }
        analytics['channel_analysis'] = channel_analysis
        
        # 3. Воронка продаж
        cursor.execute("""
            SELECT 
                status,
                COUNT(*) as count
            FROM orders
            WHERE created_at >= %s
            GROUP BY status
        """, (period_start,))
        funnel_data = cursor.fetchall()
        sales_funnel = {}
        for row in funnel_data:
            sales_funnel[row['status']] = row['count'] or 0
        analytics['sales_funnel'] = sales_funnel
        
        # 4. Распределение по статусам (текущее состояние всех заказов)
        cursor.execute("""
            SELECT 
                status,
                COUNT(*) as count
            FROM orders
            GROUP BY status
        """)
        status_data = cursor.fetchall()
        status_distribution = {}
        for row in status_data:
            status_distribution[row['status']] = row['count'] or 0
        analytics['status_distribution'] = status_distribution
        
        # 5. Дополнительные метрики
        # Среднее время обработки заказа (от создания до оплаты)
        cursor.execute("""
            SELECT 
                AVG(EXTRACT(EPOCH FROM (paid_at - created_at)) / 3600) as avg_processing_hours
            FROM orders
            WHERE status = 'paid' AND paid_at IS NOT NULL AND created_at IS NOT NULL
            AND created_at >= %s
        """, (period_start,))
        avg_processing = cursor.fetchone()
        avg_processing_hours = float(avg_processing['avg_processing_hours']) if avg_processing and avg_processing['avg_processing_hours'] else 0.0
        
        # Среднее время доставки (от оплаты до отправки)
        cursor.execute("""
            SELECT 
                AVG(EXTRACT(EPOCH FROM (shipped_at - paid_at)) / 3600) as avg_delivery_hours
            FROM orders
            WHERE status = 'shipped' AND shipped_at IS NOT NULL AND paid_at IS NOT NULL
            AND created_at >= %s
        """, (period_start,))
        avg_delivery = cursor.fetchone()
        avg_delivery_hours = float(avg_delivery['avg_delivery_hours']) if avg_delivery and avg_delivery['avg_delivery_hours'] else 0.0
        
        # Средняя стоимость доставки
        cursor.execute("""
            SELECT 
                AVG(delivery_cost) as avg_delivery_cost,
                COUNT(*) as orders_with_delivery
            FROM orders
            WHERE created_at >= %s AND delivery_cost > 0
        """, (period_start,))
        delivery_data = cursor.fetchone()
        avg_delivery_cost = float(delivery_data['avg_delivery_cost']) if delivery_data and delivery_data['avg_delivery_cost'] else 0.0
        orders_with_delivery = delivery_data['orders_with_delivery'] if delivery_data else 0
        
        # Топ городов по количеству заказов
        # Используем подзапрос для упрощения GROUP BY
        cursor.execute("""
            WITH city_data AS (
                SELECT 
                    CASE 
                        WHEN customer_address ILIKE '%%Москва%%' THEN 'Москва'
                        WHEN customer_address ILIKE '%%Санкт-Петербург%%' OR customer_address ILIKE '%%СПб%%' THEN 'Санкт-Петербург'
                        WHEN customer_address ILIKE '%%Новосибирск%%' THEN 'Новосибирск'
                        WHEN customer_address ILIKE '%%Екатеринбург%%' THEN 'Екатеринбург'
                        WHEN customer_address ILIKE '%%Казань%%' THEN 'Казань'
                        ELSE 'Другие'
                    END as city
                FROM orders
                WHERE created_at >= %s AND customer_address IS NOT NULL AND customer_address != ''
            )
            SELECT 
                city,
                COUNT(*) as orders_count
            FROM city_data
            GROUP BY city
            ORDER BY orders_count DESC
            LIMIT 10
        """, (period_start,))
        top_cities = cursor.fetchall()
        
        analytics['metrics'] = {
            "avg_processing_hours": round(avg_processing_hours, 2),
            "avg_delivery_hours": round(avg_delivery_hours, 2),
            "avg_delivery_cost": round(avg_delivery_cost, 2),
            "orders_with_delivery": orders_with_delivery,
            "top_cities": [
                {
                    "city": row['city'],
                    "orders_count": row['orders_count']
                }
                for row in top_cities
            ]
        }
        
        return analytics
        
    except Exception as e:
        logger.error(f"Error getting analytics: {e}", exc_info=True)
        # Возвращаем пустую аналитику вместо проброса ошибки
        return {
            'revenue_by_days': [],
            'channel_analysis': {},
            'sales_funnel': {
                'new': 0,
                'validated': 0,
                'invoice_created': 0,
                'paid': 0,
                'shipped': 0,
                'cancelled': 0
            },
            'status_distribution': {
                'new': 0,
                'validated': 0,
                'invoice_created': 0,
                'paid': 0,
                'shipped': 0,
                'cancelled': 0
            },
            'metrics': {
                'avg_processing_hours': 0.0,
                'avg_delivery_hours': 0.0,
                'avg_delivery_cost': 0.0,
                'orders_with_delivery': 0,
                'top_cities': []
            }
        }
    finally:
        if conn:
            try:
                cursor.close()
                return_dashboard_db_connection(conn)
            except Exception as e:
                logger.warning(f"Error closing database connection: {e}")


@app.get("/api/dashboard/analytics", response_model=AnalyticsResponse)
@rate_limit("20/minute")
async def get_analytics(
    request: Request,
    days: int = Query(30, ge=1, le=365, description="Количество дней для анализа")
):
    """
    Получение детальной аналитики для dashboard.
    
    Rate limit: 20 запросов в минуту на IP.
    Кэширование: 60 секунд (TTL настраивается через DASHBOARD_CACHE_TTL).
    
    Returns:
        Детальная аналитика: динамика выручки, анализ по каналам, воронка продаж, метрики
    """
    # Проверка кэша
    cache_key = get_cache_key("analytics", days=days)
    cached = get_from_cache(cache_key)
    if cached:
        logger.debug(f"Cache hit for analytics: {cache_key}")
        return AnalyticsResponse(**cached)
    
    try:
        # Получаем аналитику (в отдельном потоке, чтобы не блокировать)
        import asyncio
        analytics = await asyncio.to_thread(get_analytics_from_db, days=days)
        
        # Сохранение в кэш
        set_to_cache(cache_key, analytics)
        
        return AnalyticsResponse(**analytics)
    except Exception as e:
        logger.error(f"Error getting analytics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get analytics"
        )


@app.get("/api/dashboard/export/orders/excel")
async def export_orders_excel(
    status_filter: Optional[str] = Query(None, description="Фильтр по статусу"),
    channel_filter: Optional[str] = Query(None, description="Фильтр по каналу"),
    search: Optional[str] = Query(None, description="Поисковый запрос")
):
    """Экспорт заказов в Excel."""
    try:
        # Получение заказов с фильтрами (в отдельном потоке, чтобы не блокировать)
        import asyncio
        orders_data = await asyncio.to_thread(
            OrderService.list_orders,
            status=status_filter,
            channel=channel_filter,
            customer_phone=search,  # Поиск по телефону
            page=1,
            page_size=10000  # Большое количество для экспорта
        )
        orders = orders_data['items']
        
        # Дополнительная фильтрация по поиску (номер, ФИО)
        if search:
            filtered_items = []
            search_lower = search.lower()
            for order in orders:
                if (search_lower in order.order_number.lower() or
                    (order.customer_name and search_lower in order.customer_name.lower()) or
                    (order.customer_phone and search_lower in order.customer_phone.lower())):
                    filtered_items.append(order)
            orders = filtered_items
        
        if not orders:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No orders found for export"
            )
        
        # Экспорт в Excel
        # Экспорт в Excel (в отдельном потоке, чтобы не блокировать)
        import asyncio
        filepath = await asyncio.to_thread(DataExporter.export_orders_to_excel, orders)
        filename = PathLib(filepath).name
        
        # Проверка, что файл существует и имеет правильное расширение
        if not PathLib(filepath).exists():
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Export file was not created"
            )
        
        # Возврат файла с правильными заголовками
        return FileResponse(
            path=filepath,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"; filename*=UTF-8\'\'{filename}'
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting orders to Excel: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to export orders to Excel"
        )


@app.get("/api/dashboard/export/orders/csv")
async def export_orders_csv(
    status_filter: Optional[str] = Query(None, description="Фильтр по статусу"),
    channel_filter: Optional[str] = Query(None, description="Фильтр по каналу"),
    search: Optional[str] = Query(None, description="Поисковый запрос")
):
    """Экспорт заказов в CSV."""
    try:
        # Получение заказов с фильтрами (в отдельном потоке, чтобы не блокировать)
        import asyncio
        orders_data = await asyncio.to_thread(
            OrderService.list_orders,
            status=status_filter,
            channel=channel_filter,
            customer_phone=search,  # Поиск по телефону
            page=1,
            page_size=10000  # Большое количество для экспорта
        )
        orders = orders_data['items']
        
        # Дополнительная фильтрация по поиску (номер, ФИО)
        if search:
            filtered_items = []
            search_lower = search.lower()
            for order in orders:
                if (search_lower in order.order_number.lower() or
                    (order.customer_name and search_lower in order.customer_name.lower()) or
                    (order.customer_phone and search_lower in order.customer_phone.lower())):
                    filtered_items.append(order)
            orders = filtered_items
        
        if not orders:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No orders found for export"
            )
        
        # Экспорт в CSV
        # Экспорт в CSV (в отдельном потоке, чтобы не блокировать)
        import asyncio
        filepath = await asyncio.to_thread(DataExporter.export_orders_to_csv, orders)
        filename = PathLib(filepath).name
        
        # Проверка, что файл существует и имеет правильное расширение
        if not PathLib(filepath).exists():
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Export file was not created"
            )
        
        # Возврат файла с правильными заголовками
        return FileResponse(
            path=filepath,
            filename=filename,
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"; filename*=UTF-8\'\'{filename}'
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting orders to CSV: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to export orders to CSV"
        )


@app.get("/api/dashboard/export/stats/pdf")
async def export_stats_pdf(
    period: Optional[str] = Query("month", description="Период: day, week, month")
):
    """Экспорт статистики в PDF."""
    try:
        # Получение статистики
        # Получаем статистику (в отдельном потоке, чтобы не блокировать)
        import asyncio
        stats = await asyncio.to_thread(get_stats_from_db, period=period)
        
        # Экспорт в PDF (в отдельном потоке, чтобы не блокировать)
        filepath = await asyncio.to_thread(DataExporter.export_stats_to_pdf, stats)
        filename = PathLib(filepath).name
        
        # Проверка, что файл существует и имеет правильное расширение
        if not PathLib(filepath).exists():
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Export file was not created"
            )
        
        # Возврат файла с правильными заголовками
        return FileResponse(
            path=filepath,
            filename=filename,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"; filename*=UTF-8\'\'{filename}'
            }
        )
    except Exception as e:
        logger.error(f"Error exporting stats to PDF: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to export statistics to PDF"
        )


@app.get("/api/dashboard/export/analytics/pdf")
async def export_analytics_pdf(
    days: int = Query(30, ge=1, le=365, description="Количество дней для анализа")
):
    """Экспорт аналитики в PDF."""
    try:
        # Получение аналитики
        # Получаем аналитику (в отдельном потоке, чтобы не блокировать)
        import asyncio
        analytics = await asyncio.to_thread(get_analytics_from_db, days=days)
        
        # Экспорт в PDF (в отдельном потоке, чтобы не блокировать)
        filepath = await asyncio.to_thread(DataExporter.export_analytics_to_pdf, analytics)
        filename = PathLib(filepath).name
        
        # Проверка, что файл существует
        if not PathLib(filepath).exists():
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Export file was not created"
            )
        
        # Возврат файла с правильными заголовками
        return FileResponse(
            path=filepath,
            filename=filename,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"; filename*=UTF-8\'\'{filename}'
            }
        )
    except Exception as e:
        logger.error(f"Error exporting analytics to PDF: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to export analytics to PDF"
        )


@app.get("/api/dashboard/export/catalog/excel")
async def export_catalog_excel(
    q: Optional[str] = Query(None, description="Поисковый запрос")
):
    """Экспорт каталога в Excel."""
    try:
        # Получение всех товаров (без пагинации для экспорта)
        conn = None
        try:
            try:
                conn = get_dashboard_db_connection()
            except (TimeoutError, Exception) as e:
                logger.error(f"Failed to get dashboard database connection (pool may be exhausted): {e}", exc_info=True)
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Database temporarily unavailable"
                )
            
            if not conn:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Database connection failed"
                )
            
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Построение запроса с поиском
            where_conditions = []
            params = []
            
            if q:
                where_conditions.append("(name ILIKE %s OR articul ILIKE %s)")
                search_pattern = f"%{q}%"
                params.extend([search_pattern, search_pattern])
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            # Получение всех товаров
            cursor.execute(f"""
                SELECT id, articul, name, price, stock, 
                       updated_at, synced_at
                FROM products
                WHERE {where_clause}
                ORDER BY name
            """, params)
            
            products_rows = cursor.fetchall()
            products = [
                {
                    "id": str(row["id"]),
                    "articul": row["articul"],
                    "name": row["name"],
                    "price": float(row["price"]),
                    "stock": row["stock"],
                    "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                    "synced_at": row["synced_at"].isoformat() if row["synced_at"] else None
                }
                for row in products_rows
            ]
            
        finally:
            if conn:
                cursor.close()
                return_dashboard_db_connection(conn)
        
        if not products:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No products found for export"
            )
        
        # Экспорт в Excel
        # Экспорт в Excel (в отдельном потоке, чтобы не блокировать)
        import asyncio
        filepath = await asyncio.to_thread(DataExporter.export_catalog_to_excel, products)
        filename = PathLib(filepath).name
        
        # Проверка, что файл существует
        if not PathLib(filepath).exists():
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Export file was not created"
            )
        
        # Возврат файла с правильными заголовками
        return FileResponse(
            path=filepath,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"; filename*=UTF-8\'\'{filename}'
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting catalog to Excel: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to export catalog to Excel"
        )


@app.get("/api/dashboard/export/catalog/csv")
async def export_catalog_csv(
    q: Optional[str] = Query(None, description="Поисковый запрос")
):
    """Экспорт каталога в CSV."""
    try:
        # Получение всех товаров (без пагинации для экспорта)
        conn = None
        try:
            try:
                conn = get_dashboard_db_connection()
            except (TimeoutError, Exception) as e:
                logger.error(f"Failed to get dashboard database connection (pool may be exhausted): {e}", exc_info=True)
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Database temporarily unavailable"
                )
            
            if not conn:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Database connection failed"
                )
            
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Построение запроса с поиском
            where_conditions = []
            params = []
            
            if q:
                where_conditions.append("(name ILIKE %s OR articul ILIKE %s)")
                search_pattern = f"%{q}%"
                params.extend([search_pattern, search_pattern])
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            # Получение всех товаров
            cursor.execute(f"""
                SELECT id, articul, name, price, stock, 
                       updated_at, synced_at
                FROM products
                WHERE {where_clause}
                ORDER BY name
            """, params)
            
            products_rows = cursor.fetchall()
            products = [
                {
                    "id": str(row["id"]),
                    "articul": row["articul"],
                    "name": row["name"],
                    "price": float(row["price"]),
                    "stock": row["stock"],
                    "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                    "synced_at": row["synced_at"].isoformat() if row["synced_at"] else None
                }
                for row in products_rows
            ]
            
        finally:
            if conn:
                cursor.close()
                return_dashboard_db_connection(conn)
        
        if not products:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No products found for export"
            )
        
        # Экспорт в CSV
        # Экспорт в CSV (в отдельном потоке, чтобы не блокировать)
        import asyncio
        filepath = await asyncio.to_thread(DataExporter.export_catalog_to_csv, products)
        filename = PathLib(filepath).name
        
        # Проверка, что файл существует
        if not PathLib(filepath).exists():
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Export file was not created"
            )
        
        # Возврат файла с правильными заголовками
        return FileResponse(
            path=filepath,
            filename=filename,
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"; filename*=UTF-8\'\'{filename}'
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting catalog to CSV: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to export catalog to CSV"
        )


@app.get(
    "/health/live",
    summary="Liveness Probe",
    description="Проверка что сервис жив (для Kubernetes/мониторинга)",
    responses={
        200: {"description": "Сервис жив"}
    }
)
async def liveness_check():
    """
    Liveness probe - проверка что сервис жив.
    
    Всегда возвращает 200, если сервис запущен.
    Не проверяет зависимости, чтобы не блокировать при перегрузке.
    """
    return {
        "status": "ok",
        "service": "dashboard_api",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get(
    "/health/ready",
    summary="Readiness Probe",
    description="Проверка готовности сервиса к обработке запросов",
    responses={
        200: {"description": "Сервис готов"},
        503: {"description": "Сервис не готов"}
    }
)
async def readiness_check():
    """
    Readiness probe - проверка готовности сервиса.
    
    Проверяет что пул БД инициализирован, но не блокирует на получение соединения.
    """
    try:
        if dashboard_db_pool is None:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "not_ready",
                    "service": "dashboard_api",
                    "reason": "Database pool not initialized"
                }
            )
        
        # Легкая проверка без получения соединения
        # Просто проверяем что пул существует и имеет соединения
        pool_info = {
            "minconn": dashboard_db_pool.minconn,
            "maxconn": dashboard_db_pool.maxconn,
            "circuit_breaker": {
                "available": dashboard_db_available,
                "failures": dashboard_db_failures,
                "max_failures": DASHBOARD_DB_MAX_FAILURES
            }
        }
        
        # Проверяем circuit breaker
        if not dashboard_db_available:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "not_ready",
                    "service": "dashboard_api",
                    "reason": "Database circuit breaker is open",
                    "database_pool": pool_info
                }
            )
        
        return {
            "status": "ready",
            "service": "dashboard_api",
            "database_pool": pool_info,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "service": "dashboard_api",
                "error": str(e)
            }
        )


@app.get(
    "/health",
    summary="Health Check",
    description="Проверка состояния сервиса (legacy endpoint, используйте /health/live или /health/ready)",
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
    Health check endpoint (legacy).
    
    Быстрая проверка без блокирующих операций.
    Не проверяет БД, чтобы не блокировать при перегрузке.
    """
    # Просто возвращаем статус без проверки БД
    # Это гарантирует быстрый ответ даже при перегрузке
    return {
        "status": "ok",
        "service": "dashboard_api",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


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

    # Раздельные логи uvicorn (access/error) в файлы logs/dashboard_api.uvicorn.*.log
    try:
        from src.utils.logger import setup_uvicorn_logging
        setup_uvicorn_logging("dashboard_api")
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
            host=DASHBOARD_HOST,
            port=DASHBOARD_PORT,
            log_level="info",
            log_config=None,
            # Ограничиваем количество одновременных соединений для предотвращения перегрузки
            limit_concurrency=100,  # Максимум 100 одновременных соединений
            timeout_keep_alive=5  # Таймаут keep-alive соединений
        )
    except KeyboardInterrupt:
        logger.info("Dashboard API stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in Dashboard API: {e}", exc_info=True)
        sys.exit(1)
