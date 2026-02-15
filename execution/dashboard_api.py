#!/usr/bin/env python3
"""
Dashboard API - FastAPI сервер для веб-интерфейса менеджера.

Предоставляет REST API endpoints для просмотра статистики, управления заказами и мониторинга системы.
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
from pathlib import Path as PathLib

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Path, status, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# Импорты с поддержкой как относительных, так и абсолютных
try:
    from .crm_service import OrderService, Order, OrderItem
    from .data_exporter import DataExporter
except ImportError:
    import sys
    from pathlib import Path as PathLib
    project_root = PathLib(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from execution.crm_service import OrderService, Order, OrderItem
    from execution.data_exporter import DataExporter

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/dashboard_api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Переменные окружения
DASHBOARD_PORT = int(os.getenv('DASHBOARD_PORT', '8028'))
DASHBOARD_HOST = os.getenv('DASHBOARD_HOST', '0.0.0.0')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/smartorder')

# Импорт для работы с БД
import psycopg2
from psycopg2.extras import RealDictCursor
from execution.crm_service import get_db_connection, return_db_connection


# Pydantic модели
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


# Lifespan events для FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events для FastAPI."""
    # Startup
    logger.info("Dashboard API server starting")
    yield
    # Shutdown
    logger.info("Dashboard API server stopped")


# Инициализация FastAPI с lifespan
app = FastAPI(
    title="SmartOrder Engine - Dashboard API",
    description="API для веб-интерфейса менеджера. Статистика, аналитика, управление заказами и экспорт данных.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware для работы с frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене указать конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Статические файлы для frontend
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
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        now = datetime.now(timezone.utc)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
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
        cursor.execute("""
            SELECT 
                COUNT(*) as orders_count,
                COALESCE(SUM(total_amount), 0) as revenue
            FROM orders
            WHERE created_at >= %s AND status != 'cancelled'
        """, (today_start,))
        today = cursor.fetchone()
        stats['revenue_today'] = float(today['revenue']) if today else 0.0
        stats['orders_today'] = today['orders_count'] if today else 0
        
        # Неделя
        cursor.execute("""
            SELECT 
                COUNT(*) as orders_count,
                COALESCE(SUM(total_amount), 0) as revenue
            FROM orders
            WHERE created_at >= %s AND status != 'cancelled'
        """, (week_start,))
        week = cursor.fetchone()
        stats['revenue_week'] = float(week['revenue']) if week else 0.0
        stats['orders_week'] = week['orders_count'] if week else 0
        
        # Месяц
        cursor.execute("""
            SELECT 
                COUNT(*) as orders_count,
                COALESCE(SUM(total_amount), 0) as revenue
            FROM orders
            WHERE created_at >= %s AND status != 'cancelled'
        """, (month_start,))
        month = cursor.fetchone()
        stats['revenue_month'] = float(month['revenue']) if month else 0.0
        stats['orders_month'] = month['orders_count'] if month else 0
        
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
        
        # Дополнительные метрики: новые заказы
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM orders
            WHERE created_at >= %s AND status = 'new'
        """, (today_start,))
        stats['new_orders_today'] = cursor.fetchone()['count'] or 0
        
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM orders
            WHERE created_at >= %s AND status = 'new'
        """, (week_start,))
        stats['new_orders_week'] = cursor.fetchone()['count'] or 0
        
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM orders
            WHERE created_at >= %s AND status = 'new'
        """, (month_start,))
        stats['new_orders_month'] = cursor.fetchone()['count'] or 0
        
        # Дополнительные метрики: оплаченные заказы
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM orders
            WHERE created_at >= %s AND status = 'paid'
        """, (today_start,))
        stats['paid_orders_today'] = cursor.fetchone()['count'] or 0
        
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM orders
            WHERE created_at >= %s AND status = 'paid'
        """, (week_start,))
        stats['paid_orders_week'] = cursor.fetchone()['count'] or 0
        
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM orders
            WHERE created_at >= %s AND status = 'paid'
        """, (month_start,))
        stats['paid_orders_month'] = cursor.fetchone()['count'] or 0
        
        # Дополнительные метрики: отмененные заказы
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM orders
            WHERE created_at >= %s AND status = 'cancelled'
        """, (today_start,))
        stats['cancelled_orders_today'] = cursor.fetchone()['count'] or 0
        
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM orders
            WHERE created_at >= %s AND status = 'cancelled'
        """, (week_start,))
        stats['cancelled_orders_week'] = cursor.fetchone()['count'] or 0
        
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM orders
            WHERE created_at >= %s AND status = 'cancelled'
        """, (month_start,))
        stats['cancelled_orders_month'] = cursor.fetchone()['count'] or 0
        
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
                DATE(created_at AT TIME ZONE 'UTC') as date,
                COALESCE(SUM(total_amount), 0) as daily_revenue
            FROM orders
            WHERE created_at >= %s 
                AND created_at < %s
                AND status != 'cancelled'
            GROUP BY DATE(created_at AT TIME ZONE 'UTC')
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
        raise
    finally:
        if conn:
            cursor.close()
            return_db_connection(conn)


@app.get("/api/dashboard/stats", response_model=StatsResponse)
async def get_stats(
    period: Optional[str] = Query(None, description="Период: today, week, month, quarter, year, custom"),
    start_date: Optional[str] = Query(None, description="Начальная дата для произвольного периода (ISO format)"),
    end_date: Optional[str] = Query(None, description="Конечная дата для произвольного периода (ISO format)")
):
    """
    Получение статистики для dashboard.
    
    Args:
        period: Период для анализа (today, week, month, quarter, year, custom)
        start_date: Начальная дата для произвольного периода
        end_date: Конечная дата для произвольного периода
    
    Returns:
        Статистика: выручка, количество заказов, конверсия, средний чек, топ товаров, сравнение с предыдущим периодом
    """
    try:
        stats = get_stats_from_db(period=period, start_date=start_date, end_date=end_date)
        return StatsResponse(**stats)
    except Exception as e:
        logger.error(f"Error getting stats: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get statistics"
        )


@app.get("/api/dashboard/orders")
async def get_orders(
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
        # Используем существующий метод из OrderService
        result = OrderService.list_orders(
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
        order = OrderService.get_order(order_id)
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
    order_id: str = Path(..., description="UUID заказа"),
    status_update: OrderStatusUpdate = ...
):
    """
    Изменение статуса заказа.
    
    Returns:
        Обновлённый заказ
    """
    try:
        updated_order = OrderService.update_order_status(
            order_id,
            status_update.status
        )
        if not updated_order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order {order_id} not found"
            )
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
        conn = get_db_connection()
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
            return_db_connection(conn)


@app.get("/api/dashboard/sync-status", response_model=SyncStatusResponse)
async def get_sync_status():
    """
    Получение статуса синхронизации с 1С.
    
    Returns:
        Статус синхронизации: последняя синхронизация, количество товаров
    """
    conn = None
    try:
        conn = get_db_connection()
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
        
    except Exception as e:
        logger.error(f"Error getting sync status: {e}", exc_info=True)
        return SyncStatusResponse(
            last_sync=None,
            products_count=0,
            status="error"
        )
    finally:
        if conn:
            cursor.close()
            return_db_connection(conn)


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
        conn = get_db_connection()
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
        raise
    finally:
        if conn:
            cursor.close()
            return_db_connection(conn)


@app.get("/api/dashboard/analytics", response_model=AnalyticsResponse)
async def get_analytics(
    days: int = Query(30, ge=1, le=365, description="Количество дней для анализа")
):
    """
    Получение детальной аналитики для dashboard.
    
    Returns:
        Детальная аналитика: динамика выручки, анализ по каналам, воронка продаж, метрики
    """
    try:
        analytics = get_analytics_from_db(days=days)
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
        # Получение заказов с фильтрами (используем ту же логику, что и в get_orders)
        orders_data = OrderService.list_orders(
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
        filepath = DataExporter.export_orders_to_excel(orders)
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
        # Получение заказов с фильтрами (используем ту же логику, что и в get_orders)
        orders_data = OrderService.list_orders(
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
        filepath = DataExporter.export_orders_to_csv(orders)
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
        stats = get_stats_from_db(period=period)
        
        # Экспорт в PDF
        filepath = DataExporter.export_stats_to_pdf(stats)
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
        analytics = get_analytics_from_db(days=days)
        
        # Экспорт в PDF
        filepath = DataExporter.export_analytics_to_pdf(analytics)
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
            conn = get_db_connection()
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
                return_db_connection(conn)
        
        if not products:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No products found for export"
            )
        
        # Экспорт в Excel
        filepath = DataExporter.export_catalog_to_excel(products)
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
            conn = get_db_connection()
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
                return_db_connection(conn)
        
        if not products:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No products found for export"
            )
        
        # Экспорт в CSV
        filepath = DataExporter.export_catalog_to_csv(products)
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
        "service": "dashboard_api"
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
    
    # Создание директории для логов если её нет
    os.makedirs("logs", exist_ok=True)
    
    uvicorn.run(
        app,
        host=DASHBOARD_HOST,
        port=DASHBOARD_PORT,
        log_level="info"
    )
