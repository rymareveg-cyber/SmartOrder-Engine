#!/usr/bin/env python3
"""
FastAPI сервер для доступа к каталогу товаров.

Предоставляет REST API endpoints для работы с каталогом товаров из PostgreSQL.
"""

import os
import logging
from typing import Optional, List
from decimal import Decimal
from contextlib import asynccontextmanager

from dotenv import load_dotenv
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import redis
from functools import lru_cache

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Переменные окружения
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/smartorder')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
CACHE_TTL = int(os.getenv('CACHE_TTL', '300'))  # 5 минут

# Connection pools
db_pool: Optional[SimpleConnectionPool] = None
redis_client: Optional[redis.Redis] = None


def init_db_pool():
    """Инициализация connection pool для PostgreSQL."""
    global db_pool
    try:
        db_pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=DATABASE_URL
        )
        logger.info("Database connection pool initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        raise


def init_redis():
    """Инициализация Redis клиента."""
    global redis_client
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        logger.info("Redis client initialized")
    except Exception as e:
        logger.warning(f"Failed to initialize Redis: {e}")
        redis_client = None


def get_db_connection():
    """Получить соединение с БД из pool."""
    if db_pool is None:
        init_db_pool()
    return db_pool.getconn()


def return_db_connection(conn):
    """Вернуть соединение в pool."""
    if db_pool:
        db_pool.putconn(conn)


# Pydantic модели
class Product(BaseModel):
    id: str
    articul: str
    name: str
    price: float
    stock: int
    updated_at: str
    synced_at: str

    class Config:
        from_attributes = True


class ProductListResponse(BaseModel):
    items: List[Product]
    total: int
    page: int
    page_size: int
    pages: int


# Lifespan events для FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events для FastAPI."""
    # Startup
    init_db_pool()
    init_redis()
    yield
    # Shutdown
    if db_pool:
        db_pool.closeall()
    if redis_client:
        redis_client.close()


# Инициализация FastAPI с lifespan
app = FastAPI(
    title="SmartOrder Engine - API",
    description="API для доступа к каталогу товаров и работы с заказами. Поддерживает поиск, фильтрацию, кэширование через Redis и управление заказами.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Подключение Orders API
try:
    # Импортируем router из api_orders
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    # Импортируем router из api_orders
    from execution.api_orders import router as orders_router
    # Включаем все роуты из api_orders в основной app
    app.include_router(orders_router)
    logger.info("Orders API routes included successfully")
except Exception as e:
    logger.warning(f"Could not import orders API: {e}. Orders endpoints will not be available.")


def get_cache_key(endpoint: str, **params) -> str:
    """Генерация ключа кэша."""
    param_str = "_".join(f"{k}_{v}" for k, v in sorted(params.items()))
    return f"catalog:{endpoint}:{param_str}"


def get_from_cache(key: str) -> Optional[dict]:
    """Получить данные из кэша."""
    if not redis_client:
        return None
    try:
        cached = redis_client.get(key)
        if cached:
            import json
            return json.loads(cached)
    except Exception as e:
        logger.warning(f"Cache get error: {e}")
    return None


def set_to_cache(key: str, value: dict, ttl: int = CACHE_TTL):
    """Сохранить данные в кэш."""
    if not redis_client:
        return
    try:
        import json
        redis_client.setex(key, ttl, json.dumps(value))
    except Exception as e:
        logger.warning(f"Cache set error: {e}")


@app.get(
    "/api/catalog",
    response_model=ProductListResponse,
    summary="Получить список товаров",
    description="Возвращает список всех товаров из каталога с поддержкой пагинации и фильтрации",
    responses={
        200: {
            "description": "Список товаров успешно получен",
            "content": {
                "application/json": {
                    "example": {
                        "items": [
                            {
                                "id": "123e4567-e89b-12d3-a456-426614174000",
                                "articul": "ФР-00000044",
                                "name": "Варочная панель",
                                "price": 122334.0,
                                "stock": 1,
                                "updated_at": "2026-02-13T10:00:00",
                                "synced_at": "2026-02-13T10:00:00"
                            }
                        ],
                        "total": 143,
                        "page": 1,
                        "page_size": 20,
                        "pages": 8
                    }
                }
            }
        },
        500: {"description": "Внутренняя ошибка сервера"}
    }
)
async def get_catalog(
    page: int = Query(1, ge=1, description="Номер страницы (начиная с 1)"),
    page_size: int = Query(20, ge=1, le=100, description="Количество товаров на странице (максимум 100)"),
    min_stock: Optional[int] = Query(None, ge=0, description="Минимальный остаток товара на складе"),
    max_price: Optional[float] = Query(None, ge=0, description="Максимальная цена товара в рублях")
):
    """
    Получить список всех товаров из каталога.
    
    Поддерживает:
    - Пагинацию (page, page_size)
    - Фильтрацию по остатку (min_stock)
    - Фильтрацию по цене (max_price)
    - Кэширование через Redis (TTL: 5 минут)
    """
    # Проверка кэша
    cache_key = get_cache_key("list", page=page, page_size=page_size, 
                              min_stock=min_stock, max_price=max_price)
    cached = get_from_cache(cache_key)
    if cached:
        logger.info(f"Cache hit for {cache_key}")
        return JSONResponse(content=cached)
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Подсчёт общего количества
        count_query = "SELECT COUNT(*) FROM products WHERE 1=1"
        count_params = []
        
        if min_stock is not None:
            count_query += " AND stock >= %s"
            count_params.append(min_stock)
        
        if max_price is not None:
            count_query += " AND price <= %s"
            count_params.append(max_price)
        
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()[0]
        
        # Получение товаров с пагинацией
        offset = (page - 1) * page_size
        query = """
            SELECT id, articul, name, price, stock, 
                   updated_at, synced_at
            FROM products
            WHERE 1=1
        """
        params = []
        
        if min_stock is not None:
            query += " AND stock >= %s"
            params.append(min_stock)
        
        if max_price is not None:
            query += " AND price <= %s"
            params.append(max_price)
        
        query += " ORDER BY name LIMIT %s OFFSET %s"
        params.extend([page_size, offset])
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Формирование ответа
        products = []
        for row in rows:
            products.append({
                "id": str(row[0]),
                "articul": row[1],
                "name": row[2],
                "price": float(row[3]),
                "stock": row[4],
                "updated_at": row[5].isoformat() if row[5] else None,
                "synced_at": row[6].isoformat() if row[6] else None
            })
        
        pages = (total + page_size - 1) // page_size
        
        response = {
            "items": products,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages
        }
        
        # Сохранение в кэш
        set_to_cache(cache_key, response)
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting catalog: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            return_db_connection(conn)


@app.get(
    "/api/catalog/search",
    response_model=ProductListResponse,
    summary="Поиск товаров",
    description="Поиск товаров по названию или артикулу с поддержкой нечёткого поиска",
    responses={
        200: {"description": "Результаты поиска"},
        400: {"description": "Некорректный запрос (пустой поисковый запрос)"},
        500: {"description": "Внутренняя ошибка сервера"}
    }
)
async def search_catalog(
    q: str = Query(..., min_length=1, description="Поисковый запрос (название товара или артикул)"),
    fuzzy: bool = Query(True, description="Использовать нечёткий поиск (по умолчанию: true)"),
    min_price: Optional[float] = Query(None, ge=0, description="Минимальная цена товара"),
    max_price: Optional[float] = Query(None, ge=0, description="Максимальная цена товара"),
    in_stock: bool = Query(False, description="Показывать только товары в наличии (stock > 0)"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    page_size: int = Query(20, ge=1, le=100, description="Размер страницы")
):
    """
    Поиск товаров по названию или артикулу.
    
    Поддерживает:
    - Нечёткий поиск (fuzzy matching)
    - Фильтрацию по цене (min_price, max_price)
    - Фильтрацию по наличию (in_stock)
    - Сортировку по релевантности
    - Кэширование через Redis
    """
    cache_key = get_cache_key("search", q=q, fuzzy=fuzzy, min_price=min_price,
                              max_price=max_price, in_stock=in_stock,
                              page=page, page_size=page_size)
    cached = get_from_cache(cache_key)
    if cached:
        logger.info(f"Cache hit for {cache_key}")
        return JSONResponse(content=cached)
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Построение запроса поиска
        search_term = f"%{q.lower()}%"
        
        if fuzzy:
            # Нечёткий поиск по названию и артикулу
            query = """
                SELECT id, articul, name, price, stock, 
                       updated_at, synced_at,
                       CASE 
                           WHEN LOWER(articul) = LOWER(%s) THEN 1.0
                           WHEN LOWER(articul) LIKE LOWER(%s) THEN 0.9
                           WHEN LOWER(name) LIKE LOWER(%s) THEN 0.8
                           ELSE 0.5
                       END as relevance
                FROM products
                WHERE (LOWER(name) LIKE %s OR LOWER(articul) LIKE %s)
            """
            params = [q, f"{q}%", f"%{q}%", search_term, search_term]
        else:
            # Точный поиск
            query = """
                SELECT id, articul, name, price, stock, 
                       updated_at, synced_at, 1.0 as relevance
                FROM products
                WHERE LOWER(name) LIKE %s OR LOWER(articul) LIKE %s
            """
            params = [search_term, search_term]
        
        # Фильтры
        if min_price is not None:
            query += " AND price >= %s"
            params.append(min_price)
        
        if max_price is not None:
            query += " AND price <= %s"
            params.append(max_price)
        
        if in_stock:
            query += " AND stock > 0"
        
        # Подсчёт общего количества
        # Строим отдельный запрос для COUNT, используя только WHERE часть
        count_query = "SELECT COUNT(*) FROM products WHERE 1=1"
        count_params = []
        
        # Добавляем условия поиска
        count_query += " AND (LOWER(name) LIKE %s OR LOWER(articul) LIKE %s)"
        count_params.extend([search_term, search_term])
        
        # Добавляем фильтры
        if min_price is not None:
            count_query += " AND price >= %s"
            count_params.append(min_price)
        
        if max_price is not None:
            count_query += " AND price <= %s"
            count_params.append(max_price)
        
        if in_stock:
            count_query += " AND stock > 0"
        
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()[0]
        
        # Сортировка по релевантности и пагинация
        query += " ORDER BY relevance DESC, name LIMIT %s OFFSET %s"
        offset = (page - 1) * page_size
        params.extend([page_size, offset])
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Формирование ответа
        products = []
        for row in rows:
            products.append({
                "id": str(row[0]),
                "articul": row[1],
                "name": row[2],
                "price": float(row[3]),
                "stock": row[4],
                "updated_at": row[5].isoformat() if row[5] else None,
                "synced_at": row[6].isoformat() if row[6] else None,
                "relevance_score": float(row[7]) if len(row) > 7 else 1.0
            })
        
        pages = (total + page_size - 1) // page_size if total > 0 else 0
        
        response = {
            "items": products,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages
        }
        
        # Сохранение в кэш
        set_to_cache(cache_key, response)
        
        return response
        
    except Exception as e:
        logger.error(f"Error searching catalog: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            return_db_connection(conn)


@app.get("/api/catalog/{articul}", response_model=Product)
async def get_product_by_articul(articul: str):
    """
    Получить товар по артикулу.
    
    Args:
        articul: Артикул товара
    """
    cache_key = get_cache_key("product", articul=articul)
    cached = get_from_cache(cache_key)
    if cached:
        logger.info(f"Cache hit for {cache_key}")
        return JSONResponse(content=cached)
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT id, articul, name, price, stock, 
                   updated_at, synced_at
            FROM products
            WHERE articul = %s
            """,
            (articul,)
        )
        
        row = cursor.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail=f"Product with articul '{articul}' not found")
        
        product = {
            "id": str(row[0]),
            "articul": row[1],
            "name": row[2],
            "price": float(row[3]),
            "stock": row[4],
            "updated_at": row[5].isoformat() if row[5] else None,
            "synced_at": row[6].isoformat() if row[6] else None
        }
        
        # Сохранение в кэш
        set_to_cache(cache_key, product)
        
        return product
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting product {articul}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            return_db_connection(conn)


@app.get("/favicon.ico")
async def favicon():
    """Обработчик для favicon.ico."""
    from fastapi.responses import Response
    return Response(status_code=204)


@app.get(
    "/health",
    summary="Health Check",
    description="Проверка состояния сервиса и подключений к БД и Redis",
    responses={
        200: {
            "description": "Статус сервиса",
            "content": {
                "application/json": {
                    "example": {
                        "status": "ok",
                        "database": "ok",
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
    
    Проверяет:
    - Подключение к PostgreSQL
    - Подключение к Redis (опционально)
    
    Возвращает статус "ok" если БД доступна, "degraded" если Redis недоступен.
    """
    try:
        # Проверка БД
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        return_db_connection(conn)
        db_status = "ok"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        db_status = "error"
    
    try:
        # Проверка Redis
        if redis_client:
            redis_client.ping()
            redis_status = "ok"
        else:
            redis_status = "not_configured"
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        redis_status = "error"
    
    return {
        "status": "ok" if db_status == "ok" else "degraded",
        "database": db_status,
        "redis": redis_status
    }


if __name__ == "__main__":
    import uvicorn
    api_host = os.getenv('API_HOST', '0.0.0.0')
    api_port = int(os.getenv('API_PORT', '8025'))
    uvicorn.run(app, host=api_host, port=api_port)
