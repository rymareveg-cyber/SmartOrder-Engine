#!/usr/bin/env python3
"""
FastAPI сервер для доступа к каталогу товаров.

Предоставляет REST API endpoints для работы с каталогом товаров из PostgreSQL.
"""

import os
import json
from src.config import DatabaseConfig, RedisConfig, APIConfig
from typing import Optional, Any, List
from decimal import Decimal
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import psycopg2
from fastapi import FastAPI, HTTPException, Query, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from functools import lru_cache

from src.utils.logger import get_logger

logger = get_logger(__name__)

DATABASE_URL = DatabaseConfig.URL
CACHE_TTL = int(os.getenv('CACHE_TTL', '300'))

from src.utils.redis_client import init_redis_client
from src.database.pool import DatabasePool, init_db_pool as init_db_pool_util

db_pool: Optional[DatabasePool] = None
redis_client: Optional[Any] = None


def init_db_pool():
    """Инициализация connection pool для PostgreSQL."""
    global db_pool
    db_pool = init_db_pool_util(minconn=1, maxconn=10, dsn=DATABASE_URL)


def init_redis():
    """Инициализация Redis клиента."""
    global redis_client
    redis_client = init_redis_client(decode_responses=True, raise_on_error=False)


def get_db_connection():
    """Получить соединение с БД из pool."""
    if db_pool is None:
        init_db_pool()
    return db_pool.get_connection(timeout=5.0, retry_interval=0.1)


def return_db_connection(conn):
    """Вернуть соединение в pool."""
    if db_pool and conn:
        db_pool.return_connection(conn)


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events для FastAPI."""
    init_db_pool()
    init_redis()
    yield
    if db_pool:
        db_pool.close_all()
    if redis_client:
        redis_client.close()


app = FastAPI(
    title="SmartOrder Engine - API",
    description="API для доступа к каталогу товаров и работы с заказами. Поддерживает поиск, фильтрацию, кэширование через Redis и управление заказами.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    
    import os
    original_env_file = os.environ.get('ENV_FILE')
    if original_env_file:
        os.environ.pop('ENV_FILE')
    
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
    finally:
        if original_env_file:
            os.environ['ENV_FILE'] = original_env_file
    
    def rate_limit(limit_str):
        """Декоратор для применения rate limiting."""
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

try:
    from src.api.orders import router as orders_router
    app.include_router(orders_router)
    logger.info("Orders API routes included successfully")
except ImportError as e:
    logger.warning(f"Could not import orders API router: {e}. Orders endpoints will not be available.")
except Exception as e:
    logger.error(f"Error including orders API routes: {e}", exc_info=True)
    logger.warning("Orders endpoints will not be available.")


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
            return json.loads(cached)
    except Exception as e:
        logger.warning(f"Cache get error: {e}")
    return None


def set_to_cache(key: str, value: dict, ttl: int = CACHE_TTL):
    """Сохранить данные в кэш."""
    if not redis_client:
        return
    try:
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
@rate_limit("100/minute")
async def get_catalog(
    request: Request,
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
            try:
                cursor.close()
            except Exception:
                pass
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
@rate_limit("100/minute")
async def search_catalog(
    request: Request,
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
            try:
                cursor.close()
            except Exception:
                pass
            return_db_connection(conn)


@app.get("/api/catalog/{articul}", response_model=Product)
@rate_limit("100/minute")
async def get_product_by_articul(request: Request, articul: str):
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
            try:
                cursor.close()
            except Exception:
                pass
            return_db_connection(conn)


@app.get("/favicon.ico")
async def favicon():
    """Обработчик для favicon.ico."""
    from fastapi.responses import Response
    return Response(status_code=204)


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
    """
    return {
        "status": "ok",
        "service": "api_catalog_server",
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
        # Используем локальный db_pool, а не из crm_service
        global db_pool
        
        if db_pool is None:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "not_ready",
                    "service": "api_catalog_server",
                    "reason": "Database pool not initialized"
                }
            )
        
        # Легкая проверка без получения соединения
        pool_info = {
            "minconn": db_pool.minconn,
            "maxconn": db_pool.maxconn
        }
        
        redis_status = "not_configured"
        if redis_client:
            try:
                redis_client.ping()
                redis_status = "ok"
            except:
                redis_status = "error"
        
        return {
            "status": "ready",
            "service": "api_catalog_server",
            "database_pool": pool_info,
            "redis": redis_status,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "service": "api_catalog_server",
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
    Health check endpoint (legacy).
    
    Быстрая проверка без блокирующих операций.
    Не проверяет БД, чтобы не блокировать при перегрузке.
    """
    # Легкая проверка без блокирующих операций
    redis_status = "not_configured"
    if redis_client:
        try:
            redis_client.ping()
            redis_status = "ok"
        except:
            redis_status = "error"
    
    return {
        "status": "ok",
        "service": "api_catalog_server",
        "redis": redis_status,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    import sys
    import signal
    
    api_host = os.getenv('API_HOST', '0.0.0.0')
    api_port = int(os.getenv('API_PORT', '8025'))
    
    # Раздельные логи uvicorn (access/error) в файлы logs/api_catalog_server.uvicorn.*.log
    try:
        from src.utils.logger import setup_uvicorn_logging
        setup_uvicorn_logging("api_catalog_server")
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
        uvicorn.run(app, host=api_host, port=api_port, log_level="info", log_config=None)
    except KeyboardInterrupt:
        logger.info("Catalog API stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in Catalog API: {e}", exc_info=True)
        sys.exit(1)
