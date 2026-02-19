#!/usr/bin/env python3
"""
Скрипт синхронизации каталога товаров из 1С в PostgreSQL.

Автоматически синхронизирует каталог товаров из 1С:Управление нашей фирмой
в PostgreSQL с периодическим обновлением через планировщик.
"""

import os
import sys
import json
import time
import base64
from datetime import datetime, timezone
from typing import List, Dict
from decimal import Decimal

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.utils.logger import get_logger
from src.database.pool import init_db_pool, get_db_connection, return_db_connection
from src.config import OneCConfig

logger = get_logger(__name__)

ONEC_BASE_URL = OneCConfig.BASE_URL or 'http://localhost:80'
ONEC_USERNAME = OneCConfig.USERNAME or ''
ONEC_PASSWORD = OneCConfig.PASSWORD or ''
SYNC_INTERVAL_MINUTES = OneCConfig.SYNC_INTERVAL // 60


def create_http_session() -> requests.Session:
    """Создать HTTP сессию с retry логикой."""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


def fetch_catalog_from_1c() -> List[Dict]:
    """
    Получить каталог товаров из 1С API.
    
    Returns:
        List[Dict]: Список товаров из 1С
        
    Raises:
        requests.RequestException: При ошибке запроса к 1С
    """
    url = f"{ONEC_BASE_URL}{OneCConfig.CATALOG_ENDPOINT}"
    
    logger.info(f"Fetching catalog from 1C: {url}")
    
    session = create_http_session()
    
    # Подготовка Basic Auth заголовка с поддержкой кириллицы
    # Используем base64 кодирование для универсальной поддержки любых символов
    credentials = f"{ONEC_USERNAME}:{ONEC_PASSWORD}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('ascii')
    auth_header = f"Basic {encoded_credentials}"
    
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': auth_header
        }
        
        response = session.get(
            url,
            timeout=30,
            headers=headers
        )
        response.raise_for_status()
        
        data = response.json()
        
        if not isinstance(data, list):
            logger.warning(f"Unexpected response format from 1C: {type(data)}")
            return []
        
        logger.info(f"Received {len(data)} products from 1C")
        return data
        
    except requests.exceptions.Timeout:
        logger.error("Timeout while fetching catalog from 1C")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching catalog from 1C: {e}")
        raise
    finally:
        session.close()


def validate_product(product: Dict) -> bool:
    """
    Валидация данных товара.
    
    Args:
        product: Словарь с данными товара
        
    Returns:
        bool: True если товар валиден
    """
    required_fields = ['articul', 'name', 'price', 'stock']
    
    # Проверка наличия обязательных полей
    for field in required_fields:
        if field not in product:
            logger.warning(f"Product missing required field '{field}': {product}")
            return False
    
    # Проверка типов и значений
    if not isinstance(product['articul'], str) or not product['articul'].strip():
        logger.warning(f"Invalid articul: {product.get('articul')}")
        return False
    
    if not isinstance(product['name'], str) or not product['name'].strip():
        logger.warning(f"Invalid name: {product.get('name')}")
        return False
    
    try:
        price = float(product['price'])
        if price < 0:
            logger.warning(f"Negative price for product {product.get('articul')}: {price}")
            return False
    except (ValueError, TypeError):
        logger.warning(f"Invalid price for product {product.get('articul')}: {product.get('price')}")
        return False
    
    try:
        stock = int(product['stock'])
        if stock < 0:
            logger.warning(f"Negative stock for product {product.get('articul')}: {stock}")
            return False
    except (ValueError, TypeError):
        logger.warning(f"Invalid stock for product {product.get('articul')}: {product.get('stock')}")
        return False
    
    return True


def normalize_product(product: Dict) -> Dict:
    """
    Нормализация данных товара для сохранения в БД.
    
    Args:
        product: Словарь с данными товара из 1С
        
    Returns:
        Dict: Нормализованный словарь
    """
    return {
        'articul': product['articul'].strip(),
        'name': product['name'].strip(),
        'price': Decimal(str(product['price'])),
        'stock': int(product['stock'])
    }


def save_products_to_db(products: List[Dict]) -> Dict[str, int]:
    """
    Сохранить товары в PostgreSQL (upsert логика).
    
    Args:
        products: Список нормализованных товаров
        
    Returns:
        Dict: Статистика сохранения {'created': int, 'updated': int}
    """
    if not products:
        logger.warning("No products to save")
        return {'created': 0, 'updated': 0}
    
    conn = None
    stats = {'created': 0, 'updated': 0}
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Начало транзакции
        conn.autocommit = False
        
        synced_at = datetime.now(timezone.utc)
        
        # Upsert для каждого товара
        for product in products:
            try:
                # Проверка существования товара
                cursor.execute(
                    "SELECT id FROM products WHERE articul = %s",
                    (product['articul'],)
                )
                existing = cursor.fetchone()
                
                if existing:
                    # Обновление существующего товара
                    cursor.execute(
                        """
                        UPDATE products 
                        SET name = %s, price = %s, stock = %s, 
                            updated_at = CURRENT_TIMESTAMP, synced_at = %s
                        WHERE articul = %s
                        """,
                        (
                            product['name'],
                            product['price'],
                            product['stock'],
                            synced_at,
                            product['articul']
                        )
                    )
                    stats['updated'] += 1
                else:
                    # Создание нового товара
                    cursor.execute(
                        """
                        INSERT INTO products (articul, name, price, stock, synced_at)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            product['articul'],
                            product['name'],
                            product['price'],
                            product['stock'],
                            synced_at
                        )
                    )
                    stats['created'] += 1
                    
            except Exception as e:
                logger.error(f"Error saving product {product.get('articul')}: {e}")
                conn.rollback()
                continue
        
        # Коммит транзакции
        conn.commit()
        logger.info(f"Saved products: {stats['created']} created, {stats['updated']} updated")
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            return_db_connection(conn)
    
    return stats


def sync_catalog():
    """
    Основная функция синхронизации каталога.
    
    Выполняет полный цикл: получение данных из 1С → валидация → сохранение в БД.
    """
    start_time = time.time()
    logger.info("Starting catalog synchronization")
    
    try:
        # Шаг 1: Получение данных из 1С
        raw_products = fetch_catalog_from_1c()
        
        if not raw_products:
            logger.warning("Received empty catalog from 1C")
            return
        
        # Шаг 2: Валидация и нормализация
        valid_products = []
        for product in raw_products:
            if validate_product(product):
                normalized = normalize_product(product)
                valid_products.append(normalized)
            else:
                logger.warning(f"Skipping invalid product: {product.get('articul', 'unknown')}")
        
        logger.info(f"Validated {len(valid_products)} out of {len(raw_products)} products")
        
        # Шаг 3: Сохранение в БД
        stats = save_products_to_db(valid_products)
        
        # Логирование успешной синхронизации
        duration = time.time() - start_time
        logger.info(
            json.dumps({
                "level": "INFO",
                "event": "catalog_sync_success",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "products_total": len(valid_products),
                "products_updated": stats['updated'],
                "products_created": stats['created'],
                "duration_seconds": round(duration, 2)
            })
        )
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            json.dumps({
                "level": "ERROR",
                "event": "catalog_sync_error",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "duration_seconds": round(duration, 2)
            })
        )
        raise


def run_scheduler():
    """Запуск планировщика для автоматической синхронизации."""
    logger.info(f"Starting scheduler with interval: {SYNC_INTERVAL_MINUTES} minutes")
    
    # Первый запуск сразу
    sync_catalog()
    
    # Настройка планировщика
    scheduler = BlockingScheduler()
    scheduler.add_job(
        sync_catalog,
        trigger=IntervalTrigger(minutes=SYNC_INTERVAL_MINUTES),
        id='catalog_sync',
        name='Catalog synchronization from 1C',
        replace_existing=True
    )
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
        scheduler.shutdown()


if __name__ == "__main__":
    import signal
    import sys
    
    # Создание директории для логов
    os.makedirs('logs', exist_ok=True)
    
    def signal_handler(signum, frame):
        """Обработчик сигналов для graceful shutdown."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        sys.exit(0)
    
    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Инициализация БД pool (централизованная функция из src.database.pool)
        init_db_pool()
        
        # Запуск синхронизации
        if len(sys.argv) > 1 and sys.argv[1] == '--once':
            # Однократная синхронизация
            sync_catalog()
        else:
            # Запуск с планировщиком
            run_scheduler()
    except KeyboardInterrupt:
        logger.info("Catalog sync stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in catalog sync: {e}", exc_info=True)
        sys.exit(1)
