#!/usr/bin/env python3
"""
AI-парсер заказов на основе GPT-4.

Парсит неструктурированные заказы, извлекает товары из каталога 1С,
валидирует данные и формирует структурированные заказы.
"""

import os
import json
import logging
import re
import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from decimal import Decimal

from dotenv import load_dotenv
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from openai import OpenAI
from pydantic import BaseModel, Field, field_validator

# Импорты с поддержкой как относительных, так и абсолютных
try:
    # Попытка относительного импорта (когда модуль импортируется как часть пакета)
    from .prompt_templates import (
        get_parsing_prompt,
        get_clarification_questions_prompt,
        format_catalog_for_prompt
    )
    from .catalog_matcher import (
        match_products_from_text,
        validate_product_availability,
        extract_articul_from_text,
        extract_quantity_from_text
    )
except ImportError:
    # Fallback на абсолютный импорт (когда модуль запускается напрямую)
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    from execution.prompt_templates import (
        get_parsing_prompt,
        get_clarification_questions_prompt,
        format_catalog_for_prompt
    )
    from execution.catalog_matcher import (
        match_products_from_text,
        validate_product_availability,
        extract_articul_from_text,
        extract_quantity_from_text
    )

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/ai_order_parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Переменные окружения
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-4.1-mini')
OPENAI_BASE_URL = os.getenv('OPENAI_BASE_URL', 'https://api.proxyapi.ru/openai/v1')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/smartorder')

# Connection pool для PostgreSQL
db_pool: Optional[SimpleConnectionPool] = None

# Кэш каталога
catalog_cache: Optional[List[Dict[str, Any]]] = None
catalog_cache_time: Optional[datetime] = None
CATALOG_CACHE_TTL = 300  # 5 минут

# Максимальное количество попыток вызова OpenAI API
MAX_RETRIES = 3
RETRY_DELAYS = [1, 2, 4]  # секунды


# Pydantic модели
class ParsedProduct(BaseModel):
    """Модель извлечённого товара."""
    articul: str
    name: str
    quantity: int = Field(ge=1)
    price_mentioned: Optional[float] = None


class ParsedCustomer(BaseModel):
    """Модель извлечённых контактов клиента."""
    name: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None


class ParsedOrder(BaseModel):
    """Модель результата парсинга от GPT."""
    products: List[ParsedProduct] = Field(default_factory=list)
    customer: ParsedCustomer
    missing_data: List[str] = Field(default_factory=list)
    unfound_products: List[str] = Field(default_factory=list)


class ValidatedProduct(BaseModel):
    """Модель валидированного товара."""
    articul: str
    name: str
    quantity: int
    price_at_order: float
    available: bool
    stock: int
    validated: bool


class OrderResult(BaseModel):
    """Модель результата обработки заказа."""
    order_id: str
    status: str  # validated, needs_clarification
    products: List[ValidatedProduct]
    customer: ParsedCustomer
    missing_data: List[str]
    unfound_products: List[str]
    clarification_questions: List[str] = Field(default_factory=list)


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


def get_db_connection():
    """Получить соединение с БД из pool."""
    if db_pool is None:
        init_db_pool()
    return db_pool.getconn()


def return_db_connection(conn):
    """Вернуть соединение в pool."""
    if db_pool:
        db_pool.putconn(conn)


def load_catalog_from_db(force_refresh: bool = False) -> List[Dict[str, Any]]:
    """
    Загрузка каталога товаров из PostgreSQL.
    
    Args:
        force_refresh: Принудительное обновление кэша
        
    Returns:
        Список товаров из каталога
    """
    global catalog_cache, catalog_cache_time
    
    # Проверка кэша
    if not force_refresh and catalog_cache and catalog_cache_time:
        elapsed = (datetime.now(timezone.utc) - catalog_cache_time).total_seconds()
        if elapsed < CATALOG_CACHE_TTL:
            logger.debug(f"Using cached catalog ({len(catalog_cache)} items)")
            return catalog_cache
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT articul, name, price, stock
            FROM products
            ORDER BY name
        """)
        
        rows = cursor.fetchall()
        products = []
        
        for row in rows:
            products.append({
                "articul": row[0],
                "name": row[1],
                "price": float(row[2]),
                "stock": int(row[3])
            })
        
        # Обновление кэша
        catalog_cache = products
        catalog_cache_time = datetime.now(timezone.utc)
        
        logger.info(f"Loaded {len(products)} products from database")
        return products
    
    except Exception as e:
        logger.error(f"Error loading catalog: {e}", exc_info=True)
        return catalog_cache or []
    finally:
        if conn:
            return_db_connection(conn)


def init_openai_client() -> OpenAI:
    """Инициализация OpenAI клиента."""
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY not set in environment variables")
    
    client = OpenAI(
        api_key=OPENAI_API_KEY,
        base_url=OPENAI_BASE_URL
    )
    
    logger.info(f"OpenAI client initialized: model={OPENAI_MODEL}, base_url={OPENAI_BASE_URL}")
    return client


async def call_openai_api(client: OpenAI, prompt: str) -> Optional[str]:
    """
    Вызов OpenAI API с retry логикой.
    
    Args:
        client: OpenAI клиент
        prompt: Промпт для GPT
        
    Returns:
        Ответ от GPT или None в случае ошибки
    """
    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"Calling OpenAI API (attempt {attempt + 1}/{MAX_RETRIES})")
            
            # Используем asyncio.to_thread для выполнения синхронного вызова в отдельном потоке
            response = await asyncio.to_thread(
                client.chat.completions.create,
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "Ты - AI-ассистент для обработки заказов. Отвечай только валидным JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=2000,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            logger.info(f"OpenAI API response received: {len(content)} characters")
            return content
        
        except Exception as e:
            delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)] if attempt < MAX_RETRIES - 1 else 0
            logger.warning(f"OpenAI API error (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(delay)
            else:
                logger.error(f"Failed to call OpenAI API after {MAX_RETRIES} attempts")
                return None
    
    return None


def parse_gpt_response(response: str) -> Optional[ParsedOrder]:
    """
    Парсинг ответа от GPT.
    
    Args:
        response: JSON ответ от GPT
        
    Returns:
        ParsedOrder или None в случае ошибки
    """
    try:
        # Попытка извлечь JSON из ответа (на случай если GPT добавил текст)
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            response = json_match.group(0)
        
        data = json.loads(response)
        
        # Валидация через Pydantic
        parsed_order = ParsedOrder(**data)
        return parsed_order
    
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse GPT response as JSON: {e}")
        logger.debug(f"Response content: {response[:500]}")
        return None
    except Exception as e:
        logger.error(f"Error parsing GPT response: {e}", exc_info=True)
        return None


def fallback_regex_parser(message: str, catalog: List[Dict[str, Any]]) -> ParsedOrder:
    """
    Fallback парсер на основе regex правил.
    
    Используется когда GPT недоступен.
    
    Args:
        message: Сообщение клиента
        catalog: Каталог товаров
        
    Returns:
        ParsedOrder с извлечёнными данными
    """
    logger.info("Using fallback regex parser")
    
    # Извлечение товаров через catalog_matcher
    matched_products = match_products_from_text(message, catalog, max_results=10)
    
    products = []
    for product, relevance, quantity in matched_products:
        if relevance >= 0.5:  # Минимальный порог релевантности
            products.append(ParsedProduct(
                articul=product.get("articul", ""),
                name=product.get("name", ""),
                quantity=quantity
            ))
    
    # Извлечение телефона
    phone = None
    phone_patterns = [
        r'\+?7\s?\(?\d{3}\)?\s?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
        r'8\s?\(?\d{3}\)?\s?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
        r'\d{10,11}'
    ]
    for pattern in phone_patterns:
        match = re.search(pattern, message)
        if match:
            phone = match.group(0).replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
            break
    
    # Извлечение адреса (простая эвристика)
    address = None
    address_keywords = ['москва', 'санкт-петербург', 'адрес', 'доставка', 'ул.', 'улица', 'дом', 'квартира']
    for keyword in address_keywords:
        if keyword.lower() in message.lower():
            # Пытаемся извлечь фразу после ключевого слова
            match = re.search(rf'{keyword}[:\\s]+([^,\\.]+)', message, re.IGNORECASE)
            if match:
                address = match.group(1).strip()
                break
    
    customer = ParsedCustomer(
        name=None,
        phone=phone,
        address=address
    )
    
    missing_data = []
    if not customer.name:
        missing_data.append("name")
    if not customer.phone:
        missing_data.append("phone")
    if not customer.address:
        missing_data.append("address")
    
    return ParsedOrder(
        products=products,
        customer=customer,
        missing_data=missing_data,
        unfound_products=[]
    )


def validate_parsed_order(parsed_order: ParsedOrder, catalog: List[Dict[str, Any]]) -> OrderResult:
    """
    Валидация извлечённого заказа.
    
    Args:
        parsed_order: Результат парсинга от GPT
        catalog: Каталог товаров
        
    Returns:
        OrderResult с валидированными данными
    """
    validated_products = []
    unfound_products = []
    
    # Валидация товаров
    for product in parsed_order.products:
        # Поиск товара в каталоге
        found_product = None
        for cat_product in catalog:
            if cat_product.get("articul") == product.articul:
                found_product = cat_product
                break
        
        if not found_product:
            unfound_products.append(product.name)
            continue
        
        # Валидация доступности
        validation = validate_product_availability(found_product, product.quantity)
        
        validated_products.append(ValidatedProduct(
            articul=product.articul,
            name=found_product.get("name", product.name),
            quantity=product.quantity,
            price_at_order=found_product.get("price", 0),
            available=validation["available"],
            stock=validation["stock"],
            validated=validation["available"]
        ))
    
    # Определение статуса
    has_unfound = len(unfound_products) > 0 or len(parsed_order.unfound_products) > 0
    has_missing_data = len(parsed_order.missing_data) > 0
    has_unavailable = any(not p.available for p in validated_products)
    
    if has_unfound or has_missing_data or has_unavailable:
        status = "needs_clarification"
    else:
        status = "validated"
    
    # Генерация уточняющих вопросов
    clarification_questions = []
    if has_missing_data:
        for missing in parsed_order.missing_data:
            if missing == "name":
                clarification_questions.append("Укажите, пожалуйста, ваше ФИО")
            elif missing == "phone":
                clarification_questions.append("Укажите, пожалуйста, ваш телефон")
            elif missing in ["address", "full_address"]:
                clarification_questions.append("Укажите полный адрес доставки")
    
    if has_unfound:
        clarification_questions.append("Некоторые товары не найдены в каталоге. Пожалуйста, уточните артикулы или названия.")
    
    if has_unavailable:
        for product in validated_products:
            if not product.available:
                clarification_questions.append(f"Товар '{product.name}': в наличии только {product.stock} шт., запрошено {product.quantity} шт. Подтвердите количество.")
    
    # Объединение unfound_products
    all_unfound = list(set(unfound_products + parsed_order.unfound_products))
    
    return OrderResult(
        order_id=f"temp-{int(datetime.now(timezone.utc).timestamp())}",
        status=status,
        products=validated_products,
        customer=parsed_order.customer,
        missing_data=parsed_order.missing_data,
        unfound_products=all_unfound,
        clarification_questions=clarification_questions
    )


async def parse_order(message_data: Dict[str, Any]) -> Optional[OrderResult]:
    """
    Основная функция парсинга заказа.
    
    Args:
        message_data: Данные сообщения из очереди
        
    Returns:
        OrderResult или None в случае ошибки
    """
    try:
        # Извлечение текста сообщения в зависимости от канала
        channel = message_data.get("channel", "unknown")
        
        if channel == "telegram":
            customer_message = message_data.get("message", "")
        elif channel == "yandex_mail":
            customer_message = message_data.get("body", "") or message_data.get("subject", "")
        elif channel == "yandex_forms":
            # Объединение данных формы в текст
            form_data = message_data.get("data", {})
            customer_message = " ".join([f"{k}: {v}" for k, v in form_data.items()])
        else:
            logger.warning(f"Unknown channel: {channel}")
            customer_message = str(message_data)
        
        if not customer_message:
            logger.warning("Empty customer message")
            return None
        
        # Загрузка каталога
        catalog = load_catalog_from_db()
        if not catalog:
            logger.error("Catalog is empty")
            return None
        
        # Формирование промпта
        catalog_json = format_catalog_for_prompt(catalog, max_items=100)
        prompt = get_parsing_prompt(catalog_json, customer_message)
        
        # Вызов OpenAI API
        client = init_openai_client()
        gpt_response = await call_openai_api(client, prompt)
        
        parsed_order = None
        
        if gpt_response:
            parsed_order = parse_gpt_response(gpt_response)
        
        # Fallback на regex парсинг
        if not parsed_order:
            logger.warning("GPT parsing failed, using fallback regex parser")
            parsed_order = fallback_regex_parser(customer_message, catalog)
        
        # Валидация
        result = validate_parsed_order(parsed_order, catalog)
        
        logger.info(f"Order parsed: status={result.status}, products={len(result.products)}, missing_data={len(result.missing_data)}")
        
        return result
    
    except Exception as e:
        logger.error(f"Error parsing order: {e}", exc_info=True)
        return None


# Функция для использования в queue_processor
async def process_order_message(message_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Обработка сообщения заказа (для использования в queue_processor).
    
    Args:
        message_data: Данные сообщения из очереди
        
    Returns:
        Результат обработки в виде словаря или None
    """
    result = await parse_order(message_data)
    
    if result:
        return result.model_dump()
    
    return None


if __name__ == "__main__":
    # Тестирование
    import asyncio
    import sys
    from pathlib import Path
    
    # Добавление корневой директории проекта в sys.path для импорта
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    os.makedirs("logs", exist_ok=True)
    
    test_message = {
        "channel": "telegram",
        "message": "Хочу 2 варочные панели по 120 тысяч, доставка в Москву",
        "user_id": "123456789",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    async def test():
        result = await parse_order(test_message)
        if result:
            print(json.dumps(result.model_dump(), ensure_ascii=False, indent=2))
        else:
            print("Failed to parse order")
    
    asyncio.run(test())
