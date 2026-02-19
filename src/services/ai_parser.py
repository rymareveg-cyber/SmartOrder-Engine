#!/usr/bin/env python3
"""
AI-парсер заказов на основе GPT-4.

Парсит неструктурированные заказы, извлекает товары из каталога 1С,
валидирует данные и формирует структурированные заказы.
"""

import json
import re
import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import psycopg2
from openai import OpenAI
from pydantic import BaseModel, Field

from src.config import OpenAIConfig, DatabaseConfig
from src.database.pool import get_db_connection, return_db_connection
from src.utils.logger import get_logger
from src.utils.retry import retry_with_backoff, get_openai_circuit_breaker, CircuitBreakerOpenError

from src.services.prompt_templates import (
    get_parsing_prompt,
    get_clarification_questions_prompt,
    get_clarification_response_prompt,
    format_catalog_for_prompt
)
from src.services.catalog_matcher import (
    match_products_from_text,
    validate_product_availability,
    extract_articul_from_text,
    extract_quantity_from_text,
    find_by_name_fuzzy
)

logger = get_logger(__name__)

catalog_cache: Optional[List[Dict[str, Any]]] = None
catalog_cache_time: Optional[datetime] = None
CATALOG_CACHE_TTL = 300
MAX_RETRIES = 3


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
    cursor = None
    try:
        conn = get_db_connection()
        if not conn or conn.closed:
            logger.error("Failed to get valid database connection")
            return catalog_cache or []
        
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
        # Закрываем курсор перед возвратом соединения
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                logger.warning(f"Error closing cursor: {e}")
        
        # Возвращаем соединение в пул
        if conn:
            try:
                return_db_connection(conn)
            except Exception as e:
                logger.error(f"Error returning database connection: {e}", exc_info=True)


def init_openai_client() -> OpenAI:
    """Инициализация OpenAI клиента."""
    if not OpenAIConfig.API_KEY:
        raise ValueError("OPENAI_API_KEY not set in environment variables")
    
    client = OpenAI(
        api_key=OpenAIConfig.API_KEY,
        base_url=OpenAIConfig.BASE_URL
    )
    
    logger.info(f"OpenAI client initialized: model={OpenAIConfig.MODEL}, base_url={OpenAIConfig.BASE_URL}")
    return client


@retry_with_backoff(
    max_retries=MAX_RETRIES,
    initial_delay=1.0,
    max_delay=30.0,
    exponential_base=2.0,
    jitter=True,
    retry_on=(Exception,)
)
async def _call_openai_api_internal(client: OpenAI, prompt: str) -> str:
    """
    Внутренняя функция для вызова OpenAI API (без circuit breaker).
    
    Args:
        client: OpenAI клиент
        prompt: Промпт для GPT
        
    Returns:
        Ответ от GPT
        
    Raises:
        Exception: При ошибке API
    """
    logger.debug("Calling OpenAI API")
    
    # Используем asyncio.to_thread для выполнения синхронного вызова в отдельном потоке
    response = await asyncio.to_thread(
        client.chat.completions.create,
        model=OpenAIConfig.MODEL,
        messages=[
            {"role": "system", "content": "Отвечай только валидным JSON."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3,
        max_tokens=2000,
        response_format={"type": "json_object"}
    )
    
    content = response.choices[0].message.content
    logger.info(f"OpenAI API response received: {len(content)} characters")
    return content


async def call_openai_api(client: OpenAI, prompt: str) -> Optional[str]:
    """
    Вызов OpenAI API с retry логикой и circuit breaker.
    
    Args:
        client: OpenAI клиент
        prompt: Промпт для GPT
        
    Returns:
        Ответ от GPT или None в случае ошибки
    """
    circuit_breaker = get_openai_circuit_breaker()
    
    try:
        # Вызов через circuit breaker
        content = await circuit_breaker.call(_call_openai_api_internal, client, prompt)
        return content
    except CircuitBreakerOpenError as e:
        logger.error(f"OpenAI circuit breaker is OPEN: {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to call OpenAI API: {e}", exc_info=True)
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


def _is_address_complete(address: Optional[str]) -> bool:
    """
    Проверяет, является ли адрес достаточно полным для доставки.
    Требуется: улица (или переулок, проспект и т.п.) + номер дома.
    Только город/регион считается неполным адресом.
    """
    if not address:
        return False
    addr_lower = address.lower()
    # Ключевые слова типа улицы
    street_keywords = [
        "ул.", "улица", "пр.", "проспект", "пер.", "переулок",
        "бульвар", "бул.", "наб.", "набережная", "шоссе",
        "пл.", "площадь", "тракт", "аллея", "проезд"
    ]
    has_street = any(kw in addr_lower for kw in street_keywords)
    # Наличие номера дома: "д. 5", "дом 5", "д5", "15", etc.
    import re as _re
    has_house = bool(_re.search(r'\b(д\.?\s*\d+|\d+\s*[а-яё]?)\b', addr_lower))
    return has_street or has_house


def validate_parsed_order(
    parsed_order: ParsedOrder,
    catalog: List[Dict[str, Any]],
    known_customer_name: Optional[str] = None,
    known_customer_phone: Optional[str] = None
) -> OrderResult:
    """
    Валидация извлечённого заказа.
    
    Args:
        parsed_order: Результат парсинга от GPT
        catalog: Каталог товаров
        known_customer_name: Известное имя клиента (из авторизации)
        known_customer_phone: Известный телефон клиента (из авторизации)
        
    Returns:
        OrderResult с валидированными данными
    """
    validated_products = []
    unfound_products = []
    
    # Валидация товаров
    for product in parsed_order.products:
        # Поиск товара в каталоге
        found_product = None
        
        # Сначала ищем по точному артикулу
        if product.articul:
            for cat_product in catalog:
                if cat_product.get("articul", "").strip().upper() == product.articul.strip().upper():
                    found_product = cat_product
                    break
        
        # Если не найден по артикулу, ищем по названию через fuzzy matching
        if not found_product and product.name:
            try:
                matches = find_by_name_fuzzy(product.name, catalog, limit=3)
                if matches:
                    # Берем товар с наивысшей релевантностью (если >= 70%)
                    best_match, relevance = matches[0]
                    if relevance >= 0.7:  # Порог релевантности 70%
                        found_product = best_match
                        logger.info(f"Found product '{product.name}' via fuzzy matching: '{best_match.get('name')}' (relevance: {relevance:.2%})")
                    else:
                        logger.warning(f"Product '{product.name}' found but relevance too low: {relevance:.2%}")
            except Exception as e:
                logger.error(f"Error in fuzzy matching for '{product.name}': {e}")
        
        if not found_product:
            unfound_products.append(product.name)
            logger.warning(f"Product not found: name='{product.name}', articul='{product.articul}'")
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
    
    # Объединение данных клиента: используем известные данные из авторизации, если они есть
    raw_address = parsed_order.customer.address
    # Проверяем полноту адреса: адрес должен содержать не только город/регион
    # Признаки неполного адреса: только "г. ..." или "город ..." без улицы
    final_address = raw_address if _is_address_complete(raw_address) else None

    final_customer = ParsedCustomer(
        name=parsed_order.customer.name or known_customer_name,
        phone=parsed_order.customer.phone or known_customer_phone,
        address=final_address
    )

    # Определение недостающих данных (с учетом известных из авторизации)
    missing_data = list(parsed_order.missing_data)  # берем то, что GPT пометил как missing
    # Дополняем программными проверками
    if not final_customer.name and "name" not in missing_data:
        missing_data.append("name")
    if not final_customer.phone and "phone" not in missing_data:
        missing_data.append("phone")
    if not final_customer.address and "address" not in missing_data:
        missing_data.append("address")
    # Убираем дубли
    missing_data = list(dict.fromkeys(missing_data))

    # Определение статуса
    has_unfound = len(unfound_products) > 0 or len(parsed_order.unfound_products) > 0
    has_missing_data = len(missing_data) > 0
    has_unavailable = any(not p.available for p in validated_products)
    has_no_products = len(validated_products) == 0  # КРИТИЧНО: нет товаров вообще

    # Если нет товаров - обязательно needs_clarification
    if has_no_products:
        status = "needs_clarification"
        logger.warning(f"No validated products found. Unfound: {unfound_products}, Parsed products: {len(parsed_order.products)}")
    elif has_unfound or has_missing_data or has_unavailable:
        status = "needs_clarification"
    else:
        status = "validated"

    if raw_address and not final_address:
        logger.info(f"Address '{raw_address}' is incomplete (no street/house), requesting full address")

    # Генерация уточняющих вопросов (только для действительно недостающих данных)
    clarification_questions = []
    if has_missing_data:
        for missing in missing_data:
            if missing == "name":
                clarification_questions.append("Укажите, пожалуйста, ваше ФИО")
            elif missing == "phone":
                clarification_questions.append("Укажите, пожалуйста, ваш номер телефона")
            elif missing == "address":
                if raw_address and not final_address:
                    clarification_questions.append(
                        f"Уточните полный адрес доставки — нужны улица и номер дома.\n"
                        f"Вы указали: «{raw_address}»\n"
                        f"Пример полного адреса: г. Иркутск, ул. Шукшина, д. 60, кв. 15"
                    )
                else:
                    clarification_questions.append(
                        "Укажите полный адрес доставки (город, улица, дом, квартира)"
                    )

    if has_unfound:
        clarification_questions.append("Некоторые товары не найдены в каталоге. Пожалуйста, уточните артикулы или названия.")

    if has_unavailable:
        for product in validated_products:
            if not product.available:
                clarification_questions.append(
                    f"Товар «{product.name}»: в наличии только {product.stock} шт., "
                    f"вы запросили {product.quantity} шт. Подтвердите нужное количество."
                )
    
    # Объединение unfound_products
    all_unfound = list(set(unfound_products + parsed_order.unfound_products))
    
    return OrderResult(
        order_id=f"temp-{int(datetime.now(timezone.utc).timestamp())}",
        status=status,
        products=validated_products,
        customer=final_customer,
        missing_data=missing_data,
        unfound_products=all_unfound,
        clarification_questions=clarification_questions
    )


async def parse_order(message_data: Dict[str, Any]) -> Optional[OrderResult]:
    """
    Основная функция парсинга заказа.
    
    Args:
        message_data: Данные сообщения из очереди.
            Если message_data содержит 'existing_order_id' и 'clarification_context_products' —
            используется специальный промпт для обработки ответа на уточняющее письмо.
        
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
        
        # Загрузка каталога (используем кэш, обновляем только при необходимости)
        global catalog_cache, catalog_cache_time
        catalog = None
        
        # Проверка кэша в основном потоке (быстро)
        if catalog_cache and catalog_cache_time:
            elapsed = (datetime.now(timezone.utc) - catalog_cache_time).total_seconds()
            if elapsed < CATALOG_CACHE_TTL:
                catalog = catalog_cache
                logger.debug(f"Using cached catalog ({len(catalog)} items)")
        
        # Если кэш устарел или отсутствует, загружаем из БД
        if not catalog:
            try:
                catalog = await asyncio.to_thread(load_catalog_from_db, True)
            except Exception as e:
                logger.error(f"Error loading catalog: {e}", exc_info=True)
                catalog = catalog_cache or []
        
        if not catalog:
            logger.error("Catalog is empty")
            return None
        
        # Получение данных из авторизации (если есть)
        known_customer_name = message_data.get("customer_name")
        known_customer_phone = message_data.get("phone")
        known_customer_address = message_data.get("known_address")

        # Проверяем: это ответ на уточняющее письмо?
        existing_order_id = message_data.get("existing_order_id")
        clarification_context_products = message_data.get("clarification_context_products")

        catalog_json = format_catalog_for_prompt(catalog)

        if existing_order_id and clarification_context_products is not None:
            # ── Специальный промпт для уточнения заказа ──────────────────────
            logger.info(
                f"Using clarification-response prompt for order {existing_order_id}: "
                f"channel={channel}, known_name={known_customer_name}, "
                f"known_phone={known_customer_phone}, context_products={len(clarification_context_products)}"
            )
            prompt = get_clarification_response_prompt(
                catalog_json=catalog_json,
                customer_reply=customer_message,
                current_products=clarification_context_products,
                known_customer_name=known_customer_name,
                known_customer_phone=known_customer_phone,
                known_customer_address=known_customer_address
            )
        else:
            # ── Стандартный промпт для нового заказа ─────────────────────────
            logger.info(
                f"Parsing new order: channel={channel}, "
                f"known_name={known_customer_name}, known_phone={known_customer_phone}"
            )
            prompt = get_parsing_prompt(catalog_json, customer_message, known_customer_name, known_customer_phone)
        
        # Вызов OpenAI API
        client = init_openai_client()
        gpt_response = await call_openai_api(client, prompt)
        
        parsed_order = None
        
        if gpt_response:
            parsed_order = parse_gpt_response(gpt_response)
        
        # Fallback на regex парсинг (только для новых заказов, не для уточнений)
        if not parsed_order:
            if existing_order_id:
                logger.warning("GPT parsing failed for clarification response — will use context products as fallback")
                # Для уточнений не используем regex: он не знает о текущих товарах
                # Создаём минимальный ParsedOrder с данными из контекста
                ctx_products = [
                    ParsedProduct(
                        articul=p.get("articul", ""),
                        name=p.get("name", ""),
                        quantity=p.get("quantity", 1)
                    )
                    for p in (clarification_context_products or [])
                ]
                parsed_order = ParsedOrder(
                    products=ctx_products,
                    customer=ParsedCustomer(
                        name=known_customer_name,
                        phone=known_customer_phone,
                        address=known_customer_address
                    ),
                    missing_data=[],
                    unfound_products=[]
                )
            else:
                logger.warning("GPT parsing failed, using fallback regex parser")
                parsed_order = fallback_regex_parser(customer_message, catalog)
        
        # Валидация с учетом известных данных из авторизации
        result = validate_parsed_order(parsed_order, catalog, known_customer_name, known_customer_phone)
        
        logger.info(
            f"Order parsed: status={result.status}, products={len(result.products)}, "
            f"missing_data={result.missing_data}, customer_name={result.customer.name}, "
            f"customer_phone={result.customer.phone}, customer_address={result.customer.address}"
        )
        
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
