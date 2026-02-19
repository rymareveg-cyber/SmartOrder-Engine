#!/usr/bin/env python3
"""
Шаблоны промптов для AI-парсинга заказов.

Содержит шаблоны для формирования промптов для GPT-4
для извлечения товаров, контактов и других данных из заказов.
"""

from typing import List, Dict, Any, Optional


def get_parsing_prompt(catalog_json: str, customer_message: str, known_customer_name: Optional[str] = None, known_customer_phone: Optional[str] = None) -> str:
    """
    Основной промпт для парсинга заказа.
    
    Args:
        catalog_json: JSON строка с каталогом товаров
        customer_message: Сообщение клиента с заказом
        known_customer_name: Известное имя клиента (из авторизации)
        known_customer_phone: Известный телефон клиента (из авторизации)
        
    Returns:
        Промпт для GPT-4
    """
    known_data_info = ""
    if known_customer_name or known_customer_phone:
        known_data_info = "\n\nИзвестные данные клиента: "
        if known_customer_name:
            known_data_info += f"Имя: {known_customer_name}. "
        if known_customer_phone:
            known_data_info += f"Телефон: {known_customer_phone}. "
        known_data_info += "Используй эти данные, если они не указаны в сообщении."
    
    prompt = f"""Извлеки информацию о заказе из сообщения клиента.

Каталог товаров:
{catalog_json}

Сообщение клиента:
{customer_message}{known_data_info}

Задача:
1. Найди товары в каталоге по названию или артикулу (формат ФР-XXXXXXXX)
2. Извлеки количество каждого товара (по умолчанию 1)
3. Извлеки контакты: имя, телефон, адрес доставки

Верни только JSON:
{{
  "products": [
    {{
      "articul": "ФР-00000001",
      "name": "Название из каталога",
      "quantity": 2,
      "price_mentioned": 50000.0
    }}
  ],
  "customer": {{
    "name": "Иван Иванов",
    "phone": "+79991234567",
    "address": "г. Москва, ул. Ленина, д. 1, кв. 10"
  }},
  "missing_data": ["name", "phone", "address"],
  "unfound_products": ["Название товара"]
}}

Правила:
- Используй точные артикулы и названия из каталога
- Если товар не найден - добавь в unfound_products
- Если данные известны из авторизации - не добавляй в missing_data
- АДРЕС: должен содержать улицу и номер дома. Если указан только город/регион без улицы и дома - добавь "address" в missing_data и установи address: null
- Пример полного адреса: "г. Москва, ул. Ленина, д. 15, кв. 76"
- Пример неполного адреса (только город): "г. Иркутск" → address: null + "address" в missing_data
"""
    return prompt


def get_clarification_response_prompt(
    catalog_json: str,
    customer_reply: str,
    current_products: List[Dict[str, Any]],
    known_customer_name: Optional[str] = None,
    known_customer_phone: Optional[str] = None,
    known_customer_address: Optional[str] = None
) -> str:
    """
    Промпт для обработки ответа клиента на уточняющее письмо/сообщение.

    Отличие от основного промпта: AI видит ТЕКУЩИЙ список товаров в заказе
    и должен определить итоговый список с учётом слов клиента:
    «не нужно», «замените», «добавьте», «только X штук» и т.д.

    Args:
        catalog_json: Каталог товаров
        customer_reply: Ответ клиента (только новый текст, без цитат)
        current_products: Текущие товары в заказе (из БД или Redis-контекста)
        known_customer_name: Уже известное имя
        known_customer_phone: Уже известный телефон
        known_customer_address: Уже известный адрес

    Returns:
        Промпт для GPT-4
    """
    import json as _json

    current_products_text = _json.dumps(current_products, ensure_ascii=False, indent=2) if current_products else "[]"

    known_info_parts = []
    if known_customer_name:
        known_info_parts.append(f"Имя: {known_customer_name}")
    if known_customer_phone:
        known_info_parts.append(f"Телефон: {known_customer_phone}")
    if known_customer_address:
        known_info_parts.append(f"Адрес: {known_customer_address}")
    known_info = "\n".join(known_info_parts) if known_info_parts else "не указаны"

    prompt = f"""Клиент уточняет свой заказ. Обнови список товаров и данные клиента на основе его ответа.

Каталог товаров:
{catalog_json}

Текущие товары в заказе:
{current_products_text}

Уже известные данные клиента:
{known_info}

Ответ клиента:
{customer_reply}

Задача:
1. Определи ИТОГОВЫЙ список товаров:
   - Если клиент говорит "не нужно", "отмените", "уберите" — удали этот товар из списка
   - Если клиент называет новое количество — обнови его
   - Если клиент добавляет новый товар — найди в каталоге и добавь
   - Товары, которые клиент НЕ упомянул, ОСТАВЬ без изменений
2. Извлеки контактные данные из ответа (если клиент их предоставил)
3. Если товар в ответе клиента не упомянут но есть в текущем заказе — ВКЛЮЧИ его в итоговый список

Верни JSON:
{{
  "products": [
    {{
      "articul": "ФР-00000001",
      "name": "Название из каталога",
      "quantity": 2,
      "price_mentioned": null
    }}
  ],
  "customer": {{
    "name": "Иван Иванов",
    "phone": "+79991234567",
    "address": "г. Москва, ул. Ленина, д. 1, кв. 10"
  }},
  "missing_data": ["address"],
  "unfound_products": []
}}

Правила:
- Используй точные артикулы и названия из каталога
- Если товар не найден в каталоге — добавь в unfound_products
- Если данные клиента уже известны — включи их в ответ и НЕ добавляй в missing_data
- АДРЕС должен содержать улицу и номер дома; если только город — добавь "address" в missing_data
- Если список товаров стал пустым (клиент отменил всё) — включи товары из "Текущие товары в заказе", которые клиент явно НЕ отменил
"""
    return prompt


def get_clarification_questions_prompt(parsed_order: Dict[str, Any], catalog_json: str) -> str:
    """
    Промпт для генерации уточняющих вопросов.
    
    Args:
        parsed_order: Результат парсинга заказа
        catalog_json: JSON строка с каталогом товаров
        
    Returns:
        Промпт для GPT-4
    """
    prompt = f"""Сгенерируй уточняющие вопросы для клиента на основе неполных данных заказа.

Результат парсинга:
{parsed_order}

Каталог товаров:
{catalog_json}

Верни только JSON с вопросами:
{{
  "clarification_questions": [
    "Укажите, пожалуйста, ваше ФИО",
    "Укажите, пожалуйста, ваш телефон",
    "Укажите полный адрес доставки"
  ]
}}

Правила:
- Для missing_data задай вопросы о недостающих данных
- Для unfound_products предложи уточнить артикул или название
- Вопросы должны быть понятными и дружелюбными
"""
    return prompt


def format_catalog_for_prompt(catalog: List[Dict[str, Any]]) -> str:
    """
    Форматирование каталога для промпта.
    
    Args:
        catalog: Список товаров из каталога
        
    Returns:
        JSON строка с каталогом
    """
    import json
    
    # Формируем упрощенный каталог для промпта
    simplified_catalog = [
        {
            "articul": item.get("articul", ""),
            "name": item.get("name", ""),
            "price": item.get("price", 0),
            "stock": item.get("stock", 0)
        }
        for item in catalog
    ]
    
    return json.dumps(simplified_catalog, ensure_ascii=False, indent=2)
