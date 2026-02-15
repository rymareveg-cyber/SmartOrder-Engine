#!/usr/bin/env python3
"""
Шаблоны промптов для AI-парсинга заказов.

Содержит шаблоны для формирования промптов для GPT-4
для извлечения товаров, контактов и других данных из заказов.
"""

from typing import List, Dict, Any, Optional


def get_parsing_prompt(catalog_json: str, customer_message: str) -> str:
    """
    Основной промпт для парсинга заказа.
    
    Args:
        catalog_json: JSON строка с каталогом товаров
        customer_message: Сообщение клиента с заказом
        
    Returns:
        Промпт для GPT-4
    """
    prompt = f"""Ты - AI-ассистент для обработки заказов в интернет-магазине.

Каталог товаров:
{catalog_json}

Сообщение клиента:
{customer_message}

Задача:
1. Найди упомянутые товары в каталоге (по названию или артикулу)
2. Извлеки количество каждого товара
3. Извлеки контактные данные: ФИО, телефон, адрес
4. Если товар не найден или данных недостаточно - укажи это

Важно:
- Используй точные артикулы из каталога
- Если количество не указано, предполагай 1
- Телефон должен быть в формате +7XXXXXXXXXX или 8XXXXXXXXXX
- Адрес должен быть полным (город, улица, дом)

Ответ в формате JSON:
{{
  "products": [
    {{
      "articul": "string",
      "name": "string",
      "quantity": int,
      "price_mentioned": float (optional)
    }}
  ],
  "customer": {{
    "name": "string or null",
    "phone": "string or null",
    "address": "string or null"
  }},
  "missing_data": ["name", "phone", "address", "full_address"],
  "unfound_products": ["название товара"]
}}

Ответь только JSON, без дополнительного текста."""
    
    return prompt


def get_clarification_questions_prompt(missing_data: List[str], order_context: Dict[str, Any]) -> str:
    """
    Генерация уточняющих вопросов для клиента.
    
    Args:
        missing_data: Список недостающих данных
        order_context: Контекст заказа (товары, частичные данные клиента)
        
    Returns:
        Промпт для генерации вопросов
    """
    missing_str = ", ".join(missing_data)
    products_str = ", ".join([p.get("name", "") for p in order_context.get("products", [])])
    
    prompt = f"""Сгенерируй вежливые уточняющие вопросы для клиента на русском языке.

Недостающие данные: {missing_str}
Товары в заказе: {products_str}

Сгенерируй вопросы в формате JSON:
{{
  "questions": [
    "Укажите, пожалуйста, ваше ФИО",
    "Укажите, пожалуйста, ваш телефон",
    ...
  ]
}}

Ответь только JSON, без дополнительного текста."""
    
    return prompt


def get_confirmation_prompt(order_data: Dict[str, Any]) -> str:
    """
    Генерация подтверждения заказа для клиента.
    
    Args:
        order_data: Данные заказа (товары, цены, контакты)
        
    Returns:
        Промпт для генерации подтверждения
    """
    products_str = "\n".join([
        f"- {p.get('name')} (артикул {p.get('articul')}) - {p.get('quantity')} шт. × {p.get('price_at_order')}₽ = {p.get('quantity') * p.get('price_at_order')}₽"
        for p in order_data.get("products", [])
    ])
    
    total = sum(p.get("quantity", 0) * p.get("price_at_order", 0) for p in order_data.get("products", []))
    delivery_cost = order_data.get("delivery_cost", 0)
    final_total = total + delivery_cost
    
    prompt = f"""Сгенерируй подтверждение заказа для клиента на русском языке.

Товары:
{products_str}

Доставка: {delivery_cost}₽
Итого: {final_total}₽

Контактные данные:
- ФИО: {order_data.get('customer', {}).get('name', 'не указано')}
- Телефон: {order_data.get('customer', {}).get('phone', 'не указан')}
- Адрес: {order_data.get('customer', {}).get('address', 'не указан')}

Сгенерируй вежливое подтверждение заказа в формате JSON:
{{
  "confirmation_message": "Ваш заказ подтверждён. ..."
}}

Ответь только JSON, без дополнительного текста."""
    
    return prompt


def format_catalog_for_prompt(products: List[Dict[str, Any]], max_items: int = 100) -> str:
    """
    Форматирование каталога товаров для промпта.
    
    Args:
        products: Список товаров из БД
        max_items: Максимальное количество товаров для включения в промпт
        
    Returns:
        JSON строка с каталогом
    """
    import json
    
    # Ограничение количества товаров для промпта
    limited_products = products[:max_items]
    
    # Формирование упрощённого формата для промпта
    catalog_data = [
        {
            "articul": p.get("articul", ""),
            "name": p.get("name", ""),
            "price": float(p.get("price", 0)),
            "stock": int(p.get("stock", 0))
        }
        for p in limited_products
    ]
    
    return json.dumps(catalog_data, ensure_ascii=False, indent=2)
