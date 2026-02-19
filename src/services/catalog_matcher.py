#!/usr/bin/env python3
"""
Модуль сопоставления товаров из сообщений с каталогом.

Использует fuzzy matching для поиска товаров по названию и артикулу.
"""

import re
from typing import List, Dict, Any, Optional, Tuple
from rapidfuzz import fuzz, process


def find_by_articul(articul: str, catalog: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Поиск товара по артикулу (точное совпадение).
    
    Args:
        articul: Артикул для поиска
        catalog: Список товаров из каталога
        
    Returns:
        Товар из каталога или None
    """
    articul_clean = articul.strip().upper()
    
    for product in catalog:
        if product.get("articul", "").strip().upper() == articul_clean:
            return product
    
    return None


def find_by_name_fuzzy(name: str, catalog: List[Dict[str, Any]], limit: int = 5) -> List[Tuple[Dict[str, Any], float]]:
    """
    Поиск товаров по названию с использованием fuzzy matching.
    
    Args:
        name: Название товара для поиска
        catalog: Список товаров из каталога
        limit: Максимальное количество результатов
        
    Returns:
        Список кортежей (товар, релевантность) отсортированных по релевантности
    """
    if not name or not catalog:
        return []
    
    name_clean = name.strip().lower()
    
    # Извлечение названий товаров
    product_names = [item.get("name", "") for item in catalog]
    
    # Fuzzy matching
    matches = process.extract(
        name_clean,
        product_names,
        scorer=fuzz.WRatio,
        limit=limit
    )
    
    # Формирование результата с товарами
    results = []
    for match_name, score, _ in matches:
        # Поиск товара по названию
        for product in catalog:
            if product.get("name", "") == match_name:
                results.append((product, score / 100.0))  # Нормализация к 0-1
                break
    
    return results


def extract_articul_from_text(text: str) -> Optional[str]:
    """
    Извлечение артикула из текста (формат ФР-XXXXXXXX).
    
    Args:
        text: Текст для поиска артикула
        
    Returns:
        Артикул или None
    """
    # Паттерн для артикула: ФР-XXXXXXXX (8 цифр)
    pattern = r'ФР-?\s*(\d{8})'
    match = re.search(pattern, text, re.IGNORECASE)
    
    if match:
        return f"ФР-{match.group(1)}"
    
    return None


def extract_quantity_from_text(text: str, product_name: Optional[str] = None) -> int:
    """
    Извлечение количества из текста.
    
    Args:
        text: Текст для поиска количества
        product_name: Название товара (для контекста)
        
    Returns:
        Количество (по умолчанию 1)
    """
    # Паттерны для количества
    patterns = [
        r'(\d+)\s*(?:шт|штук|шт\.|x|\*|×)',
        r'(?:шт|штук|шт\.|x|\*|×)\s*(\d+)',
        r'количество[:\s]+(\d+)',
        r'кол-во[:\s]+(\d+)',
        r'(\d+)\s*(?:единиц|ед\.)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                quantity = int(match.group(1))
                if quantity > 0:
                    return quantity
            except ValueError:
                continue
    
    # Если не найдено, ищем просто число рядом с названием товара
    if product_name:
        # Ищем число перед или после названия товара
        name_pattern = re.escape(product_name)
        number_pattern = r'(\d+)'
        combined_pattern = f'{number_pattern}\\s*{name_pattern}|{name_pattern}\\s*{number_pattern}'
        match = re.search(combined_pattern, text, re.IGNORECASE)
        if match:
            try:
                quantity = int(match.group(1))
                if quantity > 0:
                    return quantity
            except ValueError:
                pass
    
    return 1  # По умолчанию


def match_products_from_text(
    text: str,
    catalog: List[Dict[str, Any]],
    max_results: int = 10
) -> List[Tuple[Dict[str, Any], float, int]]:
    """
    Поиск товаров в тексте с использованием fuzzy matching.
    
    Args:
        text: Текст для поиска товаров
        catalog: Список товаров из каталога
        max_results: Максимальное количество результатов
        
    Returns:
        Список кортежей (товар, релевантность, количество)
    """
    results = []
    
    # Сначала ищем по артикулам
    articul = extract_articul_from_text(text)
    if articul:
        product = find_by_articul(articul, catalog)
        if product:
            quantity = extract_quantity_from_text(text, product.get("name"))
            results.append((product, 1.0, quantity))
    
    # Затем ищем по названиям (fuzzy matching)
    # Извлекаем ключевые слова из текста
    words = re.findall(r'\b[А-Яа-яA-Za-z]{3,}\b', text)
    
    for word in words:
        if len(word) < 3:
            continue
        
        matches = find_by_name_fuzzy(word, catalog, limit=3)
        for product, relevance in matches:
            # Проверяем, не добавлен ли уже этот товар
            if not any(p.get("articul") == product.get("articul") for p, _, _ in results):
                quantity = extract_quantity_from_text(text, product.get("name"))
                results.append((product, relevance, quantity))
    
    # Сортировка по релевантности
    results.sort(key=lambda x: x[1], reverse=True)
    
    return results[:max_results]


def validate_product_availability(product: Dict[str, Any], requested_quantity: int) -> Dict[str, Any]:
    """
    Валидация доступности товара.
    
    Args:
        product: Товар из каталога
        requested_quantity: Запрошенное количество
        
    Returns:
        Словарь с результатами валидации:
        {
            "available": bool,
            "stock": int,
            "message": str
        }
    """
    stock = product.get("stock", 0)
    available = stock >= requested_quantity
    
    message = ""
    if not available:
        if stock == 0:
            message = "Товар отсутствует на складе"
        else:
            message = f"В наличии только {stock} шт., запрошено {requested_quantity} шт."
    
    return {
        "available": available,
        "stock": stock,
        "message": message
    }
