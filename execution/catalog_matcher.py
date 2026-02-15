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
    product_names = [p.get("name", "") for p in catalog]
    
    # Fuzzy matching с использованием rapidfuzz
    matches = process.extract(
        name_clean,
        product_names,
        scorer=fuzz.WRatio,  # Weighted Ratio - лучший для частичных совпадений
        limit=limit
    )
    
    # Формирование результатов с товарами
    results = []
    for matched_name, score, index in matches:
        if score >= 50:  # Минимальный порог релевантности
            product = catalog[index]
            results.append((product, score / 100.0))  # Нормализация к 0-1
    
    return results


def extract_articul_from_text(text: str) -> Optional[str]:
    """
    Извлечение артикула из текста (формат ФР-XXXXXXXX).
    
    Args:
        text: Текст для поиска артикула
        
    Returns:
        Найденный артикул или None
    """
    # Паттерн для артикула: ФР-XXXXXXXX (где X - цифры)
    pattern = r'ФР[-\s]?(\d{8})'
    match = re.search(pattern, text, re.IGNORECASE)
    
    if match:
        return f"ФР-{match.group(1)}"
    
    return None


def extract_quantity_from_text(text: str, product_name: Optional[str] = None) -> int:
    """
    Извлечение количества товара из текста.
    
    Args:
        text: Текст для поиска количества
        product_name: Название товара (для контекста)
        
    Returns:
        Найденное количество или 1 по умолчанию
    """
    # Поиск чисел перед названием товара или в контексте
    # Паттерны: "2 панели", "количество 3", "3 шт", "3 штуки"
    patterns = [
        r'(\d+)\s*(?:шт|штук|шт\.|штуки|шток)',
        r'количество[:\s]+(\d+)',
        r'(\d+)\s+(?:панел|шуба|товар)',
        r'(\d+)\s+(?:штук|шт)',
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
    
    # Если не найдено, ищем просто числа в начале предложения
    match = re.search(r'^(\d+)', text.strip())
    if match:
        try:
            quantity = int(match.group(1))
            if 0 < quantity <= 100:  # Разумные пределы
                return quantity
        except ValueError:
            pass
    
    return 1  # По умолчанию 1


def match_products_from_text(
    text: str,
    catalog: List[Dict[str, Any]],
    max_results: int = 5
) -> List[Tuple[Dict[str, Any], float, int]]:
    """
    Сопоставление товаров из текста с каталогом.
    
    Args:
        text: Текст с упоминанием товаров
        catalog: Список товаров из каталога
        max_results: Максимальное количество результатов для каждого товара
        
    Returns:
        Список кортежей (товар, релевантность, количество)
    """
    results = []
    
    # Попытка извлечь артикул
    articul = extract_articul_from_text(text)
    if articul:
        product = find_by_articul(articul, catalog)
        if product:
            quantity = extract_quantity_from_text(text)
            results.append((product, 1.0, quantity))
            return results
    
    # Поиск по названию (fuzzy matching)
    # Разбиваем текст на слова и ищем товары
    words = re.findall(r'\b\w+\b', text.lower())
    
    # Пробуем найти товары по ключевым словам
    for word in words:
        if len(word) < 3:  # Пропускаем короткие слова
            continue
        
        matches = find_by_name_fuzzy(word, catalog, limit=max_results)
        for product, relevance in matches:
            # Проверяем, не добавлен ли уже этот товар
            if not any(r[0].get("articul") == product.get("articul") for r in results):
                quantity = extract_quantity_from_text(text, product.get("name"))
                results.append((product, relevance, quantity))
    
    # Если ничего не найдено, пробуем поиск по всему тексту
    if not results:
        # Ищем длинные фразы (2-3 слова)
        phrases = []
        for i in range(len(words) - 1):
            phrase = f"{words[i]} {words[i+1]}"
            if len(phrase) >= 5:
                phrases.append(phrase)
        
        for phrase in phrases:
            matches = find_by_name_fuzzy(phrase, catalog, limit=3)
            for product, relevance in matches:
                if not any(r[0].get("articul") == product.get("articul") for r in results):
                    quantity = extract_quantity_from_text(text, product.get("name"))
                    results.append((product, relevance, quantity))
                    if len(results) >= max_results:
                        break
            if len(results) >= max_results:
                break
    
    # Сортировка по релевантности
    results.sort(key=lambda x: x[1], reverse=True)
    
    return results[:max_results]


def validate_product_availability(product: Dict[str, Any], quantity: int) -> Dict[str, Any]:
    """
    Валидация доступности товара.
    
    Args:
        product: Товар из каталога
        quantity: Запрошенное количество
        
    Returns:
        Словарь с результатами валидации
    """
    stock = int(product.get("stock", 0))
    available = stock >= quantity
    
    return {
        "available": available,
        "stock": stock,
        "requested_quantity": quantity,
        "can_fulfill": available,
        "message": f"В наличии {stock} шт." if available else f"В наличии только {stock} шт., запрошено {quantity} шт."
    }
