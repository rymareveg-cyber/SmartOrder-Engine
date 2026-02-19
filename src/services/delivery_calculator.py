#!/usr/bin/env python3
"""
Delivery Calculator - модуль расчёта стоимости доставки.

Рассчитывает стоимость доставки на основе города и веса товаров.
"""

import json
from typing import Dict, Any, Optional

from src.config import PROJECT_ROOT
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Правила расчёта доставки по умолчанию
DEFAULT_DELIVERY_RULES = {
    "москва": {
        "weight_ranges": [
            {"max_weight": 5.0, "cost": 500, "days": 1},
            {"max_weight": 10.0, "cost": 800, "days": 2},
            {"max_weight": float('inf'), "cost": 1200, "days": 3}
        ]
    },
    "санкт-петербург": {
        "weight_ranges": [
            {"max_weight": 5.0, "cost": 600, "days": 2},
            {"max_weight": 10.0, "cost": 900, "days": 3},
            {"max_weight": float('inf'), "cost": 1400, "days": 4}
        ]
    },
    "default": {
        "weight_ranges": [
            {"max_weight": 5.0, "cost": 1000, "days": 3},
            {"max_weight": 10.0, "cost": 1500, "days": 5},
            {"max_weight": float('inf'), "cost": 2000, "days": 7}
        ]
    }
}


def load_delivery_rules() -> Dict[str, Any]:
    """
    Загрузка правил доставки из переменной окружения или использование по умолчанию.
    
    Returns:
        Словарь с правилами доставки
    """
    import os
    delivery_rules_json = os.getenv('DELIVERY_RULES_JSON', None)
    
    if delivery_rules_json:
        try:
            rules = json.loads(delivery_rules_json)
            logger.info("Loaded delivery rules from DELIVERY_RULES_JSON")
            return rules
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse DELIVERY_RULES_JSON: {e}, using default rules")
            return DEFAULT_DELIVERY_RULES
    else:
        logger.debug("Using default delivery rules")
        return DEFAULT_DELIVERY_RULES


def normalize_city_name(city: str) -> str:
    """
    Нормализация названия города для поиска в правилах.
    
    Args:
        city: Название города
        
    Returns:
        Нормализованное название города
    """
    if not city:
        return "default"
    
    city_lower = city.lower().strip()
    city_lower = city_lower.replace("г.", "").replace("город", "").strip()
    
    # Специальные случаи
    if "москва" in city_lower or "moscow" in city_lower:
        return "москва"
    elif "санкт-петербург" in city_lower or "спб" in city_lower or "питер" in city_lower or "st. petersburg" in city_lower or "saint petersburg" in city_lower:
        return "санкт-петербург"
    
    return city_lower


def calculate_weight_from_items(items: list) -> float:
    """
    Расчёт веса на основе товаров в заказе.
    
    Args:
        items: Список товаров заказа
        
    Returns:
        Общий вес в кг
    """
    if not items:
        return 0.0
    
    total_weight = sum(item.get("quantity", 1) * 1.0 for item in items)
    logger.debug(f"Calculated weight: {total_weight} kg for {len(items)} items")
    return total_weight


class DeliveryCalculator:
    """Класс для расчёта стоимости доставки."""
    
    def __init__(self, rules: Optional[Dict[str, Any]] = None):
        """
        Инициализация калькулятора доставки.
        
        Args:
            rules: Правила доставки (опционально)
        """
        self.rules = rules or load_delivery_rules()
        logger.info("DeliveryCalculator initialized")
    
    def calculate(
        self,
        city: str,
        weight: Optional[float] = None,
        items: Optional[list] = None,
        dimensions: Optional[Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """
        Расчёт стоимости доставки.
        
        Args:
            city: Название города
            weight: Вес в кг (опционально, будет рассчитан из items если не указан)
            items: Список товаров заказа (для расчёта веса)
            dimensions: Габариты посылки (опционально, пока не используется)
            
        Returns:
            Словарь с результатами расчёта
        """
        try:
            normalized_city = normalize_city_name(city)
            
            if weight is None:
                if items:
                    weight = calculate_weight_from_items(items)
                else:
                    weight = 1.0
                    logger.warning(f"Weight not provided and no items, using default: {weight} kg")
            
            city_rules = self.rules.get(normalized_city, self.rules.get("default", {}))
            weight_ranges = city_rules.get("weight_ranges", [])
            
            if not weight_ranges:
                logger.warning(f"No weight ranges found for city '{normalized_city}', using default")
                weight_ranges = self.rules.get("default", {}).get("weight_ranges", [])
            
            cost = None
            estimated_days = None
            
            for range_def in weight_ranges:
                if weight <= range_def["max_weight"]:
                    cost = range_def["cost"]
                    estimated_days = range_def["days"]
                    break
            
            if cost is None:
                last_range = weight_ranges[-1]
                cost = last_range["cost"]
                estimated_days = last_range["days"]
            
            result = {
                "city": city,
                "weight": round(weight, 2),
                "cost": cost,
                "estimated_days": estimated_days,
                "carrier": "local"
            }
            
            logger.info(f"Delivery calculated: {city} ({weight} kg) -> {cost} RUB, {estimated_days} days")
            return result
        
        except Exception as e:
            logger.error(f"Error calculating delivery: {e}", exc_info=True)
            return {
                "city": city or "unknown",
                "weight": weight or 1.0,
                "cost": 2000,
                "estimated_days": 7,
                "carrier": "local"
            }
    
    def calculate_for_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Расчёт доставки для заказа.
        
        Args:
            order_data: Данные заказа (с customer_address и items)
            
        Returns:
            Результат расчёта доставки
        """
        city = None
        address = order_data.get("customer_address", "")
        
        if address:
            address_parts = address.split(",")
            if address_parts:
                city = address_parts[0].strip()
        
        if not city:
            city = "default"
            logger.warning("City not found in address, using default")
        
        items = order_data.get("items", [])
        return self.calculate(city=city, items=items)


# Глобальный экземпляр калькулятора
_calculator_instance: Optional[DeliveryCalculator] = None


def get_delivery_calculator() -> DeliveryCalculator:
    """
    Получение глобального экземпляра калькулятора доставки.
    
    Returns:
        Экземпляр DeliveryCalculator
    """
    global _calculator_instance
    if _calculator_instance is None:
        _calculator_instance = DeliveryCalculator()
    return _calculator_instance
