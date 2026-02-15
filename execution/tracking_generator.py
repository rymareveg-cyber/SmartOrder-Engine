#!/usr/bin/env python3
"""
Tracking Generator - генератор трек-номеров для заказов.

Генерирует трек-номера для заказов после оплаты и обновляет статус на "shipped".
"""

import os
import logging
import random
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from dotenv import load_dotenv

# Импорты с поддержкой как относительных, так и абсолютных
try:
    from .crm_service import OrderService, Order
except ImportError:
    import sys
    from pathlib import Path as PathLib
    project_root = PathLib(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from execution.crm_service import OrderService, Order

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/tracking_generator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TrackingGenerationError(Exception):
    """Исключение для ошибок генерации трек-номера."""
    pass


def generate_tracking_number() -> str:
    """
    Генерация уникального трек-номера.
    
    Формат: TRACK-{YYYYMMDD}-{6 random digits}
    Пример: TRACK-20260213-123456
    
    Returns:
        Трек-номер в формате TRACK-YYYYMMDD-XXXXXX
    """
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%d")
    # Генерация 6 случайных цифр
    random_digits = ''.join([str(random.randint(0, 9)) for _ in range(6)])
    tracking_number = f"TRACK-{date_str}-{random_digits}"
    
    logger.debug(f"Generated tracking number: {tracking_number}")
    return tracking_number


class TrackingGenerator:
    """Класс для генерации трек-номеров заказов."""
    
    @staticmethod
    def generate_and_update(order_id: str) -> Dict[str, Any]:
        """
        Генерация трек-номера для заказа и обновление статуса на "shipped".
        
        Args:
            order_id: UUID заказа
            
        Returns:
            Словарь с результатом:
            {
                "tracking_number": str,
                "order_id": str,
                "order_number": str,
                "status": "shipped",
                "shipped_at": str  # ISO 8601
            }
            
        Raises:
            TrackingGenerationError: Если заказ не найден или ошибка обработки
        """
        try:
            # Получение заказа
            order = OrderService.get_order(order_id)
            if not order:
                raise TrackingGenerationError(f"Order {order_id} not found")
            
            # Проверка статуса заказа (должен быть paid)
            if order.status != "paid":
                raise TrackingGenerationError(
                    f"Order {order_id} status is '{order.status}', expected 'paid'"
                )
            
            # Проверка, что трек-номер ещё не сгенерирован
            if order.tracking_number:
                logger.warning(
                    f"Order {order_id} already has tracking number: {order.tracking_number}",
                    extra={"order_id": order_id, "existing_tracking": order.tracking_number}
                )
                # Возвращаем существующий трек-номер
                return {
                    "tracking_number": order.tracking_number,
                    "order_id": order_id,
                    "order_number": order.order_number,
                    "status": order.status,
                    "shipped_at": order.shipped_at or datetime.now(timezone.utc).isoformat()
                }
            
            # Генерация трек-номера
            tracking_number = generate_tracking_number()
            
            logger.info(
                f"Generating tracking number for order {order_id}",
                extra={
                    "order_id": order_id,
                    "order_number": order.order_number,
                    "tracking_number": tracking_number
                }
            )
            
            # Обновление заказа (статус → "shipped", tracking_number, shipped_at)
            shipped_at = datetime.now(timezone.utc)
            
            try:
                updated_order = OrderService.update_order_status(
                    order_id,
                    "shipped",
                    tracking_number=tracking_number,
                    shipped_at=shipped_at.isoformat()
                )
                
                if not updated_order:
                    raise TrackingGenerationError(f"Failed to update order {order_id} status")
                
                logger.info(
                    f"Tracking number generated successfully for order {order_id}",
                    extra={
                        "order_id": order_id,
                        "order_number": order.order_number,
                        "tracking_number": tracking_number,
                        "shipped_at": shipped_at.isoformat()
                    }
                )
                
                # Формирование результата
                result = {
                    "tracking_number": tracking_number,
                    "order_id": order_id,
                    "order_number": order.order_number,
                    "status": "shipped",
                    "shipped_at": shipped_at.isoformat()
                }
                
                return result
                
            except ValueError as e:
                # Ошибка валидации статуса
                logger.error(
                    f"Invalid status transition for order {order_id}: {e}",
                    extra={"order_id": order_id, "current_status": order.status}
                )
                raise TrackingGenerationError(f"Cannot generate tracking number: {e}")
            except Exception as e:
                logger.error(
                    f"Error updating order status: {e}",
                    exc_info=True,
                    extra={"order_id": order_id}
                )
                raise TrackingGenerationError(f"Failed to update order: {e}")
                
        except TrackingGenerationError:
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error generating tracking number: {e}",
                exc_info=True,
                extra={"order_id": order_id}
            )
            raise TrackingGenerationError(f"Tracking number generation failed: {e}")


# Для прямого запуска (тестирование)
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python tracking_generator.py <order_id>")
        print("Example: python tracking_generator.py 123e4567-e89b-12d3-a456-426614174000")
        sys.exit(1)
    
    order_id = sys.argv[1]
    
    try:
        result = TrackingGenerator.generate_and_update(order_id)
        print(f"Tracking number generated successfully:")
        print(f"  Tracking Number: {result['tracking_number']}")
        print(f"  Order ID: {result['order_id']}")
        print(f"  Order Number: {result['order_number']}")
        print(f"  Status: {result['status']}")
        print(f"  Shipped at: {result['shipped_at']}")
    except TrackingGenerationError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
