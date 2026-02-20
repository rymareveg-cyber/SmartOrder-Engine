#!/usr/bin/env python3
"""
Tracking Generator - генератор трек-номеров для заказов.

Генерирует трек-номера для заказов после оплаты и обновляет статус на "shipped".
"""

import random
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from src.utils.logger import get_logger

from src.services.order_service import OrderService, Order

logger = get_logger(__name__)


class TrackingGenerationError(Exception):
    """Исключение для ошибок генерации трек-номера."""
    pass


def generate_tracking_number() -> str:
    """
    Генерация уникального трек-номера.
    
    Формат: TRACK-{YYYYMMDD}-{6 random digits}
    
    Returns:
        Трек-номер в формате TRACK-YYYYMMDD-XXXXXX
    """
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%d")
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
            Словарь с результатом
            
        Raises:
            TrackingGenerationError: Если заказ не найден или ошибка обработки
        """
        try:
            order = OrderService.get_order(order_id)
            if not order:
                raise TrackingGenerationError(f"Order {order_id} not found")
            
            # Трек-номер генерируется после экспорта в 1С
            if order.status != "order_created_1c":
                raise TrackingGenerationError(
                    f"Order {order_id} status is '{order.status}', expected 'order_created_1c'"
                )
            
            if order.tracking_number:
                logger.warning(
                    f"Order {order_id} already has tracking number: {order.tracking_number}",
                    extra={"order_id": order_id, "existing_tracking": order.tracking_number}
                )
                return {
                    "tracking_number": order.tracking_number,
                    "order_id": order_id,
                    "order_number": order.order_number,
                    "status": order.status,
                    "shipped_at": order.shipped_at or datetime.now(timezone.utc).isoformat()
                }
            
            tracking_number = generate_tracking_number()
            
            logger.info(
                f"Generating tracking number for order {order_id}",
                extra={
                    "order_id": order_id,
                    "order_number": order.order_number,
                    "tracking_number": tracking_number
                }
            )
            
            shipped_at = datetime.now(timezone.utc)
            
            try:
                updated_order = OrderService.update_order_status(
                    order_id,
                    "tracking_issued",
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
                
                result = {
                    "tracking_number": tracking_number,
                    "order_id": order_id,
                    "order_number": order.order_number,
                    "status": "tracking_issued",
                    "shipped_at": shipped_at.isoformat()
                }
                
                # Отправка уведомления в Telegram (если есть telegram_user_id)
                if order.telegram_user_id:
                    try:
                        from src.services.telegram_bot import send_tracking_notification
                        import asyncio
                        asyncio.run(send_tracking_notification(
                            telegram_user_id=order.telegram_user_id,
                            order_number=order.order_number,
                            tracking_number=tracking_number,
                            order_id=str(order_id),
                        ))
                    except Exception as e:
                        logger.warning(f"Failed to send Telegram tracking notification: {e}")

                # Отправка трек-номера по email (для заказов из Яндекс.Почты)
                if order.channel == "yandex_mail" and order.customer_email:
                    try:
                        from src.services.email_notifier import send_tracking_email
                        send_tracking_email(
                            to_email=order.customer_email,
                            order_number=order.order_number,
                            tracking_number=tracking_number,
                            customer_name=order.customer_name
                        )
                    except Exception as e:
                        logger.warning(f"Failed to send email tracking notification: {e}")

                return result
                
            except ValueError as e:
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
