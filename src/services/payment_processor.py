#!/usr/bin/env python3
"""
Payment Processor - обработчик оплаты (fake карта).

Обрабатывает оплату заказов с использованием fake карты (симуляция).
Всегда успешная оплата для тестирования системы.
"""

import re
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from src.services.order_service import OrderService, Order
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentValidationError(Exception):
    """Исключение для ошибок валидации данных карты."""
    pass


class PaymentProcessingError(Exception):
    """Исключение для ошибок обработки оплаты."""
    pass


def _notify_admin_async(order_number: str, error: str, header: str) -> None:
    """Отправка уведомления администратору (fire-and-forget, не блокирует)."""
    try:
        import asyncio
        from src.services.telegram_bot import send_admin_notification
        msg = f"{header}\n\nЗаказ: {order_number}\nОшибка: {error}"
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(send_admin_notification(msg))
        except RuntimeError:
            asyncio.run(send_admin_notification(msg))
    except Exception as e:
        logger.warning(f"Failed to notify admin: {e}")


def validate_card_number(card_number: str) -> bool:
    """
    Валидация номера карты (формат: 16 цифр).
    
    Args:
        card_number: Номер карты
        
    Returns:
        True если валиден, False иначе
    """
    # Удаляем пробелы и дефисы
    cleaned = re.sub(r'[\s-]', '', card_number)
    # Проверяем, что это 16 цифр
    return bool(re.match(r'^\d{16}$', cleaned))


def validate_cvv(cvv: str) -> bool:
    """
    Валидация CVV (формат: 3 цифры).
    
    Args:
        cvv: CVV код
        
    Returns:
        True если валиден, False иначе
    """
    return bool(re.match(r'^\d{3}$', cvv))


def validate_expiry(expiry: str) -> bool:
    """
    Валидация срока действия карты (формат: MM/YY).
    
    Args:
        expiry: Срок действия в формате MM/YY
        
    Returns:
        True если валиден, False иначе
    """
    if not re.match(r'^\d{2}/\d{2}$', expiry):
        return False
    
    try:
        month, year = expiry.split('/')
        month_int = int(month)
        year_int = int(year)
        
        # Проверка месяца (1-12)
        if month_int < 1 or month_int > 12:
            return False
        
        # Проверка года (текущий год или будущий)
        current_year = datetime.now().year % 100
        if year_int < current_year:
            return False
        
        return True
    except ValueError:
        return False


def validate_card_data(card_data: Dict[str, Any]) -> None:
    """
    Валидация всех данных карты.
    
    Args:
        card_data: Словарь с данными карты:
            {
                "number": str,  # 16 цифр
                "cvv": str,     # 3 цифры
                "expiry": str,  # MM/YY
                "holder_name": str  # Имя держателя
            }
            
    Raises:
        PaymentValidationError: Если данные невалидны
    """
    errors = []
    
    # Проверка обязательных полей
    if "number" not in card_data or not card_data["number"]:
        errors.append("Card number is required")
    elif not validate_card_number(card_data["number"]):
        errors.append("Card number must be 16 digits")
    
    if "cvv" not in card_data or not card_data["cvv"]:
        errors.append("CVV is required")
    elif not validate_cvv(card_data["cvv"]):
        errors.append("CVV must be 3 digits")
    
    if "expiry" not in card_data or not card_data["expiry"]:
        errors.append("Expiry date is required")
    elif not validate_expiry(card_data["expiry"]):
        errors.append("Expiry date must be in MM/YY format and valid")
    
    if "holder_name" not in card_data or not card_data["holder_name"]:
        errors.append("Holder name is required")
    elif len(card_data["holder_name"].strip()) < 2:
        errors.append("Holder name must be at least 2 characters")
    
    if errors:
        raise PaymentValidationError("; ".join(errors))


def generate_transaction_id() -> str:
    """
    Генерация уникального transaction_id.
    
    Returns:
        Transaction ID в формате TXN-YYYYMMDD-HHMMSS-{6 random digits}
    """
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%d")
    time_str = now.strftime("%H%M%S")
    random_suffix = str(uuid.uuid4().int % 1000000).zfill(6)
    return f"TXN-{date_str}-{time_str}-{random_suffix}"


class PaymentProcessor:
    """Класс для обработки оплаты заказов."""
    
    @staticmethod
    def process_payment(order_id: str, card_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обработка оплаты заказа (fake система).
        
        Args:
            order_id: UUID заказа
            card_data: Данные карты:
                {
                    "number": str,      # 16 цифр
                    "cvv": str,         # 3 цифры
                    "expiry": str,      # MM/YY
                    "holder_name": str  # Имя держателя
                }
                
        Returns:
            Словарь с результатом оплаты:
            {
                "transaction_id": str,
                "order_id": str,
                "status": "success",
                "amount": float,
                "paid_at": str,  # ISO 8601
                "card_last4": str  # Последние 4 цифры карты
            }
            
        Raises:
            PaymentValidationError: Если данные карты невалидны
            PaymentProcessingError: Если заказ не найден или ошибка обработки
        """
        try:
            # Валидация данных карты
            validate_card_data(card_data)
            
            # Получение заказа
            order = OrderService.get_order(order_id)
            if not order:
                raise PaymentProcessingError(f"Order {order_id} not found")
            
            # Проверка статуса заказа (должен быть invoice_created)
            if order.status != "invoice_created":
                raise PaymentProcessingError(
                    f"Order {order_id} status is '{order.status}', expected 'invoice_created'"
                )
            
            # Безопасное логирование (без CVV!)
            card_number = card_data["number"].replace(" ", "").replace("-", "")
            card_last4 = card_number[-4:] if len(card_number) >= 4 else "****"
            
            logger.info(
                f"Processing payment for order {order_id}",
                extra={
                    "order_id": order_id,
                    "order_number": order.order_number,
                    "amount": float(order.total_amount),
                    "card_last4": card_last4,
                    "holder_name": card_data["holder_name"],
                    "expiry": card_data["expiry"]
                    # CVV НЕ логируется!
                }
            )
            
            # Симуляция обработки (задержка 1-2 секунды)
            import random
            delay = random.uniform(1.0, 2.0)
            time.sleep(delay)
            
            # Генерация transaction_id
            transaction_id = generate_transaction_id()
            
            # Обновление заказа (статус → "paid", paid_at, transaction_id)
            paid_at = datetime.now(timezone.utc)
            
            try:
                updated_order = OrderService.update_order_status(
                    order_id,
                    "paid",
                    paid_at=paid_at.isoformat(),
                    transaction_id=transaction_id
                )
                
                if not updated_order:
                    raise PaymentProcessingError(f"Failed to update order {order_id} status")
                
                logger.info(
                    f"Payment processed successfully for order {order_id}",
                    extra={
                        "order_id": order_id,
                        "order_number": order.order_number,
                        "transaction_id": transaction_id,
                        "amount": float(order.total_amount),
                        "paid_at": paid_at.isoformat()
                    }
                )
                
                # Формирование результата
                result = {
                    "transaction_id": transaction_id,
                    "order_id": order_id,
                    "order_number": order.order_number,
                    "status": "success",
                    "amount": float(order.total_amount),
                    "paid_at": paid_at.isoformat(),
                    "card_last4": card_last4
                }
                
                # Автоматический экспорт счёта в 1С после успешной оплаты
                onec_ok = False
                try:
                    from src.services.onec_exporter import OneCExporter, OneCExportError

                    export_result = OneCExporter.export_invoice(order_id)
                    result["invoice_exported"] = export_result["exported"]
                    result["invoice_number"] = export_result.get("invoice_number")
                    onec_ok = True

                    logger.info(
                        f"Invoice exported automatically to 1C for order {order_id}",
                        extra={
                            "order_id": order_id,
                            "invoice_number": export_result.get("invoice_number"),
                        }
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to export invoice to 1C for order {order_id}: {e}",
                        extra={"order_id": order_id}
                    )
                    # Уведомить администратора
                    _notify_admin_async(
                        order.order_number, str(e),
                        "⚠️ Ошибка экспорта в 1С — требуется ручной экспорт."
                    )

                # Генерация трек-номера — только если 1С экспорт прошёл успешно
                # (статус должен быть 'order_created_1c')
                if onec_ok:
                    try:
                        from src.services.tracking_generator import TrackingGenerator

                        tracking_result = TrackingGenerator.generate_and_update(order_id)
                        result["tracking_number"] = tracking_result["tracking_number"]
                        result["shipped_at"] = tracking_result["shipped_at"]

                        logger.info(
                            f"Tracking number generated automatically for order {order_id}",
                            extra={
                                "order_id": order_id,
                                "tracking_number": tracking_result["tracking_number"]
                            }
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to generate tracking number for order {order_id}: {e}",
                            extra={"order_id": order_id}
                        )
                        _notify_admin_async(
                            order.order_number, str(e),
                            "⚠️ Ошибка генерации трек-номера — требуется ручная генерация."
                        )
                else:
                    logger.warning(
                        f"Skipping tracking generation for order {order_id}: 1C export did not succeed",
                        extra={"order_id": order_id}
                    )
                
                return result
                
            except ValueError as e:
                # Ошибка валидации статуса
                logger.error(
                    f"Invalid status transition for order {order_id}: {e}",
                    extra={"order_id": order_id, "current_status": order.status}
                )
                raise PaymentProcessingError(f"Cannot process payment: {e}")
            except Exception as e:
                logger.error(
                    f"Error updating order status: {e}",
                    exc_info=True,
                    extra={"order_id": order_id}
                )
                raise PaymentProcessingError(f"Failed to update order: {e}")
                
        except PaymentValidationError:
            raise
        except PaymentProcessingError:
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error processing payment: {e}",
                exc_info=True,
                extra={"order_id": order_id}
            )
            raise PaymentProcessingError(f"Payment processing failed: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python payment_processor.py <order_id>")
        print("Example: python payment_processor.py 123e4567-e89b-12d3-a456-426614174000")
        sys.exit(1)
    
    order_id = sys.argv[1]
    
    # Тестовые данные карты
    test_card_data = {
        "number": "4111111111111111",
        "cvv": "123",
        "expiry": "12/25",
        "holder_name": "IVAN IVANOV"
    }
    
    try:
        result = PaymentProcessor.process_payment(order_id, test_card_data)
        print(f"Payment processed successfully:")
        print(f"  Transaction ID: {result['transaction_id']}")
        print(f"  Order ID: {result['order_id']}")
        print(f"  Amount: {result['amount']}")
        print(f"  Paid at: {result['paid_at']}")
    except PaymentValidationError as e:
        print(f"Validation error: {e}")
        sys.exit(1)
    except PaymentProcessingError as e:
        print(f"Processing error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
