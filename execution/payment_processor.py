#!/usr/bin/env python3
"""
Payment Processor - обработчик оплаты (fake карта).

Обрабатывает оплату заказов с использованием fake карты (симуляция).
Всегда успешная оплата для тестирования системы.
"""

import os
import logging
import re
import time
import uuid
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
        logging.FileHandler('logs/payment_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PaymentValidationError(Exception):
    """Исключение для ошибок валидации данных карты."""
    pass


class PaymentProcessingError(Exception):
    """Исключение для ошибок обработки оплаты."""
    pass


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
            # Примечание: transaction_id можно сохранить в отдельной таблице или в JSON поле
            # Для простоты, сохраняем в комментарии или создаём отдельное поле
            # Пока используем update_order_status, который обновляет paid_at автоматически
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
                try:
                    # Импорт с поддержкой как относительных, так и абсолютных
                    # Используем importlib для модуля с числом в имени (1c_exporter)
                    import sys
                    import importlib.util
                    from pathlib import Path as PathLib
                    
                    # Попытка импорта через importlib
                    exporter_path = PathLib(__file__).parent / "1c_exporter.py"
                    if exporter_path.exists():
                        spec = importlib.util.spec_from_file_location(
                            "onec_exporter",
                            exporter_path
                        )
                        onec_exporter = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(onec_exporter)
                        OneCExporter = onec_exporter.OneCExporter
                        OneCExportError = onec_exporter.OneCExportError
                    else:
                        # Fallback: добавление в sys.path и импорт
                        project_root = PathLib(__file__).parent.parent
                        if str(project_root) not in sys.path:
                            sys.path.insert(0, str(project_root))
                        spec = importlib.util.spec_from_file_location(
                            "onec_exporter",
                            PathLib(project_root) / "execution" / "1c_exporter.py"
                        )
                        onec_exporter = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(onec_exporter)
                        OneCExporter = onec_exporter.OneCExporter
                        OneCExportError = onec_exporter.OneCExportError
                    
                    export_result = OneCExporter.export_invoice(order_id)
                    result["invoice_exported"] = export_result["exported"]
                    result["invoice_number"] = export_result.get("invoice_number")
                    
                    logger.info(
                        f"Invoice exported automatically to 1C for order {order_id}",
                        extra={
                            "order_id": order_id,
                            "invoice_number": export_result.get("invoice_number"),
                            "exported": export_result["exported"]
                        }
                    )
                except OneCExportError as e:
                    # Не критичная ошибка - логируем, но не прерываем процесс оплаты
                    logger.warning(
                        f"Failed to export invoice to 1C automatically for order {order_id}: {e}",
                        extra={"order_id": order_id, "error": str(e)}
                    )
                except Exception as e:
                    # Не критичная ошибка - логируем, но не прерываем процесс оплаты
                    logger.warning(
                        f"Unexpected error exporting invoice to 1C for order {order_id}: {e}",
                        exc_info=True,
                        extra={"order_id": order_id}
                    )
                
                # Автоматическая генерация трек-номера после успешной оплаты
                try:
                    # Импорт с поддержкой как относительных, так и абсолютных
                    try:
                        from .tracking_generator import TrackingGenerator
                    except ImportError:
                        import sys
                        from pathlib import Path as PathLib
                        project_root = PathLib(__file__).parent.parent
                        if str(project_root) not in sys.path:
                            sys.path.insert(0, str(project_root))
                        from execution.tracking_generator import TrackingGenerator
                    
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
                    # Не критичная ошибка - логируем, но не прерываем процесс оплаты
                    logger.warning(
                        f"Failed to generate tracking number automatically for order {order_id}: {e}",
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


# Для прямого запуска (тестирование)
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
