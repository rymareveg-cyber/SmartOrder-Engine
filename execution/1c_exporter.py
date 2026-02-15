#!/usr/bin/env python3
"""
1C Exporter - модуль экспорта счетов в 1С:Управление нашей фирмой.

Экспортирует счета на оплату в 1С через HTTP Service после успешной оплаты заказа.
"""

import os
import logging
import time
import base64
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
        logging.FileHandler('logs/1c_exporter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Переменные окружения
ONEC_BASE_URL = os.getenv('ONEC_BASE_URL', 'http://localhost:80')
ONEC_USERNAME = os.getenv('ONEC_USERNAME', '')
ONEC_PASSWORD = os.getenv('ONEC_PASSWORD', '')
ONEC_INVOICES_ENDPOINT = os.getenv('ONEC_INVOICES_ENDPOINT', '/hs/invoices')

# Настройки retry
MAX_RETRIES = 3
RETRY_DELAYS = [2, 5, 10]  # секунды между попытками
REQUEST_TIMEOUT = 60  # секунд


class OneCExportError(Exception):
    """Исключение для ошибок экспорта в 1С."""
    pass


def create_1c_auth_header(username: str, password: str) -> str:
    """
    Создание заголовка Authorization для Basic Auth с поддержкой Unicode.
    
    Args:
        username: Имя пользователя
        password: Пароль
        
    Returns:
        Значение заголовка Authorization
    """
    credentials = f"{username}:{password}"
    # Кодирование в UTF-8, затем base64
    encoded = base64.b64encode(credentials.encode('utf-8')).decode('ascii')
    return f"Basic {encoded}"


def format_invoice_for_1c(order: Order) -> Dict[str, Any]:
    """
    Формирование данных счёта для экспорта в 1С.
    
    Args:
        order: Заказ из БД
        
    Returns:
        Словарь с данными счёта в формате 1С
    """
    # Получение номера счёта из PDF (если был сгенерирован)
    # Или генерация на основе номера заказа
    invoice_number = f"INV-{order.order_number.replace('ORD-', '')}"
    
    # Форматирование даты
    order_date = datetime.fromisoformat(order.created_at.replace('Z', '+00:00'))
    invoice_date = order_date.strftime("%Y-%m-%d")
    
    # Формирование данных клиента
    customer = {
        "name": order.customer_name or "",
        "phone": order.customer_phone or "",
        "address": order.customer_address or ""
    }
    
    # Формирование позиций счёта
    items = []
    for item in order.items:
        items.append({
            "articul": item.product_articul,
            "name": item.product_name,
            "quantity": item.quantity,
            "price": float(item.price_at_order),
            "total": float(item.total)
        })
    
    # Формирование итогового JSON
    invoice_data = {
        "invoice_number": invoice_number,
        "date": invoice_date,
        "customer": customer,
        "items": items,
        "delivery_cost": float(order.delivery_cost),
        "total": float(order.total_amount)
    }
    
    return invoice_data


def send_invoice_to_1c(invoice_data: Dict[str, Any], attempt: int = 1) -> Dict[str, Any]:
    """
    Отправка счёта в 1С через HTTP Service.
    
    Args:
        invoice_data: Данные счёта для экспорта
        attempt: Номер попытки (для retry логики)
        
    Returns:
        Ответ от 1С
        
    Raises:
        OneCExportError: При ошибке экспорта
    """
    url = f"{ONEC_BASE_URL}{ONEC_INVOICES_ENDPOINT}"
    
    # Создание заголовков
    headers = {
        "Content-Type": "application/json",
        "Authorization": create_1c_auth_header(ONEC_USERNAME, ONEC_PASSWORD)
    }
    
    logger.info(
        f"Sending invoice to 1C (attempt {attempt}/{MAX_RETRIES})",
        extra={
            "url": url,
            "invoice_number": invoice_data.get("invoice_number"),
            "attempt": attempt
        }
    )
    
    try:
        # Настройка retry для requests
        session = requests.Session()
        retry_strategy = Retry(
            total=1,  # Управляем retry вручную
            backoff_factor=0,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Отправка запроса
        response = session.post(
            url,
            json=invoice_data,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        
        # Логирование ответа
        logger.info(
            f"1C response (attempt {attempt}): status={response.status_code}",
            extra={
                "url": url,
                "invoice_number": invoice_data.get("invoice_number"),
                "status_code": response.status_code,
                "response_text": response.text[:500] if response.text else None
            }
        )
        
        # Проверка статуса ответа
        if response.status_code == 200:
            try:
                result = response.json()
                
                # ВАЖНО: Проверка на наличие ошибки в ответе от 1С
                if isinstance(result, dict):
                    # Проверка поля "error"
                    if "error" in result:
                        error_msg = result.get("error", "Unknown error from 1C")
                        logger.error(
                            f"1C returned error in response (HTTP 200): {error_msg}",
                            extra={
                                "invoice_number": invoice_data.get("invoice_number"),
                                "1c_response": result,
                                "status_code": response.status_code
                            }
                        )
                        raise OneCExportError(
                            f"1C processing error: {error_msg}"
                        )
                    
                    # Проверка поля "success": false
                    if result.get("success") is False:
                        error_msg = result.get("message") or result.get("error") or "Unknown error from 1C"
                        logger.error(
                            f"1C returned unsuccessful response (HTTP 200): {error_msg}",
                            extra={
                                "invoice_number": invoice_data.get("invoice_number"),
                                "1c_response": result,
                                "status_code": response.status_code
                            }
                        )
                        raise OneCExportError(
                            f"1C processing error: {error_msg}"
                        )
                
                # Только если нет ошибок - логируем успех
                logger.info(
                    f"Invoice exported successfully to 1C",
                    extra={
                        "invoice_number": invoice_data.get("invoice_number"),
                        "1c_response": result
                    }
                )
                return result
            except ValueError:
                # Если ответ не JSON, но статус 200 - считаем успешным
                logger.warning(
                    f"1C returned 200 but non-JSON response: {response.text[:200]}",
                    extra={
                        "invoice_number": invoice_data.get("invoice_number"),
                        "response_text": response.text[:500]
                    }
                )
                return {"status": "success", "message": response.text[:200]}
        elif response.status_code in [404, 401, 403]:
            # Постоянные ошибки конфигурации - не делать retry
            error_msg = f"1C configuration error: {response.status_code}"
            if response.text:
                error_msg += f" - {response.text[:500]}"
            logger.error(
                f"{error_msg}. URL: {url}",
                extra={
                    "url": url,
                    "invoice_number": invoice_data.get("invoice_number"),
                    "status_code": response.status_code,
                    "response_text": response.text[:1000] if response.text else None
                }
            )
            raise OneCExportError(error_msg)
        elif response.status_code in [500, 502, 503, 504]:
            # Временные ошибки - можно повторить
            raise OneCExportError(
                f"Temporary 1C error: {response.status_code} - {response.text[:200]}"
            )
        else:
            # Другие постоянные ошибки
            raise OneCExportError(
                f"1C error: {response.status_code} - {response.text[:200]}"
            )
            
    except requests.exceptions.Timeout:
        logger.error(
            f"Timeout while sending invoice to 1C (attempt {attempt})",
            extra={
                "url": url,
                "invoice_number": invoice_data.get("invoice_number"),
                "timeout": REQUEST_TIMEOUT
            }
        )
        raise OneCExportError(f"Timeout while connecting to 1C (>{REQUEST_TIMEOUT}s)")
    except requests.exceptions.ConnectionError as e:
        logger.error(
            f"Connection error while sending invoice to 1C (attempt {attempt}): {e}",
            extra={
                "url": url,
                "invoice_number": invoice_data.get("invoice_number")
            }
        )
        raise OneCExportError(f"Connection error: {e}")
    except OneCExportError:
        raise
    except Exception as e:
        logger.error(
            f"Unexpected error while sending invoice to 1C (attempt {attempt}): {e}",
            exc_info=True,
            extra={
                "url": url,
                "invoice_number": invoice_data.get("invoice_number")
            }
        )
        raise OneCExportError(f"Unexpected error: {e}")


def update_invoice_exported_flag(order_id: str, exported: bool) -> None:
    """
    Обновление флага invoice_exported_to_1c в БД.
    
    Args:
        order_id: UUID заказа
        exported: Флаг экспорта (True/False)
    """
    try:
        conn = None
        try:
            from execution.crm_service import get_db_connection, return_db_connection
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE orders
                SET invoice_exported_to_1c = %s
                WHERE id = %s
            """, (exported, order_id))
            
            conn.commit()
            logger.info(
                f"Invoice export flag updated for order {order_id}: {exported}",
                extra={"order_id": order_id, "exported": exported}
            )
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(
                f"Error updating invoice export flag: {e}",
                exc_info=True,
                extra={"order_id": order_id}
            )
            raise
        finally:
            if conn:
                cursor.close()
                return_db_connection(conn)
    except ImportError:
        # Если не можем импортировать, логируем ошибку
        logger.warning(
            f"Cannot update invoice export flag: crm_service not available",
            extra={"order_id": order_id}
        )


class OneCExporter:
    """Класс для экспорта счетов в 1С."""
    
    @staticmethod
    def export_invoice(order_id: str) -> Dict[str, Any]:
        """
        Экспорт счёта в 1С.
        
        Args:
            order_id: UUID заказа
            
        Returns:
            Словарь с результатом экспорта:
            {
                "order_id": str,
                "invoice_number": str,
                "exported": bool,
                "1c_response": dict,
                "exported_at": str  # ISO 8601
            }
            
        Raises:
            OneCExportError: При ошибке экспорта
        """
        try:
            # Получение заказа
            order = OrderService.get_order(order_id)
            if not order:
                raise OneCExportError(f"Order {order_id} not found")
            
            # Проверка статуса заказа (должен быть paid)
            if order.status != "paid":
                raise OneCExportError(
                    f"Order {order_id} status is '{order.status}', expected 'paid'"
                )
            
            # Проверка, не экспортирован ли уже счёт
            if order.invoice_exported_to_1c:
                logger.info(
                    f"Invoice for order {order_id} already exported to 1C",
                    extra={"order_id": order_id, "order_number": order.order_number}
                )
                return {
                    "order_id": order_id,
                    "invoice_number": f"INV-{order.order_number.replace('ORD-', '')}",
                    "exported": True,
                    "already_exported": True,
                    "exported_at": datetime.now(timezone.utc).isoformat()
                }
            
            # Формирование данных счёта
            invoice_data = format_invoice_for_1c(order)
            
            logger.info(
                f"Exporting invoice to 1C for order {order_id}",
                extra={
                    "order_id": order_id,
                    "order_number": order.order_number,
                    "invoice_number": invoice_data.get("invoice_number"),
                    "items_count": len(invoice_data.get("items", [])),
                    "total": invoice_data.get("total")
                }
            )
            
            # Retry логика
            last_error = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    # Отправка в 1С
                    response = send_invoice_to_1c(invoice_data, attempt)
                    
                    # Обновление флага в БД
                    update_invoice_exported_flag(order_id, True)
                    
                    exported_at = datetime.now(timezone.utc)
                    
                    logger.info(
                        f"Invoice exported successfully to 1C",
                        extra={
                            "order_id": order_id,
                            "order_number": order.order_number,
                            "invoice_number": invoice_data.get("invoice_number"),
                            "attempt": attempt,
                            "exported_at": exported_at.isoformat()
                        }
                    )
                    
                    return {
                        "order_id": order_id,
                        "order_number": order.order_number,
                        "invoice_number": invoice_data.get("invoice_number"),
                        "exported": True,
                        "1c_response": response,
                        "exported_at": exported_at.isoformat()
                    }
                    
                except OneCExportError as e:
                    last_error = e
                    if attempt < MAX_RETRIES:
                        delay = RETRY_DELAYS[attempt - 1]
                        logger.warning(
                            f"Export attempt {attempt} failed, retrying in {delay}s: {e}",
                            extra={
                                "order_id": order_id,
                                "attempt": attempt,
                                "error": str(e)
                            }
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"All export attempts failed for order {order_id}",
                            extra={
                                "order_id": order_id,
                                "order_number": order.order_number,
                                "invoice_number": invoice_data.get("invoice_number"),
                                "max_retries": MAX_RETRIES,
                                "last_error": str(e)
                            }
                        )
            
            # Все попытки исчерпаны - помечаем как неуспешный экспорт
            # (можно добавить в dead letter queue)
            logger.error(
                f"Failed to export invoice to 1C after {MAX_RETRIES} attempts",
                extra={
                    "order_id": order_id,
                    "order_number": order.order_number,
                    "invoice_number": invoice_data.get("invoice_number"),
                    "error": str(last_error)
                }
            )
            
            # Не обновляем флаг invoice_exported_to_1c при ошибке
            raise OneCExportError(
                f"Failed to export invoice to 1C after {MAX_RETRIES} attempts: {last_error}"
            )
            
        except OneCExportError:
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error exporting invoice: {e}",
                exc_info=True,
                extra={"order_id": order_id}
            )
            raise OneCExportError(f"Invoice export failed: {e}")


# Для прямого запуска (тестирование)
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python 1c_exporter.py <order_id>")
        print("Example: python 1c_exporter.py 123e4567-e89b-12d3-a456-426614174000")
        sys.exit(1)
    
    order_id = sys.argv[1]
    
    try:
        result = OneCExporter.export_invoice(order_id)
        print(f"Invoice exported successfully:")
        print(f"  Order ID: {result['order_id']}")
        print(f"  Invoice Number: {result['invoice_number']}")
        print(f"  Exported: {result['exported']}")
        print(f"  Exported at: {result['exported_at']}")
        if '1c_response' in result:
            print(f"  1C Response: {result['1c_response']}")
    except OneCExportError as e:
        print(f"Export error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
