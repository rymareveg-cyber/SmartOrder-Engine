#!/usr/bin/env python3
"""
1C Exporter - модуль экспорта счетов в 1С:Управление нашей фирмой.

Экспортирует счета на оплату в 1С через HTTP Service после успешной оплаты заказа.
"""

import base64
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.services.order_service import OrderService, Order
from src.utils.logger import get_logger
from src.utils.retry import (
    retry_with_backoff,
    get_onec_circuit_breaker,
    CircuitBreakerOpenError,
    CircuitState
)
from src.config import OneCConfig

logger = get_logger(__name__)

ONEC_BASE_URL = OneCConfig.BASE_URL or 'http://localhost:80'
ONEC_USERNAME = OneCConfig.USERNAME or ''
ONEC_PASSWORD = OneCConfig.PASSWORD or ''
ONEC_INVOICES_ENDPOINT = OneCConfig.INVOICES_ENDPOINT

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
    
    # Форматирование даты (с fallback на текущую дату)
    try:
        if order.created_at:
            # Нормализуем формат: isoformat() даёт "+00:00", но может быть и "Z"
            _date_str = order.created_at.replace('Z', '+00:00') if isinstance(order.created_at, str) else order.created_at.isoformat()
            order_date = datetime.fromisoformat(_date_str)
        else:
            order_date = datetime.now(timezone.utc)
    except (ValueError, AttributeError) as _e:
        logger.warning(f"Cannot parse order created_at '{order.created_at}': {_e}. Using current date.")
        order_date = datetime.now(timezone.utc)
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


@retry_with_backoff(
    max_retries=MAX_RETRIES,
    initial_delay=2.0,
    max_delay=60.0,
    exponential_base=2.0,
    jitter=True,
    retry_on=(OneCExportError, requests.exceptions.RequestException),
    retry_on_not=()  # Не делаем retry для постоянных ошибок (404, 401, 403) - они обрабатываются внутри
)
def _send_invoice_to_1c_internal(invoice_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Внутренняя функция для отправки счёта в 1С (без circuit breaker).
    
    Args:
        invoice_data: Данные счёта для экспорта
        
    Returns:
        Ответ от 1С
        
    Raises:
        OneCExportError: При ошибке экспорта
        requests.exceptions.RequestException: При сетевой ошибке
    """
    url = f"{ONEC_BASE_URL}{ONEC_INVOICES_ENDPOINT}"
    
    # Создание заголовков
    headers = {
        "Content-Type": "application/json",
        "Authorization": create_1c_auth_header(ONEC_USERNAME, ONEC_PASSWORD)
    }
    
    logger.info(
        f"Отправка счёта в 1С — тело запроса",
        extra={
            "url": url,
            "invoice_number": invoice_data.get("invoice_number"),
            "request_body": invoice_data
        }
    )
    
    # Настройка retry для requests (базовый уровень)
    session = requests.Session()
    retry_strategy = Retry(
        total=1,  # Управляем retry на верхнем уровне
        backoff_factor=0,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Отправка запроса
    try:
        response = session.post(
            url,
            json=invoice_data,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
    except requests.exceptions.Timeout:
        logger.error(
            f"Timeout while sending invoice to 1C",
            extra={
                "url": url,
                "invoice_number": invoice_data.get("invoice_number"),
                "timeout": REQUEST_TIMEOUT
            }
        )
        raise OneCExportError(f"Timeout while connecting to 1C (>{REQUEST_TIMEOUT}s)")
    except requests.exceptions.ConnectionError as e:
        logger.error(
            f"Connection error while sending invoice to 1C: {e}",
            extra={
                "url": url,
                "invoice_number": invoice_data.get("invoice_number")
            }
        )
        raise OneCExportError(f"Connection error while connecting to 1C: {e}")
    
    # Логирование ответа (INFO уровень чтобы всегда видеть что ответила 1С)
    logger.info(
        f"Ответ от 1С: HTTP {response.status_code}",
        extra={
            "url": url,
            "invoice_number": invoice_data.get("invoice_number"),
            "status_code": response.status_code,
            "response_text": response.text[:1000] if response.text else "(пусто)"
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
                f"1С принял счёт (JSON ответ)",
                extra={
                    "invoice_number": invoice_data.get("invoice_number"),
                    "1c_response": result
                }
            )
            return result
        except ValueError:
            # Если ответ не JSON, но статус 200
            logger.warning(
                f"1С вернул HTTP 200 но ответ не JSON: '{response.text[:500]}'",
                extra={
                    "invoice_number": invoice_data.get("invoice_number"),
                    "response_text": response.text[:500]
                }
            )
            # Не считаем это успехом — скорее всего 1С не обработал запрос
            raise OneCExportError(
                f"1C returned non-JSON response (HTTP 200): {response.text[:200]}"
            )
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


def send_invoice_to_1c(invoice_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Отправка счёта в 1С через HTTP Service с retry.

    Примечание: функция выполняется синхронно (из потока asyncio.to_thread).
    Retry-логика реализована декоратором @retry_with_backoff на _send_invoice_to_1c_internal.

    Args:
        invoice_data: Данные счёта для экспорта

    Returns:
        Ответ от 1С

    Raises:
        OneCExportError: При ошибке экспорта
    """
    # Проверяем состояние circuit breaker синхронно (без await)
    circuit_breaker = get_onec_circuit_breaker()
    cb_state = circuit_breaker.state
    if cb_state.value == "open":
        # Проверяем, истёк ли recovery_timeout
        if (circuit_breaker.last_failure_time and
                (time.time() - circuit_breaker.last_failure_time) < circuit_breaker.recovery_timeout):
            logger.error(f"1C circuit breaker is OPEN, skipping export")
            raise OneCExportError(
                f"1C service is temporarily unavailable (circuit breaker open, "
                f"retry after {circuit_breaker.recovery_timeout}s)"
            )
        else:
            # Время вышло — переводим в half-open вручную (без lock, приемлемо для sync)
            circuit_breaker.state = CircuitState.HALF_OPEN
            circuit_breaker.success_count = 0
            logger.info("1C circuit breaker transitioning to HALF_OPEN (sync check)")

    try:
        logger.info(
            "Sending invoice to 1C",
            extra={
                "url": f"{ONEC_BASE_URL}{ONEC_INVOICES_ENDPOINT}",
                "invoice_number": invoice_data.get("invoice_number")
            }
        )
        # _send_invoice_to_1c_internal имеет собственный @retry_with_backoff
        response = _send_invoice_to_1c_internal(invoice_data)

        # Успех — сбрасываем счётчик ошибок
        circuit_breaker.failure_count = 0
        circuit_breaker.state = CircuitState.CLOSED
        return response

    except OneCExportError:
        # Считаем ошибку в circuit breaker
        circuit_breaker.failure_count += 1
        circuit_breaker.last_failure_time = time.time()
        if circuit_breaker.failure_count >= circuit_breaker.failure_threshold:
            circuit_breaker.state = CircuitState.OPEN
            logger.warning(
                f"1C circuit breaker opened after {circuit_breaker.failure_count} failures"
            )
        raise
    except Exception as e:
        circuit_breaker.failure_count += 1
        circuit_breaker.last_failure_time = time.time()
        if circuit_breaker.failure_count >= circuit_breaker.failure_threshold:
            circuit_breaker.state = CircuitState.OPEN
        logger.error(f"Failed to send invoice to 1C: {e}", exc_info=True)
        raise OneCExportError(f"Failed to send invoice to 1C: {e}")


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
            from src.database.pool import get_db_connection, return_db_connection
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
            
            # Отправка в 1С (retry и circuit breaker внутри send_invoice_to_1c)
            response = send_invoice_to_1c(invoice_data)
            
            # Обновление флага в БД
            update_invoice_exported_flag(order_id, True)
            
            # Обновление статуса заказа на "order_created_1c"
            try:
                updated_order = OrderService.update_order_status(order_id, "order_created_1c")
                logger.info(
                    f"Order {order_id} status updated to 'order_created_1c' after successful 1C export",
                    extra={
                        "order_id": order_id,
                        "order_number": order.order_number,
                        "new_status": "order_created_1c"
                    }
                )
            except Exception as e:
                logger.error(
                    f"Failed to update order status to 'order_created_1c': {e}",
                    exc_info=True,
                    extra={"order_id": order_id}
                )
                # Статус не обновлён — трек-номер не может быть сгенерирован
                raise OneCExportError(
                    f"Invoice sent to 1C but failed to update order status: {e}"
                )
            
            exported_at = datetime.now(timezone.utc)
            
            logger.info(
                f"Invoice exported successfully to 1C",
                extra={
                    "order_id": order_id,
                    "order_number": order.order_number,
                    "invoice_number": invoice_data.get("invoice_number"),
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
