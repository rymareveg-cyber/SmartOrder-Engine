#!/usr/bin/env python3
"""
Order Service - сервис для работы с заказами.

Предоставляет бизнес-логику для создания, обновления и получения заказов.
"""

import re
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from decimal import Decimal

import psycopg2
from psycopg2 import errors as psycopg2_errors
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field, field_validator

from src.config import DatabaseConfig
from src.database.pool import get_db_connection, return_db_connection
from src.utils.logger import get_logger
from src.services.delivery_calculator import DeliveryCalculator

logger = get_logger(__name__)


# Pydantic модели
class OrderItemCreate(BaseModel):
    """Модель для создания позиции заказа."""
    product_articul: str
    product_name: str
    quantity: int = Field(ge=1)
    price_at_order: float = Field(gt=0)


class OrderCreate(BaseModel):
    """Модель для создания заказа."""
    channel: str
    customer_name: Optional[str] = None
    customer_phone: Optional[str] = None
    customer_address: Optional[str] = None
    items: List[OrderItemCreate] = Field(..., min_length=1)
    delivery_cost: float = Field(default=0, ge=0)
    status: str = Field(default="new")
    telegram_user_id: Optional[int] = None
    
    @field_validator('channel')
    @classmethod
    def validate_channel(cls, v):
        allowed_channels = ['telegram', 'yandex_mail', 'yandex_forms']
        if v not in allowed_channels:
            raise ValueError(f"Channel must be one of {allowed_channels}")
        return v
    
    @field_validator('status')
    @classmethod
    def validate_status(cls, v):
        allowed_statuses = ['new', 'validated', 'invoice_created', 'paid', 'order_created_1c', 'tracking_issued', 'shipped', 'cancelled']
        if v not in allowed_statuses:
            raise ValueError(f"Status must be one of {allowed_statuses}")
        return v


class OrderItem(BaseModel):
    """Модель позиции заказа."""
    id: str
    order_id: str
    product_articul: str
    product_name: str
    quantity: int
    price_at_order: float
    total: float
    created_at: str


class Order(BaseModel):
    """Модель заказа."""
    id: str
    order_number: str
    status: str
    channel: str
    customer_name: Optional[str]
    customer_phone: Optional[str]
    customer_address: Optional[str]
    total_amount: float
    delivery_cost: float
    tracking_number: Optional[str] = None
    transaction_id: Optional[str] = None
    invoice_exported_to_1c: bool = False
    telegram_user_id: Optional[int] = None
    customer_email: Optional[str] = None
    created_at: str
    updated_at: str
    paid_at: Optional[str]
    shipped_at: Optional[str]
    items: List[OrderItem] = Field(default_factory=list)


def generate_order_number(conn) -> str:
    """
    Генерация номера заказа (ORD-YYYY-NNNN).
    
    Args:
        conn: Соединение с БД
        
    Returns:
        Номер заказа
    """
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT generate_order_number()")
        order_number = cursor.fetchone()[0]
        logger.info(f"Generated order number: {order_number}")
        return order_number
    finally:
        cursor.close()


def normalize_phone_number(phone: Optional[str]) -> Optional[str]:
    """
    Нормализация номера телефона к формату +7XXXXXXXXXX.
    
    Args:
        phone: Номер телефона в любом формате
        
    Returns:
        Нормализованный номер или None
    """
    if not phone:
        return None
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT normalize_phone(%s)", (phone,))
        result = cursor.fetchone()
        normalized = result[0] if result and result[0] else None
        return normalized
    except Exception as e:
        logger.warning(f"Error normalizing phone {phone}: {e}, using Python fallback")
        # Fallback на Python нормализацию
        cleaned = re.sub(r'[^\d+]', '', phone)
        if cleaned.startswith('8'):
            cleaned = '+7' + cleaned[1:]
        elif cleaned.startswith('7'):
            cleaned = '+7' + cleaned[1:]
        elif not cleaned.startswith('+7'):
            if cleaned.startswith('+'):
                cleaned = '+7' + cleaned[1:]
            else:
                cleaned = '+7' + cleaned
        return cleaned if len(cleaned) >= 12 else None
    finally:
        if conn:
            cursor.close()
            return_db_connection(conn)


class OrderService:
    """Сервис для работы с заказами."""
    
    @staticmethod
    def create_order(order_data: Dict[str, Any]) -> Order:
        """
        Создание заказа с товарами.
        
        Args:
            order_data: Данные заказа (OrderCreate)
            
        Returns:
            Созданный заказ
        """
        conn = None
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                conn = get_db_connection()
                conn.autocommit = False
                cursor = conn.cursor()
                
                order_number = generate_order_number(conn)
                
                # Расчёт стоимости доставки (если не указана)
                delivery_cost = order_data.get("delivery_cost", 0)
                if delivery_cost == 0 and order_data.get("customer_address"):
                    try:
                        calculator = DeliveryCalculator()
                        delivery_info = calculator.calculate_for_order(order_data)
                        delivery_cost = delivery_info["cost"]
                        logger.info(f"Delivery cost calculated: {delivery_cost} RUB for {delivery_info.get('city', 'unknown')}")
                    except Exception as e:
                        logger.warning(f"Failed to calculate delivery cost: {e}, using 0")
                        delivery_cost = 0
                
                # Расчёт общей суммы
                items_total = sum(
                    item["quantity"] * item["price_at_order"]
                    for item in order_data["items"]
                )
                total_amount = items_total + delivery_cost
                
                # Нормализация телефона перед сохранением
                customer_phone = order_data.get("customer_phone")
                normalized_phone = normalize_phone_number(customer_phone) if customer_phone else None
                
                # Создание заказа
                cursor.execute("""
                INSERT INTO orders (
                    order_number, status, channel,
                    customer_name, customer_phone, customer_address,
                    total_amount, delivery_cost, tracking_number, transaction_id,
                    invoice_exported_to_1c, telegram_user_id, customer_email
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, order_number, status, channel,
                          customer_name, customer_phone, customer_address,
                          total_amount, delivery_cost, tracking_number, transaction_id,
                          invoice_exported_to_1c, telegram_user_id, customer_email,
                          created_at, updated_at, paid_at, shipped_at
                """, (
                    order_number,
                    order_data.get("status", "new"),
                    order_data["channel"],
                    order_data.get("customer_name"),
                    normalized_phone,
                    order_data.get("customer_address"),
                    Decimal(str(total_amount)),
                    Decimal(str(delivery_cost)),
                    order_data.get("tracking_number"),
                    order_data.get("transaction_id"),
                    False,
                    order_data.get("telegram_user_id"),
                    order_data.get("customer_email")
                ))
                
                order_row = cursor.fetchone()
                order_id = order_row[0]
                
                # Создание позиций заказа
                items = []
                for item_data in order_data["items"]:
                    item_total = item_data["quantity"] * item_data["price_at_order"]
                    
                    cursor.execute("""
                        INSERT INTO order_items (
                            order_id, product_articul, product_name,
                            quantity, price_at_order, total
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id, order_id, product_articul, product_name,
                                  quantity, price_at_order, total, created_at
                    """, (
                        order_id,
                        item_data["product_articul"],
                        item_data["product_name"],
                        item_data["quantity"],
                        Decimal(str(item_data["price_at_order"])),
                        Decimal(str(item_total))
                    ))
                    
                    item_row = cursor.fetchone()
                    items.append({
                        "id": str(item_row[0]),
                        "order_id": str(item_row[1]),
                        "product_articul": item_row[2],
                        "product_name": item_row[3],
                        "quantity": item_row[4],
                        "price_at_order": float(item_row[5]),
                        "total": float(item_row[6]),
                        "created_at": item_row[7].isoformat() if item_row[7] else None
                    })
                
                conn.commit()
                
                order = Order(
                    id=str(order_row[0]),
                    order_number=order_row[1],
                    status=order_row[2],
                    channel=order_row[3],
                    customer_name=order_row[4],
                    customer_phone=order_row[5],
                    customer_address=order_row[6],
                    total_amount=float(order_row[7]),
                    delivery_cost=float(order_row[8]),
                    tracking_number=order_row[9],
                    transaction_id=order_row[10],
                    invoice_exported_to_1c=order_row[11],
                    telegram_user_id=order_row[12] if len(order_row) > 12 else None,
                    customer_email=order_row[13] if len(order_row) > 13 else None,
                    created_at=order_row[14].isoformat() if len(order_row) > 14 and order_row[14] else None,
                    updated_at=order_row[15].isoformat() if len(order_row) > 15 and order_row[15] else None,
                    paid_at=order_row[16].isoformat() if len(order_row) > 16 and order_row[16] else None,
                    shipped_at=order_row[17].isoformat() if len(order_row) > 17 and order_row[17] else None,
                    items=[OrderItem(**item) for item in items]
                )
                
                logger.info(f"Order created: {order_number} (ID: {order_id})")
                return order
                
            except psycopg2_errors.UniqueViolation as e:
                if conn:
                    conn.rollback()
                    cursor.close()
                    return_db_connection(conn)
                    conn = None
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"Error creating order: duplicate order number after {max_retries} retries: {e}")
                    raise
                logger.warning(f"Duplicate order number detected, retrying ({retry_count}/{max_retries})...")
                time.sleep(0.1 * retry_count)
                continue
                
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"Error creating order: {e}", exc_info=True)
                raise
            finally:
                if conn and (retry_count >= max_retries or retry_count == 0):
                    if 'cursor' in locals():
                        cursor.close()
                    return_db_connection(conn)
    
    @staticmethod
    def get_order(order_id: str) -> Optional[Order]:
        """
        Получение заказа по ID.
        
        Args:
            order_id: UUID заказа
            
        Returns:
            Заказ или None
        """
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT id, order_number, status, channel,
                       customer_name, customer_phone, customer_address,
                       total_amount, delivery_cost, tracking_number, transaction_id,
                       invoice_exported_to_1c, telegram_user_id, customer_email,
                       created_at, updated_at, paid_at, shipped_at
                FROM orders
                WHERE id = %s
            """, (order_id,))
            
            order_row = cursor.fetchone()
            if not order_row:
                return None
            
            cursor.execute("""
                SELECT id, order_id, product_articul, product_name,
                       quantity, price_at_order, total, created_at
                FROM order_items
                WHERE order_id = %s
                ORDER BY created_at
            """, (order_id,))
            
            items_rows = cursor.fetchall()
            items = [
                OrderItem(
                    id=str(item["id"]),
                    order_id=str(item["order_id"]),
                    product_articul=item["product_articul"],
                    product_name=item["product_name"],
                    quantity=item["quantity"],
                    price_at_order=float(item["price_at_order"]),
                    total=float(item["total"]),
                    created_at=item["created_at"].isoformat() if item["created_at"] else None
                )
                for item in items_rows
            ]
            
            order = Order(
                id=str(order_row["id"]),
                order_number=order_row["order_number"],
                status=order_row["status"],
                channel=order_row["channel"],
                customer_name=order_row["customer_name"],
                customer_phone=order_row["customer_phone"],
                customer_address=order_row["customer_address"],
                total_amount=float(order_row["total_amount"]),
                delivery_cost=float(order_row["delivery_cost"]),
                tracking_number=order_row.get("tracking_number"),
                transaction_id=order_row.get("transaction_id"),
                invoice_exported_to_1c=order_row.get("invoice_exported_to_1c", False),
                telegram_user_id=order_row.get("telegram_user_id"),
                customer_email=order_row.get("customer_email"),
                created_at=order_row["created_at"].isoformat() if order_row["created_at"] else None,
                updated_at=order_row["updated_at"].isoformat() if order_row["updated_at"] else None,
                paid_at=order_row["paid_at"].isoformat() if order_row["paid_at"] else None,
                shipped_at=order_row["shipped_at"].isoformat() if order_row["shipped_at"] else None,
                items=items
            )
            
            return order
        
        except Exception as e:
            logger.error(f"Error getting order {order_id}: {e}", exc_info=True)
            return None
        finally:
            if conn:
                cursor.close()
                return_db_connection(conn)
    
    @staticmethod
    def get_orders_by_status(statuses: List[str], limit: int = 100) -> List['Order']:
        """
        Получение заказов по списку статусов (для восстановления после рестарта).

        Args:
            statuses: Список статусов для фильтрации
            limit: Максимальное количество заказов

        Returns:
            Список заказов
        """
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            cursor.execute("""
                SELECT id, order_number, status, channel,
                       customer_name, customer_phone, customer_address,
                       total_amount, delivery_cost, tracking_number, transaction_id,
                       invoice_exported_to_1c, telegram_user_id, customer_email,
                       created_at, updated_at, paid_at, shipped_at
                FROM orders
                WHERE status = ANY(%s)
                ORDER BY created_at DESC
                LIMIT %s
            """, (statuses, limit))

            rows = cursor.fetchall()
            result = []
            for row in rows:
                try:
                    cursor2 = conn.cursor(cursor_factory=RealDictCursor)
                    cursor2.execute("""
                        SELECT id, order_id, product_articul, product_name,
                               quantity, price_at_order, total, created_at
                        FROM order_items WHERE order_id = %s ORDER BY created_at
                    """, (str(row["id"]),))
                    items_rows = cursor2.fetchall()
                    cursor2.close()
                    items = [
                        OrderItem(
                            id=str(i["id"]),
                            order_id=str(i["order_id"]),
                            product_articul=i["product_articul"],
                            product_name=i["product_name"],
                            quantity=i["quantity"],
                            price_at_order=float(i["price_at_order"]),
                            total=float(i["total"]),
                            created_at=i["created_at"].isoformat() if i["created_at"] else None
                        )
                        for i in items_rows
                    ]
                    result.append(Order(
                        id=str(row["id"]),
                        order_number=row["order_number"],
                        status=row["status"],
                        channel=row["channel"],
                        customer_name=row["customer_name"],
                        customer_phone=row["customer_phone"],
                        customer_address=row["customer_address"],
                        total_amount=float(row["total_amount"]),
                        delivery_cost=float(row["delivery_cost"]),
                        tracking_number=row.get("tracking_number"),
                        transaction_id=row.get("transaction_id"),
                        invoice_exported_to_1c=row.get("invoice_exported_to_1c", False),
                        telegram_user_id=row.get("telegram_user_id"),
                        customer_email=row.get("customer_email"),
                        created_at=row["created_at"].isoformat() if row["created_at"] else None,
                        updated_at=row["updated_at"].isoformat() if row["updated_at"] else None,
                        paid_at=row["paid_at"].isoformat() if row["paid_at"] else None,
                        shipped_at=row["shipped_at"].isoformat() if row["shipped_at"] else None,
                        items=items
                    ))
                except Exception as row_err:
                    logger.warning(f"get_orders_by_status: skipping row due to error: {row_err}")

            cursor.close()
            return result

        except Exception as e:
            logger.error(f"Error getting orders by status {statuses}: {e}", exc_info=True)
            return []
        finally:
            if conn:
                return_db_connection(conn)

    @staticmethod
    def update_order_status(order_id: str, new_status: str, **kwargs) -> Optional[Order]:
        """
        Обновление статуса заказа.
        
        Args:
            order_id: UUID заказа
            new_status: Новый статус
            **kwargs: Дополнительные поля (paid_at, shipped_at)
            
        Returns:
            Обновлённый заказ или None
        """
        valid_transitions = {
            "new": ["validated", "cancelled"],
            "validated": ["invoice_created", "cancelled"],
            "invoice_created": ["paid", "cancelled"],
            "paid": ["order_created_1c", "cancelled"],
            "order_created_1c": ["tracking_issued", "cancelled"],
            "tracking_issued": ["shipped", "cancelled"],
            "shipped": ["cancelled"],
            "cancelled": []
        }
        
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT status FROM orders WHERE id = %s", (order_id,))
            row = cursor.fetchone()
            if not row:
                return None
            
            current_status = row[0]
            
            if new_status not in valid_transitions.get(current_status, []):
                raise ValueError(f"Invalid status transition: {current_status} -> {new_status}")
            
            update_fields = ["status = %s"]
            params = [new_status, order_id]
            
            if new_status == "paid" and "paid_at" not in kwargs:
                update_fields.append("paid_at = CURRENT_TIMESTAMP")
            elif "paid_at" in kwargs:
                update_fields.append("paid_at = %s")
                params.insert(-1, kwargs["paid_at"])
            
            if new_status == "paid" and "transaction_id" in kwargs:
                update_fields.append("transaction_id = %s")
                params.insert(-1, kwargs["transaction_id"])
            
            if new_status in ("shipped", "tracking_issued") and "tracking_number" in kwargs:
                update_fields.append("tracking_number = %s")
                params.insert(-1, kwargs["tracking_number"])

            if new_status == "order_created_1c":
                update_fields.append("invoice_exported_to_1c = TRUE")

            if new_status in ("shipped", "tracking_issued") and "shipped_at" not in kwargs:
                update_fields.append("shipped_at = CURRENT_TIMESTAMP")
            elif "shipped_at" in kwargs:
                update_fields.append("shipped_at = %s")
                params.insert(-1, kwargs["shipped_at"])
            
            cursor.execute(f"""
                UPDATE orders
                SET {', '.join(update_fields)}
                WHERE id = %s
                RETURNING id
            """, params)
            
            if not cursor.fetchone():
                return None
            
            conn.commit()
            logger.info(f"Order {order_id} status updated: {current_status} -> {new_status}")
            
            return OrderService.get_order(order_id)
        
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error updating order status: {e}", exc_info=True)
            raise
        finally:
            if conn:
                cursor.close()
                return_db_connection(conn)
    
    @staticmethod
    def list_orders(
        status: Optional[str] = None,
        channel: Optional[str] = None,
        customer_phone: Optional[str] = None,
        page: int = 1,
        page_size: int = 20
    ) -> Dict[str, Any]:
        """
        Получение списка заказов с фильтрацией и пагинацией.
        
        Args:
            status: Фильтр по статусу
            channel: Фильтр по каналу
            customer_phone: Фильтр по телефону
            page: Номер страницы
            page_size: Размер страницы
            
        Returns:
            Словарь с заказами и метаданными пагинации
        """
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            where_conditions = []
            params = []
            
            if status:
                where_conditions.append("status = %s")
                params.append(status)
            
            if channel:
                where_conditions.append("channel = %s")
                params.append(channel)
            
            if customer_phone:
                normalized_phone = normalize_phone_number(customer_phone)
                if normalized_phone:
                    where_conditions.append("customer_phone = %s")
                    params.append(normalized_phone)
                else:
                    where_conditions.append("customer_phone LIKE %s")
                    params.append(f"%{customer_phone}%")
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            cursor.execute(f"""
                SELECT COUNT(*) as total
                FROM orders
                WHERE {where_clause}
            """, params)
            total = cursor.fetchone()["total"]
            
            offset = (page - 1) * page_size
            cursor.execute(f"""
                SELECT id, order_number, status, channel,
                       customer_name, customer_phone, customer_address,
                       total_amount, delivery_cost, tracking_number, transaction_id,
                       invoice_exported_to_1c, telegram_user_id, customer_email,
                       created_at, updated_at, paid_at, shipped_at
                FROM orders
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """, params + [page_size, offset])
            
            orders_rows = cursor.fetchall()
            orders = []
            
            for order_row in orders_rows:
                cursor.execute("""
                    SELECT id, order_id, product_articul, product_name,
                           quantity, price_at_order, total, created_at
                    FROM order_items
                    WHERE order_id = %s
                """, (order_row["id"],))
                
                items_rows = cursor.fetchall()
                items = [
                    OrderItem(
                        id=str(item["id"]),
                        order_id=str(item["order_id"]),
                        product_articul=item["product_articul"],
                        product_name=item["product_name"],
                        quantity=item["quantity"],
                        price_at_order=float(item["price_at_order"]),
                        total=float(item["total"]),
                        created_at=item["created_at"].isoformat() if item["created_at"] else None
                    )
                    for item in items_rows
                ]
                
                orders.append(Order(
                    id=str(order_row["id"]),
                    order_number=order_row["order_number"],
                    status=order_row["status"],
                    channel=order_row["channel"],
                    customer_name=order_row["customer_name"],
                    customer_phone=order_row["customer_phone"],
                    customer_address=order_row["customer_address"],
                    total_amount=float(order_row["total_amount"]),
                    delivery_cost=float(order_row["delivery_cost"]),
                    tracking_number=order_row.get("tracking_number"),
                    transaction_id=order_row.get("transaction_id"),
                    invoice_exported_to_1c=order_row.get("invoice_exported_to_1c", False),
                    telegram_user_id=order_row.get("telegram_user_id"),
                    customer_email=order_row.get("customer_email"),
                    created_at=order_row["created_at"].isoformat() if order_row["created_at"] else None,
                    updated_at=order_row["updated_at"].isoformat() if order_row["updated_at"] else None,
                    paid_at=order_row["paid_at"].isoformat() if order_row["paid_at"] else None,
                    shipped_at=order_row["shipped_at"].isoformat() if order_row["shipped_at"] else None,
                    items=items
                ))
            
            pages = (total + page_size - 1) // page_size if total > 0 else 0
            
            return {
                "items": orders,
                "total": total,
                "page": page,
                "page_size": page_size,
                "pages": pages
            }
        
        except Exception as e:
            logger.error(f"Error listing orders: {e}", exc_info=True)
            raise
        finally:
            if conn:
                cursor.close()
                return_db_connection(conn)
    
    @staticmethod
    def get_orders_by_phone(phone: str, telegram_user_id: Optional[int] = None) -> List[Order]:
        """
        Получение всех заказов пользователя по номеру телефона.
        
        Args:
            phone: Номер телефона (любой формат)
            telegram_user_id: Опционально, для проверки безопасности
            
        Returns:
            Список заказов пользователя из всех каналов
        """
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            normalized_phone = normalize_phone_number(phone)
            if not normalized_phone:
                logger.warning(f"Could not normalize phone: {phone}")
                return []
            
            where_conditions = ["normalize_phone(customer_phone) = normalize_phone(%s)"]
            params = [phone]
            
            if telegram_user_id is not None:
                where_conditions.append(
                    "(channel != 'telegram' OR telegram_user_id = %s)"
                )
                params.append(telegram_user_id)
            
            where_clause = " AND ".join(where_conditions)
            
            cursor.execute(f"""
                SELECT id, order_number, status, channel,
                       customer_name, customer_phone, customer_address,
                       total_amount, delivery_cost, tracking_number, transaction_id,
                       invoice_exported_to_1c, telegram_user_id, customer_email,
                       created_at, updated_at, paid_at, shipped_at
                FROM orders
                WHERE {where_clause}
                ORDER BY created_at DESC
            """, params)
            
            orders_rows = cursor.fetchall()
            orders = []
            
            for order_row in orders_rows:
                cursor.execute("""
                    SELECT id, order_id, product_articul, product_name,
                           quantity, price_at_order, total, created_at
                    FROM order_items
                    WHERE order_id = %s
                """, (order_row["id"],))
                
                items_rows = cursor.fetchall()
                items = [
                    OrderItem(
                        id=str(item["id"]),
                        order_id=str(item["order_id"]),
                        product_articul=item["product_articul"],
                        product_name=item["product_name"],
                        quantity=item["quantity"],
                        price_at_order=float(item["price_at_order"]),
                        total=float(item["total"]),
                        created_at=item["created_at"].isoformat() if item["created_at"] else None
                    )
                    for item in items_rows
                ]
                
                orders.append(Order(
                    id=str(order_row["id"]),
                    order_number=order_row["order_number"],
                    status=order_row["status"],
                    channel=order_row["channel"],
                    customer_name=order_row["customer_name"],
                    customer_phone=order_row["customer_phone"],
                    customer_address=order_row["customer_address"],
                    total_amount=float(order_row["total_amount"]),
                    delivery_cost=float(order_row["delivery_cost"]),
                    tracking_number=order_row.get("tracking_number"),
                    transaction_id=order_row.get("transaction_id"),
                    invoice_exported_to_1c=order_row.get("invoice_exported_to_1c", False),
                    telegram_user_id=order_row.get("telegram_user_id"),
                    customer_email=order_row.get("customer_email"),
                    created_at=order_row["created_at"].isoformat() if order_row["created_at"] else None,
                    updated_at=order_row["updated_at"].isoformat() if order_row["updated_at"] else None,
                    paid_at=order_row["paid_at"].isoformat() if order_row["paid_at"] else None,
                    shipped_at=order_row["shipped_at"].isoformat() if order_row["shipped_at"] else None,
                    items=items
                ))
            
            logger.info(f"Found {len(orders)} orders for phone {normalized_phone}")
            return orders
        
        except Exception as e:
            logger.error(f"Error getting orders by phone {phone}: {e}", exc_info=True)
            return []
        finally:
            if conn:
                cursor.close()
                return_db_connection(conn)
    
    @staticmethod
    def get_order_items(order_id: str) -> List[OrderItem]:
        """
        Получение позиций заказа.
        
        Args:
            order_id: UUID заказа
            
        Returns:
            Список позиций заказа
        """
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT id, order_id, product_articul, product_name,
                       quantity, price_at_order, total, created_at
                FROM order_items
                WHERE order_id = %s
                ORDER BY created_at
            """, (order_id,))
            
            items_rows = cursor.fetchall()
            items = [
                OrderItem(
                    id=str(item["id"]),
                    order_id=str(item["order_id"]),
                    product_articul=item["product_articul"],
                    product_name=item["product_name"],
                    quantity=item["quantity"],
                    price_at_order=float(item["price_at_order"]),
                    total=float(item["total"]),
                    created_at=item["created_at"].isoformat() if item["created_at"] else None
                )
                for item in items_rows
            ]
            
            return items
        
        except Exception as e:
            logger.error(f"Error getting order items: {e}", exc_info=True)
            return []
        finally:
            if conn:
                cursor.close()
                return_db_connection(conn)


    @staticmethod
    def update_order_customer_data(
        order_id: str,
        customer_name: Optional[str] = None,
        customer_phone: Optional[str] = None,
        customer_address: Optional[str] = None,
        items: Optional[list] = None,
    ) -> Optional["Order"]:
        """
        Обновляет данные клиента и (опционально) товары в существующем заказе.
        Используется при обработке ответов на уточняющие вопросы.

        Args:
            order_id: UUID заказа
            customer_name: Новое имя клиента
            customer_phone: Новый телефон клиента
            customer_address: Новый адрес доставки
            items: Обновлённый список товаров (если пересчитана сумма)

        Returns:
            Обновлённый заказ или None
        """
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Строим SET-часть запроса динамически
            set_parts = ["updated_at = CURRENT_TIMESTAMP"]
            params: list = []

            if customer_name is not None:
                set_parts.append("customer_name = %s")
                params.append(customer_name)
            if customer_phone is not None:
                normalized = normalize_phone_number(customer_phone)
                set_parts.append("customer_phone = %s")
                params.append(normalized or customer_phone)
            if customer_address is not None:
                set_parts.append("customer_address = %s")
                params.append(customer_address)

            # Пересчитываем сумму и обновляем товары, если переданы
            if items:
                from decimal import Decimal
                # Удаляем старые товары и вставляем новые
                cursor.execute("DELETE FROM order_items WHERE order_id = %s", (order_id,))
                for it in items:
                    item_total = Decimal(str(it["quantity"])) * Decimal(str(it["price_at_order"]))
                    cursor.execute("""
                        INSERT INTO order_items (order_id, product_articul, product_name, quantity, price_at_order, total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        order_id,
                        it.get("product_articul", it.get("articul", "")),
                        it.get("product_name", it.get("name", "")),
                        it["quantity"],
                        it["price_at_order"],
                        item_total
                    ))
                items_total = sum(
                    Decimal(str(it["quantity"])) * Decimal(str(it["price_at_order"]))
                    for it in items
                )
                # Пересчитываем delivery_cost (если адрес обновился, тоже пересчитываем доставку)
                new_address = customer_address
                if new_address:
                    try:
                        from src.services.delivery_calculator import DeliveryCalculator
                        calc = DeliveryCalculator()
                        delivery_info = calc.calculate_for_order({"customer_address": new_address, "items": items})
                        delivery_cost = Decimal(str(delivery_info.get("cost", 0)))
                        set_parts.append("delivery_cost = %s")
                        params.append(delivery_cost)
                    except Exception:
                        cursor.execute("SELECT delivery_cost FROM orders WHERE id = %s", (order_id,))
                        row = cursor.fetchone()
                        delivery_cost = Decimal(str(row["delivery_cost"])) if row else Decimal("0")
                else:
                    cursor.execute("SELECT delivery_cost FROM orders WHERE id = %s", (order_id,))
                    row = cursor.fetchone()
                    delivery_cost = Decimal(str(row["delivery_cost"])) if row else Decimal("0")
                total_amount = items_total + delivery_cost
                set_parts.append("total_amount = %s")
                params.append(total_amount)

            params.append(order_id)
            set_clause = ", ".join(set_parts)
            cursor.execute(
                f"UPDATE orders SET {set_clause} WHERE id = %s RETURNING id",
                params
            )
            if cursor.fetchone() is None:
                logger.warning(f"update_order_customer_data: order {order_id} not found")
                conn.commit()
                cursor.close()
                return None

            conn.commit()
            cursor.close()
            # Возвращаем обновлённый заказ
            return OrderService.get_order(order_id)

        except Exception as e:
            logger.error(f"Error updating customer data for order {order_id}: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                return_db_connection(conn)


class TelegramUserService:
    """Сервис для работы с авторизованными пользователями Telegram."""
    
    @staticmethod
    def authorize_user(telegram_user_id: int, phone: str, first_name: Optional[str] = None, 
                       last_name: Optional[str] = None, username: Optional[str] = None) -> bool:
        """
        Авторизация пользователя Telegram (сохранение в БД).
        
        Args:
            telegram_user_id: ID пользователя Telegram
            phone: Номер телефона
            first_name: Имя пользователя
            last_name: Фамилия пользователя
            username: Username пользователя
            
        Returns:
            True если успешно, False в случае ошибки
        """
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            normalized_phone = normalize_phone_number(phone)
            if not normalized_phone:
                logger.error(f"Failed to normalize phone: {phone}")
                return False
            
            cursor.execute("""
                INSERT INTO telegram_users (telegram_user_id, phone, first_name, last_name, username, authorized_at, last_activity)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (telegram_user_id) 
                DO UPDATE SET 
                    phone = EXCLUDED.phone,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    username = EXCLUDED.username,
                    last_activity = CURRENT_TIMESTAMP
            """, (telegram_user_id, normalized_phone, first_name, last_name, username))
            
            conn.commit()
            logger.info(f"User {telegram_user_id} authorized with phone {normalized_phone}")
            return True
            
        except psycopg2_errors.UniqueViolation:
            conn.rollback()
            logger.warning(f"Phone {normalized_phone} already used by another user")
            return False
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error authorizing user {telegram_user_id}: {e}", exc_info=True)
            return False
        finally:
            if conn:
                cursor.close()
                return_db_connection(conn)
    
    @staticmethod
    def is_authorized(telegram_user_id: int) -> bool:
        """
        Проверка, авторизован ли пользователь.
        
        Args:
            telegram_user_id: ID пользователя Telegram
            
        Returns:
            True если авторизован, False если нет
        """
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 1 FROM telegram_users 
                WHERE telegram_user_id = %s
            """, (telegram_user_id,))
            
            result = cursor.fetchone()
            return result is not None
            
        except Exception as e:
            logger.error(f"Error checking authorization for user {telegram_user_id}: {e}")
            return False
        finally:
            if conn:
                cursor.close()
                return_db_connection(conn)
    
    @staticmethod
    def get_user_info(telegram_user_id: int) -> Optional[Dict[str, Any]]:
        """
        Получение информации о пользователе.
        
        Args:
            telegram_user_id: ID пользователя Telegram
            
        Returns:
            Словарь с информацией о пользователе или None
        """
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT telegram_user_id, phone, first_name, last_name, username, 
                       authorized_at, last_activity
                FROM telegram_users 
                WHERE telegram_user_id = %s
            """, (telegram_user_id,))
            
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user info for {telegram_user_id}: {e}")
            return None
        finally:
            if conn:
                cursor.close()
                return_db_connection(conn)
    
    @staticmethod
    def update_last_activity(telegram_user_id: int) -> bool:
        """
        Обновление времени последней активности пользователя.
        
        Args:
            telegram_user_id: ID пользователя Telegram
            
        Returns:
            True если успешно
        """
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE telegram_users 
                SET last_activity = CURRENT_TIMESTAMP
                WHERE telegram_user_id = %s
            """, (telegram_user_id,))
            
            conn.commit()
            return True
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error updating last activity for user {telegram_user_id}: {e}")
            return False
        finally:
            if conn:
                cursor.close()
                return_db_connection(conn)
