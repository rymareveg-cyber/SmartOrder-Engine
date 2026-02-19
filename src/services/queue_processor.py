#!/usr/bin/env python3
"""
Queue Processor - обработчик очереди заказов из Redis.

Обрабатывает сообщения из Redis Queue, маршрутизирует по каналам
и вызывает AI Parser для каждого сообщения.
"""

import json
import signal
import asyncio
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

from src.config import RedisConfig, QueueConfig, APIConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)

REDIS_URL = RedisConfig.URL
QUEUE_KEY = RedisConfig.QUEUE_KEY
DEAD_LETTER_QUEUE_KEY = RedisConfig.DEAD_LETTER_QUEUE_KEY
MAX_RETRIES = RedisConfig.MAX_RETRIES
WORKER_CONCURRENCY = QueueConfig.WORKER_CONCURRENCY
HEALTH_CHECK_PORT = APIConfig.QUEUE_HEALTH_PORT
HEALTH_CHECK_HOST = APIConfig.HOST

shutdown_flag = False

metrics = {
    "processed": 0,
    "errors": 0,
    "dead_letter": 0,
    "by_channel": {
        "telegram": 0,
        "yandex_mail": 0,
        "yandex_forms": 0
    },
    "start_time": None
}

redis_pool: Optional[aioredis.ConnectionPool] = None


def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown."""
    global shutdown_flag
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_flag = True


async def get_redis_client() -> aioredis.Redis:
    """Получить Redis клиент из connection pool."""
    global redis_pool
    if not redis_pool:
        redis_pool = aioredis.ConnectionPool.from_url(
            REDIS_URL,
            decode_responses=False,
            max_connections=WORKER_CONCURRENCY + 2
        )
    return aioredis.Redis(connection_pool=redis_pool)


async def send_to_dead_letter(message_data: dict, error: str):
    """Отправить сообщение в dead letter queue."""
    try:
        redis_client = await get_redis_client()
        dead_letter_data = {
            "original_message": message_data,
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "retries": MAX_RETRIES
        }
        dead_letter_json = json.dumps(dead_letter_data, ensure_ascii=False)
        await redis_client.lpush(DEAD_LETTER_QUEUE_KEY, dead_letter_json)
        metrics["dead_letter"] += 1
        logger.error(f"Message sent to dead letter queue: {error}")
    except Exception as e:
        logger.error(f"Failed to send to dead letter queue: {e}")


async def call_ai_parser(message_data: dict) -> Optional[Dict[str, Any]]:
    """
    Вызов AI Parser для обработки сообщения.
    
    Args:
        message_data: Данные сообщения из очереди
        
    Returns:
        Результат парсинга или None в случае ошибки
    """
    try:
        from src.services.ai_parser import process_order_message
        
        channel = message_data.get("channel", "unknown")
        message_id = message_data.get('message_id') or message_data.get('submission_id') or message_data.get('email') or 'unknown'
        logger.info(f"Calling AI Parser for channel: {channel}, message_id: {message_id}")
        
        # Вызов AI Parser
        result = await process_order_message(message_data)
        
        if result:
            result_status = result.get("status", "unknown")
            logger.info(f"AI Parser successfully processed message {message_id} from {channel}, status: {result_status}")
            return result
        else:
            logger.warning(f"AI Parser returned None for message {message_id} from {channel}")
            return None
    
    except Exception as e:
        channel = message_data.get("channel", "unknown")
        message_id = message_data.get('message_id') or message_data.get('submission_id') or message_data.get('email') or 'unknown'
        logger.error(
            f"Error calling AI Parser: {e}",
            exc_info=True,
            extra={
                "channel": channel,
                "message_id": message_id,
                "error_type": type(e).__name__
            }
        )
        return None


async def process_message(message_data: dict) -> bool:
    """
    Обработка одного сообщения из очереди.
    
    Args:
        message_data: Данные сообщения
        
    Returns:
        True если успешно обработано, False в противном случае
    """
    try:
        channel = message_data.get("channel", "unknown")
        
        # Маршрутизация по каналам
        if channel not in ["telegram", "yandex_mail", "yandex_forms"]:
            logger.warning(f"Unknown channel: {channel}")
            return False
        
        # Обновление метрик
        metrics["by_channel"][channel] = metrics["by_channel"].get(channel, 0) + 1
        
        # ── Для yandex_mail: проверяем контекст уточнения по email ─────────────
        # Связываем ответ на уточняющее письмо с существующим заказом.
        # Только если письмо является ответом (тема начинается с "Re:") ИЛИ
        # содержит маркер уточнения в теме.
        if channel == "yandex_mail" and not message_data.get("existing_order_id"):
            customer_email = message_data.get("email")
            _subject = message_data.get("subject", "")
            _subject_lower = _subject.lower().strip()
            # Поддержка разных почтовых клиентов:
            # Gmail/Outlook: "Re:", Yandex.Mail RU: "Отв:", некоторые: "Ответ:", "Fwd:" игнорируем
            _is_reply = (
                _subject_lower.startswith("re:") or
                _subject_lower.startswith("отв:") or
                _subject_lower.startswith("ответ:") or
                "[уточнение]" in _subject_lower or
                "уточнение" in _subject_lower
            )
            if customer_email and _is_reply:
                try:
                    _email_redis = await get_redis_client()
                    _email_key = f"email_clarification:{customer_email}"
                    _raw_email_ctx = await _email_redis.get(_email_key)
                    if _raw_email_ctx:
                        _email_ctx = json.loads(
                            _raw_email_ctx.decode("utf-8") if isinstance(_raw_email_ctx, bytes) else _raw_email_ctx
                        )
                        # ВАЖНО: используем только тело ответа клиента (body уже очищен от цитат
                        # в mail_parser.py через strip_quoted_reply_content).
                        # НЕ объединяем с original_body — это уже сделано в clarification_context_products.
                        message_data["existing_order_id"] = _email_ctx.get("order_id")
                        # Передаём текущие товары из контекста, чтобы AI обновил их корректно
                        message_data["clarification_context_products"] = _email_ctx.get("products", [])
                        # Передаём известные данные клиента, чтобы AI не спрашивал снова
                        message_data["phone"] = message_data.get("phone") or _email_ctx.get("known_phone")
                        message_data["customer_name"] = message_data.get("customer_name") or _email_ctx.get("known_name")
                        message_data["known_address"] = _email_ctx.get("known_address")
                        await _email_redis.delete(_email_key)
                        logger.info(
                            f"Found email clarification context for {customer_email}, "
                            f"order_id={_email_ctx.get('order_id')}, "
                            f"context_products={len(_email_ctx.get('products', []))}"
                        )
                except Exception as _ectx_err:
                    logger.warning(f"Failed to check email clarification context: {_ectx_err}")

        # Извлекаем existing_order_id из message_data (для обоих каналов)
        existing_order_id = message_data.get("existing_order_id")

        # Вызов AI Parser
        logger.info(
            f"Processing message from {channel}, message_id: {message_data.get('message_id', 'unknown')}",
            extra={"channel": channel, "message_id": message_data.get('message_id', 'unknown')}
        )
        result = await call_ai_parser(message_data)
        
        if result:
            result_status = result.get("status", "needs_clarification")
            logger.info(
                f"AI Parser returned status: {result_status} for message from {channel}",
                extra={"channel": channel, "status": result_status, "message_id": message_data.get('message_id', 'unknown')}
            )
            
            # ВАЖНО: Заказ создается ВСЕГДА со статусом "new" после парсинга
            # Исключение: если это ответ на уточняющий вопрос (existing_order_id присутствует),
            # то обновляем существующий заказ вместо создания нового.
            # existing_order_id уже установлен выше (для email из Redis контекста, для telegram из message_data)

            products = result.get("products", [])

            # Fallback: если AI не вернул товары при уточнении — используем товары из контекста.
            # Это происходит, когда клиент уточняет только контактные данные, не меняя состав заказа.
            if existing_order_id and (not products or len(products) == 0):
                _ctx_products = message_data.get("clarification_context_products") or []
                if _ctx_products:
                    products = _ctx_products
                    result["products"] = products
                    logger.info(
                        f"AI returned no products for clarification of order {existing_order_id}, "
                        f"using {len(products)} fallback products from context"
                    )

            # Проверка: есть ли хотя бы один товар
            if not products or len(products) == 0:
                logger.error(f"Cannot create order: no products found")
                # Отправляем уточняющие вопросы
                if channel == "telegram":
                    telegram_user_id = message_data.get("telegram_user_id")
                    if telegram_user_id:
                        try:
                            from src.services.telegram_bot import send_clarification_message
                            await asyncio.wait_for(
                                send_clarification_message(
                                    telegram_user_id=telegram_user_id,
                                    order_number=None,
                                    clarification_questions=["Не удалось найти товары в каталоге. Пожалуйста, уточните артикулы или названия товаров."],
                                    unfound_products=result.get("unfound_products", []),
                                    parsed_products=None
                                ),
                                timeout=30.0
                            )
                        except asyncio.TimeoutError:
                            logger.warning("Timeout sending clarification message (no products)")
                        except Exception as e:
                            logger.error(f"Failed to send clarification message: {e}", exc_info=True)
                elif channel == "yandex_mail":
                    customer_email = message_data.get("email")
                    if customer_email:
                        try:
                            from src.services.email_notifier import send_clarification_email
                            asyncio.create_task(asyncio.wait_for(
                                asyncio.to_thread(
                                    send_clarification_email,
                                    to_email=customer_email,
                                    clarification_questions=["Не удалось найти товары в каталоге. Пожалуйста, уточните артикулы или названия товаров."],
                                    unfound_products=result.get("unfound_products", []),
                                    parsed_products=None
                                ),
                                timeout=30.0
                            ))
                        except Exception as e:
                            logger.error(f"Failed to send clarification email: {e}", exc_info=True)
                return True  # Сообщение обработано, но заказ не создан
            
            # Проверка что все товары имеют валидные цены
            invalid_products = [p for p in products if not p.get("price_at_order") or p.get("price_at_order", 0) <= 0]
            if invalid_products:
                logger.error(f"Cannot create order: some products have invalid prices: {invalid_products}")
                if channel == "telegram":
                    telegram_user_id = message_data.get("telegram_user_id")
                    if telegram_user_id:
                        try:
                            from src.services.telegram_bot import send_clarification_message
                            await asyncio.wait_for(
                                send_clarification_message(
                                    telegram_user_id=telegram_user_id,
                                    order_number=None,
                                    clarification_questions=["Обнаружены товары с некорректными ценами. Пожалуйста, уточните заказ."],
                                    unfound_products=[],
                                    parsed_products=products
                                ),
                                timeout=30.0
                            )
                        except asyncio.TimeoutError:
                            logger.warning("Timeout sending clarification message (invalid prices)")
                        except Exception as e:
                            logger.error(f"Failed to send clarification message: {e}", exc_info=True)
                elif channel == "yandex_mail":
                    customer_email = message_data.get("email")
                    if customer_email:
                        try:
                            from src.services.email_notifier import send_clarification_email
                            asyncio.create_task(asyncio.wait_for(
                                asyncio.to_thread(
                                    send_clarification_email,
                                    to_email=customer_email,
                                    clarification_questions=["Обнаружены товары с некорректными ценами. Пожалуйста, уточните заказ."],
                                    unfound_products=[],
                                    parsed_products=products
                                ),
                                timeout=30.0
                            ))
                        except Exception as e:
                            logger.error(f"Failed to send clarification email: {e}", exc_info=True)
                return True
            
            # Создаём новый заказ ИЛИ обновляем существующий (при ответе на уточняющий вопрос)
            order = None  # Инициализируем явно для надежной проверки ниже
            try:
                from src.services.order_service import OrderService
                from src.services.telegram_bot import send_order_confirmation

                # Данные клиента: из message_data (авторизация) или из результата парсинга
                customer_name = message_data.get("customer_name") or result.get("customer", {}).get("name")
                customer_phone = message_data.get("phone") or result.get("customer", {}).get("phone")
                customer_address = result.get("customer", {}).get("address")
                telegram_user_id = message_data.get("telegram_user_id")

                if existing_order_id:
                    # ── Ответ на уточняющий вопрос: обновляем существующий заказ ──────────────
                    logger.info(
                        f"Clarification answer for existing order {existing_order_id}: "
                        f"name={customer_name}, phone={customer_phone}, address={customer_address}, "
                        f"products={len(products)}"
                    )
                    # Формируем список товаров для обновления (если AI вернул продукты)
                    items_for_update = [
                        {
                            "product_articul": p["articul"],
                            "product_name": p["name"],
                            "quantity": p["quantity"],
                            "price_at_order": p["price_at_order"]
                        }
                        for p in products
                    ] if products else None

                    order = await asyncio.wait_for(
                        asyncio.to_thread(
                            OrderService.update_order_customer_data,
                            existing_order_id,
                            customer_name,
                            customer_phone,
                            customer_address,
                            items_for_update,
                        ),
                        timeout=15.0
                    )
                    if order is None:
                        logger.error(f"Failed to update existing order {existing_order_id} with clarification data")
                        return False
                    logger.info(
                        f"Updated existing order {order.order_number} with clarification data "
                        f"(products updated: {bool(items_for_update)})"
                    )
                else:
                    # ── Первичный заказ: создаём новую запись ──────────────────────────────────
                    order_data = {
                        "channel": channel,
                        "customer_name": customer_name,
                        "customer_phone": customer_phone,
                        "customer_address": customer_address,
                        "telegram_user_id": telegram_user_id,
                        "customer_email": message_data.get("email") if channel == "yandex_mail" else None,
                        "items": [
                            {
                                "product_articul": item["articul"],
                                "product_name": item["name"],
                                "quantity": item["quantity"],
                                "price_at_order": item["price_at_order"]
                            }
                            for item in products
                        ],
                        "delivery_cost": 0,  # Будет автоматически рассчитано в OrderService.create_order
                        "status": "new"
                    }
                    logger.info(f"Creating order: name={customer_name}, phone={customer_phone}, items={len(order_data['items'])}")
                    order = await asyncio.wait_for(
                        asyncio.to_thread(OrderService.create_order, order_data),
                        timeout=30.0
                    )
                    logger.info(f"Order saved to database: {order.order_number} (ID: {order.id})")
                
                # Если данных не хватает (needs_clarification), отправляем уточняющие вопросы
                if result_status == "needs_clarification":
                    clarification_questions = result.get("clarification_questions", [])
                    unfound_products = result.get("unfound_products", [])
                    parsed_products_for_msg = [
                        {
                            "articul": p.get("articul", ""),
                            "name": p.get("name", ""),
                            "quantity": p.get("quantity", 1)
                        }
                        for p in result.get("products", [])
                    ]

                    # Отправка уточняющих вопросов в Telegram (если канал Telegram)
                    if channel == "telegram":
                        if telegram_user_id:
                            try:
                                from src.services.telegram_bot import send_clarification_message
                                await asyncio.wait_for(
                                    send_clarification_message(
                                        telegram_user_id=telegram_user_id,
                                        order_number=order.order_number,
                                        clarification_questions=clarification_questions,
                                        unfound_products=unfound_products,
                                        parsed_products=parsed_products_for_msg if parsed_products_for_msg else None
                                    ),
                                    timeout=30.0
                                )
                                logger.info(f"Sent clarification questions to Telegram user {telegram_user_id} for order {order.order_number}")

                                # Сохраняем JSON-контекст для следующего сообщения пользователя
                                # Включаем order_id, order_number, уже собранные данные клиента
                                # и исходное сообщение — чтобы при ответе не спрашивать повторно
                                try:
                                    dialog_redis = await get_redis_client()
                                    _result_customer = result.get("customer") or {}
                                    clarification_ctx = json.dumps({
                                        "order_id": str(order.id),
                                        "order_number": order.order_number,
                                        # Сохраняем ТОЛЬКО последний вопрос + текущие товары,
                                        # а не всю накопленную историю переписки.
                                        # Это предотвращает путаницу AI при многораундовых уточнениях.
                                        "clarification_questions": clarification_questions,
                                        "products": result.get("products", []),
                                        "missing_data": result.get("missing_data", []),
                                        # Сохраняем уже собранные данные, чтобы не спрашивать снова
                                        "known_name": _result_customer.get("name"),
                                        "known_phone": _result_customer.get("phone"),
                                        "known_address": _result_customer.get("address"),
                                    }, ensure_ascii=False)
                                    await dialog_redis.setex(
                                        f"clarification:{telegram_user_id}",
                                        3600,  # 1 час
                                        clarification_ctx
                                    )
                                    logger.info(f"Saved clarification context (order_id={order.id}) for user {telegram_user_id}")
                                except Exception as ctx_err:
                                    logger.warning(f"Failed to save clarification context: {ctx_err}")
                            except Exception as e:
                                logger.error(f"Failed to send clarification message: {e}", exc_info=True)

                    # Отправка уточняющих вопросов по email (если канал yandex_mail)
                    elif channel == "yandex_mail":
                        customer_email = message_data.get("email")
                        if customer_email:
                            # Сохраняем контекст уточнения для email (чтобы ответ обновил существующий заказ)
                            try:
                                _mail_ctx_redis = await get_redis_client()
                                _mail_clarification_ctx = json.dumps({
                                    "order_id": str(order.id),
                                    "order_number": order.order_number,
                                    "original_body": message_data.get("body", ""),
                                    "products": result.get("products", []),
                                    "missing_data": result.get("missing_data", []),
                                    # Сохраняем уже известные данные клиента,
                                    # чтобы при ответе AI не спрашивал их повторно.
                                    "known_name": customer_name,
                                    "known_phone": customer_phone,
                                    "known_address": customer_address,
                                }, ensure_ascii=False)
                                await _mail_ctx_redis.setex(
                                    f"email_clarification:{customer_email}",
                                    86400,  # 24 часа
                                    _mail_clarification_ctx
                                )
                                logger.info(f"Saved email clarification context (order_id={order.id}) for {customer_email}")
                            except Exception as _mctx_err:
                                logger.warning(f"Failed to save email clarification context: {_mctx_err}")

                            try:
                                from src.services.email_notifier import send_clarification_email

                                _clar_customer_email = customer_email
                                _clar_questions = clarification_questions
                                _clar_unfound = unfound_products
                                _clar_products = parsed_products_for_msg if parsed_products_for_msg else None
                                _clar_order_number = order.order_number

                                async def send_clarification_email_background():
                                    try:
                                        await asyncio.wait_for(
                                            asyncio.to_thread(
                                                send_clarification_email,
                                                to_email=_clar_customer_email,
                                                clarification_questions=_clar_questions,
                                                unfound_products=_clar_unfound,
                                                parsed_products=_clar_products
                                            ),
                                            timeout=30.0
                                        )
                                        logger.info(f"Sent clarification email to {_clar_customer_email} for order {_clar_order_number}")
                                    except asyncio.TimeoutError:
                                        logger.warning(f"Timeout sending clarification email to {_clar_customer_email}")
                                    except Exception as e:
                                        logger.error(f"Failed to send clarification email: {e}", exc_info=True)

                                asyncio.create_task(send_clarification_email_background())
                            except Exception as e:
                                logger.error(f"Error setting up clarification email: {e}", exc_info=True)

                    # Заказ создан со статусом "new", уточняющие вопросы отправлены
                    return True
                
            except Exception as e:
                logger.error(f"Failed to save order to database: {e}", exc_info=True)
                # Не считаем это критической ошибкой, продолжаем обработку
                return False
            
            # Если статус "validated" - продолжаем обработку: генерируем счет и отправляем подтверждение
            # ВАЖНО: Этот блок находится ВНЕ блока try для создания заказа, чтобы гарантировать выполнение
            # Но только если заказ был успешно создан
            if result_status == "validated" and order is not None:
                logger.info(f"Processing validated order {order.order_number} - generating invoice and sending confirmation")
                # Обновляем статус на "validated" (если еще не обновлен)
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(OrderService.update_order_status, order.id, "validated"),
                        timeout=10.0
                    )
                    logger.info(f"Order {order.order_number} status updated to 'validated'")
                except Exception as e:
                    logger.error(f"Failed to update order status to validated: {e}", exc_info=True)
                    # Продолжаем выполнение даже если обновление статуса не удалось
                
                # Генерация счёта (PDF) - это изменит статус на "invoice_created" (в отдельном потоке)
                invoice_number = None
                invoice_pdf_path = None
                try:
                    from src.services.invoice_generator import InvoiceGenerator
                    
                    # Выполняем синхронную генерацию счета в отдельном потоке
                    invoice_result = await asyncio.wait_for(
                        asyncio.to_thread(InvoiceGenerator.generate_invoice, order.id),
                        timeout=60.0
                    )
                    invoice_number = invoice_result.get('invoice_number')
                    invoice_pdf_path = invoice_result.get('pdf_path')
                    logger.info(f"Invoice generated: {invoice_result['invoice_number']} for order {order.order_number}")
                except Exception as e:
                    logger.error(f"Failed to generate invoice: {e}", exc_info=True)
                    # Не считаем это критической ошибкой, продолжаем обработку

                # Генерация платёжной ссылки (токен → /pay/{token})
                payment_url = None
                try:
                    from src.api.payments import create_payment_token, _get_base_url
                    _pay_token = await asyncio.to_thread(create_payment_token, str(order.id))
                    payment_url = f"{_get_base_url()}/pay/{_pay_token}"
                    logger.info(f"Payment link created for order {order.order_number}: {payment_url}")
                except Exception as e:
                    logger.warning(f"Failed to create payment link for order {order.id}: {e}")
                
                # Получаем актуальные данные заказа из БД (с обновлёнными товарами и статусом)
                # ВАЖНО: берём из БД, не из `products` (AI result) — чтобы гарантировать
                # что в подтверждении те же товары, что в счёте и БД.
                updated_order = None
                try:
                    updated_order = await asyncio.wait_for(
                        asyncio.to_thread(OrderService.get_order, order.id),
                        timeout=10.0
                    )
                    order_status = updated_order.status if updated_order else order.status
                except Exception as e:
                    logger.warning(f"Failed to get updated order status: {e}, using original status")
                    order_status = order.status
                
                # Формируем order_data для отправки подтверждения
                # Используем АКТУАЛЬНЫЕ данные из БД (товары, стоимость доставки и т.д.)
                _actual_order = updated_order if updated_order else order
                _db_items = _actual_order.items if _actual_order and _actual_order.items else []
                order_data = {
                    "channel": channel,
                    "customer_name": _actual_order.customer_name or customer_name,
                    "customer_phone": _actual_order.customer_phone or customer_phone,
                    "customer_address": _actual_order.customer_address or result.get("customer", {}).get("address"),
                    "telegram_user_id": telegram_user_id,
                    # Берём товары из БД (не из AI result), чтобы notification == invoice == DB
                    "items": [
                        {
                            "product_articul": item.product_articul,
                            "product_name": item.product_name,
                            "quantity": item.quantity,
                            "price": float(item.price_at_order),
                            "price_at_order": float(item.price_at_order)
                        }
                        for item in _db_items
                    ],
                    "delivery_cost": float(_actual_order.delivery_cost or 0),
                    "status": order_status
                }
                
                # Отправка подтверждения заказа в Telegram (если канал Telegram)
                if channel == "telegram" and telegram_user_id:
                    try:
                        logger.info(
                            f"Sending order confirmation to Telegram user {telegram_user_id} for order {order.order_number}",
                            extra={
                                "telegram_user_id": telegram_user_id,
                                "order_number": order.order_number,
                                "order_id": str(order.id),
                                "channel": channel
                            }
                        )
                        await asyncio.wait_for(
                            send_order_confirmation(
                                telegram_user_id=telegram_user_id,
                                order_number=order.order_number,
                                order_data=order_data,
                                order_status=order_status,
                                invoice_number=invoice_number,
                                order_id=order.id,
                                payment_url=payment_url,
                            ),
                            timeout=30.0
                        )
                        logger.info(
                            f"Successfully sent order confirmation to Telegram user {telegram_user_id} for order {order.order_number}",
                            extra={
                                "telegram_user_id": telegram_user_id,
                                "order_number": order.order_number,
                                "channel": channel
                            }
                        )
                        # Отмечаем заказ как уведомлённый (защита от дублей при рестартах)
                        try:
                            notify_redis = await get_redis_client()
                            await notify_redis.setex(f"notified:{order.id}", 86400 * 7, "1")
                        except Exception as _nr_e:
                            logger.warning(f"Failed to set notified flag for order {order.id}: {_nr_e}")
                    except Exception as e:
                        logger.error(
                            f"Failed to send order confirmation to Telegram user {telegram_user_id}: {e}",
                            exc_info=True,
                            extra={
                                "telegram_user_id": telegram_user_id,
                                "order_number": order.order_number,
                                "channel": channel,
                                "error_type": type(e).__name__
                            }
                        )
                        # Не считаем это критической ошибкой, но логируем детально
                
                # Отправка подтверждения заказа по email (если канал yandex_mail) - в фоне
                if channel == "yandex_mail":
                    customer_email = message_data.get("email")
                    if customer_email:
                        logger.info(
                            f"Sending order confirmation email to {customer_email} for order {order.order_number}",
                            extra={
                                "customer_email": customer_email,
                                "order_number": order.order_number,
                                "order_id": str(order.id),
                                "channel": channel
                            }
                        )
                        # Создаем фоновую задачу для отправки email, чтобы не блокировать обработку
                        _email_order_number = order.order_number
                        _email_invoice_number = invoice_number
                        _email_invoice_pdf_path = invoice_pdf_path
                        _email_order_data = order_data
                        _email_payment_url = payment_url
                        async def send_email_background():
                            try:
                                from src.services.email_notifier import send_order_confirmation_email
                                try:
                                    result = await asyncio.wait_for(
                                        asyncio.to_thread(
                                            send_order_confirmation_email,
                                            to_email=customer_email,
                                            order_number=_email_order_number,
                                            order_data=_email_order_data,
                                            invoice_number=_email_invoice_number,
                                            invoice_pdf_path=_email_invoice_pdf_path,
                                            payment_url=_email_payment_url,
                                        ),
                                        timeout=60.0  # Таймаут 60 секунд для отправки email с PDF
                                    )
                                    if result:
                                        logger.info(
                                            f"Successfully sent order confirmation email to {customer_email} for order {order.order_number}",
                                            extra={
                                                "customer_email": customer_email,
                                                "order_number": order.order_number,
                                                "channel": channel
                                            }
                                        )
                                    else:
                                        logger.error(
                                            f"Failed to send order confirmation email to {customer_email} for order {order.order_number} - function returned False",
                                            extra={
                                                "customer_email": customer_email,
                                                "order_number": order.order_number,
                                                "channel": channel
                                            }
                                        )
                                except asyncio.TimeoutError:
                                    logger.error(
                                        f"Timeout sending order confirmation email to {customer_email} for order {order.order_number}",
                                        extra={
                                            "customer_email": customer_email,
                                            "order_number": order.order_number,
                                            "channel": channel
                                        }
                                    )
                                except Exception as e:
                                    logger.error(
                                        f"Failed to send order confirmation email to {customer_email}: {e}",
                                        exc_info=True,
                                        extra={
                                            "customer_email": customer_email,
                                            "order_number": order.order_number,
                                            "channel": channel,
                                            "error_type": type(e).__name__
                                        }
                                    )
                            except Exception as e:
                                logger.error(
                                    f"Error in email background task: {e}",
                                    exc_info=True,
                                    extra={
                                        "customer_email": customer_email,
                                        "order_number": order.order_number,
                                        "channel": channel,
                                        "error_type": type(e).__name__
                                    }
                                )
                        
                        # Запускаем в фоне, не ждем завершения
                        asyncio.create_task(send_email_background())
                    else:
                        logger.warning(
                            f"No email address found in message_data for yandex_mail channel, order {order.order_number}",
                            extra={
                                "order_number": order.order_number,
                                "channel": channel,
                                "message_id": message_data.get('message_id', 'unknown')
                            }
                        )
            
            # Логируем результат обработки
            if result_status == "needs_clarification":
                logger.info(f"Message from {channel} requires clarification - questions sent")
            elif result_status == "validated":
                logger.info(f"Message from {channel} validated - order created")
            else:
                logger.info(f"Message from {channel} processed with status: {result_status}")
            
            metrics["processed"] += 1
            return True
        else:
            message_id = message_data.get('message_id') or message_data.get('submission_id') or message_data.get('email') or 'unknown'
            logger.error(
                f"Failed to process message {message_id} from {channel} - AI parser returned None",
                extra={
                    "message_id": message_id,
                    "channel": channel,
                    "error": "AI parser returned None"
                }
            )
            
            # Отправляем уведомление пользователю о проблеме (если возможно)
            if channel == "telegram":
                telegram_user_id = message_data.get("telegram_user_id")
                if telegram_user_id:
                    try:
                        from src.services.telegram_bot import get_bot_instance, _send_message_with_retry, get_telegram_circuit_breaker
                        bot = get_bot_instance()
                        circuit_breaker = get_telegram_circuit_breaker()
                        
                        async def _notify_error():
                            return await _send_message_with_retry(
                                bot,
                                telegram_user_id,
                                "❌ Произошла ошибка при обработке вашего заказа.\n\n"
                                "Пожалуйста, попробуйте отправить заказ еще раз или свяжитесь с администратором."
                            )
                        
                        await circuit_breaker.call(_notify_error)
                        logger.info(f"Sent error notification to Telegram user {telegram_user_id}")
                    except Exception as e:
                        logger.error(f"Failed to send error notification to Telegram user {telegram_user_id}: {e}", exc_info=True)
            
            metrics["errors"] += 1
            return False
    
    except Exception as e:
        message_id = message_data.get('message_id') or message_data.get('submission_id') or message_data.get('email') or 'unknown'
        channel = message_data.get("channel", "unknown")
        logger.error(
            f"Error processing message {message_id} from {channel}: {e}",
            exc_info=True,
            extra={
                "message_id": message_id,
                "channel": channel,
                "error_type": type(e).__name__,
                "error_message": str(e)
            }
        )
        metrics["errors"] += 1
        
        # Отправка в dead letter queue при критических ошибках
        try:
            await send_to_dead_letter(message_data, str(e))
        except Exception as dlq_error:
            logger.error(
                f"Failed to send message to dead letter queue: {dlq_error}",
                extra={"message_id": message_id, "channel": channel}
            )
        
        return False


async def recover_pending_orders():
    """
    Восстановление обработки заказов после перезапуска сервиса.

    Ищет заказы в статусах 'validated' и 'invoice_created', для которых
    флаг notified:{id} в Redis НЕ установлен, и повторно запускает
    генерацию счёта + отправку уведомления.
    """
    logger.info("Starting startup recovery for pending orders...")
    try:
        from src.services.order_service import OrderService
        from src.services.telegram_bot import send_order_confirmation

        # Получаем заказы, требующие обработки
        pending_orders = await asyncio.wait_for(
            asyncio.to_thread(
                OrderService.get_orders_by_status,
                ["validated", "invoice_created"],
                100
            ),
            timeout=15.0
        )
        if not pending_orders:
            logger.info("No pending orders found for recovery")
            return

        redis_client = await get_redis_client()
        recovered = 0
        for order in pending_orders:
            try:
                order_id_str = str(order.id)
                # Проверяем флаг уведомления
                already_notified = await redis_client.exists(f"notified:{order_id_str}")
                if already_notified:
                    continue

                logger.info(f"Recovery: found unnotified order {order.order_number} (status={order.status})")

                # Если статус 'validated' — нужно сгенерировать счёт
                invoice_number = None
                if order.status == "validated":
                    try:
                        from src.services.invoice_generator import InvoiceGenerator
                        invoice_result = await asyncio.wait_for(
                            asyncio.to_thread(InvoiceGenerator.generate_invoice, order.id),
                            timeout=60.0
                        )
                        invoice_number = invoice_result.get("invoice_number")
                        logger.info(f"Recovery: generated invoice {invoice_number} for {order.order_number}")
                    except Exception as inv_err:
                        logger.error(f"Recovery: failed to generate invoice for {order.order_number}: {inv_err}")

                # Обновляем статус заказа после генерации
                try:
                    updated_order = await asyncio.wait_for(
                        asyncio.to_thread(OrderService.get_order, order.id),
                        timeout=10.0
                    )
                    if updated_order:
                        order = updated_order
                except Exception:
                    pass

                # Получаем telegram_user_id из заказа
                telegram_user_id = order.telegram_user_id

                # Генерация платёжной ссылки при восстановлении
                recovery_payment_url = None
                try:
                    from src.api.payments import create_payment_token, _get_base_url
                    _tok = create_payment_token(str(order.id))
                    recovery_payment_url = f"{_get_base_url()}/pay/{_tok}"
                except Exception as _pe:
                    logger.warning(f"Recovery: failed to create payment link for {order.order_number}: {_pe}")

                if order.channel == "telegram" and telegram_user_id:
                    order_data = {
                        "channel": order.channel,
                        "customer_name": order.customer_name,
                        "customer_phone": order.customer_phone,
                        "customer_address": order.customer_address,
                        "telegram_user_id": telegram_user_id,
                        "items": [
                            {
                                "product_articul": item.product_articul,
                                "product_name": item.product_name,
                                "quantity": item.quantity,
                                "price_at_order": float(item.price_at_order)
                            }
                            for item in (order.items or [])
                        ],
                        "delivery_cost": float(order.delivery_cost or 0),
                        "status": order.status
                    }
                    try:
                        await asyncio.wait_for(
                            send_order_confirmation(
                                telegram_user_id=telegram_user_id,
                                order_number=order.order_number,
                                order_data=order_data,
                                order_status=order.status,
                                invoice_number=invoice_number,
                                order_id=order.id,
                                payment_url=recovery_payment_url,
                            ),
                            timeout=30.0
                        )
                        await redis_client.setex(f"notified:{order_id_str}", 86400 * 7, "1")
                        logger.info(f"Recovery: sent notification for {order.order_number} to Telegram user {telegram_user_id}")
                        recovered += 1
                    except Exception as notify_err:
                        logger.error(f"Recovery: failed to notify for {order.order_number}: {notify_err}")

            except Exception as order_err:
                logger.error(f"Recovery: error processing order {order.order_number}: {order_err}", exc_info=True)

        logger.info(f"Startup recovery complete: {recovered} orders recovered out of {len(pending_orders)} pending")

    except Exception as e:
        logger.error(f"Startup recovery failed: {e}", exc_info=True)


async def worker(worker_id: int):
    """
    Worker для обработки сообщений из очереди.
    
    Args:
        worker_id: ID воркера
    """
    logger.info(f"Worker {worker_id} started")
    
    redis_client = await get_redis_client()
    retry_count = {}
    
    try:
        while not shutdown_flag:
            try:
                # Блокирующий pop из очереди (увеличено с 5 до 10 секунд для уменьшения количества пустых проверок)
                result = await redis_client.brpop(QUEUE_KEY, timeout=10)
                
                if not result:
                    # Таймаут - продолжаем цикл
                    continue
                
                logger.info(
                    f"Worker {worker_id} received message from queue {QUEUE_KEY}",
                    extra={"worker_id": worker_id, "queue_key": QUEUE_KEY}
                )
                queue_name, message_bytes = result
                message_json = message_bytes.decode('utf-8')
                try:
                    message_data = json.loads(message_json)
                except json.JSONDecodeError as e:
                    logger.error(
                        f"Worker {worker_id} failed to parse message JSON: {e}",
                        extra={"worker_id": worker_id, "message_preview": message_json[:200]}
                    )
                    continue
                
                # Получение уникального идентификатора сообщения
                # Приоритет: message_id > submission_id > email (только если нет message_id)
                message_id = message_data.get("message_id")
                if not message_id:
                    message_id = message_data.get("submission_id")
                if not message_id:
                    # email используется только как последний fallback, но генерируем уникальный ID
                    email = message_data.get("email")
                    if email:
                        message_id = f"email_{email}_{int(time.time())}"
                    else:
                        message_id = f"unknown_{int(time.time())}_{worker_id}"
                
                channel = message_data.get("channel", "unknown")
                
                # Проверка на уже обработанные сообщения (idempotency)
                processed_key = f"processed_message:{message_id}"
                try:
                    is_processed = await redis_client.exists(processed_key)
                    if is_processed:
                        # Сообщение уже было обработано ранее
                        logger.info(
                            f"Worker {worker_id} detected already processed message {message_id}, skipping",
                            extra={
                                "worker_id": worker_id,
                                "message_id": message_id,
                                "channel": channel
                            }
                        )
                        continue
                except Exception as e:
                    logger.warning(f"Failed to check processed flag for message {message_id}: {e}")
                
                # Проверка на дубликаты обработки (concurrency control)
                processing_key = f"processing:{message_id}"
                try:
                    # Пытаемся установить флаг обработки (SETNX - set if not exists)
                    is_processing = await redis_client.set(
                        processing_key,
                        str(worker_id),
                        ex=300,  # TTL 5 минут
                        nx=True  # Только если ключ не существует
                    )
                    if not is_processing:
                        # Сообщение уже обрабатывается другим воркером
                        logger.warning(
                            f"Worker {worker_id} detected duplicate processing for message {message_id}, skipping",
                            extra={
                                "worker_id": worker_id,
                                "message_id": message_id,
                                "channel": channel
                            }
                        )
                        continue
                except Exception as e:
                    logger.warning(f"Failed to set processing flag for message {message_id}: {e}")
                
                logger.info(
                    f"Worker {worker_id} processing message {message_id} from channel {channel}",
                    extra={
                        "worker_id": worker_id,
                        "message_id": message_id,
                        "channel": channel
                    }
                )
                
                # Обработка сообщения с улучшенной обработкой ошибок
                try:
                    success = await process_message(message_data)
                except Exception as e:
                    logger.error(
                        f"Worker {worker_id} failed to process message {message_id}: {e}",
                        exc_info=True,
                        extra={
                            "worker_id": worker_id,
                            "message_id": message_id,
                            "channel": channel,
                            "error_type": type(e).__name__
                        }
                    )
                    success = False
                
                # Удаление флага обработки после завершения
                try:
                    await redis_client.delete(processing_key)
                except Exception as e:
                    logger.warning(f"Failed to delete processing flag for message {message_id}: {e}")
                
                if success:
                    logger.info(
                        f"Worker {worker_id} successfully processed message {message_id} from channel {channel}",
                        extra={
                            "worker_id": worker_id,
                            "message_id": message_id,
                            "channel": channel
                        }
                    )
                    # Отмечаем сообщение как обработанное (для дедупликации)
                    try:
                        processed_key = f"processed_message:{message_id}"
                        await redis_client.setex(processed_key, 86400, "1")  # 24 часа
                    except Exception as e:
                        logger.warning(f"Failed to mark message as processed: {e}")
                else:
                    logger.warning(
                        f"Worker {worker_id} failed to process message {message_id} from channel {channel}",
                        extra={
                            "worker_id": worker_id,
                            "message_id": message_id,
                            "channel": channel
                        }
                    )
                
                if not success:
                    # Увеличение счётчика попыток
                    retry_key = f"retry:{message_id}"
                    retry_count[retry_key] = retry_count.get(retry_key, 0) + 1
                    
                    if retry_count[retry_key] >= MAX_RETRIES:
                        # Отправка в dead letter queue
                        await send_to_dead_letter(
                            message_data,
                            f"Failed after {MAX_RETRIES} retries"
                        )
                        del retry_count[retry_key]
                        # Удаляем флаг обработки при отправке в DLQ
                        try:
                            await redis_client.delete(processing_key)
                        except Exception:
                            pass
                    else:
                        # Возврат сообщения в очередь для повторной попытки
                        logger.info(f"Retrying message {message_id} (attempt {retry_count[retry_key]}/{MAX_RETRIES})")
                        # Удаляем флаг обработки перед возвратом в очередь
                        try:
                            await redis_client.delete(processing_key)
                        except Exception:
                            pass
                        message_json = json.dumps(message_data, ensure_ascii=False)
                        await redis_client.lpush(QUEUE_KEY, message_json)
                else:
                    # Успешная обработка - удаляем из retry_count
                    retry_key = f"retry:{message_id}"
                    if retry_key in retry_count:
                        del retry_count[retry_key]
            
            except asyncio.CancelledError:
                logger.info(f"Worker {worker_id} cancelled (graceful shutdown)")
                break
            except redis.asyncio.ConnectionError as e:
                if shutdown_flag:
                    logger.info(f"Worker {worker_id} shutting down due to shutdown flag")
                    break
                logger.error(f"Worker {worker_id} Redis connection error: {e}", exc_info=True)
                # Попытка переподключения
                old_client = redis_client
                try:
                    if old_client:
                        await old_client.close()
                except Exception as close_error:
                    logger.warning(f"Error closing old Redis client: {close_error}")
                try:
                    redis_client = await get_redis_client()
                except Exception as reconnect_error:
                    logger.error(f"Failed to reconnect to Redis: {reconnect_error}")
                    await asyncio.sleep(2)  # Задержка перед повтором
                    continue
                await asyncio.sleep(2)  # Задержка перед повтором
            except Exception as e:
                if shutdown_flag:
                    logger.info(f"Worker {worker_id} shutting down due to shutdown flag")
                    break
                logger.error(f"Worker {worker_id} error: {e}", exc_info=True)
                await asyncio.sleep(1)  # Небольшая задержка перед повтором
    
    finally:
        try:
            await redis_client.close()
            logger.info(f"Worker {worker_id} stopped gracefully")
        except Exception as e:
            logger.warning(f"Error closing Redis client in worker {worker_id}: {e}")


async def start_workers():
    """Запуск всех воркеров с graceful shutdown."""
    global shutdown_flag
    
    logger.info(f"Starting {WORKER_CONCURRENCY} workers...")
    metrics["start_time"] = datetime.now(timezone.utc).isoformat()
    
    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Создание задач для воркеров
    tasks = [
        asyncio.create_task(worker(i + 1))
        for i in range(WORKER_CONCURRENCY)
    ]
    
    try:
        # Ожидание завершения всех задач
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Workers cancelled")
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down workers...")
        shutdown_flag = True
    finally:
        # Graceful shutdown: даём воркерам время завершить текущие задачи
        logger.info("Initiating graceful shutdown of workers...")
        shutdown_flag = True
        
        # Ждём завершения текущих задач (максимум 10 секунд)
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=10.0
            )
        except asyncio.TimeoutError:
            logger.warning("Workers did not stop within timeout, cancelling...")
        
        # Отмена всех задач
        for task in tasks:
            if not task.done():
                task.cancel()
        
        # Ожидание отмены
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Закрытие connection pool
        global redis_pool
        if redis_pool:
            try:
                await redis_pool.disconnect()
                logger.info("Redis connection pool closed")
            except Exception as e:
                logger.warning(f"Error closing Redis pool: {e}")
        
        logger.info("All workers stopped gracefully")


# FastAPI для health check
app = FastAPI(
    title="SmartOrder Engine - Queue Processor",
    description="Обработчик очереди заказов из Redis. Маршрутизирует сообщения по каналам и вызывает AI Parser.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)


@app.get("/health")
@app.get("/health/live")
async def liveness_check():
    """
    Liveness probe - проверка что сервис жив.
    
    Всегда возвращает 200, если сервис запущен.
    """
    return {
        "status": "ok",
        "service": "queue_processor",
        "shutdown": shutdown_flag,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/health/ready")
async def readiness_check():
    """
    Readiness probe - проверка готовности сервиса.
    
    Проверяет что Redis доступен и воркеры запущены.
    """
    try:
        # ВАЖНО: не используем общий redis_pool воркеров и не закрываем его из health-check.
        # Иначе health-запросы могут подвешиваться/мешать воркерам.
        health_redis = aioredis.Redis.from_url(
            REDIS_URL,
            decode_responses=False,
            max_connections=2
        )
        try:
            await asyncio.wait_for(health_redis.ping(), timeout=1.5)
            redis_status = "ok"
        finally:
            await health_redis.close()
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        redis_status = "error"
    
    # Вычисление uptime
    uptime_seconds = 0
    if metrics["start_time"]:
        start_dt = datetime.fromisoformat(metrics["start_time"].replace('Z', '+00:00'))
        uptime_seconds = int((datetime.now(timezone.utc) - start_dt).total_seconds())
    
    if redis_status != "ok" or shutdown_flag:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "service": "queue_processor",
                "redis": redis_status,
                "shutdown": shutdown_flag
            }
        )
    
    return {
        "status": "ready",
        "service": "queue_processor",
        "redis": redis_status,
        "workers": WORKER_CONCURRENCY,
        "metrics": {
            "processed": metrics["processed"],
            "errors": metrics["errors"],
            "dead_letter": metrics["dead_letter"],
            "by_channel": metrics["by_channel"],
            "uptime_seconds": uptime_seconds
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/metrics")
async def get_metrics():
    """Endpoint для получения метрик."""
    return JSONResponse(content=metrics)


async def run_health_check_server():
    """Запуск FastAPI сервера для health check."""
    config = uvicorn.Config(
        app,
        host=HEALTH_CHECK_HOST,
        port=HEALTH_CHECK_PORT,
        log_level="info",
        log_config=None
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    """Главная функция."""
    # Создание директории для логов если её нет
    import os
    os.makedirs("logs", exist_ok=True)
    
    logger.info("Starting Queue Processor...")
    logger.info(f"Worker concurrency: {WORKER_CONCURRENCY}")
    logger.info(f"Redis URL: {REDIS_URL}")
    logger.info(f"Health check: http://{HEALTH_CHECK_HOST}:{HEALTH_CHECK_PORT}/health")
    
    # Запуск health check сервера в фоне
    health_check_task = asyncio.create_task(run_health_check_server())

    # Восстановление заказов, уведомления по которым были пропущены
    try:
        await recover_pending_orders()
    except Exception as _rec_err:
        logger.warning(f"Startup recovery error (non-fatal): {_rec_err}")

    try:
        # Запуск воркеров
        await start_workers()
    finally:
        # Остановка health check сервера
        health_check_task.cancel()
        try:
            await health_check_task
        except asyncio.CancelledError:
            pass
        
        logger.info("Queue Processor stopped")


if __name__ == "__main__":
    import sys
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Queue Processor stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        # Не делаем raise, чтобы процесс не упал с ошибкой
        # Вместо этого делаем graceful exit
        sys.exit(1)
