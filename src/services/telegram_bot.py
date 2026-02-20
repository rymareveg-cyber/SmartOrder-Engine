#!/usr/bin/env python3
"""
Telegram бот для приёма заказов.

Принимает текстовые сообщения от пользователей и отправляет их в Redis Queue
для дальнейшей обработки AI-парсером.
"""

import os
import json
import asyncio
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters
)
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest
from src.utils.logger import get_logger
from src.services.order_service import TelegramUserService
from src.utils.redis_client import init_redis_client, send_to_queue_sync
from src.utils.retry import retry_with_backoff, get_telegram_circuit_breaker
from src.config import TelegramConfig, RedisConfig, PROJECT_ROOT

logger = get_logger(__name__)

TELEGRAM_BOT_TOKEN = TelegramConfig.BOT_TOKEN
TELEGRAM_ADMIN_ID = TelegramConfig.ADMIN_ID

redis_client: Optional[Any] = None
QUEUE_KEY = RedisConfig.QUEUE_KEY
_global_bot: Optional[Any] = None


def init_redis():
    """Инициализация Redis клиента с retry."""
    global redis_client
    import time
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            redis_client = init_redis_client(decode_responses=False, raise_on_error=True)
            logger.info("Redis client initialized successfully")
            return
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Failed to initialize Redis (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logger.error(f"Failed to initialize Redis after {max_retries} attempts: {e}")
                logger.warning("Telegram bot will continue without Redis. Queue operations will be disabled.")
                redis_client = None


def get_bot_instance():
    """
    Получить глобальный Bot экземпляр (создается при первом вызове).
    
    Returns:
        Bot экземпляр
    """
    global _global_bot
    if _global_bot is None:
        if not TELEGRAM_BOT_TOKEN:
            raise ValueError("TELEGRAM_BOT_TOKEN not set")
        from telegram import Bot
        _global_bot = Bot(token=TELEGRAM_BOT_TOKEN)
    return _global_bot


def _format_clarification_message(
    order_number: Optional[str],
    clarification_questions: List[str],
    unfound_products: List[str],
    parsed_products: Optional[List[Dict[str, Any]]] = None
) -> str:
    """
    Форматирование текста уточняющих вопросов.

    Args:
        order_number: Номер заказа (если заказ уже создан)
        clarification_questions: Список уточняющих вопросов
        unfound_products: Список товаров, которые не были найдены
        parsed_products: Список найденных товаров (для показа что уже распознано)

    Returns:
        Отформатированный текст сообщения
    """
    message_parts = []

    # Заголовок
    if order_number:
        message_parts.append(f"📋 Заявка #{order_number} принята!\n")
    else:
        message_parts.append("📋 Обработка вашего заказа\n")

    # Показываем что уже распознано (товары)
    if parsed_products:
        message_parts.append("✅ Я понял ваш заказ:")
        for product in parsed_products:
            product_name = product.get('name', 'Неизвестно')
            quantity = product.get('quantity', 1)
            message_parts.append(f"   • {product_name} — {quantity} шт.")
        message_parts.append("")

    # Товары не найдены
    if unfound_products:
        message_parts.append("❓ Эти товары не найдены в каталоге:")
        for product in unfound_products:
            message_parts.append(f"   • {product}")
        message_parts.append("")
        message_parts.append("Уточните артикул или полное название товара из нашего каталога.\n")

    # Уточняющие вопросы
    if clarification_questions:
        message_parts.append("📝 Для оформления заказа нужна дополнительная информация:\n")
        for i, question in enumerate(clarification_questions, 1):
            message_parts.append(f"{i}. {question}")
        message_parts.append("")
        message_parts.append("💬 Просто ответьте одним сообщением — я подхвачу ваши данные.")
        message_parts.append("")
        message_parts.append("💡 Пример ответа:")
        message_parts.append("Иван Иванов, +79991234567, г. Иркутск, ул. Шукшина, д. 60, кв. 15")

    if not clarification_questions and not unfound_products:
        message_parts.append("❓ Требуется уточнение информации по заказу.")

    return "\n".join(message_parts)


async def send_to_queue(message_data: dict) -> bool:
    """
    Отправить сообщение в Redis Queue с retry логикой.
    
    Args:
        message_data: Словарь с данными сообщения
        
    Returns:
        True если успешно, False в противном случае
    """
    if not redis_client:
        logger.error("Redis client not initialized")
        return False
    
    # Используем синхронную версию через asyncio.to_thread для совместимости
    # с существующим кодом, который использует синхронный redis клиент
    try:
        import asyncio
        result = await asyncio.to_thread(
            send_to_queue_sync,
            redis_client,
            message_data,
            queue_key=QUEUE_KEY
        )
        
        # Уведомление администратора при ошибке (только если не удалось после всех попыток)
        if not result and TELEGRAM_ADMIN_ID:
                    try:
                        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
                        await app.bot.send_message(
                            chat_id=TELEGRAM_ADMIN_ID,
                    text="⚠️ Критическая ошибка: не удалось отправить сообщение в очередь Redis после всех попыток."
                        )
                    except Exception as notify_error:
                        logger.error(f"Failed to notify admin: {notify_error}")
    
        return result
    except Exception as e:
        logger.error(f"Error sending to queue: {e}", exc_info=True)
    return False


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    user = update.effective_user
    
    # Проверка авторизации
    # Проверяем авторизацию в отдельном потоке
    is_authorized = await asyncio.to_thread(TelegramUserService.is_authorized, user.id) if user else False
    if user and is_authorized:
        # Пользователь уже авторизован - получаем информацию о пользователе (в отдельном потоке)
        user_info = await asyncio.to_thread(TelegramUserService.get_user_info, user.id)
        phone = user_info.get('phone') if user_info else None
        
        # Формируем имя для отображения
        name_parts = []
        if user.first_name:
            name_parts.append(user.first_name)
        if user.last_name:
            name_parts.append(user.last_name)
        display_name = " ".join(name_parts) if name_parts else "Пользователь"
        
        # Приветственное сообщение для авторизованного пользователя
        welcome_message = (
            "👋 Добро пожаловать обратно!\n\n"
            f"👤 Имя: {display_name}\n"
            f"📞 Телефон: {phone or 'Не указан'}\n\n"
            "Вы уже авторизованы и можете оформлять заказы.\n\n"
            "Как оформить заказ:\n"
            "Просто напишите, что хотите заказать. Например:\n"
            "• Хочу 2 варочные панели по 120 тысяч\n"
            "• Нужен товар с артикулом ФР-00000044, количество 1\n"
            "• Заказ: 3 шубы норковые, доставка в Москву\n\n"
            "Я автоматически распознаю товары, количество и контакты.\n"
            "Если что-то неясно - задам уточняющие вопросы.\n\n"
            "Полезные команды:\n"
            "• /cancel_payment - отменить активную сессию оплаты\n"
            "• /help - справка\n"
            "• /status - статус системы"
        )
        # Показываем клавиатуру с кнопкой "Мои заказы"
        authorized_keyboard = get_authorized_keyboard()
        await update.message.reply_text(welcome_message, reply_markup=authorized_keyboard)
        return
    
    # Пользователь не авторизован - требуем авторизацию
    keyboard = [
        [KeyboardButton("📱 Авторизоваться по номеру телефона", request_contact=True)]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)
    
    welcome_message = (
        "👋 Добро пожаловать в SmartOrder Engine!\n\n"
        "Я помогу вам оформить заказ быстро и удобно.\n\n"
        "⚠️ Для работы необходима авторизация по номеру телефона.\n\n"
        "Нажмите кнопку ниже, чтобы авторизоваться.\n\n"
        "После авторизации вы сможете:\n"
        "✅ Оформлять заказы простым текстом\n"
        "✅ Отслеживать все ваши заказы из всех каналов\n"
        "✅ Получать уведомления о статусе заказов\n\n"
        "Без авторизации бот не будет обрабатывать ваши сообщения."
    )
    await update.message.reply_text(welcome_message, reply_markup=reply_markup)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help."""
    help_message = (
        "📖 Справка по использованию бота\n\n"
        "Команды:\n"
        "• /start - Начать работу с ботом\n"
        "• /help - Показать эту справку\n"
        "• /status - Проверить статус системы\n"
        "• /my_orders - Показать мои заказы\n"
        "• /cancel_payment - Отменить активную сессию оплаты\n\n"
        "Как оформить заказ:\n"
        "Просто напишите сообщение с описанием товара и количеством.\n"
        "Я автоматически распознаю:\n"
        "✅ Название товара или артикул\n"
        "✅ Количество\n"
        "✅ Цену (если указана)\n"
        "✅ Адрес доставки\n"
        "✅ Контактные данные\n\n"
        "Примеры заказов:\n"
        "• Хочу 2 варочные панели по 120 тысяч\n"
        "• Заказ: ФР-00000044, количество 1, доставка в Москву\n"
        "• Нужно 3 шубы норковые по 50000, доставка в Санкт-Петербург\n\n"
        "Что происходит после отправки заказа:\n"
        "1. Я обрабатываю ваш заказ (несколько секунд)\n"
        "2. Если нужны уточнения - задам вопросы\n"
        "3. После обработки вы получите подтверждение с деталями\n"
        "4. Заказ будет создан и отправлен менеджеру\n\n"
        "Отслеживание заказов:\n"
        "Все ваши заказы из всех каналов (Telegram, почта, формы) доступны по команде /my_orders."
    )
    await update.message.reply_text(help_message)


def get_authorized_keyboard():
    """Возвращает клавиатуру для авторизованных пользователей."""
    keyboard = [
        [KeyboardButton("📋 Мои заказы")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /status."""
    try:
        # Проверка Redis (в отдельном потоке)
        redis_status = "❌ Не подключен"
        if redis_client:
            try:
                await asyncio.to_thread(redis_client.ping)
                redis_status = "✅ Подключен"
            except Exception:
                redis_status = "❌ Недоступен"
        
        # Проверка очереди (в отдельном потоке)
        queue_length = 0
        if redis_client:
            try:
                queue_length = await asyncio.to_thread(redis_client.llen, QUEUE_KEY)
            except Exception:
                pass
        
        status_message = (
            "📊 Статус системы:\n\n"
            f"Redis: {redis_status}\n"
            f"Сообщений в очереди: {queue_length}\n\n"
            "Система работает нормально." if redis_status == "✅ Подключен" else "Обнаружены проблемы с подключением."
        )
        await update.message.reply_text(status_message)
    except Exception as e:
        logger.error(f"Error in status command: {e}")
        await update.message.reply_text("❌ Ошибка при проверке статуса системы.")


async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик авторизации по номеру телефона."""
    if not update.message or not update.message.contact:
        return
    
    user = update.effective_user
    contact = update.message.contact
    
    # Проверяем, что контакт принадлежит пользователю
    if contact.user_id and contact.user_id != user.id:
        await update.message.reply_text(
            "❌ Пожалуйста, авторизуйтесь своим номером телефона."
        )
        return
    
    phone_number = contact.phone_number
    
    # Проверяем, не авторизован ли уже пользователь (в отдельном потоке)
    is_already_authorized = await asyncio.to_thread(TelegramUserService.is_authorized, user.id)
    
    # Сохраняем/обновляем пользователя в БД (в отдельном потоке)
    success = await asyncio.to_thread(
        TelegramUserService.authorize_user,
        telegram_user_id=user.id,
        phone=phone_number,
        first_name=user.first_name,
        last_name=user.last_name,
        username=user.username
    )
    
    if success:
        # Формируем имя для отображения
        name_parts = []
        if user.first_name:
            name_parts.append(user.first_name)
        if user.last_name:
            name_parts.append(user.last_name)
        display_name = " ".join(name_parts) if name_parts else "Пользователь"
        
        # Получаем клавиатуру для авторизованных пользователей
        authorized_keyboard = get_authorized_keyboard()
        
        if is_already_authorized:
            # Пользователь уже был авторизован - просто обновляем данные
            confirmation_message = (
                f"✅ Данные обновлены!\n\n"
                f"👤 Имя: {display_name}\n"
                f"📞 Телефон: {phone_number}\n\n"
                "Вы уже авторизованы и можете продолжать работу."
            )
        else:
            # Первая авторизация
            confirmation_message = (
                f"✅ Авторизация успешна!\n\n"
                f"👤 Имя: {display_name}\n"
                f"📞 Телефон: {phone_number}\n\n"
                "Теперь вы можете:\n"
                "• Оформлять заказы (просто напишите, что хотите заказать)\n"
                "• Отслеживать все ваши заказы из всех каналов (Telegram, почта, формы)\n\n"
                "Используйте кнопку '📋 Мои заказы' для просмотра ваших заказов."
            )
        
        await update.message.reply_text(confirmation_message, reply_markup=authorized_keyboard)
        logger.info(f"User {user.id} authorized with phone {phone_number}, name: {display_name}")
    else:
        # Ошибка авторизации
        error_message = (
            "❌ Ошибка авторизации.\n\n"
            "Возможно, этот номер телефона уже используется другим пользователем.\n"
            "Попробуйте позже или свяжитесь с администратором."
        )
        await update.message.reply_text(error_message)
        logger.error(f"Failed to authorize user {user.id} with phone {phone_number}")


def parse_card_data_from_message(message: str) -> Optional[Dict[str, Any]]:
    """
    Парсинг данных карты из текстового сообщения.
    
    Ожидаемый формат:
    Номер карты: 1234567890123456
    CVV: 123
    Срок действия: 12/25
    Имя держателя: Иван Иванов
    
    Args:
        message: Текст сообщения
        
    Returns:
        Словарь с данными карты или None если не удалось распарсить
    """
    try:
        # Извлечение номера карты (16 цифр, возможно с пробелами/дефисами)
        card_match = re.search(r'(?:номер|карт[аы]|card|number)[:\s]+([\d\s-]{13,19})', message, re.IGNORECASE)
        if not card_match:
            # Альтернативный паттерн: просто 16 цифр
            card_match = re.search(r'\b(\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4})\b', message)
        
        if not card_match:
            return None
        
        card_number = re.sub(r'[\s-]', '', card_match.group(1))
        if len(card_number) != 16 or not card_number.isdigit():
            return None
        
        # Извлечение CVV (3 цифры)
        cvv_match = re.search(r'(?:cvv|cvc|код)[:\s]+(\d{3,4})', message, re.IGNORECASE)
        if not cvv_match:
            # Альтернативный паттерн: просто 3-4 цифры после "CVV" или "код"
            cvv_match = re.search(r'(?:cvv|cvc|код)[\s:]+(\d{3,4})', message, re.IGNORECASE)
        
        if not cvv_match:
            return None
        
        cvv = cvv_match.group(1)
        if len(cvv) not in [3, 4] or not cvv.isdigit():
            return None
        
        # Извлечение срока действия (MM/YY или MM/YYYY)
        expiry_match = re.search(r'(?:срок|действи[ия]|expir|expiry|valid)[:\s]+(\d{1,2}[/-]\d{2,4})', message, re.IGNORECASE)
        if not expiry_match:
            # Альтернативный паттерн: просто MM/YY
            expiry_match = re.search(r'\b(\d{1,2}[/-]\d{2,4})\b', message)
        
        if not expiry_match:
            return None
        
        expiry = expiry_match.group(1).replace('-', '/')
        # Нормализация формата MM/YY
        parts = expiry.split('/')
        if len(parts) == 2:
            month = parts[0].zfill(2)
            year = parts[1]
            if len(year) == 4:
                year = year[-2:]  # Берем последние 2 цифры
            expiry = f"{month}/{year}"
        else:
            return None
        
        # Извлечение имени держателя
        holder_match = re.search(r'(?:имя|держател|holder|name)[:\s]+([А-Яа-яA-Za-z\s]{2,50})', message, re.IGNORECASE)
        if not holder_match:
            # Альтернативный паттерн: просто имя после "Имя" или "Name"
            holder_match = re.search(r'(?:имя|name)[:\s]+([А-Яа-яA-Za-z\s]{2,50})', message, re.IGNORECASE)
        
        holder_name = holder_match.group(1).strip() if holder_match else "Card Holder"
        
        return {
            "number": card_number,
            "cvv": cvv,
            "expiry": expiry,
            "holder_name": holder_name
        }
    except Exception as e:
        logger.warning(f"Error parsing card data: {e}")
        return None


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений."""
    if not update.message or not update.message.text:
        return
    
    message_text = update.message.text.strip()
    
    # Игнорируем пустые сообщения
    if not message_text:
        return
    
    user = update.effective_user
    
    # ОБЯЗАТЕЛЬНАЯ ПРОВЕРКА АВТОРИЗАЦИИ (в отдельном потоке, чтобы не блокировать)
    is_authorized = await asyncio.to_thread(TelegramUserService.is_authorized, user.id) if user else False
    if not user or not is_authorized:
        # Пользователь не авторизован - бот не работает
        keyboard = [
            [KeyboardButton("📱 Авторизоваться по номеру телефона", request_contact=True)]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)
        
        error_message = (
            "⚠️ Бот не работает без авторизации.\n\n"
            "Для работы с ботом необходимо авторизоваться по номеру телефона.\n"
            "Нажмите кнопку ниже для авторизации.\n\n"
            "Без авторизации ваши сообщения не будут обрабатываться."
        )
        await update.message.reply_text(error_message, reply_markup=reply_markup)
        return
    
    # Обновляем время последней активности (в фоне, не блокируем)
    asyncio.create_task(asyncio.to_thread(TelegramUserService.update_last_activity, user.id))
    
    # ПРОВЕРКА КОНТЕКСТА ОПЛАТЫ - если пользователь отправляет данные карты
    if redis_client:
        try:
            # Ищем активный контекст оплаты (в отдельном потоке)
            payment_pattern = f"payment:{user.id}:*"
            keys = await asyncio.to_thread(redis_client.keys, payment_pattern)
            
            if keys:
                # Найдена активная сессия оплаты (в отдельном потоке)
                order_id = await asyncio.to_thread(redis_client.get, keys[0])
                if order_id:
                    order_id = order_id.decode('utf-8') if isinstance(order_id, bytes) else order_id
                    
                    # Пытаемся распарсить данные карты
                    card_data = parse_card_data_from_message(message_text)
                    if card_data:
                        # Обрабатываем оплату
                        try:
                            from src.services.payment_processor import PaymentProcessor
                            
                            # Обрабатываем оплату (в отдельном потоке, чтобы не блокировать)
                            result = await asyncio.to_thread(PaymentProcessor.process_payment, order_id, card_data)
                            
                            # Удаляем контекст оплаты (в отдельном потоке)
                            await asyncio.to_thread(redis_client.delete, keys[0])
                            
                            # Отправляем подтверждение
                            await update.message.reply_text(
                                f"✅ Оплата успешно обработана!\n\n"
                                f"📄 Номер транзакции: {result['transaction_id']}\n"
                                f"💳 Карта: ****{result['card_last4']}\n"
                                f"💰 Сумма: {result['amount']:.2f}₽\n"
                                f"📅 Дата оплаты: {result['paid_at'][:19]}\n\n"
                                f"Заказ будет отправлен в ближайшее время."
                            )
                            
                            logger.info(f"Payment processed successfully for order {order_id} by user {user.id}")
                            return
                            
                        except Exception as e:
                            logger.error(f"Error processing payment: {e}", exc_info=True)
                            await update.message.reply_text(
                                f"❌ Ошибка обработки оплаты: {str(e)}\n\n"
                                f"Пожалуйста, проверьте данные карты и попробуйте снова."
                            )
                            return
                    else:
                        # Данные карты не распознаны - показываем детальную информацию
                        # Пытаемся извлечь частично распознанные данные
                        card_number_match = re.search(r'(\d{13,19})', message_text.replace(' ', '').replace('-', ''))
                        cvv_match = re.search(r'(?:cvv|cvc|код)[:\s]+(\d{3,4})', message_text, re.IGNORECASE)
                        expiry_match = re.search(r'(\d{1,2}[/-]\d{2,4})', message_text)
                        holder_match = re.search(r'(?:имя|держател|holder|name)[:\s]+([А-Яа-яA-Za-z\s]{2,50})', message_text, re.IGNORECASE)
                        
                        recognized_parts = []
                        missing_parts = []
                        
                        if card_number_match and len(card_number_match.group(1)) == 16:
                            recognized_parts.append("✅ Номер карты")
                        else:
                            missing_parts.append("❌ Номер карты (нужно 16 цифр)")
                        
                        if cvv_match:
                            recognized_parts.append("✅ CVV")
                        else:
                            missing_parts.append("❌ CVV (нужно 3-4 цифры)")
                        
                        if expiry_match:
                            recognized_parts.append("✅ Срок действия")
                        else:
                            missing_parts.append("❌ Срок действия (формат: MM/YY)")
                        
                        if holder_match:
                            recognized_parts.append("✅ Имя держателя")
                        else:
                            missing_parts.append("❌ Имя держателя")
                        
                        error_message = "❌ Не удалось полностью распознать данные карты.\n\n"
                        
                        if recognized_parts:
                            error_message += "✅ Распознано:\n"
                            for part in recognized_parts:
                                error_message += f"   {part}\n"
                            error_message += "\n"
                        
                        if missing_parts:
                            error_message += "❌ Отсутствует или неверно:\n"
                            for part in missing_parts:
                                error_message += f"   {part}\n"
                            error_message += "\n"
                        
                        error_message += (
                            "📋 Правильный формат:\n\n"
                            "Номер карты: 1234567890123456\n"
                            "CVV: 123\n"
                            "Срок действия: 12/25\n"
                            "Имя держателя: Иван Иванов\n\n"
                            "💡 Пример:\n"
                            "Номер карты: 4111111111111111\n"
                            "CVV: 123\n"
                            "Срок действия: 12/25\n"
                            "Имя держателя: Test User"
                        )
                        
                        await update.message.reply_text(error_message)
                        return
        except Exception as e:
            logger.warning(f"Error checking payment context: {e}")
    
    # Проверяем, не нажал ли пользователь кнопку "Мои заказы"
    if message_text.strip() in ["📋 Мои заказы", "Мои заказы", "мои заказы"]:
        # Вызываем функцию показа заказов
        await my_orders_command(update, context)
        return
    
    # Получаем информацию о пользователе из БД (в отдельном потоке)
    user_info = await asyncio.to_thread(TelegramUserService.get_user_info, user.id)
    phone = user_info.get('phone') if user_info else None
    
    # Формируем имя из Telegram
    customer_name = None
    if user.first_name or user.last_name:
        name_parts = []
        if user.first_name:
            name_parts.append(user.first_name)
        if user.last_name:
            name_parts.append(user.last_name)
        customer_name = " ".join(name_parts)
    
    chat = update.effective_chat
    
    # ── Проверка контекста уточнения (ответ на вопросы бота) ──────────────────────
    clarification_context_key = f"clarification:{user.id}"
    clarification_ctx: Optional[dict] = None
    if redis_client:
        try:
            raw_ctx = await asyncio.to_thread(redis_client.get, clarification_context_key)
            if raw_ctx:
                raw_ctx_str = raw_ctx.decode('utf-8') if isinstance(raw_ctx, bytes) else raw_ctx
                try:
                    clarification_ctx = json.loads(raw_ctx_str)
                    logger.info(
                        f"Found clarification context for user {user.id}, "
                        f"order_id={clarification_ctx.get('order_id')}"
                    )
                except (json.JSONDecodeError, ValueError):
                    clarification_ctx = {"original_message": raw_ctx_str}
        except Exception as e:
            logger.warning(f"Failed to get clarification context: {e}")

    # Строим сообщение для очереди
    # При ответе на уточнение НЕ накапливаем историю — передаём только вопрос и ответ.
    # Это предотвращает путаницу AI при многораундовых уточнениях.
    existing_order_id: Optional[str] = None
    known_name_from_ctx: Optional[str] = None
    known_phone_from_ctx: Optional[str] = None
    clarification_context_products: Optional[list] = None
    known_address_from_ctx: Optional[str] = None

    if clarification_ctx:
        # Для уточнений передаём только текст ответа клиента.
        # Структурированные данные (список товаров, известные поля) передаются
        # отдельными ключами в message_data — AI использует специальный промпт.
        full_message = message_text

        existing_order_id = clarification_ctx.get("order_id")
        known_name_from_ctx = clarification_ctx.get("known_name")
        known_phone_from_ctx = clarification_ctx.get("known_phone")
        known_address_from_ctx = clarification_ctx.get("known_address")
        clarification_context_products = clarification_ctx.get("products", [])

        # Удаляем контекст — он отработал
        if redis_client:
            try:
                await asyncio.to_thread(redis_client.delete, clarification_context_key)
            except Exception:
                pass
    else:
        full_message = message_text

    # ── Генерация уникального ID для сообщения ─────────────────────────────────
    telegram_message_id = update.message.message_id
    unique_message_id = f"tg_{user.id}_{telegram_message_id}_{int(datetime.now(timezone.utc).timestamp())}"

    # Проверка дублей (sending-ключ, не processed_message)
    if redis_client:
        try:
            sending_key = f"sending:{unique_message_id}"
            duplicate_check = await asyncio.to_thread(redis_client.exists, sending_key)
            if duplicate_check:
                logger.info(f"Duplicate telegram message detected: {unique_message_id}, skipping")
                await update.message.reply_text("✅ Ваше сообщение уже отправлено в обработку. Пожалуйста, подождите.")
                return
            await asyncio.to_thread(redis_client.setex, sending_key, 300, "1")
        except Exception as e:
            logger.warning(f"Failed to check duplicate for message {unique_message_id}: {e}")

    # ── Формирование сообщения для очереди ─────────────────────────────────────
    # При ответе на уточнение используем уже собранные данные из контекста
    # (если данные не установлены в Telegram профиле, берём из контекста)
    effective_name = customer_name or known_name_from_ctx
    effective_phone = phone or known_phone_from_ctx
    message_data = {
        "channel": "telegram",
        "user_id": str(user.id) if user else "unknown",
        "telegram_user_id": user.id if user else None,
        "chat_id": str(chat.id) if chat else "unknown",
        "message": full_message,
        "phone": effective_phone,
        "customer_name": effective_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message_id": unique_message_id,
        "telegram_message_id": telegram_message_id,
    }

    # Если это ответ на уточнение — передаём order_id и контекст для AI
    if existing_order_id:
        message_data["existing_order_id"] = existing_order_id
        # Структурированные данные для clarification-response промпта
        message_data["clarification_context_products"] = clarification_context_products or []
        if known_address_from_ctx:
            message_data["known_address"] = known_address_from_ctx
        logger.info(
            f"Clarification answer for existing order {existing_order_id} from user {user.id}: "
            f"context_products={len(clarification_context_products or [])}"
        )

    # Добавляем username если есть
    if user and user.username:
        message_data["username"] = user.username
    
    # Отправка в очередь
    logger.info(
        f"Sending message to queue: user_id={user.id}, message_id={update.message.message_id}, text_length={len(message_text)}, unique_message_id={unique_message_id}",
        extra={
            "user_id": user.id,
            "telegram_message_id": telegram_message_id,
            "unique_message_id": unique_message_id,
            "text_length": len(message_text),
            "channel": "telegram"
        }
    )
    success = await send_to_queue(message_data)
    
    if success:
        logger.info(
            f"Message successfully sent to queue: unique_message_id={unique_message_id}",
            extra={
                "unique_message_id": unique_message_id,
                "user_id": user.id,
                "channel": "telegram"
            }
        )
    else:
        logger.error(
            f"Failed to send message to queue: unique_message_id={unique_message_id}",
            extra={
                "unique_message_id": unique_message_id,
                "user_id": user.id,
                "channel": "telegram"
            }
        )
    
    if success:
        logger.info(f"Message {update.message.message_id} successfully sent to queue for user {user.id}")
        # Текст подтверждения зависит от того, это ответ на уточнение или новый заказ
        if existing_order_id:
            confirmation_message = (
                "✅ Спасибо! Получил ваши данные.\n\n"
                "Обрабатываю информацию и продолжаю оформление заказа...\n"
                "Вы получите подтверждение в течение нескольких секунд."
            )
        else:
            confirmation_message = (
                "✅ Ваше сообщение получено!\n\n"
                "Я обрабатываю ваш заказ — это займёт несколько секунд.\n\n"
                "Если потребуются уточнения, я задам вам вопросы.\n"
                "После обработки вы получите подтверждение заказа с суммой и деталями."
            )
        # Показываем постоянную клавиатуру с кнопкой "Мои заказы"
        authorized_keyboard = get_authorized_keyboard()
        await update.message.reply_text(confirmation_message, reply_markup=authorized_keyboard)
    else:
        # Уведомление об ошибке
        error_message = (
            "❌ Произошла ошибка\n\n"
            "Не удалось обработать ваше сообщение.\n\n"
            "Пожалуйста, попробуйте позже или свяжитесь с администратором."
        )
        await update.message.reply_text(error_message)


async def cancel_payment_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /cancel_payment - отмена активной сессии оплаты."""
    user = update.effective_user
    
    if not user:
        await update.message.reply_text("❌ Ошибка: не удалось определить пользователя.")
        return
    
    # Проверка авторизации (в отдельном потоке)
    is_authorized = await asyncio.to_thread(TelegramUserService.is_authorized, user.id)
    if not is_authorized:
        await update.message.reply_text(
            "⚠️ Для использования этой команды необходима авторизация.\n\n"
            "Используйте команду /start для авторизации."
        )
        return
    
    # Ищем активный контекст оплаты
    if redis_client:
        try:
            payment_pattern = f"payment:{user.id}:*"
            # Ищем активные контексты оплаты (в отдельном потоке)
            keys = await asyncio.to_thread(redis_client.keys, payment_pattern)
            
            if keys:
                # Удаляем все активные сессии оплаты (в отдельном потоке)
                for key in keys:
                    await asyncio.to_thread(redis_client.delete, key)
                
                await update.message.reply_text(
                    "✅ Активная сессия оплаты отменена.\n\n"
                    "Вы можете начать оплату заново, нажав кнопку 'Оплатить' в разделе 'Мои заказы'."
                )
                logger.info(f"Payment session cancelled for user {user.id}")
            else:
                await update.message.reply_text(
                    "ℹ️ У вас нет активной сессии оплаты.\n\n"
                    "Для оплаты заказа используйте команду /my_orders и нажмите кнопку 'Оплатить'."
                )
        except Exception as e:
            logger.error(f"Error cancelling payment session: {e}", exc_info=True)
            await update.message.reply_text(
                "❌ Произошла ошибка при отмене сессии оплаты.\n\n"
                "Попробуйте позже или свяжитесь с администратором."
            )
    else:
        await update.message.reply_text(
            "❌ Сервис временно недоступен.\n\n"
            "Попробуйте позже."
        )


def _format_orders_list(orders: list, title: str = "📋 Ваши заказы") -> tuple[str, list]:
    """
    Формирует текст и inline-кнопки для списка заказов.
    Returns: (message_text, keyboard_buttons)
    """
    STATUS_LABELS = {
        "new":             ("🆕", "Новый — ждём обработки"),
        "validated":       ("✅", "Подтверждён — формируем счёт"),
        "invoice_created": ("📄", "Ожидает оплаты"),
        "paid":            ("💳", "Оплачен — готовим к отправке"),
        "order_created_1c":("📋", "Передан на склад"),
        "tracking_issued": ("📦", "Трек присвоен — посылка формируется"),
        "shipped":         ("🚚", "В пути — передан курьеру"),
        "cancelled":       ("❌", "Отменён"),
    }

    shown = orders[:10]
    header = f"{title} ({len(orders)}):\n"
    lines = [header]

    keyboard_buttons = []
    for order in shown:
        emoji, label = STATUS_LABELS.get(order.status, ("❓", order.status))

        # Дата в человекочитаемом формате
        created = "—"
        if order.created_at:
            try:
                from datetime import datetime as dt
                d = dt.fromisoformat(str(order.created_at).replace("Z", "+00:00"))
                created = d.strftime("%d.%m.%Y %H:%M")
            except Exception:
                created = str(order.created_at)[:16]

        # Строка заказа
        lines.append(f"{'─' * 28}")
        lines.append(f"{emoji} {order.order_number}")
        lines.append(f"   Статус:  {label}")
        lines.append(f"   Сумма:   {order.total_amount:,.0f} ₽")
        lines.append(f"   Дата:    {created}")

        if order.tracking_number:
            lines.append(f"   Трек:    {order.tracking_number}")
        if order.customer_address:
            addr = order.customer_address
            if len(addr) > 50:
                addr = addr[:47] + "…"
            lines.append(f"   Адрес:   {addr}")

        lines.append("")

        # Кнопка оплаты для заказов, ожидающих оплаты
        if order.status == "invoice_created":
            try:
                from src.api.payments import create_payment_token, _get_base_url
                _tok = create_payment_token(str(order.id))
                _pay_url = f"{_get_base_url()}/pay/{_tok}"
                is_local = any(x in _pay_url for x in ("localhost", "127.0.0.1", "0.0.0.0"))
                if is_local:
                    keyboard_buttons.append([
                        InlineKeyboardButton(
                            f"💳 Оплатить {order.order_number}",
                            callback_data=f"pay_order_{order.id}"
                        )
                    ])
                else:
                    keyboard_buttons.append([
                        InlineKeyboardButton(
                            f"💳 Оплатить {order.order_number}",
                            url=_pay_url
                        )
                    ])
            except Exception:
                keyboard_buttons.append([
                    InlineKeyboardButton(
                        f"💳 Оплатить {order.order_number}",
                        callback_data=f"pay_order_{order.id}"
                    )
                ])

    if len(orders) > 10:
        lines.append(f"… и ещё {len(orders) - 10} заказов\n")

    if not keyboard_buttons:
        lines.append("💡 Чтобы оформить новый заказ — просто напишите сообщение.")
    else:
        lines.append("💡 Нажмите кнопку оплаты рядом с нужным заказом.")

    return "\n".join(lines), keyboard_buttons


async def my_orders_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /my_orders — показать заказы пользователя."""
    user = update.effective_user

    # Проверка авторизации (в отдельном потоке)
    is_authorized = await asyncio.to_thread(TelegramUserService.is_authorized, user.id) if user else False
    if not user or not is_authorized:
        keyboard = [
            [KeyboardButton("📱 Авторизоваться по номеру телефона", request_contact=True)]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)
        await update.message.reply_text(
            "⚠️ Для просмотра заказов необходима авторизация.\n\n"
            "Нажмите кнопку ниже для авторизации.",
            reply_markup=reply_markup
        )
        return

    # Получаем телефон пользователя из БД
    user_info = await asyncio.to_thread(TelegramUserService.get_user_info, user.id)
    phone = user_info.get('phone') if user_info else None

    if not phone:
        await update.message.reply_text(
            "❌ Ошибка: не найден номер телефона. Пожалуйста, авторизуйтесь снова.\n\n"
            "Используйте кнопку \"📱 Поделиться телефоном\" в меню."
        )
        return

    try:
        from src.services.order_service import OrderService

        orders = await asyncio.to_thread(OrderService.get_orders_by_phone, phone, user.id if user else None)

        if not orders:
            await update.message.reply_text(
                "📭 У вас пока нет заказов.\n\n"
                "Оформите заказ — просто напишите, что хотите."
            )
            return

        message_text, keyboard_buttons = _format_orders_list(orders)
        inline_keyboard = InlineKeyboardMarkup(keyboard_buttons) if keyboard_buttons else None

        await update.message.reply_text(message_text, reply_markup=inline_keyboard)

    except Exception as e:
        logger.error(f"Error in my_orders command: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ Произошла ошибка при получении заказов.\n\n"
            "Пожалуйста, попробуйте позже."
        )


@retry_with_backoff(
    max_retries=3,
    initial_delay=1.0,
    max_delay=30.0,
    exponential_base=2.0,
    jitter=True
)
async def _send_message_with_retry(bot, chat_id: int, text: str, **kwargs):
    """
    Внутренняя функция для отправки сообщения с retry логикой и обработкой rate limiting.
    
    Args:
        bot: Bot экземпляр
        chat_id: ID чата
        text: Текст сообщения
        **kwargs: Дополнительные параметры для send_message
        
    Returns:
        Результат отправки сообщения
    """
    try:
        return await bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except RetryAfter as e:
        # Обработка rate limiting от Telegram API
        retry_after = e.retry_after
        logger.warning(f"Rate limit hit, waiting {retry_after} seconds")
        await asyncio.sleep(retry_after)
        # Повторная попытка после ожидания
        return await bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except (TimedOut, NetworkError) as e:
        # Сетевые ошибки - пробрасываем для retry
        logger.warning(f"Network error sending message: {e}")
        raise
    except BadRequest as e:
        # Ошибки запроса (например, Chat not found) - не retry
        logger.error(f"Bad request sending message: {e}")
        raise


async def send_clarification_message(
    telegram_user_id: int,
    order_number: Optional[str],
    clarification_questions: List[str],
    unfound_products: List[str],
    parsed_products: Optional[List[Dict[str, Any]]] = None
):
    """
    Отправка уточняющих вопросов пользователю в Telegram.
    
    Args:
        telegram_user_id: ID пользователя Telegram
        order_number: Номер заказа (если заказ уже создан)
        clarification_questions: Список уточняющих вопросов
        unfound_products: Список товаров, которые не были найдены
        parsed_products: Список найденных товаров (для показа что уже распознано)
    """
    try:
        # Используем circuit breaker для защиты от каскадных сбоев
        circuit_breaker = get_telegram_circuit_breaker()
        bot = get_bot_instance()
        
        # Формируем сообщение через вспомогательную функцию
        message_text = _format_clarification_message(
            order_number, clarification_questions, unfound_products, parsed_products
        )
        
        # Выполняем через circuit breaker
        async def _send():
            return await _send_message_with_retry(bot, telegram_user_id, message_text)
        
        await circuit_breaker.call(_send)
        
        logger.info(f"Sent clarification message to Telegram user {telegram_user_id}")
        
    except Exception as e:
        # Проверяем, является ли это ошибкой "Chat not found" (тестовые пользователи)
        from telegram.error import BadRequest, TimedOut, NetworkError
        if isinstance(e, BadRequest) and "Chat not found" in str(e):
            logger.warning(f"Chat not found for user {telegram_user_id}, skipping clarification message (likely test user)")
            return  # Не поднимаем исключение для тестовых пользователей
        # Для сетевых ошибок и таймаутов логируем, но не поднимаем исключение
        if isinstance(e, (TimedOut, NetworkError)):
            logger.warning(f"Network error sending clarification message to {telegram_user_id}: {e}")
            return
        logger.error(f"Failed to send clarification message: {e}", exc_info=True)
        # Не поднимаем исключение, чтобы не прерывать обработку очереди


def _build_invoice_caption(
    order_number: str,
    order_data: Dict[str, Any],
    order_status: Optional[str],
    invoice_number: Optional[str],
    payment_url: Optional[str],
) -> str:
    """
    Строит caption для PDF-счёта (≤ 1024 символа).
    """
    items_lines = []
    total_items_cost = 0.0
    for item in order_data.get("items", []):
        qty        = item.get("quantity", 1)
        price      = item.get("price_at_order", 0)
        line_total = qty * price
        total_items_cost += line_total
        items_lines.append(f"  • {item.get('product_name','Н/Д')} — {qty} шт. × {price:,.0f}₽")

    delivery_cost = order_data.get("delivery_cost", 0)
    final_total   = total_items_cost + delivery_cost

    lines = [f"✅ Заказ #{order_number} создан!", ""]

    # Товары (обрезаем если слишком много)
    lines.append("🛒 Состав:")
    if len(items_lines) <= 5:
        lines.extend(items_lines)
    else:
        lines.extend(items_lines[:4])
        lines.append(f"  … и ещё {len(items_lines) - 4} поз.")
    lines.append("")

    lines.append(f"📦 Товары: {total_items_cost:,.0f}₽")
    if delivery_cost > 0:
        lines.append(f"🚚 Доставка: {delivery_cost:,.0f}₽")
    lines.append(f"💰 Итого: {final_total:,.0f}₽")
    lines.append("")

    # Контакты
    if order_data.get("customer_name"):
        lines.append(f"👤 {order_data['customer_name']}")
    if order_data.get("customer_phone"):
        lines.append(f"📞 {order_data['customer_phone']}")
    if order_data.get("customer_address"):
        addr = order_data["customer_address"]
        if len(addr) > 60:
            addr = addr[:57] + "…"
        lines.append(f"📍 {addr}")
    lines.append("")

    # Инструкция по оплате
    if (order_status == "invoice_created" or invoice_number) and payment_url:
        is_local = any(x in payment_url for x in ("localhost", "127.0.0.1", "0.0.0.0"))
        if is_local:
            lines.append(f"💳 Ссылка для оплаты:\n{payment_url}")
        else:
            lines.append("💳 Нажмите кнопку ниже для оплаты.")
        lines.append("🔒 Ссылка действует 24 часа.")
    elif order_status == "invoice_created" or invoice_number:
        lines.append("💳 Нажмите кнопку оплаты ниже.")

    caption = "\n".join(lines)
    # Telegram ограничение caption — 1024 символа
    if len(caption) > 1020:
        caption = caption[:1017] + "…"
    return caption


async def send_order_confirmation(
    telegram_user_id: int,
    order_number: str,
    order_data: Dict[str, Any],
    order_status: Optional[str] = None,
    invoice_number: Optional[str] = None,
    order_id: Optional[str] = None,
    payment_url: Optional[str] = None,
):
    """
    Отправка подтверждения заказа пользователю в Telegram.

    Если есть PDF счёт — отправляет ОДИН документ с подробным caption и кнопками.
    Если PDF ещё не готов — отправляет текстовое сообщение.

    Args:
        telegram_user_id: ID пользователя Telegram
        order_number: Номер заказа
        order_data: Данные заказа (товары, суммы, контакты)
        order_status: Текущий статус заказа
        invoice_number: Номер счёта (если уже создан)
        order_id: UUID заказа в БД
        payment_url: Ссылка на страницу оплаты
    """
    try:
        bot = get_bot_instance()
        circuit_breaker = get_telegram_circuit_breaker()

        # ── Inline-кнопки ────────────────────────────────────────────────────
        keyboard_buttons = []

        has_invoice = (order_status == "invoice_created" or bool(invoice_number))

        if has_invoice and payment_url:
            is_local = any(x in payment_url for x in ("localhost", "127.0.0.1", "0.0.0.0"))
            if is_local:
                keyboard_buttons.append([
                    InlineKeyboardButton("💳 Открыть форму оплаты", callback_data=f"pay_order_{order_id or order_number}")
                ])
            else:
                keyboard_buttons.append([
                    InlineKeyboardButton("💳 Оплатить онлайн", url=payment_url)
                ])
        elif has_invoice:
            keyboard_buttons.append([
                InlineKeyboardButton("💳 Оплатить заказ", callback_data=f"pay_order_{order_id or order_number}")
            ])

        # Кнопка "Отменить" только пока заказ не оплачен
        if order_status not in ("paid", "order_created_1c", "tracking_issued", "shipped"):
            keyboard_buttons.append([
                InlineKeyboardButton("❌ Отменить заказ", callback_data=f"cancel_order_{order_number}")
            ])

        reply_markup = InlineKeyboardMarkup(keyboard_buttons) if keyboard_buttons else None

        # ── Пытаемся отправить PDF с caption ─────────────────────────────────
        pdf_order_id = order_id or order_data.get("order_id") or order_data.get("id")
        pdf_sent = False

        if has_invoice and pdf_order_id:
            pdf_path = PROJECT_ROOT / ".tmp" / "invoices" / f"{pdf_order_id}.pdf"
            if pdf_path.exists():
                caption = _build_invoice_caption(
                    order_number, order_data, order_status, invoice_number, payment_url
                )
                try:
                    async def _send_pdf():
                        with open(pdf_path, "rb") as pdf_file:
                            return await bot.send_document(
                                chat_id=telegram_user_id,
                                document=pdf_file,
                                filename=f"Счёт_{invoice_number or order_number}.pdf",
                                caption=caption,
                                reply_markup=reply_markup,
                            )

                    sent_msg = await circuit_breaker.call(_send_pdf)
                    pdf_sent = True
                    logger.info(f"Sent invoice PDF+caption to user {telegram_user_id} for order {order_number}")

                    # Сохраняем message_id для последующего снятия кнопок после оплаты
                    if pdf_order_id and sent_msg:
                        _store_invoice_message_id(pdf_order_id, telegram_user_id, sent_msg.message_id)

                except Exception as e:
                    logger.warning(f"Failed to send invoice PDF for {order_number}: {e}")
            else:
                logger.warning(f"Invoice PDF not found at {pdf_path} for order {order_number}")

        # ── Fallback: текстовое сообщение если PDF не отправлен ──────────────
        if not pdf_sent:
            items_lines = []
            total_items_cost = 0.0
            for item in order_data.get("items", []):
                qty        = item.get("quantity", 1)
                price      = item.get("price_at_order", 0)
                line_total = qty * price
                total_items_cost += line_total
                items_lines.append(
                    f"  • {item.get('product_name', 'Н/Д')} — {qty} шт. × {price:,.0f}₽ = {line_total:,.0f}₽"
                )

            delivery_cost = order_data.get("delivery_cost", 0)
            final_total   = total_items_cost + delivery_cost

            lines = [f"✅ Заказ #{order_number} создан!\n", "🛒 Состав заказа:"]
            lines.extend(items_lines)
            lines += [
                "",
                f"📦 Товары: {total_items_cost:,.0f}₽",
            ]
            if delivery_cost > 0:
                lines.append(f"🚚 Доставка: {delivery_cost:,.0f}₽")
            lines.append(f"💰 Итого: {final_total:,.0f}₽\n")

            if order_data.get("customer_name") or order_data.get("customer_phone") or order_data.get("customer_address"):
                lines.append("👤 Данные получателя:")
                if order_data.get("customer_name"):
                    lines.append(f"  Имя: {order_data['customer_name']}")
                if order_data.get("customer_phone"):
                    lines.append(f"  Тел: {order_data['customer_phone']}")
                if order_data.get("customer_address"):
                    lines.append(f"  Адрес: {order_data['customer_address']}")
                lines.append("")

            if has_invoice:
                if invoice_number:
                    lines.append(f"📄 Счёт #{invoice_number} готов к оплате!")
                if payment_url:
                    is_local = any(x in payment_url for x in ("localhost", "127.0.0.1", "0.0.0.0"))
                    if is_local:
                        lines.append(f"\n💳 Ссылка для оплаты:\n{payment_url}")
                    lines.append("🔒 Ссылка действует 24 часа.")
            elif order_status == "paid":
                lines.append("✅ Оплата подтверждена! Готовим заказ к отправке.")
            else:
                lines.append("⏳ Счёт будет сформирован в ближайшее время.")

            message_text = "\n".join(lines)

            async def _send_text():
                return await _send_message_with_retry(
                    bot, telegram_user_id, message_text, reply_markup=reply_markup
                )

            try:
                sent_msg = await circuit_breaker.call(_send_text)
                logger.info(f"Sent text order confirmation to {telegram_user_id} for order {order_number}")
                if pdf_order_id and sent_msg:
                    _store_invoice_message_id(pdf_order_id, telegram_user_id, sent_msg.message_id)
            except Exception as send_err:
                logger.warning(f"Failed to send with keyboard: {send_err}. Retrying without...")
                try:
                    await _send_message_with_retry(bot, telegram_user_id, message_text)
                except Exception as fallback_err:
                    logger.error(f"Failed to send order confirmation (fallback): {fallback_err}", exc_info=True)
                    raise

    except Exception as e:
        logger.error(f"Failed to send order confirmation to {telegram_user_id}: {e}", exc_info=True)


def _store_invoice_message_id(order_id: str, chat_id: int, message_id: int) -> None:
    """Сохраняет message_id счёта в Redis для последующего редактирования после оплаты."""
    try:
        if redis_client:
            key = f"tg_invoice_msg:{order_id}"
            value = f"{chat_id}:{message_id}"
            redis_client.setex(key, 48 * 3600, value)  # TTL 48 часов
    except Exception as e:
        logger.warning(f"Failed to store invoice message_id for order {order_id}: {e}")


async def remove_payment_buttons(order_id: str, order_number: str) -> None:
    """
    Убирает кнопки «Оплатить» и «Отменить» из сообщения со счётом после оплаты.
    Редактирует сообщение, заменяя кнопки на метку «✅ Оплачено».
    """
    try:
        if not redis_client:
            return
        key = f"tg_invoice_msg:{order_id}"
        value = redis_client.get(key)
        if not value:
            return
        value_str = value.decode("utf-8") if isinstance(value, bytes) else value
        parts = value_str.split(":")
        if len(parts) != 2:
            return
        chat_id, message_id = int(parts[0]), int(parts[1])
        bot = get_bot_instance()
        paid_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ Оплачен — {order_number}", callback_data="already_paid")]
        ])
        await bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=paid_markup,
        )
        redis_client.delete(key)
        logger.info(f"Removed payment buttons from invoice message for order {order_id}")
    except Exception as e:
        logger.warning(f"Could not remove payment buttons for order {order_id}: {e}")


async def send_tracking_notification(
    telegram_user_id: int,
    order_number: str,
    tracking_number: str,
    order_id: Optional[str] = None,
):
    """
    Отправка уведомления о присвоении трек-номера (tracking_issued).
    Это автоматический шаг — трек присвоен, посылка формируется на складе.

    Args:
        telegram_user_id: ID пользователя Telegram
        order_number: Номер заказа
        tracking_number: Трек-номер для отслеживания
        order_id: UUID заказа (для снятия кнопок оплаты)
    """
    try:
        bot = get_bot_instance()
        circuit_breaker = get_telegram_circuit_breaker()

        # Снимаем кнопки оплаты с предыдущего сообщения
        if order_id:
            try:
                await remove_payment_buttons(order_id, order_number)
            except Exception:
                pass

        message = (
            f"📦 Заказ #{order_number} готовится к отправке!\n\n"
            f"Трек-номер присвоен:\n"
            f"  <code>{tracking_number}</code>\n\n"
            f"Как только посылка будет передана курьеру — вы получите уведомление.\n"
            f"Трек-номер уже можно использовать для отслеживания на сайте транспортной компании."
        )

        async def _send():
            return await _send_message_with_retry(
                bot, telegram_user_id, message, parse_mode="HTML"
            )

        await circuit_breaker.call(_send)
        logger.info(f"Sent tracking notification to user {telegram_user_id} for order {order_number}")

    except BadRequest as e:
        if "Chat not found" in str(e):
            logger.warning(f"Chat not found for user {telegram_user_id} (tracking notification skipped)")
            return
        logger.error(f"Failed to send tracking notification (BadRequest): {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to send tracking notification: {e}", exc_info=True)


async def send_shipped_notification(
    telegram_user_id: int,
    order_number: str,
    tracking_number: Optional[str] = None,
    order_id: Optional[str] = None,
):
    """
    Отправка уведомления о том, что посылка передана курьеру (shipped).
    Это ручной шаг менеджера в дашборде — посылка физически отправлена.

    Args:
        telegram_user_id: ID пользователя Telegram
        order_number: Номер заказа
        tracking_number: Трек-номер (если ещё не отправлялся)
        order_id: UUID заказа
    """
    try:
        bot = get_bot_instance()
        circuit_breaker = get_telegram_circuit_breaker()

        lines = [
            f"🚚 Заказ #{order_number} передан курьеру и уже в пути!",
            "",
            "Ожидайте доставку в ближайшие дни.",
        ]
        if tracking_number:
            lines += [
                "",
                f"📦 Трек-номер для отслеживания:",
                f"  <code>{tracking_number}</code>",
            ]
        lines += ["", "Спасибо, что выбрали нас! 🙏"]

        message = "\n".join(lines)

        async def _send():
            return await _send_message_with_retry(
                bot, telegram_user_id, message, parse_mode="HTML"
            )

        await circuit_breaker.call(_send)
        logger.info(f"Sent shipped notification to user {telegram_user_id} for order {order_number}")

    except BadRequest as e:
        if "Chat not found" in str(e):
            logger.warning(f"Chat not found for user {telegram_user_id} (shipped notification skipped)")
            return
        logger.error(f"Failed to send shipped notification (BadRequest): {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to send shipped notification: {e}", exc_info=True)


async def send_status_change_notification(
    telegram_user_id: int,
    order_number: str,
    old_status: str,
    new_status: str,
    tracking_number: Optional[str] = None,
    order_id: Optional[str] = None,
):
    """
    Отправка уведомления об изменении статуса заказа пользователю в Telegram.

    Args:
        telegram_user_id: ID пользователя Telegram
        order_number: Номер заказа
        old_status: Старый статус
        new_status: Новый статус
        tracking_number: Трек-номер (для статусов tracking_issued/shipped)
        order_id: UUID заказа (для снятия кнопок оплаты)
    """
    try:
        bot = get_bot_instance()
        circuit_breaker = get_telegram_circuit_breaker()

        # Снимаем кнопки оплаты при переходе на paid или позже
        if new_status in ("paid", "order_created_1c", "tracking_issued", "shipped") and order_id:
            try:
                await remove_payment_buttons(order_id, order_number)
            except Exception:
                pass

        # Формируем текст уведомления
        if new_status == "validated":
            text = (
                f"✅ Заказ #{order_number} подтверждён!\n\n"
                f"Формируем счёт на оплату — он придёт в ближайшее время."
            )
        elif new_status == "invoice_created":
            text = (
                f"📄 Счёт на оплату по заказу #{order_number} готов!\n\n"
                f"Нажмите «📋 Мои заказы» для оплаты."
            )
        elif new_status == "paid":
            text = (
                f"💳 Оплата по заказу #{order_number} подтверждена!\n\n"
                f"Передаём заказ на склад — скоро отправим. 📦"
            )
        elif new_status == "order_created_1c":
            text = (
                f"📋 Заказ #{order_number} принят складом.\n\n"
                f"Формируем посылку, скоро отправим!"
            )
        elif new_status == "tracking_issued":
            # Автоматический шаг: трек присвоен, посылка на складе готовится
            # Делегируем в специализированную функцию
            await send_tracking_notification(
                telegram_user_id=telegram_user_id,
                order_number=order_number,
                tracking_number=tracking_number or "—",
                order_id=order_id,
            )
            return
        elif new_status == "shipped":
            # Ручной шаг менеджера: посылка физически передана курьеру
            # Делегируем в специализированную функцию
            await send_shipped_notification(
                telegram_user_id=telegram_user_id,
                order_number=order_number,
                tracking_number=tracking_number,
                order_id=order_id,
            )
            return
        elif new_status == "cancelled":
            text = (
                f"❌ Заказ #{order_number} отменён.\n\n"
                f"Если это ошибка — пожалуйста, свяжитесь с менеджером."
            )
        else:
            text = (
                f"ℹ️ Статус заказа #{order_number} обновлён.\n\n"
                f"Нажмите «📋 Мои заказы» для деталей."
            )

        async def _send():
            return await _send_message_with_retry(bot, telegram_user_id, text)

        await circuit_breaker.call(_send)
        logger.info(f"Status change notification sent to {telegram_user_id}: {old_status} → {new_status}")

    except Exception as e:
        logger.error(f"Failed to send status change notification: {e}", exc_info=True)


async def send_admin_notification(message: str):
    """
    Отправка уведомления администратору в Telegram.
    
    Args:
        message: Текст уведомления
    """
    try:
        if not TELEGRAM_ADMIN_ID:
            logger.debug("TELEGRAM_ADMIN_ID not set, skipping admin notification")
            return
        
        from telegram import Bot
        
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        
        await bot.send_message(
            chat_id=int(TELEGRAM_ADMIN_ID),
            text=message
        )
        
        logger.info(f"Sent admin notification: {message[:50]}...")
        
    except Exception as e:
        logger.error(f"Failed to send admin notification: {e}", exc_info=True)


async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback_query от inline клавиатур."""
    query = update.callback_query
    
    if not query:
        return
    
    # Подтверждение получения callback
    await query.answer()
    
    # Обработка различных типов callback
    callback_data = query.data
    
    if callback_data == "show_my_orders":
        # Показать заказы пользователя
        user = query.from_user
        
        if not user:
            await query.answer("Ошибка: не удалось определить пользователя.")
            return
        
        # Проверка авторизации (в отдельном потоке)
        is_authorized = await asyncio.to_thread(TelegramUserService.is_authorized, user.id)
        if not is_authorized:
            keyboard = [
                [KeyboardButton("📱 Авторизоваться по номеру телефона", request_contact=True)]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)
            
            await query.edit_message_text(
                "⚠️ Для просмотра заказов необходима авторизация.\n\n"
                "Нажмите кнопку ниже для авторизации.",
                reply_markup=reply_markup
            )
            return
        
        # Получаем информацию о пользователе из БД (в отдельном потоке)
        user_info = await asyncio.to_thread(TelegramUserService.get_user_info, user.id)
        phone = user_info.get('phone') if user_info else None
        
        if not phone:
            await query.edit_message_text(
                "❌ Ошибка: не найден номер телефона. Пожалуйста, авторизуйтесь снова."
            )
            return
        
        try:
            from src.services.order_service import OrderService

            # Получаем заказы по телефону (в отдельном потоке)
            orders = await asyncio.to_thread(OrderService.get_orders_by_phone, phone, user.id)

            if not orders:
                await query.edit_message_text(
                    "📭 У вас пока нет заказов.\n\n"
                    "Напишите боту с описанием товара, чтобы оформить заказ."
                )
                return

            message_text, keyboard_buttons = _format_orders_list(orders)
            reply_markup = InlineKeyboardMarkup(keyboard_buttons) if keyboard_buttons else None
            await query.edit_message_text(message_text, reply_markup=reply_markup)

        except Exception as e:
            logger.error(f"Error in show_my_orders callback: {e}", exc_info=True)
            await query.answer("Произошла ошибка при получении заказов.", show_alert=True)
    
    elif callback_data.startswith("confirm_order_"):
        # Подтверждение заказа
        order_id = callback_data.replace("confirm_order_", "")
        await query.edit_message_text(
            f"✅ Заказ #{order_id} подтверждён.\n\n"
            "Ожидайте дальнейших уведомлений."
        )
        
        # Отправка подтверждения в очередь
        message_data = {
            "channel": "telegram",
            "user_id": str(query.from_user.id),
            "chat_id": str(query.message.chat.id) if query.message else "unknown",
            "message": f"CONFIRM_ORDER:{order_id}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message_id": query.message.message_id if query.message else None,
            "callback_data": callback_data
        }
        await send_to_queue(message_data)
    
    elif callback_data.startswith("pay_order_"):
        # Обработка оплаты заказа
        order_id = callback_data.replace("pay_order_", "")
        
        try:
            from src.services.order_service import OrderService

            # Получаем заказ (в отдельном потоке)
            order = await asyncio.to_thread(OrderService.get_order, order_id)
            if not order:
                await query.answer("❌ Заказ не найден.", show_alert=True)
                return

            if order.status != "invoice_created":
                await query.answer("❌ Заказ уже оплачен или не готов к оплате.", show_alert=True)
                return

            # Инструкции по тестовой оплате
            payment_message = (
                f"💳 Оплата заказа {order.order_number}\n\n"
                f"💰 Сумма к оплате: {order.total_amount:.2f}₽\n\n"
                f"Введите данные тестовой карты в ответном сообщении:\n\n"
                f"Номер: 4111111111111111\n"
                f"Срок: 12/26\n"
                f"CVV: 123\n"
                f"Имя: Test User\n\n"
                f"После отправки данных заказ будет автоматически оплачен."
            )
            
            # Сохраняем контекст оплаты в Redis (10 минут) - в отдельном потоке
            payment_context_key = f"payment:{query.from_user.id}:{order_id}"
            if redis_client:
                try:
                    await asyncio.to_thread(redis_client.setex, payment_context_key, 600, order_id)
                    logger.info(f"Saved payment context for user {query.from_user.id}, order {order_id}")
                except Exception as e:
                    logger.warning(f"Failed to save payment context: {e}")
            
            await query.edit_message_text(payment_message)
            await query.answer("Инструкции по оплате отправлены.")
            
        except Exception as e:
            logger.error(f"Error in pay_order callback: {e}", exc_info=True)
            await query.answer("❌ Произошла ошибка при обработке оплаты.", show_alert=True)
    
    elif callback_data.startswith("cancel_order_"):
        # Отмена заказа с обновлением статуса в БД
        order_number = callback_data.replace("cancel_order_", "")
        
        try:
            from src.services.order_service import OrderService

            # Получаем информацию о пользователе
            user = query.from_user
            if not user:
                await query.answer("❌ Ошибка: не удалось определить пользователя.", show_alert=True)
                return
            
            # Получаем информацию о пользователе (в отдельном потоке)
            user_info = await asyncio.to_thread(TelegramUserService.get_user_info, user.id)
            phone = user_info.get('phone') if user_info else None
            
            if not phone:
                await query.answer("❌ Ошибка: не найден номер телефона.", show_alert=True)
                return
            
            # Ищем заказ по номеру и телефону пользователя (в отдельном потоке)
            orders = await asyncio.to_thread(OrderService.get_orders_by_phone, phone, user.id)
            order = None
            for o in orders:
                if o.order_number == order_number:
                    order = o
                    break
            
            if not order:
                await query.answer("❌ Заказ не найден или не принадлежит вам.", show_alert=True)
                return
            
            # Проверяем, можно ли отменить заказ
            if order.status in ["paid", "shipped", "cancelled"]:
                status_names = {
                    "paid": "оплачен",
                    "shipped": "отправлен",
                    "cancelled": "уже отменен"
                }
                await query.answer(
                    f"❌ Невозможно отменить заказ: он уже {status_names.get(order.status, order.status)}.",
                    show_alert=True
                )
                return
            
            # Обновляем статус на cancelled (в отдельном потоке)
            updated_order = await asyncio.to_thread(OrderService.update_order_status, order.id, "cancelled")
            
            if updated_order:
                await query.edit_message_text(
                    f"❌ Заказ {order_number} отменён.\n\n"
                    f"Если у вас возникли вопросы, свяжитесь с менеджером."
                )
                await query.answer("Заказ отменён.")
                logger.info(f"Order {order.id} cancelled by user {user.id}")
            else:
                await query.answer("❌ Ошибка при отмене заказа.", show_alert=True)
                
        except Exception as e:
            logger.error(f"Error cancelling order: {e}", exc_info=True)
            await query.answer("❌ Произошла ошибка при отмене заказа.", show_alert=True)
    
    elif callback_data == "already_paid":
        # Нажатие на неактивную кнопку "✅ Оплачен"
        await query.answer("Заказ уже оплачен.", show_alert=False)

    else:
        # Неизвестный callback
        await query.answer("Неизвестная команда.", show_alert=False)


async def error_handler(update: Optional[Update], context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок."""
    logger.error(f"Update {update} caused error {context.error}")
    
    # Уведомление администратора о критических ошибках
    if TELEGRAM_ADMIN_ID and context.error:
        try:
            bot = get_bot_instance()
            await bot.send_message(
                chat_id=TELEGRAM_ADMIN_ID,
                text=f"⚠️ Ошибка в Telegram боте:\n\n{str(context.error)}"
            )
        except Exception as notify_error:
            logger.error(f"Failed to notify admin: {notify_error}")


def get_health_status() -> Dict[str, Any]:
    """
    Получение статуса здоровья бота.
    
    Returns:
        Словарь со статусом компонентов
    """
    health_status = {
        "status": "ok",
        "checks": {}
    }
    
    # Проверка Redis
    try:
        if redis_client:
            redis_client.ping()
            health_status["checks"]["redis"] = "ok"
        else:
            health_status["checks"]["redis"] = "not_initialized"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["checks"]["redis"] = f"error: {str(e)}"
        health_status["status"] = "error"
    
    # Проверка Telegram API
    try:
        if TELEGRAM_BOT_TOKEN:
            from telegram import Bot
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            # Простая проверка - получение информации о боте
            # Это синхронный вызов, но для health check это нормально
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                bot_info = loop.run_until_complete(bot.get_me())
                health_status["checks"]["telegram_api"] = "ok"
                health_status["checks"]["bot_username"] = bot_info.username if bot_info else "unknown"
            finally:
                loop.close()
        else:
            health_status["checks"]["telegram_api"] = "not_configured"
            health_status["status"] = "error"
    except Exception as e:
        health_status["checks"]["telegram_api"] = f"error: {str(e)}"
        health_status["status"] = "error"
    
    return health_status


# Глобальный флаг для graceful shutdown
shutdown_requested = False

def main():
    """Главная функция для запуска бота."""
    # Проверка обязательных переменных
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set in environment variables")
        raise ValueError("TELEGRAM_BOT_TOKEN is required")
    
    # Инициализация Redis
    init_redis()
    
    # Создание приложения
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Регистрация обработчиков
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("my_orders", my_orders_command))
    application.add_handler(CommandHandler("cancel_payment", cancel_payment_command))
    application.add_handler(MessageHandler(filters.CONTACT, handle_contact))  # Обработка контактов
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    
    # Обработчик ошибок
    application.add_error_handler(error_handler)
    
    # Запуск бота
    logger.info("Starting Telegram bot...")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES, stop_signals=None)
    except KeyboardInterrupt:
        logger.info("Telegram bot polling stopped")
        raise


if __name__ == "__main__":
    import signal
    import sys
    
    # Создание директории для логов если её нет
    os.makedirs("logs", exist_ok=True)
    
    def signal_handler(signum, frame):
        """Обработчик сигналов для graceful shutdown."""
        global shutdown_requested
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        shutdown_requested = True
    
    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        # Не делаем raise, чтобы процесс не упал с ошибкой
        # Вместо этого делаем graceful exit
        sys.exit(1)
