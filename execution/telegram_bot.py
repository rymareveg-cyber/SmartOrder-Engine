#!/usr/bin/env python3
"""
Telegram бот для приёма заказов.

Принимает текстовые сообщения от пользователей и отправляет их в Redis Queue
для дальнейшей обработки AI-парсером.
"""

import os
import json
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
import redis
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters
)

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/telegram_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Переменные окружения
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
TELEGRAM_ADMIN_ID = os.getenv('TELEGRAM_ADMIN_ID')

# Redis клиент
redis_client: Optional[redis.Redis] = None

# Redis Queue ключ
QUEUE_KEY = "orders:queue"

# Максимальное количество попыток отправки в Redis
MAX_RETRIES = 3
RETRY_DELAYS = [1, 2, 4]  # секунды


def init_redis():
    """Инициализация Redis клиента."""
    global redis_client
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=False)
        redis_client.ping()
        logger.info("Redis client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Redis: {e}")
        raise


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
    
    message_json = json.dumps(message_data, ensure_ascii=False)
    
    for attempt in range(MAX_RETRIES):
        try:
            redis_client.lpush(QUEUE_KEY, message_json)
            logger.info(f"Message sent to queue: {message_data.get('message_id')}")
            return True
        except Exception as e:
            delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
            logger.warning(f"Failed to send to queue (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(delay)
            else:
                logger.error(f"Failed to send message to queue after {MAX_RETRIES} attempts")
                # Уведомление администратора
                if TELEGRAM_ADMIN_ID:
                    try:
                        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
                        await app.bot.send_message(
                            chat_id=TELEGRAM_ADMIN_ID,
                            text=f"⚠️ Критическая ошибка: не удалось отправить сообщение в очередь Redis.\n\nОшибка: {str(e)}"
                        )
                    except Exception as notify_error:
                        logger.error(f"Failed to notify admin: {notify_error}")
    
    return False


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    welcome_message = (
        "👋 Добро пожаловать в SmartOrder Engine!\n\n"
        "Я помогу вам оформить заказ. Просто напишите, что вы хотите заказать.\n\n"
        "Например:\n"
        "• \"Хочу 2 варочные панели по 120 тысяч\"\n"
        "• \"Нужен товар с артикулом ФР-00000044, количество 1\"\n\n"
        "Используйте /help для получения справки."
    )
    await update.message.reply_text(welcome_message)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help."""
    help_message = (
        "📖 Справка по использованию бота:\n\n"
        "**Команды:**\n"
        "• /start - Начать работу с ботом\n"
        "• /help - Показать эту справку\n"
        "• /status - Проверить статус системы\n\n"
        "**Как оформить заказ:**\n"
        "Просто напишите сообщение с описанием товара и количеством.\n"
        "Вы можете указать:\n"
        "• Название товара или артикул\n"
        "• Количество\n"
        "• Цену (опционально)\n"
        "• Адрес доставки (опционально)\n\n"
        "**Примеры:**\n"
        "• \"Хочу 2 варочные панели\"\n"
        "• \"Заказ: ФР-00000044, количество 1, доставка в Москву\"\n"
        "• \"Нужно 3 шубы норковые по 50000\"\n\n"
        "После обработки вашего заказа я отправлю подтверждение."
    )
    await update.message.reply_text(help_message, parse_mode='Markdown')


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /status."""
    try:
        # Проверка Redis
        if redis_client:
            redis_client.ping()
            redis_status = "✅ Подключен"
        else:
            redis_status = "❌ Не подключен"
        
        # Проверка очереди
        queue_length = 0
        if redis_client:
            try:
                queue_length = redis_client.llen(QUEUE_KEY)
            except Exception:
                pass
        
        status_message = (
            "📊 Статус системы:\n\n"
            f"**Redis:** {redis_status}\n"
            f"**Сообщений в очереди:** {queue_length}\n\n"
            "Система работает нормально." if redis_status == "✅ Подключен" else "Обнаружены проблемы с подключением."
        )
        await update.message.reply_text(status_message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error in status command: {e}")
        await update.message.reply_text("❌ Ошибка при проверке статуса системы.")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений."""
    if not update.message or not update.message.text:
        return
    
    message_text = update.message.text.strip()
    
    # Игнорируем пустые сообщения
    if not message_text:
        return
    
    user = update.effective_user
    chat = update.effective_chat
    
    # Формирование сообщения для очереди
    message_data = {
        "channel": "telegram",
        "user_id": str(user.id) if user else "unknown",
        "chat_id": str(chat.id) if chat else "unknown",
        "message": message_text,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message_id": update.message.message_id
    }
    
    # Добавляем username если есть
    if user and user.username:
        message_data["username"] = user.username
    
    # Отправка в очередь
    success = await send_to_queue(message_data)
    
    if success:
        # Подтверждение пользователю
        confirmation_message = (
            "✅ Ваше сообщение получено и отправлено на обработку.\n\n"
            "Ожидайте подтверждения заказа."
        )
        await update.message.reply_text(confirmation_message)
    else:
        # Уведомление об ошибке
        error_message = (
            "❌ Произошла ошибка при обработке вашего сообщения.\n\n"
            "Пожалуйста, попробуйте позже или свяжитесь с администратором."
        )
        await update.message.reply_text(error_message)


async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback_query от inline клавиатур."""
    query = update.callback_query
    
    if not query:
        return
    
    # Подтверждение получения callback
    await query.answer()
    
    # Обработка различных типов callback
    callback_data = query.data
    
    if callback_data.startswith("confirm_order_"):
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
    
    elif callback_data.startswith("cancel_order_"):
        # Отмена заказа
        order_id = callback_data.replace("cancel_order_", "")
        await query.edit_message_text(
            f"❌ Заказ #{order_id} отменён."
        )
    
    else:
        # Неизвестный callback
        await query.edit_message_text("Неизвестная команда.")


async def error_handler(update: Optional[Update], context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок."""
    logger.error(f"Update {update} caused error {context.error}")
    
    # Уведомление администратора о критических ошибках
    if TELEGRAM_ADMIN_ID and context.error:
        try:
            app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
            await app.bot.send_message(
                chat_id=TELEGRAM_ADMIN_ID,
                text=f"⚠️ Ошибка в Telegram боте:\n\n{str(context.error)}"
            )
        except Exception as notify_error:
            logger.error(f"Failed to notify admin: {notify_error}")


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
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    
    # Обработчик ошибок
    application.add_error_handler(error_handler)
    
    # Запуск бота
    logger.info("Starting Telegram bot...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    # Создание директории для логов если её нет
    os.makedirs("logs", exist_ok=True)
    
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise
