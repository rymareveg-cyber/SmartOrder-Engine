#!/usr/bin/env python3
"""
Централизованная конфигурация проекта.

Все переменные окружения и настройки в одном месте.
"""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# Корневая директория проекта
PROJECT_ROOT = Path(__file__).parent.parent


class DatabaseConfig:
    """Конфигурация базы данных."""
    URL: str = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/smartorder')
    POOL_MIN_CONNECTIONS: int = int(os.getenv('DB_POOL_MIN_CONNECTIONS', '2'))
    POOL_MAX_CONNECTIONS: int = int(os.getenv('DB_POOL_MAX_CONNECTIONS', '20'))


class RedisConfig:
    """Конфигурация Redis."""
    URL: str = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    QUEUE_KEY: str = os.getenv('REDIS_QUEUE_KEY', 'orders:queue')
    DEAD_LETTER_QUEUE_KEY: str = os.getenv('REDIS_DEAD_LETTER_QUEUE_KEY', 'orders:dead_letter')
    MAX_RETRIES: int = int(os.getenv('REDIS_MAX_RETRIES', '3'))


class TelegramConfig:
    """Конфигурация Telegram бота."""
    BOT_TOKEN: Optional[str] = os.getenv('TELEGRAM_BOT_TOKEN')
    ADMIN_ID: Optional[str] = os.getenv('TELEGRAM_ADMIN_ID')


class OpenAIConfig:
    """Конфигурация OpenAI API."""
    API_KEY: Optional[str] = os.getenv('OPENAI_API_KEY')
    MODEL: str = os.getenv('OPENAI_MODEL', 'gpt-4.1-mini')
    BASE_URL: str = os.getenv('OPENAI_BASE_URL', 'https://api.proxyapi.ru/openai/v1')


class MailConfig:
    """Конфигурация почты."""
    IMAP_HOST: str = os.getenv('YANDEX_MAIL_IMAP_HOST', 'imap.yandex.ru')
    EMAIL: Optional[str] = os.getenv('YANDEX_MAIL_EMAIL')
    PASSWORD: Optional[str] = os.getenv('YANDEX_MAIL_PASSWORD')
    FOLDER: str = os.getenv('YANDEX_MAIL_FOLDER', 'INBOX')
    POLL_INTERVAL: int = int(os.getenv('YANDEX_MAIL_POLL_INTERVAL', '120'))
    WHITELIST: list = os.getenv('YANDEX_MAIL_WHITELIST', '').split(',') if os.getenv('YANDEX_MAIL_WHITELIST') else []


class SMTPConfig:
    """Конфигурация SMTP."""
    HOST: str = os.getenv('SMTP_HOST', 'smtp.yandex.ru')
    PORT: int = int(os.getenv('SMTP_PORT', '465'))
    USER: Optional[str] = os.getenv('SMTP_USER')
    PASSWORD: Optional[str] = os.getenv('SMTP_PASSWORD')
    FROM_NAME: str = os.getenv('SMTP_FROM_NAME', 'SmartOrder Engine')
    FROM_EMAIL: Optional[str] = os.getenv('SMTP_FROM_EMAIL') or os.getenv('SMTP_USER')
    USE_TLS: bool = os.getenv('SMTP_USE_TLS', 'false').lower() == 'true'
    USE_SSL: bool = os.getenv('SMTP_USE_SSL', 'true').lower() == 'true'


class QueueConfig:
    """Конфигурация очереди."""
    WORKER_CONCURRENCY: int = int(os.getenv('WORKER_CONCURRENCY', '10'))
    MAX_RETRIES: int = int(os.getenv('QUEUE_MAX_RETRIES', '3'))


class APIConfig:
    """Конфигурация API серверов."""
    CATALOG_PORT: int = int(os.getenv('API_PORT', '8025'))
    WEBHOOK_PORT: int = int(os.getenv('WEBHOOK_PORT', '8026'))
    QUEUE_HEALTH_PORT: int = int(os.getenv('HEALTH_CHECK_PORT', '8027'))
    DASHBOARD_PORT: int = int(os.getenv('DASHBOARD_PORT', '8028'))
    PAYMENTS_PORT: int = int(os.getenv('PAYMENTS_PORT', '8029'))
    HOST: str = os.getenv('API_HOST', '0.0.0.0')
    # Публичный базовый URL платёжного сервиса (для генерации ссылок оплаты)
    # Пример: http://192.168.1.100:8029 или https://pay.mycompany.com
    # Если не задан — payments.py авто-определяет IP машины
    APP_BASE_URL: str = os.getenv('APP_BASE_URL', '')
    YANDEX_FORMS_SECRET: str = os.getenv('YANDEX_FORMS_SECRET', '')
    RATE_LIMIT_REQUESTS: int = int(os.getenv('RATE_LIMIT_REQUESTS', '100'))
    RATE_LIMIT_WINDOW: int = int(os.getenv('RATE_LIMIT_WINDOW', '60'))


class OneCConfig:
    """Конфигурация 1C."""
    BASE_URL: Optional[str] = os.getenv('ONEC_BASE_URL')
    USERNAME: Optional[str] = os.getenv('ONEC_USERNAME')
    PASSWORD: Optional[str] = os.getenv('ONEC_PASSWORD')
    INVOICES_ENDPOINT: str = os.getenv('ONEC_INVOICES_ENDPOINT', '/hs/invoices')
    CATALOG_ENDPOINT: str = os.getenv('ONEC_CATALOG_ENDPOINT', '/hs/api/get/catalog')
    SYNC_INTERVAL: int = int(os.getenv('ONEC_SYNC_INTERVAL', '3600'))
