#!/usr/bin/env python3
"""
IMAP парсер для Яндекс.Почты.

Мониторит входящие письма, извлекает заказы и отправляет их в Redis Queue
для дальнейшей обработки AI-парсером.
"""

import os
import imaplib
import email
import base64
import json
import logging
import time
import re
from datetime import datetime, timezone
from email.header import decode_header
from email.utils import parsedate_to_datetime
from typing import Optional, List, Dict
from html.parser import HTMLParser

from dotenv import load_dotenv
import redis

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/yandex_mail_parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Переменные окружения
YANDEX_MAIL_IMAP_HOST = os.getenv('YANDEX_MAIL_IMAP_HOST', 'imap.yandex.ru')
YANDEX_MAIL_EMAIL = os.getenv('YANDEX_MAIL_EMAIL')
YANDEX_MAIL_PASSWORD = os.getenv('YANDEX_MAIL_PASSWORD')
YANDEX_MAIL_FOLDER = os.getenv('YANDEX_MAIL_FOLDER', 'INBOX')
YANDEX_MAIL_POLL_INTERVAL = int(os.getenv('YANDEX_MAIL_POLL_INTERVAL', '120'))  # 2 минуты
YANDEX_MAIL_WHITELIST = os.getenv('YANDEX_MAIL_WHITELIST', '').split(',') if os.getenv('YANDEX_MAIL_WHITELIST') else []
YANDEX_MAIL_SUBJECT_KEYWORDS = os.getenv('YANDEX_MAIL_SUBJECT_KEYWORDS', '').lower().split(',') if os.getenv('YANDEX_MAIL_SUBJECT_KEYWORDS') else []
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Redis клиент
redis_client: Optional[redis.Redis] = None

# Redis Queue ключ
QUEUE_KEY = "orders:queue"

# Максимальный размер вложения (10MB)
MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024

# Поддерживаемые форматы вложений
SUPPORTED_ATTACHMENT_EXTENSIONS = ['.xlsx', '.xls', '.doc', '.docx', '.pdf', '.txt', '.csv']

# Максимальное количество попыток
MAX_RETRIES = 3
RETRY_DELAYS = [5, 10, 20]  # секунды для IMAP reconnect


class HTMLTextExtractor(HTMLParser):
    """Парсер HTML для извлечения текста."""
    
    def __init__(self):
        super().__init__()
        self.text = []
        self.in_script = False
        self.in_style = False
    
    def handle_starttag(self, tag, attrs):
        if tag.lower() in ['script', 'style']:
            self.in_script = True
            self.in_style = True
    
    def handle_endtag(self, tag):
        if tag.lower() in ['script', 'style']:
            self.in_script = False
            self.in_style = False
        elif tag.lower() in ['p', 'br', 'div', 'tr']:
            self.text.append('\n')
    
    def handle_data(self, data):
        if not self.in_script and not self.in_style:
            self.text.append(data)
    
    def get_text(self):
        return ' '.join(self.text).strip()


def decode_mime_words(s):
    """Декодирование MIME заголовков."""
    decoded_parts = decode_header(s)
    decoded_str = ''
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            if encoding:
                try:
                    decoded_str += part.decode(encoding)
                except (UnicodeDecodeError, LookupError):
                    decoded_str += part.decode('utf-8', errors='ignore')
            else:
                try:
                    decoded_str += part.decode('utf-8')
                except UnicodeDecodeError:
                    decoded_str += part.decode('windows-1251', errors='ignore')
        else:
            decoded_str += part
    return decoded_str


def get_email_body(msg) -> str:
    """Извлечение текста из email сообщения."""
    body = ""
    
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition", ""))
            
            # Пропускаем вложения
            if "attachment" in content_disposition:
                continue
            
            # Plain text
            if content_type == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or 'utf-8'
                    try:
                        body += payload.decode(charset, errors='ignore')
                    except (UnicodeDecodeError, LookupError):
                        body += payload.decode('utf-8', errors='ignore')
            
            # HTML fallback
            elif content_type == "text/html" and not body:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or 'utf-8'
                    try:
                        html_content = payload.decode(charset, errors='ignore')
                    except (UnicodeDecodeError, LookupError):
                        html_content = payload.decode('utf-8', errors='ignore')
                    
                    # Парсинг HTML
                    parser = HTMLTextExtractor()
                    parser.feed(html_content)
                    body = parser.get_text()
    else:
        # Простое письмо
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or 'utf-8'
            try:
                body = payload.decode(charset, errors='ignore')
            except (UnicodeDecodeError, LookupError):
                body = payload.decode('utf-8', errors='ignore')
    
    return body.strip()


def get_attachments(msg) -> List[Dict[str, str]]:
    """Извлечение вложений из email."""
    attachments = []
    
    if not msg.is_multipart():
        return attachments
    
    for part in msg.walk():
        content_disposition = str(part.get("Content-Disposition", ""))
        
        if "attachment" not in content_disposition:
            continue
        
        filename = part.get_filename()
        if not filename:
            continue
        
        # Декодирование имени файла
        filename = decode_mime_words(filename)
        
        # Проверка расширения
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in SUPPORTED_ATTACHMENT_EXTENSIONS:
            logger.warning(f"Unsupported attachment format: {filename}")
            continue
        
        # Получение содержимого
        payload = part.get_payload(decode=True)
        if not payload:
            continue
        
        # Проверка размера
        if len(payload) > MAX_ATTACHMENT_SIZE:
            logger.warning(f"Attachment too large ({len(payload)} bytes): {filename}, skipping")
            continue
        
        # Кодирование в base64
        try:
            content_b64 = base64.b64encode(payload).decode('utf-8')
            attachments.append({
                "filename": filename,
                "content": content_b64
            })
            logger.info(f"Extracted attachment: {filename} ({len(payload)} bytes)")
        except Exception as e:
            logger.error(f"Failed to encode attachment {filename}: {e}")
    
    return attachments


def should_process_email(from_email: str, subject: str) -> bool:
    """Проверка, нужно ли обрабатывать письмо."""
    # Проверка whitelist
    if YANDEX_MAIL_WHITELIST:
        from_email_lower = from_email.lower()
        if not any(whitelist_email.lower() in from_email_lower for whitelist_email in YANDEX_MAIL_WHITELIST if whitelist_email.strip()):
            logger.info(f"Email from {from_email} not in whitelist, skipping")
            return False
    
    # Проверка ключевых слов в теме
    if YANDEX_MAIL_SUBJECT_KEYWORDS:
        subject_lower = subject.lower()
        if not any(keyword.strip() in subject_lower for keyword in YANDEX_MAIL_SUBJECT_KEYWORDS if keyword.strip()):
            logger.info(f"Email subject '{subject}' doesn't contain keywords, skipping")
            return False
    
    return True


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


def send_to_queue(message_data: dict) -> bool:
    """Отправить сообщение в Redis Queue с retry логикой."""
    if not redis_client:
        logger.error("Redis client not initialized")
        return False
    
    message_json = json.dumps(message_data, ensure_ascii=False)
    
    for attempt in range(MAX_RETRIES):
        try:
            redis_client.lpush(QUEUE_KEY, message_json)
            logger.info(f"Message sent to queue: {message_data.get('subject')}")
            return True
        except Exception as e:
            delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)] if attempt < MAX_RETRIES - 1 else 0
            logger.warning(f"Failed to send to queue (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            
            if attempt < MAX_RETRIES - 1:
                time.sleep(delay)
            else:
                logger.error(f"Failed to send message to queue after {MAX_RETRIES} attempts")
    
    return False


def connect_imap() -> Optional[imaplib.IMAP4_SSL]:
    """Подключение к IMAP серверу с retry логикой."""
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Connecting to IMAP server {YANDEX_MAIL_IMAP_HOST} (attempt {attempt + 1}/{MAX_RETRIES})...")
            mail = imaplib.IMAP4_SSL(YANDEX_MAIL_IMAP_HOST, timeout=30)
            mail.login(YANDEX_MAIL_EMAIL, YANDEX_MAIL_PASSWORD)
            logger.info("Successfully connected to IMAP server")
            return mail
        except imaplib.IMAP4.error as e:
            logger.error(f"IMAP authentication error: {e}")
            return None
        except Exception as e:
            delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)] if attempt < MAX_RETRIES - 1 else 0
            logger.warning(f"Failed to connect to IMAP (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            
            if attempt < MAX_RETRIES - 1:
                time.sleep(delay)
            else:
                logger.error(f"Failed to connect to IMAP after {MAX_RETRIES} attempts")
                return None
    
    return None


def process_emails(mail: imaplib.IMAP4_SSL):
    """Обработка непрочитанных писем."""
    try:
        # Выбор папки
        status, messages = mail.select(YANDEX_MAIL_FOLDER)
        if status != 'OK':
            logger.error(f"Failed to select folder {YANDEX_MAIL_FOLDER}")
            return
        
        # Поиск непрочитанных писем
        status, messages = mail.search(None, 'UNSEEN')
        if status != 'OK':
            logger.error("Failed to search for emails")
            return
        
        email_ids = messages[0].split()
        
        if not email_ids:
            logger.debug("No new emails found")
            return
        
        logger.info(f"Found {len(email_ids)} new email(s)")
        
        # Обработка каждого письма
        for email_id in email_ids:
            try:
                # Получение письма
                status, msg_data = mail.fetch(email_id, '(RFC822)')
                if status != 'OK':
                    logger.warning(f"Failed to fetch email {email_id.decode()}")
                    continue
                
                # Парсинг письма
                email_body = msg_data[0][1]
                msg = email.message_from_bytes(email_body)
                
                # Извлечение данных
                from_email = decode_mime_words(msg.get("From", ""))
                subject = decode_mime_words(msg.get("Subject", ""))
                
                # Проверка фильтров
                if not should_process_email(from_email, subject):
                    # Помечаем как прочитанное даже если пропускаем
                    mail.store(email_id, '+FLAGS', '\\Seen')
                    continue
                
                # Извлечение текста
                body = get_email_body(msg)
                
                # Проверка на пустое письмо
                if not body:
                    logger.warning(f"Empty email from {from_email}, subject: {subject}")
                    mail.store(email_id, '+FLAGS', '\\Seen')
                    continue
                
                # Извлечение вложений
                attachments = get_attachments(msg)
                
                # Получение timestamp
                date_str = msg.get("Date", "")
                try:
                    timestamp = parsedate_to_datetime(date_str).isoformat()
                except Exception:
                    timestamp = datetime.now(timezone.utc).isoformat()
                
                # Формирование сообщения для очереди
                message_data = {
                    "channel": "yandex_mail",
                    "email": from_email,
                    "subject": subject,
                    "body": body,
                    "attachments": attachments,
                    "timestamp": timestamp
                }
                
                # Отправка в очередь
                if send_to_queue(message_data):
                    # Помечаем как прочитанное
                    mail.store(email_id, '+FLAGS', '\\Seen')
                    logger.info(f"Successfully processed email from {from_email}, subject: {subject}, attachments: {len(attachments)}")
                else:
                    logger.error(f"Failed to send email to queue, keeping as unread")
            
            except Exception as e:
                logger.error(f"Error processing email {email_id.decode()}: {e}", exc_info=True)
                # Помечаем как прочитанное даже при ошибке, чтобы не зациклиться
                try:
                    mail.store(email_id, '+FLAGS', '\\Seen')
                except Exception:
                    pass
    
    except Exception as e:
        logger.error(f"Error in process_emails: {e}", exc_info=True)


def main():
    """Главная функция для запуска парсера."""
    # Проверка обязательных переменных
    if not YANDEX_MAIL_EMAIL or not YANDEX_MAIL_PASSWORD:
        logger.error("YANDEX_MAIL_EMAIL and YANDEX_MAIL_PASSWORD must be set")
        raise ValueError("YANDEX_MAIL_EMAIL and YANDEX_MAIL_PASSWORD are required")
    
    # Инициализация Redis
    init_redis()
    
    logger.info("Starting Yandex Mail IMAP parser...")
    logger.info(f"Polling interval: {YANDEX_MAIL_POLL_INTERVAL} seconds")
    logger.info(f"Folder: {YANDEX_MAIL_FOLDER}")
    if YANDEX_MAIL_WHITELIST:
        logger.info(f"Whitelist: {', '.join(YANDEX_MAIL_WHITELIST)}")
    if YANDEX_MAIL_SUBJECT_KEYWORDS:
        logger.info(f"Subject keywords: {', '.join(YANDEX_MAIL_SUBJECT_KEYWORDS)}")
    
    mail = None
    
    try:
        while True:
            try:
                # Подключение к IMAP
                if not mail:
                    mail = connect_imap()
                    if not mail:
                        logger.error("Failed to connect to IMAP, retrying in 60 seconds...")
                        time.sleep(60)
                        continue
                
                # Обработка писем
                process_emails(mail)
                
                # Ожидание перед следующим опросом
                time.sleep(YANDEX_MAIL_POLL_INTERVAL)
            
            except imaplib.IMAP4.abort as e:
                logger.warning(f"IMAP connection aborted: {e}, reconnecting...")
                if mail:
                    try:
                        mail.logout()
                    except Exception:
                        pass
                mail = None
                time.sleep(10)
            
            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)
                if mail:
                    try:
                        mail.logout()
                    except Exception:
                        pass
                mail = None
                time.sleep(30)
    
    except KeyboardInterrupt:
        logger.info("Parser stopped by user")
    finally:
        if mail:
            try:
                mail.logout()
            except Exception:
                pass


if __name__ == "__main__":
    # Создание директории для логов если её нет
    os.makedirs("logs", exist_ok=True)
    
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise
