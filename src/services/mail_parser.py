#!/usr/bin/env python3
"""
IMAP парсер для Яндекс.Почты.

Мониторит входящие письма, извлекает заказы и отправляет их в Redis Queue
для дальнейшей обработки AI-парсером.
"""

import imaplib
import email
import base64
import json
import time
import re
import ssl
import hashlib
from datetime import datetime, timezone
from email.header import decode_header
from email.utils import parsedate_to_datetime, parseaddr
from typing import Optional, List, Dict, Any
from html.parser import HTMLParser

from src.config import MailConfig, RedisConfig
from src.utils.logger import get_logger
from src.utils.redis_client import init_redis_client, send_to_queue_sync

logger = get_logger(__name__)

redis_client: Optional[Any] = None
MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024
SUPPORTED_ATTACHMENT_EXTENSIONS = ['.xlsx', '.xls', '.doc', '.docx', '.pdf', '.txt', '.csv']
MAX_RETRIES = 3
RETRY_DELAYS = [5, 10, 20]
RECONNECT_DELAYS = [15, 30, 60]  # Более длинные задержки для переподключения после обрыва
NOOP_INTERVAL = 60  # Интервал keep-alive NOOP в секундах


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


def strip_quoted_reply_content(body: str) -> str:
    """
    Удаляет цитированный контент из ответного письма.

    Обрабатывает форматы:
    - Gmail/Outlook: "On <date> ... wrote:"
    - Yandex.Mail: "--- Исходное сообщение ---" / "-- Исходное сообщение --"
    - Строки, начинающиеся с ">"
    - "From:" в начале строки (пересланные письма)
    """
    if not body:
        return body

    # Маркеры начала цитаты (регулярные выражения, case-insensitive)
    import re as _re
    quote_patterns = [
        # Outlook / Mail.ru: "От: ..." или "From: ..."
        r'^\s*от\s*:.+',
        r'^\s*from\s*:.+',
        # Yandex.Mail Russian reply dividers
        r'^\s*-{2,}\s*исходное сообщение\s*-{2,}',
        r'^\s*-{2,}\s*original message\s*-{2,}',
        r'^\s*-{3,}\s*',       # просто "---"
        # Gmail / Outlook: "On Mon, 19 Feb 2026 at 10:00, ... wrote:"
        r'^\s*(on|вт|пн|ср|чт|пт|сб|вс)\b.{5,80}(wrote|написал|написала)\s*:?\s*$',
        # RFC 2822 quoted header
        r'^\s*>{1,}',
    ]

    lines = body.splitlines()
    result_lines = []
    for line in lines:
        line_lower = line.lower().strip()
        is_quote_start = any(_re.match(p, line_lower) for p in quote_patterns)
        if is_quote_start:
            # Всё после этой строки — цитата, обрезаем
            break
        result_lines.append(line)

    cleaned = "\n".join(result_lines).strip()
    # Если после очистки осталось менее 5 символов — возвращаем оригинал
    # (на случай неверного срабатывания паттерна)
    return cleaned if len(cleaned) >= 5 else body.strip()


def get_email_body(msg) -> str:
    """Извлечение текста из email сообщения (без цитированных частей)."""
    body = ""
    
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition", ""))
            
            if "attachment" in content_disposition:
                continue
            
            if content_type == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or 'utf-8'
                    try:
                        body += payload.decode(charset, errors='ignore')
                    except (UnicodeDecodeError, LookupError):
                        body += payload.decode('utf-8', errors='ignore')
            
            elif content_type == "text/html" and not body:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or 'utf-8'
                    try:
                        html_content = payload.decode(charset, errors='ignore')
                    except (UnicodeDecodeError, LookupError):
                        html_content = payload.decode('utf-8', errors='ignore')
                    
                    parser = HTMLTextExtractor()
                    parser.feed(html_content)
                    body = parser.get_text()
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or 'utf-8'
            try:
                body = payload.decode(charset, errors='ignore')
            except (UnicodeDecodeError, LookupError):
                body = payload.decode('utf-8', errors='ignore')
    
    # Удаляем цитированный контент (ответы на письма содержат оригинал)
    body = strip_quoted_reply_content(body)
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
        
        filename = decode_mime_words(filename)
        
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in SUPPORTED_ATTACHMENT_EXTENSIONS:
            logger.warning(f"Unsupported attachment format: {filename}")
            continue
        
        payload = part.get_payload(decode=True)
        if not payload:
            continue
        
        if len(payload) > MAX_ATTACHMENT_SIZE:
            logger.warning(f"Attachment too large ({len(payload)} bytes): {filename}, skipping")
            continue
        
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
    if MailConfig.WHITELIST:
        from_email_lower = from_email.lower()
        if not any(whitelist_email.lower() in from_email_lower for whitelist_email in MailConfig.WHITELIST if whitelist_email.strip()):
            logger.info(f"Email from {from_email} not in whitelist, skipping")
            return False
    
    # Проверка ключевых слов в теме (если настроено)
    import os
    subject_keywords = os.getenv('YANDEX_MAIL_SUBJECT_KEYWORDS', '').lower().split(',') if os.getenv('YANDEX_MAIL_SUBJECT_KEYWORDS') else []
    if subject_keywords:
        subject_lower = subject.lower()
        if not any(keyword.strip() in subject_lower for keyword in subject_keywords if keyword.strip()):
            logger.info(f"Email subject '{subject}' doesn't contain keywords, skipping")
            return False
    
    return True


def init_redis():
    """Инициализация Redis клиента с retry."""
    global redis_client
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            redis_client = init_redis_client(
                decode_responses=False,
                socket_timeout=5,
                socket_connect_timeout=5,
                raise_on_error=True
            )
            logger.info("Redis client initialized successfully")
            return
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Failed to initialize Redis (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logger.error(f"Failed to initialize Redis after {max_retries} attempts: {e}")
                logger.warning("Mail parser will continue without Redis. Queue operations will be disabled.")
                redis_client = None


def send_to_queue(message_data: dict) -> bool:
    """Отправить сообщение в Redis Queue."""
    return send_to_queue_sync(redis_client, message_data, queue_key=RedisConfig.QUEUE_KEY)


def connect_imap(is_reconnect: bool = False) -> Optional[imaplib.IMAP4_SSL]:
    """Подключение к IMAP серверу с retry логикой.
    
    Args:
        is_reconnect: True при переподключении после обрыва — используются
                      более длинные задержки между попытками.
    """
    delays = RECONNECT_DELAYS if is_reconnect else RETRY_DELAYS
    retries = len(delays) + 1

    for attempt in range(retries):
        try:
            logger.info(f"Connecting to IMAP server {MailConfig.IMAP_HOST} (attempt {attempt + 1}/{retries})...")
            # Создаём новый SSL-контекст при каждой попытке, чтобы избежать
            # повторного использования сломанного состояния SSL
            ssl_context = ssl.create_default_context()
            mail = imaplib.IMAP4_SSL(MailConfig.IMAP_HOST, ssl_context=ssl_context)
            mail.login(MailConfig.EMAIL, MailConfig.PASSWORD)
            logger.info("Successfully connected to IMAP server")
            return mail
        except imaplib.IMAP4.error as e:
            logger.error(f"IMAP authentication error: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed to connect to IMAP (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                delay = delays[min(attempt, len(delays) - 1)]
                logger.info(f"Retrying IMAP connection in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"Failed to connect to IMAP after {retries} attempts")
                return None

    return None


def imap_noop_sleep(mail: imaplib.IMAP4_SSL, total_seconds: int) -> bool:
    """Спим total_seconds секунд, отправляя NOOP каждые NOOP_INTERVAL секунд,
    чтобы соединение не упало из-за idle-timeout сервера.

    Returns:
        True — соединение живо после сна.
        False — соединение упало, нужно переподключиться.
    """
    elapsed = 0
    while elapsed < total_seconds:
        chunk = min(NOOP_INTERVAL, total_seconds - elapsed)
        time.sleep(chunk)
        elapsed += chunk

        if elapsed >= total_seconds:
            break  # Сон закончен, NOOP перед следующим poll не нужен

        try:
            status, _ = mail.noop()
            if status != 'OK':
                logger.warning("IMAP NOOP returned non-OK status, connection may be unstable")
                return False
            logger.debug("IMAP NOOP keep-alive sent")
        except Exception as e:
            logger.warning(f"IMAP NOOP failed (connection dropped): {e}")
            return False

    return True


def process_emails(mail: imaplib.IMAP4_SSL):
    """Обработка непрочитанных писем."""
    status = None
    try:
        try:
            status, messages = mail.select(MailConfig.FOLDER)
        except (imaplib.IMAP4.abort, ssl.SSLEOFError, OSError) as e:
            msg = str(e)
            if "EOF" in msg:
                logger.warning(f"IMAP connection EOF during select (will reconnect): {e}")
            else:
                logger.error(f"IMAP connection error during select: {e}")
            raise
        
        if status != 'OK':
            logger.error(f"Failed to select folder {MailConfig.FOLDER}")
            return
        
        try:
            status, messages = mail.search(None, 'UNSEEN')
        except (imaplib.IMAP4.abort, ssl.SSLEOFError, OSError) as e:
            msg = str(e)
            if "EOF" in msg:
                logger.warning(f"IMAP connection EOF during search (will reconnect): {e}")
            else:
                logger.error(f"IMAP connection error during search: {e}")
            raise
        
        if status != 'OK':
            logger.error("Failed to search for emails")
            return
        
        email_ids = messages[0].split()
        
        if not email_ids:
            logger.debug("No new emails found")
            return
        
        logger.info(f"Found {len(email_ids)} new email(s)")
        
        for email_id in email_ids:
            try:
                try:
                    status, msg_data = mail.fetch(email_id, '(RFC822)')
                except (imaplib.IMAP4.abort, ssl.SSLEOFError, OSError) as e:
                    msg = str(e)
                    if "EOF" in msg:
                        logger.warning(f"IMAP connection EOF during fetch (will reconnect): {e}")
                    else:
                        logger.error(f"IMAP connection error during fetch: {e}")
                        raise
                
                if status != 'OK':
                    logger.warning(f"Failed to fetch email {email_id.decode()}")
                    continue
                
                email_body = msg_data[0][1]
                msg = email.message_from_bytes(email_body)
                
                from_header = decode_mime_words(msg.get("From", ""))
                subject = decode_mime_words(msg.get("Subject", ""))
                
                name, from_email = parseaddr(from_header)
                customer_name = name.strip() if name else None
                
                if not from_email:
                    from_email = from_header
                
                if not should_process_email(from_email, subject):
                    try:
                        mail.store(email_id, '+FLAGS', '\\Seen')
                    except (imaplib.IMAP4.abort, ssl.SSLEOFError, OSError) as e:
                        logger.warning(f"IMAP connection error during store (skip): {e}")
                        raise
                    continue
                
                body = get_email_body(msg)
                
                if not body:
                    logger.warning(f"Empty email from {from_email}, subject: {subject}")
                    try:
                        mail.store(email_id, '+FLAGS', '\\Seen')
                    except (imaplib.IMAP4.abort, ssl.SSLEOFError, OSError) as e:
                        logger.warning(f"IMAP connection error during store (empty): {e}")
                        raise
                    continue
                
                attachments = get_attachments(msg)
                
                date_str = msg.get("Date", "")
                try:
                    timestamp = parsedate_to_datetime(date_str).isoformat()
                except Exception:
                    timestamp = datetime.now(timezone.utc).isoformat()
                
                email_unique_id = f"mail_{email_id.decode()}_{timestamp}"
                message_hash = hashlib.md5(email_unique_id.encode()).hexdigest()[:16]
                unique_message_id = f"ymail_{message_hash}"
                
                # Проверка на дубликаты при отправке (используем другой ключ, не processed_message)
                # processed_message ставится только ПОСЛЕ успешной обработки в queue_processor
                if redis_client:
                    try:
                        sending_key = f"sending:{unique_message_id}"
                        if redis_client.exists(sending_key):
                            logger.info(f"Duplicate email message detected (already sending): {unique_message_id}, skipping")
                            try:
                                mail.store(email_id, '+FLAGS', '\\Seen')
                            except Exception:
                                pass
                            continue
                        # Временно помечаем как отправляемое (TTL 5 минут)
                        redis_client.setex(sending_key, 300, "1")
                    except Exception as e:
                        logger.warning(f"Failed to check duplicate for message {unique_message_id}: {e}")
                
                message_data = {
                    "channel": "yandex_mail",
                    "email": from_email,
                    "customer_name": customer_name,
                    "subject": subject,
                    "body": body,
                    "attachments": attachments,
                    "timestamp": timestamp,
                    "message_id": unique_message_id,
                    "email_id": email_id.decode()
                }
                
                if send_to_queue(message_data):
                    try:
                        mail.store(email_id, '+FLAGS', '\\Seen')
                    except (imaplib.IMAP4.abort, ssl.SSLEOFError, OSError) as e:
                        logger.warning(f"IMAP connection error during store: {e}, will reconnect")
                        raise
                    logger.info(f"Successfully processed email from {from_email}, subject: {subject}, attachments: {len(attachments)}")
                else:
                    logger.error(f"Failed to send email to queue, keeping as unread")
            
            except (imaplib.IMAP4.abort, ssl.SSLEOFError, OSError) as e:
                logger.error(f"IMAP connection error processing email {email_id.decode()}: {e}")
                raise
            except Exception as e:
                logger.error(f"Error processing email {email_id.decode()}: {e}", exc_info=True)
                try:
                    mail.store(email_id, '+FLAGS', '\\Seen')
                except (imaplib.IMAP4.abort, ssl.SSLEOFError, OSError):
                    raise
                except Exception:
                    pass
    
    except (imaplib.IMAP4.abort, ssl.SSLEOFError, OSError) as e:
        msg = str(e)
        if "EOF" in msg:
            logger.warning(f"IMAP connection EOF in process_emails (will reconnect): {e}")
        else:
            logger.error(f"IMAP connection error in process_emails: {e}")
            raise
    except Exception as e:
        logger.error(f"Error in process_emails: {e}", exc_info=True)


# Глобальный флаг для graceful shutdown
shutdown_requested = False

def main():
    """Главная функция для запуска парсера."""
    global shutdown_requested
    
    if not MailConfig.EMAIL or not MailConfig.PASSWORD:
        logger.error("YANDEX_MAIL_EMAIL and YANDEX_MAIL_PASSWORD must be set")
        raise ValueError("YANDEX_MAIL_EMAIL and YANDEX_MAIL_PASSWORD are required")
    
    init_redis()
    
    logger.info("Starting Yandex Mail IMAP parser...")
    logger.info(f"Polling interval: {MailConfig.POLL_INTERVAL} seconds")
    logger.info(f"Folder: {MailConfig.FOLDER}")
    if MailConfig.WHITELIST:
        logger.info(f"Whitelist: {', '.join(MailConfig.WHITELIST)}")
    
    mail = None
    _reconnect_mode = False  # True после первого обрыва — используем длинные задержки

    def _close_mail(m):
        """Аккуратно закрываем IMAP-соединение."""
        if m:
            try:
                m.logout()
            except Exception:
                pass

    try:
        while not shutdown_requested:
            try:
                if not mail:
                    mail = connect_imap(is_reconnect=_reconnect_mode)
                    if not mail:
                        wait = 120 if _reconnect_mode else 60
                        logger.error(f"Failed to connect to IMAP, retrying in {wait}s...")
                        time.sleep(wait)
                        continue
                    _reconnect_mode = False  # Успешно подключились — сбрасываем флаг

                process_emails(mail)

                # Спим между опросами, отправляя NOOP каждые 60 сек для keep-alive
                connection_alive = imap_noop_sleep(mail, MailConfig.POLL_INTERVAL)
                if not connection_alive:
                    logger.warning("IMAP connection dropped during sleep (NOOP failed), reconnecting...")
                    _close_mail(mail)
                    mail = None
                    _reconnect_mode = True
                    # Небольшая задержка перед немедленным переподключением
                    time.sleep(5)

            except (imaplib.IMAP4.abort, ssl.SSLEOFError, OSError) as e:
                logger.warning(f"IMAP connection error: {e}, reconnecting...")
                _close_mail(mail)
                mail = None
                _reconnect_mode = True
                # Ждём перед переподключением — даём серверу время принять новое соединение
                time.sleep(30)

            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)
                _close_mail(mail)
                mail = None
                _reconnect_mode = True
                time.sleep(30)

    except KeyboardInterrupt:
        logger.info("Parser stopped by user")
    finally:
        _close_mail(mail)


if __name__ == "__main__":
    import os
    import signal
    import sys
    
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
        logger.info("Parser stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        # Не делаем raise, чтобы процесс не упал с ошибкой
        # Вместо этого делаем graceful exit
        sys.exit(1)
