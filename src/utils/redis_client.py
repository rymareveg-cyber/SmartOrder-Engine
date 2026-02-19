#!/usr/bin/env python3
"""
Утилиты для работы с Redis.

Предоставляет универсальные функции для инициализации Redis клиентов
и отправки сообщений в очередь с поддержкой sync и async операций.
"""

import json
import asyncio
import time
from typing import Optional, Any

from src.config import RedisConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)


def init_redis_client(
    decode_responses: bool = True,
    socket_timeout: Optional[int] = None,
    socket_connect_timeout: Optional[int] = None,
    raise_on_error: bool = False
) -> Optional[Any]:
    """
    Инициализация синхронного Redis клиента.
    
    Args:
        decode_responses: Декодировать ли ответы в строки (True) или оставить bytes (False)
        socket_timeout: Таймаут для операций в секундах (по умолчанию 5)
        socket_connect_timeout: Таймаут для подключения в секундах (по умолчанию 5)
        raise_on_error: Выбрасывать исключение при ошибке (True) или возвращать None (False)
    
    Returns:
        Redis клиент или None при ошибке
    """
    try:
        import redis
        
        redis_kwargs = {
            'decode_responses': decode_responses,
            'socket_timeout': socket_timeout or 5,
            'socket_connect_timeout': socket_connect_timeout or 5,
            'retry_on_timeout': True,
            'health_check_interval': 30
        }
        
        client = redis.from_url(RedisConfig.URL, **redis_kwargs)
        client.ping()
        logger.info(f"Redis client initialized (decode_responses={decode_responses})")
        return client
    except Exception as e:
        if raise_on_error:
            logger.error(f"Failed to initialize Redis: {e}")
            raise
        else:
            logger.warning(f"Failed to initialize Redis: {e}. Redis operations disabled.")
            return None


async def init_async_redis_client(
    decode_responses: bool = False,
    max_connections: Optional[int] = None
) -> Optional[Any]:
    """
    Инициализация асинхронного Redis клиента (connection pool).
    
    Args:
        decode_responses: Декодировать ли ответы в строки (True) или оставить bytes (False)
        max_connections: Максимальное количество соединений в пуле
    
    Returns:
        Redis клиент или None при ошибке
    """
    try:
        import redis.asyncio as aioredis
        
        pool = aioredis.ConnectionPool.from_url(
            RedisConfig.URL,
            decode_responses=decode_responses,
            max_connections=max_connections or 10
        )
        client = aioredis.Redis(connection_pool=pool)
        await client.ping()
        logger.info(f"Async Redis client initialized (decode_responses={decode_responses})")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize async Redis: {e}")
        return None


def send_to_queue_sync(
    redis_client: Any,
    message_data: dict,
    queue_key: Optional[str] = None,
    max_retries: int = None,
    retry_delays: list = None
) -> bool:
    """
    Отправить сообщение в Redis Queue (синхронная версия).
    
    Args:
        redis_client: Redis клиент
        message_data: Словарь с данными сообщения
        queue_key: Ключ очереди (по умолчанию из RedisConfig)
        max_retries: Максимальное количество попыток
        retry_delays: Задержки между попытками в секундах
    
    Returns:
        True если успешно, False в противном случае
    """
    if not redis_client:
        logger.error("Redis client not initialized")
        return False
    
    queue_key = queue_key or RedisConfig.QUEUE_KEY
    max_retries = max_retries or RedisConfig.MAX_RETRIES
    retry_delays = retry_delays or [1, 2, 4]
    message_json = json.dumps(message_data, ensure_ascii=False)
    
    for attempt in range(max_retries):
        try:
            redis_client.lpush(queue_key, message_json)
            message_id = (
                message_data.get('message_id') or
                message_data.get('submission_id') or
                message_data.get('email') or
                'unknown'
            )
            logger.info(f"Message {message_id} sent to queue: channel={message_data.get('channel')}, queue_key={queue_key}")
            return True
        except Exception as e:
            delay = retry_delays[min(attempt, len(retry_delays) - 1)] if attempt < max_retries - 1 else 0
            logger.warning(f"Failed to send to queue (attempt {attempt + 1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                logger.error(f"Failed to send message to queue after {max_retries} attempts")
    
    return False


async def send_to_queue_async(
    redis_client: Any,
    message_data: dict,
    queue_key: Optional[str] = None,
    max_retries: int = None,
    retry_delays: list = None
) -> bool:
    """
    Отправить сообщение в Redis Queue (асинхронная версия).
    
    Args:
        redis_client: Async Redis клиент
        message_data: Словарь с данными сообщения
        queue_key: Ключ очереди (по умолчанию из RedisConfig)
        max_retries: Максимальное количество попыток
        retry_delays: Задержки между попытками в секундах
    
    Returns:
        True если успешно, False в противном случае
    """
    if not redis_client:
        logger.error("Redis client not initialized")
        return False
    
    queue_key = queue_key or RedisConfig.QUEUE_KEY
    max_retries = max_retries or RedisConfig.MAX_RETRIES
    retry_delays = retry_delays or [1, 2, 4]
    message_json = json.dumps(message_data, ensure_ascii=False)
    
    for attempt in range(max_retries):
        try:
            await redis_client.lpush(queue_key, message_json)
            message_id = (
                message_data.get('message_id') or
                message_data.get('submission_id') or
                message_data.get('email') or
                'unknown'
            )
            logger.info(f"Message {message_id} sent to queue: channel={message_data.get('channel')}, queue_key={queue_key}")
            return True
        except Exception as e:
            delay = retry_delays[min(attempt, len(retry_delays) - 1)] if attempt < max_retries - 1 else 0
            logger.warning(f"Failed to send to queue (attempt {attempt + 1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(delay)
            else:
                logger.error(f"Failed to send message to queue after {max_retries} attempts")
    
    return False
