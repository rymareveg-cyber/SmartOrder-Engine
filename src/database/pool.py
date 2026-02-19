#!/usr/bin/env python3
"""
Thread-safe connection pool для PostgreSQL.

Использует ThreadedConnectionPool для безопасной работы с asyncio.to_thread.
"""

import time
from typing import Optional
import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from src.config import DatabaseConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DatabasePool:
    """
    Thread-safe connection pool для PostgreSQL.
    
    Использует ThreadedConnectionPool, который полностью thread-safe
    и может использоваться с asyncio.to_thread без проблем.
    """
    
    def __init__(
        self,
        minconn: Optional[int] = None,
        maxconn: Optional[int] = None,
        dsn: Optional[str] = None
    ):
        """
        Инициализация connection pool.
        
        Args:
            minconn: Минимальное количество соединений
            maxconn: Максимальное количество соединений
            dsn: DSN строка подключения
        """
        self.minconn = minconn or DatabaseConfig.POOL_MIN_CONNECTIONS
        self.maxconn = maxconn or DatabaseConfig.POOL_MAX_CONNECTIONS
        self.dsn = dsn or DatabaseConfig.URL
        
        self._pool: Optional[ThreadedConnectionPool] = None
        self._init_pool()
    
    def _init_pool(self):
        """Инициализация пула соединений."""
        try:
            self._pool = ThreadedConnectionPool(
                minconn=self.minconn,
                maxconn=self.maxconn,
                dsn=self.dsn
            )
            logger.info(f"Database connection pool initialized (min={self.minconn}, max={self.maxconn})")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    def get_connection(
        self,
        timeout: float = 5.0,
        retry_interval: float = 0.1
    ) -> psycopg2.extensions.connection:
        """
        Получить соединение с БД из pool с таймаутом и retry.
        
        ThreadedConnectionPool.getconn() является thread-safe и может быть вызван
        из разных потоков, включая потоки созданные через asyncio.to_thread.
        
        Args:
            timeout: Таймаут в секундах для получения соединения
            retry_interval: Интервал между попытками в секундах
        
        Returns:
            Соединение с БД
        
        Raises:
            TimeoutError: Если не удалось получить соединение в течение timeout
            Exception: При других ошибках
        """
        if self._pool is None:
            raise ValueError("Database pool is not initialized")
        
        start_time = time.time()
        
        while True:
            try:
                # getconn() является thread-safe
                conn = self._pool.getconn()
                if conn:
                    # Проверяем, что соединение не закрыто
                    if conn.closed:
                        logger.warning("Got closed connection from pool, retrying...")
                        time.sleep(retry_interval)
                        continue
                    return conn
            except Exception as e:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(f"Failed to get database connection within {timeout}s: {e}")
                time.sleep(retry_interval)
                continue
            
            # Если pool.getconn() вернул None (пул исчерпан)
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                raise TimeoutError(f"Database pool exhausted, could not get connection within {timeout}s")
            time.sleep(retry_interval)
    
    def return_connection(self, conn: Optional[psycopg2.extensions.connection]):
        """
        Вернуть соединение в pool.
        
        putconn() является thread-safe и может быть вызван из любого потока.
        Важно: соединение должно быть возвращено в том же состоянии, в котором было получено
        (не должно быть незавершенных транзакций, открытых курсоров и т.д.).
        
        Args:
            conn: Соединение для возврата
        """
        if self._pool and conn:
            try:
                # Проверяем, что соединение не закрыто
                if conn.closed:
                    logger.warning("Attempted to return closed connection to pool, skipping")
                    return
                
                # Откатываем любые незавершенные транзакции
                try:
                    if not conn.closed:
                        conn.rollback()
                except Exception as rollback_error:
                    logger.warning(f"Error rolling back transaction before returning connection: {rollback_error}")
                
                # Возвращаем соединение в пул
                # putconn() является thread-safe
                self._pool.putconn(conn)
            except Exception as e:
                logger.warning(f"Error returning database connection: {e}")
                # Пытаемся закрыть соединение, если не удалось вернуть в пул
                try:
                    if not conn.closed:
                        conn.close()
                except Exception:
                    pass
    
    def close_all(self):
        """Закрыть все соединения в пуле."""
        if self._pool:
            try:
                self._pool.closeall()
                logger.info("All database connections closed")
            except Exception as e:
                logger.error(f"Error closing database pool: {e}")


# Глобальный экземпляр пула
_db_pool: Optional[DatabasePool] = None


def init_db_pool(
    minconn: Optional[int] = None,
    maxconn: Optional[int] = None,
    dsn: Optional[str] = None
) -> DatabasePool:
    """
    Инициализация глобального connection pool.
    
    Args:
        minconn: Минимальное количество соединений
        maxconn: Максимальное количество соединений
        dsn: DSN строка подключения
    
    Returns:
        DatabasePool экземпляр
    """
    global _db_pool
    if _db_pool is None:
        _db_pool = DatabasePool(minconn=minconn, maxconn=maxconn, dsn=dsn)
    return _db_pool


def get_db_connection(
    timeout: float = 5.0,
    retry_interval: float = 0.1
) -> psycopg2.extensions.connection:
    """
    Получить соединение с БД из глобального pool.
    
    Args:
        timeout: Таймаут в секундах
        retry_interval: Интервал между попытками
    
    Returns:
        Соединение с БД
    """
    global _db_pool
    if _db_pool is None:
        init_db_pool()
    return _db_pool.get_connection(timeout=timeout, retry_interval=retry_interval)


def return_db_connection(conn: Optional[psycopg2.extensions.connection]):
    """
    Вернуть соединение в глобальный pool.
    
    Args:
        conn: Соединение для возврата
    """
    global _db_pool
    if _db_pool:
        _db_pool.return_connection(conn)
