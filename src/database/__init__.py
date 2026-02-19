"""
Модуль для работы с базой данных PostgreSQL.

Предоставляет thread-safe connection pool и функции для работы с БД.
"""

from .pool import (
    init_db_pool,
    get_db_connection,
    return_db_connection,
    DatabasePool
)

__all__ = [
    'init_db_pool',
    'get_db_connection',
    'return_db_connection',
    'DatabasePool'
]
