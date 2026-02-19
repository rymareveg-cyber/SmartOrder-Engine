#!/usr/bin/env python3
"""
Утилиты для обработки ошибок: retry и circuit breaker.

Предоставляет универсальные механизмы для надежной работы с внешними сервисами.
"""

import asyncio
import functools
import random
import time
from typing import Optional, Callable, Any, TypeVar
from enum import Enum

from src.utils.logger import get_logger

logger = get_logger(__name__)

T = TypeVar('T')


class CircuitState(Enum):
    """Состояния circuit breaker."""
    CLOSED = "closed"  # Нормальная работа
    OPEN = "open"  # Сервис недоступен, запросы блокируются
    HALF_OPEN = "half_open"  # Тестирование восстановления


class CircuitBreaker:
    """
    Circuit Breaker для защиты от каскадных сбоев.
    
    Принцип работы:
    - CLOSED: Запросы проходят нормально
    - OPEN: После N ошибок подряд, все запросы блокируются на время cooldown
    - HALF_OPEN: После cooldown, пробуем один запрос. Если успех -> CLOSED, если ошибка -> OPEN
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type = Exception,
        name: str = "circuit_breaker"
    ):
        """
        Args:
            failure_threshold: Количество ошибок для открытия circuit breaker
            recovery_timeout: Время в секундах до попытки восстановления
            expected_exception: Тип исключения, которое считается ошибкой
            name: Имя для логирования
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.success_count = 0
        self._lock = asyncio.Lock()
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if exc_type and issubclass(exc_type, self.expected_exception):
            await self._on_failure()
            return False
        else:
            await self._on_success()
            return False
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Выполнить функцию с защитой circuit breaker.
        
        Args:
            func: Функция для выполнения
            *args, **kwargs: Аргументы функции
            
        Returns:
            Результат выполнения функции
            
        Raises:
            CircuitBreakerOpenError: Если circuit breaker открыт
            expected_exception: Если функция выбросила исключение
        """
        async with self._lock:
            # Проверка состояния
            if self.state == CircuitState.OPEN:
                # Проверяем, прошло ли время восстановления
                if self.last_failure_time and (time.time() - self.last_failure_time) >= self.recovery_timeout:
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker {self.name} is OPEN. "
                        f"Will retry after {self.recovery_timeout}s"
                    )
            
            # Выполнение функции
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                
                # Успешное выполнение
                await self._on_success()
                return result
                
            except self.expected_exception as e:
                await self._on_failure()
                raise
    
    async def _on_success(self):
        """Обработка успешного выполнения."""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= 1:  # Один успешный запрос -> закрываем
                logger.info(f"Circuit breaker {self.name} transitioning to CLOSED (recovered)")
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.last_failure_time = None
        elif self.state == CircuitState.CLOSED:
            # Сбрасываем счетчик ошибок при успехе
            if self.failure_count > 0:
                self.failure_count = 0
    
    async def _on_failure(self):
        """Обработка ошибки."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitState.HALF_OPEN:
            # Ошибка в HALF_OPEN -> сразу в OPEN
            logger.warning(f"Circuit breaker {self.name} transitioning to OPEN (failed in HALF_OPEN)")
            self.state = CircuitState.OPEN
        elif self.state == CircuitState.CLOSED:
            if self.failure_count >= self.failure_threshold:
                logger.warning(
                    f"Circuit breaker {self.name} transitioning to OPEN "
                    f"(failure_count={self.failure_count} >= threshold={self.failure_threshold})"
                )
                self.state = CircuitState.OPEN
    
    def get_state(self) -> CircuitState:
        """Получить текущее состояние."""
        return self.state
    
    def reset(self):
        """Сбросить circuit breaker в начальное состояние."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        logger.info(f"Circuit breaker {self.name} reset")


class CircuitBreakerOpenError(Exception):
    """Исключение при открытом circuit breaker."""
    pass


def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retry_on: tuple = (Exception,),
    retry_on_not: tuple = ()
):
    """
    Декоратор для retry с exponential backoff и jitter.
    
    Args:
        max_retries: Максимальное количество попыток
        initial_delay: Начальная задержка в секундах
        max_delay: Максимальная задержка в секундах
        exponential_base: База для exponential backoff
        jitter: Добавлять ли случайную задержку (jitter)
        retry_on: Кортеж типов исключений, на которые нужно делать retry
        retry_on_not: Кортеж типов исключений, на которые НЕ нужно делать retry
    
    Returns:
        Декорированная функция
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs) -> T:
                last_exception = None
                
                for attempt in range(max_retries):
                    try:
                        return await func(*args, **kwargs)
                    except retry_on_not as e:
                        logger.debug(f"{func.__name__} raised {type(e).__name__}, not retrying")
                        raise
                    except retry_on as e:
                        last_exception = e
                        
                        if attempt < max_retries - 1:
                            delay = min(
                                initial_delay * (exponential_base ** attempt),
                                max_delay
                            )
                            
                            if jitter:
                                jitter_amount = delay * 0.2 * random.random()
                                delay += jitter_amount
                            
                            logger.warning(
                                f"{func.__name__} failed (attempt {attempt + 1}/{max_retries}): {e}. "
                                f"Retrying in {delay:.2f}s"
                            )
                            
                            await asyncio.sleep(delay)
                        else:
                            logger.error(
                                f"{func.__name__} failed after {max_retries} attempts: {e}"
                            )
                            raise
                    except Exception as e:
                        if retry_on == (Exception,):
                            last_exception = e
                            if attempt < max_retries - 1:
                                delay = min(
                                    initial_delay * (exponential_base ** attempt),
                                    max_delay
                                )
                                if jitter:
                                    jitter_amount = delay * 0.2 * random.random()
                                    delay += jitter_amount
                                
                                logger.warning(
                                    f"{func.__name__} failed (attempt {attempt + 1}/{max_retries}): {e}. "
                                    f"Retrying in {delay:.2f}s"
                                )
                                
                                await asyncio.sleep(delay)
                            else:
                                logger.error(
                                    f"{func.__name__} failed after {max_retries} attempts: {e}"
                                )
                                raise
                        else:
                            raise
                
                if last_exception:
                    raise last_exception
                raise RuntimeError(f"{func.__name__} failed after {max_retries} attempts")
            
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs) -> T:
                last_exception = None
                
                for attempt in range(max_retries):
                    try:
                        return func(*args, **kwargs)
                    except retry_on_not as e:
                        logger.debug(f"{func.__name__} raised {type(e).__name__}, not retrying")
                        raise
                    except retry_on as e:
                        last_exception = e
                        
                        if attempt < max_retries - 1:
                            delay = min(
                                initial_delay * (exponential_base ** attempt),
                                max_delay
                            )
                            
                            if jitter:
                                jitter_amount = delay * 0.2 * random.random()
                                delay += jitter_amount
                            
                            logger.warning(
                                f"{func.__name__} failed (attempt {attempt + 1}/{max_retries}): {e}. "
                                f"Retrying in {delay:.2f}s"
                            )
                            
                            time.sleep(delay)
                        else:
                            logger.error(
                                f"{func.__name__} failed after {max_retries} attempts: {e}"
                            )
                            raise
                    except Exception as e:
                        if retry_on == (Exception,):
                            last_exception = e
                            if attempt < max_retries - 1:
                                delay = min(
                                    initial_delay * (exponential_base ** attempt),
                                    max_delay
                                )
                                if jitter:
                                    jitter_amount = delay * 0.2 * random.random()
                                    delay += jitter_amount
                                
                                logger.warning(
                                    f"{func.__name__} failed (attempt {attempt + 1}/{max_retries}): {e}. "
                                    f"Retrying in {delay:.2f}s"
                                )
                                
                                time.sleep(delay)
                            else:
                                logger.error(
                                    f"{func.__name__} failed after {max_retries} attempts: {e}"
                                )
                                raise
                        else:
                            raise
                
                if last_exception:
                    raise last_exception
                raise RuntimeError(f"{func.__name__} failed after {max_retries} attempts")
            
            return sync_wrapper
    
    return decorator


# Глобальные circuit breakers для внешних сервисов
_openai_circuit_breaker: Optional[CircuitBreaker] = None
_onec_circuit_breaker: Optional[CircuitBreaker] = None
_redis_circuit_breaker: Optional[CircuitBreaker] = None
_telegram_circuit_breaker: Optional[CircuitBreaker] = None


def get_openai_circuit_breaker() -> CircuitBreaker:
    """Получить circuit breaker для OpenAI API."""
    global _openai_circuit_breaker
    if _openai_circuit_breaker is None:
        _openai_circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60.0,
            expected_exception=Exception,
            name="openai"
        )
    return _openai_circuit_breaker


def get_onec_circuit_breaker() -> CircuitBreaker:
    """Получить circuit breaker для 1C API."""
    global _onec_circuit_breaker
    if _onec_circuit_breaker is None:
        _onec_circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=120.0,
            expected_exception=Exception,
            name="1c"
        )
    return _onec_circuit_breaker


def get_redis_circuit_breaker() -> CircuitBreaker:
    """Получить circuit breaker для Redis."""
    global _redis_circuit_breaker
    if _redis_circuit_breaker is None:
        _redis_circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=30.0,
            expected_exception=Exception,
            name="redis"
        )
    return _redis_circuit_breaker


def get_telegram_circuit_breaker() -> CircuitBreaker:
    """Получить circuit breaker для Telegram Bot API."""
    global _telegram_circuit_breaker
    if _telegram_circuit_breaker is None:
        from telegram.error import BadRequest, TimedOut, NetworkError
        _telegram_circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60.0,
            expected_exception=(BadRequest, TimedOut, NetworkError),
            name="telegram"
        )
    return _telegram_circuit_breaker
