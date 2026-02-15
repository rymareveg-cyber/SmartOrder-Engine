#!/usr/bin/env python3
"""
Logger Configuration - настройка структурированного логирования.

Предоставляет JSON формат логирования для всех модулей системы.
"""

import os
import json
import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from pythonjsonlogger import jsonlogger
except ImportError:
    # Fallback если библиотека не установлена
    jsonlogger = None


class CustomJsonFormatter(logging.Formatter):
    """Кастомный JSON форматтер с дополнительными полями."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Форматирование записи в JSON."""
        if jsonlogger:
            # Используем стандартный JSON форматтер
            json_formatter = jsonlogger.JsonFormatter(
                '%(timestamp)s %(level)s %(module)s %(function)s %(message)s'
            )
            return json_formatter.format(record)
        else:
            # Fallback: создаём JSON вручную
            log_data = {
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'level': record.levelname,
                'module': record.module,
                'function': record.funcName,
                'line': record.lineno,
                'process_id': record.process,
                'thread_id': record.thread,
                'message': record.getMessage()
            }
            
            # Добавляем exception если есть
            if record.exc_info:
                log_data['exception'] = self.formatException(record.exc_info)
            
            # Добавляем extra поля если есть
            if hasattr(record, 'extra'):
                log_data.update(record.extra)
            
            return json.dumps(log_data, ensure_ascii=False)


def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    json_format: bool = True,
    console_output: bool = True
) -> logging.Logger:
    """
    Настройка логгера с JSON форматом.
    
    Args:
        name: Имя логгера
        log_file: Путь к файлу лога (опционально)
        level: Уровень логирования
        json_format: Использовать JSON формат (по умолчанию True)
        console_output: Выводить в консоль (по умолчанию True)
        
    Returns:
        Настроенный логгер
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Удаляем существующие обработчики
    logger.handlers.clear()
    
    # Создаём директорию для логов если нужно
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # JSON форматтер
    if json_format:
        json_formatter = CustomJsonFormatter()
    else:
        # Обычный форматтер для консоли (читаемый)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    # Обработчик для файла (с ротацией)
    if log_file:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding='utf-8'
        )
        if json_format:
            file_handler.setFormatter(json_formatter)
        else:
            file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Обработчик для консоли (читаемый формат)
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        # В консоль выводим читаемый формат, даже если в файл пишем JSON
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    """
    Получить настроенный логгер.
    
    Args:
        name: Имя логгера (обычно __name__)
        log_file: Путь к файлу лога (опционально, по умолчанию logs/{name}.log)
        
    Returns:
        Настроенный логгер
    """
    # Определяем путь к файлу лога
    if not log_file:
        log_file = f"logs/{Path(name).stem}.log"
    
    # Проверяем переменную окружения для формата логирования
    json_format = os.getenv('LOG_JSON_FORMAT', 'true').lower() == 'true'
    
    return setup_logger(
        name=name,
        log_file=log_file,
        level=logging.INFO,
        json_format=json_format,
        console_output=True
    )
