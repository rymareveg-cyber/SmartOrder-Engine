#!/usr/bin/env python3
"""
Логирование - настройка структурированного логирования.

Предоставляет JSON формат логирования для всех модулей системы.
"""

import os
import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

from src.config import PROJECT_ROOT


class CustomJsonFormatter(logging.Formatter):
    """Кастомный JSON форматтер с дополнительными полями."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Форматирование записи в JSON."""
        log_data: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "process_id": record.process,
            "thread_id": record.thread,
            "message": record.getMessage(),
        }

        # asyncio taskName (если есть, Python 3.12+)
        task_name = getattr(record, "taskName", None)
        if task_name:
            log_data["taskName"] = task_name

        # Добавляем exception если есть
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Поддержка extra={...}
        for k, v in record.__dict__.items():
            if k in log_data:
                continue
            # Пропускаем стандартные/шумные поля
            if k in {
                "args", "msg", "exc_info", "exc_text", "stack_info",
                "created", "msecs", "relativeCreated", "pathname",
                "filename", "module", "funcName", "levelname", "levelno",
                "lineno", "name", "thread", "threadName", "processName", "process",
            }:
                continue
            # Не сериализуем потенциально сложные объекты
            try:
                json.dumps({k: v})
                log_data[k] = v
            except Exception:
                log_data[k] = str(v)

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
        # Извлекаем имя модуля из полного пути
        module_name = name.split('.')[-1] if '.' in name else name

        # Если модуль запущен как скрипт
        if module_name == "__main__":
            try:
                argv0 = sys.argv[0] if sys.argv and sys.argv[0] else ""
                script_stem = Path(argv0).stem if argv0 else ""
                module_name = script_stem or "main"
            except Exception:
                module_name = "main"

        # Всегда пишем в папку logs внутри проекта
        logs_dir = PROJECT_ROOT / "logs"
        log_file = str(logs_dir / f"{module_name}.log")
    
    # Проверяем переменную окружения для формата логирования
    json_format = os.getenv('LOG_JSON_FORMAT', 'true').lower() == 'true'
    
    return setup_logger(
        name=name,
        log_file=log_file,
        level=logging.INFO,
        json_format=json_format,
        console_output=True
    )


def setup_uvicorn_logging(service_name: str, level: int = logging.INFO) -> None:
    """
    Настроить uvicorn-логгеры так, чтобы они писали в отдельные файлы.

    Важно: использовать вместе с `uvicorn.run(..., log_config=None)` или
    `uvicorn.Config(..., log_config=None)`, иначе uvicorn перезатрёт handlers.

    Создаёт:
    - logs/{service_name}.uvicorn.error.log
    - logs/{service_name}.uvicorn.access.log
    """
    json_format = os.getenv('LOG_JSON_FORMAT', 'true').lower() == 'true'

    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    if json_format:
        file_formatter: logging.Formatter = CustomJsonFormatter()
    else:
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    def _configure_logger(logger_name: str, filename: str) -> None:
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.propagate = False
        logger.handlers.clear()

        file_handler = RotatingFileHandler(
            str(logs_dir / filename),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # В консоль — читаемый формат
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    _configure_logger("uvicorn.error", f"{service_name}.uvicorn.error.log")
    _configure_logger("uvicorn.access", f"{service_name}.uvicorn.access.log")
