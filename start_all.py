#!/usr/bin/env python3
"""
Скрипт запуска всех сервисов SmartOrder Engine.

Запускает все компоненты системы в отдельных процессах с логированием.
"""

import os
import sys
import time
import signal
import socket
import subprocess
import logging
from pathlib import Path
from typing import List, Dict, Optional
from multiprocessing import Process

# Корневая директория проекта — тот же каталог, где лежит этот скрипт
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
try:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/start_all.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

# Список сервисов для запуска
SERVICES = [
    {
        "name": "sync_1c_catalog",
        "script": "src/services/catalog_sync.py",
        "description": "Синхронизация каталога 1С (с планировщиком)",
        "required": True
    },
    {
        "name": "api_catalog_server",
        "script": "src/api/catalog.py",
        "description": "API сервер каталога",
        "required": True,
        "port": os.getenv('API_PORT', '8025')
    },
    {
        "name": "telegram_bot",
        "script": "src/services/telegram_bot.py",
        "description": "Telegram бот",
        "required": False
    },
    {
        "name": "yandex_mail_parser",
        "script": "src/services/mail_parser.py",
        "description": "Парсер Яндекс.Почты",
        "required": False
    },
    {
        "name": "yandex_forms_webhook",
        "script": "src/api/webhooks.py",
        "description": "Webhook для Яндекс.Форм",
        "required": False,
        "port": os.getenv('WEBHOOK_PORT', '8026')
    },
    {
        "name": "queue_processor",
        "script": "src/services/queue_processor.py",
        "description": "Обработчик очереди Redis",
        "required": True
    },
    {
        "name": "dashboard_api",
        "script": "src/api/dashboard.py",
        "description": "Dashboard API",
        "required": False,
        "port": os.getenv('DASHBOARD_PORT', '8028')
    },
    {
        "name": "payments_api",
        "script": "src/api/payments.py",
        "description": "Payments API (страница оплаты)",
        "required": False,
        "port": os.getenv('PAYMENTS_PORT', '8029')
    }
]

# Хранилище процессов
processes: List[Dict[str, any]] = []


def is_port_in_use(port: int, host: str = '0.0.0.0') -> bool:
    """Проверка, занят ли порт."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            result = s.connect_ex((host if host != '0.0.0.0' else '127.0.0.1', port))
            return result == 0
    except Exception:
        return False


def start_service(service: Dict[str, any]) -> Optional[Process]:
    """Запуск сервиса в отдельном процессе."""
    script_path = project_root / service["script"]

    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        if service.get("required", False):
            raise FileNotFoundError(f"Required script not found: {script_path}")
        return None

    logger.info(f"Starting {service['name']}: {service['description']}")

    # Проверка порта, если указан
    if "port" in service:
        port = int(service["port"])
        if is_port_in_use(port):
            logger.warning(f"Port {port} is already in use for {service['name']}. Skipping...")
            if service.get("required", False):
                logger.error(f"Required service {service['name']} cannot start on port {port}")
                raise RuntimeError(f"Port {port} is already in use")
            return None

    try:
        env = os.environ.copy()
        pythonpath = env.get('PYTHONPATH', '')
        env['PYTHONPATH'] = f"{str(project_root)}{os.pathsep}{pythonpath}" if pythonpath else str(project_root)

        process = subprocess.Popen(
            [sys.executable, str(script_path)],
            cwd=str(project_root),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        wait_time = 3 if service["name"] in ["dashboard_api", "api_catalog_server", "yandex_forms_webhook"] else 2
        time.sleep(wait_time)

        if process.poll() is None:
            logger.info(f"[OK] {service['name']} started (PID: {process.pid})")
            return process
        else:
            logger.error(f"[FAIL] {service['name']} failed to start (exit code: {process.returncode}). Check logs/{service['name']}.log")
            return None

    except Exception as e:
        logger.error(f"[ERROR] Error starting {service['name']}: {e}")
        if service.get("required", False):
            raise
        return None


def check_service_health(service_name: str, port: Optional[int] = None) -> bool:
    """Проверка здоровья сервиса через health endpoint."""
    if not port:
        return True

    try:
        try:
            import requests
        except ImportError:
            return True

        if service_name == "queue_processor":
            health_port = int(os.getenv('HEALTH_CHECK_PORT', '8027'))
            health_url = f"http://localhost:{health_port}/health/live"
        else:
            health_url = f"http://localhost:{port}/health/live"

        response = requests.get(health_url, timeout=5)
        return response.status_code == 200
    except Exception:
        return False


def stop_all_services():
    """Graceful остановка всех запущенных сервисов."""
    logger.info("Stopping all services gracefully...")

    for proc_info in processes:
        process = proc_info.get("process")
        service_name = proc_info.get("name")
        if process and process.poll() is None:
            logger.info(f"Sending SIGTERM to {service_name} (PID: {process.pid})...")
            try:
                process.terminate()
            except Exception as e:
                logger.error(f"Error sending SIGTERM to {service_name}: {e}")

    start_time = time.time()
    timeout = 10.0
    remaining_processes = [p for p in processes if p.get("process") and p.get("process").poll() is None]

    while remaining_processes and (time.time() - start_time) < timeout:
        time.sleep(0.5)
        remaining_processes = [p for p in processes if p.get("process") and p.get("process").poll() is None]

    for proc_info in remaining_processes:
        process = proc_info.get("process")
        service_name = proc_info.get("name")
        if process and process.poll() is None:
            logger.warning(f"Force killing {service_name} (PID: {process.pid})...")
            try:
                process.kill()
                process.wait(timeout=2)
                logger.info(f"[OK] {service_name} force stopped")
            except Exception as e:
                logger.error(f"Error force killing {service_name}: {e}")

    logger.info("All services stopped")


def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown."""
    logger.info(f"Received signal {signum}, shutting down...")
    stop_all_services()
    sys.exit(0)


def monitor_services():
    """Мониторинг запущенных сервисов с автоперезапуском."""
    max_restart_attempts = 3
    restart_counts = {}
    first_check_done = {}

    time.sleep(5)

    while True:
        time.sleep(10)

        for proc_info in processes:
            process = proc_info.get("process")
            service_name = proc_info.get("name")
            required = proc_info.get("required", False)
            service = proc_info.get("service")

            if process:
                port = service.get("port")
                if port:
                    try:
                        port_int = int(port)
                        is_first_check = not first_check_done.get(service_name, False)
                        if is_first_check:
                            time.sleep(2)
                            first_check_done[service_name] = True
                        if not check_service_health(service_name, port_int):
                            if not is_first_check:
                                logger.warning(f"[WARNING] {service_name} health check failed (port {port_int})")
                    except Exception:
                        pass

                if process.poll() is not None:
                    logger.error(f"[ERROR] {service_name} has stopped (exit code: {process.returncode})")
                    restart_count = restart_counts.get(service_name, 0)
                    if restart_count < max_restart_attempts:
                        logger.info(f"Restarting {service_name} (attempt {restart_count + 1}/{max_restart_attempts})...")
                        new_process = start_service(service)
                        if new_process:
                            proc_info["process"] = new_process
                            restart_counts[service_name] = 0
                            logger.info(f"[OK] {service_name} restarted")
                        else:
                            restart_counts[service_name] = restart_count + 1
                            if required and restart_counts[service_name] >= max_restart_attempts:
                                logger.error(f"Max restarts reached for required service {service_name}, shutting down...")
                                stop_all_services()
                                sys.exit(1)
                    else:
                        if required:
                            logger.error(f"Max restarts reached for {service_name}, shutting down...")
                            stop_all_services()
                            sys.exit(1)


def main():
    """Главная функция запуска всех сервисов."""
    logger.info("=" * 60)
    logger.info("SmartOrder Engine - Starting all services")
    logger.info("=" * 60)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)

    for service in SERVICES:
        if not service.get("required", False):
            if service["name"] == "telegram_bot" and not os.getenv("TELEGRAM_BOT_TOKEN"):
                logger.info(f"Skipping {service['name']} (TELEGRAM_BOT_TOKEN not set)")
                continue
            elif service["name"] == "yandex_mail_parser" and not os.getenv("YANDEX_MAIL_EMAIL"):
                logger.info(f"Skipping {service['name']} (YANDEX_MAIL_EMAIL not set)")
                continue

        process = start_service(service)
        if process:
            processes.append({
                "name": service["name"],
                "process": process,
                "service": service,
                "required": service.get("required", False)
            })
        else:
            if service.get("required", False):
                logger.error(f"Failed to start required service {service['name']}, aborting...")
                stop_all_services()
                sys.exit(1)

    logger.info("=" * 60)
    logger.info(f"All services started. Total: {len(processes)}")
    logger.info("=" * 60)
    logger.info("Press Ctrl+C to stop all services")
    logger.info("")

    try:
        monitor_services()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        stop_all_services()


if __name__ == "__main__":
    main()
