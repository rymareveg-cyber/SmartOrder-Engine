FROM python:3.11-slim

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Копирование requirements и установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода приложения (исключая docs через .dockerignore)
COPY . .

# Создание директории для логов
RUN mkdir -p logs

# Переменные окружения по умолчанию
ENV PYTHONUNBUFFERED=1
ENV API_HOST=0.0.0.0
ENV API_PORT=8025

# Открытие порта
EXPOSE 8025

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8025/health')" || exit 1

# Запуск API сервера
CMD ["python", "execution/api_catalog_server.py"]
