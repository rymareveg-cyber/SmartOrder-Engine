# Инструкция по деплою в Dokploy

## Подготовка

1. Убедитесь, что репозиторий подключен к GitHub: https://github.com/rymareveg-cyber/SmartOrder-Engine.git

2. Все файлы конфигурации (Dockerfile, .dockerignore) должны быть в корне репозитория

## Настройка в Dokploy

### 1. Создание приложения

1. Войдите в Dokploy: https://dokploy.rymarev.ru/
2. Создайте новое приложение
3. Выберите "GitHub" как источник
4. Подключите репозиторий: `rymareveg-cyber/SmartOrder-Engine`
5. Выберите ветку: `main` (или вашу рабочую ветку)

### 2. Настройка Docker

- **Dockerfile Path**: `./Dockerfile` (или оставьте пустым, если Dockerfile в корне)
- **Docker Context**: `.` (корень репозитория)

### 3. Переменные окружения

Добавьте все необходимые переменные окружения в настройках приложения:

**Обязательные:**
- `DATABASE_URL` - строка подключения к PostgreSQL
- `REDIS_URL` - строка подключения к Redis
- `ONEC_BASE_URL` - URL 1С сервера
- `ONEC_USERNAME` - логин для 1С
- `ONEC_PASSWORD` - пароль для 1С

**Опциональные:**
- `API_HOST=0.0.0.0`
- `API_PORT=8025`
- `CACHE_TTL=300`
- `LOG_LEVEL=INFO`

#### Примеры переменных окружения для VPS

**Если Redis создан как сервис Dokploy:**
```
REDIS_URL=redis://:your-password@smartorder-redis:6379/0
```

**Если Redis создан как сервис Dokploy без пароля:**
```
REDIS_URL=redis://smartorder-redis:6379/0
```

**Если PostgreSQL создан как сервис Dokploy:**
```
DATABASE_URL=postgresql://user:password@postgres:5432/smartorder
```

**Если PostgreSQL на хосте VPS:**
```
DATABASE_URL=postgresql://user:password@localhost:5432/smartorder
```

**Если 1С на внешнем сервере:**
```
ONEC_BASE_URL=http://192.168.1.100:80
# или
ONEC_BASE_URL=http://1c-server.example.com:80
```

**Важно:** 
- Имена сервисов (`smartorder-redis`, `postgres`) должны совпадать с именами, указанными при создании в Dokploy (поле "Name")
- Если сервисы на хосте VPS, используйте `localhost` вместо имени сервиса
- Если пароль содержит специальные символы (@, #, %, &), их нужно URL-кодировать

**Для Telegram бота (если запускаете отдельно):**
- `TELEGRAM_BOT_TOKEN` - токен бота от @BotFather
- `TELEGRAM_ADMIN_ID` - ID администратора

**Для Яндекс.Почты (если запускаете отдельно):**
- `YANDEX_MAIL_IMAP_HOST=imap.yandex.ru`
- `YANDEX_MAIL_EMAIL` - email для мониторинга
- `YANDEX_MAIL_PASSWORD` - пароль или app password
- `YANDEX_MAIL_FOLDER=INBOX`
- `YANDEX_MAIL_POLL_INTERVAL=120`

### 4. Порты

- **Container Port**: `8025`
- **Public Port**: `8025` (или любой другой, Dokploy может проксировать)

### 5. Health Check

Dokploy автоматически использует HEALTHCHECK из Dockerfile, но можно настроить вручную:
- **Path**: `/health`
- **Interval**: 30s

### 6. Деплой

1. Нажмите "Deploy"
2. Dokploy автоматически:
   - Клонирует репозиторий
   - Соберёт Docker образ (исключая docs через .dockerignore)
   - Запустит контейнер
   - Проверит health check

## Проверка работы

После деплоя проверьте:

```bash
# Health check
curl https://your-domain.com/health

# API каталога
curl https://your-domain.com/api/catalog

# Поиск товаров
curl "https://your-domain.com/api/catalog/search?q=варочная"
```

## Обновление

При каждом push в GitHub, Dokploy автоматически пересоберёт и перезапустит приложение (если настроен auto-deploy).

Или можно вручную нажать "Redeploy" в интерфейсе Dokploy.

## Логи

Логи доступны в интерфейсе Dokploy в разделе "Logs" вашего приложения.

Также логи сохраняются в `logs/` директории внутри контейнера.

## Важные замечания

1. **Папка docs исключена**: Папка `docs/` не попадает в Docker образ благодаря `.dockerignore`
2. **Переменные окружения**: Все секретные данные настраиваются в Dokploy, не коммитьте `.env` файл
3. **База данных**: Убедитесь, что PostgreSQL доступен из контейнера (внутренняя сеть Dokploy или внешний хост)
4. **Redis**: Аналогично, Redis должен быть доступен из контейнера
5. **1С сервер**: Должен быть доступен из контейнера (проверьте сетевые настройки)

## Локальная разработка

Для локальной разработки используйте `docker-compose.yml`:

```bash
# Запуск API сервера
docker-compose up api

# Или с логами
docker-compose up -d api && docker-compose logs -f api
```

## Отдельные сервисы

Telegram бот и IMAP парсер можно запускать отдельно (не через Dokploy) или добавить как отдельные сервисы в docker-compose.yml.
