# API Documentation: SmartOrder Engine

## Общая информация

### Базовый URL
```
Production: https://api.smartorder.example.com
Development: http://localhost:8025
```

### Версия API
**v1.0**

### Формат данных
- **Request/Response**: JSON
- **Кодировка**: UTF-8
- **Дата и время**: ISO 8601 (например: `2026-02-13T10:30:00Z`)

### Аутентификация
Все защищённые endpoints требуют аутентификации через API Key или JWT токен.

**Заголовок запроса:**
```
Authorization: Bearer <token>
```
или
```
X-API-Key: <api_key>
```

### Коды ответов
- `200 OK` - успешный запрос
- `201 Created` - ресурс создан
- `400 Bad Request` - неверный запрос
- `401 Unauthorized` - требуется аутентификация
- `403 Forbidden` - недостаточно прав
- `404 Not Found` - ресурс не найден
- `422 Unprocessable Entity` - ошибка валидации
- `500 Internal Server Error` - внутренняя ошибка сервера
- `503 Service Unavailable` - сервис недоступен

### Пагинация
Для endpoints, возвращающих списки, используется пагинация:

**Query параметры:**
- `page` - номер страницы (начиная с 1, по умолчанию: 1)
- `page_size` - количество элементов на странице (по умолчанию: 20, максимум: 100)

**Response:**
```json
{
  "items": [...],
  "total": 100,
  "page": 1,
  "page_size": 20,
  "pages": 5
}
```

### Ошибки
Формат ответа при ошибке:
```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Описание ошибки",
    "details": {}
  }
}
```

---

## 1. Каталог товаров

### 1.1. Получить список товаров

**GET** `/api/catalog`

Получить список всех товаров из каталога с актуальными остатками.

**Query параметры:**
- `page` (optional) - номер страницы
- `page_size` (optional) - размер страницы
- `q` (optional) - поисковый запрос (по названию или артикулу)
- `min_stock` (optional) - минимальный остаток (фильтр)
- `max_price` (optional) - максимальная цена (фильтр)

**Response 200 OK:**
```json
{
  "items": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "articul": "IPHONE15-256GB",
      "name": "iPhone 15 256GB",
      "price": 99900.00,
      "stock": 5,
      "updated_at": "2026-02-13T10:00:00Z",
      "synced_at": "2026-02-13T10:00:00Z"
    }
  ],
  "total": 1000,
  "page": 1,
  "page_size": 20,
  "pages": 50
}
```

**Пример запроса:**
```bash
curl -X GET "http://localhost:8025/api/catalog?q=iphone&page=1&page_size=10" \
  -H "Authorization: Bearer <token>"
```

---

### 1.2. Получить товар по артикулу

**GET** `/api/catalog/{articul}`

Получить информацию о конкретном товаре.

**Path параметры:**
- `articul` - артикул товара

**Response 200 OK:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "articul": "IPHONE15-256GB",
  "name": "iPhone 15 256GB",
  "price": 99900.00,
  "stock": 5,
  "updated_at": "2026-02-13T10:00:00Z",
  "synced_at": "2026-02-13T10:00:00Z"
}
```

**Response 404 Not Found:**
```json
{
  "error": {
    "code": "PRODUCT_NOT_FOUND",
    "message": "Товар с артикулом 'IPHONE15-256GB' не найден"
  }
}
```

**Пример запроса:**
```bash
curl -X GET "http://localhost:8025/api/catalog/IPHONE15-256GB" \
  -H "Authorization: Bearer <token>"
```

---

### 1.3. Поиск товаров

**GET** `/api/catalog/search`

Расширенный поиск товаров с фильтрами.

**Query параметры:**
- `q` (required) - поисковый запрос
- `fuzzy` (optional, default: true) - использовать нечёткий поиск
- `min_price` (optional) - минимальная цена
- `max_price` (optional) - максимальная цена
- `in_stock` (optional, default: false) - только товары в наличии
- `page` (optional) - номер страницы
- `page_size` (optional) - размер страницы

**Response 200 OK:**
```json
{
  "items": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "articul": "IPHONE15-256GB",
      "name": "iPhone 15 256GB",
      "price": 99900.00,
      "stock": 5,
      "relevance_score": 0.95
    }
  ],
  "total": 5,
  "page": 1,
  "page_size": 20,
  "pages": 1
}
```

**Пример запроса:**
```bash
curl -X GET "http://localhost:8025/api/catalog/search?q=iphone&in_stock=true" \
  -H "Authorization: Bearer <token>"
```

---

## 2. Заказы

### 2.1. Создать заказ

**POST** `/api/orders`

Создать новый заказ в системе.

**Request Body:**
```json
{
  "channel": "telegram",
  "customer": {
    "name": "Иван Иванов",
    "phone": "+79991234567",
    "address": "Москва, ул. Ленина, д. 1, кв. 10"
  },
  "items": [
    {
      "articul": "IPHONE15-256GB",
      "quantity": 2,
      "price_at_order": 99900.00
    }
  ],
  "delivery_city": "Москва",
  "notes": "Доставка до 18:00"
}
```

**Response 201 Created:**
```json
{
  "id": "660e8400-e29b-41d4-a716-446655440000",
  "order_number": "ORD-2026-0001",
  "status": "new",
  "channel": "telegram",
  "customer": {
    "name": "Иван Иванов",
    "phone": "+79991234567",
    "address": "Москва, ул. Ленина, д. 1, кв. 10"
  },
  "items": [
    {
      "id": "770e8400-e29b-41d4-a716-446655440000",
      "articul": "IPHONE15-256GB",
      "product_name": "iPhone 15 256GB",
      "quantity": 2,
      "price_at_order": 99900.00,
      "total": 199800.00
    }
  ],
  "total_amount": 200300.00,
  "delivery_cost": 500.00,
  "created_at": "2026-02-13T10:30:00Z",
  "updated_at": "2026-02-13T10:30:00Z"
}
```

**Response 400 Bad Request:**
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Ошибка валидации данных",
    "details": {
      "items": ["Товар с артикулом 'IPHONE15-256GB' отсутствует в наличии"]
    }
  }
}
```

**Пример запроса:**
```bash
curl -X POST "http://localhost:8025/api/orders" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "channel": "telegram",
    "customer": {
      "name": "Иван Иванов",
      "phone": "+79991234567",
      "address": "Москва, ул. Ленина, д. 1"
    },
    "items": [
      {
        "articul": "IPHONE15-256GB",
        "quantity": 2,
        "price_at_order": 99900.00
      }
    ],
    "delivery_city": "Москва"
  }'
```

---

### 2.2. Получить список заказов

**GET** `/api/orders`

Получить список заказов с фильтрацией и пагинацией.

**Query параметры:**
- `status` (optional) - фильтр по статусу (new, validated, invoice_created, paid, shipped, cancelled)
- `channel` (optional) - фильтр по каналу (telegram, yandex_mail, yandex_forms)
- `date_from` (optional) - фильтр по дате начала (ISO 8601)
- `date_to` (optional) - фильтр по дате окончания (ISO 8601)
- `search` (optional) - поиск по номеру заказа, ФИО, телефону
- `page` (optional) - номер страницы
- `page_size` (optional) - размер страницы
- `sort_by` (optional) - сортировка (created_at, total_amount, status)
- `sort_order` (optional) - порядок сортировки (asc, desc)

**Response 200 OK:**
```json
{
  "items": [
    {
      "id": "660e8400-e29b-41d4-a716-446655440000",
      "order_number": "ORD-2026-0001",
      "status": "paid",
      "channel": "telegram",
      "customer": {
        "name": "Иван Иванов",
        "phone": "+79991234567"
      },
      "total_amount": 200300.00,
      "created_at": "2026-02-13T10:30:00Z"
    }
  ],
  "total": 150,
  "page": 1,
  "page_size": 20,
  "pages": 8
}
```

**Пример запроса:**
```bash
curl -X GET "http://localhost:8025/api/orders?status=paid&date_from=2026-02-01&page=1" \
  -H "Authorization: Bearer <token>"
```

---

### 2.3. Получить заказ по ID

**GET** `/api/orders/{order_id}`

Получить детальную информацию о заказе.

**Path параметры:**
- `order_id` - UUID заказа

**Response 200 OK:**
```json
{
  "id": "660e8400-e29b-41d4-a716-446655440000",
  "order_number": "ORD-2026-0001",
  "status": "paid",
  "channel": "telegram",
  "customer": {
    "name": "Иван Иванов",
    "phone": "+79991234567",
    "address": "Москва, ул. Ленина, д. 1, кв. 10"
  },
  "items": [
    {
      "id": "770e8400-e29b-41d4-a716-446655440000",
      "articul": "IPHONE15-256GB",
      "product_name": "iPhone 15 256GB",
      "quantity": 2,
      "price_at_order": 99900.00,
      "total": 199800.00
    }
  ],
  "total_amount": 200300.00,
  "delivery_cost": 500.00,
  "tracking_number": "TRACK-20260213-123456",
  "created_at": "2026-02-13T10:30:00Z",
  "updated_at": "2026-02-13T11:00:00Z",
  "paid_at": "2026-02-13T10:45:00Z",
  "shipped_at": "2026-02-13T11:00:00Z",
  "invoice_exported_to_1c": true,
  "status_history": [
    {
      "status": "new",
      "changed_at": "2026-02-13T10:30:00Z"
    },
    {
      "status": "validated",
      "changed_at": "2026-02-13T10:32:00Z"
    },
    {
      "status": "paid",
      "changed_at": "2026-02-13T10:45:00Z"
    }
  ]
}
```

**Response 404 Not Found:**
```json
{
  "error": {
    "code": "ORDER_NOT_FOUND",
    "message": "Заказ с ID '660e8400-e29b-41d4-a716-446655440000' не найден"
  }
}
```

**Пример запроса:**
```bash
curl -X GET "http://localhost:8025/api/orders/660e8400-e29b-41d4-a716-446655440000" \
  -H "Authorization: Bearer <token>"
```

---

### 2.4. Обновить статус заказа

**PATCH** `/api/orders/{order_id}/status`

Обновить статус заказа.

**Path параметры:**
- `order_id` - UUID заказа

**Request Body:**
```json
{
  "status": "paid",
  "notes": "Оплата подтверждена"
}
```

**Response 200 OK:**
```json
{
  "id": "660e8400-e29b-41d4-a716-446655440000",
  "order_number": "ORD-2026-0001",
  "status": "paid",
  "updated_at": "2026-02-13T10:45:00Z",
  "paid_at": "2026-02-13T10:45:00Z"
}
```

**Response 400 Bad Request:**
```json
{
  "error": {
    "code": "INVALID_STATUS_TRANSITION",
    "message": "Невозможно изменить статус с 'new' на 'paid'. Пропущен статус 'validated'"
  }
}
```

**Пример запроса:**
```bash
curl -X PATCH "http://localhost:8025/api/orders/660e8400-e29b-41d4-a716-446655440000/status" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "paid"
  }'
```

---

### 2.5. Получить заказы по телефону

**GET** `/api/orders/by-phone`

Получить все заказы пользователя по номеру телефона. Используется для Mini App и отслеживания заказов из всех каналов.

**Query параметры:**
- `phone` (required) - номер телефона (любой формат, будет нормализован)
- `telegram_user_id` (optional) - Telegram user ID для проверки безопасности (опционально)

**Response 200 OK:**
```json
{
  "phone": "+79991234567",
  "normalized_phone": "+79991234567",
  "orders": [
    {
      "id": "660e8400-e29b-41d4-a716-446655440000",
      "order_number": "ORD-2026-0001",
      "status": "paid",
      "channel": "telegram",
      "customer_name": "Иван Иванов",
      "customer_phone": "+79991234567",
      "customer_address": "Москва, ул. Ленина, д. 1",
      "total_amount": 200300.00,
      "delivery_cost": 500.00,
      "tracking_number": "TRACK-20260213-123456",
      "created_at": "2026-02-13T10:30:00Z",
      "updated_at": "2026-02-13T11:00:00Z",
      "paid_at": "2026-02-13T10:45:00Z",
      "shipped_at": "2026-02-13T11:00:00Z"
    }
  ],
  "total": 1
}
```

**Пример запроса:**
```bash
curl -X GET "http://localhost:8025/api/orders/by-phone?phone=+79991234567" \
  -H "Authorization: Bearer <token>"
```

**Примечание:** Телефон автоматически нормализуется к формату `+7XXXXXXXXXX`. Заказы возвращаются из всех каналов (Telegram, Яндекс.Почта, Яндекс.Формы).

---

### 2.6. Получить товары заказа

**GET** `/api/orders/{order_id}/items`

Получить список товаров в заказе.

**Path параметры:**
- `order_id` - UUID заказа

**Response 200 OK:**
```json
{
  "items": [
    {
      "id": "770e8400-e29b-41d4-a716-446655440000",
      "articul": "IPHONE15-256GB",
      "product_name": "iPhone 15 256GB",
      "quantity": 2,
      "price_at_order": 99900.00,
      "total": 199800.00
    }
  ],
  "total": 1
}
```

**Пример запроса:**
```bash
curl -X GET "http://localhost:8025/api/orders/660e8400-e29b-41d4-a716-446655440000/items" \
  -H "Authorization: Bearer <token>"
```

---

### 2.7. Health Check для Orders API

**GET** `/api/orders/health`

Проверка состояния сервиса Orders API и подключения к базе данных.

**Response 200 OK:**
```json
{
  "status": "ok",
  "database": "ok",
  "service": "orders_api"
}
```

**Response 200 OK (degraded):**
```json
{
  "status": "degraded",
  "database": "error",
  "service": "orders_api"
}
```

**Пример запроса:**
```bash
curl -X GET "http://localhost:8025/api/orders/health"
```

**Примечание:** Этот endpoint не требует аутентификации и используется для мониторинга.

---

### 2.8. Swagger документация для Orders API

**GET** `/api/orders/docs`

Перенаправление на корневую Swagger документацию (`/docs`).

**Примечание:** Swagger документация для всех API endpoints доступна по адресу `http://localhost:8025/docs` на корневом уровне.

---

## 3. Доставка

### 3.1. Рассчитать стоимость доставки

**POST** `/api/delivery/calculate`

Рассчитать стоимость доставки для заказа.

**Request Body:**
```json
{
  "city": "Москва",
  "items": [
    {
      "articul": "IPHONE15-256GB",
      "quantity": 2
    }
  ]
}
```

**Response 200 OK:**
```json
{
  "city": "Москва",
  "weight": 0.4,
  "cost": 500.00,
  "estimated_days": 1,
  "carrier": "local"
}
```

**Пример запроса:**
```bash
curl -X POST "http://localhost:8025/api/delivery/calculate" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "city": "Москва",
    "items": [
      {
        "articul": "IPHONE15-256GB",
        "quantity": 2
      }
    ]
  }'
```

---

## 4. Оплата

### 4.1. Обработать оплату

**POST** `/api/payments/process/{order_id}`

Обработать оплату заказа (fake система).

**Path параметры:**
- `order_id` - UUID заказа

**Request Body:**
```json
{
  "card": {
    "number": "4111111111111111",
    "cvv": "123",
    "expiry": "12/25",
    "holder_name": "IVAN IVANOV"
  }
}
```

**Response 200 OK:**
```json
{
  "transaction_id": "TXN-20260213-123456",
  "order_id": "660e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "amount": 200300.00,
  "paid_at": "2026-02-13T10:45:00Z",
  "tracking_number": "TRACK-20260213-123456"
}
```

**Response 400 Bad Request:**
```json
{
  "error": {
    "code": "INVALID_CARD_DATA",
    "message": "Неверный формат данных карты"
  }
}
```

**Пример запроса:**
```bash
curl -X POST "http://localhost:8025/api/payments/process/660e8400-e29b-41d4-a716-446655440000" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "card": {
      "number": "4111111111111111",
      "cvv": "123",
      "expiry": "12/25",
      "holder_name": "IVAN IVANOV"
    }
  }'
```

---

## 5. Счета

### 5.1. Получить счёт заказа

**GET** `/api/invoices/{order_id}`

Получить информацию о счёте заказа.

**Path параметры:**
- `order_id` - UUID заказа

**Response 200 OK:**
```json
{
  "invoice_number": "INV-2026-0001",
  "order_id": "660e8400-e29b-41d4-a716-446655440000",
  "order_number": "ORD-2026-0001",
  "date": "2026-02-13",
  "customer": {
    "name": "Иван Иванов",
    "phone": "+79991234567",
    "address": "Москва, ул. Ленина, д. 1, кв. 10"
  },
  "items": [
    {
      "articul": "IPHONE15-256GB",
      "name": "iPhone 15 256GB",
      "quantity": 2,
      "price": 99900.00,
      "total": 199800.00
    }
  ],
  "subtotal": 199800.00,
  "delivery_cost": 500.00,
  "total": 200300.00,
  "pdf_url": "/api/invoices/660e8400-e29b-41d4-a716-446655440000/pdf"
}
```

**Пример запроса:**
```bash
curl -X GET "http://localhost:8025/api/invoices/660e8400-e29b-41d4-a716-446655440000" \
  -H "Authorization: Bearer <token>"
```

---

### 5.2. Скачать PDF счёта

**GET** `/api/invoices/{order_id}/pdf`

Скачать PDF файл счёта.

**Path параметры:**
- `order_id` - UUID заказа

**Response 200 OK:**
- Content-Type: `application/pdf`
- Body: PDF файл

**Response 404 Not Found:**
```json
{
  "error": {
    "code": "INVOICE_NOT_FOUND",
    "message": "Счёт для заказа не найден"
  }
}
```

**Пример запроса:**
```bash
curl -X GET "http://localhost:8025/api/invoices/660e8400-e29b-41d4-a716-446655440000/pdf" \
  -H "Authorization: Bearer <token>" \
  -o invoice.pdf
```

---

## 6. Dashboard

### 6.1. Получить статистику

**GET** `/api/dashboard/stats`

Получить статистику для dashboard.

**Примечание:** Dashboard API работает на порту **8028**, а не 8025.

**Базовый URL для Dashboard:**
```
Development: http://localhost:8028
```

**Query параметры:**
- `period` (optional) - период (today, week, month, year, default: today)
- `date_from` (optional) - дата начала (ISO 8601)
- `date_to` (optional) - дата окончания (ISO 8601)

**Response 200 OK:**
```json
{
  "period": "today",
  "revenue": {
    "total": 500000.00,
    "currency": "RUB"
  },
  "orders": {
    "total": 25,
    "new": 5,
    "validated": 10,
    "paid": 8,
    "shipped": 2
  },
  "conversion": {
    "new_to_paid": 0.32,
    "validated_to_paid": 0.80
  },
  "average_order_value": 20000.00,
  "top_products": [
    {
      "articul": "IPHONE15-256GB",
      "name": "iPhone 15 256GB",
      "quantity_sold": 10,
      "revenue": 999000.00
    }
  ]
}
```

**Пример запроса:**
```bash
curl -X GET "http://localhost:8028/api/dashboard/stats?period=week" \
  -H "Authorization: Bearer <token>"
```

---

### 6.2. Получить статус синхронизации

**GET** `/api/dashboard/sync-status`

Получить статус синхронизации с 1С.

**Response 200 OK:**
```json
{
  "last_sync": "2026-02-13T10:30:00Z",
  "next_sync": "2026-02-13T11:00:00Z",
  "status": "success",
  "products_count": 1000,
  "sync_duration_seconds": 45,
  "errors": []
}
```

**Пример запроса:**
```bash
curl -X GET "http://localhost:8028/api/dashboard/sync-status" \
  -H "Authorization: Bearer <token>"
```

**Примечание:** Swagger документация для Dashboard API доступна по адресу `http://localhost:8028/docs`.

---

## 7. Webhooks

### 7.1. Яндекс.Формы Webhook

**POST** `/webhook/yandex-forms`

Принять данные от Яндекс.Форм.

**Request Body:**
```json
{
  "form_id": "form_123",
  "form_name": "Заказ товара",
  "submission_id": "sub_456",
  "data": {
    "name": "Иван Иванов",
    "phone": "+79991234567",
    "address": "Москва, ул. Ленина, д. 1",
    "products": "2x iPhone 15"
  },
  "timestamp": "2026-02-13T10:30:00Z"
}
```

**Response 200 OK:**
```json
{
  "status": "received",
  "message": "Данные приняты и добавлены в очередь обработки"
}
```

**Пример запроса:**
```bash
curl -X POST "http://localhost:8025/webhook/yandex-forms" \
  -H "Content-Type: application/json" \
  -H "X-Yandex-Forms-Signature: <signature>" \
  -d '{
    "form_id": "form_123",
    "form_name": "Заказ товара",
    "submission_id": "sub_456",
    "data": {
      "name": "Иван Иванов",
      "phone": "+79991234567",
      "products": "2x iPhone 15"
    }
  }'
```

---

## 8. Модели данных

### 8.1. Product (Товар)
```json
{
  "id": "UUID",
  "articul": "string (unique)",
  "name": "string",
  "price": "decimal",
  "stock": "integer",
  "updated_at": "ISO 8601",
  "synced_at": "ISO 8601"
}
```

### 8.2. Order (Заказ)
```json
{
  "id": "UUID",
  "order_number": "string (unique, format: ORD-YYYY-NNNN)",
  "status": "enum: new, validated, invoice_created, paid, shipped, cancelled",
  "channel": "enum: telegram, yandex_mail, yandex_forms",
  "customer": {
    "name": "string",
    "phone": "string",
    "address": "string"
  },
  "items": ["OrderItem"],
  "total_amount": "decimal",
  "delivery_cost": "decimal",
  "tracking_number": "string (nullable)",
  "created_at": "ISO 8601",
  "updated_at": "ISO 8601",
  "paid_at": "ISO 8601 (nullable)",
  "shipped_at": "ISO 8601 (nullable)",
  "invoice_exported_to_1c": "boolean"
}
```

### 8.3. OrderItem (Товар в заказе)
```json
{
  "id": "UUID",
  "order_id": "UUID",
  "articul": "string",
  "product_name": "string",
  "quantity": "integer",
  "price_at_order": "decimal",
  "total": "decimal"
}
```

### 8.4. Invoice (Счёт)
```json
{
  "invoice_number": "string (format: INV-YYYY-NNNN)",
  "order_id": "UUID",
  "date": "ISO 8601 date",
  "customer": {
    "name": "string",
    "phone": "string",
    "address": "string"
  },
  "items": ["InvoiceItem"],
  "subtotal": "decimal",
  "delivery_cost": "decimal",
  "total": "decimal"
}
```

---

## 9. Rate Limiting

Для защиты от злоупотреблений применяется rate limiting:

- **Публичные endpoints** (webhooks): 100 запросов/минуту на IP
- **Защищённые endpoints**: 1000 запросов/минуту на токен
- **Заголовки ответа:**
  ```
  X-RateLimit-Limit: 1000
  X-RateLimit-Remaining: 999
  X-RateLimit-Reset: 1642248000
  ```

При превышении лимита возвращается `429 Too Many Requests`.

---

## 10. Версионирование

API использует версионирование через URL:
- Текущая версия: `/api/v1/...`
- Будущие версии: `/api/v2/...`

Старые версии поддерживаются минимум 6 месяцев после выхода новой версии.

---

## 11. WebSocket (опционально)

Для real-time обновлений (статусы заказов, статистика) может использоваться WebSocket:

**Endpoint:** `ws://localhost:8025/ws`

**Подписка на события:**
```json
{
  "action": "subscribe",
  "channels": ["orders", "stats"]
}
```

**События:**
```json
{
  "channel": "orders",
  "event": "status_changed",
  "data": {
    "order_id": "660e8400-e29b-41d4-a716-446655440000",
    "status": "paid"
  }
}
```

---

## 12. Примеры использования

### Полный цикл создания заказа

```bash
# 1. Поиск товара
curl -X GET "http://localhost:8025/api/catalog/search?q=iphone" \
  -H "Authorization: Bearer <token>"

# 2. Расчёт доставки
curl -X POST "http://localhost:8025/api/delivery/calculate" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"city": "Москва", "items": [{"articul": "IPHONE15-256GB", "quantity": 2}]}'

# 3. Создание заказа
curl -X POST "http://localhost:8025/api/orders" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "channel": "telegram",
    "customer": {"name": "Иван Иванов", "phone": "+79991234567", "address": "Москва"},
    "items": [{"articul": "IPHONE15-256GB", "quantity": 2, "price_at_order": 99900.00}],
    "delivery_city": "Москва"
  }'

# 4. Получение счёта
curl -X GET "http://localhost:8025/api/invoices/{order_id}" \
  -H "Authorization: Bearer <token>"

# 5. Оплата
curl -X POST "http://localhost:8025/api/payments/process/{order_id}" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "card": {"number": "4111111111111111", "cvv": "123", "expiry": "12/25", "holder_name": "IVAN IVANOV"}
  }'
```

---

## 13. Поддержка

- **Документация**: https://docs.smartorder.example.com
- **Email поддержки**: support@smartorder.example.com
- **Статус API**: https://status.smartorder.example.com

---

**Версия документа**: 1.0  
**Последнее обновление**: 2026-02-13
