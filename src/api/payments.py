#!/usr/bin/env python3
"""
API Payments — FastAPI сервис для обработки оплаты.

Предоставляет:
  - GET  /pay/{token}          — страница оплаты (HTML)
  - GET  /api/payments/order/{token} — данные заказа (JSON) для страницы
  - POST /api/payments/pay/{token}   — обработка платежа по токену
  - POST /api/payments/process/{order_id} — прямая обработка (для дашборда)
  - POST /api/payments/create-link/{order_id} — создание платёжной ссылки
"""

import os
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status, Path as FastAPIPath
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from fastapi.exceptions import RequestValidationError
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

from src.services.payment_processor import PaymentProcessor, PaymentValidationError, PaymentProcessingError
from src.services.order_service import OrderService
from src.utils.logger import get_logger
from src.config import APIConfig, RedisConfig

logger = get_logger(__name__)

API_PORT = APIConfig.PAYMENTS_PORT
API_HOST = APIConfig.HOST

# Директория с фронтендом страницы оплаты
_FRONTEND_DIR = Path(__file__).parent / "payment_frontend"

# TTL токена — 24 часа
PAYMENT_TOKEN_TTL = 24 * 3600  # секунды


# ─────────────────────────── Redis helper ───────────────────────────

def _get_redis():
    """Получить Redis-клиент (импорт по требованию)."""
    from src.utils.redis_client import init_redis_client
    return init_redis_client(decode_responses=True)


def create_payment_token(order_id: str) -> str:
    """Создать одноразовый токен оплаты, сохранить в Redis, вернуть токен."""
    token = uuid.uuid4().hex
    redis = _get_redis()
    redis.setex(f"payment_token:{token}", PAYMENT_TOKEN_TTL, order_id)
    return token


def get_order_id_by_token(token: str) -> Optional[str]:
    """Вернуть order_id по токену или None."""
    try:
        redis = _get_redis()
        return redis.get(f"payment_token:{token}")
    except Exception:
        return None


def get_token_ttl(token: str) -> Optional[int]:
    """Вернуть оставшееся время жизни токена в секундах."""
    try:
        redis = _get_redis()
        return redis.ttl(f"payment_token:{token}")
    except Exception:
        return None


def delete_payment_token(token: str) -> None:
    """Удалить токен после успешной оплаты."""
    try:
        redis = _get_redis()
        redis.delete(f"payment_token:{token}")
    except Exception:
        pass


# ─────────────────────────── Pydantic models ───────────────────────────

class CardData(BaseModel):
    number: str = Field(..., description="Номер карты (16 цифр)")
    cvv: str = Field(..., description="CVV (3 цифры)")
    expiry: str = Field(..., description="Срок MM/YY")
    holder_name: str = Field(..., description="Имя держателя")

    @field_validator('number')
    @classmethod
    def validate_number(cls, v):
        import re
        cleaned = re.sub(r'[\s-]', '', v)
        if not re.match(r'^\d{16}$', cleaned):
            raise ValueError("Card number must be 16 digits")
        return cleaned

    @field_validator('cvv')
    @classmethod
    def validate_cvv(cls, v):
        import re
        if not re.match(r'^\d{3}$', v):
            raise ValueError("CVV must be 3 digits")
        return v

    @field_validator('expiry')
    @classmethod
    def validate_expiry(cls, v):
        import re
        if not re.match(r'^\d{2}/\d{2}$', v):
            raise ValueError("Expiry must be MM/YY")
        month, year = v.split('/')
        mm, yy = int(month), int(year)
        if mm < 1 or mm > 12:
            raise ValueError("Month 01–12")
        cur_yy = datetime.now().year % 100
        cur_mm = datetime.now().month
        if yy < cur_yy or (yy == cur_yy and mm < cur_mm):
            raise ValueError("Card has expired")
        return v


class PaymentRequest(BaseModel):
    card: CardData


class PaymentResponse(BaseModel):
    transaction_id: str
    order_id: str
    order_number: str
    status: str
    amount: float
    paid_at: str
    card_last4: str


class CreateLinkResponse(BaseModel):
    order_id: str
    order_number: str
    payment_url: str
    token: str
    expires_at: str


# ─────────────────────────── FastAPI app ───────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting Payments API on {API_HOST}:{API_PORT}")
    yield
    logger.info("Shutting down Payments API")


app = FastAPI(
    title="SmartOrder Engine — Payments API",
    description="Платёжный сервис с HTML-страницей оплаты и токен-ссылками.",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS — разрешаем запросы с любых источников (для dev-окружения)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Раздача статических файлов фронтенда (если папка существует)
if _FRONTEND_DIR.exists():
    app.mount("/static/pay", StaticFiles(directory=str(_FRONTEND_DIR)), name="pay-static")


# ─────────────────────────── Exception handlers ───────────────────────────

@app.exception_handler(RequestValidationError)
async def _validation_handler(request, exc):
    errors = [f"{' -> '.join(str(l) for l in e['loc'])}: {e['msg']}" for e in exc.errors()]
    return JSONResponse(
        status_code=422,
        content={"error": {"code": "VALIDATION_ERROR", "details": errors}},
    )


@app.exception_handler(PaymentValidationError)
async def _card_validation_handler(request, exc):
    return JSONResponse(
        status_code=400,
        content={"error": {"code": "INVALID_CARD_DATA", "message": str(exc)}},
    )


@app.exception_handler(PaymentProcessingError)
async def _payment_error_handler(request, exc):
    return JSONResponse(
        status_code=400,
        content={"error": {"code": "PAYMENT_PROCESSING_ERROR", "message": str(exc)}},
    )


# ─────────────────────────── Utility endpoints ───────────────────────────

@app.get("/")
async def root():
    return {"service": "SmartOrder Payments API", "version": "2.0.0", "status": "running"}


@app.get("/favicon.ico")
async def favicon():
    from fastapi.responses import Response
    return Response(status_code=204)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "payments_api"}

@app.get("/health/live")
async def health_live():
    return {"status": "ok"}


# ─────────────────────────── Payment page (HTML) ───────────────────────────

@app.get(
    "/pay/{token}",
    response_class=HTMLResponse,
    summary="Страница оплаты",
    description="HTML-страница ввода данных карты. Доступна по одноразовому токену.",
    include_in_schema=False,
)
async def payment_page(token: str):
    """Отдаём страницу оплаты (SPA на чистом HTML/JS)."""
    order_id = get_order_id_by_token(token)
    if not order_id:
        return HTMLResponse(
            content=_error_html("Ссылка недействительна или истекла",
                                "Запросите новую ссылку на оплату у менеджера."),
            status_code=404,
        )

    order = OrderService.get_order(order_id)
    if not order:
        return HTMLResponse(
            content=_error_html("Заказ не найден", f"Заказ для токена {token} не существует."),
            status_code=404,
        )

    if order.status not in ("invoice_created", "new", "validated"):
        if order.status == "paid":
            return HTMLResponse(content=_already_paid_html(order), status_code=200)
        return HTMLResponse(
            content=_error_html(
                "Оплата невозможна",
                f"Заказ {order.order_number} имеет статус «{order.status}». Оплата уже была произведена или заказ отменён."
            ),
            status_code=400,
        )

    html_path = _FRONTEND_DIR / "index.html"
    if not html_path.exists():
        raise HTTPException(500, "Payment frontend not found")

    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


# ─────────────────────────── Order data for frontend ───────────────────────────

@app.get(
    "/api/payments/order/{token}",
    summary="Данные заказа по токену",
    description="Возвращает информацию о заказе для отображения на странице оплаты.",
)
async def get_order_by_token(token: str):
    order_id = get_order_id_by_token(token)
    if not order_id:
        raise HTTPException(status_code=404, detail="Token expired or invalid")

    order = OrderService.get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    ttl = get_token_ttl(token) or PAYMENT_TOKEN_TTL
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=ttl)).isoformat()

    items = []
    if order.items:
        for it in order.items:
            items.append({
                "name":          it.product_name,
                "articul":       it.product_articul,
                "quantity":      it.quantity,
                "price_at_order": float(it.price_at_order),
            })

    return {
        "order_id":      str(order.id),
        "order_number":  order.order_number,
        "total_amount":  float(order.total_amount),
        "delivery_cost": float(order.delivery_cost or 0),
        "status":        order.status,
        "items":         items,
        "expires_at":    expires_at,
    }


# ─────────────────────────── Pay by token (from frontend) ───────────────────────────

@app.post(
    "/api/payments/pay/{token}",
    response_model=PaymentResponse,
    summary="Оплатить заказ по токену",
    description="Endpoint, который вызывает страница оплаты после ввода карты.",
)
async def pay_by_token(
    token: str,
    payment_request: PaymentRequest,
):
    """Обработка оплаты по токену (вызывается фронтендом платёжной страницы)."""
    import asyncio

    order_id = get_order_id_by_token(token)
    if not order_id:
        raise HTTPException(status_code=404, detail="Ссылка на оплату истекла или недействительна")

    try:
        card_data = payment_request.card.model_dump()
        result = await asyncio.to_thread(PaymentProcessor.process_payment, order_id, card_data)

        # Удаляем токен — одноразовый
        delete_payment_token(token)

        logger.info(
            f"Payment via token successful: order={result['order_number']}, "
            f"txn={result['transaction_id']}, amount={result['amount']}"
        )

        return PaymentResponse(**result)

    except (PaymentValidationError, PaymentProcessingError):
        raise
    except Exception as e:
        logger.error(f"Unexpected error paying by token {token}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка обработки платежа")


# ─────────────────────────── Direct process (dashboard) ───────────────────────────

@app.post(
    "/api/payments/process/{order_id}",
    response_model=PaymentResponse,
    summary="Прямая обработка оплаты (дашборд)",
    description="Обработка оплаты с явными данными карты. Используется из дашборда.",
)
async def process_payment_direct(
    order_id: str = FastAPIPath(..., description="UUID заказа"),
    payment_request: PaymentRequest = ...,
):
    import asyncio
    try:
        card_data = payment_request.card.model_dump()
        result = await asyncio.to_thread(PaymentProcessor.process_payment, order_id, card_data)
        logger.info(f"Direct payment: order={result['order_number']}, txn={result['transaction_id']}")
        return PaymentResponse(**result)
    except (PaymentValidationError, PaymentProcessingError):
        raise
    except Exception as e:
        logger.error(f"Error in direct payment for order {order_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Payment processing failed")


# ─────────────────────────── Create payment link ───────────────────────────

@app.post(
    "/api/payments/create-link/{order_id}",
    response_model=CreateLinkResponse,
    summary="Создать платёжную ссылку",
    description="Генерирует уникальный токен и возвращает ссылку на страницу оплаты.",
)
async def create_payment_link(
    order_id: str = FastAPIPath(..., description="UUID заказа"),
):
    """Создаёт платёжную ссылку для заказа."""
    order = OrderService.get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

    token = create_payment_token(order_id)
    base_url = _get_base_url()
    payment_url = f"{base_url}/pay/{token}"
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=PAYMENT_TOKEN_TTL)).isoformat()

    logger.info(f"Payment link created for order {order.order_number}: {payment_url}")

    return CreateLinkResponse(
        order_id=order_id,
        order_number=order.order_number,
        payment_url=payment_url,
        token=token,
        expires_at=expires_at,
    )


# ─────────────────────────── Helpers ───────────────────────────

def _get_base_url() -> str:
    """
    Базовый URL платёжного сервиса.
    
    Приоритет:
    1. Переменная APP_BASE_URL из .env (рекомендуется для prod и правильного dev)
    2. Авто-определение реального IP машины (для dev без APP_BASE_URL)
    
    ВАЖНО: localhost-ссылки не работают в email-клиентах и Telegram-кнопках.
    Используйте реальный IP машины или доменное имя в APP_BASE_URL.
    """
    from src.config import APIConfig
    base = os.getenv("APP_BASE_URL", "").rstrip("/")
    if base:
        return base
    
    port = APIConfig.PAYMENTS_PORT
    
    # Пытаемся определить реальный IP машины в локальной сети
    # Это позволяет открывать ссылки из email/Telegram на том же компьютере или из ЛВС
    try:
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            # Подключаемся к внешнему адресу (без реальной передачи данных)
            # чтобы узнать какой интерфейс используется как "основной"
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
        if local_ip and not local_ip.startswith("127."):
            return f"http://{local_ip}:{port}"
    except Exception:
        pass
    
    # Fallback к localhost
    return f"http://localhost:{port}"


def _error_html(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} — SmartOrder</title>
<style>
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
       background:#f1f5f9;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}}
  .box{{background:#fff;border-radius:14px;box-shadow:0 4px 24px rgba(0,0,0,.1);
        padding:40px;text-align:center;max-width:400px;width:90%}}
  .icon{{font-size:48px;margin-bottom:16px}}
  h1{{font-size:20px;color:#1e293b;margin-bottom:8px}}
  p{{font-size:14px;color:#64748b;line-height:1.6}}
</style></head>
<body><div class="box">
  <div class="icon">⚠️</div>
  <h1>{title}</h1>
  <p>{body}</p>
</div></body></html>"""


def _already_paid_html(order) -> str:
    return f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Уже оплачено — SmartOrder</title>
<style>
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
       background:linear-gradient(135deg,#e0e7ff,#ecfdf5);display:flex;align-items:center;
       justify-content:center;min-height:100vh;margin:0}}
  .box{{background:#fff;border-radius:14px;box-shadow:0 4px 24px rgba(0,0,0,.1);
        padding:40px;text-align:center;max-width:400px;width:90%}}
  .icon{{width:72px;height:72px;background:#dcfce7;border-radius:50%;display:flex;
         align-items:center;justify-content:center;margin:0 auto 16px;font-size:32px}}
  h1{{font-size:22px;color:#1e293b;margin-bottom:8px}}
  p{{font-size:14px;color:#64748b}}
  .badge{{display:inline-block;background:#dcfce7;color:#16a34a;border-radius:20px;
           padding:4px 14px;font-size:12px;font-weight:700;margin-top:12px}}
</style></head>
<body><div class="box">
  <div class="icon">✅</div>
  <h1>Заказ уже оплачен</h1>
  <p>Заказ <strong>{order.order_number}</strong> успешно оплачен.<br>Следите за статусом доставки.</p>
  <span class="badge">✓ Оплачено</span>
</div></body></html>"""


# ─────────────────────────── Entry point ───────────────────────────

if __name__ == "__main__":
    import uvicorn
    import sys
    import signal

    os.makedirs("logs", exist_ok=True)

    try:
        from src.utils.logger import setup_uvicorn_logging
        setup_uvicorn_logging("api_payments")
    except Exception as e:
        logger.warning(f"Failed to setup uvicorn logging: {e}")

    def _sig(signum, frame):
        logger.info(f"Signal {signum}, shutting down...")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    try:
        uvicorn.run(app, host=API_HOST, port=API_PORT, log_level="info", log_config=None)
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
