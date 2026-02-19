#!/usr/bin/env python3
"""
Email Notifier - модуль для отправки email-уведомлений клиентам.

Отправляет уведомления о создании заказа, оплате и трек-номерах.
"""

import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from typing import Optional, Dict, Any, List

from src.config import SMTPConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _get_smtp_connection():
    """
    Создание SMTP соединения.
    Пробует SSL (port 465) → если не удаётся, STARTTLS (port 587).
    """
    errors = []

    # Попытка 1: SSL на настроенном порту
    try:
        if SMTPConfig.USE_SSL:
            server = smtplib.SMTP_SSL(SMTPConfig.HOST, SMTPConfig.PORT, timeout=15)
        else:
            server = smtplib.SMTP(SMTPConfig.HOST, SMTPConfig.PORT, timeout=15)
            if SMTPConfig.USE_TLS:
                server.starttls()

        if SMTPConfig.USER and SMTPConfig.PASSWORD:
            server.login(SMTPConfig.USER, SMTPConfig.PASSWORD)

        logger.info(
            f"SMTP connected: {SMTPConfig.HOST}:{SMTPConfig.PORT} "
            f"(SSL={SMTPConfig.USE_SSL}, TLS={SMTPConfig.USE_TLS})"
        )
        return server
    except (smtplib.SMTPException, OSError, TimeoutError) as e:
        errors.append(f"SSL port {SMTPConfig.PORT}: {e}")

    # Попытка 2: STARTTLS на порту 587 (fallback)
    if SMTPConfig.PORT != 587:
        try:
            server = smtplib.SMTP(SMTPConfig.HOST, 587, timeout=15)
            server.starttls()
            if SMTPConfig.USER and SMTPConfig.PASSWORD:
                server.login(SMTPConfig.USER, SMTPConfig.PASSWORD)
            logger.info(f"SMTP connected via STARTTLS fallback: {SMTPConfig.HOST}:587")
            return server
        except (smtplib.SMTPException, OSError, TimeoutError) as e:
            errors.append(f"STARTTLS port 587: {e}")

    error_summary = "; ".join(errors)
    logger.error(
        f"All SMTP connection attempts failed. "
        f"Errors: {error_summary}. "
        f"Check SMTP_USER, SMTP_PASSWORD (use app password for Yandex) in .env"
    )
    raise smtplib.SMTPException(f"SMTP connection failed: {error_summary}")


def _send_email(msg: MIMEMultipart, to_email: str, max_retries: int = 2) -> bool:
    """Отправка письма с retry при временных ошибках."""
    for attempt in range(1, max_retries + 1):
        server = None
        try:
            server = _get_smtp_connection()
            server.send_message(msg)
            logger.info(f"Email sent to {to_email} (attempt {attempt})")
            return True
        except smtplib.SMTPResponseException as e:
            # 4xx — временная ошибка, можно повторить
            if 400 <= e.smtp_code < 500:
                logger.warning(
                    f"SMTP temporary error {e.smtp_code} sending to {to_email} "
                    f"(attempt {attempt}/{max_retries}): {e.smtp_error}. "
                    f"If this persists — check SMTP_PASSWORD: Yandex requires "
                    f"an app-specific password (passport.yandex.ru → Security → App passwords)"
                )
                if attempt < max_retries:
                    time.sleep(3 * attempt)
                    continue
            else:
                logger.error(f"SMTP permanent error {e.smtp_code} sending to {to_email}: {e.smtp_error}")
            return False
        except (smtplib.SMTPException, OSError, TimeoutError) as e:
            logger.error(f"SMTP error sending to {to_email} (attempt {attempt}): {e}")
            if attempt < max_retries:
                time.sleep(3)
                continue
            return False
        finally:
            if server:
                try:
                    server.quit()
                except Exception:
                    pass
    return False


def send_order_confirmation_email(
    to_email: str,
    order_number: str,
    order_data: Dict[str, Any],
    invoice_number: Optional[str] = None,
    invoice_pdf_path: Optional[str] = None,
    payment_url: Optional[str] = None
) -> bool:
    """
    Отправка email-уведомления о создании заказа с PDF-счётом и ссылкой на оплату.

    Args:
        to_email: Email получателя
        order_number: Номер заказа
        order_data: Данные заказа (товары, суммы, контакты)
        invoice_number: Номер счета
        invoice_pdf_path: Путь к PDF-файлу счёта для вложения
        payment_url: Ссылка на страницу оплаты

    Returns:
        True если успешно отправлено, False в противном случае
    """
    if not SMTPConfig.USER or not SMTPConfig.PASSWORD:
        logger.warning("SMTP credentials not configured, skipping email notification")
        return False

    try:
        # Формирование текста письма
        customer_name = order_data.get('customer_name', 'уважаемый клиент')
        items = order_data.get("items", [])
        delivery_cost = float(order_data.get("delivery_cost", 0.0))

        message_parts = [
            f"Здравствуйте, {customer_name}!",
            "",
            f"✅ Ваш заказ #{order_number} принят и создан счёт на оплату.",
            "",
            "📦 Состав заказа:",
            ""
        ]

        total_items = 0
        total_amount = 0.0
        for item in items:
            product_name = item.get("product_name", "Неизвестный товар")
            quantity = item.get("quantity", 1)
            price = float(item.get("price", 0.0))
            subtotal = quantity * price
            message_parts.append(
                f"   • {product_name} — {quantity} шт. × {price:,.2f} ₽ = {subtotal:,.2f} ₽"
            )
            total_items += quantity
            total_amount += subtotal

        message_parts.extend(["", f"   Итого товаров: {total_items} шт."])

        if delivery_cost > 0:
            message_parts.append(f"   Доставка: {delivery_cost:,.2f} ₽")
            total_amount += delivery_cost

        message_parts.extend([
            "",
            f"💰 Итоговая сумма к оплате: {total_amount:,.2f} ₽",
            ""
        ])

        if invoice_number:
            message_parts.append(
                f"📄 Счёт на оплату: #{invoice_number}"
                + (" (PDF во вложении)" if invoice_pdf_path else "")
            )
            message_parts.append("")

        message_parts.extend([
            "📍 Адрес доставки:",
            f"   {order_data.get('customer_address', 'не указан')}",
            "",
            "📞 Телефон для связи:",
            f"   {order_data.get('customer_phone', 'не указан')}",
            "",
        ])

        # Блок оплаты
        if payment_url:
            message_parts.extend([
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                "💳 ОПЛАТА ЗАКАЗА",
                "",
                "Для оплаты заказа перейдите по ссылке:",
                f"  {payment_url}",
                "",
                "На странице оплаты введите любые тестовые данные карты и нажмите «Оплатить».",
                "После оплаты мы сразу оформим отправку и пришлём вам трек-номер.",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            ])
        else:
            message_parts.extend([
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                "После оплаты счёта мы оформим отправку и пришлём вам трек-номер.",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            ])

        message_parts.extend([
            "",
            "С уважением,",
            SMTPConfig.FROM_NAME
        ])

        plain_text = "\n".join(message_parts)

        # HTML-версия письма для красивого отображения
        html_items_rows = ""
        total_items = 0
        total_amount_html = 0.0
        for item in items:
            product_name = item.get("product_name", "Неизвестный товар")
            quantity = item.get("quantity", 1)
            price = float(item.get("price", 0.0))
            subtotal = quantity * price
            html_items_rows += f"""
                <tr>
                    <td style="padding:6px 10px;border-bottom:1px solid #eee;">{product_name}</td>
                    <td style="padding:6px 10px;border-bottom:1px solid #eee;text-align:center;">{quantity}</td>
                    <td style="padding:6px 10px;border-bottom:1px solid #eee;text-align:right;">{price:,.2f} ₽</td>
                    <td style="padding:6px 10px;border-bottom:1px solid #eee;text-align:right;">{subtotal:,.2f} ₽</td>
                </tr>"""
            total_items += quantity
            total_amount_html += subtotal

        if delivery_cost > 0:
            html_items_rows += f"""
                <tr style="color:#555;">
                    <td colspan="3" style="padding:6px 10px;border-bottom:1px solid #eee;">Доставка</td>
                    <td style="padding:6px 10px;border-bottom:1px solid #eee;text-align:right;">{delivery_cost:,.2f} ₽</td>
                </tr>"""
            total_amount_html += delivery_cost

        payment_block_html = ""
        if payment_url:
            payment_block_html = f"""
            <div style="background:#f0f7ff;border:2px solid #007bff;border-radius:8px;padding:20px;margin:20px 0;text-align:center;">
                <p style="margin:0 0 12px;font-size:16px;font-weight:bold;color:#333;">💳 Оплата заказа</p>
                <p style="margin:0 0 16px;color:#555;">Нажмите кнопку ниже для оплаты заказа:</p>
                <a href="{payment_url}"
                   style="display:inline-block;background:#007bff;color:#fff;text-decoration:none;
                          padding:14px 32px;border-radius:6px;font-size:16px;font-weight:bold;">
                    💳 Оплатить заказ
                </a>
                <p style="margin:16px 0 0;font-size:12px;color:#888;">
                    После оплаты вы получите трек-номер для отслеживания посылки.
                </p>
            </div>"""
        else:
            payment_block_html = """
            <p style="color:#555;">После оплаты счёта мы оформим отправку и пришлём вам трек-номер.</p>"""

        invoice_note_html = ""
        if invoice_number:
            pdf_note = " (PDF-счёт во вложении)" if invoice_pdf_path else ""
            invoice_note_html = f'<p>📄 <b>Счёт на оплату:</b> #{invoice_number}{pdf_note}</p>'

        html_body = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#333;">
    <div style="background:#28a745;padding:20px;border-radius:8px 8px 0 0;text-align:center;">
        <h1 style="color:#fff;margin:0;font-size:22px;">✅ Заказ #{order_number} принят</h1>
    </div>
    <div style="background:#fff;padding:24px;border:1px solid #ddd;border-top:none;border-radius:0 0 8px 8px;">
        <p>Здравствуйте, <b>{customer_name}</b>!</p>
        <p>Ваш заказ принят и счёт на оплату создан.</p>

        <h3 style="border-bottom:2px solid #28a745;padding-bottom:6px;">📦 Состав заказа</h3>
        <table style="width:100%;border-collapse:collapse;font-size:14px;">
            <thead>
                <tr style="background:#f8f9fa;">
                    <th style="padding:8px 10px;text-align:left;border-bottom:2px solid #dee2e6;">Товар</th>
                    <th style="padding:8px 10px;text-align:center;border-bottom:2px solid #dee2e6;">Кол-во</th>
                    <th style="padding:8px 10px;text-align:right;border-bottom:2px solid #dee2e6;">Цена</th>
                    <th style="padding:8px 10px;text-align:right;border-bottom:2px solid #dee2e6;">Сумма</th>
                </tr>
            </thead>
            <tbody>{html_items_rows}
                <tr style="font-weight:bold;background:#f8f9fa;">
                    <td colspan="3" style="padding:8px 10px;">💰 Итого к оплате</td>
                    <td style="padding:8px 10px;text-align:right;">{total_amount_html:,.2f} ₽</td>
                </tr>
            </tbody>
        </table>

        {invoice_note_html}

        <h3 style="border-bottom:2px solid #28a745;padding-bottom:6px;">📋 Данные доставки</h3>
        <p>📍 <b>Адрес:</b> {order_data.get('customer_address', 'не указан')}</p>
        <p>📞 <b>Телефон:</b> {order_data.get('customer_phone', 'не указан')}</p>

        {payment_block_html}

        <hr style="border:none;border-top:1px solid #eee;margin:20px 0;">
        <p style="color:#888;font-size:12px;text-align:center;">
            С уважением, {SMTPConfig.FROM_NAME}
        </p>
    </div>
</body>
</html>"""

        msg = MIMEMultipart('mixed')
        msg['From'] = f"{SMTPConfig.FROM_NAME} <{SMTPConfig.FROM_EMAIL or SMTPConfig.USER}>"
        msg['To'] = to_email
        msg['Subject'] = f"✅ Заказ #{order_number} принят — счёт на оплату"

        # Добавляем plain + HTML альтернативы
        alt_part = MIMEMultipart('alternative')
        alt_part.attach(MIMEText(plain_text, 'plain', 'utf-8'))
        alt_part.attach(MIMEText(html_body, 'html', 'utf-8'))
        msg.attach(alt_part)

        # Прикрепляем PDF-счёт если есть
        if invoice_pdf_path:
            pdf_path = Path(invoice_pdf_path)
            if pdf_path.exists():
                try:
                    with open(pdf_path, 'rb') as f:
                        pdf_data = f.read()
                    pdf_part = MIMEBase('application', 'pdf')
                    pdf_part.set_payload(pdf_data)
                    encoders.encode_base64(pdf_part)
                    pdf_part.add_header(
                        'Content-Disposition',
                        'attachment',
                        filename=f"invoice_{order_number}.pdf"
                    )
                    msg.attach(pdf_part)
                    logger.info(f"PDF invoice attached for order {order_number}")
                except Exception as e:
                    logger.warning(f"Failed to attach PDF invoice for order {order_number}: {e}")

        return _send_email(msg, to_email)

    except Exception as e:
        logger.error(f"Failed to build order confirmation email for {to_email}: {e}", exc_info=True)
        return False


def send_clarification_email(
    to_email: str,
    clarification_questions: List[str],
    unfound_products: List[str],
    parsed_products: Optional[List[Dict[str, Any]]] = None
) -> bool:
    """
    Отправка email с уточняющими вопросами.

    Args:
        to_email: Email получателя
        clarification_questions: Список уточняющих вопросов
        unfound_products: Список нераспознанных товаров
        parsed_products: Список уже распознанных товаров

    Returns:
        True если успешно отправлено, False в противном случае
    """
    if not SMTPConfig.USER or not SMTPConfig.PASSWORD:
        logger.warning("SMTP credentials not configured, skipping clarification email")
        return False

    try:
        message_parts = [
            "Здравствуйте!",
            "",
            "Спасибо за ваше обращение! Мы получили вашу заявку и обрабатываем её.",
            "Для оформления заказа нам нужно уточнить некоторые детали.",
            ""
        ]

        if parsed_products:
            message_parts.append("✅ Уже распознано:")
            for product in parsed_products:
                product_name = product.get('name', 'Неизвестно')
                articul = product.get('articul', '')
                quantity = product.get('quantity', 1)
                articul_str = f" (арт. {articul})" if articul else ""
                message_parts.append(f"   • {product_name}{articul_str} — {quantity} шт.")
            message_parts.append("")

        if unfound_products:
            message_parts.append("❓ Товары не найдены в каталоге:")
            for product in unfound_products:
                message_parts.append(f"   • {product}")
            message_parts.append("")
            message_parts.append("Пожалуйста, уточните артикулы или точные названия этих товаров.")
            message_parts.append("")

        if clarification_questions:
            message_parts.append("❓ Пожалуйста, ответьте на следующие вопросы:")
            for i, question in enumerate(clarification_questions, 1):
                message_parts.append(f"   {i}. {question}")
            message_parts.append("")
            message_parts.append("Просто ответьте на это письмо с нужной информацией.")

        if not clarification_questions and not unfound_products:
            message_parts.append("Пожалуйста, уточните детали вашего заказа, ответив на это письмо.")

        message_parts.extend([
            "",
            "С уважением,",
            SMTPConfig.FROM_NAME
        ])

        message_text = "\n".join(message_parts)

        msg = MIMEMultipart('alternative')
        msg['From'] = f"{SMTPConfig.FROM_NAME} <{SMTPConfig.FROM_EMAIL or SMTPConfig.USER}>"
        msg['To'] = to_email
        msg['Subject'] = "❓ Уточнение по вашему заказу"

        text_part = MIMEText(message_text, 'plain', 'utf-8')
        msg.attach(text_part)

        return _send_email(msg, to_email)

    except Exception as e:
        logger.error(f"Failed to build clarification email for {to_email}: {e}", exc_info=True)
        return False


def send_tracking_email(
    to_email: str,
    order_number: str,
    tracking_number: str,
    customer_name: Optional[str] = None
) -> bool:
    """
    Отправка email с трек-номером отправления.

    Args:
        to_email: Email получателя
        order_number: Номер заказа
        tracking_number: Трек-номер
        customer_name: Имя клиента

    Returns:
        True если успешно отправлено, False в противном случае
    """
    if not SMTPConfig.USER or not SMTPConfig.PASSWORD:
        logger.warning("SMTP credentials not configured, skipping tracking email")
        return False

    try:
        name = customer_name or "уважаемый клиент"
        message_parts = [
            f"Здравствуйте, {name}!",
            "",
            f"🚚 Ваш заказ #{order_number} оплачен и передан в доставку!",
            "",
            f"Трек-номер для отслеживания: {tracking_number}",
            "",
            "Используйте трек-номер для отслеживания посылки на сайте транспортной компании.",
            "",
            "С уважением,",
            SMTPConfig.FROM_NAME
        ]

        message_text = "\n".join(message_parts)

        msg = MIMEMultipart('alternative')
        msg['From'] = f"{SMTPConfig.FROM_NAME} <{SMTPConfig.FROM_EMAIL or SMTPConfig.USER}>"
        msg['To'] = to_email
        msg['Subject'] = f"🚚 Ваш заказ #{order_number} отправлен — трек-номер"

        text_part = MIMEText(message_text, 'plain', 'utf-8')
        msg.attach(text_part)

        return _send_email(msg, to_email)

    except Exception as e:
        logger.error(f"Failed to send tracking email for order {order_number} to {to_email}: {e}", exc_info=True)
        return False
