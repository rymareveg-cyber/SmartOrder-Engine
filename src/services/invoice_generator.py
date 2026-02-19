#!/usr/bin/env python3
"""
Invoice Generator - генератор PDF счётов в стиле 1С.

Генерирует PDF счёта на оплату в формате, похожем на 1С.
"""

import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase.cidfonts import UnicodeCIDFont

from src.config import PROJECT_ROOT
from src.utils.logger import get_logger
from src.services.order_service import OrderService, Order

logger = get_logger(__name__)

COMPANY_NAME = os.getenv('COMPANY_NAME', 'ООО "Ваша Компания"')
COMPANY_INN = os.getenv('COMPANY_INN', '1234567890')
COMPANY_KPP = os.getenv('COMPANY_KPP', '123456789')
COMPANY_ADDRESS = os.getenv('COMPANY_ADDRESS', 'г. Москва, ул. Примерная, д. 1')

INVOICES_DIR = PROJECT_ROOT / '.tmp' / 'invoices'
INVOICES_DIR.mkdir(parents=True, exist_ok=True)


def register_cyrillic_fonts():
    """
    Регистрация шрифтов с поддержкой кириллицы.
    
    Пытается использовать системные шрифты Windows или fallback на Unicode CID шрифты.
    """
    try:
        import platform
        if platform.system() == 'Windows':
            font_paths = [
                ('C:/Windows/Fonts/arial.ttf', 'ArialCyrillic'),
                ('C:/Windows/Fonts/ARIAL.TTF', 'ArialCyrillic'),
                ('C:/Windows/Fonts/arialbd.ttf', 'ArialCyrillicBold'),
                ('C:/Windows/Fonts/ARIALBD.TTF', 'ArialCyrillicBold'),
                ('C:/Windows/Fonts/times.ttf', 'TimesCyrillic'),
                ('C:/Windows/Fonts/TIMES.TTF', 'TimesCyrillic'),
            ]
            
            registered_font = None
            registered_bold = None
            
            for font_path, font_name in font_paths:
                if os.path.exists(font_path):
                    try:
                        if 'Bold' in font_name or 'bd' in font_path.lower():
                            if not registered_bold:
                                pdfmetrics.registerFont(TTFont(font_name, font_path))
                                registered_bold = font_name
                                logger.info(f"Registered Cyrillic bold font: {font_path}")
                        else:
                            if not registered_font:
                                pdfmetrics.registerFont(TTFont(font_name, font_path))
                                registered_font = font_name
                                logger.info(f"Registered Cyrillic font: {font_path}")
                        
                        if registered_font and registered_bold:
                            return registered_font, registered_bold
                    except Exception as e:
                        logger.debug(f"Failed to register font {font_path}: {e}")
                        continue
            
            if registered_font:
                if not registered_bold:
                    registered_bold = registered_font
                return registered_font, registered_bold
        
        # Fallback: использование Unicode CID шрифтов
        try:
            pdfmetrics.registerFont(UnicodeCIDFont('Helvetica'))
            pdfmetrics.registerFont(UnicodeCIDFont('Helvetica-Bold'))
            logger.info("Registered Unicode CID fonts: Helvetica")
            return 'Helvetica', 'Helvetica-Bold'
        except Exception as e:
            logger.debug(f"Failed to register Unicode CID fonts: {e}")
        
        logger.warning("Could not register Cyrillic fonts, using default (may have encoding issues)")
        return 'Helvetica', 'Helvetica-Bold'
    
    except Exception as e:
        logger.warning(f"Error registering Cyrillic fonts: {e}, using default")
        return 'Helvetica', 'Helvetica-Bold'


# Регистрация шрифтов при импорте модуля
CYRILLIC_FONT, CYRILLIC_FONT_BOLD = register_cyrillic_fonts()


def generate_invoice_number(order_date: datetime) -> str:
    """
    Генерация номера счёта (INV-YYYY-NNNN).
    
    Args:
        order_date: Дата заказа
        
    Returns:
        Номер счёта
    """
    year = order_date.year
    number = int(order_date.timestamp()) % 10000
    return f"INV-{year}-{number:04d}"


def format_currency(amount: float) -> str:
    """
    Форматирование суммы в рублях.
    
    Args:
        amount: Сумма
        
    Returns:
        Отформатированная строка
    """
    return f"{amount:,.2f}".replace(',', ' ').replace('.', ',') + ' ₽'


def generate_invoice_pdf(order: Order, invoice_number: str) -> str:
    """
    Генерация PDF счёта.
    
    Args:
        order: Заказ
        invoice_number: Номер счёта
        
    Returns:
        Путь к созданному PDF файлу
    """
    try:
        pdf_path = INVOICES_DIR / f"{order.id}.pdf"
        
        doc = SimpleDocTemplate(
            str(pdf_path),
            pagesize=A4,
            rightMargin=20*mm,
            leftMargin=20*mm,
            topMargin=20*mm,
            bottomMargin=20*mm
        )
        
        styles = getSampleStyleSheet()
        
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontName=CYRILLIC_FONT,
            fontSize=16,
            textColor=colors.HexColor('#000000'),
            alignment=1,
            spaceAfter=12
        )
        
        normal_style = ParagraphStyle(
            'CustomNormal',
            parent=styles['Normal'],
            fontName=CYRILLIC_FONT,
            fontSize=10,
            leading=12
        )
        
        bold_style = ParagraphStyle(
            'CustomBold',
            parent=styles['Normal'],
            fontName=CYRILLIC_FONT_BOLD,
            fontSize=10,
            leading=12
        )
        
        story = []
        
        # Шапка: название компании
        story.append(Paragraph(COMPANY_NAME, title_style))
        story.append(Spacer(1, 5*mm))
        
        # Реквизиты компании
        company_info = [
            f"ИНН: {COMPANY_INN}",
            f"КПП: {COMPANY_KPP}",
            f"Адрес: {COMPANY_ADDRESS}"
        ]
        for info in company_info:
            story.append(Paragraph(info, normal_style))
        story.append(Spacer(1, 10*mm))
        
        # Заголовок
        story.append(Paragraph("<b>СЧЁТ НА ОПЛАТУ</b>", title_style))
        story.append(Spacer(1, 5*mm))
        
        # Номер счёта и дата
        invoice_date = datetime.now(timezone.utc).strftime("%d.%m.%Y")
        invoice_info = [
            f"№ {invoice_number}",
            f"от {invoice_date}"
        ]
        invoice_table = Table([invoice_info], colWidths=[100*mm, 70*mm])
        invoice_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), CYRILLIC_FONT),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
        ]))
        story.append(invoice_table)
        story.append(Spacer(1, 10*mm))
        
        # Плательщик
        story.append(Paragraph("<b>Плательщик:</b>", bold_style))
        payer_info = []
        if order.customer_name:
            payer_info.append(f"ФИО: {order.customer_name}")
        if order.customer_phone:
            payer_info.append(f"Телефон: {order.customer_phone}")
        if order.customer_address:
            payer_info.append(f"Адрес: {order.customer_address}")
        
        if payer_info:
            for info in payer_info:
                story.append(Paragraph(info, normal_style))
        else:
            story.append(Paragraph("Не указано", normal_style))
        story.append(Spacer(1, 10*mm))
        
        # Таблица товаров
        story.append(Paragraph("<b>Товары:</b>", bold_style))
        story.append(Spacer(1, 3*mm))
        
        table_data = [
            ['№', 'Артикул', 'Наименование', 'Кол-во', 'Цена', 'Сумма']
        ]
        
        items_subtotal = 0.0
        for idx, item in enumerate(order.items, 1):
            item_total = item.quantity * item.price_at_order
            items_subtotal += item_total
            
            product_name = item.product_name
            if len(product_name) > 35:
                product_name = product_name[:32] + '...'
            
            price_str = f"{item.price_at_order:,.2f}".replace(',', ' ').replace('.', ',')
            total_str = f"{item_total:,.2f}".replace(',', ' ').replace('.', ',')
            
            table_data.append([
                str(idx),
                item.product_articul,
                product_name,
                str(item.quantity),
                price_str,
                total_str
            ])
        
        items_table = Table(table_data, colWidths=[12*mm, 28*mm, 75*mm, 18*mm, 28*mm, 28*mm])
        items_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E0E0E0')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#000000')),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), CYRILLIC_FONT_BOLD),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('TOPPADDING', (0, 0), (-1, 0), 8),
            ('FONTNAME', (0, 1), (-1, -1), CYRILLIC_FONT),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 1), (0, -1), 'CENTER'),
            ('ALIGN', (1, 1), (1, -1), 'LEFT'),
            ('ALIGN', (2, 1), (2, -1), 'LEFT'),
            ('ALIGN', (3, 1), (3, -1), 'CENTER'),
            ('ALIGN', (4, 1), (4, -1), 'RIGHT'),
            ('ALIGN', (5, 1), (5, -1), 'RIGHT'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#CCCCCC')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        story.append(items_table)
        story.append(Spacer(1, 10*mm))
        
        # Итого
        delivery_cost = order.delivery_cost
        total_amount = items_subtotal + delivery_cost
        
        totals_data = [
            ['Сумма товаров:', format_currency(items_subtotal).replace(' ₽', '')],
            ['Доставка:', format_currency(delivery_cost).replace(' ₽', '')],
            ['ИТОГО:', format_currency(total_amount).replace(' ₽', '')]
        ]
        
        totals_table = Table(totals_data, colWidths=[100*mm, 70*mm])
        totals_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, 1), CYRILLIC_FONT),
            ('FONTNAME', (1, 0), (1, 1), CYRILLIC_FONT),
            ('FONTSIZE', (0, 0), (-1, 1), 10),
            ('FONTNAME', (0, 2), (-1, 2), CYRILLIC_FONT_BOLD),
            ('FONTSIZE', (0, 2), (-1, 2), 12),
            ('TOPPADDING', (0, 2), (-1, 2), 8),
            ('BOTTOMPADDING', (0, 2), (-1, 2), 8),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(totals_table)
        story.append(Spacer(1, 15*mm))
        
        # Реквизиты для оплаты (fake)
        story.append(Paragraph("<b>Реквизиты для оплаты:</b>", bold_style))
        payment_details = [
            "Банк: Фейковый Банк",
            "БИК: 123456789",
            "Корр. счёт: 30101810100000000593",
            "Расчётный счёт: 40702810100000000001",
            "Назначение платежа: Оплата по счёту № " + invoice_number
        ]
        for detail in payment_details:
            story.append(Paragraph(detail, normal_style))
        
        # Генерация PDF
        doc.build(story)
        
        logger.info(f"Invoice PDF generated: {pdf_path} (invoice: {invoice_number})")
        return str(pdf_path)
    
    except Exception as e:
        logger.error(f"Error generating invoice PDF: {e}", exc_info=True)
        raise


class InvoiceGenerator:
    """Класс для генерации PDF счётов."""
    
    @staticmethod
    def generate_invoice(order_id: str) -> Dict[str, Any]:
        """
        Генерация счёта для заказа.
        
        Args:
            order_id: UUID заказа
            
        Returns:
            Словарь с информацией о счёте
        """
        try:
            order = OrderService.get_order(order_id)
            if not order:
                raise ValueError(f"Order {order_id} not found")
            
            if order.status != "validated":
                if order.status in {"invoice_created", "paid", "shipped"}:
                    logger.debug(f"Order {order_id} status is {order.status}, expected 'validated' (allowed for re-generate)")
                else:
                    logger.warning(f"Order {order_id} status is {order.status}, expected 'validated'")
            
            order_date = datetime.fromisoformat(order.created_at.replace('Z', '+00:00'))
            invoice_number = generate_invoice_number(order_date)
            
            pdf_path = generate_invoice_pdf(order, invoice_number)
            
            try:
                if order.status == "invoice_created":
                    logger.debug(f"Order {order_id} already has status invoice_created, skipping status update")
                elif order.status != "validated":
                    logger.warning(
                        f"Skipping status update to invoice_created for order {order_id}: current status is {order.status}"
                    )
                else:
                    OrderService.update_order_status(order_id, "invoice_created")
                    logger.info(f"Order {order_id} status updated to invoice_created")
            except Exception as e:
                logger.warning(f"Failed to update order status: {e}")
            
            result = {
                "invoice_number": invoice_number,
                "order_id": order_id,
                "order_number": order.order_number,
                "pdf_path": pdf_path,
                "date": datetime.now(timezone.utc).strftime("%Y-%m-%d")
            }
            
            logger.info(f"Invoice generated: {invoice_number} for order {order.order_number}")
            return result
        
        except Exception as e:
            logger.error(f"Error generating invoice: {e}", exc_info=True)
            raise
