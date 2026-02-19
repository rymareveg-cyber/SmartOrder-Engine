-- =============================================================================
-- SmartOrder Engine — Database Schema
-- Migration 001: Full schema (tables, functions, triggers)
-- Run this FIRST, before 002_indexes.sql
-- =============================================================================

-- Enable trigram extension for full-text product search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =============================================================================
-- PRODUCTS TABLE (catalog synced from 1C)
-- =============================================================================
CREATE TABLE IF NOT EXISTS products (
    id         UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    articul    VARCHAR(255)  NOT NULL UNIQUE,
    name       VARCHAR(500)  NOT NULL,
    price      DECIMAL(12,2) NOT NULL CHECK (price >= 0),
    stock      INTEGER       NOT NULL CHECK (stock >= 0),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    synced_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE products IS 'Product catalog synced from 1C:УНФ';
COMMENT ON COLUMN products.articul  IS 'Unique product article (from 1C)';
COMMENT ON COLUMN products.price    IS 'Product price in rubles';
COMMENT ON COLUMN products.stock    IS 'Available stock quantity';
COMMENT ON COLUMN products.synced_at IS 'Last successful sync timestamp with 1C';

-- Auto-update updated_at for products
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_products_updated_at ON products;
CREATE TRIGGER update_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- =============================================================================
-- ORDERS TABLE
-- Statuses: new → validated → invoice_created → paid → order_created_1c → tracking_issued
-- =============================================================================
CREATE TABLE IF NOT EXISTS orders (
    id                     UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    order_number           VARCHAR(20)   NOT NULL UNIQUE,
    status                 VARCHAR(20)   NOT NULL DEFAULT 'new'
                           CHECK (status IN (
                               'new', 'validated', 'invoice_created',
                               'paid', 'order_created_1c', 'tracking_issued',
                               'shipped', 'cancelled'
                           )),
    channel                VARCHAR(20)   NOT NULL CHECK (channel IN ('telegram', 'yandex_mail', 'yandex_forms')),
    customer_name          VARCHAR(255),
    customer_phone         VARCHAR(50),
    customer_address       TEXT,
    total_amount           DECIMAL(12,2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
    delivery_cost          DECIMAL(12,2) NOT NULL DEFAULT 0 CHECK (delivery_cost >= 0),
    tracking_number        VARCHAR(100),
    transaction_id         VARCHAR(255),
    invoice_exported_to_1c BOOLEAN       DEFAULT FALSE,
    telegram_user_id       BIGINT,
    customer_email         VARCHAR(255),
    notification_sent_at   TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    created_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    paid_at                TIMESTAMP WITH TIME ZONE,
    shipped_at             TIMESTAMP WITH TIME ZONE
);

COMMENT ON TABLE orders IS 'Customer orders';
COMMENT ON COLUMN orders.order_number IS 'Human-readable order number: ORD-YYYY-NNNN';
COMMENT ON COLUMN orders.status IS 'Order lifecycle: new→validated→invoice_created→paid→order_created_1c→tracking_issued';
COMMENT ON COLUMN orders.channel IS 'Source channel: telegram, yandex_mail, yandex_forms';
COMMENT ON COLUMN orders.telegram_user_id IS 'Telegram user ID for notification delivery';
COMMENT ON COLUMN orders.customer_email IS 'Customer email address (used for yandex_mail channel notifications)';
COMMENT ON COLUMN orders.notification_sent_at IS 'When the order confirmation was sent to the client (used for recovery)';
COMMENT ON COLUMN orders.invoice_exported_to_1c IS 'True when invoice has been exported to 1C after payment';

-- Auto-update updated_at for orders
CREATE OR REPLACE FUNCTION update_orders_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_orders_updated_at_trigger ON orders;
CREATE TRIGGER update_orders_updated_at_trigger
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_orders_updated_at();

-- Sequence for readable order numbers
CREATE SEQUENCE IF NOT EXISTS order_number_seq
    START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;

CREATE OR REPLACE FUNCTION generate_order_number()
RETURNS VARCHAR(20) AS $$
DECLARE
    current_year VARCHAR(4);
    seq_number   INTEGER;
BEGIN
    current_year := TO_CHAR(CURRENT_DATE, 'YYYY');
    seq_number   := nextval('order_number_seq');
    RETURN 'ORD-' || current_year || '-' || LPAD(seq_number::TEXT, 4, '0');
END;
$$ LANGUAGE plpgsql;

-- Phone normalization: +7XXXXXXXXXX format
CREATE OR REPLACE FUNCTION normalize_phone(phone VARCHAR) RETURNS VARCHAR AS $$
BEGIN
    IF phone IS NULL OR phone = '' THEN RETURN NULL; END IF;
    RETURN regexp_replace(
        regexp_replace(
            regexp_replace(phone, '[^0-9+]', '', 'g'),
            '^8', '+7', 'g'
        ),
        '^7', '+7', 'g'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION normalize_phone(VARCHAR) IS 'Normalizes phone to +7XXXXXXXXXX format';


-- =============================================================================
-- ORDER_ITEMS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS order_items (
    id              UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID          NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_articul VARCHAR(255)  NOT NULL REFERENCES products(articul) ON DELETE RESTRICT,
    product_name    VARCHAR(500)  NOT NULL,
    quantity        INTEGER       NOT NULL CHECK (quantity > 0),
    price_at_order  DECIMAL(12,2) NOT NULL CHECK (price_at_order > 0),
    total           DECIMAL(12,2) NOT NULL CHECK (total >= 0),
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE order_items IS 'Line items for each order (snapshot of price at order time)';
COMMENT ON COLUMN order_items.price_at_order IS 'Price at the moment of ordering (snapshot)';
COMMENT ON COLUMN order_items.total IS 'quantity * price_at_order';


-- =============================================================================
-- TELEGRAM_USERS TABLE (authorized users with phone numbers)
-- =============================================================================
CREATE TABLE IF NOT EXISTS telegram_users (
    telegram_user_id BIGINT       PRIMARY KEY,
    phone            VARCHAR(50)  NOT NULL,
    first_name       VARCHAR(255),
    last_name        VARCHAR(255),
    username         VARCHAR(255),
    authorized_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_activity    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(phone)
);

COMMENT ON TABLE telegram_users IS 'Authorized Telegram users (telegram_user_id → phone mapping)';
COMMENT ON COLUMN telegram_users.phone IS 'Normalized phone number (+7XXXXXXXXXX)';
