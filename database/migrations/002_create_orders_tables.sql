-- Migration 002: Create orders and order_items tables
-- Created: 2026-02-15
-- Description: Tables for storing orders and order items with invoice export tracking

-- Create order status type
DO $$ BEGIN
    CREATE TYPE order_status AS ENUM (
        'new',
        'validated',
        'invoice_created',
        'paid',
        'shipped',
        'cancelled'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_number VARCHAR(20) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'new' CHECK (status IN ('new', 'validated', 'invoice_created', 'paid', 'shipped', 'cancelled')),
    channel VARCHAR(20) NOT NULL CHECK (channel IN ('telegram', 'yandex_mail', 'yandex_forms')),
    customer_name VARCHAR(255),
    customer_phone VARCHAR(50),
    customer_address TEXT,
    total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
    delivery_cost DECIMAL(12, 2) NOT NULL DEFAULT 0 CHECK (delivery_cost >= 0),
    tracking_number VARCHAR(100),
    transaction_id VARCHAR(255),
    invoice_exported_to_1c BOOLEAN DEFAULT FALSE,
    telegram_user_id BIGINT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    paid_at TIMESTAMP WITH TIME ZONE,
    shipped_at TIMESTAMP WITH TIME ZONE
);

-- Create order_items table
CREATE TABLE IF NOT EXISTS order_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_articul VARCHAR(255) NOT NULL REFERENCES products(articul) ON DELETE RESTRICT,
    product_name VARCHAR(500) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price_at_order DECIMAL(12, 2) NOT NULL CHECK (price_at_order > 0),
    total DECIMAL(12, 2) NOT NULL CHECK (total >= 0),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fast search
CREATE INDEX IF NOT EXISTS idx_orders_order_number ON orders(order_number);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_customer_phone ON orders(customer_phone);
CREATE INDEX IF NOT EXISTS idx_orders_channel ON orders(channel);
CREATE INDEX IF NOT EXISTS idx_orders_invoice_exported ON orders(invoice_exported_to_1c);
CREATE INDEX IF NOT EXISTS idx_orders_transaction_id ON orders(transaction_id);
CREATE INDEX IF NOT EXISTS idx_orders_telegram_user_id ON orders(telegram_user_id);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_articul ON order_items(product_articul);

-- Comments
COMMENT ON TABLE orders IS 'Customer orders';
COMMENT ON COLUMN orders.id IS 'Unique order identifier (UUID)';
COMMENT ON COLUMN orders.order_number IS 'Order number (ORD-YYYY-NNNN)';
COMMENT ON COLUMN orders.status IS 'Order status: new, validated, invoice_created, paid, shipped, cancelled';
COMMENT ON COLUMN orders.channel IS 'Order source channel: telegram, yandex_mail, yandex_forms';
COMMENT ON COLUMN orders.customer_name IS 'Customer full name';
COMMENT ON COLUMN orders.customer_phone IS 'Customer phone';
COMMENT ON COLUMN orders.customer_address IS 'Delivery address';
COMMENT ON COLUMN orders.total_amount IS 'Total order amount (items + delivery)';
COMMENT ON COLUMN orders.delivery_cost IS 'Delivery cost';
COMMENT ON COLUMN orders.tracking_number IS 'Shipping tracking number';
COMMENT ON COLUMN orders.transaction_id IS 'Payment transaction ID (generated after successful payment)';
COMMENT ON COLUMN orders.invoice_exported_to_1c IS 'Flag: invoice exported to 1C';
COMMENT ON COLUMN orders.telegram_user_id IS 'Telegram user ID for linking orders from Telegram';
COMMENT ON COLUMN orders.paid_at IS 'Payment timestamp';
COMMENT ON COLUMN orders.shipped_at IS 'Shipping timestamp';

COMMENT ON TABLE order_items IS 'Order items';
COMMENT ON COLUMN order_items.id IS 'Unique item identifier (UUID)';
COMMENT ON COLUMN order_items.order_id IS 'Reference to order';
COMMENT ON COLUMN order_items.product_articul IS 'Product articul (snapshot at order time)';
COMMENT ON COLUMN order_items.product_name IS 'Product name (snapshot at order time)';
COMMENT ON COLUMN order_items.quantity IS 'Item quantity';
COMMENT ON COLUMN order_items.price_at_order IS 'Product price at order time';
COMMENT ON COLUMN order_items.total IS 'Item total (quantity * price_at_order)';

-- Function to auto-update updated_at
CREATE OR REPLACE FUNCTION update_orders_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for auto-update updated_at
DROP TRIGGER IF EXISTS update_orders_updated_at_trigger ON orders;
CREATE TRIGGER update_orders_updated_at_trigger
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION update_orders_updated_at();

-- Function to generate order number using SEQUENCE
CREATE SEQUENCE IF NOT EXISTS order_number_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE OR REPLACE FUNCTION generate_order_number()
RETURNS VARCHAR(20) AS $$
DECLARE
    current_year VARCHAR(4);
    seq_number INTEGER;
    order_num VARCHAR(20);
BEGIN
    current_year := TO_CHAR(CURRENT_DATE, 'YYYY');
    
    -- Get next value from sequence (guaranteed unique)
    seq_number := nextval('order_number_seq');
    
    -- Format number with leading zeros
    order_num := 'ORD-' || current_year || '-' || LPAD(seq_number::TEXT, 4, '0');
    
    RETURN order_num;
END;
$$ LANGUAGE plpgsql;

-- Function to normalize phone numbers (remove spaces, dashes, parentheses, keep only digits and +)
CREATE OR REPLACE FUNCTION normalize_phone(phone VARCHAR) RETURNS VARCHAR AS $$
BEGIN
    IF phone IS NULL OR phone = '' THEN
        RETURN NULL;
    END IF;
    -- Remove all non-digit characters except +
    -- Then normalize: if starts with 8, replace with +7; if starts with 7, add +
    RETURN regexp_replace(
        regexp_replace(
            regexp_replace(phone, '[^0-9+]', '', 'g'),
            '^8', '+7', 'g'
        ),
        '^7', '+7', 'g'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Comment on function
COMMENT ON FUNCTION normalize_phone(VARCHAR) IS 'Normalizes phone numbers to format +7XXXXXXXXXX (11 digits with +7)';
