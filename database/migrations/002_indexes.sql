-- =============================================================================
-- SmartOrder Engine — Indexes
-- Migration 002: All indexes for performance
-- Run AFTER 001_schema.sql
-- =============================================================================

-- =============================================================================
-- PRODUCTS indexes
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_products_articul   ON products(articul);
CREATE INDEX IF NOT EXISTS idx_products_name      ON products(name);
CREATE INDEX IF NOT EXISTS idx_products_synced_at ON products(synced_at);
CREATE INDEX IF NOT EXISTS idx_products_stock     ON products(stock) WHERE stock > 0;

-- Full-text search on product name (requires pg_trgm from 001_schema.sql)
CREATE INDEX IF NOT EXISTS idx_products_name_trgm ON products USING gin(name gin_trgm_ops);

-- Composite indexes for catalog API filtering
CREATE INDEX IF NOT EXISTS idx_products_stock_price           ON products(stock, price);
CREATE INDEX IF NOT EXISTS idx_products_name_articul          ON products(name, articul);
CREATE INDEX IF NOT EXISTS idx_products_stock_price_available ON products(stock, price) WHERE stock > 0;

-- Covering index for sorted catalog listing (avoids heap fetch)
CREATE INDEX IF NOT EXISTS idx_products_name_covering
    ON products(name) INCLUDE (id, articul, price, stock, updated_at, synced_at);


-- =============================================================================
-- ORDERS indexes
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_orders_order_number      ON orders(order_number);
CREATE INDEX IF NOT EXISTS idx_orders_status            ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created_at        ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_customer_phone    ON orders(customer_phone);
CREATE INDEX IF NOT EXISTS idx_orders_channel           ON orders(channel);
CREATE INDEX IF NOT EXISTS idx_orders_invoice_exported  ON orders(invoice_exported_to_1c);
CREATE INDEX IF NOT EXISTS idx_orders_transaction_id    ON orders(transaction_id);
CREATE INDEX IF NOT EXISTS idx_orders_telegram_user_id  ON orders(telegram_user_id);

-- Composite indexes for dashboard statistics
CREATE INDEX IF NOT EXISTS idx_orders_created_at_status
    ON orders(created_at, status) WHERE status != 'cancelled';

CREATE INDEX IF NOT EXISTS idx_orders_created_at_delivery_cost
    ON orders(created_at, delivery_cost) WHERE delivery_cost > 0;

CREATE INDEX IF NOT EXISTS idx_orders_created_at_address
    ON orders(created_at, customer_address)
    WHERE customer_address IS NOT NULL AND customer_address != '';

CREATE INDEX IF NOT EXISTS idx_orders_paid_at    ON orders(paid_at)    WHERE paid_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_shipped_at ON orders(shipped_at) WHERE shipped_at IS NOT NULL;

-- Search by phone + channel (omnichannel order lookup)
CREATE INDEX IF NOT EXISTS idx_orders_phone_channel
    ON orders(customer_phone, channel);

-- Telegram user order lookup
CREATE INDEX IF NOT EXISTS idx_orders_telegram_user_channel
    ON orders(telegram_user_id, channel) WHERE telegram_user_id IS NOT NULL;


-- =============================================================================
-- ORDER_ITEMS indexes
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_order_items_order_id        ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_articul ON order_items(product_articul);


-- =============================================================================
-- TELEGRAM_USERS indexes
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_telegram_users_phone         ON telegram_users(phone);
CREATE INDEX IF NOT EXISTS idx_telegram_users_last_activity ON telegram_users(last_activity);


-- =============================================================================
-- Index comments
-- =============================================================================
COMMENT ON INDEX idx_products_name_trgm              IS 'Trigram index for fuzzy product name search';
COMMENT ON INDEX idx_orders_created_at_status        IS 'Dashboard stats: filter by date + status';
COMMENT ON INDEX idx_orders_created_at_delivery_cost IS 'Dashboard analytics: delivery cost by date';
COMMENT ON INDEX idx_orders_phone_channel            IS 'Omnichannel: find orders by phone across channels';
COMMENT ON INDEX idx_orders_telegram_user_channel    IS 'Telegram: find user orders by telegram_user_id';
