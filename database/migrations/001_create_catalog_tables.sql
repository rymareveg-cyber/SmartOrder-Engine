-- Миграция 001: Создание таблицы каталога товаров
-- Дата создания: 2026-02-13
-- Описание: Таблица для хранения каталога товаров из 1С

-- Создание таблицы products
CREATE TABLE IF NOT EXISTS products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    articul VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(500) NOT NULL,
    price DECIMAL(12, 2) NOT NULL CHECK (price >= 0),
    stock INTEGER NOT NULL CHECK (stock >= 0),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    synced_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_products_articul ON products(articul);
CREATE INDEX IF NOT EXISTS idx_products_name ON products(name);
CREATE INDEX IF NOT EXISTS idx_products_synced_at ON products(synced_at);
CREATE INDEX IF NOT EXISTS idx_products_stock ON products(stock) WHERE stock > 0;

-- Индекс для полнотекстового поиска по названию (для PostgreSQL)
-- Используется для поиска товаров по частичному совпадению названия
CREATE INDEX IF NOT EXISTS idx_products_name_trgm ON products USING gin(name gin_trgm_ops);

-- Комментарии к таблице и полям
COMMENT ON TABLE products IS 'Каталог товаров из 1С:Управление нашей фирмой';
COMMENT ON COLUMN products.id IS 'Уникальный идентификатор товара (UUID)';
COMMENT ON COLUMN products.articul IS 'Артикул товара (уникальный идентификатор из 1С)';
COMMENT ON COLUMN products.name IS 'Наименование товара';
COMMENT ON COLUMN products.price IS 'Цена товара в рублях';
COMMENT ON COLUMN products.stock IS 'Остаток товара на складе';
COMMENT ON COLUMN products.updated_at IS 'Время последнего обновления записи';
COMMENT ON COLUMN products.synced_at IS 'Время последней успешной синхронизации с 1С';
COMMENT ON COLUMN products.created_at IS 'Время создания записи';

-- Функция для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Триггер для автоматического обновления updated_at
CREATE TRIGGER update_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
