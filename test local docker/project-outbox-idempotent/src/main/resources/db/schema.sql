-- ============================================================
-- Схема БД для проекта Outbox + Idempotent Consumer
-- ============================================================

-- Таблица заказов (бизнес-данные)
CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    amount INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Таблица Outbox (события для отправки в Kafka)
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, created_at);

-- Таблица Inbox (идемпотентность на стороне consumer)
CREATE TABLE IF NOT EXISTS inbox (
    event_id UUID PRIMARY KEY,
    consumer_group VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Таблица платежей (бизнес-данные consumer)
CREATE TABLE IF NOT EXISTS payments (
    order_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    amount INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Таблица DLT (Dead Letter Topic) — для анализа ошибок
CREATE TABLE IF NOT EXISTS dlt_messages (
    id SERIAL PRIMARY KEY,
    event_id UUID,
    original_topic VARCHAR(64),
    error_message TEXT,
    payload TEXT,
    retry_count INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Очистка для повторного запуска
TRUNCATE TABLE orders, outbox, inbox, payments, dlt_messages;
