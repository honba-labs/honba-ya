CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    client_order_id VARCHAR(64) NOT NULL UNIQUE,
    broker_order_id VARCHAR(64),
    instrument VARCHAR(96) NOT NULL,
    side VARCHAR(16) NOT NULL,
    order_type VARCHAR(96) NOT NULL,
    tif VARCHAR(16) NOT NULL,
    quantity DECIMAL(30,16) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending_new',
    algo_id VARCHAR(64) NOT NULL,
    client_id VARCHAR(255) NOT NULL,
    filled_quantity DECIMAL(30,16) NOT NULL DEFAULT '0',
    avg_fill_price DECIMAL(30,16),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_broker_order_id ON orders(broker_order_id);
CREATE INDEX IF NOT EXISTS idx_orders_algo_id ON orders(algo_id);
CREATE INDEX IF NOT EXISTS idx_orders_client_id ON orders(client_id);
