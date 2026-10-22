CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT NOT NULL UNIQUE,
    broker_order_id TEXT,
    instrument TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    tif TEXT NOT NULL,
    quantity TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending_new',
    algo_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    filled_quantity TEXT NOT NULL DEFAULT '0',
    avg_fill_price TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_broker_order_id ON orders(broker_order_id);
CREATE INDEX IF NOT EXISTS idx_orders_algo_id ON orders(algo_id);
CREATE INDEX IF NOT EXISTS idx_orders_client_id ON orders(client_id);
