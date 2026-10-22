CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT NOT NULL UNIQUE,
    client_order_id TEXT NOT NULL,
    broker_order_id TEXT,
    instrument TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity TEXT NOT NULL,
    price TEXT NOT NULL,
    fee_breakdown_json TEXT NOT NULL DEFAULT '{}',
    algo_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    executed_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fills_client_order_id ON fills(client_order_id);
