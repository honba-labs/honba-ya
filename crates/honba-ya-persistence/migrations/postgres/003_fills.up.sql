CREATE TABLE IF NOT EXISTS fills (
    id BIGSERIAL PRIMARY KEY,
    trade_id VARCHAR(64) NOT NULL UNIQUE,
    client_order_id VARCHAR(64) NOT NULL,
    broker_order_id VARCHAR(64),
    instrument VARCHAR(96) NOT NULL,
    side VARCHAR(16) NOT NULL,
    quantity DECIMAL(30,16) NOT NULL,
    price DECIMAL(30,16) NOT NULL,
    fee_breakdown_json TEXT NOT NULL DEFAULT '{}',
    algo_id VARCHAR(64) NOT NULL,
    client_id VARCHAR(255) NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fills_client_order_id ON fills(client_order_id);
