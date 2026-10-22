CREATE TABLE IF NOT EXISTS audit_ledger (
    id BIGSERIAL PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    timestamp_utc TEXT NOT NULL,
    event_type TEXT NOT NULL,
    algo_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    broker_order_id TEXT,
    exchange_order_id TEXT,
    instrument TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    quantity TEXT NOT NULL,
    price TEXT,
    status TEXT NOT NULL,
    refusal_reason TEXT,
    latency_us BIGINT,
    metadata_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_algo_id ON audit_ledger(algo_id);
CREATE INDEX IF NOT EXISTS idx_audit_order_id ON audit_ledger(client_order_id);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_ledger(timestamp_ms);
