CREATE TABLE IF NOT EXISTS positions (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    exchange VARCHAR(16) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    instrument VARCHAR(96) NOT NULL,
    open_quantity DECIMAL(30,16) NOT NULL DEFAULT '0',
    weighted_average_cost DECIMAL(30,16) NOT NULL DEFAULT '0',
    realized_pnl DECIMAL(30,16) NOT NULL DEFAULT '0',
    last_updated TIMESTAMPTZ NOT NULL,
    UNIQUE (client_id, exchange, symbol)
);

CREATE TABLE IF NOT EXISTS position_lots (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    exchange VARCHAR(16) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    side VARCHAR(16) NOT NULL,
    open_quantity DECIMAL(30,16) NOT NULL,
    entry_price DECIMAL(30,16) NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_position_lots_fifo ON position_lots(client_id, exchange, symbol, id);
