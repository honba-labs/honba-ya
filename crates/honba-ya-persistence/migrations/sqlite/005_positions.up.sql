CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    instrument TEXT NOT NULL,
    open_quantity TEXT NOT NULL DEFAULT '0',
    weighted_average_cost TEXT NOT NULL DEFAULT '0',
    realized_pnl TEXT NOT NULL DEFAULT '0',
    last_updated TEXT NOT NULL,
    UNIQUE (client_id, exchange, symbol)
);

CREATE TABLE IF NOT EXISTS position_lots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    open_quantity TEXT NOT NULL,
    entry_price TEXT NOT NULL,
    opened_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_position_lots_fifo ON position_lots(client_id, exchange, symbol, id);
