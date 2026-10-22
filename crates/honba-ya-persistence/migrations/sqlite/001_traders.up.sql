CREATE TABLE IF NOT EXISTS traders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT NOT NULL UNIQUE,
    algo_id TEXT NOT NULL UNIQUE,
    risk_profile TEXT NOT NULL DEFAULT 'retail',
    capital TEXT NOT NULL DEFAULT '0',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
