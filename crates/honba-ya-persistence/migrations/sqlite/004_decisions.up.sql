CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id TEXT NOT NULL UNIQUE,
    algo_id TEXT NOT NULL,
    prompt_sha256 TEXT NOT NULL CHECK (length(prompt_sha256) = 64),
    model_name TEXT NOT NULL,
    seed TEXT NOT NULL,
    temperature TEXT NOT NULL,
    decision_kind TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_algo_id ON decisions(algo_id);
CREATE INDEX IF NOT EXISTS idx_decisions_prompt_sha256 ON decisions(prompt_sha256);
