CREATE TABLE IF NOT EXISTS decisions (
    id BIGSERIAL PRIMARY KEY,
    decision_id VARCHAR(64) NOT NULL UNIQUE,
    algo_id VARCHAR(64) NOT NULL,
    prompt_sha256 CHAR(64) NOT NULL,
    model_name VARCHAR(128) NOT NULL,
    seed VARCHAR(32) NOT NULL,
    temperature DECIMAL(30,16) NOT NULL,
    decision_kind VARCHAR(64) NOT NULL,
    spec_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_algo_id ON decisions(algo_id);
CREATE INDEX IF NOT EXISTS idx_decisions_prompt_sha256 ON decisions(prompt_sha256);
