# 本場・家 honba-ya

> *The house that runs it all.*

API, WebSocket dashboard, persistence, backtester, and the `honba` CLI. Depends on all four sibling repositories.

## Crates
- `honba-ya-api` — Axum REST
- `honba-ya-ws` — event bus + WS handler
- `honba-ya-persistence` — sqlx
- `honba-ya-backtest` — event-driven backtester
- `honba-ya-cli` — honba serve | backtest | migrate

## Run
    cargo run -p honba-ya-cli -- serve
    cargo run -p honba-ya-cli -- backtest --strategy momentum --from 2025-01-01 --to 2025-12-31

License: AGPL-3.0.
