# 本場・家 honba-ya

> *The house that runs it all.*

Pure-Rust library workspace for the Axum REST/WebSocket API, actor runtime, sqlx persistence, strategy compilation, enhancer, and parity backtester. Zero Rust binaries live in `honba-ya` — all operator and developer CLI subcommands (`honba serve`, `honba migrate`, `honba backtest`, `honba wtf`, etc.) are exposed through the single Python `honba` CLI in `honba-tamashii`.

## Crates
- `honba-ya-api` — versioned wire envelopes & route registry
- `honba-ya-api-rest` — Axum REST/WS router, run service, and server bootstrap (`ServeConfig`)
- `honba-ya-actors` — live actor supervisor & OMS execution bus
- `honba-ya-persistence` — sqlx repositories, SEBI audit ledger, and embedded migrations
- `honba-ya-backtest` — institutional event-driven backtester & catalog loader
- `honba-ya-strategy` — declarative strategy manifest & IR compiler
- `honba-ya-enhancer` — GA, Bayesian, and Walk-Forward optimization engine
- `honba-ya-config` — canonical run/execution/account configuration types
- `honba-ya-testing` — deterministic mock gateways and fixtures

## Run (via unified `honba` CLI in `honba-tamashii`)
    honba serve --data-dir ../honba-data/data --addr 127.0.0.1:8000
    honba migrate --database-url sqlite://honba.db
    honba backtest --data-dir ../honba-data/data --strategy buy_and_hold --universe TCS.NSE --start 2025-01-01 --end 2025-12-31 --seed 42

License: AGPL-3.0.
