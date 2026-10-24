# `honba-ya` Agent Guidelines & Architecture Invariants

**Subsystem**: House (Central Actor Runtime & Tester)
**Language**: Rust (Tokio)
**Primary Responsibility**: Tokio actor runtime, event-driven backtesting, strategy optimizer (GA/Bayesian), and Axum REST API server.

---

## 1. Role & Architectural Hints
* `honba-ya` is the orchestrator and application runtime:
  1. **Actor System**: Concurrency via message passing across `tokio::sync::mpsc` channels (`MarketDataActor`, `StrategyActor`, `RiskActor`, `ExecutionActor`).
  2. **Event-Driven Backtester**: Processes `MarketEvent` -> `SignalEvent` -> `OrderEvent` -> `FillEvent` with exact parity to live trading.
  3. **Multi-Tier Enhancer**: Vectorized pre-filter, event-driven simulation, and concurrent parameter sweeps (Genetic Algorithms, Bayesian Optimization, Walk-Forward Optimization, Deflated Sharpe Ratio).
  4. **Headless Axum REST API (`honba-ya-api-rest`)**: Production OpenAPI 3.1 endpoints and `/ws/live` streaming WebSocket.
* **Idea Inheritance**:
  - **NautilusTrader**: Deterministic event-driven backtesting with research-live parity.
  - **Barter-rs**: Concurrent backtesting and latency simulation.
  - **Jesse**: Statistical optimization criteria and route execution.

---

## 2. Coding Guidelines (DDD & TDD)
* **Zero-I/O in Domain Logic**: Simulation clock is stepped deterministically.
* **Zero Floats Policy**: All accounting and prices use `Decimal`.
* **TDD & Parallel Test Directory Rule (MANDATORY)**:
  - All tests must live in `tests/` parallel to `src/`.
  - **STRICTLY FORBIDDEN**: In-file `#[test]` inside `src/`.
* **Check .agents/HANDOFF.md**: Always verify handoff status before starting work.

---

## 3. Development & CI Workflow
* **Linting & Formatting**:
  ```bash
  cargo fmt --check
  cargo clippy --all-targets -- -D warnings
  ```
* **Testing**:
  ```bash
  cargo test --all
  ```
