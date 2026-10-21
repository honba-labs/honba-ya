# Master Plan: 本場・家 honba-ya

> **The house that runs it all.**  
> Central actor runtime, institutional event-driven backtester, REST/WebSocket API gateway, persistence & SEBI audit ledger, and unified CLI for the Honba platform. Orchestrates the zero-I/O deterministic core (`honba-core`), pre-trade risk engine (`honba-mon`), Order Management System (`honba-kokoro`), market data pipelines (`honba-me`), broker routing (`honba-te`), AI decision engines (`honba-nou`), PyO3 quantitative research bindings (`honba-tamashii`), and the terminal UI (`honba-kao`). Built with exact decimal precision (`rust_decimal`), zero-allocation message passing, strict research-live parity, and regulatory compliance for Indian equities and derivatives (NSE/BSE/NFO).

---

## 0. Executive Summary & Architectural Audit

### 0.1 Current State vs Target Architecture

`honba-ya` ("House") is the primary orchestration runtime of the entire platform. While foundational crates (`honba-core`, `honba-mon`, `honba-te-core`) have been modernized with canonical types and pre-trade risk controls, `honba-ya` currently exhibits a split architecture: modern actor/backtester crates sit alongside legacy crates referencing deprecated interfaces and raw floats:

| Area | Current State in `honba-ya` | Target State (Master Architecture) | Severity |
| :--- | :--- | :--- | :--- |
| **Domain Types & Precision** | Modern crates (`actors`, `backtest`) use `honba-core` & `rust_decimal::Decimal`. Older crates (`engine`, `sim`, `sweep`, `api`, `cli`) still rely on deprecated `honba-messages`/`entities` with raw `f64` prices/quantities. | **100% unified on `honba-core`**: Strict use of `InstrumentId`, `Price`, `Quantity`, `Money`, `Bar`, `Tick`, `OrderRequest`, `OrderAck`, `FillReport` across all crates. Zero floats for prices, fees, and quantities. | **P0 (Critical)** |
| **Dual Engine Dichotomy** | Two separate execution paradigms: legacy `honba-ya-engine` (polling event queue, deprecated traits) vs modern `honba-ya-actors` (actor runtime, supervision trees, bounded Tokio channels). | **Single Authoritative Actor Runtime**: Consolidate runtime execution into `honba-ya-actors`. Retire `honba-ya-engine` and legacy traits, routing all events through supervised Tokio actors. | **P0 (Critical)** |
| **Backtester vs CLI Disconnect** | `honba-ya-backtest` has an institutional event loop with NSE calendar, Indian costs (STT, GST, stamp duty), and T+1 settlement. However, `honba-ya-cli/src/backtest.rs` still invokes legacy `honba-sim` with `f64`. | **Unified Backtest Execution**: Wire `honba backtest` CLI directly to `honba-ya-backtest::BacktestEngine`. Expose identical command-line flags, configuration TOMLs, and performance reports (Sharpe, Sortino, max drawdown, SEBI fee breakdowns). | **P0 (Critical)** |
| **WebSocket Streaming Gateway** | `crates/honba-ya-ws` is an empty 15-line stub. `honba-ya-api-rest/src/ws.rs` implements real WebSocket streaming, but polls `AppState` quotes every 1000ms with raw `f64` calculations. | **Zero-Polling Actor Event Bus**: Retire `honba-ya-ws`. Enhance `honba-ya-api-rest/src/ws.rs` into an event-driven broadcast gateway subscribing directly to `MarketDataActor`'s broadcast channel, streaming live ticks and depth instantaneously. | **P1 (High)** |
| **Relational Persistence (sqlx)** | `honba-ya-persistence` provides `SqliteAuditLedger` for SEBI audit trails, but lacks the relational sqlx schemas/migrations for traders, orders, fills, decisions, and positions. | **Comprehensive sqlx Repositories**: Implement PostgreSQL and SQLite relational repositories with schema migrations for traders, orders, fills, LLM decisions, and portfolio positions. | **P1 (High)** |
| **Lakehouse Catalog Integration** | `honba serve` was recently patched to read `catalog/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet`, but `MarketDataActor` and backtest data loaders do not natively ingest from this layout. | **Unified Lakehouse Feed Provider**: Connect `honba-data`'s partitioned catalog directly into `MarketDataActor` for replay/serving, eliminating ad-hoc flat fixture files and data conversion steps. | **P1 (High)** |
| **Simulation Convergence** | Useful microstructure models (L2 orderbook matching, latency injection, MIS intraday square-off) live in legacy `honba-ya-sim` with `f64` types. | **Converged Simulation Engine**: Port L2 book walking, fill slippage, network latency, and MIS intraday margin rules into `honba-ya-backtest` using canonical `honba-core` types and exact decimals. | **P1 (High)** |
| **Cargo Dependency Topology** | Broken/stale relative paths (`../../../honba-core`, `../../../honba-kokoro/...`) in `honba-ya-cli`, `honba-ya-persistence`, and `honba-ya-sim`. | **Clean Workspace Topology**: Standardize workspace dependencies to clean `../../` or path declarations; prune all dead references to retired Kokoro crates. | **P1 (High)** |

---

## 1. Architectural Principles & System Integration

```text
                                  ┌─────────────────────────────────────────────────────────┐
                                  │                     User Interfaces                     │
                                  │    honba-kao (Web Terminal)  │  honba CLI  │  REST / WS │
                                  └────────────────────────────┬────────────────────────────┘
                                                               │ HTTP / WS / CLI Args
                                                               ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  本場・家 honba-ya Central Subsystem                                                                                    │
│                                                                                                                        │
│  ┌─────────────────────────────────────────────────┐   ┌────────────────────────────────────────────────────────────┐  │
│  │   honba-ya-api-rest & honba-ya-api              │   │   honba-ya-cli                                             │  │
│  │   • Axum REST Endpoints (Orders, Reports, State)│   │   • Subcommands: serve, backtest, migrate, data, verify    │  │
│  │   • Zero-Polling WebSocket Broadcast Bus (/ws)  │   │   • Configuration Loader & Artifact Codegen                │  │
│  │   • OpenAPI 3.1 & JWT Bearer Authentication     │   │   • Headless Engine Runner                                 │  │
│  └────────────────────────┬────────────────────────┘   └─────────────────────────────┬──────────────────────────────┘  │
│                           │                                                          │                                 │
│                           ▼                                                          ▼                                 │
│  ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │   honba-ya-actors (Actor Runtime & Supervision Tree)                                                             │  │
│  │                                                                                                                  │  │
│  │   ┌────────────────────┐    ┌────────────────────┐    ┌────────────────────┐    ┌─────────────────────────────┐  │  │
│  │   │  MarketDataActor   │───▶│   StrategyActor    │───▶│     RiskActor      │───▶│       ExecutionActor        │  │  │
│  │   │  Ticks, Agg Bars   │    │  Signals, Intent   │    │ honba-mon (Pure)   │    │ honba-te (Broker Adapters)  │  │  │
│  │   └─────────┬──────────┘    └────────────────────┘    └─────────┬──────────┘    └──────────────┬──────────────┘  │  │
│  │             │                                                   │                              │                 │  │
│  │             │ Broadcast Ticks                                   │ Pre-Trade Pass/Refusal       │ Order Ack/Fill  │  │
│  │             ▼                                                   ▼                              ▼                 │  │
│  │   ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────┐  │  │
│  │   │   Supervisor & Metrics: Lifecycle FSM, Prometheus Gauges (OTR, Latency, PnL), OpenTelemetry Tracing Spans │  │  │
│  │   └───────────────────────────────────────────────────────────────────────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────┬────────────────────────────────────────────────────────┘  │
│                                                            │                                                           │
│  ┌─────────────────────────────────────────────────────────┴───────────────┐   ┌────────────────────────────────────┐  │
│  │   honba-ya-backtest (Research-Live Parity Backtester)                   │   │   honba-ya-persistence             │  │
│  │   • Event Loop: Market ➔ Signal ➔ Order ➔ Fill                         │   │   • sqlx Postgres & SQLite Repos   │  │
│  │   • Indian Cost Model: STT, GST, Stamp Duty, SEBI turnover, DP charges  │   │   • SEBI Append-Only Audit Ledger  │  │
│  │   • T+1 Rolling Settlement Ledger & NSE/BSE Trading Calendars           │   │   • Order, Trade & Decision Tables │  │
│  │   • L2 Book Simulation, Latency Injection, MIS Auto-Square-Off          │   │   • Point-in-Time Run Snapshots    │  │
│  └─────────────────────────────────────────────────────────────────────────┘   └────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┬───────────────────────────────────────────────────────────┘
                                                             │ Domain Events & Protocols
                                                             ▼
┌───────────────────────────────┬───────────────────────────────┬───────────────────────────────┬────────────────────────┐
│  honba-core                   │  honba-mon                    │  honba-me                     │  honba-te              │
│  Canonical Domain Types       │  Synchronous Risk Engine      │  Market Data WebSocket Feeds  │  Broker Execution      │
│  Decimal Money & Quantities   │  OTR Breaker, Circuit Bands   │  Normalizer & Lakehouse Feed  │  Kite, Upstox, Angel   │
├───────────────────────────────┼───────────────────────────────┼───────────────────────────────┼────────────────────────┤
│  honba-nou                    │  honba-tamashii               │  honba-data                   │  honba-kokoro          │
│  LLM Decision Engine          │  PyO3 Bindings & Python SDK   │  Parquet Bar Catalog          │  OMS Accounting        │
│  PromptReplayer Backtester    │  Zero-Copy Arrow Lakehouse    │  DuckDB Fundamentals          │  FIFO Lots & Ledger    │
└───────────────────────────────┴───────────────────────────────┴───────────────────────────────┴────────────────────────┘
```

### Key Tenets
1. **Zero Compromise on Monetary Precision**: Every price, limit price, fee, turnover metric, and account cash balance across `honba-ya` utilizes `rust_decimal::Decimal`. Floating-point values (`f32`/`f64`) are strictly prohibited in domain calculation logic, backtesting ledgers, and API payloads.
2. **Deterministic Research-Live Parity**: The exact same `Strategy` trait, risk evaluation rules (`honba-mon`), statutory Indian cost formulas, and execution state machines run in backtesting as in live trading. Backtests do not take shortcuts; they process the same canonical event sequence.
3. **Actor-Based Fault Isolation & Backpressure**: All live processing occurs inside bounded, supervised Tokio actors (`MarketDataActor`, `StrategyActor`, `RiskActor`, `ExecutionActor`). Unbounded memory growth is prevented by backpressure policies (`try_send` with coalescing); crashes are caught and quarantined by the supervisor without taking down the trading node.
4. **Authoritative SEBI Pre-Trade Risk Gate**: All orders generated by strategies, REST calls, or Python scripts must pass synchronously through `honba-mon::RiskEngine` wrapped by `RiskActor`. SEBI `algo_id` validation, Order-to-Trade Ratio (OTR) breakers, scrip price bands, and kill-switches cannot be bypassed under any operational mode.
5. **Zero-Polling Real-Time WebSocket Streaming**: The API WebSocket handler subscribes directly to the actor broadcast bus. Ticks, order state transitions, fills, and depth updates stream to web clients (`honba-kao`) with sub-millisecond dispatch latency rather than periodic timer polling.
6. **Immutable Regulatory Audit Trails**: Every order attempt, cancellation, modification, pre-trade refusal, broker ack, and execution fill is persisted with microsecond-resolution timestamps, correlation IDs, and algorithm tags in an append-only SQLite/PostgreSQL audit ledger.
7. **Direct Lakehouse Partition Ingestion**: Native compatibility with `honba-data`'s Hive-partitioned directory layout (`catalog/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet`), enabling instant bar serving and backtesting without duplicate file conversions.

---

## 2. Crate Topology & Dependency Rules

The target workspace structure consolidates the existing 13 crates into a streamlined, cohesive 8-crate topology, eliminating legacy cruft and broken relative references:

```text
honba-ya/
├── Cargo.toml                     # Workspace root manifest
├── README.md                      # Architecture and operational guide
├── plan.md                        # Master implementation roadmap
├── schema/                        # Domain schemas and OpenAPI definitions
├── configs/                       # System, broker, and risk configuration TOMLs
├── migrations/                    # sqlx database migrations (Postgres/SQLite)
└── crates/
    ├── honba-ya-actors/           # Asynchronous actor runtime, supervisor & tracing spans
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs             # Module exports
    │       ├── messages.rs        # Actor message contracts & lifecycle states
    │       ├── market_data.rs     # MarketDataActor: tick normalization & broadcast
    │       ├── strategy.rs        # StrategyActor & Strategy trait
    │       ├── risk.rs            # RiskActor: honba-mon pre-trade risk evaluation
    │       ├── execution.rs       # ExecutionActor: honba-te broker gateway management
    │       ├── supervisor.rs      # Actor supervision tree, drain & restart logic
    │       ├── metrics.rs         # Prometheus metrics exporter
    │       └── tracing_spans.rs   # Structured tracing spans (OrderGenerated -> Filled)
    ├── honba-ya-backtest/         # Research-live parity event-driven backtesting engine
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs             # Module exports
    │       ├── engine.rs          # BacktestEngine: deterministic event loop
    │       ├── events.rs          # MarketEvent, SignalEvent, OrderEvent, FillEvent
    │       ├── calendar.rs        # NseTradingCalendar: holidays & Muhurat trading
    │       ├── costs.rs           # IndianCostModel: STT, GST, stamp duty, SEBI fees
    │       ├── settlement.rs      # SettlementLedger: T+1 rolling Demat settlement
    │       ├── sim/               # Converged simulation modules (absorbed from honba-ya-sim)
    │       │   ├── l2_book.rs     # L2 order book simulation & depth walking
    │       │   ├── latency.rs     # Seeded network and broker latency models
    │       │   ├── slippage.rs    # Volatility and liquidity slippage models
    │       │   └── mis.rs         # Intraday MIS 15:20 auto-square-off engine
    │       └── prompt_replay.rs   # Deterministic LLM decision replayer (honba-nou)
    ├── honba-ya-persistence/      # sqlx repositories & SEBI regulatory audit ledger
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs             # Connection pools & repository aggregator
    │       ├── audit.rs           # SqliteAuditLedger & PostgresAuditLedger
    │       ├── orders.rs          # OrderStore: idempotent order lifecycle tracking
    │       ├── fills.rs           # FillStore: trade execution ledger
    │       ├── positions.rs       # PositionStore: tax-lot FIFO ledger
    │       ├── decisions.rs       # DecisionStore: LLM prompt/decision hash logging
    │       └── migrations.rs      # Embedded sqlx migration runner
    ├── honba-ya-api/              # Transport-agnostic API contracts, schemas & IR
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs             # Core API traits & versioning
    │       ├── requests.rs        # Strongly-typed request payloads
    │       ├── responses.rs       # Versioned API response envelopes
    │       ├── capabilities.rs    # Capability negotiation manifest
    │       └── verify.rs          # Strategy IR verification & compile checks
    ├── honba-ya-api-rest/         # Axum REST server & real-time WebSocket gateway
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs             # Axum router & server lifecycle
    │       ├── state.rs           # AppState: actor handles, catalog & caches
    │       ├── ws.rs              # Zero-polling WebSocket gateway (/ws, /ws/market)
    │       ├── catalog.rs         # CatalogBars: partitioned parquet reader
    │       ├── auth.rs            # JWT Bearer token authentication & route scopes
    │       ├── market.rs          # Quotes, depth, bars, instruments endpoints
    │       ├── orders.rs          # POST /orders, DELETE /orders, approvals
    │       ├── portfolio.rs       # Portfolio accounts, positions, and trades
    │       ├── reports.rs         # Tax, ledger, WAC, fee, and concentration reports
    │       └── screener.rs        # Real-time market scanner endpoints
    ├── honba-ya-cli/              # Unified CLI executable (honba)
    │   ├── Cargo.toml
    │   └── src/
    │       ├── main.rs            # Clap CLI parser & command dispatcher
    │       ├── serve.rs           # `honba serve`: starts REST/WS server & actor tree
    │       ├── backtest.rs        # `honba backtest`: executes BacktestEngine
    │       ├── migrate.rs         # `honba migrate`: executes sqlx database migrations
    │       ├── data.rs            # `honba data`: catalog inspect & tick conversion
    │       ├── schema.rs          # `honba schema`: emits OpenAPI, TypeScript, MCP
    │       └── trading_state.rs   # `honba trading-state`: inspect/modify halt state
    ├── honba-ya-config/           # Central system configuration loader
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs             # HonbaConfig root struct
    │       ├── broker.rs          # Broker credentials & endpoint settings
    │       └── risk.rs            # Global & per-algo risk threshold parameters
    └── honba-ya-testing/          # Integration test harness & mock fixtures
        ├── Cargo.toml
        └── src/
            ├── lib.rs             # Test fixtures & assert helpers
            ├── feed.rs            # Scripted tick & bar feeds
            └── broker.rs          # Mock broker executor gateway
```

### Layering & Dependency Hierarchy
```text
Layer 4: [honba-ya-cli] (Executable Boundary)
             │
Layer 3: [honba-ya-api-rest] ──▶ [honba-ya-api]
             │
Layer 2: [honba-ya-actors]   ──▶ [honba-ya-backtest] (Simulation & Execution)
             │                          │
Layer 1: [honba-ya-persistence] ──▶ [honba-ya-config]
             │
Layer 0: Depends on external canonical crates: [honba-core], [honba-mon], [honba-te-core], [honba-me-core]
```

### Deprecation & Pruning Directive
1. **Retire `crates/honba-ya-ws`**: The 15-line broadcast stub is obsolete. Delete the crate and standardize all WebSocket streaming in `honba-ya-api-rest/src/ws.rs`.
2. **Retire `crates/honba-ya-engine`**: Superseded by `honba-ya-actors` and `honba-ya-backtest`. Remove the crate and all internal references to `honba-messages`, `honba-entities`, and `honba-ports`.
3. **Consolidate `crates/honba-ya-sim` into `honba-ya-backtest`**: Move the high-fidelity microstructure models (L2 book, latency, slippage, MIS square-off) into `honba-ya-backtest/src/sim/`, upgrading all types to `rust_decimal::Decimal`.
4. **Fold `crates/honba-ya-async` and `honba-ya-sweep`**: Migrate sweep parameter optimization into `honba-ya-backtest::sweep`, deleting standalone legacy crates.
5. **Fix All Workspace Paths**: Replace all `../../../` occurrences with proper workspace inheritance or `../../` workspace relative paths.

---

## 3. Deep-Dive Specification & Core Modules

### 3.1 `honba-ya-actors`: Asynchronous Actor Runtime & Supervision Tree

`honba-ya-actors` provides the live trading runtime. Actors run as dedicated Tokio tasks communicating over bounded `mpsc` channels with strict backpressure and supervision:

```rust
// Core actor message flow and supervision contracts
pub struct Supervisor {
    market_data: ActorHandle<MarketDataMsg>,
    strategy: ActorHandle<StrategyMsg>,
    risk: ActorHandle<RiskMsg>,
    execution: ActorHandle<ExecutionMsg>,
    status: Arc<AtomicActorStatus>,
    metrics: Arc<MetricsCollector>,
}

impl Supervisor {
    /// Launches all actors in dependency order, establishing channels and supervision
    pub async fn start(config: &SupervisorConfig) -> Result<Self, SupervisorError>;
    
    /// Coordinates graceful shutdown: stops market data, drains risk & execution queues, persists audit
    pub async fn shutdown_graceful(&self, timeout: Duration) -> ShutdownReport;
    
    /// Triggers platform emergency kill-switch: cancels active orders, halts strategies
    pub async fn emergency_halt(&self, reason: &str) -> HaltReport;
}
```

- **`MarketDataActor`**: Ingests normalized `honba_core::Tick` and `Bar` instances from `honba-me`. Maintains an in-memory Last Value Cache (LVC) and broadcasts quotes to `honba-ya-api-rest` WebSockets and subscribed `StrategyActor` instances.
- **`StrategyActor`**: Executes the trading logic (either native Rust strategies or Python strategies via `honba-tamashii`). Converts market signals into `OrderRequest` candidate structs.
- **`RiskActor`**: Wraps `honba_mon::RiskEngine` synchronously. Every candidate order is evaluated with microsecond latency measurement. Rejections are immediately logged to the audit ledger and returned to the caller; approvals are passed to `ExecutionActor`.
- **`ExecutionActor`**: Interfaces with `honba_te_core::BrokerExecutor`. Manages order state transitions (`PendingNew` $\to$ `New` $\to$ `PartiallyFilled` $\to$ `Filled`), retries on network blips, and reconciles broker acks with the persistence layer.

### 3.2 `honba-ya-backtest`: Institutional Backtester & Simulation Models

The backtest engine operates on a strict, deterministic event loop ensuring complete research-live parity:
$$\text{MarketEvent} \longrightarrow \text{SignalEvent} \longrightarrow \text{OrderEvent} \longrightarrow \text{FillEvent}$$

#### Accurate Indian Statutory Cost Model
Every fill incurs statutory charges calculated with exact `rust_decimal::Decimal` arithmetic:
- **Securities Transaction Tax (STT)**:
  - Equity Delivery: 0.1% on buy and 0.1% on sell turnover.
  - Equity Intraday: 0.025% on sell turnover only.
  - Futures: 0.02% on sell turnover.
  - Options: 0.125% on sell premium turnover.
- **Stamp Duty**: 0.015% on buy delivery, 0.003% on intraday buy.
- **Exchange Turnover Fees**: NSE (0.00297%) / BSE (0.00375%).
- **SEBI Turnover Charges**: ₹10 per crore turnover (₹0.0001%).
- **Goods and Services Tax (GST)**: 18% on (brokerage + exchange charges + SEBI charges).
- **Depository Participant (DP) Charges**: Flat ₹13.50 + 18% GST per scrip per day on debit (equity delivery sells).

#### Settlement & Calendars
- **T+1 Rolling Settlement**: Equity delivery cash and shares settle on $T+1$. Unsettled delivery proceeds cannot be used for subsequent trades until settlement confirmation.
- **NSE/BSE Calendar**: Models weekend market closures, national trading holidays, and special Muhurat trading sessions.
- **Intraday MIS Auto-Square-Off**: Orders flagged as `ProductType::Intraday` (MIS) are automatically liquidated at 15:20 IST if not closed by the strategy.

#### Microstructure Simulation (Absorbed from `sim`)
- **L2 Order Book Walking**: Matches large orders against historical depth tiers, calculating realistic non-linear market impact.
- **Latency & Slippage Injection**: Configurable execution delays (e.g. 15ms broker queue delay + 5ms exchange gateway latency).

### 3.3 `honba-ya-persistence`: sqlx Schema & SEBI Regulatory Audit Ledger

The persistence layer supports both PostgreSQL (for live institutional operations) and SQLite (for embedded edge nodes, backtests, and testing):

#### Database Relational Schema
1. **`traders`**: Trader ID, SEBI `algo_id`, risk profile, allocated capital, status.
2. **`orders`**: Canonical order record (`client_order_id`, `broker_order_id`, `algo_id`, instrument, side, order type, price, qty, status, IST/UTC timestamps).
3. **`fills`**: Trade ID, order reference, fill price, fill quantity, fee breakdown JSON, execution timestamp.
4. **`decisions`**: LLM and strategy decisions (`decision_id`, `algo_id`, prompt sha256, model metadata, decision kind, confidence, input context).
5. **`positions`**: FIFO tax-lots, open quantity, weighted average cost (WAC), realized PnL, unrealized PnL.
6. **`audit_ledger`**: Append-only immutable regulatory audit trail (microsecond timestamps, `latency_us`, refusal reasons).

```sql
-- Core SEBI regulatory audit ledger table
CREATE TABLE IF NOT EXISTS audit_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
```

### 3.4 `honba-ya-api-rest` & Unified WebSocket Gateway

The API layer is built on Axum 0.7, exposing over 30 REST endpoints and an asynchronous WebSocket streaming gateway:

- **Unified WebSocket Stream (`/ws`, `/ws/market`)**:
  - Connects web clients (`honba-kao`) and external tools directly to the `MarketDataActor` and `ExecutionActor` event streams.
  - Supports client subscription commands: `{"action": "subscribe", "symbols": ["RELIANCE", "TCS"]}`.
  - Zero-polling: pushes ticks, quote updates, order status changes, and fills as soon as they occur.
- **Catalog Integration (`CatalogBars`)**:
  - Reads `honba-data`'s partitioned Parquet catalog directly (`catalog/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet`).
  - Merges slices chronologically, deduplicating boundary rows, and serving multi-timeframe requests (1m, 5m, 15m, 1d) via `GET /bars/:id?tf=1d`.
- **OpenAPI 3.1 & JWT Bearer Authentication**:
  - Route-level role-based access control (RBAC): Read-only, Operator, Admin.
  - Constant-time token verification with audit logging of all administrative actions.

### 3.5 `honba-ya-cli`: Unified Operator & Developer CLI

The single authoritative binary `honba` provides institutional workflow commands:

```bash
# Start the full trading daemon (REST API, WebSocket, Actors)
honba serve --data-dir ../honba-data/data --addr 127.0.0.1:8000 --trading-state active

# Run a deterministic backtest with Indian statutory costs
honba backtest --config configs/momentum_nifty.toml --from 2025-01-01 --to 2025-12-31 --output results/

# Run database migrations
honba migrate --database-url sqlite://honba.db

# Inspect and validate market data catalog
honba data catalog-summary --data-dir ../honba-data/data

# Inspect or trigger emergency risk halts
honba trading-state halt --reason "High OTR breach observed"
```

---

## 4. Phase-by-Phase Implementation Roadmap

### Phase 1: Canonical Type Unification & Legacy Pruning (M0) — Week 1–2
- [ ] **1.1 Workspace Dependency Cleanup**
  - [ ] Standardize all Cargo dependencies to canonical `honba-core`, `honba-mon`, `honba-te-core`, `honba-me-core`.
  - [ ] Fix broken relative paths (`../../../`) in `honba-ya-cli`, `honba-ya-persistence`, and `honba-ya-sim`.
  - [ ] Excise all usages of `honba-messages`, `honba-entities`, and `honba-ports`.
  - [ ] Purge excised `honba-kokoro-risk` from `honba-ya/Cargo.toml` and migrate `honba-ya-config`, `honba-ya-actors::RiskActor`, and `honba-ya-api-rest` to `honba-mon` per [Risk Migration Plan](../honba-docs/docs/architecture/migration_plan_kokoro_risk_to_mon.md).
- [ ] **1.2 Decimal Arithmetic Enforcement**
  - [ ] Replace remaining `f64` prices, quantities, and cash values in `honba-ya-api` and `honba-ya-cli` with `rust_decimal::Decimal`.
  - [ ] Ensure all API serialization formats support lossless decimal strings.
- [ ] **1.3 Legacy Crates Pruning**
  - [ ] Mark `crates/honba-ya-ws` and `crates/honba-ya-engine` for removal; redirect all calls to `actors`.

### Phase 2: Production Persistence & sqlx Relational Schema (M1) — Week 3–4
- [ ] **2.1 sqlx Relational Schema & Migrations**
  - [ ] Create sqlx migration files for `traders`, `orders`, `fills`, `decisions`, and `positions`.
  - [ ] Add dual SQLite and PostgreSQL support in `honba-ya-persistence`.
- [ ] **2.2 Repositories Implementation**
  - [ ] Implement `OrderRepository` with idempotent upsert and state transition validations.
  - [ ] Implement `FillRepository` with fee breakdown persistence.
  - [ ] Implement `PositionRepository` supporting FIFO tax-lot tracking.
  - [ ] Implement `DecisionRepository` with prompt sha256 checksum tracking.
- [ ] **2.3 Persistence Integration Testing**
  - [ ] Verify database roundtrips, migration idempotency, and concurrent transaction safety under load.

### Phase 3: Actor Runtime & WebSocket Streaming Gateway (M2) — Week 5–6
- [ ] **3.1 Actor Supervision & Lifecycle Hardening**
  - [ ] Implement actor restart strategies (OneForOne, AllForOne) in `Supervisor`.
  - [ ] Add backpressure drop/coalescing policies in `MarketDataActor` to prevent memory blowups.
  - [ ] Ensure clean shutdown drain loops for in-flight orders during graceful stop.
- [ ] **3.2 Zero-Polling WebSocket Gateway**
  - [ ] Rebuild `honba-ya-api-rest/src/ws.rs` to subscribe directly to `MarketDataActor`'s broadcast channel.
  - [ ] Remove the 1000ms polling loop; stream ticks, depth, and order state events immediately.
  - [ ] Validate bidirectional client messaging (subscribe, unsubscribe, ping/pong) with `honba-kao`.

### Phase 4: Institutional Strategy Tester (`honba-ya-backtest`) (M3) — Week 7–8
- [ ] **4.1 Microstructure Simulation & Order Book Matching**
  - [ ] Port L2 order book depth matching and latency injection from `honba-ya-sim` into `honba-ya-backtest/src/sim/`.
  - [ ] Upgrade simulation models to strict `rust_decimal::Decimal` and `honba_core::Tick`.
  - [ ] Implement volume-participation caps, spread crossing penalties, and adverse fill slippage models.
  - [ ] Integrate intraday MIS 15:20 IST auto-square-off logic into `BacktestEngine`.
- [ ] **4.2 Complete Indian Statutory Cost & Settlement Modeling**
  - [ ] Wire `honba-kokoro-portfolio` (FIFO tax-lots) and `honba-kokoro-ledger` (double-entry cash/charges).
  - [ ] Implement exact Indian cost calculation: STT (0.1% delivery, 0.025% MIS sell), Stamp Duty, NSE turnover, SEBI fees, and 18% GST.
  - [ ] Model ICCL T+1 Demat rolling settlement cycles and margin holdbacks.
- [ ] **4.3 Lakehouse Catalog Ingestion**
  - [ ] Support loading historical bars directly from `honba-data/data/catalog` into backtests with zero-copy Arrow readers.

### Phase 5: Automatic Strategy Enhancer (like Jesse) (M4) — Week 9–10
- [ ] **5.1 Multi-Objective Genetic Algorithm (GA) Optimizer**
  - [ ] Implement declarative parameter chromosome encoder reading strategy parameter search spaces.
  - [ ] Implement tournament selection, uniform/arithmetic crossover, and Gaussian mutation operators.
  - [ ] Calculate multi-objective fitness balancing Sharpe, Sortino, Profit Factor, and Max Drawdown.
  - [ ] Compute and export the Pareto-optimal parameter frontier.
- [ ] **5.2 Bayesian Hyperparameter Optimization (TPE)**
  - [ ] Integrate Tree-structured Parzen Estimators for probabilistic search over high-dimensional strategy spaces.
  - [ ] Support parallel candidate evaluation across multi-threaded Rayon worker pools.
- [ ] **5.3 Walk-Forward Analysis (WFO)**
  - [ ] Implement anchored and rolling out-of-sample cross-validation partitions.
  - [ ] Compute Walk-Forward Efficiency (WFE) requiring WFE >= 0.60 for certification.
- [ ] **5.4 Overfitting Guardrails (DSR & PBO)**
  - [ ] Implement Marcos López de Prado’s Deflated Sharpe Ratio (DSR) adjusting for number of trials, return skewness, and kurtosis.
  - [ ] Implement Probability of Backtest Overfitting (PBO) via Combinatorially Symmetric Cross-Validation (CSCV).
- [ ] **5.5 Monte Carlo Permutation & Stress Testing**
  - [ ] Implement trade sequence shuffling (without replacement) to determine worst-case drawdown confidence intervals (95%, 99% VaR/CVaR).
  - [ ] Inject synthetic spread/slippage shocks (1.5x, 2.0x, 3.0x) and gap-down scenarios.
- [ ] **5.6 Closed-Loop AI Refinement Integration**
  - [ ] Interface with `honba-nou` AI Strategy Enhancer loop to ingest diagnostic reports and apply prompt-driven parameter/rule mutations.
- [ ] **5.7 StockSharp Odysseus Genetic Generation Backend Runner**
  - [ ] Implement Rayon-accelerated parallel evaluator for Odysseus candidate rule trees against historical tick/bar streams.
  - [ ] High-throughput simulation pipeline evaluating up to 1,000 rule tree genomes per second across pinned OS threads.
  - [ ] Support L2 order book depth simulation (queue position, market depth depletion, latency jitter) for StockSharp market-making and quoting strategies.

### Phase 6: REST API Hardening, JWT Auth & OpenAPI (M5) — Week 11–12
- [ ] **6.1 REST API Type Modernization**
  - [ ] Refactor `orders.rs`, `portfolio.rs`, and `reports.rs` in `honba-ya-api-rest` to consume `honba-core` types.
  - [ ] Route `POST /orders` and `DELETE /orders/:id` directly through `RiskActor` / `honba-mon`.
- [ ] **6.2 Authentication & Security**
  - [ ] Implement JWT Bearer token authentication with HMAC-SHA256 / RSA verification.
  - [ ] Add role-based access control (RBAC) scopes: `read`, `trade`, `admin`.
- [ ] **6.3 OpenAPI 3.1 & Schema Verification**
  - [ ] Update `honba schema` codegen to emit comprehensive OpenAPI 3.1 schemas for all endpoints.

### Phase 7: Unified CLI & Parameter Sweeps (M6) — Week 13–14
- [ ] **7.1 CLI Subcommands**
  - [ ] Re-wire `honba backtest` CLI directly to `honba_ya_backtest::BacktestEngine`.
  - [ ] Implement `honba optimize` subcommand supporting `--method ga`, `--method bayesian`, and `--wfo`.
  - [ ] Implement `honba migrate` subcommand for PostgreSQL/SQLite relational schema migrations.
  - [ ] Add `honba data catalog-inspect` and `catalog-verify` subcommands.
- [ ] **7.2 End-to-End System Integration**
  - [ ] Validate end-to-end pipeline: `honba-me` feed -> `honba-ya` actors -> `honba-mon` risk gate -> `honba-te` mock execution -> `honba-kao` chart display.
---

## 5. Verification, Conformance & Benchmarks

To guarantee production readiness and regulatory compliance, `honba-ya` must satisfy rigorous validation criteria:

| Category | Benchmark / Target Invariant | Verification Method |
| :--- | :--- | :--- |
| **Monetary Precision** | Exactly 0 floating point operations in PnL, cash ledgers, fees, and order quantities. | AST lint checks and fuzz testing against decimal boundary conditions. |
| **SEBI Audit Trail** | 100% of order attempts, refusals, modifications, and fills logged to SQLite/Postgres with microsecond timestamps. | `audit_tests.rs` verification under high-frequency order dispatch. |
| **Pre-Trade Risk Latency** | Synchronous pre-trade risk evaluation $< 15\,\mu\text{s}$ per order. | Criterion micro-benchmarks on `RiskActor` evaluation loops. |
| **Actor Throughput** | $> 100,000$ messages/second processed across actor supervision tree under load. | Concurrent pipeline stress tests with synthetic tick bursts. |
| **WebSocket Latency** | $< 2\,\text{ms}$ tick-to-WebSocket client delivery under normal network conditions. | Local end-to-end WebSocket roundtrip probe tests. |
| **Backtest Fidelity** | 100% deterministic results across identical seed and data feeds; exact Indian statutory fee matching. | Regression test comparing backtest fee totals with manual exchange tax contract notes. |
