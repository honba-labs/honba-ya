# Master Plan: 本場・家 honba-ya

> **The house that runs it all.**  
> Central actor runtime, institutional event-driven backtester, strategy runtime & execution state machine (`honba-ya-strategy`), automatic strategy enhancer (`honba-ya-enhancer`), REST/WebSocket API gateway, persistence & SEBI audit ledger, and unified CLI for the Honba platform. Orchestrates the zero-I/O deterministic core (`honba-core`), pre-trade risk engine (`honba-mon`), Order Management System (`honba-kokoro`), market data pipelines (`honba-me`), broker routing (`honba-te`), AI decision engines (`honba-nou`), PyO3 quantitative research bindings (`honba-tamashii`), and the terminal UI (`honba-kao`). Built with exact decimal precision (`rust_decimal`), zero-allocation message passing, strict research-live parity, and regulatory compliance for Indian equities and derivatives (NSE/BSE/NFO).

---

## 0. Executive Summary & Architectural Audit

### 0.1 Current State vs Target Architecture

**Status (2026-10-23)**: Phase 1, Phase 2, Phase 3, and Phase 4 closed out, alongside the 4-step architectural realignment:
1. **Optimizer Crate Modernized & Renamed (`honba-ya-enhancer`)**: `crates/honba-strategies` was renamed to `honba-ya-enhancer` (ASE: GA, Bayesian TPE, WFO, Odysseus rule trees, Monte Carlo stress testing), modernized for Rust 2024 (fixed `r#gen` keyword conflicts, Rayon thread-safety with `ThreadRng` `!Sync`, and float `Ord` map issues), and compiles cleanly with 0 errors (`cargo check -p honba-ya-enhancer`).
2. **Strategy Runtime Relocation (`honba-ya-strategy`)**: Sibling crate `honba-nou-strategy` was relocated from `honba-nou` into `honba-ya/crates/honba-ya-strategy` (`honba_strategy` lib), establishing the canonical zero-I/O `Strategy` trait, pull-based `StrategyRunner` state machine, and manifest contracts inside `honba-ya`.
3. **Catalog Domain Type Unification (`honba-strategies/`)**: The top-level Python quantitative strategy catalog was unified with `honba-tamashii` by replacing all deprecated `honba.entities` imports with canonical `honba.domain` types (`InstrumentId`, `Trade`, `Bar`, `Tick`, `Side`, `OrderType`, etc.). All 436 pytest tests pass in 1.40s.
4. **Manifest Alignment (`step.rs`)**: Implemented `step.rs` in `honba-ya-strategy` to deserialize `step.toml` (`step.schema.json`) manifests directly into `StrategyManifest`, verified with comprehensive integration tests across all 17 catalog strategy directories.
5. **Institutional Backtester (`honba-ya-backtest`)**: Completed Phase 4 with `StrategyRunner` integration, L2 order-book depth matching + VWAP calculation, latency injection, volume-participation caps, spread-crossing penalties, `AdverseFillModel` slippage, MIS 15:20 IST auto-square-off, `honba-kokoro-portfolio` FIFO tax-lots + ICCL T+1 Demat rolling settlement cycles + margin holdbacks, `honba-kokoro-ledger` double-entry journal postings + exact Indian statutory charges (STT, Stamp Duty, Exchange turnover, SEBI fees, 18% GST, DP charges), and zero-copy Arrow/Parquet `CatalogLoader` over `honba-data/data/catalog`. All 27 unit and integration tests pass with 0 clippy warnings.

The workspace now consists of a cohesive **10-crate topology** with 100% clean compilation (`cargo check --workspace`). All legacy `engine`, `sim`, `sweep`, `ws`, and `async` crates are excised.

The table below preserves the historical record of what Phases 1–4 and the restructure resolved:

| Area | Prior State in `honba-ya` | Target State (Master Architecture) | Severity / Resolution |
| :--- | :--- | :--- | :--- |
| **Domain Types & Precision** | Modern crates (`actors`, `backtest`) used `honba-core` & `rust_decimal::Decimal`. Older crates (`engine`, `sim`, `sweep`, `api`, `cli`) relied on deprecated `honba-messages`/`entities` with raw `f64` prices/quantities. | **100% unified on `honba-core`**: Strict use of `InstrumentId`, `Price`, `Quantity`, `Money`, `Bar`, `Tick`, `OrderRequest`, `OrderAck`, `FillReport` across all crates. Zero floats for prices, fees, and quantities. | **P0 (Critical)** — **done**, see Phase 1 |
| **Dual Engine Dichotomy** | Two separate execution paradigms: legacy `honba-ya-engine` (polling event queue, deprecated traits) vs modern `honba-ya-actors` (actor runtime, supervision trees, bounded Tokio channels). | **Single Authoritative Actor Runtime**: Consolidate runtime execution into `honba-ya-actors`. Retire `honba-ya-engine` and legacy traits, routing all events through supervised Tokio actors. | **P0 (Critical)** — **done**, `honba-ya-engine` deleted |
| **Strategy Runtime Separation** | `honba-nou-strategy` lived inside AI decision crate (`honba-nou`), while `honba-ya/crates/honba-strategies` housed optimization code under a misleading name conflicting with Python catalog `honba-strategies/`. | **Clear Architectural Boundaries**: Relocate execution runtime (`Strategy` trait, runner state machine, manifest parser) to `crates/honba-ya-strategy`. Rename optimizer to `crates/honba-ya-enhancer`. Python catalog remains `honba-strategies/`. | **P0 (Critical)** — **done**, 4-step restructure complete |
| **Backtester vs CLI Disconnect** | `honba-ya-backtest` has an institutional event loop with NSE calendar, Indian costs (STT, GST, stamp duty), and T+1 settlement. However, `honba-ya-cli/src/backtest.rs` still invoked legacy `honba-sim` with `f64`. | **Unified Backtest Execution**: Wire `honba backtest` CLI directly to `honba-ya-backtest::BacktestEngine`. Expose identical command-line flags, configuration TOMLs, and performance reports (Sharpe, Sortino, max drawdown, SEBI fee breakdowns). | **P0 (Critical)** — `honba-ya-backtest` complete in Phase 4; CLI wiring in Phase 7 |
| **WebSocket Streaming Gateway** | `crates/honba-ya-ws` was an empty 15-line stub. `honba-ya-api-rest/src/ws.rs` implements real WebSocket streaming, but polls `AppState` quotes every 1000ms with raw `f64` calculations. | **Zero-Polling Actor Event Bus**: Retire `honba-ya-ws`. Enhance `honba-ya-api-rest/src/ws.rs` into an event-driven broadcast gateway subscribing directly to `MarketDataActor`'s broadcast channel, streaming live ticks and depth instantaneously. | **P1 (High)** — **done**, see Phase 3 |
| **Relational Persistence (sqlx)** | `honba-ya-persistence` provides `SqliteAuditLedger` for SEBI audit trails, but lacks the relational sqlx schemas/migrations for traders, orders, fills, decisions, and positions. | **Comprehensive sqlx Repositories**: Implement PostgreSQL and SQLite relational repositories with schema migrations for traders, orders, fills, LLM decisions, and portfolio positions. | **P1 (High)** — **done**, see Phase 2 |
| **Lakehouse Catalog Integration** | `honba serve` was patched to read `catalog/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet`, but `MarketDataActor` and backtest data loaders do not natively ingest from this layout. | **Unified Lakehouse Feed Provider**: Connect `honba-data`'s partitioned catalog directly into `honba-ya-backtest` (`CatalogLoader` implementing `BarReader`) and `MarketDataActor` for replay/serving. | **P1 (High)** — **done**, see Phase 4 |
| **Simulation Convergence** | Useful microstructure models (L2 orderbook matching, latency injection, MIS intraday square-off) lived in legacy `honba-ya-sim` with `f64` types. | **Converged Simulation Engine**: Port L2 book walking, fill slippage, network latency, and MIS intraday margin rules into `honba-ya-backtest` using canonical `honba-core` types and exact decimals. | **P1 (High)** — **done**, see Phase 4 |
| **Cargo Dependency Topology** | Stale relative paths and dead references to retired Kokoro crates (`honba-kokoro-{messages,entities,types,ports,risk}`). | **Clean Workspace Topology**: 10 clean member crates in workspace, all dead Kokoro and legacy engine crate references excised. | **P1 (High)** — **done** across all 10 crates |

---

## 1. Architectural Principles & System Integration

```text
                                  ┌─────────────────────────────────────────────────────────┐
                                  │                     User Interfaces                     │
                                  │    honba-kao (Web Terminal)  │  honba CLI  │  REST / WS │
                                  └─────────────────────────────┬───────────────────────────┘
                                                                │ HTTP / WS / CLI Args
                                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  本場・家 honba-ya Central Subsystem                                                                   │
│                                                                                                        │
│  ┌───────────────────────────────────────────────┐   ┌──────────────────────────────────────────────┐  │
│  │   honba-ya-api-rest & honba-ya-api            │   │   honba-ya-cli                               │  │
│  │   • Axum REST Endpoints (Orders, Reports, State)  │   • Subcommands: serve, backtest, optimize   │  │
│  │   • Zero-Polling WebSocket Broadcast Bus (/ws)│   │   • Database Migrations & Catalog Inspection │  │
│  │   • OpenAPI 3.1 & JWT Bearer Authentication   │   │   • Headless Engine & Backtest Runner        │  │
│  └────────────────────────┬──────────────────────┘   └──────────────────────────────┬───────────────┘  │
│                          │                                                          │                  │
│                          ▼                                                          ▼                  │
│  ┌──────────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │   honba-ya-actors (Actor Runtime & Supervision Tree)                                             │  │
│  │                                                                                                  │  │
│  │   ┌────────────────────┐    ┌────────────────────┐    ┌────────────────────┐    ┌─────────────┐  │  │
│  │   │  MarketDataActor   │───▶│   StrategyActor    │───▶│     RiskActor      │───▶│ExecutionAct │  │  │
│  │   │  Ticks, Agg Bars   │    │  Signals, Intent   │    │ honba-mon (Pure)   │    │ honba-te    │  │  │
│  │   └─────────┬──────────┘    └────────────────────┘    └─────────┬──────────┘    └──────┬──────┘  │  │
│  │             │                                                   │                  │         │  │
│  │             │ Broadcast Ticks                                   │ Pre-Trade Pass   │ Acks    │  │
│  │             ▼                                                   ▼                  ▼         │  │
│  │   ┌──────────────────────────────────────────────────────────────────────────────────────────┐   │  │
│  │   │   Supervisor & Metrics: Lifecycle FSM, Prometheus Gauges (OTR, Latency, PnL), Tracing   │   │  │
│  │   └──────────────────────────────────────────────────────────────────────────────────────────┘   │  │
│  └───────────────────────────────────────────────────┬──────────────────────────────────────────────┘  │
│                                                      │                                                 │
│  ┌───────────────────────────────────────────────────┴──────────┐   ┌───────────────────────────────┐  │
│  │   honba-ya-backtest (Research-Live Parity Backtester)        │   │   honba-ya-strategy           │  │
│  │   • Event Loop: Market ➔ Signal ➔ Order ➔ Fill               │   │   • Zero-I/O Strategy Trait   │  │
│  │   • Indian Cost Model: STT, GST, Stamp Duty, SEBI turnover   │   │   • StrategyRunner FSM        │  │
│  │   • T+1 Rolling Settlement Ledger & NSE/BSE Trading Calendar │   │   • step.toml Manifest Parser │  │
│  │   • L2 Book Simulation, Latency Injection, MIS Auto-Square-Off│   │   • OrderIntent & PnL Ledger  │  │
│  └────────────────────────┬─────────────────────────────────────┘   └───────────────┬───────────────┘  │
│                          │                                                          │                  │
│  ┌───────────────────────┴───────────────────────┐   ┌──────────────────────────────┴───────────────┐  │
│  │   honba-ya-persistence                        │   │   honba-ya-enhancer (ASE Optimizer)           │  │
│  │   • sqlx Postgres & SQLite Repositories       │   │   • Multi-Objective Genetic Algorithm (GA)   │  │
│  │   • SEBI Append-Only Audit Ledger             │   │   • Bayesian Hyperparameter TPE              │  │
│  │   • Order, Fill, Position & Decision Tables   │   │   • Walk-Forward Analysis (WFO, WFE >= 0.60) │  │
│  │   • Point-in-Time Run Snapshots               │   │   • Odysseus Rule Tree Evolution             │  │
│  └───────────────────────────────────────────────┘   └──────────────────────────────────────────────┘  │
└──────────────────────────────────────────┬─────────────────────────────────────────────────────────────┘
                                           │ Canonical Types & Protocols
                                           ▼
┌──────────────────────────────┬──────────────────────────────┬──────────────────────────────┬──────────────────────────────┐
│  honba-core                  │  honba-mon                   │  honba-me                    │  honba-te                    │
│  Canonical Domain Types      │  Synchronous Risk Engine     │  Market Data WebSocket Feeds │  Broker Execution            │
│  Decimal Money & Quantities  │  OTR Breaker, Circuit Bands  │  Normalizer & Lakehouse Feed │  Kite, Upstox, Angel         │
├──────────────────────────────┼──────────────────────────────┼──────────────────────────────┼──────────────────────────────┤
│  honba-nou                   │  honba-tamashii              │  honba-data                  │  honba-kokoro                │
│  LLM Decision Engine         │  PyO3 Bindings & Python SDK  │  Parquet Bar Catalog         │  OMS Accounting              │
│  PromptReplayer Backtester   │  Zero-Copy Arrow Lakehouse   │  DuckDB Fundamentals         │  FIFO Lots & Ledger          │
├──────────────────────────────┴──────────────────────────────┴──────────────────────────────┴──────────────────────────────┤
│  honba-strategies (Workspace Catalog): 17+ Quantitative Strategies, step.toml Schemas, Pytest Conformance (436 passing)   │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Key Tenets
1. **Zero Compromise on Monetary Precision**: Every price, limit price, fee, turnover metric, and account cash balance across `honba-ya` utilizes `rust_decimal::Decimal`. Floating-point values (`f32`/`f64`) are strictly prohibited in domain calculation logic, backtesting ledgers, and API payloads.
2. **Deterministic Research-Live Parity**: The exact same `Strategy` trait (`honba-ya-strategy`), risk evaluation rules (`honba-mon`), statutory Indian cost formulas, and execution state machines run in backtesting as in live trading. Backtests do not take shortcuts; they process the same canonical event sequence.
3. **Actor-Based Fault Isolation & Backpressure**: All live processing occurs inside bounded, supervised Tokio actors (`MarketDataActor`, `StrategyActor`, `RiskActor`, `ExecutionActor`). Unbounded memory growth is prevented by backpressure policies (`try_send` with coalescing); crashes are caught and quarantined by the supervisor without taking down the trading node.
4. **Authoritative SEBI Pre-Trade Risk Gate**: All orders generated by strategies, REST calls, or Python scripts must pass synchronously through `honba-mon::RiskEngine` wrapped by `RiskActor`. SEBI `algo_id` validation, Order-to-Trade Ratio (OTR) breakers, scrip price bands, and kill-switches cannot be bypassed under any operational mode.
5. **Zero-Polling Real-Time WebSocket Streaming**: The API WebSocket handler subscribes directly to the actor broadcast bus. Ticks, order state transitions, fills, and depth updates stream to web clients (`honba-kao`) with sub-millisecond dispatch latency rather than periodic timer polling.
6. **Immutable Regulatory Audit Trails**: Every order attempt, cancellation, modification, pre-trade refusal, broker ack, and execution fill is persisted with microsecond-resolution timestamps, correlation IDs, and algorithm tags in an append-only SQLite/PostgreSQL audit ledger.
7. **Direct Lakehouse Partition Ingestion**: Native compatibility with `honba-data`'s Hive-partitioned directory layout (`catalog/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet`), enabling instant bar serving and backtesting without duplicate file conversions.
8. **Decoupled Strategy Runtime vs Catalog**: The execution runtime (`honba-ya-strategy`) and optimizer (`honba-ya-enhancer`) live in Rust inside `honba-ya`, while strategy definitions and research catalogs live in Python (`honba-strategies/`) using canonical `honba.domain` types and declarative `step.toml` manifests.

---

## 2. Crate Topology & Dependency Rules

The workspace consolidates into a cohesive **10-crate topology**, clearly separating the strategy execution runtime (`honba-ya-strategy`), strategy optimization & enhancement (`honba-ya-enhancer`), actor runtime (`honba-ya-actors`), event-driven backtesting (`honba-ya-backtest`), persistence (`honba-ya-persistence`), APIs, and CLI:

```text
honba-ya/
├── Cargo.toml                     # Workspace root manifest (10 members)
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
    │       ├── strategy.rs        # StrategyActor & Strategy trait bridge
    │       ├── risk.rs            # RiskActor: honba-mon pre-trade risk evaluation
    │       ├── execution.rs       # ExecutionActor: honba-te broker gateway management
    │       ├── supervisor.rs      # Actor supervision tree, drain & restart logic
    │       ├── metrics.rs         # Prometheus metrics exporter
    │       └── tracing_spans.rs   # Structured tracing spans (OrderGenerated -> Filled)
    ├── honba-ya-strategy/         # Canonical strategy trait, pull-based runner & manifest parser
    │   ├── Cargo.toml             # package: honba-ya-strategy, lib: honba_strategy
    │   └── src/
    │       ├── lib.rs             # Core exports: Strategy, StrategyRunner, Manifest
    │       ├── strategy.rs        # Strategy trait (on_bar, on_tick, on_fill, on_order_ack)
    │       ├── runner.rs          # StrategyRunner: pull-based zero-I/O event machine
    │       ├── step.rs            # StepManifest parser: deserializes step.toml into StrategyManifest
    │       ├── manifest.rs        # StrategyManifest, ParameterDef, TimeframeDef
    │       ├── context.rs         # StrategyContext: market snapshot & indicators
    │       ├── intent.rs          # OrderIntent, TargetPosition, ClosePosition
    │       ├── ir.rs              # Strategy Intermediate Representation (IR)
    │       ├── dyn_strategy.rs    # Dynamic boxed strategy dispatcher
    │       ├── error.rs           # StrategyError & RunnerError
    │       ├── buy_and_hold.rs    # Built-in reference strategy
    │       ├── sma_crossover.rs   # Built-in SMA crossover strategy
    │       └── rsi_reversal.rs    # Built-in RSI mean-reversion strategy
    ├── honba-ya-enhancer/         # Automatic Strategy Enhancer (ASE: GA, Bayesian, WFO, Odysseus)
    │   ├── Cargo.toml             # package: honba-ya-enhancer
    │   └── src/
    │       ├── lib.rs             # Core optimizer interfaces
    │       ├── ga_optimizer.rs    # Multi-Objective Genetic Algorithm & Pareto frontier
    │       ├── bayesian_optimizer.rs # Bayesian hyperparameter search (TPE)
    │       ├── walk_forward.rs    # Walk-Forward Optimization (WFO & WFE metric)
    │       ├── odysseus.rs        # StockSharp Odysseus Genetic Rule Tree Evolution
    │       ├── overfitting_guardrails.rs # Deflated Sharpe Ratio (DSR) & PBO
    │       ├── monte_carlo_stress.rs # Trade shuffling & slippage/spread shock injection
    │       └── ai_refinement.rs   # Closed-loop LLM feedback integration (honba-nou)
    ├── honba-ya-backtest/         # Research-live parity event-driven backtesting engine
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs             # Module exports
    │       ├── engine.rs          # BacktestEngine: deterministic event loop
    │       ├── catalog.rs         # CatalogLoader: zero-copy Arrow/Parquet bar reader
    │       ├── events.rs          # MarketEvent, SignalEvent, OrderEvent, FillEvent
    │       ├── calendar.rs        # NseTradingCalendar: holidays & Muhurat trading
    │       ├── sim/               # Converged simulation modules (absorbed from honba-ya-sim)
    │       │   ├── l2_book.rs     # L2 order book simulation & depth walking
    │       │   ├── latency.rs     # Seeded network and broker latency models
    │       │   ├── slippage.rs    # Volatility, spread crossing & adverse fill slippage models
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
    │       ├── optimize.rs        # `honba optimize`: executes honba-ya-enhancer
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
             │                          ▲
             ▼                          │
         [honba-ya-enhancer] (ASE Optimization Engines)
             │                          │
Layer 2: [honba-ya-actors]   ──▶ [honba-ya-backtest] ──▶ [honba-ya-strategy] (Strategy Trait & Runner)
             │                          │
Layer 1: [honba-ya-persistence] ──▶ [honba-ya-config]
             │
Layer 0: Canonical platform crates: [honba-core], [honba-mon], [honba-te-core], [honba-me-core]
```

### Completed Deprecations & Realignment
1. **Renamed & Modernized `honba-ya-enhancer`**: Formerly `honba-strategies`, this crate was renamed to accurately describe its role as the Automatic Strategy Enhancer (GA, Bayesian, WFO, Odysseus). Fixed Rust 2024 `r#gen` conflicts, Rayon `ThreadRng` `!Sync` bugs, and float `Ord` map issues.
2. **Relocated `honba-ya-strategy`**: Relocated from `honba-nou/crates/honba-nou-strategy` to `honba-ya/crates/honba-ya-strategy`. Contains the zero-I/O `Strategy` trait, pull-based `StrategyRunner`, and `step.toml` parser.
3. **Retired `crates/honba-ya-ws`**: The 15-line broadcast stub is deleted. WebSocket streaming is unified in `honba-ya-api-rest/src/ws.rs`.
4. **Retired `crates/honba-ya-engine`**: Superseded by `honba-ya-actors` and `honba-ya-backtest`. Deleted along with all usages of `honba-messages`, `honba-entities`, and `honba-ports`.
5. **Consolidated `crates/honba-ya-sim` into `honba-ya-backtest`**: High-fidelity microstructure models (L2 book, latency, slippage, MIS square-off) migrated to `honba-ya-backtest/src/sim/`, upgraded to `rust_decimal::Decimal`.
6. **Retired `crates/honba-ya-async` and `honba-ya-sweep`**: Replaced by `honba-ya-enhancer` and `honba-ya-backtest`.
7. **Python Strategy Catalog (`honba-strategies/`) Realignment**: Completely decoupled from Rust engine crates. Standardized on `honba.domain` types exported by `honba-tamashii`. All 436 pytest tests pass.
8. **Manifest Alignment (`step.rs`)**: `honba-ya-strategy` natively deserializes `step.toml` (`step.schema.json`) into `StrategyManifest`, verified by integration tests across all 17 strategies in `honba-strategies/`.

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
- **`StrategyActor`**: Executes strategy logic by wrapping `honba-ya-strategy::Strategy` instances. Converts market signals into `OrderIntent` candidate structs.
- **`RiskActor`**: Wraps `honba_mon::RiskEngine` synchronously. Every candidate order is evaluated with microsecond latency measurement. Rejections are immediately logged to the audit ledger and returned to the caller; approvals are passed to `ExecutionActor`.
- **`ExecutionActor`**: Interfaces with `honba_te_core::BrokerExecutor`. Manages order state transitions (`PendingNew` $\to$ `New` $\to$ `PartiallyFilled` $\to$ `Filled`), retries on network blips, and reconciles broker acks with the persistence layer.

### 3.2 `honba-ya-strategy`: Zero-I/O Strategy Trait, Runner & Step Manifests

`honba-ya-strategy` defines the single source of truth for strategy execution and configuration:

```rust
pub trait Strategy: Send + 'static {
    fn name(&self) -> &str;
    fn on_bar(&mut self, bar: &Bar, ctx: &StrategyContext) -> Result<Vec<OrderIntent>, StrategyError>;
    fn on_tick(&mut self, tick: &Tick, ctx: &StrategyContext) -> Result<Vec<OrderIntent>, StrategyError>;
    fn on_order_ack(&mut self, ack: &OrderAck) -> Result<(), StrategyError>;
    fn on_fill(&mut self, fill: &FillReport) -> Result<(), StrategyError>;
}
```

- **Zero-I/O `StrategyRunner`**: Pull-based state machine that advances strictly through domain events (`on_bar`, `on_tick`, `on_fill`). Does no I/O, networking, or blocking syscalls, ensuring 100% determinism.
- **`step.rs` (`StepManifest`)**: Deserializes the declarative `step.toml` format matching `step.schema.json` directly into canonical `StrategyManifest`. Reads strategy metadata, authors, timeframes, default parameters, and parameter sweep spaces (`min`, `max`, `step`, `type`).
- **Standardized Order Intents**: Strategies emit declarative `OrderIntent` candidates (`MarketOrder`, `LimitOrder`, `TargetPosition`, `CloseAll`), insulating strategy code from broker-specific order management logic.

### 3.3 `honba-ya-enhancer`: Automatic Strategy Enhancer (ASE)

Inspired by institutional optimizers and Jesse, `honba-ya-enhancer` provides advanced algorithmic optimization:

- **Multi-Objective Genetic Algorithm (`ga_optimizer.rs`)**: Chromosome encoding of strategy parameter spaces from `step.toml`. Tournament selection, uniform crossover, Gaussian mutations, and Pareto frontier optimization across Sharpe, Sortino, and Max Drawdown.
- **Bayesian Optimization (`bayesian_optimizer.rs`)**: Tree-structured Parzen Estimators (TPE) for probabilistic exploration of non-linear parameter landscapes with Rayon parallel evaluations.
- **Walk-Forward Analysis (`walk_forward.rs`)**: Anchored and rolling out-of-sample window partitioning. Computes Walk-Forward Efficiency (WFE) requiring $\text{WFE} \ge 0.60$ for production deployment.
- **StockSharp Odysseus Evolution (`odysseus.rs`)**: Genetic programming engine evolving composite strategy rule trees (indicators, boolean logic, execution conditions) at up to 1,000 candidate evaluations per second.
- **Overfitting Guardrails (`overfitting_guardrails.rs`)**: Computes Marcos López de Prado's Deflated Sharpe Ratio (DSR) and Probability of Backtest Overfitting (PBO) via CSCV.
- **Monte Carlo Stress Testing (`monte_carlo_stress.rs`)**: Trade sequence permutation, slippage/spread shock injection, and bootstrap VaR/CVaR confidence estimation.

### 3.4 `honba-ya-backtest`: Institutional Backtester & Simulation Models

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

### 3.5 `honba-ya-persistence`: sqlx Schema & SEBI Regulatory Audit Ledger

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

### 3.6 `honba-ya-api-rest` & Unified WebSocket Gateway

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

### 3.7 `honba-ya-cli`: Unified Operator & Developer CLI

The single authoritative binary `honba` provides institutional workflow commands:

```bash
# Start the full trading daemon (REST API, WebSocket, Actors)
honba serve --data-dir ../honba-data/data --addr 127.0.0.1:8000 --trading-state active

# Run a deterministic backtest with Indian statutory costs
honba backtest --config configs/momentum_nifty.toml --from 2025-01-01 --to 2025-12-31 --output results/

# Run Automatic Strategy Enhancer (GA / Bayesian / WFO)
honba optimize --strategy momentum --method ga --wfo --data-dir ../honba-data/data

# Run database migrations
honba migrate --database-url sqlite://honba.db

# Inspect and validate market data catalog
honba data catalog-summary --data-dir ../honba-data/data

# Inspect or trigger emergency risk halts
honba trading-state halt --reason "High OTR breach observed"
```

---

## 4. Phase-by-Phase Implementation Roadmap

### Phase 1: Canonical Type Unification, Legacy Pruning & Architectural Realignment (M0) — Week 1–2

**Status (2026-10-23): done.** 
`cargo check --workspace` is 100% clean across all 10 member crates. All 436 pytest tests in `honba-strategies/` pass cleanly. All 17 `step.toml` manifests validate against `honba-ya-strategy::StepManifest`.

Completed work in Phase 1 and the subsequent 4-step architectural realignment:
1. **Type Migration & Legacy Pruning**:
   - Standardized all crates on `honba-core`, `honba-mon`, `rust_decimal::Decimal`, and `honba-api::wire` types.
   - Excised all usages of deleted legacy crates: `honba_messages`, `honba_entities`, `honba_risk`, `honba_ports`, and `honba-kokoro-risk`.
   - Pruned dead crates from disk and workspace: `honba-ya-ws`, `honba-ya-engine`, `honba-ya-sim`, `honba-ya-async`, `honba-ya-sweep`.
   - Fixed corrupted `honba-ya-backtest/engine.rs` block and replaced dead `ExecutionVenue` abstraction with concrete `NextOpenSim`.
2. **Step 1: Optimizer Modernization & Renaming (`honba-ya-enhancer`)**:
   - Renamed `honba-ya/crates/honba-strategies` to `honba-ya-enhancer`.
   - Updated package metadata, fixed Rust 2024 `r#gen` identifier conflicts, repaired Rayon thread-safety (`ThreadRng` `!Sync`), and fixed float `Ord` map operations. Compiles with 0 warnings/errors.
3. **Step 2: Strategy Runtime Relocation (`honba-ya-strategy`)**:
   - Relocated runtime crate from `honba-nou/crates/honba-nou-strategy` to `honba-ya/crates/honba-ya-strategy` (`honba_strategy` lib).
   - Removed circular dependency risk; updated sibling `honba-nou` workspace to depend on `honba-ya-strategy`.
   - Both workspaces compile cleanly.
4. **Step 3: Strategy Catalog Domain Type Unification (`honba-strategies/`)**:
   - Modernized the top-level Python quantitative strategy catalog by purging all deprecated `from honba.entities.*` imports in favor of canonical `from honba.domain.*` types.
   - Exported `Trade` in `honba-tamashii/python/honba/__init__.py`.
   - All 436 pytest tests pass in 1.40s.
5. **Step 4: Manifest Alignment (`step.rs`)**:
   - Implemented `step.rs` in `honba-ya-strategy` with `StepManifest` deserializing `step.toml` (`step.schema.json`) into canonical `StrategyManifest`.
   - Added `StrategyManifest::from_step_toml_str` constructor.
   - Added comprehensive integration tests (`tests/step_manifest.rs`) validating that all 17 strategy catalogs deserialize without error.

- [x] **1.1 Workspace Dependency Cleanup**
  - [x] Standardize all Cargo dependencies to canonical `honba-core`, `honba-mon`, `honba-te-core`, `honba-me-core`.
  - [x] Excise all usages of `honba-messages`, `honba-entities`, and `honba-ports` across all crates.
  - [x] Purge excised `honba-kokoro-risk` from `honba-ya/Cargo.toml` and migrate to `honba-mon`.
- [x] **1.2 Decimal Arithmetic Enforcement**
  - [x] Replace remaining `f64` prices, quantities, and cash values in `honba-ya-api`, `honba-ya-cli`, `honba-ya-api-rest`, and `honba-ya-backtest` with `rust_decimal::Decimal`.
  - [x] Ensure all API serialization formats support lossless decimal strings.
- [x] **1.3 Legacy Crates Pruning**
  - [x] `crates/honba-ya-ws`, `crates/honba-ya-engine`, `crates/honba-ya-sim`, `crates/honba-ya-async`, and `crates/honba-ya-sweep` deleted; calls redirected to `honba-ya-actors`/`honba-ya-backtest`/`honba-ya-enhancer`.
- [x] **1.4 Strategy Runtime & Optimizer Realignment**
  - [x] Relocate `honba-nou-strategy` -> `honba-ya/crates/honba-ya-strategy`.
  - [x] Rename `honba-ya/crates/honba-strategies` -> `crates/honba-ya-enhancer` and modernize for Rust 2024.
  - [x] Unify Python catalog `honba-strategies/` on `honba.domain` types (436 passing tests).
  - [x] Implement `step.rs` in `honba-ya-strategy` to parse `step.toml` matching `step.schema.json` across all 17 catalog strategies.

### Phase 2: Production Persistence & sqlx Relational Schema (M1) — Week 3–4

**Status (2026-10-23): done.**
All 21 persistence integration tests pass. Schema migrations verified idempotent on both SQLite and PostgreSQL.

- [x] **2.1 sqlx Relational Schema & Migrations**
  - [x] Create sqlx migration files for `traders`, `orders`, `fills`, `decisions`, `positions`, and `audit_ledger`.
  - [x] Add dual SQLite and PostgreSQL support in `honba-ya-persistence` via `Database` enum and `with_executor!`.
- [x] **2.2 Repositories Implementation**
  - [x] Implement `OrderRepository` (`OrderStore`) with idempotent upsert and state transition validations.
  - [x] Implement `FillRepository` (`FillStore`) with fee breakdown persistence and automatic promotion to filled.
  - [x] Implement `PositionRepository` (`PositionStore`) supporting long-only FIFO tax-lot tracking and exact Decimal WAC calculations.
  - [x] Implement `DecisionRepository` (`DecisionStore`) with prompt sha256 checksum tracking and deterministic replays.
  - [x] Implement `TraderRepository` (`TraderStore`) for trader profiles and allocated capital management.
- [x] **2.3 Persistence Integration Testing**
  - [x] Verify database roundtrips, migration idempotency, and concurrent transaction safety under load (`concurrency_tests.rs`, `migrations_tests.rs`, `traders_tests.rs`).

### Phase 3: Actor Runtime & WebSocket Streaming Gateway (M2) — Week 5–6

**Status (2026-10-23): done.**
All actor supervision tests and live WebSocket roundtrip integration tests pass. Zero polling delay across the WebSocket broadcast bus.

- [x] **3.1 Actor Supervision & Lifecycle Hardening**
  - [x] Implement actor restart strategies (`OneForOne`, `AllForOne`) in `Supervisor` with dynamic policy updates and per-actor restart counters.
  - [x] Add backpressure drop/coalescing policies (`BackpressurePolicy::CoalesceLatest`, `DropNewest`, `DropOldest`) in `MarketDataActor` to prevent memory blowups.
  - [x] Ensure clean shutdown drain loops for in-flight orders during graceful stop in `ExecutionActor`.
- [x] **3.2 Zero-Polling WebSocket Gateway**
  - [x] Rebuild `honba-ya-api-rest/src/ws.rs` to subscribe directly to `MarketDataActor`'s broadcast channel via `AppState::subscribe_market_data`.
  - [x] Remove the 1000ms polling loop; stream ticks, depth, and order state events immediately with sub-millisecond dispatch.
  - [x] Validate bidirectional client messaging (subscribe, unsubscribe, ping/pong, close) matching `honba-fe/apps/web/src/core/data-layer.ts` verified with live WebSocket integration tests (`ws_tests.rs`).

### Phase 4: Institutional Strategy Tester (`honba-ya-backtest`) (M3) — Week 7–8

**Status (2026-10-23): done.**
All 27 unit and integration tests across `honba-ya-backtest` pass cleanly (`backtest_tests.rs`, `engine_strategy_runner_tests.rs`, `l2_latency_tests.rs`, `l2_participation_cap_tests.rs`, `mis_square_off_tests.rs`, `catalog_and_settlement_tests.rs`) with 0 clippy warnings.

- [x] **4.1 Microstructure Simulation & Order Book Matching**
  - [x] Wire `honba-ya-strategy::StrategyRunner` into `BacktestEngine` deterministic event loop.
  - [x] Port L2 order book depth matching and latency injection from `honba-ya-sim` into `honba-ya-backtest/src/sim/`.
  - [x] Upgrade simulation models to strict `rust_decimal::Decimal` and `honba_core::Tick`.
  - [x] Implement volume-participation caps, spread crossing penalties, and adverse fill slippage models.
  - [x] Integrate intraday MIS 15:20 IST auto-square-off logic into `BacktestEngine`.
- [x] **4.2 Complete Indian Statutory Cost & Settlement Modeling**
  - [x] Wire `honba-kokoro-portfolio` (FIFO tax-lots) and `honba-kokoro-ledger` (double-entry cash/charges).
  - [x] Implement exact Indian cost calculation: STT (0.1% delivery, 0.025% MIS sell), Stamp Duty, NSE turnover, SEBI fees, and 18% GST.
  - [x] Model ICCL T+1 Demat rolling settlement cycles and margin holdbacks.
- [x] **4.3 Lakehouse Catalog Ingestion**
  - [x] Support loading historical bars directly from `honba-data/data/catalog` into backtests with zero-copy Arrow readers.

### Phase 5: Automatic Strategy Enhancer (`honba-ya-enhancer`) (M4) — Week 9–10
- [ ] **5.1 Multi-Objective Genetic Algorithm (GA) Optimizer Integration**
  - [ ] Wire `ga_optimizer.rs` to read strategy chromosome spaces directly from `honba_strategy::StepManifest`.
  - [ ] Execute tournament selection, uniform/arithmetic crossover, and Gaussian mutation operators across Rayon worker threads.
  - [ ] Calculate multi-objective fitness balancing Sharpe, Sortino, Profit Factor, and Max Drawdown, exporting the Pareto-optimal frontier.
- [ ] **5.2 Bayesian Hyperparameter Optimization (TPE)**
  - [ ] Wire `bayesian_optimizer.rs` Tree-structured Parzen Estimators for probabilistic search over high-dimensional spaces.
  - [ ] Support parallel candidate evaluation across multi-threaded Rayon worker pools.
- [ ] **5.3 Walk-Forward Analysis (WFO)**
  - [ ] Wire `walk_forward.rs` anchored and rolling out-of-sample cross-validation partitions.
  - [ ] Compute Walk-Forward Efficiency (WFE) requiring WFE >= 0.60 for production certification.
- [ ] **5.4 Overfitting Guardrails (DSR & PBO)**
  - [ ] Wire `overfitting_guardrails.rs` Marcos López de Prado’s Deflated Sharpe Ratio (DSR) adjusting for number of trials, return skewness, and kurtosis.
  - [ ] Implement Probability of Backtest Overfitting (PBO) via Combinatorially Symmetric Cross-Validation (CSCV).
- [ ] **5.5 Monte Carlo Permutation & Stress Testing**
  - [ ] Wire `monte_carlo_stress.rs` trade sequence shuffling (without replacement) to determine worst-case drawdown confidence intervals (95%, 99% VaR/CVaR).
  - [ ] Inject synthetic spread/slippage shocks (1.5x, 2.0x, 3.0x) and gap-down scenarios.
- [ ] **5.6 Closed-Loop AI Refinement Integration**
  - [ ] Interface with `honba-nou` AI Strategy Enhancer loop to ingest diagnostic reports and apply prompt-driven parameter/rule mutations.
- [ ] **5.7 StockSharp Odysseus Genetic Generation Backend Runner**
  - [ ] Rayon-accelerated parallel evaluator for Odysseus candidate rule trees against historical tick/bar streams.
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
  - [ ] Implement `honba optimize` subcommand supporting `--method ga`, `--method bayesian`, and `--wfo` backed by `honba-ya-enhancer`.
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
| **Strategy Manifest Conformance** | 100% valid deserialization of all catalog `step.toml` manifests into `StrategyManifest`. | `tests/step_manifest.rs` testing all catalog strategies. |
| **Python Research Parity** | 100% test pass rate for all strategy research code with canonical `honba.domain` types. | Pytest suite in `honba-strategies/` (436 passed). |
