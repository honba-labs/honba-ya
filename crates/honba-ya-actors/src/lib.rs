//! # 本場・役 honba-ya-actors
//!
//! Asynchronous actor runtime and supervision tree for the Honba Indian equities trading platform.
//!
//! Architecture:
//! - [`MarketDataActor`]: Ingests market data feeds, normalizes broker symbols, and broadcasts ticks.
//! - [`StrategyActor`]: Consumes ticks and bars, evaluates signals, and produces candidate orders.
//! - [`PositionSizingActor`]: Supervised actor dynamically calculating position sizes based on equity and risk budget.
//! - [`RiskActor`]: Wraps [`honba_mon::RiskEngine`] synchronously to enforce SEBI OTR limits, circuit price bands, and kill switches.
//! - [`ExecutionActor`]: Manages broker order lifecycles, retries, fills, and acks via [`honba_te_core::BrokerExecutor`].
//! - [`Supervisor`]: Manages actor supervision trees with graceful shutdown and drain states.
//! - [`PythonStrategyActor`]: Isolated OS thread pool execution model with cooperative Tokio boundary and cancellation tokens.
//! - [`MetricsCollector`]: Prometheus real-time metrics exporter for OTR, latencies, PnL, turnover, mailbox depths, and refusals.
//! - Tracing spans: OpenTelemetry structured tracing spans for distributed order lifecycle and mailbox turnaround.

pub mod execution;
pub mod market_data;
pub mod messages;
pub mod metrics;
pub mod portfolio;
pub mod python;
pub mod risk;
pub mod sizing;
pub mod slo;
pub mod strategy;
pub mod supervisor;
pub mod tracing_spans;

pub use execution::ExecutionActor;
pub use market_data::{BackpressurePolicy, MarketDataActor};
pub use messages::{
    ActorStatus, ExecutionMsg, MarketDataMsg, PortfolioMsg, PositionSizingMsg, RiskMsg, StrategyMsg,
};
pub use metrics::MetricsCollector;
pub use portfolio::PortfolioActor;
pub use python::{PythonExecutionError, PythonStrategyActor, PythonStrategyEngine};
pub use risk::RiskActor;
pub use sizing::PositionSizingActor;
pub use strategy::{Strategy, StrategyActor};
pub use supervisor::{RestartStrategy, Supervisor};
pub use tracing_spans::*;
