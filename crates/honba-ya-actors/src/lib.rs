//! # 本場・役 honba-ya-actors
//!
//! Asynchronous actor runtime and supervision tree for the Honba Indian equities trading platform.
//!
//! Architecture:
//! - [`MarketDataActor`]: Ingests market data feeds, normalizes broker symbols, and broadcasts ticks.
//! - [`StrategyActor`]: Consumes ticks and bars, evaluates signals, and produces candidate orders.
//! - [`RiskActor`]: Wraps [`honba_mon::RiskEngine`] synchronously to enforce SEBI OTR limits, circuit price bands, and kill switches.
//! - [`ExecutionActor`]: Manages broker order lifecycles, retries, fills, and acks via [`honba_te_core::BrokerExecutor`].
//! - [`Supervisor`]: Manages actor supervision trees with graceful shutdown and drain states.

pub mod execution;
pub mod market_data;
pub mod messages;
pub mod risk;
pub mod strategy;
pub mod supervisor;

pub use execution::ExecutionActor;
pub use market_data::MarketDataActor;
pub use messages::{ActorStatus, ExecutionMsg, MarketDataMsg, RiskMsg, StrategyMsg};
pub use risk::RiskActor;
pub use strategy::{Strategy, StrategyActor};
pub use supervisor::Supervisor;
