#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(unsafe_code)]

//! Event kernel for the Honba trading platform.
//!
//! The kernel is deliberately small: a monotonic [`Clock`], a time-ordered
//! [`EventQueue`], a [`Handler`] trait, and an [`Engine`] that drives them.
//! Data sources and execution exchanges are injected via the [`DataFeed`] and
//! [`ExecutionEngine`] traits, so the same kernel runs backtests, paper
//! trading, and live sessions without code changes.
//!
//! A handler answers every event with an [`EngineOutput`] — orders to submit,
//! orders to cancel, or a new [`TradingState`] — and the engine is what
//! applies it: it routes the commands to the attached [`ExecutionEngine`],
//! turns fills back into [`Event::OrderFilled`](honba_messages::Event::OrderFilled)
//! messages, and appends what it did to an [`AuditLog`].

pub mod audit;
pub mod cache;
pub mod clock;
pub mod data;
pub mod decision;
pub mod engine;
pub mod error;
pub mod execution;
pub mod handler;
pub mod queue;
pub mod reconciliation;
pub mod state;

pub use audit::{
    AuditJournalReader, AuditJournalWriter, AuditKind, AuditLog, AuditRecord, JournalHeader,
    JournalV1, ReplayLlmDecision, ReplayOrder, ReplayState,
};
pub use cache::{CacheQuery, StateCache, TrackedOrder};
pub use clock::Clock;
pub use data::DataFeed;
pub use decision::{LlmDecision, LlmDecisionError, LlmDecisionKind};
pub use engine::Engine;
pub use error::{AlgoError, Result};
pub use execution::{ExecutionEngine, LegacyDrains, LegacyPortEvents, OrderRejection};
pub use handler::{EngineOutput, Handler, NoopHandler};
pub use queue::EventQueue;
pub use reconciliation::{
    BrokerOrderReport, BrokerPositionReport, BrokerSnapshot, BrokerTradeReport, GhostOrder,
    MissedFill, PositionDrift, Reconciler, ReconciliationReport, StaleOrder,
};
pub use state::TradingState;

#[cfg(test)]
mod tests;
