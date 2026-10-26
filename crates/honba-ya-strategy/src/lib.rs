#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(unsafe_code)]

//! Strategy trait and reference strategies for Honba.
//!
//! A [`Strategy`] reacts to typed events and acts only through its
//! [`StrategyContext`]: it reads the clock, positions, cash and instrument
//! metadata, and submits [`OrderIntent`]s. It does not execute; that is the
//! execution layer's job. [`StrategyRunner`] pairs a strategy and its
//! [`LedgerContext`] with an optional [`honba_mon::RiskEngine`] pre-trade
//! gate and turns submitted intents into [`honba_core::OrderRequest`]s for
//! the host (an actor runtime, a backtester, ...) to actually submit through
//! its own [`honba_core::ports::ExecutionGateway`]; this crate stays zero-I/O
//! and never talks to an execution engine directly. The contract matches the
//! Python `honba.strategies` package (ADR 008).

pub mod buy_and_hold;
pub mod context;
pub mod contract_probe;
pub mod dyn_strategy;
pub mod error;
pub mod factory;
pub mod intent;
pub mod ir;
pub mod manifest;
pub mod rsi_reversal;
pub mod runner;
pub mod sma_crossover;
pub mod step;
pub mod strategy;

pub use buy_and_hold::BuyAndHold;
pub use context::{LedgerContext, StrategyContext};
pub use contract_probe::{ContractProbe, Observation};
pub use dyn_strategy::DynStrategy;
pub use error::{Result, StrategyError};
pub use factory::{BuiltinStrategyFactory, StrategyFactory, StrategyFactoryRegistry};
pub use intent::{IntentError, OrderIntent};
pub use ir::{IrError, IrSubscriptions, IrUniverse, StrategyIr};
pub use manifest::{
    BarAggregation, ManifestError, ParameterSpec, STRATEGY_API_VERSION, StrategyManifest,
    Subscriptions, TimeframeSpec, Universe, param_range_to_schema, parameter_grid_from_manifest,
    sweep_space_from_manifest,
};
pub use rsi_reversal::RsiReversal;
pub use runner::{
    IntentRejection, OrderRejection, StrategyRunner, SubmittedIntent, SuppressedIntent,
};
pub use sma_crossover::{MAX_SMA_PERIOD, SmaCrossover};
pub use step::{
    HistoryBars, StepChain, StepConsumes, StepEmits, StepKind, StepManifest, StepParam, StepPipeline,
    StepRequires, StepShapeOrList,
};
pub use strategy::{Strategy, StrategyAdapter};

#[cfg(test)]
mod tests;
