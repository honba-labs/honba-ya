//! Event handler trait.

use honba_messages::{Event, UnixNanos};

use crate::audit::AuditKind;
use crate::error::Result;
use crate::state::TradingState;

/// Anything that reacts to events.
///
/// Handlers are called by the engine in `ts_event` order. Implementations
/// typically branch on [`Event`] variants they care about and ignore the rest.
pub trait Handler: Send {
    /// Called once at the start of a run.
    fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called for every event in the queue, in time order.
    fn on_event(&mut self, event: &Event, ts_init: UnixNanos) -> Result<EngineOutput>;

    /// Called by [`Engine::set_trading_state`](crate::Engine::set_trading_state) on every
    /// handler when the state actually changes, so a handler that submits orders itself
    /// can enforce `Halted` and reduce-only (ADR 0018 decision 7). Default: ignore.
    fn on_trading_state(&mut self, _state: TradingState) {}

    /// Whether this handler owns a risk stage. A run holds at most one stage:
    /// [`Engine::start`](crate::Engine::start) fails with
    /// [`AlgoError::DuplicateRiskStage`](crate::AlgoError::DuplicateRiskStage) otherwise.
    fn holds_risk_stage(&self) -> bool {
        false
    }

    /// Hands over the audit records this handler produced since the last call, oldest first
    /// (ADR 0018 decision 6: a handler that refuses orders itself, before they reach the
    /// engine, reports the refusal here). Drain semantics: returned records are moved, so a
    /// second call returns only what is new. The [`Engine`](crate::Engine) calls it after
    /// every `on_event` and state-change dispatch and appends the records, in order, to its
    /// own [`AuditLog`](crate::AuditLog). Default: nothing to report.
    fn drain_audit(&mut self) -> Vec<AuditKind> {
        Vec::new()
    }

    /// Called once at the end of a run.
    fn on_stop(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A `Handler` that does nothing. Useful as a default.
pub struct NoopHandler;

impl Handler for NoopHandler {
    fn on_event(&mut self, _event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        Ok(EngineOutput::None)
    }
}

/// What a handler asks the engine to do about the event it just saw.
///
/// This is the whole handler-to-engine contract: a handler decides, the
/// engine applies. It never applies a command itself and it never talks to an
/// exchange — the engine routes [`EngineOutput::Orders`] and
/// [`EngineOutput::Cancels`] to the attached
/// [`ExecutionEngine`](crate::ExecutionEngine), moves its
/// [`TradingState`] on [`EngineOutput::StateChange`], and records what it did
/// in its [`AuditLog`](crate::AuditLog).
///
/// Returning [`EngineOutput::None`] is the common case: most events are
/// observed and ignored. A handler is asked for an output on every event, so
/// it must not mutate shared state behind the engine's back.
#[derive(Debug, Clone, PartialEq)]
pub enum EngineOutput {
    /// No action to take.
    None,
    /// Submit new orders, in the order given.
    Orders(Vec<honba_messages::Order>),
    /// Cancel orders by id, in the order given.
    Cancels(Vec<honba_messages::OrderId>),
    /// Move the engine to another [`TradingState`].
    StateChange(TradingState),
}
