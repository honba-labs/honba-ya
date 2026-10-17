//! The commands an operator can send into a running engine task.

use honba_engine::TradingState;

/// Something an operator asks of a live engine task.
///
/// A command is data: it crosses a bounded channel into the task and is applied
/// between two dispatched messages, never during one. The channel is the only
/// way in — there is no shared mutable state on the engine, so a command is the
/// sole answer to "what happens next".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// Stop the run gracefully: stop reading the feed, drain the queue through
    /// [`Engine::finish`](honba_engine::Engine::finish), and run `on_stop` for
    /// every handler.
    ///
    /// This is a drain, never an abort. Every message already injected into the
    /// kernel is dispatched and audited before any handler is told the run is
    /// over, which is what keeps the audit stream complete. Dropping the
    /// [`EngineHandle`](crate::EngineHandle) instead detaches the task and gives
    /// up that guarantee, so `Stop` is the only way to end a run that is
    /// accounted for. `JoinHandle::abort` is reserved for a task that has
    /// already panicked.
    Stop,
    /// Move the engine's trading state, which is how an operator halts or
    /// resumes a live session.
    ///
    /// The kernel applies it through
    /// [`Engine::set_trading_state`](honba_engine::Engine::set_trading_state):
    /// the transition is checked, an actual change is audited, and orders from
    /// a halted engine are refused rather than dropped. The state also governs
    /// whether orders are accepted at all, so a `Halted` command is a risk
    /// control that needs no cooperation from a strategy.
    State(TradingState),
}
