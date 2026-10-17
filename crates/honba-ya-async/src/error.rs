//! The error taxonomy of the async shell.
//!
//! A failure raised by the kernel or by a port crosses the shell boundary
//! unchanged: [`AsyncError::Kernel`] and [`AsyncError::Feed`] hold the original
//! error so the edge can match on it. The shell adds only the failures that
//! exist because the engine runs in a task: the task is gone, the task
//! panicked, or a command was refused before it was ever queued.

use honba_engine::AlgoError;
use honba_ports::PortError;
use thiserror::Error;

/// The result type returned by every fallible operation in this crate.
pub type Result<T> = std::result::Result<T, AsyncError>;

/// A failure raised at the async shell boundary.
#[derive(Debug, Error, PartialEq)]
#[non_exhaustive]
pub enum AsyncError {
    /// The synchronous kernel failed. The [`AlgoError`] is propagated verbatim.
    #[error(transparent)]
    Kernel(AlgoError),

    /// A port failed, in practice the market-data feed. The [`PortError`] is
    /// propagated verbatim, so [`PortError::is_retryable`] still decides whether
    /// a supervisor should reconnect.
    #[error(transparent)]
    Feed(PortError),

    /// The engine task has stopped, so no further command can reach it.
    #[error("the engine task has stopped")]
    Closed,

    /// The engine task panicked.
    ///
    /// The panic payload is deliberately not carried: a task panic can only be
    /// observed as [`tokio::task::JoinError`], whose payload is not comparable,
    /// and keeping it would cost `AsyncError` its `PartialEq` so that an audit
    /// record and an error could still be compared in a test. The loss is the
    /// panic message itself, which the runtime has already printed on stderr.
    #[error("the engine task panicked")]
    Panicked,

    /// A command was refused before it reached the engine task.
    #[error("command rejected: {0}")]
    Rejected(String),
}

impl From<AlgoError> for AsyncError {
    fn from(err: AlgoError) -> Self {
        AsyncError::Kernel(err)
    }
}

impl From<PortError> for AsyncError {
    fn from(err: PortError) -> Self {
        AsyncError::Feed(err)
    }
}
