//! The error taxonomy of a parameter sweep.

use thiserror::Error;

/// The result type returned by every fallible operation in this crate.
pub type Result<T> = std::result::Result<T, SweepError>;

/// A failure raised by a sweep.
///
/// A sweep has two kinds of failure and they are deliberately different types.
/// A *plan* that cannot be run at all ([`SweepError::InvalidPlan`]) is the
/// caller's mistake and stops the sweep. A *trial* that fails
/// ([`SweepError::Trial`]) is a result, not a stop: [`run`](crate::run()) turns it
/// into a [`Failed`](crate::TrialOutcome::Failed) outcome and keeps going.
#[derive(Clone, Debug, Error, PartialEq)]
#[non_exhaustive]
pub enum SweepError {
    /// The plan itself cannot be run: no concurrency limit, or cash and
    /// periods a trial could not be scored with.
    #[error("invalid sweep plan: {0}")]
    InvalidPlan(String),

    /// A trial task never produced a result, because it was cancelled or
    /// panicked outside the trial itself.
    #[error("sweep task did not complete: {0}")]
    Join(String),

    /// One trial failed. The sweep carries on without it.
    #[error("trial {trial_id} failed: {reason}")]
    Trial {
        /// The trial that failed.
        trial_id: usize,
        /// Why it failed.
        reason: String,
    },

    /// Metrics could not be computed from a trial's fills.
    #[error("analytics failed: {0}")]
    Analytics(String),
}
