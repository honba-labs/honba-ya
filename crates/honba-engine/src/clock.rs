//! Monotonic event clock.

use honba_messages::UnixNanos;

use crate::error::{AlgoError, Result};

/// Tracks the timestamp of the most recently processed event.
///
/// The clock never moves backwards. Any call to [`Clock::advance_to`] with an
/// earlier timestamp returns [`AlgoError::ClockRegression`], which surfaces
/// data-feed ordering bugs immediately.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Clock {
    now: UnixNanos,
}

impl Clock {
    /// Creates a clock at the given time.
    pub fn new(now: UnixNanos) -> Self {
        Self { now }
    }

    /// Returns the current time.
    pub fn now(&self) -> UnixNanos {
        self.now
    }

    /// Advances to the given time.
    ///
    /// Returns an error if `to` is earlier than the current time.
    pub fn advance_to(&mut self, to: UnixNanos) -> Result<()> {
        if to < self.now {
            return Err(AlgoError::ClockRegression {
                current: self.now.as_u64(),
                requested: to.as_u64(),
            });
        }
        self.now = to;
        Ok(())
    }
}
