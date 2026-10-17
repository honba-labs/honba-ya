//! The two clocks the shell can run on.
//!
//! Both implement [`honba_ports::Clock`], and neither is visible to domain
//! code: a handler sees timestamps on the messages it is given and the kernel's
//! own monotonic clock, never a clock object. This is the shell's half of the
//! rule that [`Clock`] is the only time source — the kernel advances its clock
//! from `ts_event`, and an edge that needs to pace a timeout or a reconnect
//! borrows one of these.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use honba_messages::UnixNanos;
use honba_ports::{Clock, PortError, PortResult};

/// A clock backed by the runtime's timer.
///
/// `now` is the wall clock captured once at construction plus the elapsed
/// [`tokio::time::Instant`]. The origin is the runtime's own instant rather than
/// a converted `std` one, which is what makes the clock exact under test: with
/// `tokio::time::pause()` the runtime instant is virtual, so `now` advances by
/// exactly the virtual duration and not by however long the machine took to run
/// the test. A live session therefore needs no sleeping to be tested, and a
/// paused one is not merely close to the truth — it is the truth.
///
/// Reading the process clock happens once, in the constructor. Nothing else in
/// this crate reads it, and neither clock is reachable from the kernel.
#[derive(Debug, Clone, Copy)]
pub struct LiveClock {
    origin: tokio::time::Instant,
    epoch: UnixNanos,
}

impl LiveClock {
    /// Creates a clock anchored to the current runtime instant and the current
    /// wall clock.
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
            epoch: wall_clock(),
        }
    }

    /// Creates a clock anchored to `start` and the current wall clock.
    ///
    /// `start` is the instant this clock reports as its epoch, for a replay or
    /// a live session resuming mid-day: the offset between the wall clock and
    /// the runtime is `start`'s age rather than an accident of when this was
    /// constructed. Under `tokio::time::pause()` the age is real elapsed time
    /// because that is what `start` measures; use [`LiveClock::new`] when the
    /// anchor should be the paused instant itself.
    pub fn new_at(start: std::time::Instant) -> Self {
        let now = tokio::time::Instant::now();
        let origin = now.checked_sub(start.elapsed()).unwrap_or(now);
        Self {
            origin,
            epoch: wall_clock(),
        }
    }
}

impl Default for LiveClock {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Clock for LiveClock {
    async fn now(&self) -> UnixNanos {
        let elapsed = tokio::time::Instant::now().saturating_duration_since(self.origin);
        UnixNanos::from_u64(self.epoch.as_u64() + nanos_of(elapsed))
    }

    async fn sleep(&self, dur: Duration) -> PortResult<()> {
        tokio::time::sleep(dur).await;
        Ok(())
    }
}

/// A clock that moves only when told to.
///
/// `sleep` advances this clock by the requested duration instead of waiting, so
/// a replay or a backtest that paces itself on [`Clock::sleep`] finishes
/// instantly and produces the same timestamps every run. Time never moves
/// backwards: [`HistoricClock::advance_to`] refuses a rewind rather than
/// producing a stream a kernel would reject as
/// [`AlgoError::ClockRegression`](honba_engine::AlgoError::ClockRegression).
#[derive(Debug)]
pub struct HistoricClock {
    now: AtomicU64,
}

impl HistoricClock {
    /// Creates a clock reading `start`.
    pub fn new(start: UnixNanos) -> Self {
        Self {
            now: AtomicU64::new(start.as_u64()),
        }
    }

    /// Moves the clock to `next`.
    ///
    /// Returns [`PortError::InvalidRequest`] and leaves the clock untouched when
    /// `next` is earlier than the current value.
    pub fn advance_to(&self, next: UnixNanos) -> PortResult<()> {
        let current = self.now.load(Ordering::SeqCst);
        if next.as_u64() < current {
            return Err(PortError::InvalidRequest(format!(
                "cannot advance a historic clock backwards: {current} -> {}",
                next.as_u64()
            )));
        }
        self.now.store(next.as_u64(), Ordering::SeqCst);
        Ok(())
    }
}

fn nanos_of(dur: Duration) -> u64 {
    u64::try_from(dur.as_nanos()).unwrap_or(u64::MAX)
}

fn wall_clock() -> UnixNanos {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    UnixNanos::from_u64(nanos_of(since_epoch))
}

#[async_trait]
impl Clock for HistoricClock {
    async fn now(&self) -> UnixNanos {
        UnixNanos::from_u64(self.now.load(Ordering::SeqCst))
    }

    async fn sleep(&self, dur: Duration) -> PortResult<()> {
        let next = self
            .now
            .load(Ordering::SeqCst)
            .saturating_add(nanos_of(dur));
        self.now.store(next, Ordering::SeqCst);
        Ok(())
    }
}
