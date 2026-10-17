//! Seeded latency models and the depth-survival discount (E3-S2).
//!
//! A [`LatencyModel`] delays an order's acknowledgement and its place in the
//! queue at the touch. Both delays are drawn from the simulator's seeded
//! [`StdRng`], so the same seed gives the same delays.

use rand::rngs::StdRng;
use rand::Rng;

/// The seeded delay an order sees before it is acknowledged, and before it can
/// reach the front of a price queue.
pub trait LatencyModel: Send + Sync {
    /// Milliseconds before the venue acknowledges the order.
    fn ack_delay_ms(&self, rng: &mut StdRng) -> u64;

    /// Milliseconds a resting order waits behind the depth already at the touch.
    fn queue_delay_ms(&self, rng: &mut StdRng) -> u64;

    /// The total delay in milliseconds: acknowledgement plus queue.
    ///
    /// A simulator calls this once per fill; it draws both delays in a fixed
    /// order (ack, then queue) so a run is reproducible.
    fn total_delay_ms(&self, rng: &mut StdRng) -> u64 {
        self.ack_delay_ms(rng)
            .saturating_add(self.queue_delay_ms(rng))
    }
}

/// No latency: both delays are zero. The default.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoLatency;

impl LatencyModel for NoLatency {
    fn ack_delay_ms(&self, _rng: &mut StdRng) -> u64 {
        0
    }

    fn queue_delay_ms(&self, _rng: &mut StdRng) -> u64 {
        0
    }
}

/// Seeded uniform latency: the ack delay is drawn from `0..=ack_max_ms` and the
/// queue delay from `0..=queue_max_ms`, in that order, from the simulator's
/// stream. A fixed seed reproduces the same delays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeededLatency {
    ack_max_ms: u64,
    queue_max_ms: u64,
}

impl SeededLatency {
    /// Creates a model with the given inclusive maxima.
    pub const fn new(ack_max_ms: u64, queue_max_ms: u64) -> Self {
        Self {
            ack_max_ms,
            queue_max_ms,
        }
    }

    /// The inclusive maximum acknowledgement delay.
    pub const fn ack_max_ms(&self) -> u64 {
        self.ack_max_ms
    }

    /// The inclusive maximum queue delay.
    pub const fn queue_max_ms(&self) -> u64 {
        self.queue_max_ms
    }
}

impl LatencyModel for SeededLatency {
    fn ack_delay_ms(&self, rng: &mut StdRng) -> u64 {
        rng.gen_range(0..=self.ack_max_ms)
    }

    fn queue_delay_ms(&self, rng: &mut StdRng) -> u64 {
        rng.gen_range(0..=self.queue_max_ms)
    }
}

/// The fraction of resting depth expected to survive while an order is in
/// flight. It is one function, shared by signal sizing and simulated fills so
/// the two cannot disagree.
///
/// **Stub.** No depth source exists yet (`/depth` returns none), so the
/// discount is always `1.0` (all depth survives). `resting_depth` (in units)
/// and `elapsed_ms` are accepted now so callers and the wire shape do not change
/// when a depth feed lands. Per-instrument survival curves are deferred with it.
pub fn depth_survival_discount(_resting_depth: f64, _elapsed_ms: u64) -> f64 {
    1.0
}
