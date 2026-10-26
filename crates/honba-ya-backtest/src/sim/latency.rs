//! Seeded network and broker latency models for backtest simulation.

/// Seeded random number generator for deterministic latency simulation.
///
/// Uses a simple xorshift algorithm seeded at construction for reproducible
/// backtest runs with varying latency profiles.
#[derive(Debug, Clone)]
pub struct SeededRng {
    state: u64,
}

impl SeededRng {
    /// Create a new seeded RNG from a seed value.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Generate next u64 in sequence.
    fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.state = x;
        x
    }
}

/// Network latency model with configurable profile.
///
/// Simulates variable network propagation delay between market data feed and
/// broker execution venue. Latency can be:
/// - Constant: fixed delay (e.g., 5ms)
/// - Uniform: random between min and max
/// - Trending: increasing or decreasing over time
/// - Spiky: occasional high-latency events
#[derive(Debug, Clone)]
pub struct NetworkLatency {
    /// Mean latency in microseconds
    mean_us: u64,
    /// Standard deviation in microseconds
    std_dev_us: u64,
    /// Seeded RNG for deterministic simulation
    rng: SeededRng,
    /// Current latency in microseconds
    current_latency_us: u64,
    /// Step counter for trending profiles
    step: u64,
}

impl NetworkLatency {
    /// Create a new latency model with mean and standard deviation.
    pub fn new(mean_us: u64, std_dev_us: u64, seed: u64) -> Self {
        Self {
            mean_us,
            std_dev_us,
            rng: SeededRng::new(seed),
            current_latency_us: mean_us,
            step: 0,
        }
    }

    /// Sample a latency value from the distribution.
    pub fn sample(&mut self) -> u64 {
        self.step += 1;

        // Simple deterministic approximation: mean + triangular deviation
        // In a full implementation, would use proper random sampling
        let deviation = if self.std_dev_us > 0 {
            // Triangular distribution approximation: mean ± std_dev
            let sign = if (self.rng.next() & 1) == 0 { 1 } else { -1 };
            sign * (self.std_dev_us as i64 / 2)
        } else {
            0
        };

        let sampled = self.mean_us as i64 + deviation;
        self.current_latency_us = sampled.max(1) as u64;
        self.current_latency_us
    }

    /// Get current latency setting.
    pub fn current_latency(&self) -> u64 {
        self.current_latency_us
    }

    /// Advance the latency profile (for trending models).
    pub fn advance_step(&mut self) {
        self.step += 1;
        // Simple trending: latency increases slowly over time
        self.current_latency_us = self
            .mean_us
            .saturating_add(self.step / 1000)
            .max(self.mean_us);
    }
}

/// Broker execution latency model with square-off timing.
///
/// Models the MIS (Margin Intraday Square-off) at 15:20 IST where all
/// open intraday positions are forcibly squared off.
#[derive(Debug, Clone)]
pub struct MisSquareOffLatency {
    /// Square-off time in IST (15:20 = 15*3600 + 20*60 = 55200 seconds from midnight)
    square_off_seconds: u64,
    /// Current simulation time in IST seconds from midnight
    current_time_ist_seconds: u64,
    /// Whether square-off has been triggered today
    square_off_triggered: bool,
}

impl MisSquareOffLatency {
    /// Create a new MIS square-off latency model.
    pub fn new() -> Self {
        // 15:20 IST = 15 hours 20 minutes from midnight = 55200 seconds
        Self {
            square_off_seconds: 15 * 3600 + 20 * 60,
            current_time_ist_seconds: 0,
            square_off_triggered: false,
        }
    }

    /// Advance simulation time by one second (IST).
    pub fn advance_time_ist(&mut self, seconds: u64) {
        self.current_time_ist_seconds += seconds;
        // Check if we've passed square-off time
        if !self.square_off_triggered && self.current_time_ist_seconds >= self.square_off_seconds {
            self.square_off_triggered = true;
        }
    }

    /// Check if square-off has been triggered for today.
    pub fn is_square_off_triggered(&self) -> bool {
        self.square_off_triggered
    }

    /// Reset for a new trading day.
    pub fn reset_day(&mut self) {
        self.current_time_ist_seconds = 0;
        self.square_off_triggered = false;
    }

    /// Get time until square-off.
    pub fn time_until_square_off(&self) -> Option<u64> {
        if self.square_off_triggered {
            return None;
        }
        let remaining = self
            .square_off_seconds
            .saturating_sub(self.current_time_ist_seconds);
        Some(remaining)
    }
}

impl Default for MisSquareOffLatency {
    fn default() -> Self {
        Self::new()
    }
}
