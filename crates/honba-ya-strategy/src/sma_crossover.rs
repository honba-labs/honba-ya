//! SMA crossover strategy.

use honba_core::{Bar, InstrumentId, Quantity};
use honba_indicators::{Indicator, Sma};
use rust_decimal::prelude::ToPrimitive;

use crate::context::StrategyContext;
use crate::error::Result;
use crate::intent::OrderIntent;
use crate::strategy::Strategy;

/// The largest SMA period [`SmaCrossover`] accepts through untrusted input
/// (the `run_strategy` binding, configs, agents). Far above any practical
/// lookback (the longest conventional one is 200) yet small enough that
/// the indicator buffers stay trivial.
pub const MAX_SMA_PERIOD: usize = 10_000;

/// Emits a buy when the fast SMA crosses above the slow SMA, and a sell
/// when it crosses back below.
///
/// The strategy tracks only the *cross*, not the position. It emits one buy
/// per upward cross and one sell per downward cross, leaving position
/// accounting to the execution layer.
///
/// ```
/// use honba_strategy::{SmaCrossover, Strategy};
/// use honba_core::{Exchange, InstrumentId, Quantity};
///
/// let strategy = SmaCrossover::new(
///     InstrumentId::new(Exchange::Nse, "NIFTY50"),
///     5, 20, Quantity::from_u64(75),
/// );
/// assert_eq!(strategy.name(), "sma_crossover");
/// ```
pub struct SmaCrossover {
    instrument_id: InstrumentId,
    fast: Sma,
    slow: Sma,
    quantity: Quantity,
    prev_above: Option<bool>,
}

impl SmaCrossover {
    /// Creates an SMA crossover strategy.
    ///
    /// # Panics
    ///
    /// Panics if `fast >= slow` or either is zero.
    pub fn new(instrument_id: InstrumentId, fast: usize, slow: usize, quantity: Quantity) -> Self {
        assert!(fast > 0 && slow > 0, "SMA periods must be positive");
        assert!(fast < slow, "fast period must be less than slow period");
        Self {
            instrument_id,
            fast: Sma::new(fast),
            slow: Sma::new(slow),
            quantity,
            prev_above: None,
        }
    }

    /// Returns the fast period.
    pub fn fast_period(&self) -> usize {
        self.fast.period()
    }

    /// Returns the slow period.
    pub fn slow_period(&self) -> usize {
        self.slow.period()
    }
}

impl Strategy for SmaCrossover {
    fn name(&self) -> &str {
        "sma_crossover"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, bar: &Bar) -> Result<()> {
        let close = bar.close.0.to_f64().unwrap_or(0.0);
        let f = self.fast.update(close);
        let s = self.slow.update(close);

        if let (Some(f), Some(s)) = (f, s) {
            let above = f > s;
            match self.prev_above {
                Some(prev) if prev != above => {
                    let intent = if above {
                        OrderIntent::market_buy(self.instrument_id.clone(), self.quantity)
                    } else {
                        OrderIntent::market_sell(self.instrument_id.clone(), self.quantity)
                    };
                    ctx.submit(intent);
                }
                _ => {}
            }
            self.prev_above = Some(above);
        }
        Ok(())
    }
}
