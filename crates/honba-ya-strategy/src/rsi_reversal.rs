//! RSI mean-reversion strategy.

use honba_core::{Bar, InstrumentId, Quantity};
use honba_indicators::{Indicator, Rsi};
use rust_decimal::prelude::ToPrimitive;

use crate::context::StrategyContext;
use crate::error::Result;
use crate::intent::OrderIntent;
use crate::strategy::Strategy;

/// Where the strategy believes it sits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Flat,
    Long,
    Short,
}

/// Buys when RSI drops below the oversold threshold, sells when it rises
/// above the overbought threshold.
///
/// The strategy tracks its own assumed side so it doesn't emit duplicate
/// signals while RSI remains in the extreme zone.
///
/// ```
/// use honba_strategy::{RsiReversal, Strategy};
/// use honba_core::{Exchange, InstrumentId, Quantity};
///
/// let strategy = RsiReversal::new(
///     InstrumentId::new(Exchange::Nse, "NIFTY50"),
///     14, 30.0, 70.0, Quantity::from_u64(75),
/// );
/// assert_eq!(strategy.name(), "rsi_reversal");
/// ```
pub struct RsiReversal {
    instrument_id: InstrumentId,
    rsi: Rsi,
    oversold: f64,
    overbought: f64,
    quantity: Quantity,
    side: Side,
}

impl RsiReversal {
    /// Creates an RSI reversal strategy.
    ///
    /// # Panics
    ///
    /// Panics if `period` is zero or thresholds are out of order.
    pub fn new(
        instrument_id: InstrumentId,
        period: usize,
        oversold: f64,
        overbought: f64,
        quantity: Quantity,
    ) -> Self {
        assert!(period > 0, "RSI period must be positive");
        assert!(oversold < overbought, "oversold must be below overbought");
        Self {
            instrument_id,
            rsi: Rsi::new(period),
            oversold,
            overbought,
            quantity,
            side: Side::Flat,
        }
    }

    /// Returns the RSI period.
    pub fn period(&self) -> usize {
        self.rsi.period()
    }
}

impl Strategy for RsiReversal {
    fn name(&self) -> &str {
        "rsi_reversal"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, bar: &Bar) -> Result<()> {
        let close = bar.close.0.to_f64().unwrap_or(0.0);
        let Some(v) = self.rsi.update(close) else {
            return Ok(());
        };

        if v < self.oversold && self.side != Side::Long {
            ctx.submit(OrderIntent::market_buy(
                self.instrument_id.clone(),
                self.quantity,
            ));
            self.side = Side::Long;
        } else if v > self.overbought && self.side != Side::Short {
            ctx.submit(OrderIntent::market_sell(
                self.instrument_id.clone(),
                self.quantity,
            ));
            self.side = Side::Short;
        }
        Ok(())
    }
}
