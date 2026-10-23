//! Buy-and-hold reference strategy.

use honba_core::{Bar, InstrumentId, Quantity};

use crate::context::StrategyContext;
use crate::error::Result;
use crate::intent::OrderIntent;
use crate::strategy::Strategy;

/// Buys once on the first bar, then does nothing.
///
/// The simplest possible strategy — useful as a baseline and as a minimal
/// example of the [`Strategy`] trait.
///
/// ```
/// use honba_strategy::{BuyAndHold, Strategy};
/// use honba_core::{Exchange, InstrumentId, Quantity};
///
/// let strategy = BuyAndHold::new(
///     InstrumentId::new(Exchange::Nse, "NIFTY50"),
///     Quantity::from_u64(75),
/// );
/// assert_eq!(strategy.name(), "buy_and_hold");
/// ```
pub struct BuyAndHold {
    instrument_id: InstrumentId,
    quantity: Quantity,
    bought: bool,
}

impl BuyAndHold {
    /// Creates a buy-and-hold strategy.
    pub fn new(instrument_id: InstrumentId, quantity: Quantity) -> Self {
        Self {
            instrument_id,
            quantity,
            bought: false,
        }
    }

    /// Returns the instrument being traded.
    pub fn instrument_id(&self) -> &InstrumentId {
        &self.instrument_id
    }

    /// Returns the order quantity.
    pub fn quantity(&self) -> Quantity {
        self.quantity
    }

    /// Returns `true` if the strategy has already emitted its buy.
    pub fn has_bought(&self) -> bool {
        self.bought
    }
}

impl Strategy for BuyAndHold {
    fn name(&self) -> &str {
        "buy_and_hold"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> Result<()> {
        if !self.bought {
            ctx.submit(OrderIntent::market_buy(
                self.instrument_id.clone(),
                self.quantity,
            ));
            self.bought = true;
        }
        Ok(())
    }
}
