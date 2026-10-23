//! A type-erased [`Strategy`], for callers that store strategies of mixed types.

use honba_core::{Bar, FillReport, Tick};

use crate::context::StrategyContext;
use crate::error::Result;
use crate::intent::{IntentError, OrderIntent};
use crate::strategy::Strategy;

/// A type-erased [`Strategy`], delegating every hook to the boxed strategy.
///
/// [`StrategyRunner`](crate::StrategyRunner) and
/// [`StrategyAdapter`](crate::StrategyAdapter) are generic over the strategy
/// they wrap, which is what a single backtest wants: the engine hands out
/// `Box<dyn Handler>` and each handler is its own concrete type. A sweep is
/// different — it holds a *list* of trials, each with its own strategy, before
/// any of them is run, so the list has to be one type. This is that type.
///
/// Every hook delegates, including [`Strategy::on_start`] and
/// [`Strategy::on_stop`]: a hook that silently did nothing would silently
/// change the behaviour of a boxed strategy, which is exactly the bug a
/// type-erasing wrapper is prone to. Errors propagate unchanged, so a strategy
/// that fails inside a sweep fails the same way it would on its own.
///
/// ```
/// use honba_strategy::{BuyAndHold, DynStrategy, Strategy};
/// use honba_messages::{Exchange, InstrumentId};
///
/// let strategy = DynStrategy::new(Box::new(BuyAndHold::new(
///     InstrumentId::new("NIFTY50", Exchange::new("NSE")),
///     75.0,
/// )));
/// assert_eq!(strategy.name(), "buy_and_hold");
/// ```
pub struct DynStrategy(Box<dyn Strategy>);

impl DynStrategy {
    /// Erases `inner`'s type.
    pub fn new(inner: Box<dyn Strategy>) -> Self {
        Self(inner)
    }
}

impl Strategy for DynStrategy {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn on_start(&mut self, ctx: &mut dyn StrategyContext) -> Result<()> {
        self.0.on_start(ctx)
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, bar: &Bar) -> Result<()> {
        self.0.on_bar(ctx, bar)
    }

    fn on_quote(&mut self, ctx: &mut dyn StrategyContext, quote: &Tick) -> Result<()> {
        self.0.on_quote(ctx, quote)
    }

    fn on_trade(&mut self, ctx: &mut dyn StrategyContext, trade: &Tick) -> Result<()> {
        self.0.on_trade(ctx, trade)
    }

    fn on_fill(&mut self, ctx: &mut dyn StrategyContext, fill: &FillReport) -> Result<()> {
        self.0.on_fill(ctx, fill)
    }

    fn on_intent_rejected(
        &mut self,
        ctx: &mut dyn StrategyContext,
        intent: &OrderIntent,
        error: &IntentError,
    ) -> Result<()> {
        self.0.on_intent_rejected(ctx, intent, error)
    }

    fn on_stop(&mut self, ctx: &mut dyn StrategyContext) -> Result<()> {
        self.0.on_stop(ctx)
    }
}
