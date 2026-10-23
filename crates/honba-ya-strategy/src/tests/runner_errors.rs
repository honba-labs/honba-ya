//! Unit tests for the runner when the strategy fails during a hook.
//!
//! An `Err` from `on_event` or `on_fill` is surfaced to the caller while
//! leaving the context consistent.

use honba_core::uuid::Uuid;
use honba_core::{Bar, FillReport, MarketEvent, OrderSide, Price, Quantity};
use honba_testing::fixtures::{flat_bar, instrument, utc_nanos};
use rust_decimal::Decimal;

use crate::{OrderIntent, Result, Strategy, StrategyContext, StrategyError, StrategyRunner};

/// Buys one unit of each of `A`, `B`, `C` on the first bar.
struct ThreeBuys {
    sent: bool,
    fail_on_fill: bool,
    fail_on_reject: bool,
}

impl Strategy for ThreeBuys {
    fn name(&self) -> &str {
        "three"
    }
    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> Result<()> {
        if !std::mem::replace(&mut self.sent, true) {
            ctx.submit(OrderIntent::market_buy(
                instrument("A"),
                Quantity::from_u64(1),
            ));
            if self.fail_on_reject {
                ctx.submit(OrderIntent::market_buy(
                    instrument("B"),
                    Quantity::new(Decimal::ZERO),
                ));
            } else {
                ctx.submit(OrderIntent::market_buy(
                    instrument("B"),
                    Quantity::from_u64(1),
                ));
            }
            ctx.submit(OrderIntent::market_buy(
                instrument("C"),
                Quantity::from_u64(1),
            ));
        }
        Ok(())
    }
    fn on_fill(&mut self, _ctx: &mut dyn StrategyContext, _fill: &FillReport) -> Result<()> {
        if self.fail_on_fill {
            Err(StrategyError::other("on_fill failed"))
        } else {
            Ok(())
        }
    }
    fn on_intent_rejected(
        &mut self,
        _ctx: &mut dyn StrategyContext,
        _intent: &OrderIntent,
        _error: &crate::IntentError,
    ) -> Result<()> {
        if self.fail_on_reject {
            Err(StrategyError::other("on_intent_rejected failed"))
        } else {
            Ok(())
        }
    }
}

fn first_bar(r: &mut StrategyRunner<ThreeBuys>) -> Result<()> {
    let bar = flat_bar("X", 10.0, 1);
    r.on_event(&MarketEvent::Bar(bar), utc_nanos(1))
}

#[test]
fn an_intent_rejection_hook_error_propagates() {
    let mut r = StrategyRunner::new(ThreeBuys {
        sent: false,
        fail_on_fill: false,
        fail_on_reject: true,
    });
    assert!(first_bar(&mut r).is_err());
    // The first order was queued and is still working.
    assert_eq!(r.submitted().len(), 1);
    assert!(r.context().busy(&instrument("A")));
    // The invalid intent never made B busy.
    assert!(!r.context().busy(&instrument("B")));
}

#[test]
fn a_failing_on_fill_still_books_the_fill_in_context() {
    let mut r = StrategyRunner::new(ThreeBuys {
        sent: false,
        fail_on_fill: true,
        fail_on_reject: false,
    });
    first_bar(&mut r).unwrap();
    let fill = FillReport::new(
        "F-0",
        "three-0",
        Uuid::nil(),
        instrument("A"),
        OrderSide::Buy,
        Price::new(Decimal::from(10)),
        Quantity::from_u64(1),
        "three",
        utc_nanos(1),
    );
    assert!(r.on_fill(&fill).is_err());
    // The fill was booked in the context before the hook failed.
    assert_eq!(r.context().position(&instrument("A")), Decimal::ONE);
    assert!(!r.context().busy(&instrument("A")));
}
