//! `StrategyRunner` over one ordered event stream (ADR 0019 decision 4, E2-S6(b)).
//!
//! Real flow: strategy -> runner -> `ScriptedExecution` -> events -> runner. Each scenario
//! asserts the invariant `filled + released == ordered` per instrument and the order's FSM
//! status as the runner tracked it.

use std::collections::HashSet;

use honba_engine::{Handler, Result};
use honba_messages::{Bar, InstrumentId, OrderStatus, UnixNanos};
use honba_sim::{Behavior, ScriptedExecution, VenueAction};
use honba_strategy::{OrderIntent, Strategy, StrategyContext, StrategyRunner};
use honba_testing::VecFeed;
use honba_testing::fixtures::instrument;

/// Buys `qty` of each symbol on the first bar it sees.
struct BuyOnce {
    symbols: Vec<(&'static str, f64)>,
    done: HashSet<u64>,
}

impl Strategy for BuyOnce {
    fn name(&self) -> &str {
        "s"
    }
    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, bar: &Bar) -> Result<()> {
        if self.done.insert(bar.ts_event().as_u64()) && self.done.len() == 1 {
            for (sym, qty) in &self.symbols {
                ctx.submit(OrderIntent::market_buy(instrument(sym), *qty));
            }
        }
        Ok(())
    }
}

fn runner(
    symbols: &[(&'static str, f64)],
    exec: ScriptedExecution,
) -> StrategyRunner<BuyOnce, ScriptedExecution> {
    let s = BuyOnce {
        symbols: symbols.to_vec(),
        done: HashSet::new(),
    };
    StrategyRunner::new(s, exec)
}

fn bar(r: &mut impl Handler, ts: u64) {
    let b = VecFeed::bar("X", 10.0, ts).event().clone();
    r.on_event(&b, UnixNanos::from_u64(ts)).unwrap();
}

fn filled(r: &StrategyRunner<BuyOnce, ScriptedExecution>, id: &InstrumentId) -> f64 {
    r.fills()
        .iter()
        .filter(|t| t.instrument_id() == id)
        .map(|t| t.quantity())
        .sum()
}

#[test]
fn partial_then_fill_releases_nothing() {
    let exec = ScriptedExecution::new(10.0).with("s-0", Behavior::Hold);
    let venue = exec.clone();
    let mut r = runner(&[("X", 10.0)], exec);
    bar(&mut r, 1);
    venue
        .venue(
            "s-0",
            VenueAction::Fill { quantity: 4.0 },
            UnixNanos::from_u64(2),
        )
        .unwrap();
    bar(&mut r, 2);
    assert_eq!(
        r.order_state("s-0").unwrap().status,
        OrderStatus::PartiallyFilled
    );
    assert!(r.context().busy(&instrument("X")));
    venue
        .venue(
            "s-0",
            VenueAction::Fill { quantity: 6.0 },
            UnixNanos::from_u64(3),
        )
        .unwrap();
    bar(&mut r, 3);
    let x = instrument("X");
    assert_eq!(r.order_state("s-0").unwrap().status, OrderStatus::Filled);
    assert_eq!(filled(&r, &x), 10.0);
    assert_eq!(r.released_quantity(&x), 0.0);
    assert!(!r.context().busy(&x));
}

#[test]
fn venue_reject_after_partial_releases_only_the_remainder() {
    let exec = ScriptedExecution::new(10.0).with("s-0", Behavior::partial(4.0, "venue_halt"));
    let mut r = runner(&[("X", 10.0)], exec);
    bar(&mut r, 1);
    let x = instrument("X");
    assert_eq!(filled(&r, &x), 4.0);
    assert_eq!(r.released_quantity(&x), 6.0);
    assert_eq!(filled(&r, &x) + r.released_quantity(&x), 10.0);
    assert_eq!(r.order_state("s-0").unwrap().status, OrderStatus::Rejected);
    assert!(!r.context().busy(&x));
}

#[test]
fn cancel_race_books_fill_before_cancel_and_sums_to_ordered() {
    let exec = ScriptedExecution::new(10.0).with("s-0", Behavior::Hold);
    let venue = exec.clone();
    let mut r = runner(&[("X", 10.0)], exec);
    bar(&mut r, 1);
    // The fill is enqueued before the cancel is processed, both at the same ts.
    venue
        .venue(
            "s-0",
            VenueAction::Fill { quantity: 4.0 },
            UnixNanos::from_u64(1),
        )
        .unwrap();
    r.cancel("s-0").unwrap();
    let x = instrument("X");
    assert_eq!(filled(&r, &x), 4.0);
    assert_eq!(r.released_quantity(&x), 6.0);
    assert_eq!(r.order_state("s-0").unwrap().status, OrderStatus::Cancelled);
    assert_eq!(r.order_rejections().len(), 1);
    assert!(r.order_rejections()[0].is_cancelled());
    assert!(!r.context().busy(&x));
    // Cancelling again, or a finished order, releases nothing more.
    r.cancel("s-0").unwrap();
    assert_eq!(r.released_quantity(&x), 6.0);
}

#[test]
fn expiry_releases_per_instrument() {
    let exec = ScriptedExecution::new(10.0).with("s-0", Behavior::Expire);
    let mut r = runner(&[("X", 7.0), ("Y", 3.0)], exec);
    bar(&mut r, 1);
    let (x, y) = (instrument("X"), instrument("Y"));
    assert_eq!(r.released_quantity(&x), 7.0);
    assert_eq!(r.released_quantity(&y), 0.0);
    assert_eq!(filled(&r, &y), 3.0);
    assert_eq!(r.order_state("s-0").unwrap().status, OrderStatus::Expired);
    assert_eq!(r.order_state("s-1").unwrap().status, OrderStatus::Filled);
    assert_eq!(r.order_rejections()[0].reason, "expired");
    assert!(!r.context().busy(&x) && !r.context().busy(&y));
}

#[test]
fn a_fill_after_its_reject_does_not_double_release() {
    // Release comes from the one ordered stream; the FSM refuses the late fill's state change
    // but the ledger still books it (a venue fact), and nothing is released twice.
    let exec = ScriptedExecution::new(10.0).with("s-0", Behavior::reject("no_funds"));
    let mut r = runner(&[("X", 5.0)], exec);
    bar(&mut r, 1);
    let x = instrument("X");
    assert_eq!(r.released_quantity(&x), 5.0);
    bar(&mut r, 2);
    assert_eq!(r.released_quantity(&x), 5.0);
}
