//! Unit tests for the runner when the execution port or the strategy fails mid-drain.
//!
//! An `Err` from `on_event` is terminal for the run, but it must leave the
//! context consistent: nothing drained is silently dropped.

use honba_engine::{AlgoError, ExecutionEngine, Handler, Result};
use honba_entities::{Currency, Trade};
use honba_messages::{Order, OrderId, OrderSide, UnixNanos};
use honba_testing::VecFeed;
use honba_testing::fixtures::instrument;

use crate::{OrderIntent, Strategy, StrategyContext, StrategyRunner};

/// Buys one unit of each of `A`, `B`, `C` on the first bar.
struct ThreeBuys {
    sent: bool,
    fail_on_fill: bool,
}

impl Strategy for ThreeBuys {
    fn name(&self) -> &str {
        "three"
    }
    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &honba_messages::Bar) -> Result<()> {
        if !std::mem::replace(&mut self.sent, true) {
            for s in ["A", "B", "C"] {
                ctx.submit(OrderIntent::market_buy(instrument(s), 1.0));
            }
        }
        Ok(())
    }
    fn on_fill(&mut self, _ctx: &mut dyn StrategyContext, _fill: &Trade) -> Result<()> {
        if self.fail_on_fill {
            Err(AlgoError::Component("on_fill failed".into()))
        } else {
            Ok(())
        }
    }
}

/// Fails the `fail_at`-th submit (0-based); accepts and holds the others.
/// Optionally reports a fill for every order id in `fill_all` on the next drain.
struct FlakyPort {
    fail_at: Option<usize>,
    submits: usize,
    accepted: Vec<String>,
    fill_on_drain: bool,
}

impl ExecutionEngine for FlakyPort {
    fn submit(&mut self, order: Order) -> Result<()> {
        let n = self.submits;
        self.submits += 1;
        if self.fail_at == Some(n) {
            return Err(AlgoError::Component("port down".into()));
        }
        self.accepted
            .push(order.instrument_id().symbol().to_string());
        Ok(())
    }
    fn cancel(&mut self, _order_id: &str, _now: UnixNanos) -> Result<()> {
        Ok(())
    }
    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        if !self.fill_on_drain {
            return Ok(Vec::new());
        }
        self.fill_on_drain = false;
        Ok(["A", "B", "C"]
            .iter()
            .enumerate()
            .map(|(i, s)| {
                Trade::new(
                    OrderId::new(format!("three-{i}")),
                    instrument(s),
                    OrderSide::Buy,
                    1.0,
                    10.0,
                    Currency::Inr,
                    UnixNanos::from_u64(1),
                    UnixNanos::from_u64(1),
                )
            })
            .collect())
    }
}

fn first_bar(r: &mut impl Handler) -> Result<honba_engine::EngineOutput> {
    let bar = VecFeed::bar("X", 10.0, 1).event().clone();
    r.on_event(&bar, UnixNanos::from_u64(1))
}

fn runner(port: FlakyPort, fail_on_fill: bool) -> StrategyRunner<ThreeBuys, FlakyPort> {
    StrategyRunner::new(
        ThreeBuys {
            sent: false,
            fail_on_fill,
        },
        port,
    )
}

#[test]
fn a_submit_error_releases_the_failed_and_remaining_intents() {
    let port = FlakyPort {
        fail_at: Some(1),
        submits: 0,
        accepted: Vec::new(),
        fill_on_drain: false,
    };
    let mut r = runner(port, false);
    assert!(first_bar(&mut r).is_err());
    // The first order was accepted by the port and is still working.
    assert_eq!(r.submitted().len(), 1);
    assert!(r.context().busy(&instrument("A")));
    // The refused one and the one never attempted must not stay busy.
    assert!(!r.context().busy(&instrument("B")));
    assert!(!r.context().busy(&instrument("C")));
}

#[test]
fn a_failing_on_fill_does_not_drop_the_remaining_fills() {
    let port = FlakyPort {
        fail_at: None,
        submits: 0,
        accepted: Vec::new(),
        fill_on_drain: true,
    };
    let mut r = runner(port, true);
    assert!(first_bar(&mut r).is_err());
    // Every drained fill is booked and recorded even though the hook failed.
    assert_eq!(r.fills().len(), 3);
    for s in ["A", "B", "C"] {
        assert_eq!(r.context().position(&instrument(s)), 1.0, "{s}");
        assert!(!r.context().busy(&instrument(s)), "{s}");
    }
}
