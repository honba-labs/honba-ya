//! Unit tests for the runner's rejection queue (cross-language vectors: `tests/order_rejections.rs`).

use honba_core::uuid::Uuid;
use honba_core::{FillReport, MarketEvent, OrderEvent, OrderSide, Price, Quantity, RejectReason};
use honba_testing::fixtures::{any_instrument, flat_bar, utc_nanos};
use rust_decimal::Decimal;

use crate::{BuyAndHold, StrategyContext, StrategyRunner};

fn bar(r: &mut StrategyRunner<BuyAndHold>, ts: u64) {
    let bar = flat_bar("X", 10.0, ts);
    r.on_event(&MarketEvent::Bar(bar), utc_nanos(ts)).unwrap();
}

fn first_order_id() -> String {
    format!(
        "{}-0",
        crate::Strategy::name(&BuyAndHold::new(any_instrument(), Quantity::from_u64(1)))
    )
}

#[test]
fn an_engine_that_never_rejects_records_nothing() {
    let mut r = StrategyRunner::new(BuyAndHold::new(any_instrument(), Quantity::from_u64(1)));
    r.on_order_event(&OrderEvent::Cancelled {
        order_id: first_order_id(),
    })
    .unwrap();
    bar(&mut r, 1);
    let fill = FillReport::new(
        "F-0",
        first_order_id(),
        Uuid::nil(),
        any_instrument(),
        OrderSide::Buy,
        Price::new(Decimal::from(10)),
        Quantity::from_u64(1),
        "buy_and_hold",
        utc_nanos(1),
    );
    r.on_fill(&fill).unwrap();
    assert_eq!(r.fills().len(), 1);
    assert!(r.order_rejections().is_empty());
}

#[test]
fn a_rejection_is_recorded_and_releases_the_instrument() {
    let mut r = StrategyRunner::new(BuyAndHold::new(any_instrument(), Quantity::from_u64(1)));
    bar(&mut r, 1);
    r.on_order_event(&OrderEvent::Rejected {
        order_id: first_order_id(),
        reason: RejectReason::InsufficientMargin,
    })
    .unwrap();
    assert_eq!(r.order_rejections().len(), 1);
    assert!(r.order_rejections()[0].reason.contains("InsufficientMargin"));
    assert!(!r.context().busy(&any_instrument()));
}

#[test]
fn cancel_books_the_remainder_at_once() {
    let id = first_order_id();
    let mut r = StrategyRunner::new(BuyAndHold::new(any_instrument(), Quantity::from_u64(1)));
    bar(&mut r, 1);
    assert!(r.context().busy(&any_instrument()));
    assert!(r.order_rejections().is_empty());

    r.on_order_event(&OrderEvent::Cancelled { order_id: id })
        .unwrap();
    assert!(!r.context().busy(&any_instrument()));
    let got = &r.order_rejections()[0];
    assert_eq!(got.reason, "cancelled");
    assert_eq!(got.quantity, Quantity::from_u64(1));
}

#[test]
fn cancelling_twice_releases_once_and_a_later_bar_does_not_resurrect_pending() {
    let id = first_order_id();
    let mut r = StrategyRunner::new(BuyAndHold::new(any_instrument(), Quantity::from_u64(1)));
    bar(&mut r, 1);
    r.on_order_event(&OrderEvent::Cancelled {
        order_id: id.clone(),
    })
    .unwrap();
    r.on_order_event(&OrderEvent::Cancelled { order_id: id })
        .unwrap();
    bar(&mut r, 2);
    assert_eq!(r.order_rejections().len(), 1);
    assert!(!r.context().busy(&any_instrument()));
    assert!(r.fills().is_empty());
}
