//! Unit tests for the runner's single ordered event walk (ADR 0019 decision 4).

use honba_core::uuid::Uuid;
use honba_core::{FillReport, MarketEvent, OrderEvent, OrderSide, Price, Quantity};
use honba_testing::fixtures::{flat_bar, utc_nanos};
use rust_decimal::Decimal;

use super::any_instrument;
use crate::{BuyAndHold, StrategyContext, StrategyRunner};

fn bar(r: &mut StrategyRunner<BuyAndHold>, ts: u64) {
    let b = flat_bar("X", 10.0, ts);
    r.on_event(&MarketEvent::Bar(b), utc_nanos(ts)).unwrap();
}

fn cancelled(id: &str) -> OrderEvent {
    OrderEvent::Cancelled {
        order_id: id.to_string(),
    }
}

fn held() -> (StrategyRunner<BuyAndHold>, String) {
    let strategy = BuyAndHold::new(any_instrument(), Quantity::from_u64(5));
    let id = format!("{}-0", crate::Strategy::name(&strategy));
    (StrategyRunner::new(strategy), id)
}

#[test]
fn a_duplicate_cancelled_event_releases_once() {
    let (mut r, id) = held();
    bar(&mut r, 1);
    r.on_order_event(&cancelled(&id)).unwrap();
    r.on_order_event(&cancelled(&id)).unwrap();
    bar(&mut r, 2);
    assert_eq!(r.order_rejections().len(), 1);
    assert_eq!(r.released_quantity(&any_instrument()), Decimal::from(5));
    assert!(!r.context().busy(&any_instrument()));
}

#[test]
fn a_cancel_after_the_order_filled_releases_nothing() {
    let strategy = BuyAndHold::new(any_instrument(), Quantity::from_u64(5));
    let mut r = StrategyRunner::new(strategy);
    bar(&mut r, 1);
    let fill = FillReport::new(
        "F-0",
        "buy_and_hold-0",
        Uuid::nil(),
        any_instrument(),
        OrderSide::Buy,
        Price::new(Decimal::from(10)),
        Quantity::from_u64(5),
        "buy_and_hold",
        utc_nanos(1),
    );
    r.on_fill(&fill).unwrap();
    r.on_order_event(&cancelled("buy_and_hold-0")).unwrap();
    assert!(r.order_rejections().is_empty());
    assert_eq!(r.released_quantity(&any_instrument()), Decimal::ZERO);
    assert!(!r.context().busy(&any_instrument()));
}
