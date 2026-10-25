//! `OrderLedger` rules that no route reaches directly: fill booking, exposure.

use chrono::{DateTime, Utc};
use honba_api::{ErrorCode, ErrorDetail};
use honba_core::rust_decimal::Decimal;
use honba_core::{Exchange, InstrumentId, OrderSide, TimeInForce};
use rust_decimal_macros::dec;

use crate::OrderLedger;
use crate::orders::{Order, OrderStatus, OrderType, RejectReason};

fn iid() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "X")
}

/// A `Submitted` market order of `qty` for `X.NSE` recorded under `id`.
fn working(ledger: &OrderLedger, id: &str, side: OrderSide, qty: Decimal) {
    ledger.record(
        Order::new(
            id,
            iid(),
            side,
            OrderType::Market,
            qty,
            None,
            TimeInForce::Day,
            Utc::now(),
        )
        .with_status(OrderStatus::Submitted),
    );
}

#[test]
fn a_fill_with_a_non_finite_or_non_positive_price_or_quantity_is_dropped() {
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, dec!(10));
    for (qty, px, cum) in [
        (dec!(5), dec!(0), dec!(5)),
        (dec!(5), dec!(-1), dec!(5)),
        (dec!(0), dec!(100), dec!(5)),
        (dec!(-5), dec!(100), dec!(5)),
    ] {
        assert!(ledger.apply_fill("o1", qty, px, cum, Utc::now(), OrderStatus::PartiallyFilled));
        let row = ledger.get("o1").unwrap();
        assert_eq!(row.status(), OrderStatus::Submitted, "{qty} {px} {cum}");
        assert_eq!(row.filled_qty(), dec!(0), "{qty} {px} {cum}");
        assert_eq!(row.avg_fill_price(), None, "{qty} {px} {cum}");
        assert!(serde_json::to_value(&row).is_ok());
    }
}

fn refusal() -> RejectReason {
    RejectReason::new(ErrorCode::ValidationInvalidRequest, "remainder refused")
}

#[test]
fn completing_a_part_filled_row_clears_the_reject_reason_of_its_remainder() {
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, dec!(10));
    assert!(ledger.apply_fill(
        "o1",
        dec!(4),
        dec!(100),
        dec!(4),
        Utc::now(),
        OrderStatus::PartiallyFilled
    ));
    assert!(ledger.reject("o1", refusal()));
    let row = ledger.get("o1").unwrap();
    assert_eq!(row.status(), OrderStatus::PartiallyFilled);
    assert!(row.reject_reason().is_some());

    // The venue then fills the rest: a completed order was not refused.
    assert!(ledger.apply_fill(
        "o1",
        dec!(6),
        dec!(101),
        dec!(10),
        Utc::now(),
        OrderStatus::Filled
    ));
    let row = ledger.get("o1").unwrap();
    assert_eq!(row.status(), OrderStatus::Filled);
    assert_eq!(row.reject_reason(), None);
    assert_eq!(row.filled_qty(), dec!(10));
}

#[test]
fn a_reject_reason_survives_a_partial_that_is_then_cancelled() {
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, dec!(10));
    ledger.apply_fill(
        "o1",
        dec!(4),
        dec!(100),
        dec!(4),
        Utc::now(),
        OrderStatus::PartiallyFilled,
    );
    ledger.reject("o1", refusal());
    ledger.set_status("o1", OrderStatus::Cancelled);
    let row = ledger.get("o1").unwrap();
    assert_eq!(row.status(), OrderStatus::Cancelled);
    assert_eq!(row.reject_reason(), Some(&refusal()));
}

#[test]
fn the_average_follows_cum_qty_deltas_when_the_venue_disagrees_with_fill_sizes() {
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, dec!(10));
    ledger.apply_fill(
        "o1",
        dec!(4),
        dec!(100),
        dec!(4),
        Utc::now(),
        OrderStatus::PartiallyFilled,
    );
    ledger.apply_fill(
        "o1",
        dec!(2),
        dec!(110),
        dec!(10),
        Utc::now(),
        OrderStatus::Filled,
    );
    let row = ledger.get("o1").unwrap();
    assert_eq!(row.filled_qty(), dec!(10));
    assert_eq!(row.avg_fill_price(), Some(dec!(106)));
}

#[test]
fn a_replayed_or_stale_fill_does_not_move_the_average() {
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, dec!(10));
    ledger.apply_fill(
        "o1",
        dec!(4),
        dec!(100),
        dec!(4),
        Utc::now(),
        OrderStatus::PartiallyFilled,
    );
    // The same report again, and an older one (lower cumulative quantity).
    ledger.apply_fill(
        "o1",
        dec!(4),
        dec!(100),
        dec!(4),
        Utc::now(),
        OrderStatus::PartiallyFilled,
    );
    ledger.apply_fill(
        "o1",
        dec!(1),
        dec!(500),
        dec!(2),
        Utc::now(),
        OrderStatus::PartiallyFilled,
    );
    let row = ledger.get("o1").unwrap();
    assert_eq!(row.filled_qty(), dec!(4));
    assert_eq!(row.avg_fill_price(), Some(dec!(100)));
}

#[test]
fn a_first_report_larger_than_its_fill_prices_the_unseen_quantity_at_that_fill() {
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, dec!(10));
    ledger.apply_fill(
        "o1",
        dec!(2),
        dec!(100),
        dec!(5),
        Utc::now(),
        OrderStatus::PartiallyFilled,
    );
    let row = ledger.get("o1").unwrap();
    assert_eq!(row.filled_qty(), dec!(5));
    assert_eq!(row.avg_fill_price(), Some(dec!(100)));
}

fn position(ledger: &OrderLedger) -> Decimal {
    ledger.position(&iid())
}

#[test]
fn exposure_is_the_filled_part_plus_whatever_is_still_working() {
    let ledger = OrderLedger::new();
    working(&ledger, "w", OrderSide::Buy, dec!(10));
    assert_eq!(position(&ledger), dec!(10));
    ledger.apply_fill(
        "w",
        dec!(4),
        dec!(100),
        dec!(4),
        Utc::now(),
        OrderStatus::PartiallyFilled,
    );
    assert_eq!(position(&ledger), dec!(10));
    ledger.set_status("w", OrderStatus::Cancelled);
    assert_eq!(position(&ledger), dec!(4));
}

#[test]
fn a_part_filled_row_whose_remainder_was_refused_counts_only_what_filled() {
    let ledger = OrderLedger::new();
    working(&ledger, "w", OrderSide::Sell, dec!(10));
    ledger.apply_fill(
        "w",
        dec!(3),
        dec!(100),
        dec!(3),
        Utc::now(),
        OrderStatus::PartiallyFilled,
    );
    ledger.reject("w", refusal());
    assert_eq!(position(&ledger), dec!(-3));
}

#[test]
fn a_row_that_never_filled_adds_nothing_once_it_is_not_working() {
    let ledger = OrderLedger::new();
    for (id, status) in [
        ("c", OrderStatus::Cancelled),
        ("r", OrderStatus::Rejected),
        ("e", OrderStatus::Expired),
        ("i", OrderStatus::Initialized),
    ] {
        working(&ledger, id, OrderSide::Buy, dec!(10));
        ledger.set_status(id, status);
    }
    assert_eq!(position(&ledger), dec!(0));
}

#[test]
fn an_expired_partial_keeps_its_filled_part_and_a_filled_row_counts_in_full() {
    let ledger = OrderLedger::new();
    working(&ledger, "e", OrderSide::Buy, dec!(10));
    ledger.apply_fill(
        "e",
        dec!(2),
        dec!(100),
        dec!(2),
        Utc::now(),
        OrderStatus::PartiallyFilled,
    );
    ledger.set_status("e", OrderStatus::Expired);
    // A seeded `Filled` row carries no fill fields yet still counts at its quantity.
    ledger.record(
        Order::new(
            "f",
            iid(),
            OrderSide::Sell,
            OrderType::Market,
            dec!(5),
            None,
            TimeInForce::Day,
            Utc::now(),
        )
        .with_status(OrderStatus::Filled),
    );
    assert_eq!(position(&ledger), dec!(-3));
    assert_eq!(ledger.positions(), vec![(iid(), dec!(-3))]);
}

#[test]
fn filled_at_is_the_latest_fill_time_and_ignores_stale_reports() {
    let ledger = OrderLedger::new();
    let t50 = DateTime::from_timestamp(50, 0).unwrap();
    let t60 = DateTime::from_timestamp(60, 0).unwrap();
    let t70 = DateTime::from_timestamp(70, 0).unwrap();
    working(&ledger, "o1", OrderSide::Buy, dec!(10));
    assert_eq!(ledger.get("o1").unwrap().filled_at(), None);
    ledger.apply_fill(
        "o1",
        dec!(4),
        dec!(100),
        dec!(4),
        t50,
        OrderStatus::PartiallyFilled,
    );
    assert_eq!(ledger.get("o1").unwrap().filled_at(), Some(t50));
    // A replay (no growth in cum_qty) does not move it, even with a later timestamp.
    ledger.apply_fill(
        "o1",
        dec!(4),
        dec!(100),
        dec!(4),
        t60,
        OrderStatus::PartiallyFilled,
    );
    assert_eq!(ledger.get("o1").unwrap().filled_at(), Some(t50));
    ledger.apply_fill("o1", dec!(6), dec!(100), dec!(10), t70, OrderStatus::Filled);
    assert_eq!(ledger.get("o1").unwrap().filled_at(), Some(t70));
}

#[test]
fn replacing_a_refused_row_drops_its_replayable_refusal() {
    let ledger = OrderLedger::new();
    let detail = ErrorDetail::new(ErrorCode::RiskMaxNotionalExceeded, "x");
    let refused = Order::new(
        "k",
        iid(),
        OrderSide::Buy,
        OrderType::Market,
        dec!(10),
        None,
        TimeInForce::Day,
        Utc::now(),
    );
    ledger.record_refusal(refused, detail.clone());
    assert_eq!(ledger.refusal("k"), Some(detail));
    working(&ledger, "k", OrderSide::Buy, dec!(10));
    assert_eq!(ledger.refusal("k"), None);
}
