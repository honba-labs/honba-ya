//! `OrderLedger` rules that no route reaches directly: fill booking, exposure.

use honba_messages::{
    Exchange, InstrumentId, Order, OrderSide, OrderStatus, OrderType, TimeInForce, UnixNanos,
};

use crate::OrderLedger;

fn iid() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("NSE"))
}

/// A `Submitted` market order of `qty` for `X.NSE` recorded under `id`.
fn working(ledger: &OrderLedger, id: &str, side: OrderSide, qty: f64) {
    ledger.record(
        Order::new(
            honba_messages::OrderId::new(id),
            iid(),
            side,
            OrderType::Market,
            qty,
            None,
            TimeInForce::Day,
            UnixNanos::new(0),
            UnixNanos::new(0),
        )
        .with_status(OrderStatus::Submitted),
    );
}

#[test]
fn a_fill_with_a_non_finite_or_non_positive_price_or_quantity_is_dropped() {
    // `apply_fill` is public, so a venue adapter (or a future caller) can hand it values a
    // simulator never produces; the guard keeps them out of the row, which would otherwise
    // fail validation or serialization of `GET /orders`.
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, 10.0);
    for (qty, px, cum) in [
        (5.0, f64::NAN, 5.0),
        (5.0, f64::INFINITY, 5.0),
        (5.0, 0.0, 5.0),
        (5.0, -1.0, 5.0),
        (0.0, 100.0, 5.0),
        (-5.0, 100.0, 5.0),
        (f64::NAN, 100.0, 5.0),
        (5.0, 100.0, f64::NAN),
        (5.0, 100.0, f64::INFINITY),
    ] {
        assert!(ledger.apply_fill(
            "o1",
            qty,
            px,
            cum,
            UnixNanos::new(1),
            OrderStatus::PartiallyFilled
        ));
        let row = ledger.get("o1").unwrap();
        assert_eq!(row.status(), OrderStatus::Submitted, "{qty} {px} {cum}");
        assert_eq!(row.filled_qty(), 0.0, "{qty} {px} {cum}");
        assert_eq!(row.avg_fill_price(), None, "{qty} {px} {cum}");
        assert!(serde_json::to_value(&row).is_ok());
    }
}

fn refusal() -> honba_messages::RejectReason {
    honba_messages::RejectReason::new(
        honba_messages::ErrorCode::OrderRejected,
        "remainder refused",
    )
}

#[test]
fn completing_a_part_filled_row_clears_the_reject_reason_of_its_remainder() {
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, 10.0);
    assert!(ledger.apply_fill(
        "o1",
        4.0,
        100.0,
        4.0,
        UnixNanos::new(1),
        OrderStatus::PartiallyFilled
    ));
    assert!(ledger.reject("o1", refusal()));
    let row = ledger.get("o1").unwrap();
    assert_eq!(row.status(), OrderStatus::PartiallyFilled);
    assert!(row.reject_reason().is_some());

    // The venue then fills the rest: a completed order was not refused.
    assert!(ledger.apply_fill(
        "o1",
        6.0,
        101.0,
        10.0,
        UnixNanos::new(1),
        OrderStatus::Filled
    ));
    let row = ledger.get("o1").unwrap();
    assert_eq!(row.status(), OrderStatus::Filled);
    assert_eq!(row.reject_reason(), None);
    assert_eq!(row.filled_qty(), 10.0);
}

#[test]
fn a_reject_reason_survives_a_partial_that_is_then_cancelled() {
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, 10.0);
    ledger.apply_fill(
        "o1",
        4.0,
        100.0,
        4.0,
        UnixNanos::new(1),
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
    // The venue's cumulative quantity is the truth for how much has filled. Here its second
    // report says 10 cumulative although the two fill sizes add to 6: the 6 new units
    // (10 - 4) are priced at the latest fill price, so the average is (4*100 + 6*110) / 10.
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, 10.0);
    ledger.apply_fill(
        "o1",
        4.0,
        100.0,
        4.0,
        UnixNanos::new(1),
        OrderStatus::PartiallyFilled,
    );
    ledger.apply_fill(
        "o1",
        2.0,
        110.0,
        10.0,
        UnixNanos::new(1),
        OrderStatus::Filled,
    );
    let row = ledger.get("o1").unwrap();
    assert_eq!(row.filled_qty(), 10.0);
    assert!((row.avg_fill_price().unwrap() - 106.0).abs() < 1e-9);
}

#[test]
fn a_replayed_or_stale_fill_does_not_move_the_average() {
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, 10.0);
    ledger.apply_fill(
        "o1",
        4.0,
        100.0,
        4.0,
        UnixNanos::new(1),
        OrderStatus::PartiallyFilled,
    );
    // The same report again, and an older one (lower cumulative quantity).
    ledger.apply_fill(
        "o1",
        4.0,
        100.0,
        4.0,
        UnixNanos::new(1),
        OrderStatus::PartiallyFilled,
    );
    ledger.apply_fill(
        "o1",
        1.0,
        500.0,
        2.0,
        UnixNanos::new(1),
        OrderStatus::PartiallyFilled,
    );
    let row = ledger.get("o1").unwrap();
    assert_eq!(row.filled_qty(), 4.0);
    assert_eq!(row.avg_fill_price(), Some(100.0));
}

#[test]
fn a_first_report_larger_than_its_fill_prices_the_unseen_quantity_at_that_fill() {
    // The ledger missed earlier fills (cum 5 on the first report it sees): all 5 units are
    // booked at the only price known.
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, 10.0);
    ledger.apply_fill(
        "o1",
        2.0,
        100.0,
        5.0,
        UnixNanos::new(1),
        OrderStatus::PartiallyFilled,
    );
    let row = ledger.get("o1").unwrap();
    assert_eq!(row.filled_qty(), 5.0);
    assert_eq!(row.avg_fill_price(), Some(100.0));
}

fn position(ledger: &OrderLedger) -> f64 {
    ledger.position(&iid())
}

#[test]
fn exposure_is_the_filled_part_plus_whatever_is_still_working() {
    let ledger = OrderLedger::new();
    // Working and untouched: the whole quantity is exposure.
    working(&ledger, "w", OrderSide::Buy, 10.0);
    assert_eq!(position(&ledger), 10.0);
    // Partly filled and still working: filled 4 + 6 working = 10, not double counted.
    ledger.apply_fill(
        "w",
        4.0,
        100.0,
        4.0,
        UnixNanos::new(1),
        OrderStatus::PartiallyFilled,
    );
    assert_eq!(position(&ledger), 10.0);
    // Cancelled after the partial: only the 4 that filled remain.
    ledger.set_status("w", OrderStatus::Cancelled);
    assert_eq!(position(&ledger), 4.0);
}

#[test]
fn a_part_filled_row_whose_remainder_was_refused_counts_only_what_filled() {
    let ledger = OrderLedger::new();
    working(&ledger, "w", OrderSide::Sell, 10.0);
    ledger.apply_fill(
        "w",
        3.0,
        100.0,
        3.0,
        UnixNanos::new(1),
        OrderStatus::PartiallyFilled,
    );
    ledger.reject("w", refusal());
    assert_eq!(position(&ledger), -3.0);
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
        working(&ledger, id, OrderSide::Buy, 10.0);
        ledger.set_status(id, status);
    }
    assert_eq!(position(&ledger), 0.0);
}

#[test]
fn an_expired_partial_keeps_its_filled_part_and_a_filled_row_counts_in_full() {
    let ledger = OrderLedger::new();
    working(&ledger, "e", OrderSide::Buy, 10.0);
    ledger.apply_fill(
        "e",
        2.0,
        100.0,
        2.0,
        UnixNanos::new(1),
        OrderStatus::PartiallyFilled,
    );
    ledger.set_status("e", OrderStatus::Expired);
    // A seeded `Filled` row carries no fill fields yet still counts at its quantity.
    ledger.record(
        Order::new(
            honba_messages::OrderId::new("f"),
            iid(),
            OrderSide::Sell,
            OrderType::Market,
            5.0,
            None,
            TimeInForce::Day,
            UnixNanos::new(0),
            UnixNanos::new(0),
        )
        .with_status(OrderStatus::Filled),
    );
    assert_eq!(position(&ledger), 2.0 - 5.0);
    assert_eq!(ledger.positions(), vec![(iid(), -3.0)]);
}

#[test]
fn filled_at_is_the_latest_fill_time_and_ignores_stale_reports() {
    let ledger = OrderLedger::new();
    working(&ledger, "o1", OrderSide::Buy, 10.0);
    assert_eq!(ledger.get("o1").unwrap().filled_at(), None);
    ledger.apply_fill(
        "o1",
        4.0,
        100.0,
        4.0,
        UnixNanos::new(50),
        OrderStatus::PartiallyFilled,
    );
    assert_eq!(
        ledger.get("o1").unwrap().filled_at(),
        Some(UnixNanos::new(50))
    );
    // A replay (no growth in cum_qty) does not move it, even with a later timestamp.
    ledger.apply_fill(
        "o1",
        4.0,
        100.0,
        4.0,
        UnixNanos::new(60),
        OrderStatus::PartiallyFilled,
    );
    assert_eq!(
        ledger.get("o1").unwrap().filled_at(),
        Some(UnixNanos::new(50))
    );
    ledger.apply_fill(
        "o1",
        6.0,
        100.0,
        10.0,
        UnixNanos::new(70),
        OrderStatus::Filled,
    );
    assert_eq!(
        ledger.get("o1").unwrap().filled_at(),
        Some(UnixNanos::new(70))
    );
}

#[test]
fn replacing_a_refused_row_drops_its_replayable_refusal() {
    let ledger = OrderLedger::new();
    let detail =
        honba_messages::ErrorDetail::new(honba_messages::ErrorCode::RiskTradingHalted, "x");
    let refused = Order::new(
        honba_messages::OrderId::new("k"),
        iid(),
        OrderSide::Buy,
        OrderType::Market,
        10.0,
        None,
        TimeInForce::Day,
        UnixNanos::new(0),
        UnixNanos::new(0),
    );
    ledger.record_refusal(refused, detail.clone());
    assert_eq!(ledger.refusal("k"), Some(detail));
    working(&ledger, "k", OrderSide::Buy, 10.0);
    assert_eq!(ledger.refusal("k"), None);
}
