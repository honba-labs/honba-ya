//! Unit tests for `crate::margin`: margin queries and MIS square-off markers.

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::{InstrumentKind, PositionSide};
use honba_market::settlement::PercentageMarginModel;
use honba_market::NullMarketProfile;
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId, Order, OrderId,
    OrderSide, OrderType, PriceType, Product, TimeInForce, UnixNanos,
};

use crate::margin::{default_session_close_ts, is_intraday, MarginQuery, MisPositions};
use crate::BarFillEngine;

fn iid(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("TEST"))
}

fn t(ts: u64) -> UnixNanos {
    UnixNanos::from_u64(ts)
}

fn bar(symbol: &str, ts: u64, close: f64) -> Bar {
    let bt = BarType::new(
        iid(symbol),
        BarSpecification::new(1, BarAggregation::Day, PriceType::Last),
    );
    Bar::new(bt, close, close, close, close, 1.0, t(ts), t(ts))
}

fn order(id: &str, symbol: &str, side: OrderSide, qty: f64, product: Product) -> Order {
    Order::new(
        OrderId::new(id),
        iid(symbol),
        side,
        OrderType::Market,
        qty,
        None,
        TimeInForce::Day,
        t(1),
        t(1),
    )
    .with_product(product)
}

#[test]
fn is_intraday_recognises_only_the_intraday_product() {
    assert!(is_intraday(Some(Product::Intraday)));
    assert!(!is_intraday(Some(Product::Delivery)));
    assert!(!is_intraday(Some(Product::Carry)));
    assert!(!is_intraday(None));
}

#[test]
fn default_session_close_is_2359_utc_of_the_reference_day() {
    // 23:59:00 in nanoseconds since the epoch.
    let close = 86_340_000_000_000;
    assert_eq!(default_session_close_ts(t(0)).as_u64(), close);
    // Just after the close, the next session's close is one day later.
    assert_eq!(
        default_session_close_ts(UnixNanos::from_u64(close + 1)).as_u64(),
        close + 86_400_000_000_000
    );
}

#[test]
fn margin_query_reads_initial_and_maintenance_from_a_model() {
    let model = PercentageMarginModel::new(0.20, 0.15);
    let query = MarginQuery::new(&model);
    assert_eq!(
        query.initial_margin(InstrumentKind::Equity, PositionSide::Long, 1_000.0),
        200.0
    );
    assert_eq!(
        query.maintenance_margin(InstrumentKind::Equity, PositionSide::Long, 1_000.0),
        150.0
    );
}

#[test]
fn margin_query_reads_the_active_market_profile() {
    let profile = NullMarketProfile::default();
    let query = MarginQuery::from_profile(&profile);
    assert_eq!(
        query.initial_margin(InstrumentKind::Equity, PositionSide::Long, 1_000.0),
        0.0
    );
}

#[test]
fn mis_positions_marks_intraday_and_clears_when_flat() {
    let mut mis = MisPositions::new();
    mis.apply(&iid("A"), Some(Product::Intraday), 5.0);
    mis.apply(&iid("B"), Some(Product::Delivery), 3.0);
    assert!(mis.is_intraday(&iid("A")));
    assert!(!mis.is_intraday(&iid("B")));
    mis.apply(&iid("A"), Some(Product::Intraday), 0.0);
    assert!(!mis.is_intraday(&iid("A")));
    assert!(mis.intraday().is_empty());
}

#[test]
fn square_off_at_session_end_closes_mis_but_not_cnc() {
    let mut exec = BarFillEngine::new();
    exec.on_event(&Event::Bar(bar("A", 1, 100.0)), t(1))
        .unwrap();
    exec.submit(order("MIS-1", "A", OrderSide::Buy, 5.0, Product::Intraday))
        .unwrap();
    exec.submit(order("CNC-1", "B", OrderSide::Buy, 3.0, Product::Delivery))
        .unwrap();
    assert_eq!(exec.drain_fills().unwrap().len(), 2);
    assert_eq!(exec.position(&iid("A")), 5.0);
    assert_eq!(exec.position(&iid("B")), 3.0);

    exec.on_session_close(t(2)).unwrap();

    // MIS is flattened at the session close, CNC is held overnight.
    assert_eq!(exec.position(&iid("A")), 0.0);
    assert_eq!(exec.position(&iid("B")), 3.0);
    let fills = exec.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].instrument_id(), &iid("A"));
    assert_eq!(fills[0].side(), OrderSide::Sell);
    assert_eq!((fills[0].quantity(), fills[0].price()), (5.0, 100.0));
}

#[test]
fn session_close_without_auto_square_off_refuses_a_held_mis_position() {
    let mut exec = BarFillEngine::new().with_auto_square_off_mis(false);
    exec.on_event(&Event::Bar(bar("A", 1, 100.0)), t(1))
        .unwrap();
    exec.submit(order("MIS-1", "A", OrderSide::Buy, 5.0, Product::Intraday))
        .unwrap();
    exec.drain_fills().unwrap();
    assert!(exec.on_session_close(t(2)).is_err());
    assert_eq!(exec.position(&iid("A")), 5.0);
}
