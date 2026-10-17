//! Unit tests for the `SessionOpen` path of `crate::next_open` (Python `_from_open` leniency).

use honba_engine::ExecutionEngine;
use honba_entities::{Currency, Money};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Exchange, InstrumentId, OrderSide, PriceType,
    UnixNanos,
};

use super::market_for;
use crate::NextOpenSim;

fn sim() -> NextOpenSim {
    NextOpenSim::new(Money::new(1_000_000, Currency::Inr)).unwrap()
}

fn iid(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("TEST"))
}

fn bar(symbol: &str, ts: u64, open: f64) -> Bar {
    let bt = BarType::new(
        iid(symbol),
        BarSpecification::new(1, BarAggregation::Day, PriceType::Last),
    );
    let t = UnixNanos::from_u64(ts);
    Bar::new(bt, open, open, open, open, 1.0, t, t)
}

fn t(ts: u64) -> UnixNanos {
    UnixNanos::from_u64(ts)
}

#[test]
fn a_session_open_fills_eligible_orders_at_its_bars() {
    let mut s = sim();
    s.on_session_open(t(1), &[bar("A", 1, 10.0)]).unwrap();
    s.submit(market_for("O", "A", OrderSide::Buy, 3.0, 1))
        .unwrap();
    s.on_session_open(t(2), &[bar("A", 2, 11.0)]).unwrap();
    let f = s.drain_fills().unwrap();
    assert_eq!((f.len(), f[0].price()), (1, 11.0));
}

#[test]
fn bars_after_a_session_open_never_open_or_order_sessions() {
    let mut s = sim();
    s.on_session_open(t(10), &[bar("A", 1, 10.0)]).unwrap();
    s.submit(market_for("O", "A", OrderSide::Buy, 1.0, 1))
        .unwrap();
    // later ts: ignored (would open a session without the SessionOpen)
    s.on_bar(&bar("A", 20, 12.0)).unwrap();
    assert!(s.drain_fills().unwrap().is_empty());
    // earlier ts: ignored, no non-monotonic error
    s.on_bar(&bar("A", 5, 12.0)).unwrap();
    // an opened instrument at the session ts: ignored, no duplicate error
    s.on_bar(&bar("A", 10, 12.0)).unwrap();
    assert_eq!(s.working_orders(), vec!["O".to_string()]);
}

#[test]
fn a_bar_at_the_session_ts_opens_an_instrument_the_open_left_out() {
    let mut s = sim();
    s.on_session_open(t(1), &[bar("A", 1, 10.0)]).unwrap();
    s.submit(market_for("O", "B", OrderSide::Buy, 2.0, 1))
        .unwrap();
    s.on_session_open(t(2), &[bar("A", 2, 10.0)]).unwrap();
    s.on_bar(&bar("B", 2, 7.0)).unwrap();
    let f = s.drain_fills().unwrap();
    assert_eq!(
        (f.len(), f[0].price(), f[0].ts_event().as_u64()),
        (1, 7.0, 2)
    );
}

#[test]
fn a_session_open_must_advance_and_leaves_the_mode_alone_on_error() {
    let mut s = sim();
    s.open_session(t(3), &[bar("A", 3, 1.0)]).unwrap(); // not a SessionOpen: bars still strict
    assert!(s.on_session_open(t(3), &[]).is_err());
    assert!(s.on_bar(&bar("A", 2, 1.0)).is_err()); // still the strict (non-SessionOpen) path
}

#[test]
fn open_session_alone_keeps_the_strict_bar_rules() {
    let mut s = sim();
    s.open_session(t(3), &[bar("A", 3, 1.0)]).unwrap();
    assert!(s.on_bar(&bar("A", 3, 2.0)).is_err());
}

#[test]
fn a_failed_session_open_does_not_enter_the_lenient_mode() {
    let mut s = sim().with_costs(Box::new(|_, _, _| Ok(Money::new(-1, Currency::Inr))));
    s.open_session(t(1), &[bar("A", 1, 10.0)]).unwrap();
    s.submit(market_for("O", "A", OrderSide::Buy, 1.0, 1))
        .unwrap();
    assert!(s.on_session_open(t(2), &[bar("A", 2, 10.0)]).is_err());
    // the session did advance (as in the reference) but the strict path stays active
    assert!(s.on_bar(&bar("A", 1, 10.0)).is_err());
}
