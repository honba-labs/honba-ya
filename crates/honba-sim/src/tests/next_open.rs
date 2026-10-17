//! Unit tests for `crate::next_open`.

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::{Currency, Money};
use honba_messages::{Event, OrderSide, OrderType, UnixNanos};

use super::{any_instrument, bar_event, limit, market};
use crate::NextOpenSim;

fn inr(rupees: i64) -> Money {
    Money::new(rupees * 100, Currency::Inr)
}

fn sim(cash: i64) -> NextOpenSim {
    NextOpenSim::new(inr(cash)).unwrap()
}

fn feed(sim: &mut NextOpenSim, open: f64, ts: u64) {
    let Event::Bar(b) = bar_event(open, ts) else {
        unreachable!()
    };
    sim.on_bar(&b).unwrap();
}

#[test]
fn negative_cash_is_refused() {
    assert!(NextOpenSim::new(Money::new(-1, Currency::Inr)).is_err());
}

#[test]
fn an_order_fills_at_the_next_bar_open() {
    let mut s = sim(10_000);
    feed(&mut s, 100.0, 1);
    s.submit(market("O-1", OrderSide::Buy, 10.0, 1)).unwrap();
    assert!(s.drain_fills().unwrap().is_empty());
    feed(&mut s, 110.0, 2);
    let fills = s.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(
        (
            fills[0].price(),
            fills[0].quantity(),
            fills[0].ts_event().as_u64()
        ),
        (110.0, 10.0, 2)
    );
    assert_eq!(s.cash(), inr(10_000 - 1_100));
    assert_eq!(s.position(&any_instrument()), 10.0);
}

#[test]
fn the_handler_path_drives_the_same_sessions() {
    let mut s = sim(10_000);
    s.on_event(&bar_event(100.0, 1), UnixNanos::from_u64(1))
        .unwrap();
    s.submit(market("O-1", OrderSide::Buy, 1.0, 1)).unwrap();
    s.on_event(&bar_event(101.0, 2), UnixNanos::from_u64(2))
        .unwrap();
    assert_eq!(s.drain_fills().unwrap().len(), 1);
}

#[test]
fn a_limit_order_is_rejected_unsupported_at_its_submit_time() {
    let mut s = sim(10_000);
    s.submit(limit("O-1", OrderSide::Buy, 3.0, 99.0, 7))
        .unwrap();
    let r = s.drain_rejections().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(
        (r[0].reason.as_str(), r[0].quantity, r[0].ts.as_u64()),
        ("unsupported_order_type", 3.0, 7)
    );
    assert!(s.working_orders().is_empty());
}

#[test]
fn a_trailing_stop_is_rejected_unsupported_not_treated_as_a_stop() {
    let mut s = sim(10_000);
    let o = crate::tests::order(
        "O-1",
        OrderSide::Sell,
        OrderType::TrailingStop,
        3.0,
        None,
        7,
    )
    .with_trail_amount(5.0);
    s.submit(o).unwrap();
    let r = s.drain_rejections().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].reason.as_str(), "unsupported_order_type");
    assert!(s.working_orders().is_empty());
}

#[test]
fn an_order_without_a_side_is_an_error() {
    let mut s = sim(10_000);
    let o = crate::tests::order(
        "O-1",
        OrderSide::NoOrderSide,
        OrderType::Market,
        1.0,
        None,
        1,
    );
    assert!(s.submit(o).is_err());
}

#[test]
fn a_duplicate_working_id_is_an_error_and_a_finished_id_is_free() {
    let mut s = sim(10_000);
    s.submit(market("O-1", OrderSide::Buy, 1.0, 1)).unwrap();
    assert!(s.submit(market("O-1", OrderSide::Buy, 2.0, 1)).is_err());
    feed(&mut s, 10.0, 1);
    s.submit(market("O-2", OrderSide::Buy, 1.0, 1)).unwrap();
}

#[test]
fn cancel_is_stamped_with_the_time_of_the_cancel() {
    let mut s = sim(10_000);
    s.submit(market("O-1", OrderSide::Buy, 3.0, 2)).unwrap();
    s.cancel("O-1", UnixNanos::from_u64(9)).unwrap();
    s.cancel("O-1", UnixNanos::from_u64(10)).unwrap(); // already gone: no-op
    s.cancel("unknown", UnixNanos::from_u64(10)).unwrap();
    let r = s.drain_rejections().unwrap();
    assert_eq!(r.len(), 1);
    assert!(r[0].is_cancelled());
    assert_eq!((r[0].quantity, r[0].ts.as_u64()), (3.0, 9));
}

#[test]
fn bars_must_not_go_back_or_repeat_an_instrument() {
    let mut s = sim(10_000);
    feed(&mut s, 10.0, 5);
    let Event::Bar(earlier) = bar_event(10.0, 4) else {
        unreachable!()
    };
    assert!(s.on_bar(&earlier).is_err());
    let Event::Bar(again) = bar_event(11.0, 5) else {
        unreachable!()
    };
    assert!(s.on_bar(&again).is_err());
    // state unchanged: the session still accepts a later bar
    feed(&mut s, 12.0, 6);
}

#[test]
fn open_session_must_advance() {
    let mut s = sim(10_000);
    s.open_session(UnixNanos::from_u64(3), &[]).unwrap();
    assert!(s.open_session(UnixNanos::from_u64(3), &[]).is_err());
    assert!(s.open_session(UnixNanos::from_u64(2), &[]).is_err());
}

#[test]
fn lot_size_must_be_positive_and_finite() {
    let mut s = sim(10);
    for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        assert!(s.set_lot_size(&any_instrument(), bad).is_err());
    }
    assert!(s.set_lot_size(&any_instrument(), 5.0).is_ok());
}

#[test]
fn set_position_seeds_a_holding_a_sell_can_use() {
    let mut s = sim(0);
    s.set_position(&any_instrument(), 4.0).unwrap();
    assert_eq!(s.position(&any_instrument()), 4.0);
    feed(&mut s, 10.0, 1);
    s.submit(market("S-1", OrderSide::Sell, 6.0, 1)).unwrap();
    feed(&mut s, 10.0, 2);
    let fills = s.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].quantity(), 4.0);
    assert_eq!(s.cash(), inr(40));
    assert!(s.positions().is_empty());
}

#[test]
fn set_position_replaces_in_place_and_flat_or_non_finite_values_are_handled() {
    let mut s = sim(0);
    let a = any_instrument();
    s.set_position(&a, 4.0).unwrap();
    s.set_position(&a, 9.0).unwrap();
    assert_eq!(s.positions(), vec![(a.clone(), 9.0)]);
    s.set_position(&a, 0.0).unwrap();
    assert!(s.positions().is_empty());
    assert!(s.set_position(&a, f64::NAN).is_err());
    assert!(s.set_position(&a, f64::INFINITY).is_err());
    assert!(s.positions().is_empty());
}

#[test]
fn session_ts_is_none_until_a_session_opens_then_follows_it() {
    let mut s = sim(100);
    assert_eq!(s.session_ts(), None);
    feed(&mut s, 10.0, 5);
    assert_eq!(s.session_ts(), Some(5));
    feed(&mut s, 10.0, 9);
    assert_eq!(s.session_ts(), Some(9));
}
