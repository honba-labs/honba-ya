//! Unit tests for the settlement cycle of `crate::next_open` (ADR 0016, chunk 2).

use honba_engine::ExecutionEngine;
use honba_entities::{Currency, Money};
use honba_messages::{Event, OrderSide};

use super::{any_instrument, bar_event, market};
use crate::NextOpenSim;

fn inr(rupees: i64) -> Money {
    Money::new(rupees * 100, Currency::Inr)
}

fn sim(cash: i64, days: i64) -> NextOpenSim {
    NextOpenSim::new(inr(cash))
        .unwrap()
        .with_settlement_days(days)
        .unwrap()
}

fn feed(sim: &mut NextOpenSim, open: f64, ts: u64) {
    let Event::Bar(b) = bar_event(open, ts) else {
        unreachable!()
    };
    sim.on_bar(&b).unwrap();
}

/// Holds 10 units bought at 100 in session 1 (cash 1000 of 2000 left), then sells them at the
/// session-3 open (`ts` 3); the proceeds are due `days` sessions later.
fn sold_ten(days: i64) -> NextOpenSim {
    let mut s = sim(2_000, days);
    feed(&mut s, 100.0, 1);
    s.submit(market("B", OrderSide::Buy, 10.0, 1)).unwrap();
    feed(&mut s, 100.0, 2);
    s.submit(market("S", OrderSide::Sell, 10.0, 2)).unwrap();
    feed(&mut s, 100.0, 3);
    s
}

#[test]
fn settlement_defaults_to_same_session_availability() {
    let s = NextOpenSim::new(inr(100)).unwrap();
    assert_eq!(s.settlement_days(), 0);
    assert_eq!(s.unsettled(), inr(0));
    assert_eq!(s.available_cash(), inr(100));
    assert!(s.receivables().is_empty());
}

#[test]
fn negative_settlement_days_are_refused() {
    assert!(NextOpenSim::new(inr(1))
        .unwrap()
        .with_settlement_days(-1)
        .is_err());
    let mut s = NextOpenSim::new(inr(1)).unwrap();
    assert!(s.set_settlement_days(-1).is_err());
    assert_eq!(s.settlement_days(), 0);
}

#[test]
fn proceeds_are_booked_at_once_but_available_after_the_cycle() {
    let mut s = sold_ten(1);
    assert_eq!(s.cash(), inr(2_000));
    assert_eq!(s.unsettled(), inr(1_000));
    assert_eq!(s.available_cash(), inr(1_000));
    assert_eq!(s.receivables(), vec![(3, inr(1_000))]);
    feed(&mut s, 100.0, 4); // session index 3 = due
    assert_eq!(s.unsettled(), inr(0));
    assert_eq!(s.available_cash(), inr(2_000));
    assert!(s.receivables().is_empty());
}

#[test]
fn t_plus_two_keeps_proceeds_pending_one_session_longer() {
    let mut s = sold_ten(2);
    feed(&mut s, 100.0, 4);
    assert_eq!(s.unsettled(), inr(1_000));
    feed(&mut s, 100.0, 5);
    assert_eq!(s.unsettled(), inr(0));
}

#[test]
fn t_plus_zero_proceeds_are_available_immediately() {
    let s = sold_ten(0);
    assert_eq!(s.unsettled(), inr(0));
    assert_eq!(s.available_cash(), inr(2_000));
}

#[test]
fn a_buy_that_cash_cannot_cover_waits_for_pending_proceeds() {
    let mut s = sold_ten(1);
    s.submit(market("W", OrderSide::Buy, 15.0, 3)).unwrap();
    feed(&mut s, 100.0, 4); // proceeds settle at this open: the wait is over, in full
    let fills = s.drain_fills().unwrap();
    assert_eq!(fills.len(), 3);
    let w = fills.iter().find(|f| f.order_id().as_str() == "W").unwrap();
    assert_eq!(w.quantity(), 15.0);
    assert!(s.drain_rejections().unwrap().is_empty());
    assert_eq!(s.cash(), inr(500));
}

#[test]
fn a_waiting_buy_stays_working_and_is_cut_once_the_wait_is_over() {
    let mut s = sold_ten(2);
    // order considered at session index 3 (first_try) while 1000 are pending until index 4
    s.submit(market("W", OrderSide::Buy, 15.0, 3)).unwrap();
    feed(&mut s, 100.0, 4);
    assert_eq!(s.working_orders(), vec!["W".to_string()]);
    assert!(s.drain_rejections().unwrap().is_empty());
    feed(&mut s, 100.0, 5);
    assert!(s.working_orders().is_empty());
    let r = s.drain_fills().unwrap();
    assert_eq!(r.last().unwrap().quantity(), 15.0);
}

#[test]
fn a_buy_is_cut_when_the_wait_is_over_and_cash_still_falls_short() {
    let mut s = sold_ten(1);
    s.submit(market("W", OrderSide::Buy, 25.0, 3)).unwrap();
    feed(&mut s, 100.0, 4); // available 2000 -> only 20 fit
    let rej = s.drain_rejections().unwrap();
    assert_eq!(rej.len(), 1);
    assert_eq!(
        (rej[0].reason.as_str(), rej[0].quantity),
        ("insufficient_funds", 5.0)
    );
    assert_eq!(s.position(&any_instrument()), 20.0);
}

#[test]
fn no_pending_proceeds_means_no_waiting() {
    let mut s = sim(1_000, 3);
    feed(&mut s, 100.0, 1);
    s.submit(market("W", OrderSide::Buy, 20.0, 1)).unwrap();
    feed(&mut s, 100.0, 2);
    assert!(s.working_orders().is_empty());
    let rej = s.drain_rejections().unwrap();
    assert_eq!(
        (rej[0].reason.as_str(), rej[0].quantity),
        ("insufficient_funds", 10.0)
    );
}

#[test]
fn settlement_days_can_only_change_before_the_first_session() {
    let mut s = sim(100, 0);
    s.set_settlement_days(2).unwrap();
    assert_eq!(s.settlement_days(), 2);
    feed(&mut s, 10.0, 1);
    assert!(s.set_settlement_days(1).is_err());
    assert_eq!(s.settlement_days(), 2);
}
