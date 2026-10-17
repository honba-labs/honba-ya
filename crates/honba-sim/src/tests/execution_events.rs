//! Unit tests for the one ordered event drain of every engine in this crate (ADR 0019 (b)).

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::{Currency, ExecutionEvent, Money};
use honba_messages::{OrderEventKind as K, OrderId, OrderSide, UnixNanos, VenueOrderId};

use super::{any_instrument, bar_event, limit, market};
use crate::{BarFillEngine, Behavior, NextOpenSim, PaperExecution, ScriptedExecution, VenueAction};

fn t(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn kinds(events: &[ExecutionEvent]) -> Vec<K> {
    events.iter().map(|e| e.order_event().kind()).collect()
}

/// `(kind, quantity, cum_qty-or-released, complete)` of each event.
fn summary(events: &[ExecutionEvent]) -> Vec<(K, f64, bool)> {
    events
        .iter()
        .map(|e| match e {
            ExecutionEvent::Fill {
                trade,
                cum_qty,
                complete,
                ..
            } => {
                assert!(*cum_qty >= trade.quantity());
                (K::Fill, *cum_qty, *complete)
            }
            ExecutionEvent::Rejected { quantity, .. }
            | ExecutionEvent::Cancelled { quantity, .. }
            | ExecutionEvent::Expired { quantity, .. }
            | ExecutionEvent::Accepted { quantity, .. } => {
                (e.order_event().kind(), *quantity, false)
            }
            other => (other.order_event().kind(), 0.0, false),
        })
        .collect()
}

#[test]
fn bar_fill_emits_one_complete_fill_and_never_acks() {
    let mut exec = BarFillEngine::new();
    exec.on_event(&bar_event(101.0, 1), t(1)).unwrap();
    assert!(exec.native_events());
    exec.submit(market("O-1", OrderSide::Buy, 5.0, 1)).unwrap();
    let events = exec.drain_events().unwrap();
    assert_eq!(summary(&events), vec![(K::Fill, 5.0, true)]);
    assert!(exec.drain_events().unwrap().is_empty());
}

#[test]
fn paper_emits_one_complete_fill_and_never_acks() {
    let mut exec = PaperExecution::new(10.0);
    assert!(exec.native_events());
    exec.submit(market("O-1", OrderSide::Sell, 2.0, 1)).unwrap();
    assert_eq!(
        summary(&exec.drain_events().unwrap()),
        vec![(K::Fill, 2.0, true)]
    );
}

#[test]
fn scripted_events_follow_the_script_in_order() {
    let exec = ScriptedExecution::new(10.0)
        .with("p", Behavior::partial(4.0, "insufficient_funds"))
        .with("r", Behavior::reject("no_position"))
        .with("h", Behavior::Hold)
        .with("e", Behavior::Expire);
    let mut e = exec.clone();
    assert!(e.native_events());
    e.submit(market("f", OrderSide::Buy, 3.0, 1)).unwrap();
    e.submit(market("p", OrderSide::Buy, 10.0, 2)).unwrap();
    e.submit(market("r", OrderSide::Sell, 5.0, 3)).unwrap();
    e.submit(market("h", OrderSide::Buy, 7.0, 4)).unwrap();
    e.submit(market("e", OrderSide::Buy, 2.0, 5)).unwrap();
    exec.venue("h", VenueAction::Fill { quantity: 2.0 }, t(6))
        .unwrap();
    e.cancel("h", t(7)).unwrap();
    assert_eq!(
        summary(&e.drain_events().unwrap()),
        vec![
            (K::Fill, 3.0, true),
            (K::Fill, 4.0, false),
            (K::Rejected, 6.0, false),
            (K::Rejected, 5.0, false),
            (K::Expired, 2.0, false),
            (K::Fill, 2.0, false),
            (K::Cancelled, 5.0, false),
        ]
    );
}

#[test]
fn scripted_only_acks_when_the_script_says_so() {
    let exec = ScriptedExecution::new(10.0)
        .with("a", Behavior::Hold)
        .with("b", Behavior::Hold);
    let mut e = exec.clone();
    e.submit(market("a", OrderSide::Buy, 3.0, 1)).unwrap();
    e.submit(market("b", OrderSide::Buy, 3.0, 1)).unwrap();
    assert!(e.drain_events().unwrap().is_empty(), "L1: no ack");

    exec.venue(
        "a",
        VenueAction::Accept {
            venue_order_id: Some(VenueOrderId::new("V-9")),
        },
        t(2),
    )
    .unwrap();
    exec.venue("a", VenueAction::Fill { quantity: 1.0 }, t(3))
        .unwrap();
    exec.venue("b", VenueAction::Cancel, t(3)).unwrap();
    let events = e.drain_events().unwrap();
    assert_eq!(kinds(&events), vec![K::Accepted, K::Fill, K::Cancelled]);
    match &events[0] {
        ExecutionEvent::Accepted {
            venue_order_id,
            quantity,
            ts,
            ..
        } => {
            assert_eq!(venue_order_id, &Some(VenueOrderId::new("V-9")));
            assert_eq!((*quantity, ts.as_u64()), (3.0, 2));
        }
        other => panic!("not an ack: {other:?}"),
    }
    assert_eq!(exec.working_orders(), vec!["a".to_string()]);
    // A venue action on an order the venue does not hold is an error.
    assert!(exec.venue("b", VenueAction::Expire, t(4)).is_err());
    assert!(exec
        .venue("a", VenueAction::Fill { quantity: 5.0 }, t(4))
        .is_err());
}

#[test]
fn scripted_inject_passes_a_raw_event_through() {
    let exec = ScriptedExecution::new(10.0);
    let raw = ExecutionEvent::CancelRequested {
        order_id: OrderId::new("x"),
        ts: t(1),
    };
    exec.inject(raw.clone());
    assert_eq!(exec.clone().drain_events().unwrap(), vec![raw]);
}

#[test]
fn next_open_fills_before_it_rejects_the_remainder() {
    let mut s = NextOpenSim::new(Money::new(1_000_000, Currency::Inr)).unwrap();
    assert!(s.native_events());
    s.set_position(&any_instrument(), 3.0).unwrap();
    s.on_event(&bar_event(100.0, 1), t(1)).unwrap();
    s.submit(market("s", OrderSide::Sell, 5.0, 1)).unwrap();
    s.submit(limit("l", OrderSide::Buy, 1.0, 99.0, 1)).unwrap();
    s.submit(market("c", OrderSide::Buy, 1.0, 1)).unwrap();
    s.cancel("c", t(1)).unwrap();
    s.on_event(&bar_event(101.0, 2), t(2)).unwrap();
    let events = s.drain_events().unwrap();
    assert_eq!(
        summary(&events),
        vec![
            (K::Rejected, 1.0, false), // unsupported_order_type at submit
            (K::Cancelled, 1.0, false),
            (K::Fill, 3.0, false),
            (K::Rejected, 2.0, false), // no_position, after the fill
        ]
    );
    assert!(
        matches!(&events[3], ExecutionEvent::Rejected { reason, .. } if reason == "no_position")
    );
}

#[test]
fn the_legacy_pair_on_a_native_engine_is_the_buffered_split() {
    let mut s = NextOpenSim::new(Money::new(1_000_000, Currency::Inr)).unwrap();
    s.set_position(&any_instrument(), 3.0).unwrap();
    s.on_event(&bar_event(100.0, 1), t(1)).unwrap();
    s.submit(market("s", OrderSide::Sell, 5.0, 1)).unwrap();
    s.on_event(&bar_event(101.0, 2), t(2)).unwrap();
    // Draining fills first keeps the rejection for the rejection drain.
    let fills = s.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].quantity(), 3.0);
    let rejections = s.drain_rejections().unwrap();
    assert_eq!(rejections.len(), 1);
    assert_eq!(
        (rejections[0].quantity, rejections[0].reason.as_str()),
        (2.0, "no_position")
    );
    assert!(s.drain_fills().unwrap().is_empty() && s.drain_rejections().unwrap().is_empty());
}
