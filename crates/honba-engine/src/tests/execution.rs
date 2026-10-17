//! Unit tests for `crate::execution`.

use honba_entities::Trade;
use honba_messages::{OrderId, OrderSide, UnixNanos};

use super::any_instrument;

use crate::{ExecutionEngine, OrderRejection, Result};

/// An engine written before the rejection queue: only the required methods.
struct Legacy;

impl ExecutionEngine for Legacy {
    fn submit(&mut self, _order: honba_messages::Order) -> Result<()> {
        Ok(())
    }
    fn cancel(&mut self, _order_id: &str, _now: UnixNanos) -> Result<()> {
        Ok(())
    }
    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }
}

#[test]
fn engines_without_a_rejection_queue_report_none() {
    assert!(Legacy.drain_rejections().unwrap().is_empty());
}

#[test]
fn rejection_carries_the_unfilled_remainder() {
    let r = OrderRejection::rejected(
        OrderId::new("s-0"),
        any_instrument(),
        OrderSide::Buy,
        6.0,
        "insufficient_funds",
        UnixNanos::from_u64(7),
    );
    assert_eq!(r.order_id.as_str(), "s-0");
    assert_eq!(r.quantity, 6.0);
    assert_eq!(r.reason, "insufficient_funds");
    assert_eq!(r.ts.as_u64(), 7);
    assert!(!r.is_cancelled());
}

#[test]
fn cancellation_is_a_rejection_with_the_cancelled_flag() {
    let r = OrderRejection::cancelled(
        OrderId::new("s-1"),
        any_instrument(),
        OrderSide::Sell,
        3.0,
        UnixNanos::from_u64(9),
    );
    assert!(r.is_cancelled());
    assert_eq!(r.reason, OrderRejection::CANCELLED);
    assert_eq!(r.reason, "cancelled");
    assert_eq!(r.side, OrderSide::Sell);
}

#[test]
fn cancelled_and_reason_cannot_disagree() {
    let cancelled = OrderRejection::cancelled(
        OrderId::new("s-1"),
        any_instrument(),
        OrderSide::Buy,
        1.0,
        UnixNanos::from_u64(1),
    );
    assert!(cancelled.is_cancelled());

    // Even built field by field, the kind is read from the reason: there is no
    // second flag to contradict it.
    let mut r = cancelled.clone();
    r.reason = "no_position".to_string();
    assert!(!r.is_cancelled());
    r.reason = OrderRejection::CANCELLED.to_string();
    assert!(r.is_cancelled());

    let rejected = OrderRejection::rejected(
        OrderId::new("s-2"),
        any_instrument(),
        OrderSide::Buy,
        1.0,
        "insufficient_funds",
        UnixNanos::from_u64(1),
    );
    assert!(!rejected.is_cancelled());
}

// ---- ADR 0019 (b): the one event drain and its legacy shims ----

use honba_entities::{Currency, ExecutionEvent};
use honba_messages::{Exchange, InstrumentId, Order, OrderType, TimeInForce, VenueOrderId};

use crate::{LegacyDrains, LegacyPortEvents};

fn t(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn trade(id: &str, qty: f64) -> Trade {
    Trade::new(
        OrderId::new(id),
        any_instrument(),
        OrderSide::Buy,
        qty,
        10.0,
        Currency::Inr,
        t(1),
        t(1),
    )
}

fn fill(id: &str, qty: f64, cum: f64, complete: bool) -> ExecutionEvent {
    ExecutionEvent::Fill {
        trade: trade(id, qty),
        cum_qty: cum,
        complete,
        venue_order_id: None,
    }
}

fn released(kind: &str, id: &str, qty: f64) -> ExecutionEvent {
    let (order_id, instrument_id, side, quantity, ts) = (
        OrderId::new(id),
        any_instrument(),
        OrderSide::Buy,
        qty,
        t(2),
    );
    match kind {
        "rejected" => ExecutionEvent::Rejected {
            order_id,
            instrument_id,
            side,
            quantity,
            reason: "no_position".into(),
            venue_order_id: None,
            ts,
        },
        "cancelled" => ExecutionEvent::Cancelled {
            order_id,
            instrument_id,
            side,
            quantity,
            venue_order_id: None,
            ts,
        },
        _ => ExecutionEvent::Expired {
            order_id,
            instrument_id,
            side,
            quantity,
            venue_order_id: None,
            ts,
        },
    }
}

#[test]
fn buffered_shim_loses_no_event() {
    let mut shim = LegacyDrains::new();
    shim.absorb(vec![
        ExecutionEvent::Submitted {
            order_id: OrderId::new("a"),
            instrument_id: any_instrument(),
            side: OrderSide::Buy,
            quantity: 3.0,
            ts: t(1),
        },
        ExecutionEvent::Accepted {
            order_id: OrderId::new("a"),
            instrument_id: any_instrument(),
            side: OrderSide::Buy,
            quantity: 3.0,
            venue_order_id: Some(VenueOrderId::new("V-1")),
            ts: t(1),
        },
        fill("a", 1.0, 1.0, false),
        released("rejected", "a", 2.0),
        ExecutionEvent::CancelRequested {
            order_id: OrderId::new("b"),
            ts: t(2),
        },
        released("cancelled", "b", 4.0),
    ]);
    // The fill buffer is drained first; the rejection buffer keeps its items.
    let fills = shim.take_fills();
    assert_eq!(fills, vec![trade("a", 1.0)]);
    shim.absorb(vec![
        fill("c", 5.0, 5.0, true),
        released("expired", "d", 6.0),
    ]);
    let rejections = shim.take_rejections();
    assert_eq!(
        rejections
            .iter()
            .map(|r| (r.order_id.as_str(), r.quantity, r.reason.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("a", 2.0, "no_position"),
            ("b", 4.0, OrderRejection::CANCELLED),
            ("d", 6.0, OrderRejection::EXPIRED),
        ]
    );
    assert!(rejections[1].is_cancelled() && !rejections[2].is_cancelled());
    assert_eq!(shim.take_fills(), vec![trade("c", 5.0)]);
    assert!(shim.take_fills().is_empty() && shim.take_rejections().is_empty());
}

/// A legacy engine that fills `filled` of each order and rejects the rest.
struct LegacyPartial {
    filled: f64,
    fills: Vec<Trade>,
    rejections: Vec<OrderRejection>,
}

impl ExecutionEngine for LegacyPartial {
    fn submit(&mut self, order: Order) -> Result<()> {
        let id = order.order_id().as_str();
        let half = self.filled / 2.0;
        self.fills.push(trade(id, half));
        self.fills.push(trade(id, half));
        if order.quantity() <= self.filled {
            return Ok(());
        }
        self.rejections.push(OrderRejection::rejected(
            order.order_id().clone(),
            order.instrument_id().clone(),
            order.side(),
            order.quantity() - self.filled,
            "insufficient_funds",
            order.ts_event(),
        ));
        Ok(())
    }
    fn cancel(&mut self, _order_id: &str, _now: UnixNanos) -> Result<()> {
        Ok(())
    }
    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        Ok(std::mem::take(&mut self.fills))
    }
    fn drain_rejections(&mut self) -> Result<Vec<OrderRejection>> {
        Ok(std::mem::take(&mut self.rejections))
    }
}

fn buy(id: &str, qty: f64) -> Order {
    Order::new(
        OrderId::new(id),
        InstrumentId::new("X", Exchange::new("NSE")),
        OrderSide::Buy,
        OrderType::Market,
        qty,
        None,
        TimeInForce::Day,
        t(1),
        t(1),
    )
}

#[test]
fn legacy_engines_are_not_native_and_their_default_drain_is_fills_then_rejections() {
    let mut legacy = LegacyPartial {
        filled: 2.0,
        fills: Vec::new(),
        rejections: Vec::new(),
    };
    assert!(!legacy.native_events());
    legacy.submit(buy("o", 5.0)).unwrap();
    let kinds: Vec<_> = legacy
        .drain_events()
        .unwrap()
        .iter()
        .map(|e| e.order_event().kind())
        .collect();
    use honba_messages::OrderEventKind as K;
    assert_eq!(kinds, vec![K::Fill, K::Fill, K::Rejected]);
    assert!(legacy.drain_events().unwrap().is_empty());
}

#[test]
fn legacy_port_events_derive_partial_and_complete_fills_from_their_own_state() {
    let mut port = LegacyPortEvents::new(LegacyPartial {
        filled: 4.0,
        fills: Vec::new(),
        rejections: Vec::new(),
    });
    assert!(port.native_events());
    port.submit(buy("p", 10.0)).unwrap();
    port.submit(buy("q", 4.0)).unwrap();
    let got: Vec<(String, f64, bool)> = port
        .drain_events()
        .unwrap()
        .into_iter()
        .filter_map(|e| match e {
            ExecutionEvent::Fill {
                trade,
                cum_qty,
                complete,
                ..
            } => Some((trade.order_id().as_str().to_string(), cum_qty, complete)),
            _ => None,
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("p".to_string(), 2.0, false),
            ("p".to_string(), 4.0, false),
            ("q".to_string(), 2.0, false),
            ("q".to_string(), 4.0, true),
        ]
    );
}
