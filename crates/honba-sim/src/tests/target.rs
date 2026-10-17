//! Unit tests for `crate::target`: target-position deltas and per-instrument serialisation.

use honba_entities::{Currency, Instrument, InstrumentKind, Position, PositionSide};
use honba_market::{InstrumentRules, NullMarketProfile};
use honba_messages::{Exchange, InstrumentId, OrderSide, UnixNanos};

use crate::target::{PerInstrumentLock, SmartOrderController, TargetPosition};

fn iid(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("NSE"))
}

fn t(ts: u64) -> UnixNanos {
    UnixNanos::from_u64(ts)
}

/// A net position in `instrument`: `+` long, `-` short, `0` flat.
fn held(instrument: &InstrumentId, qty: f64) -> Position {
    let mut position = Position::flat(instrument.clone(), Currency::Inr);
    if qty > 0.0 {
        position.apply_fill(PositionSide::Long, qty, 100.0);
    } else if qty < 0.0 {
        position.apply_fill(PositionSide::Short, -qty, 100.0);
    }
    position
}

#[test]
fn target_delta_computed() {
    let id = iid("NIFTY");
    let flat = held(&id, 0.0);
    let sizing = TargetPosition::new().with_lot_size(75.0).unwrap();

    // From flat, target 100 floors to one 75 lot: buy 75.
    let order = sizing
        .order(&flat, 100.0, "tp-1", None, t(1))
        .unwrap()
        .unwrap();
    assert_eq!(order.instrument_id(), &id);
    assert_eq!(order.order_id().as_str(), "tp-1");
    assert_eq!(order.side(), OrderSide::Buy);
    assert_eq!(order.quantity(), 75.0);

    // The pure delta agrees, and a long 150 -> 0 sells two lots.
    assert_eq!(sizing.delta(&flat, 100.0), 75.0);
    assert_eq!(sizing.delta(&held(&id, 150.0), 0.0), -150.0);
    let sell = sizing
        .order(&held(&id, 150.0), 0.0, "tp-2", None, t(2))
        .unwrap()
        .unwrap();
    assert_eq!(sell.side(), OrderSide::Sell);
    assert_eq!(sell.quantity(), 150.0);

    // Already at target: no order.
    assert!(sizing
        .order(&held(&id, 75.0), 75.0, "tp-3", None, t(3))
        .unwrap()
        .is_none());

    // A contradicting side hint is refused, never silently flipped.
    assert!(sizing
        .order(&flat, 100.0, "tp-4", Some(OrderSide::Sell), t(4))
        .is_err());
}

#[test]
fn target_delta_reads_lot_from_a_market_profile() {
    let id = iid("BANKNIFTY");
    let instrument = Instrument::new(id.clone(), InstrumentKind::Index, Currency::Inr, 75.0, 0.05);
    let sizing = TargetPosition::from_profile(&NullMarketProfile::default(), &instrument);
    assert_eq!(sizing.lot_size(), 75.0);
    assert_eq!(sizing.delta(&held(&id, 0.0), 100.0), 75.0);
}

#[test]
fn target_delta_caps_at_the_freeze_without_splitting() {
    let id = iid("NIFTY");
    let rules = InstrumentRules::new(75.0, 0.05).with_max_quantity(100.0);
    let sizing = TargetPosition::from_rules(&rules);
    assert_eq!(sizing.max_order_quantity(), Some(100.0));
    // A target of 300 would need four clips; the splitter (E3-S6) is deferred, so one clip.
    assert_eq!(sizing.delta(&held(&id, 0.0), 300.0), 75.0);
}

#[test]
fn invalid_lot_size_is_refused() {
    assert!(TargetPosition::new().with_lot_size(0.0).is_err());
    assert!(TargetPosition::new().with_lot_size(f64::NAN).is_err());
}

#[test]
fn per_instrument_lock_serialises_acquire_and_release() {
    let id = iid("A");
    let mut lock = PerInstrumentLock::new();
    assert!(lock.acquire(&id));
    assert!(!lock.acquire(&id));
    assert!(lock.is_locked(&id));
    assert_eq!(lock.locked(), vec![id.clone()]);
    lock.release(&id);
    assert!(!lock.is_locked(&id));
    assert!(lock.acquire(&id));
    lock.clear();
    assert!(lock.locked().is_empty());
}

#[test]
fn controller_defers_while_an_order_is_outstanding() {
    let id = iid("A");
    let flat = held(&id, 0.0);
    let mut controller = SmartOrderController::new();

    let first = controller
        .submit_target_position(&flat, 100.0, None, t(1))
        .unwrap()
        .expect("the first target emits an order");
    assert_eq!(first.side(), OrderSide::Buy);
    assert_eq!(first.quantity(), 100.0);
    assert!(controller.is_busy(&id));

    // A second target while the first is working is deferred, not doubled.
    assert!(controller
        .submit_target_position(&flat, 100.0, None, t(2))
        .unwrap()
        .is_none());

    // Releasing frees the instrument for the next target.
    controller.release(&id);
    let second = controller
        .submit_target_position(&held(&id, 100.0), 0.0, None, t(3))
        .unwrap()
        .expect("after release the flattening target emits an order");
    assert_eq!(second.side(), OrderSide::Sell);
    assert_eq!(second.quantity(), 100.0);
}
