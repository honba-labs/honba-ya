//! Unit tests for reconciliation on startup/reconnect (E2-S7).

use honba_messages::{
    Event, Exchange, InstrumentId, OrderId, OrderSide, OrderState, OrderStatus, UnixNanos,
    VenueOrderId,
};

use crate::cache::{CacheQuery, StateCache, TrackedOrder};
use crate::reconciliation::{BrokerOrderReport, BrokerSnapshot, Reconciler};

fn ts(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn inst(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("NSE"))
}

#[test]
fn missed_fill_detected() {
    let mut cache = StateCache::new();
    let instrument = inst("RELIANCE");
    let mut tracked = TrackedOrder {
        state: OrderState::new(),
        instrument_id: instrument.clone(),
        side: OrderSide::Buy,
        venue_order_id: Some(VenueOrderId::new("V-100")),
    };
    tracked.state.status = OrderStatus::Accepted;
    tracked.state.quantity = 10.0;
    cache.seed_order("O-1".to_string(), tracked);

    // Broker reports order with 6.0 filled at 2500.0
    let snapshot = BrokerSnapshot::new()
        .with_order(BrokerOrderReport {
            order_id: Some(OrderId::new("O-1")),
            venue_order_id: VenueOrderId::new("V-100"),
            instrument_id: instrument.clone(),
            side: OrderSide::Buy,
            quantity: 10.0,
            filled_qty: 6.0,
            avg_price: Some(2500.0),
            status: OrderStatus::PartiallyFilled,
        })
        .with_position(instrument.clone(), 6.0);

    let report = Reconciler::reconcile(&cache, &snapshot, ts(100));

    assert_eq!(report.missed_fills.len(), 1);
    let fill = &report.missed_fills[0];
    assert_eq!(fill.order_id.as_str(), "O-1");
    assert_eq!(fill.quantity, 6.0);
    assert_eq!(fill.cum_qty, 6.0);
    assert_eq!(fill.price, 2500.0);
    assert!(!fill.completes_order);

    assert_eq!(report.synthetic_events.len(), 1);
    assert!(matches!(
        &report.synthetic_events[0],
        Event::OrderPartiallyFilled {
            order_id,
            last_qty,
            cum_qty,
            last_px,
            ..
        } if order_id.as_str() == "O-1" && *last_qty == 6.0 && *cum_qty == 6.0 && *last_px == 2500.0
    ));

    // After applying missed fills, there should be zero position drift
    assert_eq!(report.position_drifts.len(), 0);

    // Apply to cache and check state
    report.apply_to_cache(&mut cache);
    assert_eq!(cache.position(&instrument), 6.0);
    let updated = cache.order("O-1").unwrap();
    assert_eq!(updated.state.status, OrderStatus::PartiallyFilled);
    assert_eq!(updated.state.filled_qty, 6.0);
}

#[test]
fn missed_fill_completes_order() {
    let mut cache = StateCache::new();
    let instrument = inst("TCS");
    let mut tracked = TrackedOrder {
        state: OrderState::new(),
        instrument_id: instrument.clone(),
        side: OrderSide::Buy,
        venue_order_id: Some(VenueOrderId::new("V-200")),
    };
    tracked.state.status = OrderStatus::PartiallyFilled;
    tracked.state.quantity = 10.0;
    tracked.state.filled_qty = 4.0;
    cache.seed_position(instrument.clone(), 4.0);
    cache.seed_order("O-2".to_string(), tracked);

    // Broker reports fully filled at 3500.0
    let snapshot = BrokerSnapshot::new()
        .with_order(BrokerOrderReport {
            order_id: Some(OrderId::new("O-2")),
            venue_order_id: VenueOrderId::new("V-200"),
            instrument_id: instrument.clone(),
            side: OrderSide::Buy,
            quantity: 10.0,
            filled_qty: 10.0,
            avg_price: Some(3500.0),
            status: OrderStatus::Filled,
        })
        .with_position(instrument.clone(), 10.0);

    let report = Reconciler::reconcile(&cache, &snapshot, ts(200));

    assert_eq!(report.missed_fills.len(), 1);
    let fill = &report.missed_fills[0];
    assert_eq!(fill.quantity, 6.0); // 10.0 - 4.0
    assert!(fill.completes_order);

    assert_eq!(report.synthetic_events.len(), 1);
    assert!(matches!(
        &report.synthetic_events[0],
        Event::OrderFilled {
            order_id,
            last_qty,
            last_px,
            ..
        } if order_id.as_str() == "O-2" && *last_qty == 6.0 && *last_px == 3500.0
    ));

    report.apply_to_cache(&mut cache);
    assert_eq!(cache.position(&instrument), 10.0);
    assert_eq!(
        cache.order("O-2").unwrap().state.status,
        OrderStatus::Filled
    );
    assert_eq!(cache.open_orders().len(), 0);
}

#[test]
fn ghost_order_detected() {
    let cache = StateCache::new();
    let instrument = inst("INFY");

    // Broker has an order that engine knows nothing about
    let snapshot = BrokerSnapshot::new().with_order(BrokerOrderReport {
        order_id: None,
        venue_order_id: VenueOrderId::new("V-GHOST-1"),
        instrument_id: instrument.clone(),
        side: OrderSide::Sell,
        quantity: 50.0,
        filled_qty: 0.0,
        avg_price: None,
        status: OrderStatus::Accepted,
    });

    let report = Reconciler::reconcile(&cache, &snapshot, ts(300));
    assert_eq!(report.ghost_orders.len(), 1);
    let ghost = &report.ghost_orders[0];
    assert_eq!(ghost.venue_order_id.as_str(), "V-GHOST-1");
    assert_eq!(ghost.instrument_id, instrument);
    assert_eq!(ghost.quantity, 50.0);
    assert_eq!(ghost.side, OrderSide::Sell);
}

#[test]
fn position_drift_detected() {
    let mut cache = StateCache::new();
    let instrument = inst("SBIN");
    cache.seed_position(instrument.clone(), 20.0);

    // Broker reports 25.0 held without any pending fills
    let snapshot = BrokerSnapshot::new().with_position(instrument.clone(), 25.0);

    let report = Reconciler::reconcile(&cache, &snapshot, ts(400));
    assert!(report.has_drift());
    assert_eq!(report.position_drifts.len(), 1);
    let drift = &report.position_drifts[0];
    assert_eq!(drift.instrument_id, instrument);
    assert_eq!(drift.cache_quantity, 20.0);
    assert_eq!(drift.broker_quantity, 25.0);
    assert_eq!(drift.drift, 5.0);
}

#[test]
fn stale_open_orders_cancelled_and_dropped() {
    let mut cache = StateCache::new();
    let inst1 = inst("WIPRO");
    let inst2 = inst("HDFCBANK");

    // O-10: open in cache, cancelled at broker
    let mut t1 = TrackedOrder {
        state: OrderState::new(),
        instrument_id: inst1.clone(),
        side: OrderSide::Buy,
        venue_order_id: Some(VenueOrderId::new("V-10")),
    };
    t1.state.status = OrderStatus::Accepted;
    t1.state.quantity = 100.0;
    cache.seed_order("O-10".to_string(), t1);

    // O-20: open in cache, completely omitted from broker (dropped)
    let mut t2 = TrackedOrder {
        state: OrderState::new(),
        instrument_id: inst2.clone(),
        side: OrderSide::Buy,
        venue_order_id: Some(VenueOrderId::new("V-20")),
    };
    t2.state.status = OrderStatus::Submitted;
    t2.state.quantity = 50.0;
    cache.seed_order("O-20".to_string(), t2);

    let snapshot = BrokerSnapshot::new().with_order(BrokerOrderReport {
        order_id: Some(OrderId::new("O-10")),
        venue_order_id: VenueOrderId::new("V-10"),
        instrument_id: inst1,
        side: OrderSide::Buy,
        quantity: 100.0,
        filled_qty: 0.0,
        avg_price: None,
        status: OrderStatus::Cancelled,
    });

    let report = Reconciler::reconcile(&cache, &snapshot, ts(500));
    assert_eq!(report.stale_orders.len(), 2);

    // Both generate synthetic cancel events
    assert_eq!(report.synthetic_events.len(), 2);
    assert!(report.synthetic_events.iter().any(
        |e| matches!(e, Event::OrderCancelled { order_id, .. } if order_id.as_str() == "O-10")
    ));
    assert!(report.synthetic_events.iter().any(
        |e| matches!(e, Event::OrderCancelled { order_id, .. } if order_id.as_str() == "O-20")
    ));

    report.apply_to_cache(&mut cache);
    assert_eq!(cache.open_orders().len(), 0);
    assert_eq!(
        cache.order("O-10").unwrap().state.status,
        OrderStatus::Cancelled
    );
    assert_eq!(
        cache.order("O-20").unwrap().state.status,
        OrderStatus::Cancelled
    );
}
