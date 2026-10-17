//! End-to-end integration tests for startup and reconnect reconciliation (E2-S7).

use std::sync::{Arc, Mutex};

use honba_engine::{
    BrokerOrderReport, BrokerSnapshot, CacheQuery, Engine, EngineOutput, Handler, Result,
};
use honba_messages::{
    Event, Exchange, InstrumentId, OrderId, OrderSide, OrderStatus, UnixNanos, VenueOrderId,
};

fn ts(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn inst(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("NSE"))
}

#[derive(Default, Clone)]
struct RecordingHandler {
    events: Arc<Mutex<Vec<Event>>>,
}

impl Handler for RecordingHandler {
    fn on_event(&mut self, event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        self.events.lock().unwrap().push(event.clone());
        Ok(EngineOutput::None)
    }
}

#[test]
fn engine_reconcile_and_inject_catches_up_state_and_handler() {
    let mut engine = Engine::new();
    let handler = RecordingHandler::default();
    engine.add_handler(handler.clone());

    let instrument = inst("RELIANCE");

    // Seed an order ahead of reconnect (e.g. before connection dropped)
    let mut tracked = honba_engine::TrackedOrder {
        state: honba_messages::OrderState::new(),
        instrument_id: instrument.clone(),
        side: OrderSide::Buy,
        venue_order_id: Some(VenueOrderId::new("V-123")),
    };
    tracked.state.status = OrderStatus::Accepted;
    tracked.state.quantity = 10.0;
    engine.cache_mut().seed_order("ORD-1".to_string(), tracked);

    assert_eq!(engine.position(&instrument), 0.0);
    assert_eq!(engine.cache().open_orders().len(), 1);

    // On reconnect, broker reports order partially filled (6.0 at 2500.0)
    let snapshot = BrokerSnapshot::new()
        .with_order(BrokerOrderReport {
            order_id: Some(OrderId::new("ORD-1")),
            venue_order_id: VenueOrderId::new("V-123"),
            instrument_id: instrument.clone(),
            side: OrderSide::Buy,
            quantity: 10.0,
            filled_qty: 6.0,
            avg_price: Some(2500.0),
            status: OrderStatus::PartiallyFilled,
        })
        .with_position(instrument.clone(), 6.0);

    let report = engine.reconcile_and_inject(&snapshot);
    assert_eq!(report.missed_fills.len(), 1);
    assert_eq!(report.synthetic_events.len(), 1);

    // Pump the engine to dispatch the injected synthetic events
    while engine.pump().unwrap() {}

    // Handler observed the synthetic fill event
    let recorded = handler.events.lock().unwrap().clone();
    assert!(recorded.iter().any(|e| matches!(
        e,
        Event::OrderPartiallyFilled {
            order_id,
            last_qty,
            cum_qty,
            last_px,
            ..
        } if order_id.as_str() == "ORD-1" && *last_qty == 6.0 && *cum_qty == 6.0 && *last_px == 2500.0
    )));

    // Cache position caught up to 6.0
    assert_eq!(engine.position(&instrument), 6.0);
    assert_eq!(engine.cache().position(&instrument), 6.0);
}

#[test]
fn recorded_fixture_reconciliation_scenarios() {
    let fixture_json = r#"{
        "orders": [
            {
                "order_id": "ORD-MISSED",
                "venue_order_id": "V-1",
                "instrument": "TCS.NSE",
                "side": "buy",
                "quantity": 20.0,
                "filled_qty": 20.0,
                "avg_price": 3400.0,
                "status": "filled"
            },
            {
                "order_id": "ORD-STALE",
                "venue_order_id": "V-2",
                "instrument": "INFY.NSE",
                "side": "sell",
                "quantity": 10.0,
                "filled_qty": 0.0,
                "avg_price": null,
                "status": "cancelled"
            },
            {
                "order_id": null,
                "venue_order_id": "V-GHOST",
                "instrument": "SBIN.NSE",
                "side": "buy",
                "quantity": 100.0,
                "filled_qty": 0.0,
                "avg_price": null,
                "status": "accepted"
            }
        ],
        "positions": [
            { "instrument": "TCS.NSE", "quantity": 20.0 },
            { "instrument": "INFY.NSE", "quantity": 0.0 },
            { "instrument": "ITC.NSE", "quantity": 50.0 }
        ]
    }"#;

    let parsed: serde_json::Value = serde_json::from_str(fixture_json).unwrap();
    let mut snapshot = BrokerSnapshot::new();

    for o in parsed["orders"].as_array().unwrap() {
        let (sym, ex) = o["instrument"].as_str().unwrap().split_once('.').unwrap();
        let inst = InstrumentId::new(sym, Exchange::new(ex));
        let side = match o["side"].as_str().unwrap() {
            "buy" => OrderSide::Buy,
            _ => OrderSide::Sell,
        };
        let status = match o["status"].as_str().unwrap() {
            "filled" => OrderStatus::Filled,
            "cancelled" => OrderStatus::Cancelled,
            _ => OrderStatus::Accepted,
        };
        snapshot = snapshot.with_order(BrokerOrderReport {
            order_id: o["order_id"].as_str().map(OrderId::new),
            venue_order_id: VenueOrderId::new(o["venue_order_id"].as_str().unwrap()),
            instrument_id: inst,
            side,
            quantity: o["quantity"].as_f64().unwrap(),
            filled_qty: o["filled_qty"].as_f64().unwrap(),
            avg_price: o["avg_price"].as_f64(),
            status,
        });
    }

    for p in parsed["positions"].as_array().unwrap() {
        let (sym, ex) = p["instrument"].as_str().unwrap().split_once('.').unwrap();
        let inst = InstrumentId::new(sym, Exchange::new(ex));
        snapshot = snapshot.with_position(inst, p["quantity"].as_f64().unwrap());
    }

    // Cache has ORD-MISSED with 0 filled
    let tcs = inst("TCS");
    let mut tracked_tcs = honba_engine::TrackedOrder {
        state: honba_messages::OrderState::new(),
        instrument_id: tcs.clone(),
        side: OrderSide::Buy,
        venue_order_id: Some(VenueOrderId::new("V-1")),
    };
    tracked_tcs.state.status = OrderStatus::Accepted;
    tracked_tcs.state.quantity = 20.0;

    // Cache has ORD-STALE in working Accepted status
    let infy = inst("INFY");
    let mut tracked_infy = honba_engine::TrackedOrder {
        state: honba_messages::OrderState::new(),
        instrument_id: infy.clone(),
        side: OrderSide::Sell,
        venue_order_id: Some(VenueOrderId::new("V-2")),
    };
    tracked_infy.state.status = OrderStatus::Accepted;
    tracked_infy.state.quantity = 10.0;

    let mut cache = honba_engine::StateCache::new();
    cache.seed_order("ORD-MISSED".to_string(), tracked_tcs);
    cache.seed_order("ORD-STALE".to_string(), tracked_infy);

    let report = honba_engine::Reconciler::reconcile(&cache, &snapshot, ts(100));

    // 1. Missed fill on TCS
    assert_eq!(report.missed_fills.len(), 1);
    assert_eq!(report.missed_fills[0].order_id.as_str(), "ORD-MISSED");
    assert_eq!(report.missed_fills[0].quantity, 20.0);
    assert!(report.missed_fills[0].completes_order);

    // 2. Stale order on INFY
    assert_eq!(report.stale_orders.len(), 1);
    assert_eq!(report.stale_orders[0].order_id.as_str(), "ORD-STALE");

    // 3. Ghost order on SBIN
    assert_eq!(report.ghost_orders.len(), 1);
    assert_eq!(report.ghost_orders[0].venue_order_id.as_str(), "V-GHOST");

    // 4. Position drift on ITC (broker has 50, cache has 0)
    assert_eq!(report.position_drifts.len(), 1);
    assert_eq!(report.position_drifts[0].instrument_id, inst("ITC"));
    assert_eq!(report.position_drifts[0].drift, 50.0);
}
