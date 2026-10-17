//! Cross-language vectors for the order-state machine (ADR 0019 decision 8).
//!
//! Reads `schema/conformance/order_state.json` and replays scenarios through each
//! gateway the core ships (`ScriptedExecution`, `BarFillEngine`, `PaperExecution`,
//! `NextOpenSim`), under their supported profiles (`l1`, `ack`), comparing the
//! normalised stream per profile (decision 8).

use std::collections::HashMap;
use std::path::PathBuf;

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::{Currency, ExecutionEvent, Money};
use honba_messages::{
    Exchange, InstrumentId, OrderId, OrderSide, OrderState, OrderStatus, UnixNanos,
};
use honba_sim::{
    BarFillEngine, Behavior, NextOpenSim, PaperExecution, ScriptedExecution, VenueAction,
};
use serde_json::{json, Value};

const EPS: f64 = 1e-9;

fn fixture() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../schema/conformance/order_state.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn side(s: &str) -> OrderSide {
    match s {
        "buy" => OrderSide::Buy,
        "sell" => OrderSide::Sell,
        other => panic!("unknown side {other}"),
    }
}

fn status_name(s: &OrderStatus) -> &'static str {
    match s {
        OrderStatus::Initialized => "initialized",
        OrderStatus::Submitted => "submitted",
        OrderStatus::Accepted => "accepted",
        OrderStatus::PartiallyFilled => "partially_filled",
        OrderStatus::Filled => "filled",
        OrderStatus::Cancelled => "cancelled",
        OrderStatus::Rejected => "rejected",
        OrderStatus::Expired => "expired",
        _ => "other",
    }
}

struct TrackedOrder {
    state: OrderState,
    _instrument: InstrumentId,
    _side: OrderSide,
}

#[derive(Default)]
struct StateHarness {
    orders: HashMap<String, TrackedOrder>,
    events: Vec<Value>,
    drain: Vec<ExecutionEvent>,
}

impl StateHarness {
    fn step(&mut self, step: &Value, engine: &mut dyn ExecutionEngine) {
        let oid = step["order_id"].as_str().unwrap().to_string();
        let ts = UnixNanos::from_u64(step["ts"].as_u64().unwrap());
        match step["op"].as_str().unwrap() {
            "submit" => {
                let instrument =
                    InstrumentId::new(step["symbol"].as_str().unwrap(), Exchange::new("NSE"));
                let side = side(step["side"].as_str().unwrap());
                let quantity = step["quantity"].as_f64().unwrap();
                let order_type = match step.get("order_type").and_then(|v| v.as_str()) {
                    Some("limit") => honba_messages::OrderType::Limit,
                    _ => honba_messages::OrderType::Market,
                };
                let price = step.get("price").and_then(|v| v.as_f64());
                let mut tracked = TrackedOrder {
                    state: OrderState::new(),
                    _instrument: instrument.clone(),
                    _side: side,
                };
                let sub_ev = ExecutionEvent::Submitted {
                    order_id: OrderId::new(&oid),
                    instrument_id: instrument.clone(),
                    side,
                    quantity,
                    ts,
                };
                let changed = tracked.state.apply(&sub_ev.order_event()).unwrap();
                if changed {
                    let projected = project(&sub_ev, &tracked.state, quantity);
                    self.drain.push(sub_ev);
                    self.events.push(projected);
                }
                self.orders.insert(oid.clone(), tracked);
                let order = honba_messages::Order::new(
                    OrderId::new(&oid),
                    instrument,
                    side,
                    order_type,
                    quantity,
                    price,
                    honba_messages::TimeInForce::Day,
                    ts,
                    ts,
                );
                engine.submit(order).unwrap();
                self.drain(engine);
            }
            "cancel" => {
                let Some(o) = self.orders.get_mut(&oid) else {
                    return;
                };
                let working = matches!(
                    o.state.status,
                    OrderStatus::Submitted | OrderStatus::Accepted | OrderStatus::PartiallyFilled
                );
                if !working || o.state.cancel_requested {
                    return;
                }
                let cancel_ev = ExecutionEvent::CancelRequested {
                    order_id: OrderId::new(&oid),
                    ts,
                };
                let changed = o.state.apply(&cancel_ev.order_event()).unwrap();
                if changed {
                    let released = o.state.quantity - o.state.filled_qty;
                    let projected = project(&cancel_ev, &o.state, released);
                    self.drain.push(cancel_ev);
                    self.events.push(projected);
                }
                engine.cancel(&oid, ts).unwrap();
                self.drain(engine);
            }
            "venue" => {
                // Handled directly where applicable
            }
            other => panic!("unsupported op at the state level: {other}"),
        }
    }

    fn drain(&mut self, engine: &mut dyn ExecutionEngine) {
        let events = engine.drain_events().unwrap();
        for ev in events {
            if let Some(tracked) = self.orders.get_mut(ev.order_id().as_str()) {
                let released = tracked.state.quantity - tracked.state.filled_qty;
                let order_event = ev.order_event();
                match tracked.state.apply(&order_event) {
                    Err(err) => panic!("unexpected {err}"),
                    Ok(changed) => {
                        if changed {
                            let projected = project(&ev, &tracked.state, released);
                            self.drain.push(ev);
                            self.events.push(projected);
                        }
                    }
                }
            }
        }
    }
}

fn project(ev: &ExecutionEvent, st: &OrderState, released: f64) -> Value {
    let oid = ev.order_id().as_str();
    match ev {
        ExecutionEvent::Submitted { .. } => {
            json!({"type": "order", "order_id": oid, "status": "submitted"})
        }
        ExecutionEvent::Accepted { .. } => json!({"type": "order_accepted", "order_id": oid}),
        ExecutionEvent::CancelRequested { .. } => {
            json!({"type": "order_cancel_requested", "order_id": oid})
        }
        ExecutionEvent::Fill { trade, .. } if st.status == OrderStatus::Filled => {
            json!({"type": "order_filled", "order_id": oid, "last_qty": trade.quantity()})
        }
        ExecutionEvent::Fill { trade, .. } => json!({
            "type": "order_partially_filled", "order_id": oid,
            "last_qty": trade.quantity(), "cum_qty": st.filled_qty
        }),
        ExecutionEvent::Rejected { .. } => {
            json!({"type": "order_rejected", "order_id": oid, "quantity": released})
        }
        ExecutionEvent::Cancelled { .. } => {
            json!({"type": "order_cancelled", "order_id": oid, "quantity": released})
        }
        ExecutionEvent::Expired { .. } => {
            json!({"type": "order_expired", "order_id": oid, "quantity": released})
        }
        _ => panic!("unhandled ExecutionEvent variant"),
    }
}

fn matches(actual: &Value, expected: &Value) -> bool {
    expected
        .as_object()
        .unwrap()
        .iter()
        .all(|(k, want)| match (actual.get(k), want) {
            (Some(got), Value::Number(_)) => {
                (got.as_f64().unwrap() - want.as_f64().unwrap()).abs() <= EPS
            }
            (Some(got), _) => got == want,
            (None, _) => false,
        })
}

fn normalise(events: &[Value], profile: &str) -> Vec<Value> {
    events
        .iter()
        .filter(|e| !(profile == "l1" && e["type"] == "order_accepted"))
        .cloned()
        .collect()
}

#[test]
fn bar_fill_engine_replays_bar_realisable_scenarios_under_l1_profile() {
    let doc = fixture();
    let scenarios: Vec<&Value> = doc["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| {
            s.get("bar_realisable").and_then(|v| v.as_bool()) == Some(true)
                && s["profile"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|p| p.as_str() == Some("l1"))
        })
        .collect();

    assert_eq!(scenarios.len(), 2);

    for scenario in scenarios {
        let name = scenario["name"].as_str().unwrap();
        let mut engine = BarFillEngine::new();
        let bt = honba_messages::BarType::new(
            InstrumentId::new("X", Exchange::new("NSE")),
            honba_messages::BarSpecification::new(
                1,
                honba_messages::BarAggregation::Minute,
                honba_messages::PriceType::Last,
            ),
        );
        let t0 = UnixNanos::from_u64(1);
        let bar = honba_messages::Bar::new(bt, 10.0, 10.0, 10.0, 10.0, 1000.0, t0, t0);
        engine
            .on_event(&honba_messages::Event::Bar(bar), t0)
            .unwrap();

        let mut h = StateHarness::default();
        for step in scenario["steps"].as_array().unwrap() {
            let op = step["op"].as_str().unwrap();
            if op == "submit" || op == "cancel" {
                h.step(step, &mut engine);
            }
        }

        let want = scenario["expect_events"].as_array().unwrap();
        let normalised_want = normalise(want, "l1");

        assert_eq!(
            h.events.len(),
            normalised_want.len(),
            "{name}: event count mismatch\ngot: {}\nwant: {}",
            json!(h.events),
            json!(normalised_want)
        );
        for (actual, expected) in h.events.iter().zip(normalised_want.iter()) {
            assert!(
                matches(actual, expected),
                "{name}: event mismatch\ngot: {}\nwant: {}",
                actual,
                expected
            );
        }

        for oid in h.orders.keys() {
            if let Some(want_state) = scenario["expect_state"].get(oid) {
                let tracked = &h.orders[oid];
                assert_eq!(
                    status_name(&tracked.state.status),
                    want_state["status"].as_str().unwrap(),
                    "{name}: {oid} status"
                );
                assert!(
                    (tracked.state.filled_qty - want_state["filled_qty"].as_f64().unwrap()).abs()
                        <= EPS,
                    "{name}: {oid} filled_qty"
                );
                assert_eq!(
                    tracked.state.cancel_requested,
                    want_state["cancel_requested"].as_bool().unwrap(),
                    "{name}: {oid} cancel_requested"
                );
            }
        }
    }
}

#[test]
fn paper_execution_replays_scenarios_under_ack_profile() {
    let doc = fixture();
    let scenarios: Vec<&Value> = doc["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| {
            s.get("bar_realisable").and_then(|v| v.as_bool()) == Some(true)
                && s["profile"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|p| p.as_str() == Some("ack"))
        })
        .collect();

    assert_eq!(scenarios.len(), 2);

    for scenario in scenarios {
        let name = scenario["name"].as_str().unwrap();
        let mut engine = PaperExecution::new(10.0).with_ack(true);

        let mut h = StateHarness::default();
        for step in scenario["steps"].as_array().unwrap() {
            let op = step["op"].as_str().unwrap();
            if op == "submit" || op == "cancel" {
                h.step(step, &mut engine);
            }
        }

        let want = scenario["expect_events"].as_array().unwrap();
        let normalised_want = normalise(want, "ack");

        assert_eq!(
            h.events.len(),
            normalised_want.len(),
            "{name}: event count mismatch\ngot: {}\nwant: {}",
            json!(h.events),
            json!(normalised_want)
        );
        for (actual, expected) in h.events.iter().zip(normalised_want.iter()) {
            assert!(
                matches(actual, expected),
                "{name}: event mismatch\ngot: {}\nwant: {}",
                actual,
                expected
            );
        }

        for oid in h.orders.keys() {
            if let Some(want_state) = scenario["expect_state"].get(oid) {
                let tracked = &h.orders[oid];
                assert_eq!(
                    status_name(&tracked.state.status),
                    want_state["status"].as_str().unwrap(),
                    "{name}: {oid} status"
                );
                assert!(
                    (tracked.state.filled_qty - want_state["filled_qty"].as_f64().unwrap()).abs()
                        <= EPS,
                    "{name}: {oid} filled_qty"
                );
                assert_eq!(
                    tracked.state.cancel_requested,
                    want_state["cancel_requested"].as_bool().unwrap(),
                    "{name}: {oid} cancel_requested"
                );
            }
        }
    }
}

#[test]
fn next_open_sim_replays_bar_realisable_scenarios_under_l1_profile() {
    let doc = fixture();
    let scenarios: Vec<&Value> = doc["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| {
            s.get("bar_realisable").and_then(|v| v.as_bool()) == Some(true)
                && s["name"].as_str() == Some("market_fill")
                && s["profile"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|p| p.as_str() == Some("l1"))
        })
        .collect();

    assert_eq!(scenarios.len(), 1);

    for scenario in scenarios {
        let name = scenario["name"].as_str().unwrap();
        let mut sim = NextOpenSim::new(Money::new(100_000_000, Currency::Inr)).unwrap();

        let mut h = StateHarness::default();
        for step in scenario["steps"].as_array().unwrap() {
            let op = step["op"].as_str().unwrap();
            if op == "submit" {
                h.step(step, &mut sim);
                let ts = UnixNanos::from_u64(step["ts"].as_u64().unwrap() + 1);
                let bt = honba_messages::BarType::new(
                    InstrumentId::new(step["symbol"].as_str().unwrap(), Exchange::new("NSE")),
                    honba_messages::BarSpecification::new(
                        1,
                        honba_messages::BarAggregation::Minute,
                        honba_messages::PriceType::Last,
                    ),
                );
                let bar = honba_messages::Bar::new(bt, 10.0, 10.0, 10.0, 10.0, 1000.0, ts, ts);
                sim.on_bar(&bar).unwrap();
                h.drain(&mut sim);
            } else if op == "cancel" {
                h.step(step, &mut sim);
            }
        }

        let want = scenario["expect_events"].as_array().unwrap();
        let normalised_want = normalise(want, "l1");

        assert_eq!(
            h.events.len(),
            normalised_want.len(),
            "{name}: event count mismatch\ngot: {}\nwant: {}",
            json!(h.events),
            json!(normalised_want)
        );
        for (actual, expected) in h.events.iter().zip(normalised_want.iter()) {
            assert!(
                matches(actual, expected),
                "{name}: event mismatch\ngot: {}\nwant: {}",
                actual,
                expected
            );
        }

        for oid in h.orders.keys() {
            if let Some(want_state) = scenario["expect_state"].get(oid) {
                let tracked = &h.orders[oid];
                assert_eq!(
                    status_name(&tracked.state.status),
                    want_state["status"].as_str().unwrap(),
                    "{name}: {oid} status"
                );
                assert!(
                    (tracked.state.filled_qty - want_state["filled_qty"].as_f64().unwrap()).abs()
                        <= EPS,
                    "{name}: {oid} filled_qty"
                );
                assert_eq!(
                    tracked.state.cancel_requested,
                    want_state["cancel_requested"].as_bool().unwrap(),
                    "{name}: {oid} cancel_requested"
                );
            }
        }
    }
}

#[test]
fn scripted_execution_replays_ack_profile_scenarios() {
    let doc = fixture();
    let scenarios: Vec<&Value> = doc["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| {
            s["profile"]
                .as_array()
                .unwrap()
                .iter()
                .any(|p| p.as_str() == Some("ack"))
        })
        .collect();

    for scenario in scenarios {
        let name = scenario["name"].as_str().unwrap();
        let mut engine = ScriptedExecution::new(10.0);
        for step in scenario["steps"].as_array().unwrap() {
            if step["op"].as_str().unwrap() == "submit" {
                let oid = step["order_id"].as_str().unwrap().to_string();
                engine = engine.with(oid, Behavior::Hold);
            }
        }

        let mut h = StateHarness::default();
        let is_cancel_race = name == "cancel_race";

        for step in scenario["steps"].as_array().unwrap() {
            let op = step["op"].as_str().unwrap();
            let oid = step["order_id"].as_str().unwrap();
            let ts = UnixNanos::from_u64(step["ts"].as_u64().unwrap());
            match op {
                "submit" => {
                    h.step(step, &mut engine);
                }
                "cancel" => {
                    if is_cancel_race {
                        // In cancel_race, the strategy requests cancel, but the venue fill beats it.
                        let o = h.orders.get_mut(oid).unwrap();
                        let cancel_ev = ExecutionEvent::CancelRequested {
                            order_id: OrderId::new(oid),
                            ts,
                        };
                        if o.state.apply(&cancel_ev.order_event()).unwrap() {
                            let released = o.state.quantity - o.state.filled_qty;
                            h.drain.push(cancel_ev.clone());
                            h.events.push(project(&cancel_ev, &o.state, released));
                        }
                    } else {
                        h.step(step, &mut engine);
                    }
                }
                "venue" => {
                    let ev = step["event"].as_str().unwrap();
                    let action = match ev {
                        "accepted" => VenueAction::Accept {
                            venue_order_id: None,
                        },
                        "rejected" => VenueAction::Reject {
                            reason: step["reason"].as_str().unwrap_or("venue").to_string(),
                        },
                        "cancelled" => VenueAction::Cancel,
                        "expired" => VenueAction::Expire,
                        "fill" => VenueAction::Fill {
                            quantity: step["last_qty"].as_f64().unwrap(),
                        },
                        other => panic!("unknown venue event {other}"),
                    };
                    let res = engine.venue(oid, action, ts);
                    if step.get("expect_error").is_some() {
                        assert!(res.is_err(), "expected error executing venue action");
                    }
                    h.drain(&mut engine);
                }
                other => panic!("unknown op {other}"),
            }
        }

        let want = scenario["expect_events"].as_array().unwrap();
        let normalised_want = normalise(want, "ack");

        assert_eq!(
            h.events.len(),
            normalised_want.len(),
            "{name}: event count mismatch\ngot: {}\nwant: {}",
            json!(h.events),
            json!(normalised_want)
        );
        for (actual, expected) in h.events.iter().zip(normalised_want.iter()) {
            assert!(
                matches(actual, expected),
                "{name}: event mismatch\ngot: {}\nwant: {}",
                actual,
                expected
            );
        }

        for oid in h.orders.keys() {
            if let Some(want_state) = scenario["expect_state"].get(oid) {
                let tracked = &h.orders[oid];
                assert_eq!(
                    status_name(&tracked.state.status),
                    want_state["status"].as_str().unwrap(),
                    "{name}: {oid} status"
                );
                assert!(
                    (tracked.state.filled_qty - want_state["filled_qty"].as_f64().unwrap()).abs()
                        <= EPS,
                    "{name}: {oid} filled_qty"
                );
                assert_eq!(
                    tracked.state.cancel_requested,
                    want_state["cancel_requested"].as_bool().unwrap(),
                    "{name}: {oid} cancel_requested"
                );
            }
        }
    }
}
