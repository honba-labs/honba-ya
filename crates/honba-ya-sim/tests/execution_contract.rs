//! The shared `ExecutionEngine` contract, run against every engine in this crate.
//!
//! Contract (ADR 008, decision 13): fills belong to submitted orders, and once
//! the working orders are cancelled `filled + released == ordered` for every
//! order; draining twice never repeats an item; cancelling a finished or
//! unknown order is a no-op.

use std::collections::HashMap;

use honba_engine::{ExecutionEngine, Handler};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId, Order, OrderId,
    OrderSide, OrderType, PriceType, TimeInForce, UnixNanos,
};
use honba_sim::{BarFillEngine, Behavior, NextOpenSim, PaperExecution, ScriptedExecution};

fn order(id: &str, side: OrderSide, qty: f64, ts: u64) -> Order {
    let t = UnixNanos::from_u64(ts);
    Order::new(
        OrderId::new(id),
        InstrumentId::new("X", Exchange::new("TEST")),
        side,
        OrderType::Market,
        qty,
        None,
        TimeInForce::Day,
        t,
        t,
    )
}

fn check_execution_contract(engine: &mut dyn ExecutionEngine, ids: &[&str]) {
    let mut ordered: HashMap<String, f64> = HashMap::new();
    for (i, id) in ids.iter().enumerate() {
        let qty = 10.0 + i as f64;
        ordered.insert((*id).to_string(), qty);
        engine
            .submit(order(id, OrderSide::Buy, qty, i as u64 + 1))
            .unwrap();
    }
    for id in ids {
        engine.cancel(id, UnixNanos::from_u64(100)).unwrap();
    }
    engine
        .cancel("unknown-order", UnixNanos::from_u64(100))
        .unwrap();

    let fills = engine.drain_fills().unwrap();
    let rejections = engine.drain_rejections().unwrap();
    let mut settled: HashMap<String, f64> = HashMap::new();
    for f in &fills {
        assert!(ordered.contains_key(f.order_id().as_str()), "stray fill");
        *settled
            .entry(f.order_id().as_str().to_string())
            .or_default() += f.quantity();
    }
    for r in &rejections {
        assert!(ordered.contains_key(r.order_id.as_str()), "stray rejection");
        assert!(r.quantity > 0.0 && !r.reason.is_empty());
        *settled.entry(r.order_id.as_str().to_string()).or_default() += r.quantity;
    }
    for (id, qty) in &ordered {
        assert_eq!(
            settled.get(id),
            Some(qty),
            "filled + released != ordered for {id}"
        );
    }
    assert!(engine.drain_fills().unwrap().is_empty());
    assert!(engine.drain_rejections().unwrap().is_empty());
}

#[test]
fn bar_fill_engine_honours_the_contract() {
    // A fill needs an observed price: show the engine one bar first.
    let mut engine = BarFillEngine::new();
    let bt = BarType::new(
        InstrumentId::new("X", Exchange::new("TEST")),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    let t = UnixNanos::from_u64(1);
    let bar = Bar::new(bt, 10.0, 10.0, 10.0, 10.0, 1.0, t, t);
    engine.on_event(&Event::Bar(bar), t).unwrap();
    check_execution_contract(&mut engine, &["a", "b"]);
}

#[test]
fn paper_execution_honours_the_contract() {
    check_execution_contract(&mut PaperExecution::new(10.0), &["a", "b"]);
}

#[test]
fn scripted_execution_honours_the_contract() {
    let mut engine = ScriptedExecution::new(10.0)
        .with("a", Behavior::Hold)
        .with("b", Behavior::partial(4.0, "insufficient_funds"))
        .with("c", Behavior::reject("no_position"))
        .with("d", Behavior::Fill);
    check_execution_contract(&mut engine, &["a", "b", "c", "d"]);
}

#[test]
fn next_open_sim_honours_the_contract() {
    use honba_entities::{Currency, Money};
    let mut engine = NextOpenSim::new(Money::new(1_000_000, Currency::Inr)).unwrap();
    check_execution_contract(&mut engine, &["a", "b"]);
}

// ---- ADR 0019 (b): the same contract over the one ordered event drain ----

use honba_entities::ExecutionEvent;
use honba_messages::{OrderEvent, OrderState};

/// Submits, cancels every order, then replays the drained events through the
/// FSM behind a submitter-synthesised `Submitted`: no transition may be
/// illegal, every order ends terminal, and `filled + released == ordered`.
fn check_event_contract(engine: &mut dyn ExecutionEngine, ids: &[&str]) {
    assert!(engine.native_events());
    let mut states: HashMap<String, OrderState> = HashMap::new();
    let mut ordered: HashMap<String, f64> = HashMap::new();
    for (i, id) in ids.iter().enumerate() {
        let qty = 10.0 + i as f64;
        ordered.insert((*id).to_string(), qty);
        let mut state = OrderState::new();
        state
            .apply(&OrderEvent::Submitted { quantity: qty })
            .unwrap();
        states.insert((*id).to_string(), state);
        engine
            .submit(order(id, OrderSide::Buy, qty, i as u64 + 1))
            .unwrap();
    }
    for id in ids {
        engine.cancel(id, UnixNanos::from_u64(100)).unwrap();
    }
    engine
        .cancel("unknown-order", UnixNanos::from_u64(100))
        .unwrap();

    let mut settled: HashMap<String, f64> = HashMap::new();
    for ev in engine.drain_events().unwrap() {
        let id = ev.order_id().as_str().to_string();
        let state = states.get_mut(&id).expect("stray event");
        state
            .apply(&ev.order_event())
            .unwrap_or_else(|e| panic!("{id}: {e} on {ev:?}"));
        let qty = match &ev {
            ExecutionEvent::Fill { trade, .. } => trade.quantity(),
            ExecutionEvent::Rejected { quantity, .. }
            | ExecutionEvent::Cancelled { quantity, .. }
            | ExecutionEvent::Expired { quantity, .. } => *quantity,
            other => panic!("an L1 engine emits no {other:?}"),
        };
        *settled.entry(id).or_default() += qty;
    }
    for (id, qty) in &ordered {
        assert_eq!(
            settled.get(id),
            Some(qty),
            "filled + released != ordered for {id}"
        );
        let status = states[id].status;
        assert!(
            matches!(
                status,
                honba_messages::OrderStatus::Filled
                    | honba_messages::OrderStatus::Cancelled
                    | honba_messages::OrderStatus::Rejected
                    | honba_messages::OrderStatus::Expired
            ),
            "{id} is not terminal: {status:?}"
        );
    }
    assert!(engine.drain_events().unwrap().is_empty());
}

#[test]
fn bar_fill_engine_honours_the_event_contract() {
    let mut engine = BarFillEngine::new();
    let bt = BarType::new(
        InstrumentId::new("X", Exchange::new("TEST")),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    let t = UnixNanos::from_u64(1);
    let bar = Bar::new(bt, 10.0, 10.0, 10.0, 10.0, 1.0, t, t);
    engine.on_event(&Event::Bar(bar), t).unwrap();
    check_event_contract(&mut engine, &["a", "b"]);
}

#[test]
fn paper_execution_honours_the_event_contract() {
    check_event_contract(&mut PaperExecution::new(10.0), &["a", "b"]);
}

#[test]
fn scripted_execution_honours_the_event_contract() {
    use honba_sim::VenueAction;
    let mut engine = ScriptedExecution::new(10.0)
        .with("a", Behavior::Hold)
        .with("b", Behavior::partial(4.0, "insufficient_funds"))
        .with("c", Behavior::reject("no_position"))
        .with("d", Behavior::Fill)
        .with("e", Behavior::Expire)
        .with(
            "f",
            Behavior::Script(vec![
                VenueAction::Fill { quantity: 1.0 },
                VenueAction::Fill { quantity: 2.0 },
            ]),
        );
    check_event_contract(&mut engine, &["a", "b", "c", "d", "e", "f"]);
}

#[test]
fn next_open_sim_honours_the_event_contract() {
    use honba_entities::{Currency, Money};
    let mut engine = NextOpenSim::new(Money::new(1_000_000, Currency::Inr)).unwrap();
    check_event_contract(&mut engine, &["a", "b"]);
}

#[test]
fn next_open_sim_partial_sell_replays_cleanly() {
    use honba_entities::{Currency, Money};
    let mut engine = NextOpenSim::new(Money::new(1_000_000, Currency::Inr)).unwrap();
    let x = InstrumentId::new("X", Exchange::new("TEST"));
    engine.set_position(&x, 4.0).unwrap();
    let bt = BarType::new(
        x,
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    let bar = |ts: u64| {
        let t = UnixNanos::from_u64(ts);
        Event::Bar(Bar::new(bt.clone(), 10.0, 10.0, 10.0, 10.0, 1.0, t, t))
    };
    engine.on_event(&bar(1), UnixNanos::from_u64(1)).unwrap();
    engine.submit(order("s", OrderSide::Sell, 10.0, 1)).unwrap();
    engine.on_event(&bar(2), UnixNanos::from_u64(2)).unwrap();
    let mut state = OrderState::new();
    state
        .apply(&OrderEvent::Submitted { quantity: 10.0 })
        .unwrap();
    for ev in engine.drain_events().unwrap() {
        state.apply(&ev.order_event()).unwrap();
    }
    assert_eq!(state.status, honba_messages::OrderStatus::Rejected);
    assert_eq!(state.filled_qty, 4.0);
}
