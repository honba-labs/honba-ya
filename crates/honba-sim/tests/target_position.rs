//! Target-position run through the public API: a controller turns desired positions into single
//! orders and serialises per instrument, so a re-emitted target while one is working is deferred.

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::{Currency, Money, Position, PositionSide};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId, OrderSide,
    PriceType, UnixNanos,
};
use honba_sim::{NextOpenSim, SmartOrderController};

fn iid(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("NSE"))
}

fn t(ts: u64) -> UnixNanos {
    UnixNanos::from_u64(ts)
}

fn bar(symbol: &str, ts: u64, price: f64) -> Event {
    let bt = BarType::new(
        iid(symbol),
        BarSpecification::new(1, BarAggregation::Day, PriceType::Last),
    );
    Event::Bar(Bar::new(bt, price, price, price, price, 1.0, t(ts), t(ts)))
}

fn step(sim: &mut NextOpenSim, symbol: &str, ts: u64, price: f64) {
    sim.on_event(&bar(symbol, ts, price), t(ts)).unwrap();
}

/// A net position in `instrument`: `+` long, `-` short, `0` flat.
fn snapshot(instrument: &InstrumentId, qty: f64) -> Position {
    let mut position = Position::flat(instrument.clone(), Currency::Inr);
    if qty > 0.0 {
        position.apply_fill(PositionSide::Long, qty, 100.0);
    } else if qty < 0.0 {
        position.apply_fill(PositionSide::Short, -qty, 100.0);
    }
    position
}

#[test]
fn target_position_run() {
    let aaa = iid("AAA");
    let mut sim = NextOpenSim::new(Money::new(10_000_000, Currency::Inr)).unwrap();
    let mut controller = SmartOrderController::new();

    // Day 1 opens the first session; the run is flat.
    step(&mut sim, "AAA", 1, 100.0);

    // Strategy: target 100 from flat -> one buy order.
    let entry = controller
        .submit_target_position(&snapshot(&aaa, sim.position(&aaa)), 100.0, None, t(1))
        .unwrap()
        .expect("target 100 from flat emits a buy");
    assert_eq!(entry.order_id().as_str(), "target-AAA-1");
    assert_eq!(entry.side(), OrderSide::Buy);
    assert_eq!(entry.quantity(), 100.0);

    // The target is re-emitted before the order fills: serialisation defers it.
    assert!(controller
        .submit_target_position(&snapshot(&aaa, sim.position(&aaa)), 100.0, None, t(1))
        .unwrap()
        .is_none());

    sim.submit(entry).unwrap();
    step(&mut sim, "AAA", 2, 110.0); // fills at day 2's open, 110
    assert_eq!(sim.position(&aaa), 100.0);

    // The fill releases the instrument; target 0 from 100 flattens.
    controller.release(&aaa);
    let exit = controller
        .submit_target_position(&snapshot(&aaa, sim.position(&aaa)), 0.0, None, t(2))
        .unwrap()
        .expect("target 0 from 100 emits a sell");
    assert_eq!(exit.order_id().as_str(), "target-AAA-2");
    assert_eq!(exit.side(), OrderSide::Sell);
    assert_eq!(exit.quantity(), 100.0);

    sim.submit(exit).unwrap();
    step(&mut sim, "AAA", 3, 120.0); // fills at day 3's open, 120
    assert_eq!(sim.position(&aaa), 0.0);

    let fills = sim.drain_fills().unwrap();
    assert_eq!(fills.len(), 2);
    assert_eq!(
        (fills[0].side(), fills[0].quantity(), fills[0].price()),
        (OrderSide::Buy, 100.0, 110.0)
    );
    assert_eq!(
        (fills[1].side(), fills[1].quantity(), fills[1].price()),
        (OrderSide::Sell, 100.0, 120.0)
    );
    // 10_000_000 - (100 * 110 * 100) + (100 * 120 * 100) = 10_100_000 minor units.
    assert_eq!(sim.cash().minor(), 10_100_000);
}
