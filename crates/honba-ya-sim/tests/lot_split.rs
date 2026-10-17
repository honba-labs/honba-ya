//! Lot/freeze splitter run through the public API: a target of 300 shares with lot 75 and freeze
//! 100 is sliced into lot-aligned clips, and a real simulator fills each clip.

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::{Currency, Money, Position, PositionSide};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId, OrderSide,
    PriceType, UnixNanos,
};
use honba_sim::{NextOpenSim, SmartOrderController, TargetPosition};

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
fn order_sliced_in_sim() {
    let aaa = iid("AAA");
    let mut sim = NextOpenSim::new(Money::new(10_000_000, Currency::Inr)).unwrap();
    sim.set_lot_size(&aaa, 75.0).unwrap();
    let sizing = TargetPosition::new()
        .with_lot_size(75.0)
        .unwrap()
        .with_freeze(100.0)
        .unwrap();
    let mut controller = SmartOrderController::with_sizing(sizing);

    // Day 1 opens the first session; the run is flat.
    step(&mut sim, "AAA", 1, 100.0);

    // A target of 300 with lot 75 / freeze 100 slices into four lot-aligned clips of 75 each.
    let orders = controller
        .submit_target_orders(&snapshot(&aaa, sim.position(&aaa)), 300.0, None, t(1))
        .unwrap();
    let sizes: Vec<f64> = orders.iter().map(|o| o.quantity()).collect();
    assert_eq!(sizes, vec![75.0, 75.0, 75.0, 75.0]);
    assert!(orders.iter().all(|o| o.side() == OrderSide::Buy));

    for order in &orders {
        sim.submit(order.clone()).unwrap();
    }
    step(&mut sim, "AAA", 2, 110.0); // fills at day 2's open, 110
    assert_eq!(sim.position(&aaa), 300.0);

    let fills = sim.drain_fills().unwrap();
    assert_eq!(
        fills.iter().map(|f| f.quantity()).collect::<Vec<_>>(),
        vec![75.0, 75.0, 75.0, 75.0]
    );
    assert!(fills.iter().all(|f| f.price() == 110.0));
}
