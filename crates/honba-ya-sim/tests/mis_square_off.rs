//! MIS (intraday) auto square-off through the public API: bars arrive as engine events
//! (`Handler`), orders and drains go through `ExecutionEngine`, as in a runner-driven backtest.

use std::sync::Arc;

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::{Currency, Money};
use honba_market::settlement::PercentageMarginModel;
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId, Order, OrderId,
    OrderSide, OrderType, PriceType, Product, TimeInForce, UnixNanos,
};
use honba_sim::NextOpenSim;

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

fn mis_market(id: &str, symbol: &str, side: OrderSide, qty: f64, ts: u64) -> Order {
    Order::new(
        OrderId::new(id),
        iid(symbol),
        side,
        OrderType::Market,
        qty,
        None,
        TimeInForce::Day,
        t(ts),
        t(ts),
    )
    .with_product(Product::Intraday)
}

fn close_bar(symbol: &str, ts: u64, close: f64) -> Bar {
    let bt = BarType::new(
        iid(symbol),
        BarSpecification::new(1, BarAggregation::Day, PriceType::Last),
    );
    Bar::new(bt, close, close, close, close, 1.0, t(ts), t(ts))
}

#[test]
fn mis_run_flat_by_close() {
    let mut sim = NextOpenSim::new(Money::new(1_000_000, Currency::Inr))
        .unwrap()
        .with_margin_model(Arc::new(PercentageMarginModel::new(0.20, 0.15)))
        .with_auto_square_off_mis(true);

    // Day 1: submit an MIS (intraday) buy; it fills at day 2's open.
    step(&mut sim, "AAA", 1, 100.0);
    sim.submit(mis_market("mis-buy", "AAA", OrderSide::Buy, 10.0, 1))
        .unwrap();
    step(&mut sim, "AAA", 2, 110.0);
    assert_eq!(sim.position(&iid("AAA")), 10.0);

    // Session close: the run must be flattened at the close price, unasked.
    sim.on_session_close(t(3), &[close_bar("AAA", 3, 120.0)])
        .unwrap();

    assert!(sim.positions().is_empty(), "the run must end flat");
    let fills = sim.drain_fills().unwrap();
    let square_off = fills
        .iter()
        .find(|f| f.order_id().as_str().starts_with("__square_off"))
        .expect("a square-off fill");
    assert_eq!(square_off.instrument_id(), &iid("AAA"));
    assert_eq!(square_off.side(), OrderSide::Sell);
    assert_eq!((square_off.quantity(), square_off.price()), (10.0, 120.0));
    // Bought 10 @ 110 (= 110_000 minor) and squared 10 @ 120 (= 120_000 minor).
    assert_eq!(sim.cash().minor(), 1_010_000);
}
