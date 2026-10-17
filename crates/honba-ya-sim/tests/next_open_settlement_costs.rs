//! Settlement and costs through the public API: bars arrive as engine events (`Handler`),
//! orders and drains go through `ExecutionEngine`, as in a runner-driven backtest.

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::{Currency, Money};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId, Order, OrderId,
    OrderSide, OrderType, TimeInForce, UnixNanos,
};
use honba_sim::NextOpenSim;

fn iid(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("NSE"))
}

fn bar(symbol: &str, ts: u64, open: f64) -> Event {
    let bt = BarType::new(
        iid(symbol),
        BarSpecification::new(1, BarAggregation::Day, honba_messages::PriceType::Last),
    );
    let t = UnixNanos::from_u64(ts);
    Event::Bar(Bar::new(bt, open, open, open, open, 1.0, t, t))
}

fn market(id: &str, symbol: &str, side: OrderSide, qty: f64, ts: u64) -> Order {
    let t = UnixNanos::from_u64(ts);
    Order::new(
        OrderId::new(id),
        iid(symbol),
        side,
        OrderType::Market,
        qty,
        None,
        TimeInForce::Day,
        t,
        t,
    )
}

fn step(sim: &mut NextOpenSim, symbol: &str, ts: u64, open: f64) {
    sim.on_event(&bar(symbol, ts, open), UnixNanos::from_u64(ts))
        .unwrap();
}

#[test]
fn a_round_trip_with_t_plus_one_and_costs_conserves_cash() {
    // 1% of notional per fill; proceeds settle one session later.
    let mut sim = NextOpenSim::new(Money::new(100_000, Currency::Inr))
        .unwrap()
        .with_settlement_days(1)
        .unwrap()
        .with_costs(Box::new(|_, qty, price| {
            let n = Money::mul_qty(qty, price, Currency::Inr).unwrap().minor();
            Ok(Money::new((n + 50) / 100, Currency::Inr))
        }));
    step(&mut sim, "AAA", 1, 100.0);
    sim.submit(market("b1", "AAA", OrderSide::Buy, 5.0, 1))
        .unwrap();
    step(&mut sim, "AAA", 2, 100.0);
    sim.submit(market("s1", "AAA", OrderSide::Sell, 5.0, 2))
        .unwrap();
    // a second buy that only the sale proceeds could fund
    sim.submit(market("b2", "AAA", OrderSide::Buy, 9.0, 2))
        .unwrap();
    step(&mut sim, "AAA", 3, 120.0);
    assert_eq!(sim.working_orders(), vec!["b2".to_string()]); // waits for T+1
    step(&mut sim, "AAA", 4, 120.0);
    assert!(sim.working_orders().is_empty());

    let fills = sim.drain_fills().unwrap();
    let ids: Vec<_> = fills
        .iter()
        .map(|f| f.order_id().as_str().to_string())
        .collect();
    assert_eq!(ids, ["b1", "s1", "b2"]);
    let paid: i64 = fills.iter().map(|f| f.costs().minor()).sum();
    assert_eq!(sim.fees().minor(), paid);
    let signed: i64 = fills
        .iter()
        .map(|f| {
            let n = Money::mul_qty(f.quantity(), f.price(), Currency::Inr)
                .unwrap()
                .minor();
            if f.side() == OrderSide::Buy {
                -n
            } else {
                n
            }
        })
        .sum();
    assert_eq!(sim.cash().minor(), 100_000 + signed - paid);
    // 990 cash fund 8 of the 9 units once the cost is counted; the last one is rejected
    assert_eq!(sim.position(&iid("AAA")), 8.0);
    let rej = sim.drain_rejections().unwrap();
    assert_eq!((rej.len(), rej[0].quantity), (1, 1.0));
    assert_eq!(sim.unsettled().minor(), 0);
}

#[test]
fn a_failing_cost_function_surfaces_through_the_handler_and_keeps_the_order() {
    let mut sim = NextOpenSim::new(Money::new(100_000, Currency::Inr))
        .unwrap()
        .with_costs(Box::new(|_, _, _| Ok(Money::new(-1, Currency::Inr))));
    step(&mut sim, "AAA", 1, 100.0);
    sim.submit(market("b1", "AAA", OrderSide::Buy, 1.0, 1))
        .unwrap();
    let r = sim.on_event(&bar("AAA", 2, 100.0), UnixNanos::from_u64(2));
    assert!(r.is_err());
    assert_eq!(sim.working_orders(), vec!["b1".to_string()]);
}
