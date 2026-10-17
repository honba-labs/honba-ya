//! Seeded fill and latency models through the public API (E3-S2).
//!
//! The acceptance contract is reproducibility: the same seed plus the same
//! orders and bars must produce byte-identical fill ledgers. These tests drive
//! a full `NextOpenSim` run through the `Handler`/`ExecutionEngine` path, as a
//! runner-driven backtest would, and compare the serialized journals.

use std::sync::Arc;

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::{Currency, Money};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId, Order, OrderId,
    OrderSide, OrderType, PriceType, TimeInForce, UnixNanos,
};
use honba_sim::{NextOpenSim, SeededLatency, SlippageModel};

fn iid(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("NSE"))
}

fn bar(symbol: &str, ts: u64, open: f64) -> Event {
    let bt = BarType::new(
        iid(symbol),
        BarSpecification::new(1, BarAggregation::Day, PriceType::Last),
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

/// A repeatable backtest: the same bars and orders on every call. Both a seeded
/// slippage model and a seeded latency model are installed.
fn run(seed: u64) -> NextOpenSim {
    let mut sim = NextOpenSim::new(Money::new(1_000_000, Currency::Inr))
        .unwrap()
        .with_slippage(SlippageModel::Seed { bps: 8.0, seed })
        .with_latency_model(Arc::new(SeededLatency::new(3, 7)));
    let opens = [100.0, 101.0, 99.5, 102.0, 100.25, 98.75];
    for (i, open) in opens.into_iter().enumerate() {
        let ts = i as u64 + 1;
        step(&mut sim, "AAA", ts, open);
        let side = if i % 2 == 0 {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        sim.submit(market(&format!("O-{ts}"), "AAA", side, 10.0, ts))
            .unwrap();
    }
    step(&mut sim, "AAA", opens.len() as u64 + 1, 100.0);
    sim
}

#[test]
fn seeded_slippage_reproducible() {
    let a: Vec<f64> = run(7)
        .drain_fills()
        .unwrap()
        .iter()
        .map(|f| f.price())
        .collect();
    let b: Vec<f64> = run(7)
        .drain_fills()
        .unwrap()
        .iter()
        .map(|f| f.price())
        .collect();
    assert!(!a.is_empty());
    assert_eq!(a, b, "same seed must give the same fill prices");
}

#[test]
fn same_seed_byte_identical_fills() {
    let journal = |seed| serde_json::to_string(&run(seed).drain_fills().unwrap()).unwrap();
    let a = journal(42);
    let b = journal(42);
    assert_eq!(a, b, "same seed must give a byte-identical fill ledger");
    assert_ne!(a, journal(43), "a different seed must change the fills");
}
