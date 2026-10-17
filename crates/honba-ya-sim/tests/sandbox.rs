//! Integration: a sandbox run over a live-style feed books exactly the fills a
//! `BarFillEngine` backtest books over the same bars and orders (E3-S7).
//!
//! Seeded, deterministic and offline: the feed replays a fixed bar slice and the
//! sandbox wraps the same matching core the backtest drives.

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::{Currency, ExecutionEvent, Money, Trade};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId, Order, OrderId,
    OrderSide, OrderType, PriceType, TimeInForce, UnixNanos,
};
use honba_sim::{BarFillEngine, SandboxAdapter, SimulatedFeed};

fn iid() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("TEST"))
}

fn t(ts: u64) -> UnixNanos {
    UnixNanos::from_u64(ts)
}

fn bar(open: f64, close: f64, ts: u64) -> Bar {
    let bt = BarType::new(
        iid(),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    Bar::new(
        bt,
        open,
        open.max(close),
        open.min(close),
        close,
        1.0,
        t(ts),
        t(ts),
    )
}

fn order(id: &str, side: OrderSide, qty: f64, ts: u64) -> Order {
    Order::new(
        OrderId::new(id),
        iid(),
        side,
        OrderType::Market,
        qty,
        None,
        TimeInForce::Day,
        t(ts),
        t(ts),
    )
}

fn fills(events: Vec<ExecutionEvent>, out: &mut Vec<Trade>) {
    for ev in events {
        if let ExecutionEvent::Fill { trade, .. } = ev {
            out.push(trade);
        }
    }
}

#[test]
fn sandbox_vs_backtest_same_fills() {
    let bars = vec![
        bar(100.0, 101.0, 1),
        bar(101.0, 99.0, 2),
        bar(99.0, 103.0, 3),
        bar(103.0, 102.0, 4),
    ];
    let stream: Vec<(usize, Order)> = vec![
        (0, order("b1", OrderSide::Buy, 10.0, 1)),
        (2, order("s1", OrderSide::Sell, 4.0, 3)),
        (3, order("b2", OrderSide::Buy, 3.0, 4)),
    ];

    // Backtest: the same core, driven directly.
    let mut backtest = BarFillEngine::new().with_seed(7);
    let mut bt_fills: Vec<Trade> = Vec::new();
    for (i, b) in bars.iter().enumerate() {
        backtest
            .on_event(&Event::Bar(b.clone()), b.ts_event())
            .unwrap();
        for (_, o) in stream.iter().filter(|(k, _)| *k == i) {
            backtest.submit(o.clone()).unwrap();
            fills(backtest.drain_events().unwrap(), &mut bt_fills);
        }
    }

    // Sandbox: the same core behind a live-style feed.
    let mut sandbox = SandboxAdapter::new(
        SimulatedFeed::from_bars(bars.clone()),
        BarFillEngine::new().with_seed(7),
        Money::new(100_000_000, Currency::Inr),
    );
    let mut sb_fills: Vec<Trade> = Vec::new();
    for (i, _) in bars.iter().enumerate() {
        assert!(sandbox.advance().unwrap());
        for (_, o) in stream.iter().filter(|(k, _)| *k == i) {
            sandbox.place_order(o.clone()).unwrap();
            fills(sandbox.drain_events(), &mut sb_fills);
        }
    }

    assert_eq!(bt_fills.len(), 3);
    assert_eq!(
        sb_fills, bt_fills,
        "sandbox and backtest must book byte-identical fills"
    );
}
