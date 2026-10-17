//! Strategy -> engine -> simulated execution, driven by an in-memory feed.
//!
//! honba-sim may not dev-depend on honba-strategy or honba-testing (see
//! `scripts/dependency_graph.py`), so the "strategy" here is a minimal
//! engine `Handler` that turns bars into orders, and the feed is local.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use honba_engine::{DataFeed, Engine, ExecutionEngine, Handler, Result};
use honba_entities::Trade;
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId, Message, Order,
    OrderId, OrderSide, OrderType, PriceType, TimeInForce, UnixNanos,
};
use honba_sim::{BarFillEngine, PaperExecution};

fn instrument() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("TEST"))
}

fn bar(close: f64, ts: u64) -> Message {
    let bt = BarType::new(
        instrument(),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    let t = UnixNanos::from_u64(ts);
    Message::new(
        Event::Bar(Bar::new(bt, close, close, close, close, 1.0, t, t)),
        t,
    )
}

struct Feed(VecDeque<Message>);

impl DataFeed for Feed {
    fn next(&mut self) -> Result<Option<Message>> {
        Ok(self.0.pop_front())
    }
}

/// Goes long one unit when the close crosses above `level` and flattens when
/// it crosses back below, submitting market orders to `exec`.
struct Threshold<E> {
    level: f64,
    long: bool,
    next_id: u64,
    exec: E,
    on_bar: fn(&mut E, &Bar),
}

impl<E: ExecutionEngine> Threshold<E> {
    fn new(level: f64, exec: E, on_bar: fn(&mut E, &Bar)) -> Self {
        Self {
            level,
            long: false,
            next_id: 1,
            exec,
            on_bar,
        }
    }

    fn submit(&mut self, side: OrderSide, ts: UnixNanos) -> Result<()> {
        let id = OrderId::new(format!("O-{}", self.next_id));
        self.next_id += 1;
        self.exec.submit(Order::new(
            id,
            instrument(),
            side,
            OrderType::Market,
            1.0,
            None,
            TimeInForce::Day,
            ts,
            ts,
        ))
    }
}

impl<E: ExecutionEngine> Handler for Threshold<E> {
    fn on_event(
        &mut self,
        event: &Event,
        _ts_init: UnixNanos,
    ) -> Result<honba_engine::EngineOutput> {
        let Event::Bar(b) = event else {
            return Ok(honba_engine::EngineOutput::None);
        };
        (self.on_bar)(&mut self.exec, b);
        if !self.long && b.close() > self.level {
            self.long = true;
            self.submit(OrderSide::Buy, b.ts_event())?;
        } else if self.long && b.close() < self.level {
            self.long = false;
            self.submit(OrderSide::Sell, b.ts_event())?;
        }
        Ok(honba_engine::EngineOutput::None)
    }
}

const CLOSES: [f64; 8] = [99.0, 101.0, 102.0, 98.0, 97.0, 103.0, 104.0, 96.0];

fn feed() -> Feed {
    Feed(
        CLOSES
            .iter()
            .enumerate()
            .map(|(i, &c)| bar(c, (i as u64 + 1) * 1_000))
            .collect(),
    )
}

/// Runs the threshold strategy against a `BarFillEngine` registered as a
/// handler ahead of the strategy, so each order fills at its own bar's close.
fn run_bar_fill(mut feed: Feed) -> Vec<Trade> {
    let exec = BarFillEngine::new();
    let mut engine = Engine::new();
    engine.add_handler(exec.clone());
    engine.add_handler(Threshold::new(100.0, exec.clone(), |_, _| {}));
    engine.run(&mut feed).unwrap();
    exec.clone().drain_fills().unwrap()
}

#[test]
fn strategy_orders_fill_at_each_signal_bar_close() {
    let fills = run_bar_fill(feed());
    let got: Vec<(OrderSide, f64, u64)> = fills
        .iter()
        .map(|f| (f.side(), f.price(), f.ts_event().as_u64()))
        .collect();
    assert_eq!(
        got,
        [
            (OrderSide::Buy, 101.0, 2_000),
            (OrderSide::Sell, 98.0, 4_000),
            (OrderSide::Buy, 103.0, 6_000),
            (OrderSide::Sell, 96.0, 8_000),
        ]
    );
    let ids: Vec<&str> = fills.iter().map(|f| f.order_id().as_str()).collect();
    assert_eq!(ids, ["O-1", "O-2", "O-3", "O-4"]);
    assert!(fills.iter().all(|f| f.instrument_id() == &instrument()));
}

#[test]
fn out_of_order_feed_is_replayed_in_time_order() {
    let mut shuffled: Vec<Message> = feed().0.into();
    shuffled.reverse();
    let fills = run_bar_fill(Feed(shuffled.into()));
    assert_eq!(fills, run_bar_fill(feed()));
}

#[test]
fn bar_fill_run_is_deterministic() {
    assert_eq!(run_bar_fill(feed()), run_bar_fill(feed()));
}

/// The same strategy against `PaperExecution`, whose price the strategy sets
/// from each bar close before deciding.
#[test]
fn paper_execution_fills_strategy_orders_at_the_bar_price() {
    let fills = Arc::new(Mutex::new(Vec::new()));
    struct Collect<E> {
        inner: Threshold<E>,
        out: Arc<Mutex<Vec<Trade>>>,
    }
    impl Handler for Collect<PaperExecution> {
        fn on_event(
            &mut self,
            event: &Event,
            ts_init: UnixNanos,
        ) -> Result<honba_engine::EngineOutput> {
            self.inner.on_event(event, ts_init)?;
            let new = self.inner.exec.drain_fills()?;
            self.out.lock().unwrap().extend(new);
            Ok(honba_engine::EngineOutput::None)
        }
    }

    let mut engine = Engine::new();
    engine.add_handler(Collect {
        inner: Threshold::new(100.0, PaperExecution::new(0.0), |e, b| {
            e.set_price(b.close())
        }),
        out: fills.clone(),
    });
    engine.run(&mut feed()).unwrap();

    let fills = fills.lock().unwrap();
    let got: Vec<(OrderSide, f64)> = fills.iter().map(|f| (f.side(), f.price())).collect();
    assert_eq!(
        got,
        [
            (OrderSide::Buy, 101.0),
            (OrderSide::Sell, 98.0),
            (OrderSide::Buy, 103.0),
            (OrderSide::Sell, 96.0),
        ]
    );
}

/// ADR 0011 through the engine: every fill's costs round per leg, so the run's
/// total is the sum of integer per-fill costs, each 1 minor unit flat + 1 bps.
#[test]
fn costs_round_per_leg_on_every_fill_of_a_run() {
    use honba_sim::FillCosts;

    // 1 bps of each close (96..104) is ~0.01: each leg rounds to a minor unit once.
    let exec = BarFillEngine::with_costs(FillCosts::new(0.005, 1.0).unwrap());
    let mut engine = Engine::new();
    engine.add_handler(exec.clone());
    engine.add_handler(Threshold::new(100.0, exec.clone(), |_, _| {}));
    engine.run(&mut feed()).unwrap();
    let fills = exec.clone().drain_fills().unwrap();
    let costs: Vec<i64> = fills.iter().map(|f| f.costs().minor()).collect();
    assert_eq!(costs, [2, 2, 2, 2]);
}

/// A cost that cannot be represented stops the run with an error instead of
/// settling a free fill.
#[test]
fn an_unrepresentable_cost_stops_the_run() {
    use honba_sim::FillCosts;

    let exec = BarFillEngine::with_costs(FillCosts::new(0.0, 10_000.0).unwrap());
    let mut engine = Engine::new();
    engine.add_handler(exec.clone());
    engine.add_handler(Threshold::new(100.0, exec.clone(), |_, _| {}));
    let mut huge = Feed(vec![bar(99.0, 1_000), bar(2e18, 2_000)].into());
    let err = engine.run(&mut huge).unwrap_err();
    assert!(err.to_string().contains("fill cost"), "{err}");
}
