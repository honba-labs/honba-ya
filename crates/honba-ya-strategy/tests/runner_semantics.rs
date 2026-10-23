//! `StrategyRunner` semantics shared with Python (ADR 008, decision 5).
//! Mirrors `python/tests/unit/test_strategy_runner.py`.

use std::sync::{Arc, Mutex};

use honba_engine::{ExecutionEngine, Handler, Result};
use honba_entities::{Currency, Money, Trade};
use honba_messages::{Bar, Event, InstrumentId, Order, OrderSide, QuoteTick, TradeTick, UnixNanos};
use honba_strategy::{LedgerContext, OrderIntent, Strategy, StrategyContext, StrategyRunner};
use honba_testing::VecFeed;
use honba_testing::fixtures::{any_instrument, instrument};

type Log = Arc<Mutex<Vec<(&'static str, u64)>>>;

/// Logs every hook with the context clock and submits what the test scripts.
struct Recorder {
    log: Log,
    script: Vec<(&'static str, OrderIntent)>,
}

impl Recorder {
    fn new(script: Vec<(&'static str, OrderIntent)>) -> (Self, Log) {
        let log = Log::default();
        (
            Self {
                log: log.clone(),
                script,
            },
            log,
        )
    }

    fn hook(&mut self, ctx: &mut dyn StrategyContext, hook: &'static str) -> Result<()> {
        self.log.lock().unwrap().push((hook, ctx.now().as_u64()));
        let (due, rest): (Vec<_>, Vec<_>) = self.script.drain(..).partition(|(h, _)| *h == hook);
        self.script = rest;
        for (_, intent) in due {
            ctx.submit(intent);
        }
        Ok(())
    }
}

impl Strategy for Recorder {
    fn name(&self) -> &str {
        "rec"
    }
    fn on_start(&mut self, ctx: &mut dyn StrategyContext) -> Result<()> {
        self.hook(ctx, "on_start")
    }
    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> Result<()> {
        self.hook(ctx, "on_bar")
    }
    fn on_quote(&mut self, ctx: &mut dyn StrategyContext, _quote: &QuoteTick) -> Result<()> {
        self.hook(ctx, "on_quote")
    }
    fn on_trade(&mut self, ctx: &mut dyn StrategyContext, _trade: &TradeTick) -> Result<()> {
        self.hook(ctx, "on_trade")
    }
    fn on_fill(&mut self, ctx: &mut dyn StrategyContext, _fill: &Trade) -> Result<()> {
        self.hook(ctx, "on_fill")
    }
    fn on_stop(&mut self, ctx: &mut dyn StrategyContext) -> Result<()> {
        self.hook(ctx, "on_stop")
    }
}

/// Records submissions; fills only what the test queues.
#[derive(Clone, Default)]
struct FakeExecution {
    orders: Arc<Mutex<Vec<Order>>>,
    fills: Arc<Mutex<Vec<Trade>>>,
}

impl ExecutionEngine for FakeExecution {
    fn submit(&mut self, order: Order) -> Result<()> {
        self.orders.lock().unwrap().push(order);
        Ok(())
    }
    fn cancel(&mut self, _order_id: &str, _now: honba_messages::UnixNanos) -> Result<()> {
        Ok(())
    }
    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        Ok(std::mem::take(&mut *self.fills.lock().unwrap()))
    }
}

fn x() -> InstrumentId {
    instrument("X")
}

fn send(runner: &mut impl Handler, event: Event, ts_init: u64) {
    runner
        .on_event(&event, UnixNanos::from_u64(ts_init))
        .unwrap();
}

fn bar_event(close: f64, ts: u64) -> Event {
    VecFeed::bar("X", close, ts).event().clone()
}

#[test]
fn hooks_dispatch_by_event_type_with_the_clock_at_ts_init() {
    let (s, log) = Recorder::new(vec![]);
    let mut runner = StrategyRunner::new(s, FakeExecution::default());
    runner.on_start().unwrap();
    send(&mut runner, bar_event(1.0, 10), 11);
    send(
        &mut runner,
        VecFeed::quote("X", 1.0, 1.1, 20).event().clone(),
        20,
    );
    send(
        &mut runner,
        VecFeed::trade("X", 1.0, 1.0, 30).event().clone(),
        30,
    );
    let accepted = Event::OrderAccepted {
        order_id: "O-1".into(),
        venue_order_id: None,
        ts_event: UnixNanos::from_u64(40),
    };
    send(&mut runner, accepted, 40); // not market data: no hook
    runner.on_stop().unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        vec![
            ("on_start", 0),
            ("on_bar", 11),
            ("on_quote", 20),
            ("on_trade", 30),
            ("on_stop", 40),
        ]
    );
}

#[test]
fn runner_uses_the_given_context() {
    let (s, _) = Recorder::new(vec![]);
    let runner = StrategyRunner::with_context(
        s,
        FakeExecution::default(),
        LedgerContext::with_cash(Money::from_major_f64(5.0, Currency::Inr).unwrap()),
    );
    assert_eq!(runner.context().cash().minor(), 500);
}

#[test]
fn start_intents_go_out_with_the_first_event_and_stop_intents_never() {
    let buy = OrderIntent::market_buy(x(), 1.0);
    let sell = OrderIntent::market_sell(x(), 1.0);
    let (s, _) = Recorder::new(vec![("on_start", buy.clone()), ("on_stop", sell)]);
    let ex = FakeExecution::default();
    let mut runner = StrategyRunner::new(s, ex.clone());
    runner.on_start().unwrap();
    assert!(ex.orders.lock().unwrap().is_empty());
    send(&mut runner, bar_event(1.0, 5), 5);
    runner.on_stop().unwrap();

    let orders = ex.orders.lock().unwrap();
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].order_id().as_str(), "rec-0");
    assert_eq!(orders[0].ts_init(), UnixNanos::from_u64(5));
    let submitted: Vec<_> = runner
        .submitted()
        .iter()
        .map(|s| {
            (
                s.ts_init.as_u64(),
                s.intent.clone(),
                s.order_id.as_str().to_owned(),
            )
        })
        .collect();
    assert_eq!(submitted, vec![(5, buy, "rec-0".to_owned())]);
}

#[test]
fn fills_update_the_context_before_on_fill_and_on_fill_intents_wait_for_the_next_event() {
    let protect = OrderIntent::stop_sell(x(), 2.0, 9.0);
    let (s, _) = Recorder::new(vec![
        ("on_bar", OrderIntent::market_buy(x(), 2.0)),
        ("on_fill", protect.clone()),
    ]);
    let ex = FakeExecution::default();
    let mut runner = StrategyRunner::new(s, ex.clone());
    runner.on_start().unwrap();
    let fill = Trade::new(
        "rec-0".into(),
        x(),
        OrderSide::Buy,
        2.0,
        10.0,
        Currency::Inr,
        UnixNanos::from_u64(5),
        UnixNanos::from_u64(5),
    );
    ex.fills.lock().unwrap().push(fill.clone());
    send(&mut runner, bar_event(10.0, 5), 5);
    assert_eq!(runner.context().position(&x()), 2.0);
    assert_eq!(runner.context().cash().minor(), -2000);
    assert_eq!(ex.orders.lock().unwrap().len(), 1, "protect not yet sent");
    send(&mut runner, bar_event(10.0, 6), 6);
    let orders = ex.orders.lock().unwrap();
    assert_eq!(orders[1].order_id().as_str(), "rec-1");
    assert_eq!(orders[1].trigger_price(), Some(9.0));
    assert_eq!(orders[1].ts_init(), UnixNanos::from_u64(6));
    assert_eq!(runner.fills(), &[fill]);
}

#[test]
fn a_strategy_works_with_any_context_implementation() {
    // The adapter path: drive hooks by hand against a bare ledger.
    let (mut s, log) = Recorder::new(vec![(
        "on_bar",
        OrderIntent::market_buy(any_instrument(), 1.0),
    )]);
    let mut ctx = LedgerContext::new();
    ctx.set_now(UnixNanos::from_u64(3));
    let bar = honba_testing::fixtures::flat_bar("X", 1.0, 3);
    s.on_bar(&mut ctx, &bar).unwrap();
    assert_eq!(ctx.drain_intents().len(), 1);
    assert_eq!(*log.lock().unwrap(), vec![("on_bar", 3)]);
}

/// An execution port whose `submit` always fails.
struct FailingExecution;

impl ExecutionEngine for FailingExecution {
    fn submit(&mut self, _order: Order) -> Result<()> {
        Err(honba_engine::AlgoError::Component("exchange down".into()))
    }
    fn cancel(&mut self, _order_id: &str, _now: honba_messages::UnixNanos) -> Result<()> {
        Ok(())
    }
    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }
}

#[test]
fn intent_is_recorded_only_after_a_successful_submit() {
    let (strategy, _log) = Recorder::new(vec![(
        "on_bar",
        OrderIntent::market_buy(any_instrument(), 1.0),
    )]);
    let mut runner = StrategyRunner::new(strategy, FailingExecution);
    let bar = VecFeed::bar("NIFTY50", 100.0, 1);
    let result = runner.on_event(bar.event(), bar.ts_init());
    assert!(result.is_err(), "the execution error propagates");
    assert!(
        runner.submitted().is_empty(),
        "a failed submit must not be recorded (Python parity)"
    );
}

#[test]
fn fill_costs_reach_the_context_cash_through_the_runner() {
    // The fixture's bar_close model charges no costs; this covers the cost signs end to end.
    let (s, _) = Recorder::new(vec![
        ("on_bar", OrderIntent::market_buy(x(), 2.0)),
        ("on_quote", OrderIntent::market_sell(x(), 2.0)),
    ]);
    let ex = FakeExecution::default();
    let mut runner = StrategyRunner::new(s, ex.clone());
    runner.on_start().unwrap();
    let fill = |side, price, ts: u64, id: &str, costs: f64| {
        Trade::new(
            id.into(),
            x(),
            side,
            2.0,
            price,
            Currency::Inr,
            UnixNanos::from_u64(ts),
            UnixNanos::from_u64(ts),
        )
        .with_costs(Money::from_major_f64(costs, Currency::Inr).unwrap())
    };
    ex.fills
        .lock()
        .unwrap()
        .push(fill(OrderSide::Buy, 10.0, 5, "rec-0", 1.5));
    send(&mut runner, bar_event(10.0, 5), 5);
    // A buy debits the cost on top of the notional.
    assert_eq!(runner.context().cash().minor(), -2150);
    ex.fills
        .lock()
        .unwrap()
        .push(fill(OrderSide::Sell, 11.0, 6, "rec-1", 2.0));
    send(
        &mut runner,
        VecFeed::quote("X", 10.9, 11.1, 6).event().clone(),
        6,
    );
    assert_eq!(runner.context().position(&x()), 0.0);
    // A sell credits the notional minus the cost.
    assert_eq!(runner.context().cash().minor(), -150);
}

#[test]
fn a_fill_the_ledger_cannot_book_fails_the_event_and_leaves_the_ledger_untouched() {
    // ADR 0011: a USD-costed fill booked into an INR ledger used to move the
    // position and silently skip the cash. The runner now surfaces it.
    let (s, _) = Recorder::new(vec![]);
    let ex = FakeExecution::default();
    let mut runner = StrategyRunner::with_context(
        s,
        ex.clone(),
        LedgerContext::with_cash(Money::new(1_000, Currency::Inr)),
    );
    runner.on_start().unwrap();
    let usd = Trade::new(
        "rec-0".into(),
        x(),
        OrderSide::Buy,
        1.0,
        1.0,
        Currency::Usd,
        UnixNanos::from_u64(5),
        UnixNanos::from_u64(5),
    )
    .with_costs(Money::new(1, Currency::Usd));
    ex.fills.lock().unwrap().push(usd);
    let err = runner
        .on_event(&bar_event(1.0, 5), UnixNanos::from_u64(5))
        .unwrap_err();
    assert!(err.to_string().contains("booking fill"), "{err}");
    assert_eq!(runner.context().cash(), Money::new(1_000, Currency::Inr));
    assert_eq!(runner.context().position(&x()), 0.0);
}
