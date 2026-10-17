//! Integration tests for the async shell, through the public API only.
//!
//! Every test drives a real [`Engine`] on a real tokio runtime through
//! [`EngineHandle`], with the deterministic fakes from `honba-testing` for the
//! feed and `honba-sim` for execution.
//!
//! # How these tests order themselves without timers
//!
//! Two properties of the shell make a test's next action deterministic, and
//! both are relied on throughout:
//!
//! - **The feed branch wins.** `drive` polls the feed first and is biased, so a
//!   message the transport already has is always injected and dispatched before
//!   a queued command is applied.
//! - **A command channel of capacity one is a barrier.** `submit` only completes
//!   once the engine task has taken the previous command, so
//!   `submit(a).await; submit(b).await` proves `a` was applied before `b` was
//!   even queued. `Command::Stop` is used as the final barrier: once its send
//!   has completed, every earlier command is applied, and the bars sent after it
//!   still reach the engine because the feed outranks it.

use async_trait::async_trait;
use honba_async::{AsyncError, BoxedFeed, Command, EngineHandle, TradingState};
use honba_engine::{AlgoError, AuditKind, AuditRecord, Engine, EngineOutput, Handler, Result};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId, Message, Order,
    OrderId, OrderSide, OrderType, PriceType, TimeInForce, UnixNanos,
};
use honba_ports::{MarketDataFeed, PortError, PortResult};
use honba_sim::BarFillEngine;
use honba_testing::{Recorder, VecMessageFeed};

/// Capacity of every command channel these tests open: one, so that a completed
/// `submit` is a barrier proving the previous command was applied.
const BARRIER: usize = 1;

fn ts(nanos: u64) -> UnixNanos {
    UnixNanos::from_u64(nanos)
}

fn instrument() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("NSE"))
}

fn bar(close: f64, ts_event: u64) -> Message {
    let bar_type = BarType::new(
        instrument(),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    let at = ts(ts_event);
    Message::new(
        Event::Bar(Bar::new(bar_type, close, close, close, close, 10.0, at, at)),
        at,
    )
}

fn bars(closes: &[(f64, u64)]) -> Vec<Message> {
    closes.iter().map(|(close, at)| bar(*close, *at)).collect()
}

fn market_order(id: &str, ts_event: u64) -> Order {
    Order::new(
        OrderId::new(id),
        instrument(),
        OrderSide::Buy,
        OrderType::Market,
        5.0,
        None,
        TimeInForce::Day,
        ts(ts_event),
        ts(ts_event),
    )
}

fn dispatched(audit: &[AuditRecord]) -> Vec<u64> {
    audit
        .iter()
        .filter_map(|record| match record.kind {
            AuditKind::EventDispatched { ts_event } => Some(ts_event),
            _ => None,
        })
        .collect()
}

fn state_changes(audit: &[AuditRecord]) -> Vec<(TradingState, TradingState)> {
    audit
        .iter()
        .filter_map(|record| match record.kind {
            AuditKind::StateChanged { from, to } => Some((from, to)),
            _ => None,
        })
        .collect()
}

/// Order verdicts in audit order: every refused order and every routed one.
fn order_verdicts(audit: &[AuditRecord]) -> Vec<String> {
    audit
        .iter()
        .filter_map(|record| match &record.kind {
            AuditKind::OrderSubmitted { order_id, .. } => Some(format!("submitted {order_id}")),
            AuditKind::OrderRejected { order_id, reason } => {
                Some(format!("rejected {order_id} ({reason})"))
            }
            _ => None,
        })
        .collect()
}

/// The bar timestamps a recorder saw, ignoring the `OrderFilled` acks the engine
/// turns a strategy's orders into.
fn bars_seen_by(recorder: &Recorder) -> Vec<u64> {
    recorder
        .events()
        .iter()
        .filter(|event| matches!(event, Event::Bar(_)))
        .map(|event| event.ts_event().as_u64())
        .collect()
}

/// A feed that blocks on a channel the test owns, so a test can decide exactly
/// when the engine task reaches its next feed read.
struct GatedFeed {
    incoming: tokio::sync::mpsc::Receiver<Message>,
}

impl GatedFeed {
    fn new() -> (Self, Gate) {
        let (incoming, receiver) = tokio::sync::mpsc::channel(8);
        (Self { incoming: receiver }, Gate { incoming })
    }
}

/// The test's side of a [`GatedFeed`].
struct Gate {
    incoming: tokio::sync::mpsc::Sender<Message>,
}

impl Gate {
    async fn send(&self, msg: Message) {
        self.incoming
            .send(msg)
            .await
            .expect("the engine task dropped the feed");
    }
}

#[async_trait]
impl MarketDataFeed for GatedFeed {
    async fn subscribe(&mut self, _symbols: &[InstrumentId]) -> PortResult<()> {
        Ok(())
    }

    async fn unsubscribe(&mut self, _symbols: &[InstrumentId]) -> PortResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> PortResult<Option<Message>> {
        Ok(self.incoming.recv().await)
    }
}

/// Submits one order per bar and nothing for the fills the engine turns those
/// orders into, so the command/ack loop settles instead of feeding itself.
struct OrderOnEveryBar {
    bars_seen: std::sync::Arc<std::sync::Mutex<Vec<u64>>>,
}

impl OrderOnEveryBar {
    fn new() -> (Self, std::sync::Arc<std::sync::Mutex<Vec<u64>>>) {
        let bars_seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        (
            Self {
                bars_seen: bars_seen.clone(),
            },
            bars_seen,
        )
    }
}

impl Handler for OrderOnEveryBar {
    fn on_event(&mut self, event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        if !matches!(event, Event::Bar(_)) {
            return Ok(EngineOutput::None);
        }
        let at = event.ts_event().as_u64();
        self.bars_seen.lock().unwrap().push(at);
        Ok(EngineOutput::Orders(vec![market_order(
            &format!("O-{at}"),
            at,
        )]))
    }
}

/// An engine that submits one order per bar into a real execution sink.
fn trading_engine() -> (Engine, std::sync::Arc<std::sync::Mutex<Vec<u64>>>, Recorder) {
    let recorder = Recorder::new();
    let (strategy, bars_seen) = OrderOnEveryBar::new();
    let mut engine = Engine::new();
    let execution = BarFillEngine::new();
    engine.set_execution(Box::new(execution.clone()));
    engine.add_handler(execution);
    engine.add_handler(recorder.clone());
    engine.add_handler(strategy);
    (engine, bars_seen, recorder)
}

#[tokio::test]
async fn the_shell_drives_a_feed_to_exhaustion_and_drains_on_stop() {
    let recorder = Recorder::new();
    let mut engine = Engine::new();
    engine.add_handler(recorder.clone());
    let handle = EngineHandle::spawn(
        engine,
        Box::new(VecMessageFeed::new(bars(&[
            (101.0, 1),
            (102.0, 2),
            (103.0, 3),
        ]))) as BoxedFeed,
    );

    let audit = handle.shutdown_and_audit().await.unwrap();

    assert!(
        recorder.started(),
        "on_start must run before the first feed read"
    );
    assert_eq!(
        recorder.timestamps(),
        vec![1, 2, 3],
        "every message the feed delivered must reach a handler"
    );
    assert!(
        recorder.stopped(),
        "Stop must run on_stop so the audit stream is complete"
    );
    assert_eq!(
        dispatched(&audit),
        vec![1, 2, 3],
        "exactly one audit record per dispatched message"
    );
    assert_eq!(
        audit.iter().map(|record| record.seq).collect::<Vec<_>>(),
        (0..audit.len() as u64).collect::<Vec<_>>(),
        "audit sequence numbers must be contiguous from zero"
    );
}

#[tokio::test]
async fn the_kernel_orders_a_burst_the_feed_delivered_out_of_order() {
    let recorder = Recorder::new();
    let mut engine = Engine::new();
    engine.add_handler(recorder.clone());
    engine.inject(bar(103.0, 3));
    engine.inject(bar(101.0, 1));
    engine.inject(bar(102.0, 2));

    let handle = EngineHandle::spawn(engine, Box::new(VecMessageFeed::empty()) as BoxedFeed);
    let audit = handle.shutdown_and_audit().await.unwrap();

    assert_eq!(
        recorder.timestamps(),
        vec![1, 2, 3],
        "the kernel, not the transport, decides dispatch order"
    );
    assert_eq!(dispatched(&audit), vec![1, 2, 3]);
}

#[tokio::test]
async fn a_feed_that_goes_backwards_is_a_kernel_error_not_a_silent_reorder() {
    let recorder = Recorder::new();
    let mut engine = Engine::new();
    engine.add_handler(recorder.clone());
    let handle = EngineHandle::spawn(
        engine,
        Box::new(VecMessageFeed::new(bars(&[(102.0, 2), (101.0, 1)]))) as BoxedFeed,
    );

    let err = handle.shutdown_and_audit().await.unwrap_err();

    assert_eq!(
        err,
        AsyncError::Kernel(AlgoError::ClockRegression {
            current: 2,
            requested: 1,
        }),
        "a feed that regresses the clock is reported, not silently reordered"
    );
    assert_eq!(
        recorder.timestamps(),
        vec![2],
        "what was dispatched stays dispatched"
    );
}

#[tokio::test]
async fn stop_drains_the_queue_before_finishing() {
    let recorder = Recorder::new();
    let mut engine = Engine::new();
    engine.add_handler(recorder.clone());
    engine.inject(bar(103.0, 3));
    engine.inject(bar(101.0, 1));
    engine.inject(bar(102.0, 2));

    let (feed, _gate) = GatedFeed::new();
    let handle = EngineHandle::spawn_with_capacity(engine, Box::new(feed) as BoxedFeed, BARRIER);
    handle.submit(Command::Stop).await.unwrap();
    let audit = handle.join_and_audit().await.unwrap();

    assert_eq!(
        recorder.timestamps(),
        vec![1, 2, 3],
        "Stop must drain what is already in the kernel, not discard it"
    );
    assert_eq!(dispatched(&audit), vec![1, 2, 3]);
    assert_eq!(
        audit.iter().map(|record| record.seq).collect::<Vec<_>>(),
        (0..audit.len() as u64).collect::<Vec<_>>(),
        "a drained run leaves a contiguous audit trail"
    );
    assert!(
        recorder.stopped(),
        "on_stop runs after the drain, never instead of it"
    );
}

#[tokio::test]
async fn commands_are_bounded_and_apply_backpressure() {
    let (feed, _gate) = GatedFeed::new();
    let engine = Engine::new();
    let handle = EngineHandle::spawn_with_capacity(engine, Box::new(feed) as BoxedFeed, BARRIER);

    handle
        .try_submit(Command::State(TradingState::Reducing))
        .expect("the first command fits in an empty channel of capacity one");
    let err = handle
        .try_submit(Command::State(TradingState::Halted))
        .unwrap_err();
    assert_eq!(
        err,
        AsyncError::Rejected("the command channel is full".to_string()),
        "a full channel must be reported, never waited on"
    );

    handle
        .submit(Command::State(TradingState::Halted))
        .await
        .expect("submit must wait for room rather than fail");
    handle
        .submit(Command::Stop)
        .await
        .expect("Stop queues once the waiting command has been applied");

    let audit = handle.join_and_audit().await.unwrap();
    assert_eq!(
        state_changes(&audit),
        vec![
            (TradingState::Active, TradingState::Reducing),
            (TradingState::Reducing, TradingState::Halted),
        ],
        "the waiting submission reaches the engine after the one that filled the channel"
    );
}

#[tokio::test]
async fn state_command_halts_the_engine() {
    let (engine, bars_seen, recorder) = trading_engine();
    let (feed, gate) = GatedFeed::new();
    let handle = EngineHandle::spawn_with_capacity(engine, Box::new(feed) as BoxedFeed, BARRIER);

    gate.send(bar(101.0, 1)).await;
    handle
        .submit(Command::State(TradingState::Halted))
        .await
        .unwrap();
    handle.submit(Command::Stop).await.unwrap();
    gate.send(bar(102.0, 2)).await;

    let audit = handle.join_and_audit().await.unwrap();

    assert_eq!(
        *bars_seen.lock().unwrap(),
        vec![1, 2],
        "a halted engine still observes events"
    );
    assert_eq!(bars_seen_by(&recorder), vec![1, 2]);
    assert!(recorder.stopped());
    assert_eq!(
        order_verdicts(&audit),
        vec![
            "submitted O-1".to_string(),
            // ADR 0018 decision 5: the pre-gate reason is the ErrorCode spelling.
            "rejected O-2 (risk_trading_halted)".to_string(),
        ],
        "Halted must refuse orders instead of routing them"
    );
    assert_eq!(
        state_changes(&audit),
        vec![(TradingState::Active, TradingState::Halted)]
    );
}

#[tokio::test]
async fn state_command_resumes_a_halted_engine() {
    let (engine, bars_seen, recorder) = trading_engine();
    let (feed, gate) = GatedFeed::new();
    let handle = EngineHandle::spawn_with_capacity(engine, Box::new(feed) as BoxedFeed, BARRIER);

    handle
        .submit(Command::State(TradingState::Halted))
        .await
        .unwrap();
    handle
        .submit(Command::State(TradingState::Active))
        .await
        .expect("the halt is applied before the resume is queued");
    handle.submit(Command::Stop).await.unwrap();
    gate.send(bar(101.0, 1)).await;

    let audit = handle.join_and_audit().await.unwrap();

    assert_eq!(*bars_seen.lock().unwrap(), vec![1]);
    assert_eq!(bars_seen_by(&recorder), vec![1]);
    assert!(recorder.stopped());
    assert_eq!(
        state_changes(&audit),
        vec![
            (TradingState::Active, TradingState::Halted),
            (TradingState::Halted, TradingState::Active),
        ],
        "the halt really was applied, and then lifted"
    );
    assert_eq!(
        order_verdicts(&audit),
        vec!["submitted O-1".to_string()],
        "orders are submitted again once the engine is active"
    );
}

#[tokio::test]
async fn feed_errors_surface_as_async_error() {
    struct FailingFeed {
        items: std::collections::VecDeque<Message>,
    }

    #[async_trait]
    impl MarketDataFeed for FailingFeed {
        async fn subscribe(&mut self, _symbols: &[InstrumentId]) -> PortResult<()> {
            Ok(())
        }

        async fn unsubscribe(&mut self, _symbols: &[InstrumentId]) -> PortResult<()> {
            Ok(())
        }

        async fn next(&mut self) -> PortResult<Option<Message>> {
            match self.items.pop_front() {
                Some(msg) => Ok(Some(msg)),
                None => Err(PortError::Transport("websocket closed".to_string())),
            }
        }
    }

    let recorder = Recorder::new();
    let mut engine = Engine::new();
    engine.add_handler(recorder.clone());
    let handle = EngineHandle::spawn(
        engine,
        Box::new(FailingFeed {
            items: bars(&[(101.0, 1), (102.0, 2)]).into(),
        }) as BoxedFeed,
    );

    let err = handle.shutdown_and_audit().await.unwrap_err();

    assert_eq!(
        err,
        AsyncError::Feed(PortError::Transport("websocket closed".to_string())),
        "a broken stream is an error, not a silent end of feed"
    );
    assert_eq!(
        recorder.timestamps(),
        vec![1, 2],
        "everything delivered before the failure is dispatched"
    );
    assert!(
        recorder.stopped(),
        "on_stop must run even when the feed failed"
    );
}

#[tokio::test]
async fn a_panicking_handler_is_reported_not_swallowed() {
    struct Panicking;

    impl Handler for Panicking {
        fn on_event(&mut self, _event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
            panic!("handler exploded");
        }
    }

    let mut engine = Engine::new();
    engine.add_handler(Panicking);
    let handle = EngineHandle::spawn(
        engine,
        Box::new(VecMessageFeed::new(bars(&[(101.0, 1)]))) as BoxedFeed,
    );

    let err = handle.join().await.unwrap_err();

    assert_eq!(
        err,
        AsyncError::Panicked,
        "a panic in the task must be reported, never swallowed into Ok"
    );
}

#[tokio::test]
async fn a_kernel_error_reaches_the_caller_verbatim() {
    struct Failing;

    impl Handler for Failing {
        fn on_event(&mut self, _event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
            Err(AlgoError::Component("handler said no".to_string()))
        }
    }

    let recorder = Recorder::new();
    let mut engine = Engine::new();
    engine.add_handler(recorder);
    engine.add_handler(Failing);
    let handle = EngineHandle::spawn(
        engine,
        Box::new(VecMessageFeed::new(bars(&[(101.0, 1)]))) as BoxedFeed,
    );

    let err = handle.join().await.unwrap_err();

    assert_eq!(
        err,
        AsyncError::Kernel(AlgoError::Component("handler said no".to_string())),
        "a kernel failure must not be flattened into a string"
    );
}

async fn run_one(close: f64, timestamps: &[u64]) -> Vec<AuditRecord> {
    let recorder = Recorder::new();
    let (strategy, _) = OrderOnEveryBar::new();
    let mut engine = Engine::new();
    let execution = BarFillEngine::new();
    engine.set_execution(Box::new(execution.clone()));
    engine.add_handler(execution);
    engine.add_handler(recorder.clone());
    engine.add_handler(strategy);
    let messages: Vec<Message> = timestamps
        .iter()
        .map(|ts_event| bar(close + *ts_event as f64, *ts_event))
        .collect();
    let handle = EngineHandle::spawn(engine, Box::new(VecMessageFeed::new(messages)));
    let audit = handle.shutdown_and_audit().await.unwrap();
    assert!(recorder.stopped(), "every engine must run on_stop");
    audit
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_handle_is_one_engine() {
    let (audit_a, audit_b) = tokio::join!(run_one(101.0, &[1, 2, 3]), run_one(201.0, &[11, 12]));

    // ADR 0019 decision 1/5: each bar, then the submitter's `order` and the
    // fill's `order_filled`, all stamped with the bar's ts_event.
    assert_eq!(
        dispatched(&audit_a),
        vec![1, 1, 1, 2, 2, 2, 3, 3, 3],
        "each bar is dispatched and then its own order and fill acks, stamped with the same ts_event"
    );
    assert_eq!(
        dispatched(&audit_b),
        vec![11, 11, 11, 12, 12, 12],
        "the second engine sees only its own feed"
    );
    let contiguous = |audit: &[AuditRecord]| {
        audit.iter().map(|r| r.seq).collect::<Vec<_>>()
            == (0..audit.len() as u64).collect::<Vec<_>>()
    };
    assert!(
        contiguous(&audit_a) && contiguous(&audit_b),
        "each engine numbers its own audit from zero: {audit_a:?} / {audit_b:?}"
    );
    assert_eq!(
        order_verdicts(&audit_a),
        vec![
            "submitted O-1".to_string(),
            "submitted O-2".to_string(),
            "submitted O-3".to_string(),
        ],
        "the engines share no audit state"
    );
    assert_eq!(
        order_verdicts(&audit_b),
        vec!["submitted O-11".to_string(), "submitted O-12".to_string(),]
    );
}

/// One seeded run through [`EngineHandle`] on a fresh multi-thread runtime with `worker_threads`
/// workers, returning the serialised audit. The runtime is built on its own OS thread, so the
/// engine task really runs on that runtime's worker threads (`EngineHandle::spawn` is a
/// `tokio::spawn`), unlike the run service's own OS pool.
fn audit_under_worker_threads(worker_threads: usize) -> Vec<u8> {
    std::thread::Builder::new()
        .name(format!("determinism-{worker_threads}"))
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .enable_all()
                .build()
                .expect("a multi-thread runtime");
            runtime.block_on(async {
                const TIMESTAMPS: [u64; 6] = [1, 2, 3, 4, 5, 6];
                format!("{:?}", run_one(101.0, &TIMESTAMPS).await).into_bytes()
            })
        })
        .expect("a determinism thread")
        .join()
        .expect("the run must not panic")
}

/// The single-worker audit every other worker count must match byte for byte.
fn single_worker_audit() -> Vec<u8> {
    let audit = audit_under_worker_threads(1);
    assert!(
        audit.len() > 100,
        "audit suspiciously small: {} bytes",
        audit.len()
    );
    audit
}

#[test]
fn determinism_under_threads_one_worker() {
    assert_eq!(single_worker_audit(), audit_under_worker_threads(1));
}

#[test]
fn determinism_under_threads_four_workers() {
    assert_eq!(single_worker_audit(), audit_under_worker_threads(4));
}

#[test]
fn determinism_under_threads_sixteen_workers() {
    assert_eq!(single_worker_audit(), audit_under_worker_threads(16));
}
