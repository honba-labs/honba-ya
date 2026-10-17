//! Fakes for the six async ports, and the shared contract suite every edge implementation must
//! pass.
//!
//! # Two things live here
//!
//! - **Fakes** ([`VecMessageFeed`], [`RecordingSink`], [`FixedClock`], [`MapInstrumentMaster`],
//!   [`StubGateway`], [`StubSecretStore`]): deterministic, in-memory implementations with readback,
//!   for tests that need a port to exist but not to be interesting.
//! - **The contract suite** ([`check_feed_contract`] and friends): generic checks that assert the
//!   behaviour every implementation owes its caller. An adapter crate implements the ports, points
//!   a `check_*` function at the implementation, and fails CI when the contract is broken. This is
//!   the shared port test suite; `tests/ports_contract.rs` is a worked example to copy.
//!
//! The checks take `&mut F`/`&F` over `?Sized`, so a registry-held `Box<dyn MarketDataFeed>` or
//! `Arc<dyn Clock>` can be checked directly.
//!
//! # What the checks can and cannot see
//!
//! A check only sees what the port trait exposes, so it verifies protocol behaviour: results,
//! exhaustion, monotonic time, id uniqueness, the round trip between `list_instruments` and
//! `get_instrument`. Completeness and ordering *inside* a sink, and the exact contents of a
//! broker's fills, need the implementation's own readback — every fake here exposes one for that
//! reason.
//!
//! A check returns [`PortError::Internal`], prefixed `port contract violated:`, for anything the
//! implementation did wrong — including an error the port itself returned, which is quoted inside
//! the message so the failure is diagnosable.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use honba_entities::{Instrument, Trade};
use honba_messages::{
    Event, Exchange, InstrumentId, Message, Order, OrderId, OrderSide, OrderType, TimeInForce,
    UnixNanos,
};
use honba_ports::{
    Clock, ExecutionGateway, InstrumentMaster, MarketDataFeed, PortError, PortResult, SecretStore,
    Sink,
};

use crate::fixtures;

/// The key [`check_secret_contract`] expects a secret store to be seeded with.
pub const CONTRACT_PROBE_KEY: &str = "honba-testing/contract-probe";

/// The value [`check_secret_contract`] expects under [`CONTRACT_PROBE_KEY`].
pub const CONTRACT_PROBE_SECRET: &str = "probe-secret";

const MAX_POLLS: usize = 64;
const PROBE_SYMBOL: &str = "CONTRACT-PROBE";
const PROBE_FILL_PRICE: f64 = 100.0;
const PROBE_SLEEP: Duration = Duration::from_millis(1);

fn violation(what: impl fmt::Display) -> PortError {
    PortError::Internal(format!("port contract violated: {what}"))
}

fn probe_message(ts: u64) -> Message {
    let at = UnixNanos::from_u64(ts);
    Message::new(
        Event::OrderCancelled {
            order_id: OrderId::new(format!("PROBE-{ts}")),
            ts_event: at,
        },
        at,
    )
}

fn probe_order(id: &str) -> Order {
    Order::new(
        OrderId::new(id),
        fixtures::any_instrument(),
        OrderSide::Buy,
        OrderType::Limit,
        10.0,
        Some(PROBE_FILL_PRICE),
        TimeInForce::Day,
        UnixNanos::from_u64(1),
        UnixNanos::from_u64(1),
    )
}

/// A [`MarketDataFeed`] that replays a fixed `Vec<Message>` and records subscriptions.
pub struct VecMessageFeed {
    items: VecDeque<Message>,
    subscribed: Vec<InstrumentId>,
    unsubscribed: Vec<InstrumentId>,
}

impl VecMessageFeed {
    /// Creates a feed that will yield `items` in order, then report exhaustion.
    pub fn new(items: Vec<Message>) -> Self {
        Self {
            items: items.into(),
            subscribed: Vec::new(),
            unsubscribed: Vec::new(),
        }
    }

    /// Creates a feed with nothing queued.
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    /// Returns every instrument passed to `subscribe`, in call order.
    pub fn subscriptions(&self) -> &[InstrumentId] {
        &self.subscribed
    }

    /// Returns every instrument passed to `unsubscribe`, in call order.
    pub fn unsubscriptions(&self) -> &[InstrumentId] {
        &self.unsubscribed
    }

    /// Returns the number of messages not yet yielded.
    pub fn remaining(&self) -> usize {
        self.items.len()
    }
}

#[async_trait]
impl MarketDataFeed for VecMessageFeed {
    async fn subscribe(&mut self, symbols: &[InstrumentId]) -> PortResult<()> {
        self.subscribed.extend_from_slice(symbols);
        Ok(())
    }

    async fn unsubscribe(&mut self, symbols: &[InstrumentId]) -> PortResult<()> {
        self.unsubscribed.extend_from_slice(symbols);
        Ok(())
    }

    async fn next(&mut self) -> PortResult<Option<Message>> {
        Ok(self.items.pop_front())
    }
}

/// A [`Sink`] that keeps every message it accepts, readable from any clone.
#[derive(Clone)]
pub struct RecordingSink {
    written: Arc<Mutex<Vec<Message>>>,
    flushes: Arc<AtomicUsize>,
}

impl RecordingSink {
    /// Creates an empty sink.
    pub fn new() -> Self {
        Self {
            written: Arc::new(Mutex::new(Vec::new())),
            flushes: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns every message written so far, in write order.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned by a panic in another thread.
    pub fn messages(&self) -> Vec<Message> {
        self.written.lock().expect("recording sink lock").clone()
    }

    /// Returns how many times `flush` has been called.
    pub fn flush_count(&self) -> usize {
        self.flushes.load(Ordering::SeqCst)
    }
}

impl Default for RecordingSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for RecordingSink {
    async fn write(&mut self, msg: Message) -> PortResult<()> {
        self.written.lock().expect("recording sink lock").push(msg);
        Ok(())
    }

    async fn flush(&mut self) -> PortResult<()> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A [`Clock`] that starts at a given instant and moves forward only when told to.
pub struct FixedClock {
    now: AtomicU64,
}

impl FixedClock {
    /// Creates a clock reading `start`.
    pub fn new(start: UnixNanos) -> Self {
        Self {
            now: AtomicU64::new(start.as_u64()),
        }
    }
}

#[async_trait]
impl Clock for FixedClock {
    async fn now(&self) -> UnixNanos {
        UnixNanos::from_u64(self.now.load(Ordering::SeqCst))
    }

    async fn sleep(&self, dur: Duration) -> PortResult<()> {
        let nanos = u64::try_from(dur.as_nanos()).unwrap_or(u64::MAX);
        self.now.fetch_add(nanos, Ordering::SeqCst);
        Ok(())
    }
}

/// An [`InstrumentMaster`] backed by an in-memory map.
#[derive(Default)]
pub struct MapInstrumentMaster {
    instruments: BTreeMap<InstrumentId, Instrument>,
}

impl MapInstrumentMaster {
    /// Creates an empty master.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a master holding `instruments`.
    pub fn with(instruments: impl IntoIterator<Item = Instrument>) -> Self {
        let mut master = Self::new();
        for instrument in instruments {
            master.insert(instrument);
        }
        master
    }

    /// Registers `instrument`, replacing any previous entry for the same id.
    pub fn insert(&mut self, instrument: Instrument) {
        self.instruments.insert(instrument.id().clone(), instrument);
    }

    /// Returns the number of registered instruments.
    pub fn len(&self) -> usize {
        self.instruments.len()
    }

    /// Returns `true` when no instrument is registered.
    pub fn is_empty(&self) -> bool {
        self.instruments.is_empty()
    }
}

#[async_trait]
impl InstrumentMaster for MapInstrumentMaster {
    async fn get_instrument(&self, id: &InstrumentId) -> PortResult<Option<Instrument>> {
        Ok(self.instruments.get(id).cloned())
    }

    async fn list_instruments(&self) -> PortResult<Vec<Instrument>> {
        Ok(self.instruments.values().cloned().collect())
    }
}

/// A [`SecretStore`] backed by an in-memory map.
#[derive(Default)]
pub struct StubSecretStore {
    secrets: BTreeMap<String, String>,
}

impl StubSecretStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a store seeded with the contract probe secret.
    ///
    /// Required by [`check_secret_contract`].
    pub fn with_probe_secret() -> Self {
        let mut store = Self::new();
        store.insert(CONTRACT_PROBE_KEY, CONTRACT_PROBE_SECRET);
        store
    }

    /// Stores `value` under `key`.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.secrets.insert(key.into(), value.into());
    }

    /// Returns the number of stored secrets.
    pub fn len(&self) -> usize {
        self.secrets.len()
    }

    /// Returns `true` when nothing is stored.
    pub fn is_empty(&self) -> bool {
        self.secrets.is_empty()
    }
}

#[async_trait]
impl SecretStore for StubSecretStore {
    async fn get_secret(&self, key: &str) -> PortResult<Option<String>> {
        Ok(self.secrets.get(key).cloned())
    }
}

/// An [`ExecutionGateway`] that numbers orders sequentially and fills once.
///
/// Each submission replaces the pending fill, so the next
/// [`next_fill`](ExecutionGateway::next_fill) yields exactly one fill for the most recently
/// submitted order, and every call after that returns `Ok(None)`. Cancels and modifies are
/// recorded rather than acted on.
pub struct StubGateway {
    next_id: u64,
    fill_price: f64,
    pending_fill: Option<(OrderId, Order)>,
    submitted: Vec<OrderId>,
    cancelled: Vec<OrderId>,
    modified: Vec<(OrderId, f64, Option<f64>)>,
}

impl StubGateway {
    /// Creates a gateway that fills at 100.0.
    pub fn new() -> Self {
        Self {
            next_id: 0,
            fill_price: PROBE_FILL_PRICE,
            pending_fill: None,
            submitted: Vec::new(),
            cancelled: Vec::new(),
            modified: Vec::new(),
        }
    }

    /// Creates a gateway that fills at `fill_price`.
    pub fn with_fill_price(fill_price: f64) -> Self {
        Self {
            fill_price,
            ..Self::new()
        }
    }

    /// Returns the ids assigned to submitted orders, in submission order.
    pub fn submitted(&self) -> &[OrderId] {
        &self.submitted
    }

    /// Returns the ids passed to `cancel_order`, in call order.
    pub fn cancellations(&self) -> &[OrderId] {
        &self.cancelled
    }

    /// Returns the `(id, quantity, price)` triples passed to `modify_order`, in call order.
    pub fn modifications(&self) -> &[(OrderId, f64, Option<f64>)] {
        &self.modified
    }

    /// Returns `1` while a fill is pending and `0` once the single fill has been taken.
    pub fn fills_remaining(&self) -> usize {
        usize::from(self.pending_fill.is_some())
    }
}

impl Default for StubGateway {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExecutionGateway for StubGateway {
    async fn submit_order(&mut self, order: Order) -> PortResult<OrderId> {
        self.next_id += 1;
        let sequence = self.next_id;
        let id = OrderId::new(format!("STUB-{sequence}"));
        self.submitted.push(id.clone());
        self.pending_fill = Some((id.clone(), order));
        Ok(id)
    }

    async fn cancel_order(&mut self, id: OrderId) -> PortResult<()> {
        self.cancelled.push(id);
        Ok(())
    }

    async fn modify_order(
        &mut self,
        id: OrderId,
        new_qty: f64,
        new_price: Option<f64>,
    ) -> PortResult<()> {
        self.modified.push((id, new_qty, new_price));
        Ok(())
    }

    async fn next_fill(&mut self) -> PortResult<Option<Trade>> {
        Ok(self.pending_fill.take().map(|(id, order)| {
            Trade::new(
                id,
                order.instrument_id().clone(),
                order.side(),
                order.quantity(),
                self.fill_price,
                honba_entities::Currency::Inr,
                order.ts_event(),
                order.ts_init(),
            )
        }))
    }
}

/// Asserts the [`MarketDataFeed`] contract: `subscribe` and `unsubscribe` succeed, the queued
/// messages are yielded, and the feed then reports exhaustion and stays exhausted.
///
/// The feed under test must be exhaustible (a fixture or recorded feed, not a live socket): a
/// feed that never returns `Ok(None)` is reported as a violation.
pub async fn check_feed_contract<F: MarketDataFeed + ?Sized>(feed: &mut F) -> PortResult<()> {
    let probe = fixtures::instrument(PROBE_SYMBOL);
    feed.subscribe(std::slice::from_ref(&probe))
        .await
        .map_err(|e| violation(format!("subscribe failed: {e}")))?;
    feed.unsubscribe(std::slice::from_ref(&probe))
        .await
        .map_err(|e| violation(format!("unsubscribe failed: {e}")))?;

    let mut seen = 0usize;
    loop {
        match feed.next().await {
            Ok(Some(_)) => {
                seen += 1;
                if seen > MAX_POLLS {
                    return Err(violation(format!(
                        "next() yielded more than {MAX_POLLS} messages without ever returning \
                         Ok(None); the feed under test must be exhaustible"
                    )));
                }
            }
            Ok(None) => break,
            Err(e) => return Err(violation(format!("next() failed: {e}"))),
        }
    }

    if seen == 0 {
        return Err(violation(
            "next() returned Ok(None) without ever yielding a message; the feed under test must \
             be seeded with at least one message",
        ));
    }

    match feed.next().await {
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err(violation("next() produced a message after exhaustion")),
        Err(e) => Err(violation(format!("next() failed after exhaustion: {e}"))),
    }
}

/// Asserts the [`Sink`] contract: writes are accepted, and `flush` succeeds and stays successful
/// when called again.
///
/// Completeness and ordering of what was written are invisible through the trait; check them with
/// the implementation's own readback.
pub async fn check_sink_contract<S: Sink + ?Sized>(sink: &mut S) -> PortResult<()> {
    for ts in 1..=3u64 {
        sink.write(probe_message(ts))
            .await
            .map_err(|e| violation(format!("write() failed: {e}")))?;
    }
    sink.flush()
        .await
        .map_err(|e| violation(format!("flush() failed: {e}")))?;
    sink.flush()
        .await
        .map_err(|e| violation(format!("second flush() failed: {e}")))?;
    Ok(())
}

/// Asserts the [`Clock`] contract: time never moves backwards, and `sleep` waits at least as long
/// as it was asked to.
pub async fn check_clock_contract<C: Clock + ?Sized>(clock: &C) -> PortResult<()> {
    let before = clock.now().await;
    clock
        .sleep(PROBE_SLEEP)
        .await
        .map_err(|e| violation(format!("sleep() failed: {e}")))?;
    let after = clock.now().await;

    if after < before {
        return Err(violation(format!(
            "now() moved backwards: {} then {}",
            before.as_u64(),
            after.as_u64()
        )));
    }

    let waited = after.as_u64().saturating_sub(before.as_u64());
    let requested = u64::try_from(PROBE_SLEEP.as_nanos()).unwrap_or(u64::MAX);
    if waited < requested {
        return Err(violation(format!(
            "sleep() returned after {waited}ns, less than the {requested}ns requested"
        )));
    }
    Ok(())
}

/// Asserts the [`InstrumentMaster`] contract: an empty listing is not an answer, every listed
/// instrument can be fetched back unchanged, and an unknown id is `Ok(None)` rather than an
/// invented instrument.
///
/// The master under test must hold at least one instrument.
pub async fn check_master_contract<M: InstrumentMaster + ?Sized>(master: &M) -> PortResult<()> {
    let listed = master
        .list_instruments()
        .await
        .map_err(|e| violation(format!("list_instruments() failed: {e}")))?;
    let Some(first) = listed.first() else {
        return Err(violation(
            "list_instruments() returned no instruments; the master under test must hold at least \
             one",
        ));
    };

    let fetched = master
        .get_instrument(first.id())
        .await
        .map_err(|e| violation(format!("get_instrument() failed: {e}")))?;
    match fetched {
        Some(found) if &found == first => {}
        Some(_) => {
            return Err(violation(format!(
                "get_instrument({}) returned a different instrument than list_instruments()",
                first.id()
            )))
        }
        None => {
            return Err(violation(format!(
                "get_instrument({}) returned Ok(None) for an instrument list_instruments() reports",
                first.id()
            )))
        }
    }

    let absent = InstrumentId::new(
        "HONBA-TESTING-ABSENT",
        Exchange::new(fixtures::TEST_EXCHANGE),
    );
    match master.get_instrument(&absent).await {
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err(violation(format!(
            "get_instrument({absent}) invented an instrument that was never registered"
        ))),
        Err(e) => Err(violation(format!(
            "get_instrument({absent}) must return Ok(None) for an unknown id, got: {e}"
        ))),
    }
}

/// Asserts the [`ExecutionGateway`] contract: a submission yields a non-empty id, two
/// submissions yield different ids, cancel and modify succeed, the submitted order is eventually
/// filled with sane numbers, and the fill stream then reports exhaustion.
///
/// The gateway under test must be able to fill the probe order at least once; a venue that
/// rejects everything cannot pass, and says so by failing the check.
pub async fn check_gateway_contract<G: ExecutionGateway + ?Sized>(
    gateway: &mut G,
) -> PortResult<()> {
    let first = gateway
        .submit_order(probe_order("PROBE-1"))
        .await
        .map_err(|e| violation(format!("submit_order() failed: {e}")))?;
    let second = gateway
        .submit_order(probe_order("PROBE-2"))
        .await
        .map_err(|e| violation(format!("second submit_order() failed: {e}")))?;

    if first.as_str().is_empty() {
        return Err(violation("submit_order() returned an empty order id"));
    }
    if first == second {
        return Err(violation(format!(
            "submit_order() reused the order id {} for two orders",
            first
        )));
    }

    gateway
        .cancel_order(first.clone())
        .await
        .map_err(|e| violation(format!("cancel_order() failed: {e}")))?;
    gateway
        .modify_order(second.clone(), 5.0, Some(PROBE_FILL_PRICE + 1.0))
        .await
        .map_err(|e| violation(format!("modify_order() failed: {e}")))?;

    let Some(fill) = gateway
        .next_fill()
        .await
        .map_err(|e| violation(format!("next_fill() failed: {e}")))?
    else {
        return Err(violation(
            "next_fill() returned Ok(None) for a submitted order; the gateway under test must be \
             able to fill the probe order",
        ));
    };
    let positive = |value: f64| value.is_finite() && value > 0.0;
    if !positive(fill.quantity()) || !positive(fill.price()) {
        return Err(violation(format!(
            "next_fill() returned a fill with quantity {} and price {}",
            fill.quantity(),
            fill.price()
        )));
    }

    match gateway
        .next_fill()
        .await
        .map_err(|e| violation(format!("next_fill() failed: {e}")))?
    {
        None => Ok(()),
        Some(extra) => Err(violation(format!(
            "next_fill() kept producing fills after the probe order was filled (order {}, \
             quantity {})",
            extra.order_id(),
            extra.quantity()
        ))),
    }
}

/// Asserts the [`SecretStore`] contract: the probe key resolves to a value, and a key that was
/// never stored is `Ok(None)` rather than an error or an invented value.
///
/// The store under test must be seeded with [`CONTRACT_PROBE_SECRET`] under
/// [`CONTRACT_PROBE_KEY`] — the one thing a secret store cannot be checked for without knowing
/// what is in it.
pub async fn check_secret_contract<S: SecretStore + ?Sized>(store: &S) -> PortResult<()> {
    let probe = store
        .get_secret(CONTRACT_PROBE_KEY)
        .await
        .map_err(|e| violation(format!("get_secret() failed: {e}")))?;
    match probe {
        Some(value) if !value.is_empty() => {}
        Some(_) => {
            return Err(violation(format!(
                "get_secret({CONTRACT_PROBE_KEY}) returned an empty value"
            )))
        }
        None => {
            return Err(violation(format!(
                "get_secret({CONTRACT_PROBE_KEY}) returned Ok(None); seed the store with \
                 {CONTRACT_PROBE_SECRET} under that key"
            )))
        }
    }

    match store.get_secret("honba-testing/contract-absent").await {
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err(violation(
            "get_secret() returned a value for a key that was never stored",
        )),
        Err(e) => Err(violation(format!(
            "get_secret() must return Ok(None) for an unknown key, got: {e}"
        ))),
    }
}
