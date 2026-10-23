//! Port traits over canonical [`honba_core`] types, the in-memory fakes that implement them,
//! and the shared contract suite every edge implementation must pass.
//!
//! # Two things live here
//!
//! - **Traits and fakes**: [`MarketDataFeed`], [`Sink`], [`Clock`], [`InstrumentMaster`],
//!   [`ExecutionGateway`] and [`SecretStore`] with deterministic, in-memory implementations
//!   ([`VecMessageFeed`], [`RecordingSink`], [`FixedClock`], [`MapInstrumentMaster`],
//!   [`StubGateway`], [`StubSecretStore`]), each exposing readback for tests that need a port to
//!   exist but not to be interesting.
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
use honba_core::chrono::{DateTime, Utc};
use honba_core::rust_decimal::Decimal;
use honba_core::{
    Bar, Exchange, FillReport, InstrumentId, OrderAck, OrderRequest, OrderSide, OrderStatus,
    OrderType, Price, Quantity, Tick,
};

use crate::fixtures;

/// Errors a port implementation may surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortError {
    /// The transport under the port failed (socket closed, stream reset).
    Transport(String),
    /// The backing service is temporarily unreachable (vault offline, session expired).
    Unavailable(String),
    /// The venue or policy rejected the request with a machine-readable code.
    Rejected {
        /// Venue/policy error code.
        code: String,
        /// Human-readable rejection reason.
        message: String,
    },
    /// The operation exceeded its time budget.
    Timeout,
    /// The port does not support this operation at all.
    Unsupported(String),
    /// The implementation broke its own contract or failed internally.
    Internal(String),
}

impl fmt::Display for PortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(m) => write!(f, "transport error: {m}"),
            Self::Unavailable(m) => write!(f, "unavailable: {m}"),
            Self::Rejected { code, message } => write!(f, "rejected ({code}): {message}"),
            Self::Timeout => write!(f, "timed out"),
            Self::Unsupported(m) => write!(f, "unsupported: {m}"),
            Self::Internal(m) => write!(f, "internal error: {m}"),
        }
    }
}

impl std::error::Error for PortError {}

/// Result alias used by every port trait in this module.
pub type PortResult<T> = Result<T, PortError>;

/// One market-data item yielded by a [`MarketDataFeed`] and accepted by a [`Sink`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedItem {
    /// A canonical broker tick.
    Tick(Tick),
    /// An aggregated OHLCV bar.
    Bar(Bar),
}

impl FeedItem {
    /// The item's event timestamp: receive time for ticks, bar timestamp for bars.
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::Tick(t) => t.receive_ts(),
            Self::Bar(b) => b.timestamp,
        }
    }

    /// The instrument the item belongs to.
    pub fn instrument(&self) -> &InstrumentId {
        match self {
            Self::Tick(t) => t.instrument(),
            Self::Bar(b) => &b.instrument,
        }
    }
}

/// Coarse instrument class held by an [`InstrumentMaster`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InstrumentKind {
    /// Exchange-traded equity share.
    Equity,
    /// Market index.
    Index,
    /// Futures contract.
    Future,
    /// Options contract.
    Option,
    /// Commodity contract.
    Commodity,
    /// Currency contract.
    Currency,
}

/// Reference-data metadata for one instrument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentInfo {
    /// Canonical instrument identifier.
    pub id: InstrumentId,
    /// Instrument class.
    pub kind: InstrumentKind,
    /// Minimum price increment.
    pub tick_size: Decimal,
    /// Minimum tradable quantity increment.
    pub lot_size: Quantity,
}

impl InstrumentInfo {
    /// Creates reference data for `id`.
    pub fn new(
        id: InstrumentId,
        kind: InstrumentKind,
        tick_size: Decimal,
        lot_size: Quantity,
    ) -> Self {
        Self {
            id,
            kind,
            tick_size,
            lot_size,
        }
    }
}

/// Replayable market-data source.
#[async_trait]
pub trait MarketDataFeed {
    /// Subscribes to `instruments`; implementations record the call.
    async fn subscribe(&mut self, instruments: &[InstrumentId]) -> PortResult<()>;
    /// Unsubscribes from `instruments`; implementations record the call.
    async fn unsubscribe(&mut self, instruments: &[InstrumentId]) -> PortResult<()>;
    /// Yields the next item in order, or `Ok(None)` once exhausted.
    async fn next(&mut self) -> PortResult<Option<FeedItem>>;
}

/// Append-only destination for market data.
#[async_trait]
pub trait Sink {
    /// Accepts one item into the sink.
    async fn write(&mut self, item: FeedItem) -> PortResult<()>;
    /// Flushes buffered items to the backing store.
    async fn flush(&mut self) -> PortResult<()>;
}

/// Event-time source for deterministic runs.
#[async_trait]
pub trait Clock {
    /// Current time.
    async fn now(&self) -> DateTime<Utc>;
    /// Advances time by at least `dur`.
    async fn sleep(&self, dur: Duration) -> PortResult<()>;
}

/// Reference-data lookup for canonical instruments.
#[async_trait]
pub trait InstrumentMaster {
    /// Resolves `id`, or `Ok(None)` when it was never registered.
    async fn get_instrument(&self, id: &InstrumentId) -> PortResult<Option<InstrumentInfo>>;
    /// Lists every registered instrument.
    async fn list_instruments(&self) -> PortResult<Vec<InstrumentInfo>>;
}

/// Order submission, cancellation and the fill stream.
#[async_trait]
pub trait ExecutionGateway {
    /// Submits `order` and returns the venue acknowledgement.
    async fn submit_order(&mut self, order: OrderRequest) -> PortResult<OrderAck>;
    /// Cancels the venue order `order_id`.
    async fn cancel_order(&mut self, order_id: &str) -> PortResult<()>;
    /// Modifies quantity and/or limit price of the venue order `order_id`.
    async fn modify_order(
        &mut self,
        order_id: &str,
        new_qty: Quantity,
        new_price: Option<Price>,
    ) -> PortResult<()>;
    /// Yields the next fill, or `Ok(None)` when the fill stream is exhausted.
    async fn next_fill(&mut self) -> PortResult<Option<FillReport>>;
}

/// Keyed secret lookup.
#[async_trait]
pub trait SecretStore {
    /// Resolves `key`, or `Ok(None)` when it was never stored.
    async fn get_secret(&self, key: &str) -> PortResult<Option<String>>;
}

/// The key [`check_secret_contract`] expects a secret store to be seeded with.
pub const CONTRACT_PROBE_KEY: &str = "honba-testing/contract-probe";

/// The value [`check_secret_contract`] expects under [`CONTRACT_PROBE_KEY`].
pub const CONTRACT_PROBE_SECRET: &str = "probe-secret";

const MAX_POLLS: usize = 64;
const PROBE_SYMBOL: &str = "CONTRACT-PROBE";
const PROBE_FILL_PRICE: i32 = 100;
const PROBE_SLEEP: Duration = Duration::from_millis(1);

fn violation(what: impl fmt::Display) -> PortError {
    PortError::Internal(format!("port contract violated: {what}"))
}

fn probe_price() -> Price {
    Price::new(Decimal::from(PROBE_FILL_PRICE))
}

fn probe_message(ts: u64) -> FeedItem {
    let at = fixtures::utc_nanos(ts);
    FeedItem::Tick(Tick::Ltp {
        instrument: fixtures::instrument(PROBE_SYMBOL),
        price: probe_price(),
        volume: Quantity::from_u64(1),
        exchange_ts: at,
        receive_ts: at,
    })
}

fn probe_order(tag: &str) -> OrderRequest {
    let mut order = OrderRequest::new(
        fixtures::any_instrument(),
        OrderSide::Buy,
        OrderType::Limit(probe_price()),
        Quantity::from_u64(10),
        "port-contract",
    );
    order.tag = Some(tag.to_string());
    order
}

/// A [`MarketDataFeed`] that replays a fixed `Vec<FeedItem>` and records subscriptions.
pub struct VecMessageFeed {
    items: VecDeque<FeedItem>,
    subscribed: Vec<InstrumentId>,
    unsubscribed: Vec<InstrumentId>,
}

impl VecMessageFeed {
    /// Creates a feed that will yield `items` in order, then report exhaustion.
    pub fn new(items: Vec<FeedItem>) -> Self {
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

    /// Returns the number of items not yet yielded.
    pub fn remaining(&self) -> usize {
        self.items.len()
    }
}

#[async_trait]
impl MarketDataFeed for VecMessageFeed {
    async fn subscribe(&mut self, instruments: &[InstrumentId]) -> PortResult<()> {
        self.subscribed.extend_from_slice(instruments);
        Ok(())
    }

    async fn unsubscribe(&mut self, instruments: &[InstrumentId]) -> PortResult<()> {
        self.unsubscribed.extend_from_slice(instruments);
        Ok(())
    }

    async fn next(&mut self) -> PortResult<Option<FeedItem>> {
        Ok(self.items.pop_front())
    }
}

/// A [`Sink`] that keeps every item it accepts, readable from any clone.
#[derive(Clone)]
pub struct RecordingSink {
    written: Arc<Mutex<Vec<FeedItem>>>,
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

    /// Returns every item written so far, in write order.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned by a panic in another thread.
    pub fn items(&self) -> Vec<FeedItem> {
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
    async fn write(&mut self, item: FeedItem) -> PortResult<()> {
        self.written.lock().expect("recording sink lock").push(item);
        Ok(())
    }

    async fn flush(&mut self) -> PortResult<()> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A [`Clock`] that starts at a given instant and moves forward only when told to.
pub struct FixedClock {
    now_nanos: AtomicU64,
}

impl FixedClock {
    /// Creates a clock reading `start`.
    pub fn new(start: DateTime<Utc>) -> Self {
        Self {
            now_nanos: AtomicU64::new(start.timestamp_nanos_opt().unwrap_or(0) as u64),
        }
    }
}

#[async_trait]
impl Clock for FixedClock {
    async fn now(&self) -> DateTime<Utc> {
        fixtures::utc_from_nanos(self.now_nanos.load(Ordering::SeqCst) as i64)
    }

    async fn sleep(&self, dur: Duration) -> PortResult<()> {
        let nanos = u64::try_from(dur.as_nanos()).unwrap_or(u64::MAX);
        self.now_nanos.fetch_add(nanos, Ordering::SeqCst);
        Ok(())
    }
}

/// An [`InstrumentMaster`] backed by an in-memory map.
///
/// Keyed by the canonical `InstrumentId` display form (`"EXCHANGE:SYMBOL"`)
/// because `InstrumentId` itself derives no `Ord`.
#[derive(Default)]
pub struct MapInstrumentMaster {
    instruments: BTreeMap<String, InstrumentInfo>,
}

impl MapInstrumentMaster {
    /// Creates an empty master.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a master holding `instruments`.
    pub fn with(instruments: impl IntoIterator<Item = InstrumentInfo>) -> Self {
        let mut master = Self::new();
        for instrument in instruments {
            master.insert(instrument);
        }
        master
    }

    /// Registers `instrument`, replacing any previous entry for the same id.
    pub fn insert(&mut self, instrument: InstrumentInfo) {
        self.instruments
            .insert(instrument.id.to_string(), instrument);
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
    async fn get_instrument(&self, id: &InstrumentId) -> PortResult<Option<InstrumentInfo>> {
        Ok(self.instruments.get(&id.to_string()).cloned())
    }

    async fn list_instruments(&self) -> PortResult<Vec<InstrumentInfo>> {
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
/// recorded rather than acted on. Acknowledgements carry a deterministic epoch timestamp.
pub struct StubGateway {
    next_id: u64,
    fill_seq: u64,
    fill_price: Price,
    pending_fill: Option<(String, OrderRequest)>,
    submitted: Vec<String>,
    cancelled: Vec<String>,
    modified: Vec<(String, Quantity, Option<Price>)>,
}

impl StubGateway {
    /// Creates a gateway that fills at 100.
    pub fn new() -> Self {
        Self {
            next_id: 0,
            fill_seq: 0,
            fill_price: probe_price(),
            pending_fill: None,
            submitted: Vec::new(),
            cancelled: Vec::new(),
            modified: Vec::new(),
        }
    }

    /// Creates a gateway that fills at `fill_price`.
    pub fn with_fill_price(fill_price: Price) -> Self {
        Self {
            fill_price,
            ..Self::new()
        }
    }

    /// Returns the venue ids assigned to submitted orders, in submission order.
    pub fn submitted(&self) -> &[String] {
        &self.submitted
    }

    /// Returns the ids passed to `cancel_order`, in call order.
    pub fn cancellations(&self) -> &[String] {
        &self.cancelled
    }

    /// Returns the `(id, quantity, price)` triples passed to `modify_order`, in call order.
    pub fn modifications(&self) -> &[(String, Quantity, Option<Price>)] {
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
    async fn submit_order(&mut self, order: OrderRequest) -> PortResult<OrderAck> {
        self.next_id += 1;
        let venue_id = format!("STUB-{}", self.next_id);
        let ack = OrderAck::new(
            venue_id.as_str(),
            order.client_order_id,
            OrderStatus::New,
            fixtures::utc_nanos(0),
        );
        self.submitted.push(venue_id.clone());
        self.pending_fill = Some((venue_id, order));
        Ok(ack)
    }

    async fn cancel_order(&mut self, order_id: &str) -> PortResult<()> {
        self.cancelled.push(order_id.to_string());
        Ok(())
    }

    async fn modify_order(
        &mut self,
        order_id: &str,
        new_qty: Quantity,
        new_price: Option<Price>,
    ) -> PortResult<()> {
        self.modified
            .push((order_id.to_string(), new_qty, new_price));
        Ok(())
    }

    async fn next_fill(&mut self) -> PortResult<Option<FillReport>> {
        let Some((venue_id, order)) = self.pending_fill.take() else {
            return Ok(None);
        };
        self.fill_seq += 1;
        let fill = FillReport::new(
            format!("{venue_id}-F{}", self.fill_seq),
            venue_id,
            order.client_order_id,
            order.instrument,
            order.side,
            self.fill_price,
            order.quantity,
            order.algo_id,
            fixtures::utc_nanos(0),
        );
        Ok(Some(fill))
    }
}

/// Asserts the [`MarketDataFeed`] contract: `subscribe` and `unsubscribe` succeed, the queued
/// items are yielded, and the feed then reports exhaustion and stays exhausted.
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
                        "next() yielded more than {MAX_POLLS} items without ever returning \
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
            "next() returned Ok(None) without ever yielding an item; the feed under test must \
             be seeded with at least one item",
        ));
    }

    match feed.next().await {
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err(violation("next() produced an item after exhaustion")),
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

    let before_nanos = before.timestamp_nanos_opt().unwrap_or(0);
    let after_nanos = after.timestamp_nanos_opt().unwrap_or(0);

    if after < before {
        return Err(violation(format!(
            "now() moved backwards: {} then {}",
            before_nanos, after_nanos
        )));
    }

    let waited = after_nanos - before_nanos;
    let requested = i64::try_from(PROBE_SLEEP.as_nanos()).unwrap_or(i64::MAX);
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
        .get_instrument(&first.id)
        .await
        .map_err(|e| violation(format!("get_instrument() failed: {e}")))?;
    match fetched {
        Some(found) if &found == first => {}
        Some(_) => {
            return Err(violation(format!(
                "get_instrument({}) returned a different instrument than list_instruments()",
                first.id
            )));
        }
        None => {
            return Err(violation(format!(
                "get_instrument({}) returned Ok(None) for an instrument list_instruments() reports",
                first.id
            )));
        }
    }

    let absent = InstrumentId::new(Exchange::Nse, "HONBA-TESTING-ABSENT");
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

/// Asserts the [`ExecutionGateway`] contract: a submission yields a non-empty venue order id, two
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

    if first.order_id.is_empty() {
        return Err(violation("submit_order() returned an empty order id"));
    }
    if first.order_id == second.order_id {
        return Err(violation(format!(
            "submit_order() reused the order id {} for two orders",
            first.order_id
        )));
    }

    gateway
        .cancel_order(&first.order_id)
        .await
        .map_err(|e| violation(format!("cancel_order() failed: {e}")))?;
    gateway
        .modify_order(
            &second.order_id,
            Quantity::from_u64(5),
            Some(Price::new(Decimal::from(PROBE_FILL_PRICE + 1))),
        )
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
    if !(fill.price.is_positive() && fill.quantity.is_positive()) {
        return Err(violation(format!(
            "next_fill() returned a fill with quantity {} and price {}",
            fill.quantity, fill.price
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
            extra.order_id, extra.quantity
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
            )));
        }
        None => {
            return Err(violation(format!(
                "get_secret({CONTRACT_PROBE_KEY}) returned Ok(None); seed the store with \
                 {CONTRACT_PROBE_SECRET} under that key"
            )));
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
