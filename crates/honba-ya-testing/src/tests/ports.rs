//! Unit tests for `crate::ports`: the fakes' pinned behaviour, and the shared contract
//! harness proven to catch implementations that break the contract.

use std::time::Duration;

use async_trait::async_trait;
use honba_entities::{Currency, Instrument, InstrumentKind, Trade};
use honba_messages::{
    Event, InstrumentId, Message, Order, OrderId, OrderSide, OrderType, TimeInForce, UnixNanos,
};
use honba_ports::{
    Clock, ExecutionGateway, InstrumentMaster, MarketDataFeed, PortError, PortResult, SecretStore,
    Sink,
};

use crate::fixtures::{any_instrument, instrument};
use crate::ports::*;

fn message(ts: u64) -> Message {
    let t = UnixNanos::from_u64(ts);
    Message::new(
        Event::OrderCancelled {
            order_id: OrderId::new(format!("O-{ts}")),
            ts_event: t,
        },
        t,
    )
}

fn order(id: &str) -> Order {
    Order::new(
        OrderId::new(id),
        any_instrument(),
        OrderSide::Buy,
        OrderType::Limit,
        10.0,
        Some(100.0),
        TimeInForce::Day,
        UnixNanos::from_u64(1),
        UnixNanos::from_u64(1),
    )
}

fn instrument_meta(id: &InstrumentId) -> Instrument {
    Instrument::new(id.clone(), InstrumentKind::Equity, Currency::Inr, 1.0, 0.05)
}

fn assert_violation<T: std::fmt::Debug>(result: PortResult<T>, context: &str) {
    let err = result.expect_err(context);
    assert!(
        err.to_string().contains("contract violated"),
        "{context}: expected a contract violation, got {err}"
    );
}

struct DeadFeed;

#[async_trait]
impl MarketDataFeed for DeadFeed {
    async fn subscribe(&mut self, _symbols: &[InstrumentId]) -> PortResult<()> {
        Err(PortError::Transport("socket closed".into()))
    }

    async fn unsubscribe(&mut self, _symbols: &[InstrumentId]) -> PortResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> PortResult<Option<Message>> {
        Ok(None)
    }
}

struct EndlessFeed;

#[async_trait]
impl MarketDataFeed for EndlessFeed {
    async fn subscribe(&mut self, _symbols: &[InstrumentId]) -> PortResult<()> {
        Ok(())
    }

    async fn unsubscribe(&mut self, _symbols: &[InstrumentId]) -> PortResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> PortResult<Option<Message>> {
        Ok(Some(message(1)))
    }
}

struct LossySink;

#[async_trait]
impl Sink for LossySink {
    async fn write(&mut self, _msg: Message) -> PortResult<()> {
        Ok(())
    }

    async fn flush(&mut self) -> PortResult<()> {
        Err(PortError::Transport("journal offline".into()))
    }
}

struct RejectingSink;

#[async_trait]
impl Sink for RejectingSink {
    async fn write(&mut self, _msg: Message) -> PortResult<()> {
        Err(PortError::Internal("queue full".into()))
    }

    async fn flush(&mut self) -> PortResult<()> {
        Ok(())
    }
}

struct FrozenClock {
    now: UnixNanos,
}

#[async_trait]
impl Clock for FrozenClock {
    async fn now(&self) -> UnixNanos {
        self.now
    }

    async fn sleep(&self, _dur: Duration) -> PortResult<()> {
        Ok(())
    }
}

struct FailingClock;

#[async_trait]
impl Clock for FailingClock {
    async fn now(&self) -> UnixNanos {
        UnixNanos::default()
    }

    async fn sleep(&self, _dur: Duration) -> PortResult<()> {
        Err(PortError::Timeout)
    }
}

struct EmptyMaster;

#[async_trait]
impl InstrumentMaster for EmptyMaster {
    async fn get_instrument(&self, _id: &InstrumentId) -> PortResult<Option<Instrument>> {
        Ok(None)
    }

    async fn list_instruments(&self) -> PortResult<Vec<Instrument>> {
        Ok(Vec::new())
    }
}

struct LyingMaster {
    instruments: Vec<Instrument>,
}

#[async_trait]
impl InstrumentMaster for LyingMaster {
    async fn get_instrument(&self, _id: &InstrumentId) -> PortResult<Option<Instrument>> {
        Ok(None)
    }

    async fn list_instruments(&self) -> PortResult<Vec<Instrument>> {
        Ok(self.instruments.clone())
    }
}

struct InventingMaster;

#[async_trait]
impl InstrumentMaster for InventingMaster {
    async fn get_instrument(&self, id: &InstrumentId) -> PortResult<Option<Instrument>> {
        Ok(Some(instrument_meta(id)))
    }

    async fn list_instruments(&self) -> PortResult<Vec<Instrument>> {
        Ok(vec![instrument_meta(&any_instrument())])
    }
}

struct DuplicatingGateway {
    submitted: Vec<OrderId>,
}

#[async_trait]
impl ExecutionGateway for DuplicatingGateway {
    async fn submit_order(&mut self, _order: Order) -> PortResult<OrderId> {
        self.submitted.push(OrderId::new("ALWAYS-THE-SAME"));
        Ok(OrderId::new("ALWAYS-THE-SAME"))
    }

    async fn cancel_order(&mut self, _id: OrderId) -> PortResult<()> {
        Ok(())
    }

    async fn modify_order(
        &mut self,
        _id: OrderId,
        _new_qty: f64,
        _new_price: Option<f64>,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn next_fill(&mut self) -> PortResult<Option<Trade>> {
        Ok(None)
    }
}

struct EndlessGateway;

#[async_trait]
impl ExecutionGateway for EndlessGateway {
    async fn submit_order(&mut self, _order: Order) -> PortResult<OrderId> {
        Ok(OrderId::new("O-1"))
    }

    async fn cancel_order(&mut self, _id: OrderId) -> PortResult<()> {
        Ok(())
    }

    async fn modify_order(
        &mut self,
        _id: OrderId,
        _new_qty: f64,
        _new_price: Option<f64>,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn next_fill(&mut self) -> PortResult<Option<Trade>> {
        Ok(Some(Trade::new(
            OrderId::new("O-1"),
            any_instrument(),
            OrderSide::Buy,
            1.0,
            100.0,
            honba_entities::Currency::Inr,
            UnixNanos::from_u64(1),
            UnixNanos::from_u64(1),
        )))
    }
}

struct RefusingGateway;

#[async_trait]
impl ExecutionGateway for RefusingGateway {
    async fn submit_order(&mut self, _order: Order) -> PortResult<OrderId> {
        Err(PortError::Rejected {
            code: "E_MARGIN".into(),
            message: "insufficient funds".into(),
        })
    }

    async fn cancel_order(&mut self, _id: OrderId) -> PortResult<()> {
        Ok(())
    }

    async fn modify_order(
        &mut self,
        _id: OrderId,
        _new_qty: f64,
        _new_price: Option<f64>,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn next_fill(&mut self) -> PortResult<Option<Trade>> {
        Ok(None)
    }
}

struct FabricatingSecretStore;

#[async_trait]
impl SecretStore for FabricatingSecretStore {
    async fn get_secret(&self, key: &str) -> PortResult<Option<String>> {
        Ok(Some(format!("invented-{key}")))
    }
}

struct UnreachableSecretStore;

#[async_trait]
impl SecretStore for UnreachableSecretStore {
    async fn get_secret(&self, _key: &str) -> PortResult<Option<String>> {
        Err(PortError::Unavailable("vault offline".into()))
    }
}

#[tokio::test]
async fn vec_feed_drains_in_order_then_stays_exhausted() {
    let mut feed = VecMessageFeed::new(vec![message(3), message(1)]);

    assert_eq!(feed.next().await.unwrap(), Some(message(3)));
    assert_eq!(feed.next().await.unwrap(), Some(message(1)));
    assert_eq!(feed.next().await.unwrap(), None);
    assert_eq!(feed.next().await.unwrap(), None);
    assert_eq!(feed.remaining(), 0);
}

#[tokio::test]
async fn vec_feed_records_subscriptions_and_unsubscriptions() {
    let mut feed = VecMessageFeed::empty();
    let a = instrument("A");
    let b = instrument("B");

    feed.subscribe(std::slice::from_ref(&a)).await.unwrap();
    feed.subscribe(&[a.clone(), b.clone()]).await.unwrap();
    feed.unsubscribe(std::slice::from_ref(&a)).await.unwrap();

    assert_eq!(feed.subscriptions(), &[a.clone(), a, b]);
    assert_eq!(feed.unsubscriptions(), &[instrument("A")]);
}

#[tokio::test]
async fn recording_sink_reads_back_in_write_order() {
    let handle = RecordingSink::new();
    let mut sink = handle.clone();

    sink.write(message(1)).await.unwrap();
    sink.write(message(2)).await.unwrap();
    assert_eq!(sink.flush_count(), 0);
    sink.flush().await.unwrap();
    sink.flush().await.unwrap();

    assert_eq!(handle.messages(), vec![message(1), message(2)]);
    assert_eq!(handle.flush_count(), 2);
}

#[tokio::test]
async fn fixed_clock_starts_where_told_and_advances_on_sleep() {
    let clock = FixedClock::new(UnixNanos::from_u64(1_000));

    assert_eq!(clock.now().await, UnixNanos::from_u64(1_000));
    clock.sleep(Duration::from_millis(2)).await.unwrap();
    assert_eq!(clock.now().await, UnixNanos::from_u64(1_000 + 2_000_000));
    clock.sleep(Duration::from_nanos(1)).await.unwrap();
    assert_eq!(clock.now().await, UnixNanos::from_u64(1_000 + 2_000_001));
}

#[tokio::test]
async fn map_instrument_master_returns_registered_instruments() {
    let known = instrument_meta(&instrument("RELIANCE"));
    let other = instrument_meta(&instrument("TCS"));
    let master = MapInstrumentMaster::with([known.clone(), other.clone()]);

    assert_eq!(master.len(), 2);
    assert!(!master.is_empty());
    assert_eq!(
        master.get_instrument(known.id()).await.unwrap(),
        Some(known)
    );
    assert_eq!(
        master.get_instrument(&instrument("MISSING")).await.unwrap(),
        None
    );
    assert_eq!(master.list_instruments().await.unwrap().len(), 2);
}

#[tokio::test]
async fn stub_secret_store_seeds_the_contract_probe() {
    let store = StubSecretStore::with_probe_secret();

    assert_eq!(store.len(), 1);
    assert_eq!(
        store.get_secret(CONTRACT_PROBE_KEY).await.unwrap(),
        Some(CONTRACT_PROBE_SECRET.to_string())
    );
    assert_eq!(store.get_secret("never/seeded").await.unwrap(), None);
}

#[tokio::test]
async fn stub_gateway_numbers_orders_and_fills_once() {
    let mut gateway = StubGateway::new();

    let first = gateway.submit_order(order("O-1")).await.unwrap();
    let second = gateway.submit_order(order("O-2")).await.unwrap();
    gateway.cancel_order(first.clone()).await.unwrap();
    gateway
        .modify_order(second.clone(), 4.0, Some(101.25))
        .await
        .unwrap();

    assert_eq!(first, OrderId::new("STUB-1"));
    assert_eq!(second, OrderId::new("STUB-2"));
    assert_eq!(
        gateway.submitted(),
        &[OrderId::new("STUB-1"), OrderId::new("STUB-2")]
    );
    assert_eq!(gateway.cancellations(), &[OrderId::new("STUB-1")]);
    assert_eq!(
        gateway.modifications(),
        &[(OrderId::new("STUB-2"), 4.0, Some(101.25))]
    );

    let fill = gateway.next_fill().await.unwrap().expect("one fill");
    assert_eq!(fill.order_id(), &second);
    assert_eq!(fill.quantity(), 10.0);
    assert_eq!(fill.price(), 100.0);
    assert_eq!(gateway.next_fill().await.unwrap(), None);
    assert_eq!(gateway.fills_remaining(), 0);
}

#[tokio::test]
async fn stub_gateway_honours_a_custom_fill_price() {
    let mut gateway = StubGateway::with_fill_price(250.75);
    gateway.submit_order(order("O-1")).await.unwrap();

    let fill = gateway.next_fill().await.unwrap().expect("one fill");
    assert_eq!(fill.price(), 250.75);
}

#[tokio::test]
async fn harness_accepts_every_fake() {
    let mut feed = VecMessageFeed::new(vec![message(1)]);
    check_feed_contract(&mut feed).await.unwrap();

    let mut sink = RecordingSink::new();
    check_sink_contract(&mut sink).await.unwrap();

    let clock = FixedClock::new(UnixNanos::from_u64(1));
    check_clock_contract(&clock).await.unwrap();

    let master = MapInstrumentMaster::with([instrument_meta(&any_instrument())]);
    check_master_contract(&master).await.unwrap();

    let mut gateway = StubGateway::new();
    check_gateway_contract(&mut gateway).await.unwrap();

    let secrets = StubSecretStore::with_probe_secret();
    check_secret_contract(&secrets).await.unwrap();
}

#[tokio::test]
async fn harness_runs_against_trait_objects() {
    let mut feed: Box<dyn MarketDataFeed> = Box::new(VecMessageFeed::new(vec![message(1)]));
    check_feed_contract(&mut *feed).await.unwrap();

    let mut sink: Box<dyn Sink> = Box::new(RecordingSink::new());
    check_sink_contract(&mut *sink).await.unwrap();

    let mut gateway: Box<dyn ExecutionGateway> = Box::new(StubGateway::new());
    check_gateway_contract(&mut *gateway).await.unwrap();

    let clock: std::sync::Arc<dyn Clock> =
        std::sync::Arc::new(FixedClock::new(UnixNanos::default()));
    check_clock_contract(&*clock).await.unwrap();

    let master: std::sync::Arc<dyn InstrumentMaster> = std::sync::Arc::new(
        MapInstrumentMaster::with([instrument_meta(&any_instrument())]),
    );
    check_master_contract(&*master).await.unwrap();

    let secrets: std::sync::Arc<dyn SecretStore> =
        std::sync::Arc::new(StubSecretStore::with_probe_secret());
    check_secret_contract(&*secrets).await.unwrap();
}

#[tokio::test]
async fn feed_contract_rejects_a_failing_subscribe() {
    assert_violation(
        check_feed_contract(&mut DeadFeed).await,
        "a feed that cannot subscribe violates the contract",
    );
}

#[tokio::test]
async fn feed_contract_rejects_a_feed_that_never_exhausts() {
    assert_violation(
        check_feed_contract(&mut EndlessFeed).await,
        "a feed that never returns Ok(None) violates the contract",
    );
}

#[tokio::test]
async fn sink_contract_rejects_a_failing_flush() {
    assert_violation(
        check_sink_contract(&mut LossySink).await,
        "a sink whose flush fails violates the contract",
    );
}

#[tokio::test]
async fn sink_contract_rejects_a_failing_write() {
    assert_violation(
        check_sink_contract(&mut RejectingSink).await,
        "a sink whose write fails violates the contract",
    );
}

#[tokio::test]
async fn clock_contract_rejects_a_sleep_that_does_not_advance_time() {
    assert_violation(
        check_clock_contract(&FrozenClock {
            now: UnixNanos::from_u64(5),
        })
        .await,
        "a clock whose sleep returns without waiting violates the contract",
    );
}

#[tokio::test]
async fn clock_contract_rejects_a_failing_sleep() {
    assert_violation(
        check_clock_contract(&FailingClock).await,
        "a clock whose sleep fails violates the contract",
    );
}

#[tokio::test]
async fn master_contract_rejects_an_empty_master() {
    assert_violation(
        check_master_contract(&EmptyMaster).await,
        "a master with no instruments violates the contract",
    );
}

#[tokio::test]
async fn master_contract_rejects_a_lookup_that_lies() {
    assert_violation(
        check_master_contract(&LyingMaster {
            instruments: vec![instrument_meta(&any_instrument())],
        })
        .await,
        "a master that cannot find a listed instrument violates the contract",
    );
}

#[tokio::test]
async fn master_contract_rejects_an_invented_instrument() {
    assert_violation(
        check_master_contract(&InventingMaster).await,
        "a master that invents unknown instruments violates the contract",
    );
}

#[tokio::test]
async fn gateway_contract_rejects_reused_order_ids() {
    assert_violation(
        check_gateway_contract(&mut DuplicatingGateway {
            submitted: Vec::new(),
        })
        .await,
        "a gateway that reuses an order id violates the contract",
    );
}

#[tokio::test]
async fn gateway_contract_rejects_a_gateway_that_never_fills() {
    assert_violation(
        check_gateway_contract(&mut EndlessGateway).await,
        "a gateway that never reports a fill violates the contract",
    );
}

#[tokio::test]
async fn gateway_contract_rejects_a_rejecting_venue() {
    assert_violation(
        check_gateway_contract(&mut RefusingGateway).await,
        "a gateway that rejects every order violates the contract",
    );
}

#[tokio::test]
async fn secret_contract_rejects_a_store_without_the_probe_key() {
    assert_violation(
        check_secret_contract(&StubSecretStore::new()).await,
        "a store without the contract probe key violates the contract",
    );
}

#[tokio::test]
async fn secret_contract_rejects_an_unreachable_store() {
    assert_violation(
        check_secret_contract(&UnreachableSecretStore).await,
        "a store that errors on every key violates the contract",
    );
}

#[tokio::test]
async fn secret_contract_rejects_a_store_that_invents_secrets() {
    assert_violation(
        check_secret_contract(&FabricatingSecretStore).await,
        "a store that returns a value for any key violates the contract",
    );
}

#[tokio::test]
async fn contract_probe_key_is_absent_from_every_store_built_fresh() {
    let store = StubSecretStore::new();
    assert!(store.is_empty());
    assert_eq!(
        store.get_secret(CONTRACT_PROBE_KEY).await.unwrap(),
        None,
        "the probe key is only present in a store seeded with with_probe_secret()"
    );
}

#[test]
fn probe_key_is_namespaced_so_it_cannot_collide_with_a_venue_key() {
    assert!(CONTRACT_PROBE_KEY.starts_with("honba-testing/"));
    assert!(!CONTRACT_PROBE_SECRET.is_empty());
}
