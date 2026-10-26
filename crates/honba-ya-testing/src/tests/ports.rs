//! Unit tests for `crate::ports`: the fakes' pinned behaviour, and the shared contract
//! harness proven to catch implementations that break the contract.

use std::time::Duration;

use async_trait::async_trait;
use honba_core::chrono::{DateTime, Utc};
use honba_core::uuid::Uuid;
use honba_core::{
    FillReport, InstrumentId, OrderAck, OrderRequest, OrderSide, OrderStatus, OrderType, Price,
    Quantity, Tick,
};
use rust_decimal_macros::dec;

use crate::fixtures::{any_instrument, instrument, utc_nanos};
use crate::ports::*;

fn message(ts: u64) -> FeedItem {
    let at = utc_nanos(ts);
    FeedItem::Tick(Tick::Ltp {
        instrument: instrument("PROBE"),
        price: Price::new(dec!(100)),
        volume: Quantity::from_u64(1),
        exchange_ts: at,
        receive_ts: at,
    })
}

fn order(tag: &str) -> OrderRequest {
    let mut order = OrderRequest::new(
        any_instrument(),
        OrderSide::Buy,
        OrderType::Limit(Price::new(dec!(100))),
        Quantity::from_u64(10),
        "port-contract",
    );
    order.tag = Some(tag.to_string());
    order
}

fn instrument_meta(id: &InstrumentId) -> InstrumentInfo {
    InstrumentInfo::new(
        id.clone(),
        InstrumentKind::Equity,
        dec!(0.05),
        Quantity::from_u64(1),
    )
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
    async fn subscribe(&mut self, _instruments: &[InstrumentId]) -> PortResult<()> {
        Err(PortError::Transport("socket closed".into()))
    }

    async fn unsubscribe(&mut self, _instruments: &[InstrumentId]) -> PortResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> PortResult<Option<FeedItem>> {
        Ok(None)
    }
}

struct EndlessFeed;

#[async_trait]
impl MarketDataFeed for EndlessFeed {
    async fn subscribe(&mut self, _instruments: &[InstrumentId]) -> PortResult<()> {
        Ok(())
    }

    async fn unsubscribe(&mut self, _instruments: &[InstrumentId]) -> PortResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> PortResult<Option<FeedItem>> {
        Ok(Some(message(1)))
    }
}

struct LossySink;

#[async_trait]
impl Sink for LossySink {
    async fn write(&mut self, _item: FeedItem) -> PortResult<()> {
        Ok(())
    }

    async fn flush(&mut self) -> PortResult<()> {
        Err(PortError::Transport("journal offline".into()))
    }
}

struct RejectingSink;

#[async_trait]
impl Sink for RejectingSink {
    async fn write(&mut self, _item: FeedItem) -> PortResult<()> {
        Err(PortError::Internal("queue full".into()))
    }

    async fn flush(&mut self) -> PortResult<()> {
        Ok(())
    }
}

struct FrozenClock {
    now: DateTime<Utc>,
}

#[async_trait]
impl Clock for FrozenClock {
    async fn now(&self) -> DateTime<Utc> {
        self.now
    }

    async fn sleep(&self, _dur: Duration) -> PortResult<()> {
        Ok(())
    }
}

struct FailingClock;

#[async_trait]
impl Clock for FailingClock {
    async fn now(&self) -> DateTime<Utc> {
        utc_nanos(0)
    }

    async fn sleep(&self, _dur: Duration) -> PortResult<()> {
        Err(PortError::Timeout)
    }
}

struct EmptyMaster;

#[async_trait]
impl InstrumentMaster for EmptyMaster {
    async fn get_instrument(&self, _id: &InstrumentId) -> PortResult<Option<InstrumentInfo>> {
        Ok(None)
    }

    async fn list_instruments(&self) -> PortResult<Vec<InstrumentInfo>> {
        Ok(Vec::new())
    }
}

struct LyingMaster {
    instruments: Vec<InstrumentInfo>,
}

#[async_trait]
impl InstrumentMaster for LyingMaster {
    async fn get_instrument(&self, _id: &InstrumentId) -> PortResult<Option<InstrumentInfo>> {
        Ok(None)
    }

    async fn list_instruments(&self) -> PortResult<Vec<InstrumentInfo>> {
        Ok(self.instruments.clone())
    }
}

struct InventingMaster;

#[async_trait]
impl InstrumentMaster for InventingMaster {
    async fn get_instrument(&self, id: &InstrumentId) -> PortResult<Option<InstrumentInfo>> {
        Ok(Some(instrument_meta(id)))
    }

    async fn list_instruments(&self) -> PortResult<Vec<InstrumentInfo>> {
        Ok(vec![instrument_meta(&any_instrument())])
    }
}

struct DuplicatingGateway;

#[async_trait]
impl ExecutionGateway for DuplicatingGateway {
    async fn submit_order(&mut self, order: OrderRequest) -> PortResult<OrderAck> {
        Ok(OrderAck::new(
            "ALWAYS-THE-SAME",
            order.client_order_id,
            OrderStatus::New,
            utc_nanos(0),
        ))
    }

    async fn cancel_order(&mut self, _order_id: &str) -> PortResult<()> {
        Ok(())
    }

    async fn modify_order(
        &mut self,
        _order_id: &str,
        _new_qty: Quantity,
        _new_price: Option<Price>,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn next_fill(&mut self) -> PortResult<Option<FillReport>> {
        Ok(None)
    }
}

struct EndlessGateway;

#[async_trait]
impl ExecutionGateway for EndlessGateway {
    async fn submit_order(&mut self, _order: OrderRequest) -> PortResult<OrderAck> {
        Ok(OrderAck::new(
            "O-1",
            Uuid::nil(),
            OrderStatus::New,
            utc_nanos(0),
        ))
    }

    async fn cancel_order(&mut self, _order_id: &str) -> PortResult<()> {
        Ok(())
    }

    async fn modify_order(
        &mut self,
        _order_id: &str,
        _new_qty: Quantity,
        _new_price: Option<Price>,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn next_fill(&mut self) -> PortResult<Option<FillReport>> {
        Ok(Some(FillReport::new(
            "F-1",
            "O-1",
            Uuid::nil(),
            any_instrument(),
            OrderSide::Buy,
            Price::new(dec!(100)),
            Quantity::from_u64(1),
            "port-contract",
            utc_nanos(1),
        )))
    }
}

struct RefusingGateway;

#[async_trait]
impl ExecutionGateway for RefusingGateway {
    async fn submit_order(&mut self, _order: OrderRequest) -> PortResult<OrderAck> {
        Err(PortError::Rejected {
            code: "E_MARGIN".into(),
            message: "insufficient funds".into(),
        })
    }

    async fn cancel_order(&mut self, _order_id: &str) -> PortResult<()> {
        Ok(())
    }

    async fn modify_order(
        &mut self,
        _order_id: &str,
        _new_qty: Quantity,
        _new_price: Option<Price>,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn next_fill(&mut self) -> PortResult<Option<FillReport>> {
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

    assert_eq!(handle.items(), vec![message(1), message(2)]);
    assert_eq!(handle.flush_count(), 2);
}

#[tokio::test]
async fn fixed_clock_starts_where_told_and_advances_on_sleep() {
    let clock = FixedClock::new(utc_nanos(1_000));

    assert_eq!(clock.now().await, utc_nanos(1_000));
    clock.sleep(Duration::from_millis(2)).await.unwrap();
    assert_eq!(clock.now().await, utc_nanos(1_000 + 2_000_000));
    clock.sleep(Duration::from_nanos(1)).await.unwrap();
    assert_eq!(clock.now().await, utc_nanos(1_000 + 2_000_001));
}

#[tokio::test]
async fn map_instrument_master_returns_registered_instruments() {
    let known = instrument_meta(&instrument("RELIANCE"));
    let other = instrument_meta(&instrument("TCS"));
    let master = MapInstrumentMaster::with([known.clone(), other.clone()]);

    assert_eq!(master.len(), 2);
    assert!(!master.is_empty());
    assert_eq!(master.get_instrument(&known.id).await.unwrap(), Some(known));
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
    gateway.cancel_order(&first.order_id).await.unwrap();
    gateway
        .modify_order(
            &second.order_id,
            Quantity::from_u64(4),
            Some(Price::new(dec!(101.25))),
        )
        .await
        .unwrap();

    assert_eq!(first.order_id, "STUB-1");
    assert_eq!(second.order_id, "STUB-2");
    assert_eq!(
        gateway.submitted(),
        &["STUB-1".to_string(), "STUB-2".to_string()]
    );
    assert_eq!(gateway.cancellations(), &["STUB-1".to_string()]);
    assert_eq!(
        gateway.modifications(),
        &[(
            "STUB-2".to_string(),
            Quantity::from_u64(4),
            Some(Price::new(dec!(101.25)))
        )]
    );

    let fill = gateway.next_fill().await.unwrap().expect("one fill");
    assert_eq!(fill.order_id, second.order_id);
    assert_eq!(fill.quantity, Quantity::from_u64(10));
    assert_eq!(fill.price, Price::new(dec!(100)));
    assert!(gateway.next_fill().await.unwrap().is_none());
    assert_eq!(gateway.fills_remaining(), 0);
}

#[tokio::test]
async fn stub_gateway_honours_a_custom_fill_price() {
    let mut gateway = StubGateway::with_fill_price(Price::new(dec!(250.75)));
    gateway.submit_order(order("O-1")).await.unwrap();

    let fill = gateway.next_fill().await.unwrap().expect("one fill");
    assert_eq!(fill.price, Price::new(dec!(250.75)));
}

#[tokio::test]
async fn harness_accepts_every_fake() {
    let mut feed = VecMessageFeed::new(vec![message(1)]);
    check_feed_contract(&mut feed).await.unwrap();

    let mut sink = RecordingSink::new();
    check_sink_contract(&mut sink).await.unwrap();

    let clock = FixedClock::new(utc_nanos(1));
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

    let clock: std::sync::Arc<dyn Clock> = std::sync::Arc::new(FixedClock::new(utc_nanos(0)));
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
        check_clock_contract(&FrozenClock { now: utc_nanos(5) }).await,
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
        check_gateway_contract(&mut DuplicatingGateway).await,
        "a gateway that reuses an order id violates the contract",
    );
}

#[tokio::test]
async fn gateway_contract_rejects_a_gateway_that_never_fills() {
    assert_violation(
        check_gateway_contract(&mut EndlessGateway).await,
        "a gateway that never reports exhaustion of the fill stream violates the contract",
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
