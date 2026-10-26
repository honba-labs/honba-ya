//! The shared port contract suite, run from outside `honba-testing`.
//!
//! An adapter crate copies this file: implement the ports, then call the `check_*` functions
//! against the implementation to prove it honours the contract.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use honba_core::chrono::{DateTime, Utc};
use honba_core::{InstrumentId, OrderAck, OrderRequest, Price, Quantity, Tick};
use rust_decimal_macros::dec;

use honba_testing::fixtures::{any_instrument, instrument, utc_nanos};
use honba_testing::{
    CONTRACT_PROBE_KEY, CONTRACT_PROBE_SECRET, Clock, ExecutionGateway, FeedItem, FixedClock,
    InstrumentInfo, InstrumentKind, InstrumentMaster, MapInstrumentMaster, MarketDataFeed,
    PortError, PortResult, RecordingSink, SecretStore, Sink, StubGateway, StubSecretStore,
    VecMessageFeed, check_clock_contract, check_feed_contract, check_gateway_contract,
    check_master_contract, check_secret_contract, check_sink_contract,
};

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

fn instrument_meta(id: &InstrumentId) -> InstrumentInfo {
    InstrumentInfo::new(
        id.clone(),
        InstrumentKind::Equity,
        dec!(0.05),
        Quantity::from_u64(1),
    )
}

#[tokio::test]
async fn feed_implementation_passes_the_contract() {
    let mut feed = VecMessageFeed::new(vec![message(1), message(2)]);
    check_feed_contract(&mut feed).await.unwrap();
    assert_eq!(feed.subscriptions(), &[instrument("CONTRACT-PROBE")]);
    assert_eq!(feed.remaining(), 0);
}

#[tokio::test]
async fn sink_implementation_passes_the_contract() {
    let handle = RecordingSink::new();
    let mut sink = handle.clone();
    check_sink_contract(&mut sink).await.unwrap();
    assert_eq!(handle.items().len(), 3);
    assert_eq!(handle.flush_count(), 2);
}

#[tokio::test]
async fn clock_implementation_passes_the_contract() {
    let clock = FixedClock::new(utc_nanos(1_000));
    check_clock_contract(&clock).await.unwrap();
    assert!(clock.now().await >= utc_nanos(1_000 + 1_000_000));
}

#[tokio::test]
async fn master_implementation_passes_the_contract() {
    let master = MapInstrumentMaster::with([instrument_meta(&instrument("RELIANCE"))]);
    check_master_contract(&master).await.unwrap();
    assert_eq!(master.len(), 1);
}

#[tokio::test]
async fn gateway_implementation_passes_the_contract() {
    let mut gateway = StubGateway::new();
    check_gateway_contract(&mut gateway).await.unwrap();
    assert_eq!(gateway.submitted().len(), 2);
}

#[tokio::test]
async fn secret_implementation_passes_the_contract() {
    let store = StubSecretStore::with_probe_secret();
    check_secret_contract(&store).await.unwrap();
    assert_eq!(
        store.get_secret(CONTRACT_PROBE_KEY).await.unwrap(),
        Some(CONTRACT_PROBE_SECRET.into())
    );
}

struct BrokerFeed {
    feed: VecMessageFeed,
}

#[async_trait]
impl MarketDataFeed for BrokerFeed {
    async fn subscribe(&mut self, instruments: &[InstrumentId]) -> PortResult<()> {
        self.feed.subscribe(instruments).await
    }

    async fn unsubscribe(&mut self, instruments: &[InstrumentId]) -> PortResult<()> {
        self.feed.unsubscribe(instruments).await
    }

    async fn next(&mut self) -> PortResult<Option<FeedItem>> {
        self.feed.next().await
    }
}

struct BrokerSink {
    inner: RecordingSink,
}

#[async_trait]
impl Sink for BrokerSink {
    async fn write(&mut self, item: FeedItem) -> PortResult<()> {
        self.inner.write(item).await
    }

    async fn flush(&mut self) -> PortResult<()> {
        self.inner.flush().await
    }
}

struct BrokerGateway {
    inner: StubGateway,
}

#[async_trait]
impl ExecutionGateway for BrokerGateway {
    async fn submit_order(&mut self, order: OrderRequest) -> PortResult<OrderAck> {
        self.inner.submit_order(order).await
    }

    async fn cancel_order(&mut self, order_id: &str) -> PortResult<()> {
        self.inner.cancel_order(order_id).await
    }

    async fn modify_order(
        &mut self,
        order_id: &str,
        new_qty: Quantity,
        new_price: Option<Price>,
    ) -> PortResult<()> {
        self.inner.modify_order(order_id, new_qty, new_price).await
    }

    async fn next_fill(&mut self) -> PortResult<Option<honba_core::FillReport>> {
        self.inner.next_fill().await
    }
}

#[tokio::test]
async fn adapter_style_wrappers_pass_the_contract() {
    let mut feed = BrokerFeed {
        feed: VecMessageFeed::new(vec![message(1)]),
    };
    check_feed_contract(&mut feed).await.unwrap();

    let mut sink = BrokerSink {
        inner: RecordingSink::new(),
    };
    check_sink_contract(&mut sink).await.unwrap();

    let mut gateway = BrokerGateway {
        inner: StubGateway::new(),
    };
    check_gateway_contract(&mut gateway).await.unwrap();
}

struct FlakyBrokerGateway {
    inner: StubGateway,
}

#[async_trait]
impl ExecutionGateway for FlakyBrokerGateway {
    async fn submit_order(&mut self, _order: OrderRequest) -> PortResult<OrderAck> {
        Err(PortError::Unavailable("session expired".into()))
    }

    async fn cancel_order(&mut self, order_id: &str) -> PortResult<()> {
        self.inner.cancel_order(order_id).await
    }

    async fn modify_order(
        &mut self,
        order_id: &str,
        new_qty: Quantity,
        new_price: Option<Price>,
    ) -> PortResult<()> {
        self.inner.modify_order(order_id, new_qty, new_price).await
    }

    async fn next_fill(&mut self) -> PortResult<Option<honba_core::FillReport>> {
        self.inner.next_fill().await
    }
}

#[tokio::test]
async fn a_non_conforming_adapter_fails_the_contract() {
    let mut gateway = FlakyBrokerGateway {
        inner: StubGateway::new(),
    };
    let err = check_gateway_contract(&mut gateway)
        .await
        .expect_err("a gateway that rejects every order must not pass");
    assert!(
        err.to_string().contains("contract violated"),
        "expected a contract violation, got {err}"
    );
}

struct VendorMaster {
    inner: MapInstrumentMaster,
}

#[async_trait]
impl InstrumentMaster for VendorMaster {
    async fn get_instrument(&self, id: &InstrumentId) -> PortResult<Option<InstrumentInfo>> {
        self.inner.get_instrument(id).await
    }

    async fn list_instruments(&self) -> PortResult<Vec<InstrumentInfo>> {
        self.inner.list_instruments().await
    }
}

struct VendorClock {
    inner: FixedClock,
}

#[async_trait]
impl Clock for VendorClock {
    async fn now(&self) -> DateTime<Utc> {
        self.inner.now().await
    }

    async fn sleep(&self, dur: Duration) -> PortResult<()> {
        self.inner.sleep(dur).await
    }
}

struct VendorSecrets {
    inner: StubSecretStore,
}

#[async_trait]
impl SecretStore for VendorSecrets {
    async fn get_secret(&self, key: &str) -> PortResult<Option<String>> {
        self.inner.get_secret(key).await
    }
}

#[tokio::test]
async fn adapter_style_wrappers_pass_the_read_only_contracts() {
    let master = VendorMaster {
        inner: MapInstrumentMaster::with([instrument_meta(&any_instrument())]),
    };
    check_master_contract(&master).await.unwrap();

    let clock: Arc<VendorClock> = Arc::new(VendorClock {
        inner: FixedClock::new(utc_nanos(1)),
    });
    check_clock_contract(&*clock).await.unwrap();

    let secrets = VendorSecrets {
        inner: StubSecretStore::with_probe_secret(),
    };
    check_secret_contract(&secrets).await.unwrap();
}
