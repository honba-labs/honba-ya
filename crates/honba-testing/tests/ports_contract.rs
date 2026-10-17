//! The shared port contract suite, run from outside `honba-testing`.
//!
//! An adapter crate copies this file: implement the ports, then call the `check_*` functions
//! against the implementation to prove it honours the contract.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use honba_entities::{Currency, Instrument, InstrumentKind, Trade};
use honba_messages::{Event, InstrumentId, Message, Order, OrderId, UnixNanos};
use honba_ports::{
    Clock, ExecutionGateway, InstrumentMaster, MarketDataFeed, PortError, PortResult, SecretStore,
    Sink,
};
use honba_testing::fixtures::{any_instrument, instrument};
use honba_testing::{
    check_clock_contract, check_feed_contract, check_gateway_contract, check_master_contract,
    check_secret_contract, check_sink_contract, FixedClock, MapInstrumentMaster, RecordingSink,
    StubGateway, StubSecretStore, VecMessageFeed, CONTRACT_PROBE_KEY, CONTRACT_PROBE_SECRET,
};

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

fn instrument_meta(id: &InstrumentId) -> Instrument {
    Instrument::new(id.clone(), InstrumentKind::Equity, Currency::Inr, 1.0, 0.05)
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
    assert_eq!(handle.messages().len(), 3);
    assert_eq!(handle.flush_count(), 2);
}

#[tokio::test]
async fn clock_implementation_passes_the_contract() {
    let clock = FixedClock::new(UnixNanos::from_u64(1_000));
    check_clock_contract(&clock).await.unwrap();
    assert!(clock.now().await >= UnixNanos::from_u64(1_000 + 1_000_000));
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
    async fn subscribe(&mut self, symbols: &[InstrumentId]) -> PortResult<()> {
        self.feed.subscribe(symbols).await
    }

    async fn unsubscribe(&mut self, symbols: &[InstrumentId]) -> PortResult<()> {
        self.feed.unsubscribe(symbols).await
    }

    async fn next(&mut self) -> PortResult<Option<Message>> {
        self.feed.next().await
    }
}

struct BrokerSink {
    inner: RecordingSink,
}

#[async_trait]
impl Sink for BrokerSink {
    async fn write(&mut self, msg: Message) -> PortResult<()> {
        self.inner.write(msg).await
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
    async fn submit_order(&mut self, order: Order) -> PortResult<OrderId> {
        self.inner.submit_order(order).await
    }

    async fn cancel_order(&mut self, id: OrderId) -> PortResult<()> {
        self.inner.cancel_order(id).await
    }

    async fn modify_order(
        &mut self,
        id: OrderId,
        new_qty: f64,
        new_price: Option<f64>,
    ) -> PortResult<()> {
        self.inner.modify_order(id, new_qty, new_price).await
    }

    async fn next_fill(&mut self) -> PortResult<Option<Trade>> {
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
    async fn submit_order(&mut self, _order: Order) -> PortResult<OrderId> {
        Err(PortError::Unavailable("session expired".into()))
    }

    async fn cancel_order(&mut self, id: OrderId) -> PortResult<()> {
        self.inner.cancel_order(id).await
    }

    async fn modify_order(
        &mut self,
        id: OrderId,
        new_qty: f64,
        new_price: Option<f64>,
    ) -> PortResult<()> {
        self.inner.modify_order(id, new_qty, new_price).await
    }

    async fn next_fill(&mut self) -> PortResult<Option<Trade>> {
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
    async fn get_instrument(&self, id: &InstrumentId) -> PortResult<Option<Instrument>> {
        self.inner.get_instrument(id).await
    }

    async fn list_instruments(&self) -> PortResult<Vec<Instrument>> {
        self.inner.list_instruments().await
    }
}

struct VendorClock {
    inner: FixedClock,
}

#[async_trait]
impl Clock for VendorClock {
    async fn now(&self) -> UnixNanos {
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
        inner: FixedClock::new(UnixNanos::from_u64(1)),
    });
    check_clock_contract(&*clock).await.unwrap();

    let secrets = VendorSecrets {
        inner: StubSecretStore::with_probe_secret(),
    };
    check_secret_contract(&secrets).await.unwrap();
}
