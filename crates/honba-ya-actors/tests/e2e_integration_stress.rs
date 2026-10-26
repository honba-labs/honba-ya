//! Phase 7.2 End-to-End System Integration & Stress Validation:
//! `honba-me` feed -> `honba-ya` actors -> `honba-mon` risk gate -> `honba-te` mock execution -> `honba-kao` chart display
//! Zero-data-loss stress test under 50,000 synthetic ticks/sec.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use tokio::sync::broadcast;

use honba_actors::*;
use honba_core::*;
use honba_mon::{OtrConfig, RiskEngine, RiskLimits};
use honba_ya_testing::MockBrokerExecutor;

struct IntegrationStrategy {
    instrument: InstrumentId,
    trigger_price: Price,
    orders_sent: usize,
    fills_received: usize,
}

#[async_trait]
impl Strategy for IntegrationStrategy {
    fn name(&self) -> &str {
        "E2EIntegrationStrategy"
    }

    fn algo_id(&self) -> &str {
        "e2e-algo-001"
    }

    async fn on_tick(&mut self, tick: &Tick) -> Vec<OrderRequest> {
        if let Some(price) = tick.last_price() {
            if price >= self.trigger_price && self.orders_sent < 5 {
                self.orders_sent += 1;
                return vec![OrderRequest::new(
                    self.instrument.clone(),
                    OrderSide::Buy,
                    OrderType::Limit(price),
                    Quantity::from_u64(10),
                    self.algo_id(),
                )];
            }
        }
        Vec::new()
    }

    async fn on_fill(&mut self, _fill: &FillReport) {
        self.fills_received += 1;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_e2e_full_pipeline_integration() {
    let instrument = InstrumentId::new(Exchange::Nse, "TCS");
    let trigger_price = Price::from_rupees("3500.00").unwrap();

    let strategy = IntegrationStrategy {
        instrument: instrument.clone(),
        trigger_price,
        orders_sent: 0,
        fills_received: 0,
    };

    let limits = RiskLimits {
        max_notional_per_order: Some(Decimal::from(500_000)),
        max_quantity_per_order: Some(Quantity::from_u64(100)),
        ..Default::default()
    };
    let mut risk_engine = RiskEngine::new(limits, OtrConfig::default());
    // Pre-register algo_id to comply with SEBI algo registry rule
    risk_engine.register_algo("e2e-algo-001");

    let broker = Arc::new(MockBrokerExecutor::new(true));

    let supervisor = Supervisor::spawn_with_strategy(
        strategy,
        risk_engine,
        broker.clone(),
        1024,
        RestartStrategy::OneForOne,
    );

    // Simulated honba-kao chart display subscriber
    let mut chart_display_rx = supervisor.resubscribe_ticks();

    // 1. Subscribe to market data
    supervisor
        .market_data_sender()
        .send(MarketDataMsg::Subscribe(instrument.clone()))
        .await
        .unwrap();

    // 2. Feed ticks from honba-me feed format
    let tick1 = Tick::Ltp {
        instrument: instrument.clone(),
        price: Price::from_rupees("3490.00").unwrap(),
        volume: Quantity::from_u64(100),
        exchange_ts: Utc::now(),
        receive_ts: Utc::now(),
    };
    supervisor
        .market_data_sender()
        .send(MarketDataMsg::IngestTick(tick1.clone()))
        .await
        .unwrap();

    // Verify honba-kao receives chart tick
    let displayed_tick1 = chart_display_rx.recv().await.unwrap();
    assert_eq!(displayed_tick1.instrument(), &instrument);
    assert_eq!(
        displayed_tick1.last_price(),
        Some(Price::from_rupees("3490.00").unwrap())
    );

    // 3. Send trigger tick -> Strategy generates order -> Risk checks -> Broker executes
    let tick2 = Tick::Ltp {
        instrument: instrument.clone(),
        price: Price::from_rupees("3505.00").unwrap(),
        volume: Quantity::from_u64(200),
        exchange_ts: Utc::now(),
        receive_ts: Utc::now(),
    };
    supervisor
        .market_data_sender()
        .send(MarketDataMsg::IngestTick(tick2.clone()))
        .await
        .unwrap();

    let displayed_tick2 = chart_display_rx.recv().await.unwrap();
    assert_eq!(
        displayed_tick2.last_price(),
        Some(Price::from_rupees("3505.00").unwrap())
    );

    // Allow async execution actor to process fill
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Verify mock execution received the order
    let history = broker.placed_orders().await;
    assert!(!history.is_empty(), "Broker should have executed orders");
    assert_eq!(history[0].instrument, instrument);
    assert_eq!(history[0].quantity, Quantity::from_u64(10));

    // 4. Graceful shutdown
    supervisor
        .shutdown_gracefully(std::time::Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_zero_data_loss_stress_50k_ticks() {
    let instrument = InstrumentId::new(Exchange::Nse, "INFY");
    let count = 50_000usize;

    // Direct MarketDataActor channel stress test for 50,000 ticks/sec zero data loss
    let (md_tx, md_rx) = tokio::sync::mpsc::channel(65_536);
    let (strat_tx, mut strat_rx) = tokio::sync::mpsc::channel(65_536);

    let (md_actor, mut chart_rx) = MarketDataActor::new(md_rx, vec![strat_tx], 65_536);
    let md_actor = md_actor.with_backpressure_policy(BackpressurePolicy::Passthrough);

    let actor_handle = tokio::spawn(async move {
        md_actor.run().await;
    });

    // Subscribe
    md_tx
        .send(MarketDataMsg::Subscribe(instrument.clone()))
        .await
        .unwrap();

    // Consumer tasks: Chart display & Strategy
    let chart_handle = tokio::spawn(async move {
        let mut received = 0usize;
        while received < count {
            match chart_rx.recv().await {
                Ok(_) => received += 1,
                Err(broadcast::error::RecvError::Lagged(lag)) => {
                    panic!("Chart subscriber lagged by {lag} ticks! Data loss detected.");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
        received
    });

    let strat_consumer_handle = tokio::spawn(async move {
        let mut received = 0usize;
        while received < count {
            if let Some(msg) = strat_rx.recv().await {
                if let StrategyMsg::OnTick(_) = msg {
                    received += 1;
                }
            } else {
                break;
            }
        }
        received
    });

    let start = Instant::now();

    // Produce 50,000 synthetic ticks at maximum throughput
    let now = Utc::now();
    for i in 0..count {
        let tick = Tick::Ltp {
            instrument: instrument.clone(),
            price: Price::from_rupees(&format!("{}.{:02}", 1500 + (i % 50), i % 100)).unwrap(),
            volume: Quantity::from_u64(100),
            exchange_ts: now,
            receive_ts: now,
        };
        md_tx.send(MarketDataMsg::IngestTick(tick)).await.unwrap();
    }

    let chart_received = chart_handle.await.unwrap();
    let strat_received = strat_consumer_handle.await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(
        chart_received, count,
        "Zero data loss failed: Chart display missed ticks"
    );
    assert_eq!(
        strat_received, count,
        "Zero data loss failed: Strategy actor missed ticks"
    );

    let throughput = (count as f64) / elapsed.as_secs_f64();
    println!(
        "\n=== Zero Data Loss Stress Test Passed ===\nProcessed {} ticks in {:?} ({:.0} ticks/sec)\n",
        count, elapsed, throughput
    );

    // Drain and shut down actor
    drop(md_tx);
    let _ = actor_handle.await;
}
