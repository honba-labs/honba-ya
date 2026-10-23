use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use honba_actors::{*, RestartStrategy};
use honba_core::*;
use honba_mon::{OtrConfig, RiskEngine, RiskLimits, RiskRefusal};
use honba_ya_testing::MockBrokerExecutor;
use honba_te_core::BrokerError;

struct SimpleBreakoutStrategy {
    symbol: InstrumentId,
    threshold: Price,
    placed: bool,
}

#[async_trait]
impl Strategy for SimpleBreakoutStrategy {
    fn name(&self) -> &str {
        "SimpleBreakoutStrategy"
    }

    fn algo_id(&self) -> &str {
        "algo-breakout-1"
    }

    async fn on_tick(&mut self, tick: &Tick) -> Vec<OrderRequest> {
        if self.placed {
            return Vec::new();
        }

        if let Some(price) = tick.last_price() {
            if price >= self.threshold {
                self.placed = true;
                return vec![OrderRequest::new(
                    self.symbol.clone(),
                    OrderSide::Buy,
                    OrderType::Limit(price),
                    Quantity::from_u64(10),
                    self.algo_id(),
                )];
            }
        }
        Vec::new()
    }
}

#[tokio::test]
async fn test_actor_runtime_end_to_end_pipeline() {
    let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let threshold = Price::from_rupees("2500.00").unwrap();

    let strategy = SimpleBreakoutStrategy {
        symbol: inst.clone(),
        threshold,
        placed: false,
    };

    let limits = RiskLimits {
        max_notional_per_order: Some(rust_decimal::Decimal::from(1_000_000)),
        ..Default::default()
    };
    let risk_engine = RiskEngine::new(limits, OtrConfig::default());
    let broker = Arc::new(MockBrokerExecutor::new(true));

    let supervisor = Supervisor::spawn_with_strategy(strategy, risk_engine, broker, 64, RestartStrategy::OneForOne);
    let mut tick_rx = supervisor.resubscribe_ticks();

    // 1. Subscribe to the instrument first
    supervisor
        .market_data_sender()
        .send(MarketDataMsg::Subscribe(inst.clone()))
        .await
        .unwrap();

    // 2. Send tick below threshold -> no order
    let t1 = Tick::Ltp {
        instrument: inst.clone(),
        price: Price::from_rupees("2490.00").unwrap(),
        volume: Quantity::from_u64(100),
        exchange_ts: Utc::now(),
        receive_ts: Utc::now(),
    };
    supervisor
        .market_data_sender()
        .send(MarketDataMsg::IngestTick(t1.clone()))
        .await
        .unwrap();

    let r_t1 = tick_rx.recv().await.unwrap();
    assert_eq!(r_t1.instrument(), &inst);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 3. Send tick meeting threshold -> triggers order -> passes risk -> fills at broker
    let t2 = Tick::Ltp {
        instrument: inst.clone(),
        price: Price::from_rupees("2505.00").unwrap(),
        volume: Quantity::from_u64(150),
        exchange_ts: Utc::now(),
        receive_ts: Utc::now(),
    };
    supervisor
        .market_data_sender()
        .send(MarketDataMsg::IngestTick(t2.clone()))
        .await
        .unwrap();

    let r_t2 = tick_rx.recv().await.unwrap();
    assert_eq!(
        r_t2.last_price(),
        Some(Price::from_rupees("2505.00").unwrap())
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 4. Graceful staged drain and shutdown
    supervisor
        .shutdown_gracefully(Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_risk_actor_blocks_out_of_band_orders() {
    let inst = InstrumentId::new(Exchange::Nse, "TCS");
    let limits = RiskLimits::default();
    let risk_engine = RiskEngine::new(limits, OtrConfig::default());
    let broker = Arc::new(MockBrokerExecutor::new(false));

    let (risk_tx, risk_rx) = mpsc::channel(16);
    let (exec_tx, exec_rx) = mpsc::channel(16);

    let risk_actor = RiskActor::new(risk_engine, risk_rx, exec_tx);
    let exec_actor = ExecutionActor::new(broker, exec_rx, None, None);

    let risk_handle = tokio::spawn(async move { risk_actor.run().await });
    let exec_handle = tokio::spawn(async move { exec_actor.run().await });

    // Order with empty algo_id should be refused by SEBI algo-id check
    let bad_req = OrderRequest::new(
        inst.clone(),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("3500.00").unwrap()),
        Quantity::from_u64(10),
        "", // missing algo_id
    );

    let (reply_tx, reply_rx) = oneshot::channel();
    risk_tx
        .send(RiskMsg::EvaluateOrder {
            request: bad_req,
            now: Utc::now(),
            reply_to: reply_tx,
        })
        .await
        .unwrap();

    let res = reply_rx.await.unwrap();
    assert!(matches!(
        res,
        Err(BrokerError::RiskRefusal(r)) if matches!(*r, RiskRefusal::InvalidOrder { .. })
    ));

    // Global kill switch test
    risk_tx
        .send(RiskMsg::HaltGlobal {
            reason: "emergency halt".into(),
        })
        .await
        .unwrap();

    let valid_req = OrderRequest::new(
        inst,
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("3500.00").unwrap()),
        Quantity::from_u64(10),
        "algo-valid-1",
    );

    let (reply_tx2, reply_rx2) = oneshot::channel();
    risk_tx
        .send(RiskMsg::EvaluateOrder {
            request: valid_req,
            now: Utc::now(),
            reply_to: reply_tx2,
        })
        .await
        .unwrap();

    let res2 = reply_rx2.await.unwrap();
    assert!(matches!(
        res2,
        Err(BrokerError::RiskRefusal(r)) if matches!(*r, RiskRefusal::GlobalHalted { .. })
    ));

    // Shutdown
    risk_tx.send(RiskMsg::Shutdown).await.unwrap();
    let _ = tokio::join!(risk_handle, exec_handle);
}

#[tokio::test]
async fn test_market_data_backpressure_policies() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let (md_tx, md_rx) = mpsc::channel(64);
    let (strat_tx, mut strat_rx) = mpsc::channel(64);

    let (actor, mut tick_rx) = MarketDataActor::new(md_rx, vec![strat_tx], 64);
    let actor = actor
        .with_max_buffer_size(2)
        .with_backpressure_policy(BackpressurePolicy::DropNewest)
        .with_coalesce_interval_ms(20);

    let actor_handle = tokio::spawn(async move { actor.run().await });

    // 1. Subscribe
    md_tx.send(MarketDataMsg::Subscribe(inst.clone())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    // 2. Send 4 ticks quickly: first 2 fill buffer, 3rd and 4th dropped by DropNewest
    for p in ["1500.00", "1505.00", "1510.00", "1520.00"] {
        let tick = Tick::Ltp {
            instrument: inst.clone(),
            price: Price::from_rupees(p).unwrap(),
            volume: Quantity::from_u64(10),
            exchange_ts: Utc::now(),
            receive_ts: Utc::now(),
        };
        md_tx.send(MarketDataMsg::IngestTick(tick)).await.unwrap();
    }

    // Wait for coalesce timer to flush buffer
    tokio::time::sleep(Duration::from_millis(50)).await;

    // We should receive coalesced tick from broadcast
    let broadcasted = tick_rx.recv().await.unwrap();
    assert_eq!(broadcasted.instrument(), &inst);

    // Shutdown actor
    md_tx.send(MarketDataMsg::Shutdown).await.unwrap();
    let status = actor_handle.await.unwrap();
    assert_eq!(status, ActorStatus::Stopped);
}

#[tokio::test]
async fn test_supervisor_restart_strategies() {
    let inst = InstrumentId::new(Exchange::Nse, "SBIN");
    let strategy = SimpleBreakoutStrategy {
        symbol: inst.clone(),
        threshold: Price::from_rupees("500.00").unwrap(),
        placed: false,
    };
    let risk_engine = RiskEngine::default();
    let broker = Arc::new(MockBrokerExecutor::new(true));

    let mut supervisor = Supervisor::spawn_with_strategy(
        strategy,
        risk_engine,
        broker,
        32,
        RestartStrategy::OneForOne,
    );

    assert_eq!(supervisor.restart_strategy(), &RestartStrategy::OneForOne);

    // Test OneForOne restart of market data
    let dummy_handle = tokio::spawn(async { ActorStatus::Running });
    supervisor.restart_actor_handle("market_data", dummy_handle);
    let (md_r, strat_r, risk_r, exec_r) = supervisor.restart_counts();
    assert_eq!(md_r, 1);
    assert_eq!(strat_r, 0);

    // Switch strategy to AllForOne
    supervisor.set_restart_strategy(RestartStrategy::AllForOne);
    assert_eq!(supervisor.restart_strategy(), &RestartStrategy::AllForOne);

    let dummy_handle2 = tokio::spawn(async { ActorStatus::Running });
    supervisor.restart_actor_handle("execution", dummy_handle2);
    let (_, _, _, exec_r2) = supervisor.restart_counts();
    assert_eq!(exec_r2, 1);

    // Graceful stop
    let _ = supervisor.shutdown_gracefully(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn test_execution_actor_clean_shutdown_drain_loop() {
    let broker = Arc::new(MockBrokerExecutor::new(true));
    let (exec_tx, exec_rx) = mpsc::channel(16);
    let exec_actor = ExecutionActor::new(broker, exec_rx, None, None);

    let handle = tokio::spawn(async move { exec_actor.run().await });

    let inst = InstrumentId::new(Exchange::Nse, "TATAMOTORS");
    let req = OrderRequest::new(
        inst,
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("950.00").unwrap()),
        Quantity::from_u64(10),
        "algo-drain-test",
    );

    let (reply_tx, reply_rx) = oneshot::channel();

    // Send order and immediately send shutdown so order is drained in-flight
    exec_tx
        .send(ExecutionMsg::ExecuteOrder {
            request: req,
            reply_to: reply_tx,
        })
        .await
        .unwrap();

    exec_tx.send(ExecutionMsg::Shutdown).await.unwrap();

    // Verify reply was processed and received during the drain loop
    let ack = reply_rx.await.unwrap().expect("ack received during drain");
    assert_eq!(ack.status, OrderStatus::New);

    let final_status = handle.await.unwrap();
    assert_eq!(final_status, ActorStatus::Stopped);
}
