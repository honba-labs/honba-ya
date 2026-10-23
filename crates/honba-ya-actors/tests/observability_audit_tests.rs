use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use chrono::Utc;
use honba_actors::{
    ExecutionMsg, MarketDataMsg, MetricsCollector, RiskMsg, Strategy, StrategyMsg, Supervisor, span_broker_acked,
    span_broker_submitted, span_cancelled, span_filled, span_order_generated, span_risk_evaluated,
};
use honba_core::{
    Exchange, FillReport, Money, InstrumentId, OrderAck, OrderRequest, OrderSide, OrderStatus, OrderType,
    Price, PriceBand, Quantity, Tick,
};
use honba_mon::{OtrConfig, RiskEngine, RiskLimits};
use honba_persistence::SqliteAuditLedger;
use honba_ya_testing::MockBrokerExecutor;
use rust_decimal::Decimal;

struct TestObservabilityStrategy {
    algo_id: String,
    placed: bool,
}

#[async_trait::async_trait]
impl Strategy for TestObservabilityStrategy {
    fn name(&self) -> &str {
        "TestObservabilityStrategy"
    }

    fn algo_id(&self) -> &str {
        &self.algo_id
    }

    async fn on_tick(&mut self, tick: &Tick) -> Vec<OrderRequest> {
        if !self.placed {
            self.placed = true;
            vec![
                OrderRequest::new(
                    tick.instrument().clone(),
                    OrderSide::Buy,
                    OrderType::Limit(tick.last_price().unwrap()),
                    Quantity::from_u64(50),
                    &self.algo_id,
                )
                .with_client_id("client-vip-1"),
            ]
        } else {
            vec![]
        }
    }
}

#[tokio::test]
async fn test_observability_tracing_and_prometheus_export() {
    let metrics = Arc::new(MetricsCollector::new().unwrap());

    let algo_id = "algo-obs-999";
    let client_id = "client-vip-1";
    let inst = InstrumentId::new(Exchange::Nse, "TCS");
    let req = OrderRequest::new(
        inst.clone(),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("3800.00").unwrap()),
        Quantity::from_u64(50),
        algo_id,
    )
    .with_client_id(client_id);
    let correlation_id = req.correlation_id();

    // 1. Verify structured tracing span creation
    let s_gen = span_order_generated(&req);
    let _g = s_gen.enter();
    metrics.record_order_generation(&req);
    drop(_g);

    let s_risk = span_risk_evaluated(&req, 45, true, None);
    let _r = s_risk.enter();
    metrics.record_risk_evaluation(&req, Duration::from_micros(45), true, None);
    drop(_r);

    let s_submit = span_broker_submitted(&req, "mock_broker");
    let _s = s_submit.enter();
    drop(_s);

    let ack = OrderAck {
        order_id: "BRK-777".to_string(),
        client_order_id: req.client_order_id,
        exchange_order_id: Some("EX-777".to_string()),
        status: OrderStatus::New,
        timestamp: Utc::now(),
        correlation_id: Some(correlation_id),
        client_id: Some(client_id.to_string()),
    };

    let s_ack = span_broker_acked(&req, &ack, 1500);
    let _a = s_ack.enter();
    metrics.record_broker_dispatch(&req, "mock_broker", Duration::from_micros(1500), true);
    drop(_a);

    let fill = FillReport {
        fill_id: "FILL-777".to_string(),
        order_id: ack.order_id.clone(),
        client_order_id: req.client_order_id,
        instrument: inst.clone(),
        side: OrderSide::Buy,
        price: Price::from_rupees("3800.00").unwrap(),
        quantity: Quantity::from_u64(50),
        algo_id: algo_id.to_string(),
        timestamp: Utc::now(),
        correlation_id: Some(correlation_id),
        client_id: Some(client_id.to_string()),
        fee: Money::zero_inr(),
        fee_breakdown: None,
    };

    let s_fill = span_filled(&fill);
    let _f = s_fill.enter();
    metrics.record_fill(&fill);
    drop(_f);

    let s_cancel = span_cancelled(
        algo_id,
        client_id,
        correlation_id,
        "BRK-777",
        "NSE:TCS",
        200,
    );
    let _c = s_cancel.enter();
    metrics.record_cancellation(algo_id, "TCS");
    drop(_c);

    // Record PnL & OTR in Prometheus metrics
    metrics.record_pnl(algo_id, Decimal::from_str("15250.75").unwrap());
    metrics.update_otr(algo_id, "TCS", 12.5);

    // Record risk refusal metric
    metrics.record_risk_evaluation(
        &req,
        Duration::from_micros(30),
        false,
        Some("price_band_violation"),
    );

    // 2. Validate Prometheus text exposition
    let exported = metrics.export_metrics().unwrap();
    assert!(exported.contains("honba_otr_ratio{algo_id=\"algo-obs-999\",symbol=\"TCS\"} 12.5"));
    assert!(exported.contains(
        "honba_notional_turnover_inr_total{algo_id=\"algo-obs-999\",symbol=\"TCS\"} 190000"
    ));
    assert!(exported.contains("honba_daily_pnl_inr{algo_id=\"algo-obs-999\"} 15250.75"));
    assert!(exported.contains(
        "honba_risk_refusals_total{algo_id=\"algo-obs-999\",rule=\"price_band_violation\"} 1"
    ));
    assert!(exported.contains("honba_risk_evaluation_duration_seconds"));
    assert!(exported.contains("honba_order_dispatch_duration_seconds"));
}

#[tokio::test]
async fn test_actor_runtime_with_observability_and_audit() {
    let metrics = Arc::new(MetricsCollector::new().unwrap());
    let audit_ledger = Arc::new(SqliteAuditLedger::new_in_memory().await.unwrap());

    // Mock broker
    let mock_broker = Arc::new(MockBrokerExecutor::new(true));

    // Risk Engine
    let mut risk_engine = RiskEngine::new(RiskLimits::default(), OtrConfig::default());
    let inst = InstrumentId::new(Exchange::Nse, "TCS");
    risk_engine.set_price_band(
        inst.clone(),
        PriceBand::new(
            Price::from_rupees("3500.00").unwrap(),
            Price::from_rupees("4000.00").unwrap(),
            Price::from_rupees("3750.00").unwrap(),
        ),
    );
    // Register the algo_id so the risk engine accepts orders from it
    risk_engine.register_algo("algo-obs-runtime");

    let strategy = TestObservabilityStrategy {
        algo_id: "algo-obs-runtime".to_string(),
        placed: false,
    };

    // Spawn 4-actor supervision tree with observability & audit attached
    let supervisor = Supervisor::spawn_with_observability(
        strategy,
        risk_engine,
        mock_broker,
        64,
        Arc::clone(&metrics),
        Arc::clone(&audit_ledger),
    );

    // Send a market tick to trigger strategy order generation -> risk -> execution
    let tick = Tick::Ltp {
        instrument: inst.clone(),
        price: Price::from_rupees("3750.00").unwrap(),
        volume: Quantity::from_u64(100),
        exchange_ts: Utc::now(),
        receive_ts: Utc::now(),
    };
    // Subscribe to the instrument first, then send tick through market data actor
    supervisor
        .market_data_sender()
        .send(MarketDataMsg::Subscribe(inst.clone()))
        .await
        .unwrap();

    // Give actors a moment to start up
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a market tick to trigger strategy order generation -> risk -> execution
    supervisor
        .market_data_sender()
        .send(MarketDataMsg::IngestTick(tick))
        .await
        .unwrap();

    // Give actors a moment to process the pipeline
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Get the broker order ID assigned to the placed order from the audit ledger
    let acks = audit_ledger
        .query_by_event_type("broker_ack")
        .await
        .unwrap();
    assert_eq!(acks.len(), 1);
    let placed_order_id = acks[0].broker_order_id.clone().unwrap();

    // Send a cancellation command for the placed order through execution actor
    let (cancel_reply_tx, cancel_reply_rx) = tokio::sync::oneshot::channel();
    let cancel_cmd = ExecutionMsg::CancelOrder {
        order_id: placed_order_id.clone(),
        algo_id: "algo-obs-runtime".to_string(),
        reply_to: cancel_reply_tx,
    };
    supervisor
        .execution_sender()
        .send(cancel_cmd)
        .await
        .unwrap();
    let cancel_res = cancel_reply_rx.await.unwrap();
    assert!(cancel_res.is_ok(), "Cancellation should succeed");

    // Send an out-of-band order directly to risk to test refusal recording
    let bad_req = OrderRequest::new(
        inst.clone(),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("4500.00").unwrap()), // Out of band (>4000)
        Quantity::from_u64(10),
        "algo-obs-runtime",
    );
    let (eval_reply_tx, eval_reply_rx) = tokio::sync::oneshot::channel();
    supervisor
        .risk_sender()
        .send(RiskMsg::EvaluateOrder {
            request: bad_req,
            now: Utc::now(),
            reply_to: eval_reply_tx,
        })
        .await
        .unwrap();
    let eval_res = eval_reply_rx.await.unwrap();
    assert!(eval_res.is_err(), "Out-of-band order must be refused");

    // Send fill report to risk actor
    let fill = FillReport {
        fill_id: "FILL-999".to_string(),
        order_id: placed_order_id,
        client_order_id: Uuid::new_v4(),
        instrument: inst.clone(),
        side: OrderSide::Buy,
        price: Price::from_rupees("3750.00").unwrap(),
        quantity: Quantity::from_u64(50),
        algo_id: "algo-obs-runtime".to_string(),
        timestamp: Utc::now(),
        correlation_id: Some(Uuid::new_v4()),
        client_id: Some("default".to_string()),
        fee: Money::zero_inr(),
        fee_breakdown: None,
    };
    supervisor
        .risk_sender()
        .send(RiskMsg::RecordFill(fill))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify SEBI Audit Ledger contains all events:
    // - Order attempt
    // - Broker ack
    // - Cancellation
    // - Risk refusal (out of band)
    // - Fill
    let count = audit_ledger.count().await.unwrap();
    assert_eq!(count, 5, "Audit ledger must record all 5 lifecycle events");

    let refusals = audit_ledger
        .query_by_event_type("risk_refusal")
        .await
        .unwrap();
    assert_eq!(refusals.len(), 1);
    assert_eq!(refusals[0].status, "refused");
    assert!(
        refusals[0]
            .refusal_reason
            .as_ref()
            .unwrap()
            .contains("Price band violation")
    );

    let attempts = audit_ledger
        .query_by_event_type("order_attempt")
        .await
        .unwrap();
    assert_eq!(attempts.len(), 1);

    let broker_acks = audit_ledger
        .query_by_event_type("broker_ack")
        .await
        .unwrap();
    assert_eq!(broker_acks.len(), 1);

    let cancels = audit_ledger
        .query_by_event_type("cancellation")
        .await
        .unwrap();
    assert_eq!(cancels.len(), 1);

    let fills = audit_ledger.query_by_event_type("fill").await.unwrap();
    assert_eq!(fills.len(), 1);

    // Verify Prometheus Metrics
    let metrics_text = metrics.export_metrics().unwrap();
    assert!(metrics_text.contains(
        "honba_risk_refusals_total{algo_id=\"algo-obs-runtime\",rule=\"price_band_violation\"} 1"
    ));
    assert!(metrics_text.contains(
        "honba_notional_turnover_inr_total{algo_id=\"algo-obs-runtime\",symbol=\"TCS\"} 187500"
    ));

    // Gracefully shutdown
    supervisor
        .shutdown_gracefully(Duration::from_secs(2))
        .await
        .unwrap();
}
