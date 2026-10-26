//! Phase 8 Integration & Conformance Tests:
//! 1. OpenTelemetry mailbox tracing spans & Prometheus metrics
//! 2. Pipeline SLO conformance verification
//! 3. Supervised PositionSizingActor chaining
//! 4. PythonStrategyActor async isolation, timeout cancellation & degraded recovery

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::mpsc;
use uuid::Uuid;

use honba_actors::slo::{
    SLO_END_TO_END_TICK_TO_WIRE_MAX, SLO_PRE_TRADE_RISK_GATE_MAX, SLO_STRATEGY_SIGNAL_TO_ORDER_MAX,
    SLO_TICK_TO_QUOTE_WS_BROADCAST_MAX, verify_risk_gate_slo, verify_strategy_signal_slo,
    verify_wire_dispatch_slo, verify_ws_broadcast_slo,
};
use honba_actors::tracing_spans::{
    derive_or_create_trace_context, extract_trace_context, inject_trace_context,
};
use honba_actors::{
    ActorStatus, MetricsCollector, PositionSizingActor, PositionSizingMsg, PythonExecutionError,
    PythonStrategyActor, PythonStrategyEngine, RiskMsg, StrategyMsg,
};
use honba_core::sizing::SizingMethod;
use honba_core::wire::TraceContext;
use honba_core::{
    Bar, Currency, Exchange, FillReport, InstrumentId, OrderAck, OrderRequest, OrderSide,
    OrderStatus, OrderType, Price, Quantity, Tick, TimeInForce,
};
use honba_kokoro::sizing::Portfolio;
use honba_portfolio::PortfolioState;

#[tokio::test]
async fn test_phase8_metrics_and_slo_verification() {
    let metrics = Arc::new(MetricsCollector::new().expect("Metrics collector creation"));

    // Verify initial metrics recording
    metrics.set_mailbox_queue_depth("market_data", 5);
    metrics.set_mailbox_queue_depth("strategy", 2);
    metrics.record_actor_latency("market_data", Duration::from_micros(450));
    metrics.record_actor_latency("strategy", Duration::from_micros(650));
    metrics.record_actor_latency("risk", Duration::from_micros(120));
    metrics.record_actor_latency("execution", Duration::from_millis(1));
    metrics.record_backpressure_drop("market_data", "coalesced");
    metrics.record_actor_error("strategy", "timeout");

    let exported = metrics.export_metrics().expect("Export prometheus metrics");
    assert!(exported.contains("actor_mailbox_queue_depth"));
    assert!(exported.contains("actor_processing_latency_seconds"));
    assert!(exported.contains("actor_backpressure_drops_total"));
    assert!(exported.contains("actor_errors_total"));

    // Verify SLO evaluation functions:
    // 1. Tick-to-Quote < 2ms
    assert!(verify_ws_broadcast_slo(Duration::from_micros(900)).is_ok());
    assert!(
        verify_ws_broadcast_slo(SLO_TICK_TO_QUOTE_WS_BROADCAST_MAX + Duration::from_millis(1))
            .is_err()
    );

    // 2. Strategy signal-to-order < 1ms
    assert!(verify_strategy_signal_slo(Duration::from_micros(450)).is_ok());
    assert!(
        verify_strategy_signal_slo(SLO_STRATEGY_SIGNAL_TO_ORDER_MAX + Duration::from_millis(1))
            .is_err()
    );

    // 3. End-to-end tick-to-wire broker dispatch < 5ms
    assert!(verify_wire_dispatch_slo(Duration::from_millis(3)).is_ok());
    assert!(
        verify_wire_dispatch_slo(SLO_END_TO_END_TICK_TO_WIRE_MAX + Duration::from_millis(2))
            .is_err()
    );

    // 4. Pre-trade risk gate evaluation < 15us
    assert!(verify_risk_gate_slo(Duration::from_micros(10)).is_ok());
    assert!(verify_risk_gate_slo(SLO_PRE_TRADE_RISK_GATE_MAX + Duration::from_micros(5)).is_err());
}

#[tokio::test]
async fn test_phase8_w3c_trace_context_propagation() {
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let trace_ctx = TraceContext::from_traceparent(traceparent).expect("Valid traceparent parse");

    let mut order_req = OrderRequest {
        client_order_id: Uuid::new_v4(),
        client_id: Some("client_default".to_string()),
        correlation_id: Some(Uuid::new_v4()),
        algo_id: "test_algo".to_string(),
        instrument: InstrumentId::new(Exchange::Nse, "RELIANCE"),
        side: OrderSide::Buy,
        order_type: OrderType::Limit(Price(dec!(2500.00))),
        quantity: Quantity::from_u64(10),
        tif: TimeInForce::Day,
        tag: None,
        trace_context: None,
    };

    inject_trace_context(&mut order_req, &trace_ctx);
    assert!(order_req.trace_context.is_some());

    let extracted = extract_trace_context(&order_req).expect("TraceContext should be extracted");
    assert_eq!(extracted.to_traceparent(), traceparent);

    let derived = derive_or_create_trace_context(order_req.trace_context.as_ref());
    assert_eq!(derived.trace_id, trace_ctx.trace_id);
}

#[tokio::test]
async fn test_phase8_position_sizing_actor_chaining() {
    let (sizing_tx, sizing_rx) = mpsc::channel(100);
    let (risk_tx, mut risk_rx) = mpsc::channel(100);

    let portfolio = Portfolio::new(PortfolioState::new(Currency::Inr));
    let sizing_actor = PositionSizingActor::new(sizing_rx, risk_tx, portfolio).with_sizing_method(
        SizingMethod::FixedFractional {
            risk_pct: dec!(0.02), // 2% risk fraction
        },
    );

    let sizing_handle = tokio::spawn(async move { sizing_actor.run().await });

    // Send order to position sizing actor
    let order_req = OrderRequest {
        client_order_id: Uuid::new_v4(),
        client_id: Some("client_default".to_string()),
        correlation_id: Some(Uuid::new_v4()),
        algo_id: "test_sizing_algo".to_string(),
        instrument: InstrumentId::new(Exchange::Nse, "RELIANCE"),
        side: OrderSide::Buy,
        order_type: OrderType::Limit(Price(dec!(2500.00))),
        quantity: Quantity::from_u64(1), // initial small lot
        tif: TimeInForce::Day,
        tag: None,
        trace_context: None,
    };

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    sizing_tx
        .send(PositionSizingMsg::SizeOrder {
            request: order_req,
            now: Utc::now(),
            reply_to: reply_tx,
        })
        .await
        .expect("Send SizeOrder");

    // Risk actor receives sized order
    let received_risk_msg = risk_rx.recv().await.expect("RiskActor receives message");
    match received_risk_msg {
        RiskMsg::EvaluateOrder {
            request,
            reply_to,
            now: _,
        } => {
            assert_eq!(request.instrument.symbol, "RELIANCE");
            assert!(request.quantity.value() >= Decimal::ONE);
            let _ = reply_to.send(Ok(OrderAck {
                order_id: "ACK-SIZED-1".to_string(),
                client_order_id: request.client_order_id,
                client_id: request.client_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
                exchange_order_id: None,
                status: OrderStatus::New,
                trace_context: None,
            }));
        }
        _ => panic!("Expected EvaluateOrder in RiskActor"),
    }

    let sizing_result = reply_rx.await.expect("Sizing reply received");
    assert!(sizing_result.is_ok());

    sizing_tx.send(PositionSizingMsg::Shutdown).await.unwrap();
    let status = sizing_handle.await.unwrap();
    assert_eq!(status, ActorStatus::Stopped);
}

struct MockPythonEngine {
    should_timeout: bool,
}

impl PythonStrategyEngine for MockPythonEngine {
    fn name(&self) -> &str {
        "mock_python_momentum"
    }

    fn algo_id(&self) -> &str {
        "py_momentum_algo"
    }

    fn on_tick(&mut self, _tick: &Tick) -> Result<Vec<OrderRequest>, PythonExecutionError> {
        if self.should_timeout {
            // Block thread to simulate long Python calculation
            std::thread::sleep(Duration::from_millis(300));
        }
        Ok(vec![])
    }

    fn on_bar(&mut self, _bar: &Bar) -> Result<Vec<OrderRequest>, PythonExecutionError> {
        Ok(vec![])
    }

    fn on_fill(&mut self, _fill: &FillReport) -> Result<(), PythonExecutionError> {
        Ok(())
    }
}

#[tokio::test]
async fn test_phase8_python_strategy_actor_isolation_and_timeout() {
    let (msg_tx, msg_rx) = mpsc::channel(100);
    let (risk_tx, mut risk_rx) = mpsc::channel(100);

    let engine = MockPythonEngine {
        should_timeout: true,
    };
    let python_actor = PythonStrategyActor::spawn(engine, msg_rx, risk_tx, 100)
        .with_timeout(Duration::from_millis(50)); // Strict 50ms deadline

    let actor_handle = tokio::spawn(async move { python_actor.run().await });

    // Send a tick to trigger Python execution
    let tick = Tick::Ltp {
        instrument: InstrumentId::new(Exchange::Nse, "TCS"),
        price: Price(dec!(3500.00)),
        volume: Quantity::from_u64(100),
        exchange_ts: Utc::now(),
        receive_ts: Utc::now(),
    };

    msg_tx.send(StrategyMsg::OnTick(tick)).await.unwrap();

    // Verify supervisor/risk actor receives emergency HaltAlgo protective signal
    let received_risk = tokio::time::timeout(Duration::from_millis(800), risk_rx.recv())
        .await
        .expect("Risk receives halt within timeout")
        .expect("Valid RiskMsg");

    match received_risk {
        RiskMsg::HaltAlgo { algo_id, reason } => {
            assert_eq!(algo_id, "py_momentum_algo");
            assert!(reason.to_lowercase().contains("timeout"));
        }
        _ => panic!("Expected HaltAlgo on python timeout"),
    }

    // Cleanup
    msg_tx.send(StrategyMsg::Shutdown).await.unwrap();
    let final_status = actor_handle.await.unwrap();
    assert_eq!(final_status, ActorStatus::Stopped);
}
