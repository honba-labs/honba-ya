//! Execution actor managing broker order placement, status queries, retries, and fills.

use chrono::Utc;
use honba_core::{OrderAck, OrderStatus};
use honba_kokoro_oms::{OrderStateMachine, OrderState, TransitionError};
use honba_persistence::SqliteAuditLedger;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::info;
use uuid::Uuid;

/// Minimal broker error type for execution actor.
#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("auth failure: {0}")]
    Auth(String),
    #[error("order rejected by broker: {0}")]
    Rejected(String),
    #[error("network or transport error: {0}")]
    Transport(String),
    #[error("broker request timeout")]
    Timeout,
    #[error("order not found: {0}")]
    OrderNotFound(String),
    #[error("invalid request or parameters: {0}")]
    InvalidRequest(String),
    #[error("other: {0}")]
    Other(String),
}

/// Minimal broker executor trait.
#[async_trait::async_trait]
pub trait BrokerExecutor: Send + Sync {
    fn name(&self) -> &str;
    async fn place_order(&self, req: honba_core::OrderRequest) -> Result<honba_core::OrderAck, BrokerError>;
    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError>;
    async fn get_order_status(&self, order_id: &str) -> Result<honba_core::OrderStatus, BrokerError>;
}

use crate::messages::{ActorStatus, ExecutionMsg, RiskMsg, StrategyMsg};
use crate::metrics::MetricsCollector;
use crate::tracing_spans::{span_broker_acked, span_broker_submitted, span_cancelled};

/// Execution actor interfacing with broker adapters.
pub struct ExecutionActor {
    broker: Arc<dyn BrokerExecutor>,
    rx: mpsc::Receiver<ExecutionMsg>,
    risk_tx: Option<mpsc::Sender<RiskMsg>>,
    #[allow(dead_code)]
    strategy_tx: Option<mpsc::Sender<StrategyMsg>>,
    status: ActorStatus,
    metrics: Option<Arc<MetricsCollector>>,
    audit_ledger: Option<Arc<SqliteAuditLedger>>,
    /// Optional OMS for idempotency and fill reconciliation
    oms: Option<OrderStateMachine>,
}

impl ExecutionActor {
    /// Create a new execution actor with attached broker adapter.
    pub fn new(
        broker: Arc<dyn BrokerExecutor>,
        rx: mpsc::Receiver<ExecutionMsg>,
        risk_tx: Option<mpsc::Sender<RiskMsg>>,
        strategy_tx: Option<mpsc::Sender<StrategyMsg>>,
    ) -> Self {
        Self {
            broker,
            rx,
            risk_tx,
            strategy_tx,
            status: ActorStatus::Starting,
            metrics: None,
            audit_ledger: None,
            oms: None,
        }
    }

    /// Attach Prometheus metrics collector.
    pub fn with_metrics(mut self, metrics: Arc<MetricsCollector>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Attach SEBI SQLite audit ledger.
    pub fn with_audit_ledger(mut self, audit_ledger: Arc<SqliteAuditLedger>) -> Self {
        self.audit_ledger = Some(audit_ledger);
        self
    }

    /// Attach Order State Machine for idempotency and fill reconciliation.
    pub fn with_oms(mut self, oms: OrderStateMachine) -> Self {
        self.oms = Some(oms);
        self
    }

    /// Access broker adapter name.
    pub fn broker_name(&self) -> &str {
        self.broker.name()
    }

    /// Run the execution actor event loop.
    pub async fn run(mut self) -> ActorStatus {
        info!(broker = self.broker.name(), "ExecutionActor starting");
        self.status = ActorStatus::Running;

        while let Some(msg) = self.rx.recv().await {
            match msg {
                ExecutionMsg::ExecuteOrder { request, reply_to } => {
                    // 0. OMS: Check idempotency and register order
                    if let Some(ref mut oms) = self.oms {
                        let client_order_id_str = request.client_order_id.to_string();
                        // Check if client_order_id is already registered
                        if let Err(e) = oms.check_idempotency(&client_order_id_str) {
                            let _ = reply_to.send(Err(crate::messages::BrokerError::Other(
                                format!("Idempotency check failed: {}", e)
                            )));
                            continue;
                        }
                        // Register the order in the OMS
                        oms.register_order(
                            client_order_id_str,
                            OrderState::PendingNew,
                            Utc::now(),
                        );
                    }

                    // 1. Structured tracing span: BrokerSubmitted
                    let submit_span = span_broker_submitted(&request, self.broker.name());
                    let _enter = submit_span.enter();

                    // 2. Persist order attempt in SEBI audit ledger
                    if let Some(ref audit) = self.audit_ledger {
                        let _ = audit.record_order_attempt(&request).await;
                    }

                    // 3. Dispatch to broker adapter with latency timer
                    let dispatch_start = Instant::now();
                    let res = self.broker.place_order(request.clone()).await;
                    let dispatch_duration = dispatch_start.elapsed();
                    let dispatch_duration_us = dispatch_duration.as_micros() as u64;

                    match &res {
                        Ok(ack) => {
                            // Structured tracing span: BrokerAcked
                            let ack_span = span_broker_acked(&request, ack, dispatch_duration_us);
                            let _enter_ack = ack_span.enter();

                            if let Some(ref m) = self.metrics {
                                m.record_broker_dispatch(
                                    &request,
                                    self.broker.name(),
                                    dispatch_duration,
                                    true,
                                );
                            }

                            if let Some(ref audit) = self.audit_ledger {
                                let _ = audit
                                    .record_broker_ack(&request, ack, dispatch_duration_us)
                                    .await;
                            }

                            // OMS: Process the successful ack as a fill (for now, just mark as New)
                            if let Some(ref mut oms) = self.oms {
                                let client_order_id_str = request.client_order_id.to_string();
                                // When broker acks, order transitions to New
                                if let Some(state) = oms.get_state(&client_order_id_str).cloned() {
                                    let _ = OrderState::transition(state, OrderState::New);
                                }
                            }
                        }
                        Err(_e) => {
                            if let Some(ref m) = self.metrics {
                                m.record_broker_dispatch(
                                    &request,
                                    self.broker.name(),
                                    dispatch_duration,
                                    false,
                                );
                            }

                            // OMS: Mark order as rejected on broker error
                            if let Some(ref mut oms) = self.oms {
                                let client_order_id_str = request.client_order_id.to_string();
                                if let Some(state) = oms.get_state(&client_order_id_str).cloned() {
                                    let _ = OrderState::transition(state, OrderState::Rejected);
                                }
                            }
                        }
                    }

                    // Convert execution::BrokerError to messages::BrokerError for the reply channel
                    let res_converted: Result<OrderAck, crate::messages::BrokerError> = res.map_err(|e| {
                        crate::messages::BrokerError::Other(e.to_string())
                    });
                    let _ = reply_to.send(res_converted);
                }

                ExecutionMsg::CancelOrder {
                    order_id,
                    algo_id,
                    reply_to,
                } => {
                    let cancel_start = Instant::now();
                    let res = self.broker.cancel_order(&order_id).await;
                    let cancel_duration_us = cancel_start.elapsed().as_micros() as u64;

                    if res.is_ok() {
                        let correlation_id = Uuid::new_v4();
                        let cancel_span = span_cancelled(
                            &algo_id,
                            "default",
                            correlation_id,
                            &order_id,
                            "unknown",
                            cancel_duration_us,
                        );
                        let _enter = cancel_span.enter();

                        if let Some(ref audit) = self.audit_ledger {
                            let _ = audit
                                .record_cancellation(
                                    &algo_id,
                                    "default",
                                    correlation_id,
                                    &order_id,
                                    "unknown",
                                    "broker cancel acked",
                                    cancel_duration_us,
                                )
                                .await;
                        }

                        if let Some(ref m) = self.metrics {
                            m.record_cancellation(&algo_id, "unknown");
                        }

                        if let Some(ref r_tx) = self.risk_tx {
                            let _ = r_tx.send(RiskMsg::RecordCancellation { algo_id }).await;
                        }
                    }
                    // Convert execution::BrokerError to messages::BrokerError for the reply channel
                        let res_converted: Result<(), crate::messages::BrokerError> = res.map_err(|e| {
                            crate::messages::BrokerError::Other(e.to_string())
                        });
                        let _ = reply_to.send(res_converted);
                    }

                ExecutionMsg::QueryStatus { order_id, reply_to } => {
                    let res = self.broker.get_order_status(&order_id).await;
                    let res_converted: Result<OrderStatus, crate::messages::BrokerError> = res.map_err(|e| {
                        crate::messages::BrokerError::Other(e.to_string())
                    });
                    let _ = reply_to.send(res_converted);
                }

                ExecutionMsg::RiskBreachEvent(event) => {
                    // Persist risk breach event to SEBI audit ledger
                    if let Some(ref audit) = self.audit_ledger {
                        let _ = audit.record_risk_breach_event(&event).await;
                    }
                }

                ExecutionMsg::Shutdown => {
                    info!("ExecutionActor received shutdown signal");
                    self.status = ActorStatus::Draining;
                    break;
                }
            }
        }

        self.status = ActorStatus::Stopped;
        info!("ExecutionActor stopped");
        self.status
    }
}
