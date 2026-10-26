//! Execution actor managing broker order placement, status queries, retries, and fills.

use chrono::Utc;
use honba_core::{OrderAck};
use honba_kokoro_oms::{OrderState, OrderStateMachine};
use honba_persistence::{
    SqliteAuditLedger, messages::RiskBreachEvent as PersistenceRiskBreachEvent,
};
use honba_te_core::BrokerExecutor;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::info;
use uuid::Uuid;

use crate::messages::{ActorStatus, ExecutionMsg, RiskMsg, StrategyMsg};
use crate::metrics::MetricsCollector;
use crate::tracing_spans::{
    span_actor_process_message, span_broker_acked, span_broker_submitted, span_cancelled,
    span_execution_dispatch_wire,
};

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
            if matches!(msg, ExecutionMsg::Shutdown) {
                info!("ExecutionActor received shutdown signal; entering draining state");
                self.status = ActorStatus::Draining;
                while let Ok(in_flight) = self.rx.try_recv() {
                    if matches!(in_flight, ExecutionMsg::Shutdown) {
                        continue;
                    }
                    self.process_msg(in_flight).await;
                }
                break;
            }
            self.process_msg(msg).await;
        }

        self.status = ActorStatus::Stopped;
        info!("ExecutionActor stopped cleanly after drain");
        self.status
    }

    async fn process_msg(&mut self, msg: ExecutionMsg) {
        let start = Instant::now();
        let msg_kind = match &msg {
            ExecutionMsg::ExecuteOrder { .. } => "execute_order",
            ExecutionMsg::CancelOrder { .. } => "cancel_order",
            ExecutionMsg::QueryStatus { .. } => "query_status",
            ExecutionMsg::RiskBreachEvent(_) => "risk_breach_event",
            ExecutionMsg::Shutdown => "shutdown",
        };
        let proc_span = span_actor_process_message("execution", msg_kind, None);
        let _enter_proc = proc_span.enter();

        match msg {
            ExecutionMsg::ExecuteOrder { request, reply_to } => {
                let wire_span = span_execution_dispatch_wire(
                    &request,
                    self.broker.name(),
                    request.trace_context.as_ref(),
                );
                let _enter_wire = wire_span.enter();

                // 0. OMS: Check idempotency and register order
                if let Some(ref mut oms) = self.oms {
                    let client_order_id_str = request.client_order_id.to_string();
                    // Check if client_order_id is already registered
                    if let Err(e) = oms.check_idempotency(&client_order_id_str) {
                        let _ = reply_to.send(Err(crate::messages::BrokerError::InvalidRequest(
                            format!("Idempotency check failed: {}", e),
                        )));
                        return;
                    }
                    // Register the order in the OMS
                    oms.register_order(client_order_id_str, OrderState::PendingNew, Utc::now());
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
                            m.record_actor_error("execution", "broker_dispatch_error");
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
                let res_converted: Result<OrderAck, crate::messages::BrokerError> =
                    res.map_err(|e| crate::messages::BrokerError::InvalidRequest(e.to_string()));
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
                                "user_requested",
                                cancel_duration_us,
                            )
                            .await;
                    }

                    if let Some(ref risk_tx) = self.risk_tx {
                        let _ = risk_tx
                            .send(RiskMsg::RecordCancellation {
                                algo_id: algo_id.clone(),
                            })
                            .await;
                    }
                } else if let Some(ref m) = self.metrics {
                    m.record_actor_error("execution", "broker_cancel_error");
                }

                let res_converted =
                    res.map_err(|e| crate::messages::BrokerError::InvalidRequest(e.to_string()));
                let _ = reply_to.send(res_converted);
            }

            ExecutionMsg::QueryStatus { order_id, reply_to } => {
                let res = self.broker.get_order_status(&order_id).await;
                let res_converted =
                    res.map_err(|e| crate::messages::BrokerError::InvalidRequest(e.to_string()));
                let _ = reply_to.send(res_converted);
            }

            ExecutionMsg::RiskBreachEvent(breach) => {
                if let Some(ref audit) = self.audit_ledger {
                    let pers_breach = PersistenceRiskBreachEvent {
                        ts: breach.ts,
                        algo_id: breach.algo_id,
                        instrument: breach.instrument,
                        refusal_reason: breach.refusal_reason,
                        rule_code: breach.rule_code,
                        eval_duration_us: breach.eval_duration_us,
                        requested_qty: breach.requested_qty,
                        requested_price: breach.requested_price,
                    };
                    let _ = audit.record_risk_breach_event(&pers_breach).await;
                }
            }

            ExecutionMsg::Shutdown => {}
        }

        if let Some(ref m) = self.metrics {
            m.record_actor_latency("execution", start.elapsed());
        }
    }
}
