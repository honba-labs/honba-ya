//! Risk actor wrapping `honba-mon::RiskEngine` synchronously to enforce pre-trade controls.

use chrono::Utc;
use honba_mon::RiskEngine;
use honba_persistence::SqliteAuditLedger;
use honba_te_core::BrokerError;
use rust_decimal::prelude::ToPrimitive;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use crate::messages::{ActorStatus, ExecutionMsg, RiskBreachEvent, RiskMsg};
use crate::metrics::MetricsCollector;
use crate::tracing_spans::{span_filled, span_risk_evaluated};

/// Actor enforcing deterministic pre-trade risk controls and regulatory limits.
pub struct RiskActor {
    risk_engine: RiskEngine,
    rx: mpsc::Receiver<RiskMsg>,
    execution_tx: mpsc::Sender<ExecutionMsg>,
    status: ActorStatus,
    metrics: Option<Arc<MetricsCollector>>,
    audit_ledger: Option<Arc<SqliteAuditLedger>>,
}

impl RiskActor {
    /// Create a new risk actor.
    pub fn new(
        risk_engine: RiskEngine,
        rx: mpsc::Receiver<RiskMsg>,
        execution_tx: mpsc::Sender<ExecutionMsg>,
    ) -> Self {
        Self {
            risk_engine,
            rx,
            execution_tx,
            status: ActorStatus::Starting,
            metrics: None,
            audit_ledger: None,
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

    /// Access the underlying synchronous risk engine.
    pub fn risk_engine(&self) -> &RiskEngine {
        &self.risk_engine
    }

    /// Access the mutable synchronous risk engine.
    pub fn risk_engine_mut(&mut self) -> &mut RiskEngine {
        &mut self.risk_engine
    }

    /// Run the risk actor event loop.
    pub async fn run(mut self) -> ActorStatus {
        info!("RiskActor starting");
        self.status = ActorStatus::Running;

        while let Some(msg) = self.rx.recv().await {
            match msg {
                RiskMsg::EvaluateOrder {
                    request,
                    now,
                    reply_to,
                } => {
                    // 1. Synchronous, deterministic pre-trade risk evaluation with latency measurement
                    let eval_start = Instant::now();
                    let risk_result = self.risk_engine.check_order(&request, now);
                    let eval_duration = eval_start.elapsed();
                    let eval_duration_us = eval_duration.as_micros() as u64;

                    if let Err(refusal) = risk_result {
                        let span = span_risk_evaluated(
                            &request,
                            eval_duration_us,
                            false,
                            Some(refusal.rule()),
                        );
                        let _enter = span.enter();

                        warn!(
                            algo_id = %request.algo_id,
                            client_id = %request.client_id_str(),
                            correlation_id = %request.correlation_id(),
                            refusal = ?refusal,
                            eval_duration_us = eval_duration_us,
                            "Pre-trade risk gate rejected order"
                        );

                        // Emit structured RiskBreachEvent for persistence audit stream
                        let breach_event = RiskBreachEvent {
                            ts: now,
                            algo_id: request.algo_id.clone(),
                            instrument: request.instrument.clone(),
                            refusal_reason: refusal.to_string(),
                            rule_code: refusal.rule(),
                            eval_duration_us,
                            requested_qty: Some(request.quantity),
                            requested_price: request.order_type.limit_price(),
                        };

                        // Export Prometheus telemetry
                        if let Some(ref m) = self.metrics {
                            m.record_risk_evaluation(
                                &request,
                                eval_duration,
                                false,
                                Some(refusal.rule()),
                            );
                        }

                        // Persist to SEBI audit ledger
                        if let Some(ref audit) = self.audit_ledger {
                            let _ = audit
                                .record_risk_refusal(
                                    &request,
                                    &refusal.to_string(),
                                    eval_duration_us,
                                )
                                .await;
                        }

                        let _ = reply_to.send(Err(BrokerError::RiskRefusal(Box::new(refusal))));
                        // Emit breach event to audit stream after reply
                        let _ = self
                            .execution_tx
                            .send(crate::messages::ExecutionMsg::RiskBreachEvent(breach_event))
                            .await;
                        continue;
                    }

                    // Pre-trade evaluation passed
                    let span = span_risk_evaluated(&request, eval_duration_us, true, None);
                    let _enter = span.enter();

                    if let Some(ref m) = self.metrics {
                        m.record_risk_evaluation(&request, eval_duration, true, None);
                        if let Some(otr) = self.risk_engine.otr_for_algo(&request.algo_id) {
                            if let Some(val) = otr.to_f64() {
                                m.update_otr(&request.algo_id, &request.instrument.symbol, val);
                            }
                        }
                    }

                    // 2. Order is approved by risk gate -> forward to ExecutionActor
                    let (exec_tx, exec_rx) = oneshot::channel();
                    let exec_cmd = ExecutionMsg::ExecuteOrder {
                        request: request.clone(),
                        reply_to: exec_tx,
                    };

                    if let Err(e) = self.execution_tx.send(exec_cmd).await {
                        let _ = reply_to.send(Err(BrokerError::Transport(format!(
                            "ExecutionActor unreachable: {e}"
                        ))));
                        continue;
                    }

                    // 3. Await broker acknowledgement
                    match exec_rx.await {
                        Ok(result) => {
                            let _ = reply_to.send(result);
                        }
                        Err(_) => {
                            let _ = reply_to.send(Err(BrokerError::Transport(
                                "ExecutionActor dropped reply channel".into(),
                            )));
                        }
                    }
                }

                RiskMsg::RecordCancellation { algo_id } => {
                    self.risk_engine.record_cancellation(&algo_id, Utc::now());
                    if let Some(ref m) = self.metrics {
                        m.record_cancellation(&algo_id, "ALL");
                        if let Some(otr) = self.risk_engine.otr_for_algo(&algo_id) {
                            if let Some(val) = otr.to_f64() {
                                m.update_otr(&algo_id, "ALL", val);
                            }
                        }
                    }
                }

                RiskMsg::RecordFill(fill) => {
                    let span = span_filled(&fill);
                    let _enter = span.enter();

                    self.risk_engine.record_fill(&fill);

                    if let Some(ref m) = self.metrics {
                        m.record_fill(&fill);
                        if let Some(otr) = self.risk_engine.otr_for_algo(&fill.algo_id) {
                            if let Some(val) = otr.to_f64() {
                                m.update_otr(&fill.algo_id, &fill.instrument.symbol, val);
                            }
                        }
                    }

                    if let Some(ref audit) = self.audit_ledger {
                        let _ = audit.record_fill(&fill).await;
                    }
                }

                RiskMsg::HaltAlgo { algo_id, reason } => {
                    warn!(algo_id = %algo_id, reason = %reason, "Halting algo in RiskActor");
                    self.risk_engine.halt_algo(&algo_id, reason, Utc::now());
                }

                RiskMsg::ResumeAlgo { algo_id } => {
                    info!(algo_id = %algo_id, "Resuming algo in RiskActor");
                    self.risk_engine.resume_algo(&algo_id);
                }

                RiskMsg::HaltGlobal { reason } => {
                    warn!(reason = %reason, "Global emergency halt triggered in RiskActor");
                    self.risk_engine.halt_global(reason, Utc::now());
                }

                RiskMsg::ResumeGlobal => {
                    info!("Resuming global trading in RiskActor");
                    self.risk_engine.resume_global();
                }

                RiskMsg::Shutdown => {
                    info!("RiskActor received shutdown signal");
                    self.status = ActorStatus::Draining;
                    break;
                }
            }
        }

        self.status = ActorStatus::Stopped;
        info!("RiskActor stopped");
        self.status
    }
}
