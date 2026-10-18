//! Risk actor wrapping `honba-mon::RiskEngine` synchronously to enforce pre-trade controls.

use chrono::Utc;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use honba_mon::RiskEngine;
use honba_te_core::BrokerError;

use crate::messages::{ActorStatus, ExecutionMsg, RiskMsg};

/// Actor enforcing deterministic pre-trade risk controls and regulatory limits.
pub struct RiskActor {
    risk_engine: RiskEngine,
    rx: mpsc::Receiver<RiskMsg>,
    execution_tx: mpsc::Sender<ExecutionMsg>,
    status: ActorStatus,
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
        }
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
                    // 1. Synchronous, deterministic pre-trade risk evaluation
                    if let Err(refusal) = self.risk_engine.check_order(&request, now) {
                        warn!(
                            algo_id = %request.algo_id,
                            refusal = ?refusal,
                            "Pre-trade risk gate rejected order"
                        );
                        let _ = reply_to.send(Err(BrokerError::RiskRefusal(refusal)));
                        continue;
                    }

                    // 2. Order is approved by risk gate -> forward to ExecutionActor
                    let (exec_tx, exec_rx) = oneshot::channel();
                    let exec_cmd = ExecutionMsg::ExecuteOrder {
                        request,
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
                    self.risk_engine.record_cancellation(&algo_id);
                }

                RiskMsg::RecordFill(fill) => {
                    self.risk_engine.record_fill(&fill);
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
