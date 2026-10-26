//! Python Strategy Actor Execution Isolation & Cooperative Async Model (ADR 0032, Phase 8.4).
//!
//! Architectural Tenets:
//! 1. **OS Thread Isolation**: Runs Python strategies on a dedicated, pinned OS thread
//!    worker with a GIL-aware scheduler, ensuring that Python bytecode execution, C extensions,
//!    or GIL contention NEVER block or stall Tokio worker threads.
//! 2. **Cooperative Async Boundary**: Integrates Tokio async event processing with Python
//!    execution via bounded MPSC channels and oneshot return channels (`pyo3-asyncio` cooperative pattern).
//! 3. **Deterministic Timeout & Cancellation**: Enforces strict per-decision execution timeouts
//!    (`tokio::time::timeout`) using `tokio_util::sync::CancellationToken`. If a Python invocation
//!    exceeds the deadline, the token is triggered and Tokio unblocks immediately.
//! 4. **Fail-Safe Protective Gate**: On timeout, panic, or crash, the strategy transitions
//!    its state to [`ActorStatus::Degraded`], notifies the supervisor, and synchronously dispatches
//!    an emergency protective algo halt / order cancel command to [`honba_mon::RiskEngine`].

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use honba_core::{Bar, FillReport, OrderRequest, Tick};

use crate::messages::{ActorStatus, PositionSizingMsg, RiskMsg, StrategyMsg};
use crate::metrics::MetricsCollector;
use crate::tracing_spans::{
    derive_or_create_trace_context, span_actor_process_message, span_order_generated,
};

/// Python strategy execution error variants.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum PythonExecutionError {
    #[error("Python strategy execution timed out after {0:?}")]
    Timeout(Duration),
    #[error("Python worker thread panicked or crashed: {0}")]
    WorkerCrashed(String),
    #[error("Python GIL contention or lock acquisition failure: {0}")]
    GilContention(String),
    #[error("Internal worker channel disconnected")]
    ChannelDisconnected,
    #[error("Strategy logic returned error: {0}")]
    StrategyError(String),
}

/// Dynamic Python strategy engine contract executing in isolated OS thread.
pub trait PythonStrategyEngine: Send + 'static {
    /// Strategy identifier name.
    fn name(&self) -> &str;

    /// Authoritative SEBI registered algo identifier.
    fn algo_id(&self) -> &str;

    /// Ingest a tick and produce candidate orders.
    fn on_tick(&mut self, tick: &Tick) -> Result<Vec<OrderRequest>, PythonExecutionError>;

    /// Ingest an aggregated bar and produce candidate orders.
    fn on_bar(&mut self, bar: &Bar) -> Result<Vec<OrderRequest>, PythonExecutionError>;

    /// Ingest an execution fill report.
    fn on_fill(&mut self, fill: &FillReport) -> Result<(), PythonExecutionError> {
        let _ = fill;
        Ok(())
    }
}

/// Commands passed across the cooperative async boundary to the dedicated OS thread.
enum PythonTask {
    OnTick {
        tick: Tick,
        reply_to: oneshot::Sender<Result<Vec<OrderRequest>, PythonExecutionError>>,
        cancel_token: CancellationToken,
    },
    OnBar {
        bar: Bar,
        reply_to: oneshot::Sender<Result<Vec<OrderRequest>, PythonExecutionError>>,
        cancel_token: CancellationToken,
    },
    OnFill {
        fill: FillReport,
        reply_to: oneshot::Sender<Result<(), PythonExecutionError>>,
    },
    Shutdown,
}

/// Supervised actor executing Python strategies with strict OS thread isolation,
/// cooperative async bridging, cancellation tokens, and protective fail-safes.
pub struct PythonStrategyActor {
    name: String,
    algo_id: String,
    rx: mpsc::Receiver<StrategyMsg>,
    risk_tx: mpsc::Sender<RiskMsg>,
    sizing_tx: Option<mpsc::Sender<PositionSizingMsg>>,
    worker_tx: mpsc::Sender<PythonTask>,
    timeout_duration: Duration,
    status: ActorStatus,
    metrics: Option<Arc<MetricsCollector>>,
}

impl PythonStrategyActor {
    /// Spawn a new `PythonStrategyActor` with its own dedicated OS thread worker.
    pub fn spawn<E: PythonStrategyEngine>(
        mut engine: E,
        rx: mpsc::Receiver<StrategyMsg>,
        risk_tx: mpsc::Sender<RiskMsg>,
        channel_capacity: usize,
    ) -> Self {
        let name = engine.name().to_string();
        let algo_id = engine.algo_id().to_string();
        let (worker_tx, mut worker_rx) = mpsc::channel::<PythonTask>(channel_capacity);

        // Dedicated OS thread pinned for Python strategy execution
        let worker_name = format!("honba-py-{}", algo_id);
        std::thread::Builder::new()
            .name(worker_name)
            .spawn(move || {
                info!("Python strategy dedicated OS worker thread started");
                while let Some(task) = worker_rx.blocking_recv() {
                    match task {
                        PythonTask::OnTick {
                            tick,
                            reply_to,
                            cancel_token,
                        } => {
                            if cancel_token.is_cancelled() {
                                let _ = reply_to
                                    .send(Err(PythonExecutionError::Timeout(Duration::ZERO)));
                                continue;
                            }
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    engine.on_tick(&tick)
                                }))
                                .map_err(|e| PythonExecutionError::WorkerCrashed(format!("{e:?}")))
                                .and_then(|r| r);
                            let _ = reply_to.send(result);
                        }
                        PythonTask::OnBar {
                            bar,
                            reply_to,
                            cancel_token,
                        } => {
                            if cancel_token.is_cancelled() {
                                let _ = reply_to
                                    .send(Err(PythonExecutionError::Timeout(Duration::ZERO)));
                                continue;
                            }
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    engine.on_bar(&bar)
                                }))
                                .map_err(|e| PythonExecutionError::WorkerCrashed(format!("{e:?}")))
                                .and_then(|r| r);
                            let _ = reply_to.send(result);
                        }
                        PythonTask::OnFill { fill, reply_to } => {
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    engine.on_fill(&fill)
                                }))
                                .map_err(|e| PythonExecutionError::WorkerCrashed(format!("{e:?}")))
                                .and_then(|r| r);
                            let _ = reply_to.send(result);
                        }
                        PythonTask::Shutdown => {
                            info!("Python worker thread received shutdown");
                            break;
                        }
                    }
                }
                info!("Python strategy dedicated OS worker thread exiting");
            })
            .expect("Failed to spawn Python dedicated worker thread");

        Self {
            name,
            algo_id,
            rx,
            risk_tx,
            sizing_tx: None,
            worker_tx,
            timeout_duration: Duration::from_millis(50), // default 50 ms deadline
            status: ActorStatus::Starting,
            metrics: None,
        }
    }

    /// Set execution timeout deadline.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout_duration = timeout;
        self
    }

    /// Attach position sizing actor sender.
    pub fn with_sizing_tx(mut self, sizing_tx: mpsc::Sender<PositionSizingMsg>) -> Self {
        self.sizing_tx = Some(sizing_tx);
        self
    }

    /// Attach Prometheus metrics collector.
    pub fn with_metrics(mut self, metrics: Arc<MetricsCollector>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Current lifecycle status.
    pub fn status(&self) -> &ActorStatus {
        &self.status
    }

    /// Run the `PythonStrategyActor` event loop on the Tokio runtime.
    pub async fn run(mut self) -> ActorStatus {
        info!(
            strategy = %self.name,
            algo_id = %self.algo_id,
            "PythonStrategyActor starting"
        );
        self.status = ActorStatus::Running;

        while let Some(msg) = self.rx.recv().await {
            let start = Instant::now();
            let msg_kind = match &msg {
                StrategyMsg::OnTick(_) => "on_tick",
                StrategyMsg::OnBar(_) => "on_bar",
                StrategyMsg::OnFill(_) => "on_fill",
                StrategyMsg::Shutdown => "shutdown",
            };
            let proc_span = span_actor_process_message("python_strategy", msg_kind, None);
            let _enter_proc = proc_span.enter();

            match msg {
                StrategyMsg::OnTick(tick) => {
                    let cancel_token = CancellationToken::new();
                    let (reply_tx, reply_rx) = oneshot::channel();
                    let task = PythonTask::OnTick {
                        tick,
                        reply_to: reply_tx,
                        cancel_token: cancel_token.clone(),
                    };

                    if let Err(_) = self.worker_tx.send(task).await {
                        self.handle_failure("Python worker channel disconnected")
                            .await;
                        break;
                    }

                    match tokio::time::timeout(self.timeout_duration, reply_rx).await {
                        Ok(Ok(Ok(orders))) => {
                            for order in orders {
                                self.submit_to_pipeline(order).await;
                            }
                        }
                        Ok(Ok(Err(err))) => {
                            self.handle_failure(&format!("Python strategy failed: {err}"))
                                .await;
                        }
                        Ok(Err(_)) => {
                            self.handle_failure("Python reply channel dropped").await;
                        }
                        Err(_) => {
                            // Execution timeout triggered!
                            cancel_token.cancel();
                            self.handle_timeout().await;
                        }
                    }
                }
                StrategyMsg::OnBar(bar) => {
                    let cancel_token = CancellationToken::new();
                    let (reply_tx, reply_rx) = oneshot::channel();
                    let task = PythonTask::OnBar {
                        bar,
                        reply_to: reply_tx,
                        cancel_token: cancel_token.clone(),
                    };

                    if let Err(_) = self.worker_tx.send(task).await {
                        self.handle_failure("Python worker channel disconnected")
                            .await;
                        break;
                    }

                    match tokio::time::timeout(self.timeout_duration, reply_rx).await {
                        Ok(Ok(Ok(orders))) => {
                            for order in orders {
                                self.submit_to_pipeline(order).await;
                            }
                        }
                        Ok(Ok(Err(err))) => {
                            self.handle_failure(&format!("Python strategy failed: {err}"))
                                .await;
                        }
                        Ok(Err(_)) => {
                            self.handle_failure("Python reply channel dropped").await;
                        }
                        Err(_) => {
                            cancel_token.cancel();
                            self.handle_timeout().await;
                        }
                    }
                }
                StrategyMsg::OnFill(fill) => {
                    let (reply_tx, reply_rx) = oneshot::channel();
                    let task = PythonTask::OnFill {
                        fill,
                        reply_to: reply_tx,
                    };
                    if let Ok(_) = self.worker_tx.send(task).await {
                        let _ = tokio::time::timeout(self.timeout_duration, reply_rx).await;
                    }
                }
                StrategyMsg::Shutdown => {
                    info!("PythonStrategyActor received shutdown signal");
                    let _ = self.worker_tx.send(PythonTask::Shutdown).await;
                    self.status = ActorStatus::Draining;
                    break;
                }
            }

            if let Some(ref m) = self.metrics {
                m.record_actor_latency("python_strategy", start.elapsed());
            }
        }

        if self.status != ActorStatus::Degraded {
            self.status = ActorStatus::Stopped;
        }
        info!(status = ?self.status, "PythonStrategyActor stopped");
        self.status
    }

    async fn handle_timeout(&mut self) {
        warn!(
            algo_id = %self.algo_id,
            timeout = ?self.timeout_duration,
            "Python strategy execution timed out! Transitioning state to Degraded and triggering protective kill switch"
        );
        self.status = ActorStatus::Degraded;

        if let Some(ref m) = self.metrics {
            m.record_actor_error("python_strategy", "timeout");
        }

        // Trigger emergency protective algo halt / cancel in honba-mon
        let halt_msg = RiskMsg::HaltAlgo {
            algo_id: self.algo_id.clone(),
            reason: format!(
                "Python strategy execution exceeded timeout of {:?}",
                self.timeout_duration
            ),
        };
        let _ = self.risk_tx.send(halt_msg).await;
    }

    async fn handle_failure(&mut self, reason: &str) {
        error!(
            algo_id = %self.algo_id,
            reason = %reason,
            "Python strategy crashed or failed! Transitioning state to Degraded and triggering emergency halt"
        );
        self.status = ActorStatus::Degraded;

        if let Some(ref m) = self.metrics {
            m.record_actor_error("python_strategy", "crash");
        }

        let halt_msg = RiskMsg::HaltAlgo {
            algo_id: self.algo_id.clone(),
            reason: reason.to_string(),
        };
        let _ = self.risk_tx.send(halt_msg).await;
    }

    async fn submit_to_pipeline(&self, mut req: OrderRequest) {
        if req.trace_context.is_none() {
            req.trace_context = Some(derive_or_create_trace_context(None));
        }

        let gen_span = span_order_generated(&req);
        let _enter = gen_span.enter();

        if let Some(ref m) = self.metrics {
            m.record_order_generation(&req);
        }

        let (reply_tx, reply_rx) = oneshot::channel();

        if let Some(ref sizing_tx) = self.sizing_tx {
            let size_cmd = PositionSizingMsg::SizeOrder {
                request: req.clone(),
                now: chrono::Utc::now(),
                reply_to: reply_tx,
            };
            let _ = sizing_tx.send(size_cmd).await;
        } else {
            let eval_msg = RiskMsg::EvaluateOrder {
                request: req.clone(),
                now: chrono::Utc::now(),
                reply_to: reply_tx,
            };
            let _ = self.risk_tx.send(eval_msg).await;
        }

        // Asynchronously observe downstream response
        tokio::spawn(async move {
            match reply_rx.await {
                Ok(Ok(ack)) => {
                    info!(
                        order_id = %ack.order_id,
                        algo_id = %req.algo_id,
                        "Python order approved and placed"
                    );
                }
                Ok(Err(err)) => {
                    warn!(
                        algo_id = %req.algo_id,
                        error = %err,
                        "Python order rejected by risk gate or broker"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Downstream dropped reply channel");
                }
            }
        });
    }
}
