//! Strategy actor for generating trading signals and candidate orders.

use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use honba_core::{Bar, FillReport, OrderRequest, Tick};

use crate::messages::{ActorStatus, PositionSizingMsg, RiskMsg, StrategyMsg};
use crate::metrics::MetricsCollector;
use crate::tracing_spans::{
    derive_or_create_trace_context, span_actor_process_message, span_order_generated,
};

/// Contract implemented by trading strategies (momentum, mean-reversion, ML/LLM-driven).
#[async_trait]
pub trait Strategy: Send + Sync {
    /// Strategy name for logging and telemetry.
    fn name(&self) -> &str;

    /// SEBI-registered algo identifier for audit trails and OTR accounting.
    fn algo_id(&self) -> &str;

    /// Process a new market data tick and produce candidate orders.
    async fn on_tick(&mut self, tick: &Tick) -> Vec<OrderRequest>;

    /// Process a new OHLCV bar and produce candidate orders.
    async fn on_bar(&mut self, bar: &Bar) -> Vec<OrderRequest> {
        let _ = bar;
        Vec::new()
    }

    /// Update internal state upon receiving a fill report.
    async fn on_fill(&mut self, fill: &FillReport) {
        let _ = fill;
    }
}

/// Strategy actor executing strategy logic and routing orders to the pre-trade risk gate.
pub struct StrategyActor<S: Strategy> {
    strategy: S,
    rx: mpsc::Receiver<StrategyMsg>,
    risk_tx: mpsc::Sender<RiskMsg>,
    sizing_tx: Option<mpsc::Sender<PositionSizingMsg>>,
    status: ActorStatus,
    metrics: Option<Arc<MetricsCollector>>,
}

impl<S: Strategy> StrategyActor<S> {
    /// Create a new strategy actor.
    pub fn new(
        strategy: S,
        rx: mpsc::Receiver<StrategyMsg>,
        risk_tx: mpsc::Sender<RiskMsg>,
    ) -> Self {
        Self {
            strategy,
            rx,
            risk_tx,
            sizing_tx: None,
            status: ActorStatus::Starting,
            metrics: None,
        }
    }

    /// Attach a position sizing actor sender for dynamic position sizing chaining.
    pub fn with_sizing_tx(mut self, sizing_tx: mpsc::Sender<PositionSizingMsg>) -> Self {
        self.sizing_tx = Some(sizing_tx);
        self
    }

    /// Attach Prometheus metrics collector.
    pub fn with_metrics(mut self, metrics: Arc<MetricsCollector>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Access the underlying strategy.
    pub fn strategy(&self) -> &S {
        &self.strategy
    }

    /// Run the strategy actor event loop.
    pub async fn run(mut self) -> ActorStatus {
        info!(
            strategy = self.strategy.name(),
            algo_id = self.strategy.algo_id(),
            "StrategyActor starting"
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
            let proc_span = span_actor_process_message("strategy", msg_kind, None);
            let _enter_proc = proc_span.enter();

            match msg {
                StrategyMsg::OnTick(tick) => {
                    let orders = self.strategy.on_tick(&tick).await;
                    for order in orders {
                        self.submit_to_pipeline(order).await;
                    }
                }
                StrategyMsg::OnBar(bar) => {
                    let orders = self.strategy.on_bar(&bar).await;
                    for order in orders {
                        self.submit_to_pipeline(order).await;
                    }
                }
                StrategyMsg::OnFill(fill) => {
                    self.strategy.on_fill(&fill).await;
                }
                StrategyMsg::Shutdown => {
                    info!("StrategyActor received shutdown signal");
                    self.status = ActorStatus::Draining;
                    break;
                }
            }

            if let Some(ref m) = self.metrics {
                m.record_actor_latency("strategy", start.elapsed());
            }
        }

        self.status = ActorStatus::Stopped;
        info!("StrategyActor stopped");
        self.status
    }

    async fn submit_to_pipeline(&self, mut req: OrderRequest) {
        // Ensure W3C distributed trace context is stamped
        if req.trace_context.is_none() {
            req.trace_context = Some(derive_or_create_trace_context(None));
        }

        // Structured tracing span: OrderGenerated
        let gen_span = span_order_generated(&req);
        let _enter = gen_span.enter();

        if let Some(ref m) = self.metrics {
            m.record_order_generation(&req);
        }

        let (reply_tx, reply_rx) = oneshot::channel();

        if let Some(ref sizing_tx) = self.sizing_tx {
            let size_cmd = PositionSizingMsg::SizeOrder {
                request: req.clone(),
                now: Utc::now(),
                reply_to: reply_tx,
            };
            if let Err(e) = sizing_tx.send(size_cmd).await {
                error!(error = %e, "Failed to send order to PositionSizingActor");
                return;
            }
        } else {
            let eval_msg = RiskMsg::EvaluateOrder {
                request: req.clone(),
                now: Utc::now(),
                reply_to: reply_tx,
            };

            if let Err(e) = self.risk_tx.send(eval_msg).await {
                error!(error = %e, "Failed to send order to RiskActor");
                return;
            }
        }

        // Wait for risk evaluation and broker ack asynchronously
        match reply_rx.await {
            Ok(Ok(ack)) => {
                info!(
                    order_id = %ack.order_id,
                    algo_id = %req.algo_id,
                    "Order approved by risk gate and accepted by broker"
                );
            }
            Ok(Err(refusal_or_broker_err)) => {
                warn!(
                    algo_id = %req.algo_id,
                    error = %refusal_or_broker_err,
                    "Order rejected by risk gate or broker"
                );
            }
            Err(e) => {
                error!(error = %e, "Reply channel dropped by downstream actor");
            }
        }
    }
}
