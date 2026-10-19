//! Strategy actor for generating trading signals and candidate orders.

use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use honba_core::{Bar, FillReport, OrderRequest, Tick};

use crate::messages::{ActorStatus, RiskMsg, StrategyMsg};
use crate::metrics::MetricsCollector;
use crate::tracing_spans::span_order_generated;

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
            status: ActorStatus::Starting,
            metrics: None,
        }
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
            match msg {
                StrategyMsg::OnTick(tick) => {
                    let orders = self.strategy.on_tick(&tick).await;
                    for order in orders {
                        self.submit_to_risk(order).await;
                    }
                }
                StrategyMsg::OnBar(bar) => {
                    let orders = self.strategy.on_bar(&bar).await;
                    for order in orders {
                        self.submit_to_risk(order).await;
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
        }

        self.status = ActorStatus::Stopped;
        info!("StrategyActor stopped");
        self.status
    }

    async fn submit_to_risk(&self, req: OrderRequest) {
        // Structured tracing span: OrderGenerated
        let gen_span = span_order_generated(&req);
        let _enter = gen_span.enter();

        if let Some(ref m) = self.metrics {
            m.record_order_generation(&req);
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        let eval_msg = RiskMsg::EvaluateOrder {
            request: req.clone(),
            now: Utc::now(),
            reply_to: reply_tx,
        };

        if let Err(e) = self.risk_tx.send(eval_msg).await {
            error!(error = %e, "Failed to send order to RiskActor");
            return;
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
                error!(error = %e, "RiskActor dropped reply channel");
            }
        }
    }
}
