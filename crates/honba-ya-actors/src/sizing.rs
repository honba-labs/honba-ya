//! Supervised Position Sizing Actor executing hexagonal `PositionSizer` port.
//!
//! Sits between `StrategyActor` and `RiskActor` in the supervision tree.
//! Dynamically sizes candidate orders based on `honba-kokoro` instantaneous
//! portfolio equity, gross leverage headroom, and stop-loss distances.

use std::sync::Arc;
use std::time::Instant;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::mpsc;
use tracing::{info, warn};

use honba_core::ports::{CanonicalPositionSizer, PositionSizer};
use honba_core::sizing::{PositionSizingRequest, SizingMethod};
use honba_core::{PositionSide, Price, Quantity};
use honba_kokoro::sizing::{Portfolio, evaluate_risk_budget};

use crate::messages::{ActorStatus, PositionSizingMsg, RiskMsg};
use crate::metrics::MetricsCollector;
use crate::tracing_spans::span_actor_process_message;

/// Supervised actor enforcing dynamic, portfolio-aware position sizing.
pub struct PositionSizingActor {
    rx: mpsc::Receiver<PositionSizingMsg>,
    risk_tx: mpsc::Sender<RiskMsg>,
    sizer: Box<dyn PositionSizer>,
    portfolio: Portfolio,
    sizing_method: SizingMethod,
    risk_pct: Decimal,
    status: ActorStatus,
    metrics: Option<Arc<MetricsCollector>>,
}

impl PositionSizingActor {
    /// Create a new `PositionSizingActor`.
    pub fn new(
        rx: mpsc::Receiver<PositionSizingMsg>,
        risk_tx: mpsc::Sender<RiskMsg>,
        portfolio: Portfolio,
    ) -> Self {
        Self {
            rx,
            risk_tx,
            sizer: Box::new(CanonicalPositionSizer),
            portfolio,
            sizing_method: SizingMethod::FixedFractional {
                risk_pct: dec!(0.01),
            },
            risk_pct: dec!(0.01),
            status: ActorStatus::Starting,
            metrics: None,
        }
    }

    /// Set a custom `PositionSizer` port implementation.
    pub fn with_sizer(mut self, sizer: Box<dyn PositionSizer>) -> Self {
        self.sizer = sizer;
        self
    }

    /// Set dynamic sizing model.
    pub fn with_sizing_method(mut self, method: SizingMethod) -> Self {
        self.sizing_method = method;
        self
    }

    /// Set risk percentage per trade.
    pub fn with_risk_pct(mut self, risk_pct: Decimal) -> Self {
        self.risk_pct = risk_pct;
        self
    }

    /// Attach Prometheus metrics collector.
    pub fn with_metrics(mut self, metrics: Arc<MetricsCollector>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Access current portfolio.
    pub fn portfolio(&self) -> &Portfolio {
        &self.portfolio
    }

    /// Run the `PositionSizingActor` event loop.
    pub async fn run(mut self) -> ActorStatus {
        info!("PositionSizingActor starting");
        self.status = ActorStatus::Running;

        while let Some(msg) = self.rx.recv().await {
            let start = Instant::now();
            let span = span_actor_process_message("position_sizing", "dispatch", None);
            let _enter = span.enter();

            match msg {
                PositionSizingMsg::SizeOrder {
                    mut request,
                    now,
                    reply_to,
                } => {
                    let budget = evaluate_risk_budget(&self.portfolio, self.risk_pct);
                    let entry_price = request
                        .order_type
                        .limit_price()
                        .unwrap_or(Price(dec!(100.00)));

                    let side = match request.side {
                        honba_core::OrderSide::Buy => PositionSide::Long,
                        honba_core::OrderSide::Sell => PositionSide::Short,
                    };

                    let sizing_req = PositionSizingRequest::new(
                        request.instrument.clone(),
                        side,
                        entry_price,
                        budget.equity,
                        budget,
                        self.sizing_method.clone(),
                    );

                    match self.sizer.calculate_size(&sizing_req) {
                        Ok(result) => {
                            if result.quantity.value() > Decimal::ZERO {
                                request.quantity = result.quantity;
                            } else {
                                warn!(
                                    algo_id = %request.algo_id,
                                    instrument = %request.instrument,
                                    reason = ?result.reason,
                                    "Dynamic position sizing returned zero quantity; keeping minimal lot"
                                );
                                request.quantity = Quantity::from_u64(1);
                            }
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                "Dynamic position sizing calculation failed; forwarding unadjusted quantity"
                            );
                            if let Some(ref m) = self.metrics {
                                m.record_actor_error("position_sizing", "calculation_failed");
                            }
                        }
                    }

                    // Forward sized request to RiskActor
                    let risk_cmd = RiskMsg::EvaluateOrder {
                        request,
                        now,
                        reply_to,
                    };
                    if let Err(e) = self.risk_tx.send(risk_cmd).await {
                        warn!("Failed to forward sized order to RiskActor: {e}");
                        if let Some(ref m) = self.metrics {
                            m.record_actor_error("position_sizing", "risk_channel_closed");
                        }
                    }
                }
                PositionSizingMsg::UpdatePortfolio(p) => {
                    self.portfolio = p;
                }
                PositionSizingMsg::SetSizingMethod(m) => {
                    self.sizing_method = m;
                }
                PositionSizingMsg::SetRiskPct(r) => {
                    self.risk_pct = r;
                }
                PositionSizingMsg::Shutdown => {
                    info!("PositionSizingActor received shutdown signal");
                    self.status = ActorStatus::Draining;
                    break;
                }
            }

            if let Some(ref m) = self.metrics {
                m.record_actor_latency("position_sizing", start.elapsed());
            }
        }

        self.status = ActorStatus::Stopped;
        info!("PositionSizingActor stopped");
        self.status
    }
}
