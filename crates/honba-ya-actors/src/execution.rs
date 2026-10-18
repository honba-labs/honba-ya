//! Execution actor managing broker order placement, status queries, retries, and fills.

use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

use honba_te_core::BrokerExecutor;

use crate::messages::{ActorStatus, ExecutionMsg, RiskMsg, StrategyMsg};

/// Execution actor interfacing with broker adapters.
pub struct ExecutionActor {
    broker: Arc<dyn BrokerExecutor>,
    rx: mpsc::Receiver<ExecutionMsg>,
    risk_tx: Option<mpsc::Sender<RiskMsg>>,
    #[allow(dead_code)]
    strategy_tx: Option<mpsc::Sender<StrategyMsg>>,
    status: ActorStatus,
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
        }
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
                    let res = self.broker.place_order(request).await;
                    let _ = reply_to.send(res);
                }

                ExecutionMsg::CancelOrder {
                    order_id,
                    algo_id,
                    reply_to,
                } => {
                    let res = self.broker.cancel_order(&order_id).await;
                    if res.is_ok() {
                        if let Some(ref r_tx) = self.risk_tx {
                            let _ = r_tx.send(RiskMsg::RecordCancellation { algo_id }).await;
                        }
                    }
                    let _ = reply_to.send(res);
                }

                ExecutionMsg::QueryStatus { order_id, reply_to } => {
                    let res = self.broker.get_order_status(&order_id).await;
                    let _ = reply_to.send(res);
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
