//! Actor supervision tree managing lifecycles, health monitoring, and graceful drain/shutdown.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use honba_core::Tick;
use honba_mon::RiskEngine;
use honba_te_core::BrokerExecutor;

use crate::execution::ExecutionActor;
use crate::market_data::MarketDataActor;
use crate::messages::{ActorStatus, ExecutionMsg, MarketDataMsg, RiskMsg, StrategyMsg};
use crate::risk::RiskActor;
use crate::strategy::{Strategy, StrategyActor};

/// Supervisor managing actor tasks and coordinating graceful shutdown.
pub struct Supervisor {
    market_data_tx: mpsc::Sender<MarketDataMsg>,
    strategy_tx: mpsc::Sender<StrategyMsg>,
    risk_tx: mpsc::Sender<RiskMsg>,
    execution_tx: mpsc::Sender<ExecutionMsg>,
    broadcast_rx: broadcast::Receiver<Tick>,

    md_handle: JoinHandle<ActorStatus>,
    strat_handle: JoinHandle<ActorStatus>,
    risk_handle: JoinHandle<ActorStatus>,
    exec_handle: JoinHandle<ActorStatus>,
}

impl Supervisor {
    /// Spawn a complete 4-actor runtime supervision tree.
    pub fn spawn<S: Strategy + 'static>(
        strategy: S,
        risk_engine: RiskEngine,
        broker: Arc<dyn BrokerExecutor>,
        channel_capacity: usize,
    ) -> Self {
        let (md_tx, md_rx) = mpsc::channel(channel_capacity);
        let (strat_tx, strat_rx) = mpsc::channel(channel_capacity);
        let (risk_tx, risk_rx) = mpsc::channel(channel_capacity);
        let (exec_tx, exec_rx) = mpsc::channel(channel_capacity);

        // 1. MarketDataActor
        let (md_actor, broadcast_rx) =
            MarketDataActor::new(md_rx, vec![strat_tx.clone()], channel_capacity);
        let md_handle = tokio::spawn(async move { md_actor.run().await });

        // 2. StrategyActor
        let strat_actor = StrategyActor::new(strategy, strat_rx, risk_tx.clone());
        let strat_handle = tokio::spawn(async move { strat_actor.run().await });

        // 3. RiskActor
        let risk_actor = RiskActor::new(risk_engine, risk_rx, exec_tx.clone());
        let risk_handle = tokio::spawn(async move { risk_actor.run().await });

        // 4. ExecutionActor
        let exec_actor = ExecutionActor::new(
            broker,
            exec_rx,
            Some(risk_tx.clone()),
            Some(strat_tx.clone()),
        );
        let exec_handle = tokio::spawn(async move { exec_actor.run().await });

        Self {
            market_data_tx: md_tx,
            strategy_tx: strat_tx,
            risk_tx,
            execution_tx: exec_tx,
            broadcast_rx,
            md_handle,
            strat_handle,
            risk_handle,
            exec_handle,
        }
    }

    /// Access market data sender to inject live ticks or manage subscriptions.
    pub fn market_data_sender(&self) -> &mpsc::Sender<MarketDataMsg> {
        &self.market_data_tx
    }

    /// Access strategy sender for direct command injection.
    pub fn strategy_sender(&self) -> &mpsc::Sender<StrategyMsg> {
        &self.strategy_tx
    }

    /// Access risk sender for administrative halt/kill controls.
    pub fn risk_sender(&self) -> &mpsc::Sender<RiskMsg> {
        &self.risk_tx
    }

    /// Access execution sender for direct order/cancel commands.
    pub fn execution_sender(&self) -> &mpsc::Sender<ExecutionMsg> {
        &self.execution_tx
    }

    /// Subscribe to normalized tick broadcast.
    pub fn resubscribe_ticks(&self) -> broadcast::Receiver<Tick> {
        self.broadcast_rx.resubscribe()
    }

    /// Perform a graceful staged shutdown and drain across all actors.
    ///
    /// 1. Shut down MarketDataActor (stop accepting ticks)
    /// 2. Shut down StrategyActor (drain pending calculations)
    /// 3. Shut down RiskActor (drain evaluations)
    /// 4. Shut down ExecutionActor (drain open broker requests)
    pub async fn shutdown_gracefully(self, timeout: Duration) -> Result<(), String> {
        info!("Supervisor starting staged graceful drain and shutdown");

        // Stage 1: Market data drain
        let _ = self.market_data_tx.send(MarketDataMsg::Shutdown).await;
        // Stage 2: Strategy drain
        let _ = self.strategy_tx.send(StrategyMsg::Shutdown).await;
        // Stage 3: Risk drain
        let _ = self.risk_tx.send(RiskMsg::Shutdown).await;
        // Stage 4: Execution drain
        let _ = self.execution_tx.send(ExecutionMsg::Shutdown).await;

        let shutdown_future = async {
            let (r1, r2, r3, r4) = tokio::join!(
                self.md_handle,
                self.strat_handle,
                self.risk_handle,
                self.exec_handle
            );
            if let Err(e) = r1 {
                warn!("MarketDataActor exited with error: {e}");
            }
            if let Err(e) = r2 {
                warn!("StrategyActor exited with error: {e}");
            }
            if let Err(e) = r3 {
                warn!("RiskActor exited with error: {e}");
            }
            if let Err(e) = r4 {
                warn!("ExecutionActor exited with error: {e}");
            }
        };

        match tokio::time::timeout(timeout, shutdown_future).await {
            Ok(_) => {
                info!("Supervisor: all actors drained and stopped cleanly");
                Ok(())
            }
            Err(_) => {
                error!("Supervisor: shutdown timed out, draining aborted");
                Err("shutdown timed out".into())
            }
        }
    }
}
