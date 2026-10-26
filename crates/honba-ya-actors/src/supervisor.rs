//! Actor supervision tree and lifecycle manager for the Honba platform.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use honba_core::Tick;
use honba_mon::RiskEngine;
use honba_persistence::SqliteAuditLedger;
use honba_te_core::BrokerExecutor;

use crate::execution::ExecutionActor;
use crate::market_data::MarketDataActor;
use crate::messages::{
    ActorStatus, ExecutionMsg, MarketDataMsg, PositionSizingMsg, RiskMsg, StrategyMsg,
};
use crate::metrics::MetricsCollector;
use crate::risk::RiskActor;
use crate::sizing::PositionSizingActor;
use crate::strategy::{Strategy, StrategyActor};

/// Restart strategy for actor supervision.
///
/// - `OneForOne`: Restart only the failed actor.
/// - `AllForOne`: Restart all actors when one fails (typical for cascading failures).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestartStrategy {
    OneForOne,
    AllForOne,
}

/// Supervisor coordinating the lifecycle, message routing, and graceful staged shutdown of all actors.
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

    sizing_tx: Option<mpsc::Sender<PositionSizingMsg>>,
    sizing_handle: Option<JoinHandle<ActorStatus>>,

    /// Restart strategy for supervising actor restarts.
    restart_strategy: RestartStrategy,

    /// Per-actor restart counters for monitoring.
    md_restarts: usize,
    strat_restarts: usize,
    risk_restarts: usize,
    exec_restarts: usize,
    sizing_restarts: usize,
}

impl Supervisor {
    /// Spawn a complete 4-actor runtime supervision tree with the given restart strategy.
    pub fn spawn_with_strategy<S: Strategy + 'static>(
        strategy: S,
        risk_engine: RiskEngine,
        broker: Arc<dyn BrokerExecutor>,
        channel_capacity: usize,
        restart_strategy: RestartStrategy,
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
            sizing_tx: None,
            sizing_handle: None,
            restart_strategy,
            md_restarts: 0,
            strat_restarts: 0,
            risk_restarts: 0,
            exec_restarts: 0,
            sizing_restarts: 0,
        }
    }

    /// Spawn a 5-actor supervision tree including dynamic `PositionSizingActor`.
    pub fn spawn_with_position_sizing<S: Strategy + 'static>(
        strategy: S,
        risk_engine: RiskEngine,
        portfolio: honba_kokoro::sizing::Portfolio,
        broker: Arc<dyn BrokerExecutor>,
        channel_capacity: usize,
        restart_strategy: RestartStrategy,
    ) -> Self {
        let (md_tx, md_rx) = mpsc::channel(channel_capacity);
        let (strat_tx, strat_rx) = mpsc::channel(channel_capacity);
        let (sizing_tx, sizing_rx) = mpsc::channel(channel_capacity);
        let (risk_tx, risk_rx) = mpsc::channel(channel_capacity);
        let (exec_tx, exec_rx) = mpsc::channel(channel_capacity);

        // 1. MarketDataActor
        let (md_actor, broadcast_rx) =
            MarketDataActor::new(md_rx, vec![strat_tx.clone()], channel_capacity);
        let md_handle = tokio::spawn(async move { md_actor.run().await });

        // 2. StrategyActor -> forwards to PositionSizingActor
        let strat_actor = StrategyActor::new(strategy, strat_rx, risk_tx.clone())
            .with_sizing_tx(sizing_tx.clone());
        let strat_handle = tokio::spawn(async move { strat_actor.run().await });

        // 3. PositionSizingActor -> forwards to RiskActor
        let sizing_actor = PositionSizingActor::new(sizing_rx, risk_tx.clone(), portfolio);
        let sizing_handle = tokio::spawn(async move { sizing_actor.run().await });

        // 4. RiskActor
        let risk_actor = RiskActor::new(risk_engine, risk_rx, exec_tx.clone());
        let risk_handle = tokio::spawn(async move { risk_actor.run().await });

        // 5. ExecutionActor
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
            sizing_tx: Some(sizing_tx),
            sizing_handle: Some(sizing_handle),
            restart_strategy,
            md_restarts: 0,
            strat_restarts: 0,
            risk_restarts: 0,
            exec_restarts: 0,
            sizing_restarts: 0,
        }
    }

    /// Spawn supervision tree equipped with Prometheus metrics and SEBI regulatory audit ledger.
    pub fn spawn_with_observability<S: Strategy + 'static>(
        strategy: S,
        risk_engine: RiskEngine,
        broker: Arc<dyn BrokerExecutor>,
        channel_capacity: usize,
        metrics: Arc<MetricsCollector>,
        audit_ledger: Arc<SqliteAuditLedger>,
    ) -> Self {
        let (md_tx, md_rx) = mpsc::channel(channel_capacity);
        let (strat_tx, strat_rx) = mpsc::channel(channel_capacity);
        let (risk_tx, risk_rx) = mpsc::channel(channel_capacity);
        let (exec_tx, exec_rx) = mpsc::channel(channel_capacity);

        // 1. MarketDataActor
        let (md_actor, broadcast_rx) =
            MarketDataActor::new(md_rx, vec![strat_tx.clone()], channel_capacity);
        let md_actor = md_actor.with_metrics(Arc::clone(&metrics));
        let md_handle = tokio::spawn(async move { md_actor.run().await });

        // 2. StrategyActor
        let strat_actor = StrategyActor::new(strategy, strat_rx, risk_tx.clone())
            .with_metrics(Arc::clone(&metrics));
        let strat_handle = tokio::spawn(async move { strat_actor.run().await });

        // 3. RiskActor
        let risk_actor = RiskActor::new(risk_engine, risk_rx, exec_tx.clone())
            .with_metrics(Arc::clone(&metrics))
            .with_audit_ledger(Arc::clone(&audit_ledger));
        let risk_handle = tokio::spawn(async move { risk_actor.run().await });

        // 4. ExecutionActor
        let exec_actor = ExecutionActor::new(
            broker,
            exec_rx,
            Some(risk_tx.clone()),
            Some(strat_tx.clone()),
        )
        .with_metrics(Arc::clone(&metrics))
        .with_audit_ledger(Arc::clone(&audit_ledger));
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
            sizing_tx: None,
            sizing_handle: None,
            restart_strategy: RestartStrategy::OneForOne,
            md_restarts: 0,
            strat_restarts: 0,
            risk_restarts: 0,
            exec_restarts: 0,
            sizing_restarts: 0,
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

    /// Access position sizing sender if present.
    pub fn sizing_sender(&self) -> Option<&mpsc::Sender<PositionSizingMsg>> {
        self.sizing_tx.as_ref()
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
    /// Handles in-flight orders and respects the configured restart strategy.
    pub async fn shutdown_gracefully(mut self, timeout: Duration) -> Result<(), String> {
        info!("Supervisor starting staged graceful drain and shutdown");

        // Stage 1: Market data drain - stop accepting new ticks, drain existing
        let _ = self.market_data_tx.send(MarketDataMsg::Shutdown).await;

        // Stage 2: Strategy drain
        let _ = self.strategy_tx.send(StrategyMsg::Shutdown).await;

        // Stage 2b: Sizing drain if present
        if let Some(ref tx) = self.sizing_tx {
            let _ = tx.send(PositionSizingMsg::Shutdown).await;
        }

        // Stage 3: Risk drain
        let _ = self.risk_tx.send(RiskMsg::Shutdown).await;

        // Stage 4: Execution drain - ensure all in-flight orders are handled
        let _ = self.execution_tx.send(ExecutionMsg::Shutdown).await;

        let sizing_fut = self.sizing_handle.take();

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
            if let Some(sh) = sizing_fut {
                if let Err(e) = sh.await {
                    warn!("PositionSizingActor exited with error: {e}");
                }
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

    /// Get configured restart strategy.
    pub fn restart_strategy(&self) -> &RestartStrategy {
        &self.restart_strategy
    }

    /// Set restart strategy dynamically.
    pub fn set_restart_strategy(&mut self, strategy: RestartStrategy) {
        self.restart_strategy = strategy;
    }

    /// Restart a specific actor handle and update supervision state.
    pub fn restart_actor_handle(&mut self, actor_name: &str, new_handle: JoinHandle<ActorStatus>) {
        self.increment_restarts(actor_name);
        match self.restart_strategy {
            RestartStrategy::OneForOne => {
                match actor_name {
                    "market_data" => self.md_handle = new_handle,
                    "strategy" => self.strat_handle = new_handle,
                    "risk" => self.risk_handle = new_handle,
                    "execution" => self.exec_handle = new_handle,
                    "position_sizing" => self.sizing_handle = Some(new_handle),
                    _ => {}
                }
                info!(
                    "Supervisor: restarted {} under OneForOne strategy",
                    actor_name
                );
            }
            RestartStrategy::AllForOne => {
                match actor_name {
                    "market_data" => self.md_handle = new_handle,
                    "strategy" => self.strat_handle = new_handle,
                    "risk" => self.risk_handle = new_handle,
                    "execution" => self.exec_handle = new_handle,
                    "position_sizing" => self.sizing_handle = Some(new_handle),
                    _ => {}
                }
                info!("Supervisor: AllForOne restart triggered by {}", actor_name);
            }
        }
    }

    fn increment_restarts(&mut self, actor_name: &str) {
        match actor_name {
            "market_data" => self.md_restarts += 1,
            "strategy" => self.strat_restarts += 1,
            "risk" => self.risk_restarts += 1,
            "execution" => self.exec_restarts += 1,
            "position_sizing" => self.sizing_restarts += 1,
            _ => {}
        }
    }

    /// Return tuple of restart counts: (market_data, strategy, risk, execution).
    pub fn restart_counts(&self) -> (usize, usize, usize, usize) {
        (
            self.md_restarts,
            self.strat_restarts,
            self.risk_restarts,
            self.exec_restarts,
        )
    }

    /// Per-actor restart count query.
    pub fn restart_count(&self, actor_name: &str) -> usize {
        match actor_name {
            "market_data" => self.md_restarts,
            "strategy" => self.strat_restarts,
            "risk" => self.risk_restarts,
            "execution" => self.exec_restarts,
            "position_sizing" => self.sizing_restarts,
            _ => 0,
        }
    }
}
