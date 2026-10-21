//! Market data actor for ingesting, normalizing, and broadcasting live feeds.

use std::collections::{HashMap, HashSet};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

use honba_core::{Bar, InstrumentId, Tick};

use crate::messages::{ActorStatus, MarketDataMsg, StrategyMsg};

/// Minimal symbol mapper for broker token normalization.
#[derive(Debug, Default)]
pub struct SymbolMapper {
    token_to_instrument: HashMap<String, InstrumentId>,
    instrument_to_token: HashMap<InstrumentId, String>,
}

impl SymbolMapper {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn map_token(&self, token: &str) -> Option<InstrumentId> {
        self.token_to_instrument.get(token).cloned()
    }

    pub fn map_instrument(&self, instrument: &InstrumentId) -> Option<String> {
        self.instrument_to_token.get(instrument).cloned()
    }

    pub fn register(&mut self, instrument: InstrumentId, token: String) {
        let token_clone = token.clone();
        self.token_to_instrument.insert(token, instrument.clone());
        self.instrument_to_token.insert(instrument, token_clone);
    }
}

/// Market data ingestion actor.
pub struct MarketDataActor {
    rx: mpsc::Receiver<MarketDataMsg>,
    strategy_senders: Vec<mpsc::Sender<StrategyMsg>>,
    broadcast_tx: broadcast::Sender<Tick>,
    symbol_mapper: SymbolMapper,
    active_subscriptions: HashSet<InstrumentId>,
    status: ActorStatus,
}

impl MarketDataActor {
    /// Create a new market data actor.
    pub fn new(
        rx: mpsc::Receiver<MarketDataMsg>,
        strategy_senders: Vec<mpsc::Sender<StrategyMsg>>,
        broadcast_capacity: usize,
    ) -> (Self, broadcast::Receiver<Tick>) {
        let (broadcast_tx, broadcast_rx) = broadcast::channel(broadcast_capacity);
        (
            Self {
                rx,
                strategy_senders,
                broadcast_tx,
                symbol_mapper: SymbolMapper::new(),
                active_subscriptions: HashSet::new(),
                status: ActorStatus::Starting,
            },
            broadcast_rx,
        )
    }

    /// Access the symbol mapper for registering tokens.
    pub fn symbol_mapper_mut(&mut self) -> &mut SymbolMapper {
        &mut self.symbol_mapper
    }

    /// Run the market data actor event loop.
    pub async fn run(mut self) -> ActorStatus {
        info!("MarketDataActor starting");
        self.status = ActorStatus::Running;

        while let Some(msg) = self.rx.recv().await {
            match msg {
                MarketDataMsg::IngestTick(tick) => {
                    self.handle_tick(tick).await;
                }
                MarketDataMsg::IngestBar(bar) => {
                    self.handle_bar(bar).await;
                }
                MarketDataMsg::Subscribe(inst) => {
                    debug!(instrument = %inst, "Subscribed instrument");
                    self.active_subscriptions.insert(inst);
                }
                MarketDataMsg::Unsubscribe(inst) => {
                    debug!(instrument = %inst, "Unsubscribed instrument");
                    self.active_subscriptions.remove(&inst);
                }
                MarketDataMsg::Shutdown => {
                    info!("MarketDataActor received shutdown signal");
                    self.status = ActorStatus::Draining;
                    break;
                }
            }
        }

        self.status = ActorStatus::Stopped;
        info!("MarketDataActor stopped");
        self.status
    }

    async fn handle_tick(&self, tick: Tick) {
        // Broadcast to general listeners
        let _ = self.broadcast_tx.send(tick.clone());

        // Dispatch to all subscribed strategy actors
        for sender in &self.strategy_senders {
            if let Err(e) = sender.try_send(StrategyMsg::OnTick(tick.clone())) {
                match e {
                    mpsc::error::TrySendError::Full(_) => {
                        warn!("StrategyActor queue full, dropping tick under backpressure");
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        debug!("StrategyActor channel closed");
                    }
                }
            }
        }
    }

    async fn handle_bar(&self, bar: Bar) {
        for sender in &self.strategy_senders {
            let _ = sender.send(StrategyMsg::OnBar(bar.clone())).await;
        }
    }
}
