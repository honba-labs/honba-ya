//! Market data ingestion actor.

use std::collections::HashSet;

use honba_core::{InstrumentId, Tick};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

use crate::messages::{ActorStatus, MarketDataMsg, StrategyMsg};
use crate::portfolio::SymbolMapper;

/// Market data ingestion actor.
pub struct MarketDataActor {
    rx: mpsc::Receiver<MarketDataMsg>,
    strategy_senders: Vec<mpsc::Sender<StrategyMsg>>,
    broadcast_tx: broadcast::Sender<Tick>,
    symbol_mapper: SymbolMapper,
    active_subscriptions: HashSet<InstrumentId>,
    status: ActorStatus,
    /// Tick buffer for coalescing recent ticks within a window.
    tick_buffer: Vec<Tick>,
    /// Maximum number of ticks to buffer before coalescing.
    max_buffer_size: usize,
    /// Tick coalescing timer interval in milliseconds.
    coalesce_interval: u64,
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
                tick_buffer: Vec::new(),
                max_buffer_size: 100,
                coalesce_interval: 50, // coalesce every 50ms
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

        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(self.coalesce_interval));
        loop {
            tokio::select! {
                biased;

                msg = self.rx.recv() => {
                    match msg {
                        Some(MarketDataMsg::Subscribe(instrument_id)) => {
                            self.symbol_mapper.register(instrument_id.to_string(), instrument_id.clone());
                            self.active_subscriptions.insert(instrument_id.clone());
                            debug!("Subscribed to {}", instrument_id);
                        }
                        Some(MarketDataMsg::Unsubscribe(instrument_id)) => {
                            self.active_subscriptions.remove(&instrument_id);
                            debug!("Unsubscribed from {}", instrument_id);
                        }
                        Some(MarketDataMsg::IngestTick(tick)) => {
                            self.handle_tick(tick).await;
                        }
                        Some(MarketDataMsg::IngestBar(bar)) => {
                            self.handle_bar(bar).await;
                        }
                        Some(MarketDataMsg::Shutdown) => {
                            info!("MarketDataActor received shutdown");
                            self.status = ActorStatus::Stopped;
                            break;
                        }
                        None => {
                            info!("MarketDataActor channel closed");
                            self.status = ActorStatus::Stopped;
                            break;
                        }
                    }
                }

                _ = interval.tick() => {
                    self.flush_buffer().await;
                }
            }
        }
        self.status
    }

    async fn handle_tick(&mut self, tick: Tick) {
        // Only forward ticks for active subscriptions
        let instrument_id = match &tick {
            Tick::Ltp { instrument, .. } => instrument,
            Tick::Quote { instrument, .. } => instrument,
            Tick::Depth { instrument, .. } => instrument,
        };

        if !self.active_subscriptions.contains(instrument_id) {
            return;
        }

        // Buffer for coalescing (clone to avoid move)
        self.tick_buffer.push(tick.clone());
        if self.tick_buffer.len() >= self.max_buffer_size {
            self.flush_buffer().await;
        }
    }

    async fn handle_bar(&mut self, bar: honba_core::Bar) {
        // Forward bar to strategies
        for sender in &self.strategy_senders {
            let _ = sender.try_send(StrategyMsg::OnBar(bar.clone()));
        }
    }

    async fn flush_buffer(&mut self) {
        if self.tick_buffer.is_empty() {
            return;
        }

        let ticks = std::mem::take(&mut self.tick_buffer);

        // Coalesce ticks by instrument: keep only the latest tick per instrument
        let mut latest = std::collections::HashMap::new();
        for tick in ticks {
            let key = match &tick {
                Tick::Ltp { instrument, .. } => instrument.clone(),
                Tick::Quote { instrument, .. } => instrument.clone(),
                Tick::Depth { instrument, .. } => instrument.clone(),
            };
            latest.insert(key, tick);
        }

        for tick in latest.into_values() {
            let tick_for_strategies = tick.clone();
            let _ = self.broadcast_tx.send(tick);
            for sender in &self.strategy_senders {
                let _ = sender.try_send(StrategyMsg::OnTick(tick_for_strategies.clone()));
            }
        }
    }
}