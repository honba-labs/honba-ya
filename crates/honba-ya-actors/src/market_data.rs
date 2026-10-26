//! Market data ingestion actor.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use honba_core::{InstrumentId, Tick};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

use crate::messages::{ActorStatus, MarketDataMsg, StrategyMsg};
use crate::metrics::MetricsCollector;
use crate::portfolio::SymbolMapper;
use crate::tracing_spans::{span_actor_process_message, span_market_data_coalesce};

/// Backpressure policy for market data ingestion when buffers are full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressurePolicy {
    /// Forward every tick directly without coalescing (zero data loss stream).
    Passthrough,
    /// Coalesce recent ticks keeping only the latest tick per instrument (default).
    #[default]
    CoalesceLatest,
    /// Drop incoming ticks if buffer reaches maximum capacity.
    DropNewest,
    /// Evict oldest ticks when buffer reaches maximum capacity.
    DropOldest,
}

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
    /// Backpressure policy when buffer capacity is reached.
    backpressure_policy: BackpressurePolicy,
    metrics: Option<Arc<MetricsCollector>>,
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
                backpressure_policy: BackpressurePolicy::CoalesceLatest,
                metrics: None,
            },
            broadcast_rx,
        )
    }

    /// Set backpressure policy.
    pub fn with_backpressure_policy(mut self, policy: BackpressurePolicy) -> Self {
        self.backpressure_policy = policy;
        self
    }

    /// Set maximum buffer size before backpressure kicks in.
    pub fn with_max_buffer_size(mut self, max_size: usize) -> Self {
        self.max_buffer_size = max_size;
        self
    }

    /// Set coalesce interval in milliseconds.
    pub fn with_coalesce_interval_ms(mut self, ms: u64) -> Self {
        self.coalesce_interval = ms;
        self
    }

    /// Attach Prometheus metrics collector.
    pub fn with_metrics(mut self, metrics: Arc<MetricsCollector>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Access the symbol mapper for registering tokens.
    pub fn symbol_mapper_mut(&mut self) -> &mut SymbolMapper {
        &mut self.symbol_mapper
    }

    /// Run the market data actor event loop.
    pub async fn run(mut self) -> ActorStatus {
        info!("MarketDataActor starting");
        self.status = ActorStatus::Running;

        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_millis(self.coalesce_interval));
        loop {
            tokio::select! {
                biased;

                msg = self.rx.recv() => {
                    let start = Instant::now();
                    match msg {
                        Some(MarketDataMsg::Subscribe(instrument_id)) => {
                            let _span = span_actor_process_message("market_data", "subscribe", None);
                            self.symbol_mapper.register(instrument_id.to_string(), instrument_id.clone());
                            self.active_subscriptions.insert(instrument_id.clone());
                            debug!("Subscribed to {}", instrument_id);
                            if let Some(ref m) = self.metrics {
                                m.record_actor_latency("market_data", start.elapsed());
                            }
                        }
                        Some(MarketDataMsg::Unsubscribe(instrument_id)) => {
                            let _span = span_actor_process_message("market_data", "unsubscribe", None);
                            self.active_subscriptions.remove(&instrument_id);
                            debug!("Unsubscribed from {}", instrument_id);
                            if let Some(ref m) = self.metrics {
                                m.record_actor_latency("market_data", start.elapsed());
                            }
                        }
                        Some(MarketDataMsg::IngestTick(tick)) => {
                            let _span = span_actor_process_message("market_data", "ingest_tick", None);
                            self.handle_tick(tick).await;
                            if let Some(ref m) = self.metrics {
                                m.record_actor_latency("market_data", start.elapsed());
                            }
                        }
                        Some(MarketDataMsg::IngestBar(bar)) => {
                            let _span = span_actor_process_message("market_data", "ingest_bar", None);
                            self.handle_bar(bar).await;
                            if let Some(ref m) = self.metrics {
                                m.record_actor_latency("market_data", start.elapsed());
                            }
                        }
                        Some(MarketDataMsg::Shutdown) => {
                            info!("MarketDataActor received shutdown; flushing remaining buffer");
                            self.flush_buffer().await;
                            self.status = ActorStatus::Stopped;
                            break;
                        }
                        None => {
                            info!("MarketDataActor channel closed");
                            self.flush_buffer().await;
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

        if self.backpressure_policy == BackpressurePolicy::Passthrough {
            for sender in &self.strategy_senders {
                let _ = sender.send(StrategyMsg::OnTick(tick.clone())).await;
            }
            let _ = self.broadcast_tx.send(tick);
            return;
        }

        // Apply backpressure policy if buffer reached capacity
        if self.tick_buffer.len() >= self.max_buffer_size {
            match self.backpressure_policy {
                BackpressurePolicy::CoalesceLatest => {
                    self.flush_buffer().await;
                    self.tick_buffer.push(tick);
                }
                BackpressurePolicy::DropNewest => {
                    warn!(instrument = %instrument_id, "MarketDataActor buffer full, dropping newest tick due to backpressure");
                    if let Some(ref m) = self.metrics {
                        m.record_backpressure_drop("market_data", "drop_newest");
                    }
                }
                BackpressurePolicy::DropOldest => {
                    warn!(instrument = %instrument_id, "MarketDataActor buffer full, evicting oldest tick due to backpressure");
                    if !self.tick_buffer.is_empty() {
                        self.tick_buffer.remove(0);
                    }
                    if let Some(ref m) = self.metrics {
                        m.record_backpressure_drop("market_data", "drop_oldest");
                    }
                    self.tick_buffer.push(tick);
                }
                BackpressurePolicy::Passthrough => unreachable!(),
            }
        } else {
            self.tick_buffer.push(tick);
        }
    }

    async fn handle_bar(&mut self, bar: honba_core::Bar) {
        // Forward bar to strategies using backpressure try_send
        for sender in &self.strategy_senders {
            if let Err(e) = sender.try_send(StrategyMsg::OnBar(bar.clone())) {
                warn!("Strategy channel busy or closed when forwarding bar: {e}");
                if let Some(ref m) = self.metrics {
                    m.record_backpressure_drop("market_data", "strategy_busy");
                }
            }
        }
    }

    async fn flush_buffer(&mut self) {
        if self.tick_buffer.is_empty() {
            return;
        }

        let ticks = std::mem::take(&mut self.tick_buffer);
        let count = ticks.len();
        let _span = span_market_data_coalesce(count);

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

        let unique_count = latest.len();
        let coalesced_drops = count.saturating_sub(unique_count);
        if coalesced_drops > 0 {
            if let Some(ref m) = self.metrics {
                for _ in 0..coalesced_drops {
                    m.record_backpressure_drop("market_data", "coalesced");
                }
            }
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

