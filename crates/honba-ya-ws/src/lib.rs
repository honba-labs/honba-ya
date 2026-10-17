//! WebSocket event bus.
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub enum Event {
    Decision { trader: String, text: String },
    Fill { order_id: String, qty: u64 },
    Pnl { inr: String },
}

pub type EventBus = broadcast::Sender<Event>;

pub fn new_bus() -> EventBus {
    broadcast::channel(1024).0
}
