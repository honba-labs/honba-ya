//! WebSocket endpoint for live market data (`GET /ws`, `GET /ws/market`).
//!
//! # Functionality gap
//!
//! This used to subscribe directly to a `MarketDataActor`'s broadcast channel via
//! `state.market_data_broadcast_rx` and `crate::actors::Supervisor`. Neither exists: `AppState`
//! (`state.rs`) has no `market_data_broadcast_rx` field, and this crate has no `actors` module —
//! there is no live tick broadcast tee wired into the composition root to subscribe to. The
//! previous version of this file also had a plain syntax error (`_ = broadcast_msg => {}` as a
//! second match arm on the same `tokio::select!` branch) and referenced undefined locals (`s`,
//! `prev`), so it could never have compiled as written; this is a feature gap, not a rename.
//!
//! Until a real market-data broadcast channel is wired into [`crate::AppState`], this handler
//! honestly accepts the WebSocket connection and the client protocol
//! ([`WsClientMsg::Subscribe`]/`Unsubscribe`/`Ping`/`Close`) — tracking subscriptions and
//! answering pings — but pushes no [`WsTick`] frames, since there is nothing to source them
//! from. A caller only relying on ping/pong keepalive and connection lifecycle sees correct
//! behavior; a caller expecting live ticks gets none.

use std::collections::HashSet;
use std::sync::Arc;
use tracing::debug;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

use crate::AppState;

/// Market tick payload streamed to WebSocket clients.
///
/// Matches the expected shape in `honba-fe/apps/web/src/core/data-layer.ts`. Not currently
/// produced by [`handle_socket`] — see the module doc — but kept as the wire contract a real
/// market-data tee would serialize into.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsTick {
    /// Instrument symbol, e.g. "RELIANCE".
    pub symbol: String,
    /// Last traded or mid price.
    pub price: f64,
    /// Absolute change from previous price or open.
    pub change: f64,
    /// Percentage change from previous price or open.
    #[serde(rename = "changePercent")]
    pub change_percent: f64,
    /// Traded volume or total bid/ask size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<f64>,
    /// Milliseconds since Unix epoch.
    pub timestamp: i64,
}

/// Subscribe/unsubscribe messages from WebSocket clients.
#[derive(Debug, Serialize, Deserialize)]
pub enum WsClientMsg {
    /// Subscribe to tick updates for an instrument.
    Subscribe { symbol: String },
    /// Unsubscribe from tick updates for an instrument.
    Unsubscribe { symbol: String },
    /// Ping message for connection liveness.
    Ping,
    /// Close connection.
    Close,
}

/// Upgrades an HTTP connection to a WebSocket for live market data.
///
/// See the module doc: no ticks are currently pushed (no broadcast source is wired in), but the
/// connection and client subscribe/ping/close protocol work.
pub async fn ws_handler(ws: WebSocketUpgrade, State(app_state): State<Arc<AppState>>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, app_state))
}

/// Handles one WebSocket connection's client-message protocol.
async fn handle_socket(mut socket: WebSocket, _state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut subscribed: HashSet<String> = HashSet::new();

    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                let Ok(parsed) = serde_json::from_str::<WsClientMsg>(&text) else {
                    continue;
                };
                match parsed {
                    WsClientMsg::Subscribe { symbol } => {
                        debug!("client subscribed to {symbol}");
                        subscribed.insert(symbol);
                    }
                    WsClientMsg::Unsubscribe { symbol } => {
                        debug!("client unsubscribed from {symbol}");
                        subscribed.remove(&symbol);
                    }
                    WsClientMsg::Ping => {
                        if sender.send(Message::Ping(Vec::new())).await.is_err() {
                            return;
                        }
                    }
                    WsClientMsg::Close => {
                        debug!("client requested close");
                        return;
                    }
                }
            }
            Ok(Message::Ping(bytes)) => {
                if sender.send(Message::Pong(bytes)).await.is_err() {
                    return;
                }
            }
            Ok(Message::Close(_)) | Err(_) => {
                debug!("WebSocket client closed connection");
                return;
            }
            _ => {}
        }
    }
}
