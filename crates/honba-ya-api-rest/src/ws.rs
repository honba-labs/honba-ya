//! Zero-polling WebSocket gateway for live market data (`GET /ws`, `GET /ws/market`).
//!
//! Subscribes directly to `MarketDataActor`'s broadcast channel via [`crate::AppState::subscribe_market_data`].
//! Ticks, quotes, and market depth stream to clients with zero polling latency.
//!
//! Handles bidirectional client messaging:
//! - Subscriptions: `{"action": "subscribe", "symbols": ["RELIANCE", "TCS"]}` or `{"symbol": "INFY"}`
//! - Unsubscriptions: `{"action": "unsubscribe", "symbols": ["TCS"]}`
//! - Liveness: `{"action": "ping"}` or WebSocket standard `Ping` -> responds with `Pong`
//! - Disconnect: `{"action": "close"}` or standard WebSocket `Close`

use std::collections::{HashMap, HashSet};
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
use honba_core::rust_decimal::prelude::ToPrimitive;
use honba_core::Tick;
use serde::{Deserialize, Serialize};

use crate::AppState;

/// Market tick payload streamed to WebSocket clients.
/// Matches the expected contract in `honba-fe/apps/web/src/core/data-layer.ts`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsTick {
    /// Instrument symbol, e.g. "RELIANCE".
    pub symbol: String,
    /// Last traded or mid price.
    pub price: f64,
    /// Absolute change from previous price.
    pub change: f64,
    /// Percentage change from previous price.
    #[serde(rename = "changePercent")]
    pub change_percent: f64,
    /// Traded volume or total bid/ask size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<f64>,
    /// Milliseconds since Unix epoch.
    pub timestamp: i64,
}

/// Market depth level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsDepthLevel {
    /// Depth level price.
    pub price: f64,
    /// Depth level quantity.
    pub quantity: f64,
    /// Number of orders at this level.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orders: Option<u32>,
}

/// Market depth payload streamed to WebSocket clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsDepth {
    /// Instrument symbol, e.g. "RELIANCE".
    pub symbol: String,
    /// Top bid price levels.
    pub bids: Vec<WsDepthLevel>,
    /// Top ask price levels.
    pub asks: Vec<WsDepthLevel>,
    /// Milliseconds since Unix epoch.
    pub timestamp: i64,
}

/// Incoming client command payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WsClientCommand {
    /// Tagged command with action field, e.g. `{"action": "subscribe", "symbols": ["RELIANCE"]}`.
    Action {
        /// Requested action string.
        action: String,
        /// Single target symbol if specified.
        #[serde(default)]
        symbol: Option<String>,
        /// Multiple target symbols if specified.
        #[serde(default)]
        symbols: Option<Vec<String>>,
    },
    /// Direct command variant e.g. `{"Subscribe": {"symbol": "RELIANCE"}}`.
    Direct(WsDirectMsg),
}

/// Direct client message format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WsDirectMsg {
    /// Subscribe to tick updates for an instrument.
    Subscribe {
        /// Instrument symbol to subscribe to.
        symbol: String,
    },
    /// Unsubscribe from tick updates for an instrument.
    Unsubscribe {
        /// Instrument symbol to unsubscribe from.
        symbol: String,
    },
    /// Ping message for connection liveness.
    Ping,
    /// Close connection.
    Close,
}

/// Upgrades an HTTP connection to a WebSocket for live market data.
pub async fn ws_handler(ws: WebSocketUpgrade, State(app_state): State<Arc<AppState>>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, app_state))
}

/// Convert canonical `honba_core::Tick` into frontend-compatible `WsTick`.
pub fn tick_to_ws(tick: &Tick, prev_price: Option<f64>) -> WsTick {
    let (symbol, price, volume, ts) = match tick {
        Tick::Ltp {
            instrument,
            price,
            volume,
            exchange_ts,
            ..
        } => (
            instrument.symbol.clone(),
            price.0.to_f64().unwrap_or(0.0),
            volume.0.to_f64(),
            exchange_ts.timestamp_millis(),
        ),
        Tick::Quote {
            instrument,
            bid,
            ask,
            last_price,
            bid_qty,
            ask_qty,
            exchange_ts,
            ..
        } => {
            let p = last_price
                .map(|lp| lp.0.to_f64().unwrap_or(0.0))
                .unwrap_or_else(|| (bid.0.to_f64().unwrap_or(0.0) + ask.0.to_f64().unwrap_or(0.0)) / 2.0);
            let v = (bid_qty.0 + ask_qty.0).to_f64().unwrap_or(0.0);
            (instrument.symbol.clone(), p, Some(v), exchange_ts.timestamp_millis())
        }
        Tick::Depth {
            instrument,
            bids,
            last_price,
            exchange_ts,
            ..
        } => {
            let p = last_price
                .map(|lp| lp.0.to_f64().unwrap_or(0.0))
                .unwrap_or_else(|| bids.first().map(|b| b.price.0.to_f64().unwrap_or(0.0)).unwrap_or(0.0));
            (instrument.symbol.clone(), p, None, exchange_ts.timestamp_millis())
        }
    };

    let (change, change_percent) = match prev_price {
        Some(prev) if prev > 0.0 => {
            let diff = price - prev;
            (diff, (diff / prev) * 100.0)
        }
        _ => (0.0, 0.0),
    };

    WsTick {
        symbol,
        price,
        change,
        change_percent,
        volume,
        timestamp: ts,
    }
}

/// Handles one WebSocket connection using zero-polling broadcast event stream.
async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut subscribed: HashSet<String> = HashSet::new();
    let mut previous_prices: HashMap<String, f64> = HashMap::new();

    // Subscribe directly to the market data actor broadcast channel
    let mut broadcast_rx = state.subscribe_market_data();

    loop {
        tokio::select! {
            biased;

            // Handle client messages (subscriptions, pings, disconnects)
            client_msg = receiver.next() => {
                let Some(msg) = client_msg else {
                    debug!("WebSocket client disconnected");
                    break;
                };

                match msg {
                    Ok(Message::Text(text)) => {
                        if let Ok(cmd) = serde_json::from_str::<WsClientCommand>(&text) {
                            match cmd {
                                WsClientCommand::Action { action, symbol, symbols } => {
                                    match action.to_lowercase().as_str() {
                                        "subscribe" => {
                                            if let Some(s) = symbol {
                                                debug!("Client subscribed to {}", s);
                                                subscribed.insert(s);
                                            }
                                            if let Some(syms) = symbols {
                                                for s in syms {
                                                    debug!("Client subscribed to {}", s);
                                                    subscribed.insert(s);
                                                }
                                            }
                                        }
                                        "unsubscribe" => {
                                            if let Some(s) = symbol {
                                                subscribed.remove(&s);
                                            }
                                            if let Some(syms) = symbols {
                                                for s in syms {
                                                    subscribed.remove(&s);
                                                }
                                            }
                                        }
                                        "ping" => {
                                            let pong = serde_json::json!({"action": "pong", "timestamp": chrono::Utc::now().timestamp_millis()});
                                            if sender.send(Message::Text(pong.to_string())).await.is_err() {
                                                return;
                                            }
                                        }
                                        "close" => {
                                            return;
                                        }
                                        _ => {}
                                    }
                                }
                                WsClientCommand::Direct(direct) => {
                                    match direct {
                                        WsDirectMsg::Subscribe { symbol } => {
                                            subscribed.insert(symbol);
                                        }
                                        WsDirectMsg::Unsubscribe { symbol } => {
                                            subscribed.remove(&symbol);
                                        }
                                        WsDirectMsg::Ping => {
                                            if sender.send(Message::Pong(Vec::new())).await.is_err() {
                                                return;
                                            }
                                        }
                                        WsDirectMsg::Close => {
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(Message::Ping(bytes)) => {
                        if sender.send(Message::Pong(bytes)).await.is_err() {
                            return;
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => {
                        debug!("WebSocket connection closed");
                        return;
                    }
                    _ => {}
                }
            }

            // Zero-polling event stream from MarketDataActor
            broadcast_msg = async {
                match broadcast_rx.as_mut() {
                    Some(rx) => rx.recv().await.ok(),
                    None => std::future::pending().await,
                }
            } => {
                if let Some(tick) = broadcast_msg {
                    let symbol = tick.instrument().symbol.clone();
                    // If no explicit symbols subscribed, or if instrument is in subscriptions
                    if subscribed.is_empty() || subscribed.contains(&symbol) {
                        let prev = previous_prices.get(&symbol).copied();
                        let ws_tick = tick_to_ws(&tick, prev);
                        previous_prices.insert(symbol, ws_tick.price);

                        if let Ok(json) = serde_json::to_string(&ws_tick) {
                            if sender.send(Message::Text(json)).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}
