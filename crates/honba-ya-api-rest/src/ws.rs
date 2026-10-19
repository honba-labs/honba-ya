//! WebSocket streaming for live market data (ticks, quotes).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::AppState;

/// Market tick payload streamed to WebSocket clients.
///
/// Matches the expected shape in `honba-fe/apps/web/src/core/data-layer.ts`.
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

/// Upgrades an HTTP connection to a WebSocket for live market data.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut interval = tokio::time::interval(Duration::from_millis(1000));
    let mut prev_prices = HashMap::<String, f64>::new();
    let mut subscribed = HashSet::<String>::new();

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Ok(instruments) = state.instruments.list_instruments().await {
                    for inst in instruments {
                        let id = inst.id();
                        let symbol = id.symbol().to_string();
                        if !subscribed.is_empty() && !subscribed.contains(&symbol) {
                            continue;
                        }

                        if let Ok(Some(quote)) = state.quotes.read_quote(id, None).await {
                            let price = quote.mid_price();
                            if !price.is_finite() {
                                continue;
                            }
                            let prev = prev_prices.get(&symbol).copied().unwrap_or(price);
                            let change = price - prev;
                            let change_percent = if prev > 0.0 { (change / prev) * 100.0 } else { 0.0 };
                            prev_prices.insert(symbol.clone(), price);

                            let now_ms = chrono::Utc::now().timestamp_millis();
                            let vol = quote.bid_size() + quote.ask_size();
                            let tick = WsTick {
                                symbol,
                                price,
                                change,
                                change_percent,
                                volume: if vol > 0.0 { Some(vol) } else { None },
                                timestamp: now_ms,
                            };

                            if let Ok(json_str) = serde_json::to_string(&tick) {
                                if sender.send(Message::Text(json_str)).await.is_err() {
                                    debug!("WebSocket client disconnected during send");
                                    return;
                                }
                            }
                        }
                    }
                }
            }
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(arr) = parsed.get("symbols").and_then(|s| s.as_array()) {
                                for s in arr {
                                    if let Some(sym) = s.as_str() {
                                        subscribed.insert(sym.to_string());
                                    }
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        if sender.send(Message::Pong(bytes)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                        debug!("WebSocket client closed connection");
                        return;
                    }
                    _ => {}
                }
            }
        }
    }
}
