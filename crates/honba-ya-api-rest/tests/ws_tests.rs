use chrono::Utc;
use futures::{SinkExt, StreamExt};
use honba_api_rest::ws::{tick_to_ws, ws_handler, WsClientCommand, WsDirectMsg, WsTick};
use honba_api_rest::AppState;
use honba_core::{DepthLevel, Exchange, InstrumentId, Price, Quantity, Tick};
use honba_feed::{Dataset, DatasetReader};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[test]
fn test_tick_to_ws_ltp_and_change() {
    let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let t1 = Tick::Ltp {
        instrument: inst.clone(),
        price: Price::from_rupees("2500.00").unwrap(),
        volume: Quantity::from_u64(100),
        exchange_ts: Utc::now(),
        receive_ts: Utc::now(),
    };

    let ws1 = tick_to_ws(&t1, None);
    assert_eq!(ws1.symbol, "RELIANCE");
    assert_eq!(ws1.price, 2500.0);
    assert_eq!(ws1.change, 0.0);
    assert_eq!(ws1.change_percent, 0.0);
    assert_eq!(ws1.volume, Some(100.0));

    let t2 = Tick::Ltp {
        instrument: inst.clone(),
        price: Price::from_rupees("2525.00").unwrap(),
        volume: Quantity::from_u64(150),
        exchange_ts: Utc::now(),
        receive_ts: Utc::now(),
    };
    let ws2 = tick_to_ws(&t2, Some(ws1.price));
    assert_eq!(ws2.price, 2525.0);
    assert_eq!(ws2.change, 25.0);
    assert_eq!(ws2.change_percent, 1.0);
}

#[test]
fn test_tick_to_ws_quote_and_depth() {
    let inst = InstrumentId::new(Exchange::Nse, "TCS");
    let quote = Tick::Quote {
        instrument: inst.clone(),
        bid: Price::from_rupees("3500.00").unwrap(),
        ask: Price::from_rupees("3502.00").unwrap(),
        bid_qty: Quantity::from_u64(50),
        ask_qty: Quantity::from_u64(75),
        last_price: Some(Price::from_rupees("3501.00").unwrap()),
        exchange_ts: Utc::now(),
        receive_ts: Utc::now(),
        band: None,
    };
    let ws_q = tick_to_ws(&quote, None);
    assert_eq!(ws_q.symbol, "TCS");
    assert_eq!(ws_q.price, 3501.0);
    assert_eq!(ws_q.volume, Some(125.0));

    let depth = Tick::Depth {
        instrument: inst.clone(),
        bids: vec![DepthLevel {
            price: Price::from_rupees("3500.00").unwrap(),
            quantity: Quantity::from_u64(50),
            orders_count: Some(3),
        }],
        asks: vec![DepthLevel {
            price: Price::from_rupees("3502.00").unwrap(),
            quantity: Quantity::from_u64(75),
            orders_count: Some(5),
        }],
        last_price: None,
        exchange_ts: Utc::now(),
        receive_ts: Utc::now(),
    };
    let ws_d = tick_to_ws(&depth, None);
    assert_eq!(ws_d.symbol, "TCS");
    assert_eq!(ws_d.price, 3500.0);
}

#[test]
fn test_ws_client_command_deserialization() {
    let json_action = r#"{"action": "subscribe", "symbols": ["RELIANCE", "TCS"]}"#;
    let cmd: WsClientCommand = serde_json::from_str(json_action).unwrap();
    match cmd {
        WsClientCommand::Action { action, symbols, .. } => {
            assert_eq!(action, "subscribe");
            assert_eq!(symbols, Some(vec!["RELIANCE".to_string(), "TCS".to_string()]));
        }
        _ => panic!("Expected Action variant"),
    }

    let json_direct = r#"{"Subscribe": {"symbol": "INFY"}}"#;
    let cmd2: WsClientCommand = serde_json::from_str(json_direct).unwrap();
    match cmd2 {
        WsClientCommand::Direct(WsDirectMsg::Subscribe { symbol }) => {
            assert_eq!(symbol, "INFY");
        }
        _ => panic!("Expected Direct variant"),
    }
}

#[tokio::test]
async fn test_ws_zero_polling_broadcast_gateway_roundtrip() {
    let (broadcast_tx, _) = broadcast::channel(128);

    // Build AppState using DatasetReader::new
    let reader = DatasetReader::new(Dataset::new(), vec![]);
    let app_state = Arc::new(
        AppState::from_reader(reader).with_market_data_broadcast(broadcast_tx.clone()),
    );

    let router = axum::Router::new()
        .route("/ws", axum::routing::get(ws_handler))
        .route("/ws/market", axum::routing::get(ws_handler))
        .with_state(app_state);

    // Start ephemeral local server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    // Connect WebSocket client
    let url = format!("ws://{}/ws/market", addr);
    let (mut ws_stream, _) = connect_async(&url).await.expect("Failed to connect WS");

    // 1. Send Ping -> receive Pong
    let ping_msg = serde_json::json!({"action": "ping"}).to_string();
    ws_stream.send(Message::Text(ping_msg)).await.unwrap();

    let pong_resp = ws_stream.next().await.unwrap().unwrap();
    assert!(pong_resp.to_text().unwrap().contains("pong"));

    // 2. Subscribe to RELIANCE
    let sub_msg = serde_json::json!({
        "action": "subscribe",
        "symbols": ["RELIANCE"]
    })
    .to_string();
    ws_stream.send(Message::Text(sub_msg)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // 3. Emit INFY tick -> should be ignored (not subscribed)
    let infy_inst = InstrumentId::new(Exchange::Nse, "INFY");
    let infy_tick = Tick::Ltp {
        instrument: infy_inst,
        price: Price::from_rupees("1600.00").unwrap(),
        volume: Quantity::from_u64(10),
        exchange_ts: Utc::now(),
        receive_ts: Utc::now(),
    };
    broadcast_tx.send(infy_tick).unwrap();

    // 4. Emit RELIANCE tick -> should be streamed immediately to client
    let rel_inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let rel_tick = Tick::Ltp {
        instrument: rel_inst,
        price: Price::from_rupees("2550.00").unwrap(),
        volume: Quantity::from_u64(25),
        exchange_ts: Utc::now(),
        receive_ts: Utc::now(),
    };
    broadcast_tx.send(rel_tick).unwrap();

    // Wait for the tick message
    let msg = tokio::time::timeout(Duration::from_millis(300), ws_stream.next())
        .await
        .expect("Timeout waiting for WS tick")
        .unwrap()
        .unwrap();

    let text = msg.to_text().unwrap();
    let tick: WsTick = serde_json::from_str(text).expect("Valid WsTick JSON");
    assert_eq!(tick.symbol, "RELIANCE");
    assert_eq!(tick.price, 2550.0);
    assert_eq!(tick.volume, Some(25.0));

    // 5. Send close message
    let close_msg = serde_json::json!({"action": "close"}).to_string();
    let _ = ws_stream.send(Message::Text(close_msg)).await;
}
