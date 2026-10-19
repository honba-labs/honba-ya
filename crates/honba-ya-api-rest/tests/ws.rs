//! Integration tests for the /ws and /ws/market WebSocket endpoints.

use honba_api_rest::{AppState, serve};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

async fn ws_handshake(addr: std::net::SocketAddr, path: &str) -> u16 {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    let status_line = response.lines().next().expect("response status line");
    status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status")
}

#[tokio::test]
async fn ws_endpoint_responds_with_101_switching_protocols() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, AppState::default(), async move {
        let _ = stopped.await;
    }));

    let status = ws_handshake(addr, "/ws").await;
    assert_eq!(status, 101);

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn ws_market_endpoint_responds_with_101_switching_protocols() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, AppState::default(), async move {
        let _ = stopped.await;
    }));

    let status = ws_handshake(addr, "/ws/market").await;
    assert_eq!(status, 101);

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn ws_streams_ticks_to_client() {
    use futures::StreamExt;
    use honba_api_rest::ws::WsTick;
    use honba_entities::{Currency, Instrument, InstrumentKind};
    use honba_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
    use honba_messages::{Exchange, InstrumentId, UnixNanos};

    let id = InstrumentId::new("RELIANCE", Exchange::new("NSE"));
    let spec = honba_api::parse_timeframe("1m").unwrap();
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    builder
        .push(UnixNanos::new(1), 2500.0, 2500.0, 2500.0, 2500.0, 100.0)
        .unwrap();
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    let instrument = Instrument::new(id, InstrumentKind::Equity, Currency::Inr, 1.0, 0.05);
    let state = AppState::from_reader(DatasetReader::new(dataset, vec![instrument]));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, state, async move {
        let _ = stopped.await;
    }));

    let ws_url = format!("ws://{addr}/ws/market");
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();

    // The server should send the tick immediately on connection
    let msg = ws_stream.next().await.unwrap().unwrap();
    let text = msg.to_text().unwrap();
    let tick: WsTick = serde_json::from_str(text).unwrap();

    assert_eq!(tick.symbol, "RELIANCE");
    assert!((tick.price - 2500.0).abs() < f64::EPSILON);

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

