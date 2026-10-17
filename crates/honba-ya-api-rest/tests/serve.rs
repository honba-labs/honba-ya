//! The router served over a real loopback socket: bound to an ephemeral port in
//! process, called with a plain TCP client, stopped through the shutdown future.
//! No external network and no wall clock.

use honba_api_rest::{serve, AppState};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

async fn http_get(addr: std::net::SocketAddr, path: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.0\r\nHost: {addr}\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8(raw).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

#[tokio::test]
async fn the_router_answers_over_a_socket_and_stops_when_told() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, AppState::default(), async move {
        let _ = stopped.await;
    }));

    let (status, body) = http_get(addr, "/health").await;
    assert_eq!(status, 200);
    assert_eq!(body["data"]["status"], "ok");
    let (status, body) = http_get(addr, "/instruments").await;
    assert_eq!(status, 200);
    assert_eq!(body["data"]["instruments"], serde_json::json!([]));

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
    assert!(
        TcpStream::connect(addr).await.is_err(),
        "listener is closed"
    );
}
