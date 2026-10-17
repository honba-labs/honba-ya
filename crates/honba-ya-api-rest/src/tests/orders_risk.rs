//! Unit tests for the `POST /orders` risk gate (ADR 0018 decision 7, E2-S2 r5b).

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use honba_entities::{Currency, Instrument, InstrumentKind};
use honba_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use honba_messages::{Exchange, InstrumentId, UnixNanos};
use serde_json::{json, Value};
use tower::util::ServiceExt;

use crate::state::AppState;
use crate::{api_router, api_router_with};

fn instrument_id() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("NSE"))
}

/// A state whose master knows one instrument (lot 25, tick 0.05, INR equity)
/// with one bar closing at 100: quotes derive bid = ask = 100 from the bar.
fn state_with_x() -> AppState {
    let id = instrument_id();
    let spec = honba_api::parse_timeframe("1m").unwrap();
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    builder
        .push(UnixNanos::new(1), 100.0, 100.0, 100.0, 100.0, 1.0)
        .unwrap();
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    let instrument = Instrument::new(id, InstrumentKind::Equity, Currency::Inr, 25.0, 0.05);
    AppState::from_reader(DatasetReader::new(dataset, vec![instrument]))
}

async fn call(
    router: axum::Router,
    method: Method,
    uri: &str,
    body: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(text) => {
            builder = builder.header("content-type", "application/json");
            Body::from(text.to_owned())
        }
        None => Body::empty(),
    };
    let res = router.oneshot(builder.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("{uri}: non-JSON body {:?}", String::from_utf8_lossy(&bytes)));
    (status, parsed)
}

#[tokio::test]
async fn an_unknown_instrument_is_a_422_carrying_risk_instrument_unknown() {
    let (status, parsed) = call(
        api_router(),
        Method::POST,
        "/orders",
        Some(
            &json!({"instrument_id": {"symbol": "Z", "exchange": "NSE"},
                    "side": "buy", "qty": 25.0})
            .to_string(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(parsed["error"]["code"], json!("risk_instrument_unknown"));
}

#[tokio::test]
async fn an_approved_order_returns_its_ledger_row() {
    let (status, parsed) = call(
        api_router_with(state_with_x()),
        Method::POST,
        "/orders",
        Some(
            &json!({"instrument_id": {"symbol": "X", "exchange": "NSE"},
                    "side": "buy", "qty": 25.0, "price": 100.0,
                    "client_order_id": "approved-order"})
            .to_string(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(parsed["data"]["order_id"], json!("approved-order"));
    assert_eq!(parsed["data"]["status"], json!("submitted"));
}

#[tokio::test]
async fn cancel_and_close_bypass_the_gate() {
    let (status, parsed) = call(api_router(), Method::DELETE, "/orders/o1", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(parsed["data"]["status"], json!("cancelled"));
    let (status, parsed) = call(api_router(), Method::POST, "/positions/close", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(parsed["data"]["status"], json!("closing"));
}
