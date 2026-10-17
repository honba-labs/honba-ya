//! `POST /strategies/verify` through the real router: a manifest in, the
//! verified IR (or a 422 with a stable error code) out.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::util::ServiceExt;

fn manifest(instruments: Value) -> Value {
    let tcs = json!({"symbol": "TCS", "exchange": "NSE"});
    json!({
        "api_version": "1.0.0",
        "name": "sma",
        "source_hash": "sha256:abc",
        "universe": {"explicit": [tcs]},
        "subscriptions": {"instruments": instruments},
        "driving_timeframe": {"interval": 1, "aggregation": "day"},
        "warmup_bars": 20
    })
}

async fn post(body: Value) -> (StatusCode, Value) {
    let res = honba_api_rest::api_router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/strategies/verify")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn a_valid_manifest_returns_its_ir() {
    let (status, body) = post(manifest(json!([{"symbol": "TCS", "exchange": "NSE"}]))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["warmup_bars"], json!(20));
    assert_eq!(body["data"]["manifest"]["name"], json!("sma"));
    assert!(body["error"].is_null());
}

#[tokio::test]
async fn an_unverifiable_manifest_is_a_422_with_a_stable_code() {
    let (status, body) = post(manifest(json!([]))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("validation_invalid_request"));
    assert_eq!(
        body["error"]["context"]["reason"],
        json!("no_subscriptions")
    );
    assert!(body["data"].is_null());
}
