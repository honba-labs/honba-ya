//! The run routes on a state without a journals root (the default, and `honba._honba`
//! before a `journals_dir` is given): reads are honest 404s, writes are validated first.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::util::ServiceExt;

use crate::api_router;

async fn call(method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(v) => {
            req = req.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let res = api_router().oneshot(req.body(body).unwrap()).await.unwrap();
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
async fn reads_without_a_journals_root_are_404() {
    for uri in [
        "/backtests/01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "/backtests/01ARZ3NDEKTSV4RRFFQ69G5FAV/journal",
        "/journals/01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "/backtests/..%2Fetc",
        "/journals/x",
    ] {
        let (status, body) = call("GET", uri, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(body["error"]["code"], "not_found", "{uri}");
    }
}

#[tokio::test]
async fn a_submit_is_validated_before_the_missing_root_is_reported() {
    let (status, body) = call("POST", "/backtests", Some(json!({}))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["context"]["field"], "seed");
}

#[tokio::test]
async fn a_valid_submit_without_a_journals_root_is_unsupported_not_a_fake_success() {
    let request = json!({
        "strategy": "sma_crossover", "universe": "TCS.NSE", "start": "2024-01-01",
        "end": "2024-06-01", "seed": 1
    });
    let (status, body) = call("POST", "/backtests", Some(request)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"]["code"], "unsupported");
    assert_eq!(body["error"]["context"]["reason"], "no_journals_dir");
    assert!(body["data"].is_null());
}
