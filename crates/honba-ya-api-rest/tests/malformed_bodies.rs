//! Every POST route answers a body that does not parse with the standard
//! error envelope (`validation_invalid_request`), never axum's plain text.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::util::ServiceExt;

const POST_ROUTES: [&str; 5] = [
    "/strategies",
    "/strategies/verify",
    "/backtests",
    "/sweeps",
    "/orders",
];

async fn post_raw(uri: &str, content_type: Option<&str>, body: &str) -> (StatusCode, Value) {
    let mut req = Request::builder().method("POST").uri(uri);
    if let Some(ct) = content_type {
        req = req.header("content-type", ct);
    }
    let res = honba_api_rest::api_router()
        .oneshot(req.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("{uri}: non-JSON body {:?}", String::from_utf8_lossy(&bytes)));
    (status, parsed)
}

fn assert_envelope(status: StatusCode, body: &Value, uri: &str) {
    assert!(status.is_client_error(), "{uri}: {status}");
    assert_eq!(
        body["error"]["code"],
        json!("validation_invalid_request"),
        "{uri}: {body}"
    );
    assert!(body["data"].is_null(), "{uri}: {body}");
    assert!(body["api_version"].is_string(), "{uri}: {body}");
}

#[tokio::test]
async fn malformed_json_returns_the_error_envelope_on_every_post_route() {
    for uri in POST_ROUTES {
        let (status, body) = post_raw(uri, Some("application/json"), "{not json").await;
        assert_envelope(status, &body, uri);
    }
}

#[tokio::test]
async fn an_unknown_field_returns_the_error_envelope() {
    let (status, body) = post_raw(
        "/strategies/verify",
        Some("application/json"),
        r#"{"bogus_field": 1}"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_envelope(status, &body, "/strategies/verify");
}

#[tokio::test]
async fn a_missing_content_type_returns_the_error_envelope() {
    let (status, body) = post_raw("/backtests", None, "{}").await;
    assert_envelope(status, &body, "/backtests");
}
