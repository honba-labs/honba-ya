//! Routes whose behaviour is not built yet answer 501 with the standard
//! envelope (`not_implemented`), never a fake success.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::util::ServiceExt;

async fn call(method: &str, uri: &str, body: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(text) => {
            req = req.header("content-type", "application/json");
            Body::from(text.to_owned())
        }
        None => Body::empty(),
    };
    let res = honba_api_rest::api_router()
        .oneshot(req.body(body).unwrap())
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

#[tokio::test]
async fn unbuilt_routes_answer_501_not_implemented_envelopes() {
    // ADR 0017 q2c: the backtest and journal routes are built and left this list on purpose;
    // the sweep routes are now built (E4-S5). ADR 0018 decision 7 (E2-S2 r5b): POST /orders,
    // DELETE /orders/:id and POST /positions/close are risk-gated; E11-S7 built the write
    // ledger, so GET /orders left the list as well and no placeholder route remains.
    let routes: [(&str, &str, Option<&str>); 0] = [];
    for (method, uri, body) in routes {
        let (status, parsed) = call(method, uri, body).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{method} {uri}");
        assert_eq!(parsed["error"]["code"], json!("not_implemented"), "{uri}");
        assert!(parsed["data"].is_null(), "{uri}: {parsed}");
        assert!(parsed["api_version"].is_string(), "{uri}");
    }
}
