//! Unit tests for the router fallback: an unknown route is a `not_found` envelope.

use axum::http::StatusCode;
use serde_json::Value;
use tower::util::ServiceExt;

#[tokio::test]
async fn an_unknown_route_is_a_not_found_envelope() {
    let res = crate::api_router()
        .oneshot(
            axum::http::Request::builder()
                .uri("/no/such/route")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).expect("a JSON envelope body");
    assert_eq!(body["error"]["code"], "not_found");
    assert!(body["error"]["message"].is_string());
}

#[tokio::test]
async fn an_unsupported_method_is_a_405_envelope() {
    let res = crate::api_router()
        .oneshot(
            axum::http::Request::builder()
                .method("PUT")
                .uri("/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED);
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).expect("a JSON envelope body");
    assert_eq!(body["error"]["code"], "unsupported");
}
