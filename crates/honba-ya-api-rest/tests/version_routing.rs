//! Tests for Phase 1 REST API Dual Routing (`honba-ya-api-rest`).
//!
//! Validates:
//! - Multi-version route tree (/api/v1 and /api/v2)
//! - Legacy fallback with RFC 8594 deprecation and canonical link headers
//! - Content negotiation and header-based version dispatch
//! - OpenAPI endpoints and Swagger UI version selector

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use honba_api_rest::api_router;
use serde_json::Value;
use tower::util::ServiceExt;

async fn call_req(req: Request<Body>) -> (StatusCode, axum::http::HeaderMap, Value) {
    let res = api_router().oneshot(req).await.unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, headers, body)
}

#[tokio::test]
async fn v1_prefixed_routes_return_v1_envelope_and_version_header() {
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/capabilities")
        .body(Body::empty())
        .unwrap();
    let (status, headers, body) = call_req(req).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-honba-version").unwrap(), "1");
    assert!(headers.get("deprecation").is_none());
    assert_eq!(body["api_version"], "v1");
    assert!(body["data"]["capabilities"]["endpoints"].is_array());
}

#[tokio::test]
async fn v2_prefixed_routes_return_v2_envelope_and_version_header() {
    let req = Request::builder()
        .method("GET")
        .uri("/api/v2/capabilities")
        .body(Body::empty())
        .unwrap();
    let (status, headers, body) = call_req(req).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-honba-version").unwrap(), "2");
    assert!(headers.get("deprecation").is_none());
    assert_eq!(body["api_version"], "v2");
    assert!(body["data"]["capabilities"]["endpoints"].is_array());
}

#[tokio::test]
async fn legacy_unprefixed_routes_return_deprecation_and_canonical_link_headers() {
    let req = Request::builder()
        .method("GET")
        .uri("/capabilities")
        .body(Body::empty())
        .unwrap();
    let (status, headers, body) = call_req(req).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("deprecation").unwrap(), "true");
    assert_eq!(headers.get("x-honba-version").unwrap(), "1");
    assert_eq!(
        headers.get("link").unwrap(),
        "</api/v1/capabilities>; rel=\"canonical\""
    );
    assert_eq!(body["api_version"], "v1");

    let req_quotes = Request::builder()
        .method("GET")
        .uri("/instruments")
        .body(Body::empty())
        .unwrap();
    let (status_q, headers_q, body_q) = call_req(req_quotes).await;
    assert_eq!(status_q, StatusCode::OK);
    assert_eq!(headers_q.get("deprecation").unwrap(), "true");
    assert_eq!(
        headers_q.get("link").unwrap(),
        "</api/v1/instruments>; rel=\"canonical\""
    );
    assert_eq!(body_q["api_version"], "v1");
}

#[tokio::test]
async fn header_negotiation_dispatches_to_v2() {
    // Via X-Honba-Version: 2
    let req1 = Request::builder()
        .method("GET")
        .uri("/capabilities")
        .header("x-honba-version", "2")
        .body(Body::empty())
        .unwrap();
    let (status1, headers1, body1) = call_req(req1).await;
    assert_eq!(status1, StatusCode::OK);
    assert_eq!(headers1.get("x-honba-version").unwrap(), "2");
    assert!(headers1.get("deprecation").is_none());
    assert_eq!(
        headers1.get("link").unwrap(),
        "</api/v2/capabilities>; rel=\"canonical\""
    );
    assert_eq!(body1["api_version"], "v2");

    // Via Accept: application/vnd.honba.v2+json
    let req2 = Request::builder()
        .method("GET")
        .uri("/instruments")
        .header(header::ACCEPT, "application/vnd.honba.v2+json")
        .body(Body::empty())
        .unwrap();
    let (status2, headers2, body2) = call_req(req2).await;
    assert_eq!(status2, StatusCode::OK);
    assert_eq!(headers2.get("x-honba-version").unwrap(), "2");
    assert!(headers2.get("deprecation").is_none());
    assert_eq!(
        headers2.get("link").unwrap(),
        "</api/v2/instruments>; rel=\"canonical\""
    );
    assert_eq!(body2["api_version"], "v2");
}

#[tokio::test]
async fn openapi_endpoints_serve_versioned_schemas_and_index() {
    // GET /schema/openapi/v1.json
    let req_v1 = Request::builder()
        .method("GET")
        .uri("/schema/openapi/v1.json")
        .body(Body::empty())
        .unwrap();
    let (status_v1, headers_v1, body_v1) = call_req(req_v1).await;
    assert_eq!(status_v1, StatusCode::OK);
    assert_eq!(headers_v1.get(header::CONTENT_TYPE).unwrap(), "application/json");
    assert_eq!(body_v1["openapi"], "3.1.0");
    assert_eq!(body_v1["info"]["version"], "1.0.0");
    assert_eq!(body_v1["servers"][0]["url"], "/api/v1");

    // GET /schema/openapi/v2.json
    let req_v2 = Request::builder()
        .method("GET")
        .uri("/schema/openapi/v2.json")
        .body(Body::empty())
        .unwrap();
    let (status_v2, headers_v2, body_v2) = call_req(req_v2).await;
    assert_eq!(status_v2, StatusCode::OK);
    assert_eq!(headers_v2.get(header::CONTENT_TYPE).unwrap(), "application/json");
    assert_eq!(body_v2["openapi"], "3.1.0");
    assert_eq!(body_v2["info"]["version"], "2.0.0");
    assert_eq!(body_v2["servers"][0]["url"], "/api/v2");

    // GET /schema/openapi (index)
    let req_idx = Request::builder()
        .method("GET")
        .uri("/schema/openapi")
        .body(Body::empty())
        .unwrap();
    let (status_idx, _, body_idx) = call_req(req_idx).await;
    assert_eq!(status_idx, StatusCode::OK);
    assert_eq!(body_idx["latest"], "v2");
    assert!(body_idx["openapi_versions"].is_array());
}

#[tokio::test]
async fn swagger_ui_serves_html_with_multi_version_selector() {
    let req = Request::builder()
        .method("GET")
        .uri("/docs")
        .body(Body::empty())
        .unwrap();
    let res = api_router().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(html.contains("SwaggerUIBundle"));
    assert!(html.contains("/schema/openapi/v1.json"));
    assert!(html.contains("/schema/openapi/v2.json"));
    assert!(html.contains("v2 (Latest & Recommended)"));

    // Also /swagger-ui
    let req2 = Request::builder()
        .method("GET")
        .uri("/swagger-ui")
        .body(Body::empty())
        .unwrap();
    let res2 = api_router().oneshot(req2).await.unwrap();
    assert_eq!(res2.status(), StatusCode::OK);
}
