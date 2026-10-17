use axum::body::Body;
use axum::http::{header, Request};
use tower::util::ServiceExt;

use crate::{api_router, api_router_with_config, ApiConfig, AppState};

#[test]
fn the_default_config_allows_no_origin() {
    assert!(ApiConfig::default().cors_origins().is_empty());
}

#[test]
fn an_origin_that_is_not_a_header_value_is_rejected() {
    assert!(ApiConfig::default()
        .with_cors_origins(["https://ok.example", "bad\norigin"])
        .is_err());
}

fn health(origin: &str) -> Request<Body> {
    Request::builder()
        .uri("/health")
        .header(header::ORIGIN, origin)
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn the_default_router_sends_no_cors_headers() {
    let res = api_router()
        .oneshot(health("https://evil.example"))
        .await
        .unwrap();
    assert!(res
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .is_none());
}

#[tokio::test]
async fn only_listed_origins_get_cors_headers() {
    let config = ApiConfig::default()
        .with_cors_origins(["https://app.example"])
        .unwrap();
    let app = || api_router_with_config(AppState::default(), &config);
    let res = app().oneshot(health("https://app.example")).await.unwrap();
    assert_eq!(
        res.headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .unwrap(),
        "https://app.example"
    );
    let res = app().oneshot(health("https://evil.example")).await.unwrap();
    assert!(res
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .is_none());
}
