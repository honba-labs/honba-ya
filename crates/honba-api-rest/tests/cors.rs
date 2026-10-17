//! CORS is off unless origins are allow-listed, over the real router including preflight.

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use honba_api_rest::{api_router_with, api_router_with_config, ApiConfig, AppState};
use tower::util::ServiceExt;

fn preflight(origin: &str) -> Request<Body> {
    Request::builder()
        .method(Method::OPTIONS)
        .uri("/instruments")
        .header(header::ORIGIN, origin)
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn preflight_is_refused_without_an_allow_list() {
    let res = api_router_with(AppState::default())
        .oneshot(preflight("https://app.example"))
        .await
        .unwrap();
    assert!(res
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .is_none());
    assert!(res
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_METHODS)
        .is_none());
}

#[tokio::test]
async fn preflight_is_answered_for_an_allow_listed_origin_only() {
    let config = ApiConfig::default()
        .with_cors_origins(["https://app.example"])
        .unwrap();
    let app = || api_router_with_config(AppState::default(), &config);
    let res = app()
        .oneshot(preflight("https://app.example"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .unwrap(),
        "https://app.example"
    );
    let res = app()
        .oneshot(preflight("https://evil.example"))
        .await
        .unwrap();
    assert!(res
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .is_none());
}
