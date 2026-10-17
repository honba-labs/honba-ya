use axum::http::StatusCode;
use tower::util::ServiceExt;

#[tokio::test]
async fn health_endpoint() {
    let app = crate::api_router();
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}
