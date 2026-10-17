//! The scope map only rots when a route is added without one (E5-S3, ADR 0023 §2).
//!
//! `drift_test_fails_on_new_route` proves the drift guard bites: the registry is
//! fully scoped, and a route that is not makes `unscoped_routes` non-empty.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use honba_api::ENDPOINTS;
use honba_api_rest::{api_router, unscoped_routes, ROUTE_SCOPES};
use tower::util::ServiceExt;

#[test]
fn drift_test_fails_on_new_route() {
    // Every endpoint in the registry already has a scope row: no drift today.
    assert!(
        unscoped_routes(ENDPOINTS).is_empty(),
        "the registry has an unscoped endpoint: {:?}",
        unscoped_routes(ENDPOINTS)
    );

    // Add a route that is not in `ROUTE_SCOPES`: the guard must report it, so a
    // new endpoint cannot ship as an accidental public route.
    let mut routes = ENDPOINTS.to_vec();
    routes.push(("GET", "/brand/new"));
    assert_eq!(unscoped_routes(&routes), vec!["GET /brand/new".to_owned()]);
}

/// Every scoped route is a route the router actually serves (no phantom rows).
///
/// A missing route or a wrong method answers the `unknown_route` /
/// `unsupported_method` fallback; a handler's own 404 (e.g. an unknown
/// instrument) carries a different message, so it does not look like the
/// fallback.
#[tokio::test]
async fn every_scoped_route_is_registered_on_the_router() {
    let router = api_router();
    for (method, path, _) in ROUTE_SCOPES {
        let uri = path.replace("{id}", "X.NSE").replace("{name}", "main");
        let res = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(*method)
                    .uri(&uri)
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = res.status();
        assert_ne!(
            status,
            StatusCode::METHOD_NOT_ALLOWED,
            "{method} {path} has a scope row but the router does not allow the method"
        );
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        if status == StatusCode::NOT_FOUND {
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
            let message = body
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or_default();
            assert!(
                !message.starts_with("no such route"),
                "{method} {path} has a scope row but the router does not serve it"
            );
        }
    }
}
