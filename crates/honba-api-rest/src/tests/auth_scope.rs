//! The route-to-scope map covers every registry endpoint (E5-S3, ADR 0023 §2).

use honba_api::ENDPOINTS;

use crate::{scope_for, unscoped_routes, RouteScope, ROUTE_SCOPES};

/// The scopes ADR 0023 §2 defines; no route may name any other.
const SCOPES: &[&str] = &[
    "read:market",
    "read:account",
    "write:orders",
    "research",
    "operator",
];

#[test]
fn scope_for_every_endpoint() {
    let missing = unscoped_routes(ENDPOINTS);
    assert!(
        missing.is_empty(),
        "endpoints with no scope row (would be public): {missing:?}"
    );

    for (method, path) in ENDPOINTS {
        let scope = scope_for(method, path)
            .unwrap_or_else(|| panic!("{method} {path} has no route-to-scope row"));
        if let RouteScope::Required(name) = scope {
            assert!(
                SCOPES.contains(&name),
                "{method} {path} names unknown scope {name:?}"
            );
        }
    }
}

#[test]
fn route_scopes_have_no_duplicate_keys() {
    let mut seen = std::collections::BTreeSet::new();
    for (method, path, _) in ROUTE_SCOPES {
        assert!(
            seen.insert((*method, *path)),
            "duplicate scope row for {method} {path}"
        );
    }
}

#[test]
fn known_route_needs_its_scope_and_unknown_is_unmapped() {
    assert_eq!(
        scope_for("GET", "/instruments"),
        Some(RouteScope::Required("read:market"))
    );
    assert_eq!(
        scope_for("GET", "/bars/ABC"),
        Some(RouteScope::Required("read:market"))
    );
    assert_eq!(
        scope_for("POST", "/orders"),
        Some(RouteScope::Required("write:orders"))
    );
    assert_eq!(scope_for("GET", "/capabilities"), Some(RouteScope::Public));
    assert_eq!(scope_for("GET", "/brand/new"), None);
}

#[test]
fn trading_state_routes_need_the_operator_scope() {
    for method in ["GET", "PUT"] {
        assert_eq!(
            scope_for(method, "/trading-state"),
            Some(RouteScope::Required("operator"))
        );
    }
}

#[test]
fn profile_routes_need_any_valid_token_and_no_scope() {
    for (method, path) in [
        ("GET", "/profile"),
        ("PUT", "/profile"),
        ("GET", "/profile/layouts"),
        ("PUT", "/profile/layouts/main"),
        ("DELETE", "/profile/layouts/main"),
        ("GET", "/profile/modules"),
        ("PUT", "/profile/modules/m1"),
        ("DELETE", "/profile/modules/m1"),
    ] {
        assert_eq!(
            scope_for(method, path),
            Some(RouteScope::Authenticated),
            "{method} {path}"
        );
    }
}

#[test]
fn head_is_scoped_like_get() {
    for path in ["/orders", "/profile", "/instruments", "/trading-state"] {
        assert_eq!(scope_for("HEAD", path), scope_for("GET", path), "{path}");
        assert!(matches!(
            scope_for("HEAD", path),
            Some(RouteScope::Required(_) | RouteScope::Authenticated)
        ));
    }
    // HEAD is not a way into a route that has no GET row.
    assert_eq!(scope_for("HEAD", "/sweeps"), None);
}

#[tokio::test]
async fn profile_caller_refuses_a_missing_token_when_auth_is_on() {
    use axum::extract::FromRequestParts;

    use crate::auth::{AuthEnforced, TokenInfo};
    use crate::profile::Caller;

    let parts = |setup: &dyn Fn(&mut axum::http::Extensions)| {
        let (mut parts, ()) = axum::http::Request::new(()).into_parts();
        setup(&mut parts.extensions);
        parts
    };
    // Auth off: the default user.
    let mut off = parts(&|_| {});
    assert_eq!(
        Caller::from_request_parts(&mut off, &())
            .await
            .ok()
            .unwrap()
            .0,
        "default"
    );
    // Auth on, token present: its own id.
    let mut on = parts(&|e| {
        e.insert(AuthEnforced);
        e.insert(TokenInfo {
            token_id: "alice".into(),
            scopes: vec![],
        });
    });
    assert_eq!(
        Caller::from_request_parts(&mut on, &())
            .await
            .ok()
            .unwrap()
            .0,
        "alice"
    );
    // Auth on, no token: 401, not the default user.
    let mut bare = parts(&|e| {
        e.insert(AuthEnforced);
    });
    let rejection = Caller::from_request_parts(&mut bare, &())
        .await
        .err()
        .unwrap();
    assert_eq!(rejection.status(), axum::http::StatusCode::UNAUTHORIZED);
}
