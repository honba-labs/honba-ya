//! Tests for the endpoint registry (docs/archive/plan.md E11-S1).

use honba_messages::{write_endpoints, Access, Endpoint, HttpMethod, ParamLocation, ENDPOINTS};

#[test]
fn path_params_are_extracted_in_order() {
    let ep: Endpoint<(), ()> = Endpoint::new(
        HttpMethod::Get,
        "/backtests/{id}/journal",
        Access::ReadOnly,
        ParamLocation::Path,
        false,
        "journal",
    );
    assert_eq!(ep.path_params(), vec!["id"]);
    assert_eq!(ep.key(), "GET /backtests/{id}/journal");
}

#[test]
fn a_path_without_params_yields_none() {
    let ep: Endpoint<(), ()> = Endpoint::new(
        HttpMethod::Get,
        "/health",
        Access::ReadOnly,
        ParamLocation::Query,
        false,
        "health",
    );
    assert!(ep.path_params().is_empty());
}

#[test]
fn multiple_path_params_are_all_found() {
    let ep: Endpoint<(), ()> = Endpoint::new(
        HttpMethod::Get,
        "/instruments/{exchange}/{symbol}",
        Access::ReadOnly,
        ParamLocation::Path,
        false,
        "instrument",
    );
    assert_eq!(ep.path_params(), vec!["exchange", "symbol"]);
}

#[test]
fn an_unterminated_placeholder_does_not_loop_forever() {
    // A malformed path must not hang the generator that walks the registry.
    let ep: Endpoint<(), ()> = Endpoint::new(
        HttpMethod::Get,
        "/broken/{id",
        Access::ReadOnly,
        ParamLocation::Path,
        false,
        "broken",
    );
    assert!(ep.path_params().is_empty());
}

#[test]
fn write_endpoints_are_exactly_the_order_position_and_bar_ingest_routes() {
    assert_eq!(
        write_endpoints(),
        vec![
            "POST /bars",
            "PUT /trading-state",
            "POST /orders",
            "DELETE /orders/{id}",
            "POST /positions/close",
        ]
    );
}

#[test]
fn every_registered_endpoint_is_unique_per_method() {
    let mut seen = std::collections::BTreeSet::new();
    for (method, path) in ENDPOINTS {
        assert!(
            seen.insert(format!("{method} {path}")),
            "duplicate endpoint {method} {path}"
        );
    }
}

#[test]
fn every_registered_path_starts_at_the_root_and_has_no_trailing_slash() {
    // OpenAPI paths are joined onto `/api/v1`, so a leading or trailing slash
    // here would produce a double slash in the served spec.
    for (_, path) in ENDPOINTS {
        assert!(path.starts_with('/'), "{path} must start with /");
        assert!(!path.ends_with('/'), "{path} must not end with /");
        assert!(!path.contains("//"), "{path} must not contain //");
    }
}

#[test]
fn every_write_endpoint_is_registered() {
    // A gating rule that names an unregistered endpoint would silently never
    // fire, so the two lists are cross-checked.
    for entry in write_endpoints() {
        assert!(
            ENDPOINTS.iter().any(|(m, p)| format!("{m} {p}") == entry),
            "{entry} is gated but not in the registry"
        );
    }
}

#[test]
fn every_path_placeholder_is_balanced() {
    for (_, path) in ENDPOINTS {
        assert_eq!(
            path.matches('{').count(),
            path.matches('}').count(),
            "unbalanced braces in {path}"
        );
    }
}

#[test]
fn http_methods_serialize_uppercase_for_openapi() {
    // OpenAPI requires uppercase method keys; a lowercase key is ignored by
    // generators, silently dropping the operation.
    assert_eq!(
        serde_json::to_value(HttpMethod::Get).unwrap(),
        serde_json::json!("GET")
    );
    assert_eq!(HttpMethod::Delete.as_str(), "DELETE");
    assert_eq!(HttpMethod::Put.as_str(), "PUT");
}

#[test]
fn strategy_verification_is_registered_and_not_gated() {
    // Verifying compiles a manifest and moves no money, so it must never land
    // behind the approval queue.
    assert!(ENDPOINTS.contains(&("POST", "/strategies/verify")));
    assert!(!write_endpoints().contains(&"POST /strategies/verify".to_string()));
}
