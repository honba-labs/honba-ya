//! `GET /capabilities` is derived from the endpoint registry and agrees with the router.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use honba_api::ENDPOINTS;
use honba_api_rest::{api_router, NOT_IMPLEMENTED_ENDPOINTS};
use serde_json::Value;
use tower::util::ServiceExt;

async fn call(method: &str, uri: &str) -> (StatusCode, Value) {
    let res = api_router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn capabilities_list_every_registry_endpoint_in_order() {
    let (status, body) = call("GET", "/capabilities").await;
    assert_eq!(status, StatusCode::OK);
    let manifest = &body["data"]["capabilities"];
    let want: Vec<String> = ENDPOINTS.iter().map(|(m, p)| format!("{m} {p}")).collect();
    assert_eq!(strings(&manifest["endpoints"]), want);
}

#[tokio::test]
async fn the_not_implemented_flags_match_what_the_router_answers() {
    let (_, body) = call("GET", "/capabilities").await;
    let flagged = strings(&body["data"]["capabilities"]["not_implemented"]);
    let mut probed = Vec::new();
    for (method, path) in ENDPOINTS {
        let uri = path.replace("{id}", "X.NSE");
        let (status, _) = call(method, &uri).await;
        assert_ne!(status, StatusCode::METHOD_NOT_ALLOWED, "{method} {path}");
        if status == StatusCode::NOT_IMPLEMENTED {
            probed.push(format!("{method} {path}"));
        }
    }
    assert_eq!(flagged, probed);
    let constant: Vec<String> = NOT_IMPLEMENTED_ENDPOINTS
        .iter()
        .map(|(m, p)| format!("{m} {p}"))
        .collect();
    assert_eq!(flagged, constant);
}

#[test]
fn the_strategy_routes_are_built() {
    for route in [("POST", "/strategies"), ("GET", "/strategies")] {
        assert!(
            !NOT_IMPLEMENTED_ENDPOINTS.contains(&route),
            "{route:?} is implemented and must not be flagged"
        );
    }
}

#[tokio::test]
async fn the_backtest_and_journal_routes_are_built_and_only_sweeps_and_trading_stay_flagged() {
    // ADR 0017 decision 7 shrinks the list to the four trading rows once sweeps (E4-S5) land;
    // ADR 0018 decision 7 (E2-S2 r5b) builds the risk gate for POST /orders, DELETE /orders/:id and POST /positions/close, so those
    // leave the list too; E11-S7 builds the write ledger, so GET /orders leaves it as well
    // and the flagged set is empty.
    let (_, body) = call("GET", "/capabilities").await;
    assert_eq!(
        strings(&body["data"]["capabilities"]["not_implemented"]),
        Vec::<String>::new()
    );
    for route in [
        ("POST", "/backtests"),
        ("GET", "/backtests/{id}"),
        ("GET", "/backtests/{id}/journal"),
        ("GET", "/journals/{id}"),
        ("POST", "/sweeps"),
        ("GET", "/sweeps/{id}"),
    ] {
        assert!(
            !NOT_IMPLEMENTED_ENDPOINTS.contains(&route),
            "{route:?} is implemented and must not be flagged"
        );
    }
}

#[tokio::test]
async fn probing_the_unflagged_routes_answers_non_501_and_writes_nothing() {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("capabilities-probe-journals");
    let _ = std::fs::remove_dir_all(&dir);
    let state = honba_api_rest::AppState::default()
        .with_journals_dir(&dir, honba_api_rest::RunServiceConfig::default())
        .unwrap();
    let router = honba_api_rest::api_router_with(state);
    let flagged: Vec<String> = NOT_IMPLEMENTED_ENDPOINTS
        .iter()
        .map(|(m, p)| format!("{m} {p}"))
        .collect();
    for (method, path) in ENDPOINTS {
        if flagged.contains(&format!("{method} {path}")) {
            continue;
        }
        let uri = path.replace("{id}", "X.NSE");
        let res = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(*method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(res.status(), StatusCode::NOT_IMPLEMENTED, "{method} {path}");
    }
    let written = std::fs::read_dir(&dir).map_or(0, Iterator::count);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(written, 0, "the probe wrote into the journals root");
}
