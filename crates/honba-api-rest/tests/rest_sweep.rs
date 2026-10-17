//! The sweep routes through the real router (E4-S5): `POST /sweeps`, `GET /sweeps/{id}`.

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use honba_api::RunExecutor;
use honba_api_rest::{api_router_with, AppState, RunServiceConfig};
use serde_json::{json, Value};
use support::*;
use tower::util::ServiceExt;

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(v) => {
            req = req.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let res = router
        .clone()
        .oneshot(req.body(body).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("{uri}: non-JJSON {:?}", String::from_utf8_lossy(&bytes)));
    (status, parsed)
}

fn base_state() -> AppState {
    let reader = fixture_reader();
    AppState::new(reader.clone(), reader.clone(), reader.clone(), reader)
}

#[allow(dead_code)]
fn router_with(
    scratch: &support::Scratch,
    executor: Arc<dyn RunExecutor>,
    cfg: RunServiceConfig,
) -> (Router, Arc<honba_api_rest::RunStore>) {
    let clock = support::FakeClock::at(support::T0);
    let store = Arc::new(support::store(scratch.journals(), &clock));
    let service = support::start_service(store.clone(), executor, cfg);
    (
        api_router_with(base_state().with_run_service(service)),
        store,
    )
}

async fn poll_terminal(router: &Router, id: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (status, body) = call(router, "GET", &format!("/sweeps/{id}"), None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        if matches!(
            body["data"]["status"].as_str(),
            Some("completed" | "failed")
        ) {
            return body;
        }
        assert!(Instant::now() < deadline, "run {id} never finished");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn rest_sweep_roundtrip() {
    let scratch = support::Scratch::new("rest-sweep-roundtrip");
    let state = base_state()
        .with_journals_dir(&scratch.journals(), RunServiceConfig::default())
        .unwrap();
    let router = api_router_with(state);

    let (status, body) = call(
        &router,
        "POST",
        "/sweeps",
        Some(json!({
            "strategy": "sma_crossover",
            "seed": 42,
            "universe": "TCS.NSE",
            "start": "2024-01-01",
            "end": "2024-06-01",
            "bar_spec": "1d",
            "initial_capital": 1_000_000.0,
            "params": {"fast": [10, 30, 10], "slow": [50, 100, 25]},
            "trials": 9
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["error"].is_null());
    let id = body["data"]["job_id"].as_str().unwrap().to_owned();
    assert_eq!(id.len(), 26);
    assert_eq!(body["data"]["status"], "pending");
    assert!(body["data"]["report"].is_null());

    let done = poll_terminal(&router, &id).await;
    assert_eq!(done["data"]["status"], "completed", "{done}");
    let report = &done["data"]["report"];
    assert!(report["ranked"].is_array());
    assert_eq!(report["ranked"].as_array().unwrap().len(), 9);
    assert!(report["best"].is_object());

    // GET /sweeps/{id}/journal should 422 (per-trial journals, not one for the sweep)
    let (status, body) = call(&router, "GET", &format!("/sweeps/{id}/journal"), None).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(
        body["error"]["context"]["reason"],
        "sweep_journal_per_trial"
    );
}
