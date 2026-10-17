//! The backtest and journal routes through the real router (ADR 0017 decision 7): a real
//! `RunService` over scratch journals, a deterministic executor where timing matters, and
//! the real kernel for the round trip. No socket, no network, no `/tmp`.

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use honba_api::{RunExecutor, RunKind};
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
        .unwrap_or_else(|_| panic!("{uri}: non-JSON {:?}", String::from_utf8_lossy(&bytes)));
    (status, parsed)
}

fn base_state() -> AppState {
    let reader = fixture_reader();
    AppState::new(reader.clone(), reader.clone(), reader.clone(), reader)
}

/// A router over a service with `executor`; returns the store for direct inspection.
fn router_with(
    scratch: &Scratch,
    executor: Arc<dyn RunExecutor>,
    cfg: RunServiceConfig,
) -> (Router, Arc<honba_api_rest::RunStore>) {
    let clock = FakeClock::at(T0);
    let store = Arc::new(store(scratch.journals(), &clock));
    let service = start_service(store.clone(), executor, cfg);
    (
        api_router_with(base_state().with_run_service(service)),
        store,
    )
}

fn submit_body(seed: u64) -> Value {
    serde_json::to_value(sma_request(seed)).unwrap()
}

async fn poll_terminal(router: &Router, id: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (status, body) = call(router, "GET", &format!("/backtests/{id}"), None).await;
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
async fn rest_backtest_roundtrip() {
    let scratch = Scratch::new("rest-roundtrip");
    let state = base_state()
        .with_journals_dir(&scratch.journals(), RunServiceConfig::default())
        .unwrap();
    let router = api_router_with(state);

    let (status, body) = call(&router, "POST", "/backtests", Some(submit_body(42))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["error"].is_null());
    let id = body["data"]["run_id"].as_str().unwrap().to_owned();
    assert_eq!(id.len(), 26);
    assert_eq!(body["data"]["status"], "pending");
    assert!(body["data"]["metrics"].is_null());

    let done = poll_terminal(&router, &id).await;
    assert_eq!(done["data"]["status"], "completed", "{done}");
    assert!(done["data"]["metrics"]["trades"].as_u64().unwrap() >= 1);
    assert!(done["data"]["assumptions"]["not_modelled"].is_array());
    assert!(done["data"].get("error").map_or(true, Value::is_null));

    let (status, journal) = call(&router, "GET", &format!("/backtests/{id}/journal"), None).await;
    assert_eq!(status, StatusCode::OK, "{journal}");
    let trades = journal["data"]["trades"].as_array().unwrap();
    assert!(trades.len() as u64 >= 2 * done["data"]["metrics"]["trades"].as_u64().unwrap());
    assert_eq!(trades[0]["instrument_id"]["symbol"], "TCS");
    assert!(trades[0]["costs"].is_object() || trades[0]["costs"].is_number());

    let (status, same) = call(&router, "GET", &format!("/journals/{id}"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(same["data"], journal["data"]);
}

#[tokio::test]
async fn ill_formed_and_unknown_ids_are_404_before_any_filesystem_access() {
    let scratch = Scratch::new("rest-bad-ids");
    let (router, store) = router_with(&scratch, real_executor(), config(1, 4, 100));
    let canary = scratch.path().join("canary");
    std::fs::write(&canary, "x").unwrap();
    let bad = [
        "..",
        "../",
        "..%2F",
        "..%2F..%2Fetc%2Fpasswd",
        "%2e%2e",
        "%2e%2e%2f%2e%2e%2fcanary",
        "..%5Ccanary",
        "01arz3ndektsv4rrffq69g5fav",
        "01ARZ3NDEKTSV4RRFFQ69G5FA",
        "01ARZ3NDEKTSV4RRFFQ69G5FAVV",
        "01ARZ3NDEKTSV4RRFFQ69G5FAU",
        "X.NSE",
        "%00",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV", // well-formed but unknown
    ];
    for id in bad {
        for uri in [
            format!("/backtests/{id}"),
            format!("/backtests/{id}/journal"),
            format!("/journals/{id}"),
        ] {
            let (status, body) = call(&router, "GET", &uri, None).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
            assert_eq!(body["error"]["code"], "not_found", "{uri}");
            assert!(body["data"].is_null(), "{uri}");
        }
    }
    assert!(store.list().is_empty());
    assert!(
        !scratch.journals().exists(),
        "a read created the journals root"
    );
    assert_eq!(std::fs::read_to_string(canary).unwrap(), "x");
}

#[tokio::test]
async fn a_sweep_id_is_404_on_backtest_routes_and_422_on_the_journal_route() {
    let scratch = Scratch::new("rest-sweep-id");
    let (router, store) = router_with(&scratch, real_executor(), config(1, 4, 100));
    let sweep = submit(&store, sweep_request(7));
    assert_eq!(sweep.kind, RunKind::Sweep);
    let id = sweep.run_id.to_string();

    for uri in [
        format!("/backtests/{id}"),
        format!("/backtests/{id}/journal"),
    ] {
        let (status, body) = call(&router, "GET", &uri, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(body["error"]["code"], "not_found");
    }
    let (status, body) = call(&router, "GET", &format!("/journals/{id}"), None).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "validation_invalid_request");
    assert_eq!(
        body["error"]["context"]["reason"],
        "sweep_journal_per_trial"
    );
}

#[tokio::test]
async fn sweeps_are_implemented_e4_s5() {
    // This test documents that sweeps are now implemented (E4-S5)
    let scratch = Scratch::new("rest-sweeps-e4s5");
    let (router, _) = router_with(&scratch, real_executor(), config(1, 4, 100));
    // POST /sweeps should now be implemented (not 501)
    let (_status, _body) = call(
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
    // Should be 200 (pending) or 422 (validation), not 501
    assert_ne!(
        _status,
        StatusCode::NOT_IMPLEMENTED,
        "POST /sweeps should be implemented"
    );
    // GET /sweeps/{id} should also be implemented
    let (status, _body) = call(&router, "GET", "/sweeps/01ARZ3NDEKTSV4RRFFQ69G5FAV", None).await;
    assert_ne!(
        status,
        StatusCode::NOT_IMPLEMENTED,
        "GET /sweeps/{{id}} should be implemented"
    );
}

#[tokio::test]
async fn a_full_queue_is_429_with_a_reason() {
    let scratch = Scratch::new("rest-queue-full");
    let gate = Arc::new(Gate::default());
    let (router, _) = router_with(
        &scratch,
        Arc::new(GatedExecutor(gate.clone())),
        config(1, 1, 100),
    );
    let (s1, first) = call(&router, "POST", "/backtests", Some(submit_body(1))).await;
    assert_eq!(s1, StatusCode::OK, "{first}");
    wait_started(&gate, 1);
    let (s2, second) = call(&router, "POST", "/backtests", Some(submit_body(2))).await;
    assert_eq!(s2, StatusCode::OK, "{second}");

    let (s3, third) = call(&router, "POST", "/backtests", Some(submit_body(3))).await;
    assert_eq!(s3, StatusCode::TOO_MANY_REQUESTS, "{third}");
    assert_eq!(third["error"]["code"], "rate_limited");
    assert_eq!(third["error"]["context"]["reason"], "run_queue_full");
    assert_eq!(third["error"]["context"]["max_queued"], 1);
    assert!(third["data"].is_null());

    gate.open();
    for body in [first, second] {
        let id = body["data"]["run_id"].as_str().unwrap();
        assert_eq!(
            poll_terminal(&router, id).await["data"]["status"],
            "completed"
        );
    }
}

#[tokio::test]
async fn the_journal_of_a_running_run_is_an_empty_prefix_not_an_error() {
    let scratch = Scratch::new("rest-partial");
    let gate = Arc::new(Gate::default());
    let (router, _) = router_with(
        &scratch,
        Arc::new(GatedExecutor(gate.clone())),
        config(1, 4, 100),
    );
    let (_, body) = call(&router, "POST", "/backtests", Some(submit_body(1))).await;
    let id = body["data"]["run_id"].as_str().unwrap().to_owned();
    wait_started(&gate, 1);
    let (status, running) = call(&router, "GET", &format!("/backtests/{id}"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(running["data"]["status"], "running");
    assert!(running["data"]["metrics"].is_null());
    for uri in [
        format!("/backtests/{id}/journal"),
        format!("/journals/{id}"),
    ] {
        let (status, journal) = call(&router, "GET", &uri, None).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {journal}");
        assert_eq!(journal["data"]["trades"], json!([]));
    }
    gate.open();
    poll_terminal(&router, &id).await;
}

#[tokio::test]
async fn resolver_failures_are_422_with_the_field_and_create_nothing() {
    let scratch = Scratch::new("rest-validation");
    let (router, store) = router_with(&scratch, real_executor(), config(1, 4, 100));
    let mut unknown = submit_body(1);
    unknown["strategy"] = json!("no_such_strategy");
    let mut bad_universe = submit_body(1);
    bad_universe["universe"] = json!("TCS.NSE,INFY.NSE");
    let mut zero_seed = submit_body(1);
    zero_seed["seed"] = json!(0);
    let cases = [
        (json!({}), "seed"),
        (unknown, "strategy"),
        (bad_universe, "universe"),
        (zero_seed, "seed"),
    ];
    for (body, field) in cases {
        let (status, parsed) = call(&router, "POST", "/backtests", Some(body)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{parsed}");
        assert_eq!(parsed["error"]["code"], "validation_invalid_request");
        assert_eq!(parsed["error"]["context"]["field"], field, "{parsed}");
    }
    let (status, parsed) = call(&router, "POST", "/backtests", Some(json!({"seed": "x"}))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{parsed}");
    assert!(store.list().is_empty());
    assert!(!scratch.journals().exists());
}

#[tokio::test]
async fn a_failed_run_is_a_200_poll_carrying_its_error() {
    let scratch = Scratch::new("rest-failed");
    let (router, _) = router_with(
        &scratch,
        Arc::new(FailingExecutor(false)),
        config(1, 4, 100),
    );
    let (_, body) = call(&router, "POST", "/backtests", Some(submit_body(1))).await;
    let id = body["data"]["run_id"].as_str().unwrap();
    let done = poll_terminal(&router, id).await;
    assert_eq!(done["data"]["status"], "failed");
    assert!(done["data"]["metrics"].is_null());
    assert_eq!(done["data"]["error"]["code"], "market_data_unavailable");
    assert_eq!(done["data"]["error"]["context"]["reason"], "no_data");
    assert!(done["data"]["assumptions"].is_null());
}

/// A completed run whose `events.ndjson` the test then rewrites.
async fn completed_run(router: &Router, scratch: &Scratch) -> (String, std::path::PathBuf) {
    let (_, body) = call(router, "POST", "/backtests", Some(submit_body(42))).await;
    let id = body["data"]["run_id"].as_str().unwrap().to_owned();
    assert_eq!(
        poll_terminal(router, &id).await["data"]["status"],
        "completed"
    );
    let events = scratch.journals().join(&id).join("events.ndjson");
    (id, events)
}

#[tokio::test]
async fn a_trailing_partial_record_is_ignored() {
    let scratch = Scratch::new("rest-torn");
    let (router, _) = router_with(&scratch, real_executor(), config(1, 4, 100));
    let (id, events) = completed_run(&router, &scratch).await;
    let uri = format!("/backtests/{id}/journal");
    let (_, before) = call(&router, "GET", &uri, None).await;
    let mut text = std::fs::read_to_string(&events).unwrap();
    text.push_str("{\"schema_version\":4,\"ev");
    std::fs::write(&events, text).unwrap();
    let (status, after) = call(&router, "GET", &uri, None).await;
    assert_eq!(status, StatusCode::OK, "{after}");
    assert_eq!(after["data"], before["data"]);
}

#[tokio::test]
async fn a_journal_at_another_schema_version_is_422_unsupported() {
    let scratch = Scratch::new("rest-schema");
    let (router, _) = router_with(&scratch, real_executor(), config(1, 4, 100));
    let (id, events) = completed_run(&router, &scratch).await;
    std::fs::write(
        &events,
        "{\"schema_version\":3,\"event\":{},\"ts_init\":1}\n",
    )
    .unwrap();
    for uri in [
        format!("/backtests/{id}/journal"),
        format!("/journals/{id}"),
    ] {
        let (status, body) = call(&router, "GET", &uri, None).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{uri}: {body}");
        assert_eq!(body["error"]["code"], "unsupported");
        assert_eq!(body["error"]["context"]["found"], 3);
        assert_eq!(
            body["error"]["context"]["expected"],
            honba_messages::SCHEMA_VERSION
        );
    }
}

#[tokio::test]
async fn a_fill_without_an_order_record_is_a_500_journal_orphan_fill() {
    let scratch = Scratch::new("rest-orphan");
    let (router, _) = router_with(&scratch, real_executor(), config(1, 4, 100));
    let (id, events) = completed_run(&router, &scratch).await;
    let fill = honba_messages::Message::new(
        honba_messages::Event::OrderFilled {
            order_id: honba_messages::OrderId::new("O-404"),
            last_qty: 1.0,
            last_px: 1.0,
            ts_event: honba_messages::UnixNanos::from_u64(1),
        },
        honba_messages::UnixNanos::from_u64(2),
    );
    std::fs::write(
        &events,
        format!("{}\n", serde_json::to_string(&fill).unwrap()),
    )
    .unwrap();
    let (status, body) = call(&router, "GET", &format!("/backtests/{id}/journal"), None).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"]["code"], "internal_error");
    assert_eq!(body["error"]["context"]["reason"], "journal_orphan_fill");
}

#[tokio::test]
async fn restart_recovery_through_the_router() {
    let scratch = Scratch::new("rest-restart");
    let cfg = || RunServiceConfig {
        max_concurrent: 1,
        ..RunServiceConfig::default()
    };
    let first = api_router_with(
        base_state()
            .with_journals_dir(&scratch.journals(), cfg())
            .unwrap(),
    );
    let (id, _) = completed_run(&first, &scratch).await;
    let (_, metrics) = call(&first, "GET", &format!("/backtests/{id}"), None).await;
    let (_, trades) = call(&first, "GET", &format!("/backtests/{id}/journal"), None).await;
    drop(first);

    // A manifest left non-terminal on disk is what a crash looks like.
    let crashed = {
        let clock = FakeClock::at(T0);
        submit_backtest(&store(scratch.journals(), &clock), 9)
            .run_id
            .to_string()
    };

    let second = api_router_with(
        base_state()
            .with_journals_dir(&scratch.journals(), cfg())
            .unwrap(),
    );
    let (status, again) = call(&second, "GET", &format!("/backtests/{id}"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again["data"], metrics["data"]);
    let (_, trades_again) = call(&second, "GET", &format!("/backtests/{id}/journal"), None).await;
    assert_eq!(trades_again["data"], trades["data"]);

    let (status, interrupted) = call(&second, "GET", &format!("/backtests/{crashed}"), None).await;
    assert_eq!(status, StatusCode::OK, "{interrupted}");
    assert_eq!(interrupted["data"]["status"], "failed");
    assert_eq!(interrupted["data"]["error"]["code"], "internal_error");
    assert_eq!(
        interrupted["data"]["error"]["context"]["reason"],
        "interrupted"
    );
}

#[tokio::test]
async fn shutting_down_stops_admission() {
    let scratch = Scratch::new("rest-shutdown");
    let state = base_state()
        .with_journals_dir(&scratch.journals(), config(1, 4, 50))
        .unwrap();
    let router = api_router_with(state.clone());
    let report = state.shutdown_runs().expect("a service is configured");
    assert_eq!(report.cancelled_pending, 0);
    let (status, body) = call(&router, "POST", "/backtests", Some(submit_body(1))).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(body["error"]["context"]["reason"], "shutting_down");
    assert!(base_state().shutdown_runs().is_none());
}
