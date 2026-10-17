//! A persistent Parquet tick store (ADR 0030 follow-up) drives a `fill_model = l2` run through
//! the real router: the store is written to disk, reopened, and the run matches against the
//! ticks it reads back instead of an in-memory reader.

mod support;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use honba_api_rest::{api_router_with, AppState, RunServiceConfig};
use honba_config::{ExecutionConfig, FillModel};
use honba_feed::{write_tick_file, ParquetTickStore, TICK_DIR};
use honba_messages::{Exchange, InstrumentId, QuoteTick, UnixNanos};
use serde_json::Value;
use support::*;
use tower::util::ServiceExt;

const BARS_FROM_NS: u64 = 1_704_067_200_000_000_000;
const DAY_NS: u64 = 86_400_000_000_000;

fn l2() -> ExecutionConfig {
    ExecutionConfig {
        fill_model: FillModel::L2,
        ..ExecutionConfig::default()
    }
}

/// Writes a store directory holding one quote per fixture bar, then reopens it.
fn parquet_store(root: &Path) -> ParquetTickStore {
    let tcs = InstrumentId::new("TCS", Exchange::new("NSE"));
    let quotes: Vec<QuoteTick> = (0..80u64)
        .map(|i| {
            let t = UnixNanos::from_u64(BARS_FROM_NS + i * DAY_NS);
            let mid = 100.0 + i as f64 * 0.05;
            QuoteTick::new(tcs.clone(), mid - 0.1, mid + 0.1, 1e6, 1e6, t, t)
        })
        .collect();
    let file = root.join(TICK_DIR).join("ticks.parquet");
    write_tick_file(&file, &quotes, &[]).expect("tick file written");
    let store = ParquetTickStore::open(root).expect("store reopened");
    let (quotes_in_store, _) = store.len();
    assert_eq!(quotes_in_store, 80, "every tick read back");
    store
}

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
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn run_to_end(router: &Router) -> Value {
    let body = serde_json::to_value(sma_request(42)).unwrap();
    let (status, submitted) = call(router, "POST", "/backtests", Some(body)).await;
    assert_eq!(status, StatusCode::OK, "{submitted}");
    let id = submitted["data"]["run_id"].as_str().unwrap().to_owned();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (_, body) = call(router, "GET", &format!("/backtests/{id}"), None).await;
        if matches!(
            body["data"]["status"].as_str(),
            Some("completed" | "failed")
        ) {
            return body;
        }
        assert!(Instant::now() < deadline, "run never finished");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn l2_run_matches_against_a_parquet_tick_store() {
    let scratch = Scratch::new("rest-l2-parquet");
    let store = parquet_store(scratch.path());
    let reader = fixture_reader();
    let state = AppState::new(reader.clone(), reader.clone(), reader.clone(), reader)
        .with_execution(l2())
        .with_tick_reader(Arc::new(store))
        .with_journals_dir(&scratch.journals(), RunServiceConfig::default())
        .unwrap();
    let router = api_router_with(state);
    let done = run_to_end(&router).await;
    assert_eq!(done["data"]["status"], "completed", "{done}");
}

#[tokio::test]
async fn a_parquet_store_with_no_ticks_is_still_the_l2_refusal() {
    let scratch = Scratch::new("rest-l2-parquet-empty");
    let empty = scratch.path().join(TICK_DIR);
    std::fs::create_dir_all(&empty).unwrap();
    let store = ParquetTickStore::open(scratch.path()).expect("an empty store opens");
    let reader = fixture_reader();
    let state = AppState::new(reader.clone(), reader.clone(), reader.clone(), reader)
        .with_execution(l2())
        .with_tick_reader(Arc::new(store))
        .with_journals_dir(&scratch.journals(), RunServiceConfig::default())
        .unwrap();
    let router = api_router_with(state);
    let done = run_to_end(&router).await;
    assert_eq!(done["data"]["status"], "failed", "{done}");
    assert_eq!(
        done["data"]["error"]["context"]["reason"],
        "l2_requires_ticks"
    );
}
