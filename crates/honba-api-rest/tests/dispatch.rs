//! `dispatch` drives the real router in process (no socket): the same statuses and
//! envelopes the served API gives, over a Parquet directory written in a temp dir.

use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use honba_api_rest::{api_router_with, dispatch, AppState, DispatchError};
use honba_feed::import::parquet_source::bar_schema;
use parquet::arrow::ArrowWriter;
use serde_json::{json, Value};

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// A fresh directory per call, so parallel tests never share files.
fn data_dir() -> PathBuf {
    let name = NEXT.fetch_add(1, Ordering::Relaxed).to_string();
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("honba-api-rest-dispatch")
        .join(&name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let schema = Arc::new(bar_schema());
    let col = |v: f64| -> ArrayRef { Arc::new(Float64Array::from(vec![v; 2])) };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![60_000_000_000_i64, 120_000_000_000])),
        col(10.0),
        col(12.0),
        col(9.0),
        col(11.0),
        col(100.0),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let file = File::create(dir.join("TCS.NSE.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    dir
}

async fn call(method: &str, target: &str, body: Option<&str>) -> (u16, Value) {
    let state = AppState::from_parquet_dir(&data_dir()).unwrap();
    let (status, text) = dispatch(api_router_with(state), method, target, body)
        .await
        .unwrap();
    (status, serde_json::from_str(&text).unwrap_or(Value::Null))
}

#[tokio::test]
async fn a_get_returns_the_envelope_and_status() {
    let (status, body) = call("GET", "/health", None).await;
    assert_eq!((status, &body["data"]["status"]), (200, &json!("ok")));
    let (status, body) = call("GET", "/quotes?symbols=TCS", None).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"]["quotes"][0]["bid_price"], 11.0);
}

#[tokio::test]
async fn errors_keep_their_status_and_envelope() {
    let (status, body) = call("GET", "/instruments/NOPE.NSE", None).await;
    assert_eq!(status, 404);
    assert_eq!(body["error"]["code"], "instrument_not_found");
    let (status, body) = call("GET", "/bars/TCS.NSE?tf=banana", None).await;
    assert_eq!(status, 422);
    assert_eq!(body["error"]["context"]["field"], "tf");
    let (status, body) = call("GET", "/orders", None).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"]["orders"], json!([]));
    let (status, body) = call("GET", "/no/such/route", None).await;
    assert_eq!(status, 404);
    assert_eq!(body["error"]["code"], "not_found");
    let (status, body) = call("PUT", "/health", None).await;
    assert_eq!(status, 405);
    assert_eq!(body["error"]["code"], "unsupported");
}

#[tokio::test]
async fn a_post_body_is_sent_as_json() {
    let (status, body) = call("POST", "/strategies/verify", Some("{}")).await;
    assert_eq!(status, 422, "{body}");
    assert_eq!(body["error"]["code"], "validation_invalid_request");
}

#[tokio::test]
async fn bad_arguments_are_errors_not_panics() {
    let router = api_router_with(AppState::default());
    assert!(matches!(
        dispatch(router.clone(), "NOT A METHOD", "/health", None).await,
        Err(DispatchError::InvalidMethod(_))
    ));
    assert!(matches!(
        dispatch(router, "GET", "no-slash", None).await,
        Err(DispatchError::InvalidTarget(_))
    ));
}
