//! `GET /instruments`, `GET /instruments/{id}` and `GET /bars/{id}` through the real
//! router, over an in-memory dataset and over a Parquet directory generated in a
//! temp dir. No network, no wall clock.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use honba_api_rest::{api_router_with, AppState};
use honba_feed::import::parquet_source::bar_schema;
use honba_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use honba_messages::{
    BarAggregation, BarSpecification, Exchange, InstrumentId, PriceType, UnixNanos,
};
use parquet::arrow::ArrowWriter;
use serde_json::{json, Value};
use tower::util::ServiceExt;

const MINUTE: u64 = 60_000_000_000;
/// 2024-01-01T00:00:00Z.
const T0: u64 = 1_704_067_200_000_000_000;

fn slice(symbol: &str, exchange: &str, count: u64) -> honba_feed::ColumnarSlice {
    let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let mut builder =
        ColumnarSliceBuilder::new(InstrumentId::new(symbol, Exchange::new(exchange)), spec);
    for i in 0..count {
        let px = 100.0 + i as f64;
        builder
            .push(
                UnixNanos::from_u64(T0 + i * MINUTE),
                px,
                px + 2.0,
                px - 1.0,
                px + 1.0,
                1000.0,
            )
            .unwrap();
    }
    builder.finish().unwrap()
}

fn app() -> Router {
    // Supplied out of order on purpose: the API must not depend on input order.
    let dataset = Dataset::from_slices(vec![
        slice("TCS", "NSE", 5),
        slice("INFY", "NSE", 2),
        slice("TCS", "BSE", 1),
    ])
    .unwrap();
    api_router_with(AppState::from_reader(DatasetReader::from_dataset(dataset)))
}

async fn get(app: Router, uri: &str) -> (StatusCode, Value) {
    let res = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("{uri}: non-JSON {:?}", String::from_utf8_lossy(&bytes)));
    (status, body)
}

fn assert_error(status: StatusCode, body: &Value, want: StatusCode, code: &str) {
    assert_eq!(status, want, "{body}");
    assert_eq!(body["error"]["code"], json!(code), "{body}");
    assert!(body["data"].is_null(), "{body}");
    assert!(body["api_version"].is_string(), "{body}");
}

fn ids(body: &Value) -> Vec<String> {
    body["data"]["instruments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| {
            format!(
                "{}.{}",
                i["id"]["symbol"].as_str().unwrap(),
                i["id"]["exchange"].as_str().unwrap()
            )
        })
        .collect()
}

fn unix_nanos(body: &Value) -> Vec<String> {
    body["data"]["bars"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["ts_event"]["unix_nanos"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn instruments_list_in_id_order_inside_the_envelope() {
    let (status, body) = get(app(), "/instruments").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["error"].is_null());
    assert!(body["api_version"].is_string());
    assert_eq!(ids(&body), vec!["INFY.NSE", "TCS.BSE", "TCS.NSE"]);
    let first = &body["data"]["instruments"][0];
    assert_eq!(first["kind"], "equity");
    assert_eq!(first["currency"], "INR");
    assert!(first["lot_size"].is_number() && first["tick_size"].is_number());
}

#[tokio::test]
async fn instruments_filter_by_exchange_symbol_and_both() {
    let (_, by_exchange) = get(app(), "/instruments?exchange=NSE").await;
    assert_eq!(ids(&by_exchange), vec!["INFY.NSE", "TCS.NSE"]);
    let (_, by_symbol) = get(app(), "/instruments?symbol=TCS").await;
    assert_eq!(ids(&by_symbol), vec!["TCS.BSE", "TCS.NSE"]);
    let (_, both) = get(app(), "/instruments?exchange=BSE&symbol=TCS").await;
    assert_eq!(ids(&both), vec!["TCS.BSE"]);
}

#[tokio::test]
async fn a_filter_with_no_match_is_an_empty_list_not_an_error() {
    let (status, body) = get(app(), "/instruments?exchange=LSE").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["instruments"], json!([]));
}

#[tokio::test]
async fn an_unknown_instruments_query_parameter_is_a_422() {
    let (status, body) = get(app(), "/instruments?exhange=NSE").await;
    assert_error(
        status,
        &body,
        StatusCode::UNPROCESSABLE_ENTITY,
        "validation_invalid_request",
    );
}

#[tokio::test]
async fn one_instrument_is_returned_by_id() {
    let (status, body) = get(app(), "/instruments/TCS.NSE").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["data"],
        json!({
            "id": {"symbol": "TCS", "exchange": "NSE"},
            "kind": "equity",
            "currency": "INR",
            "lot_size": 1.0,
            "tick_size": 0.05,
        })
    );
}

#[tokio::test]
async fn an_unknown_instrument_is_a_404_instrument_not_found_envelope() {
    let (status, body) = get(app(), "/instruments/NOPE.NSE").await;
    assert_error(status, &body, StatusCode::NOT_FOUND, "instrument_not_found");
}

#[tokio::test]
async fn a_malformed_instrument_id_is_a_422() {
    let (status, body) = get(app(), "/instruments/TCS").await;
    assert_error(
        status,
        &body,
        StatusCode::UNPROCESSABLE_ENTITY,
        "validation_invalid_request",
    );
    assert_eq!(body["error"]["context"]["field"], "id");
}

#[tokio::test]
async fn bars_come_back_ascending_with_iso_and_unix_nanos_timestamps() {
    let (status, body) = get(app(), "/bars/TCS.NSE").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["error"].is_null());
    let want: Vec<String> = (0..5).map(|i| (T0 + i * MINUTE).to_string()).collect();
    assert_eq!(unix_nanos(&body), want);
    let first = &body["data"]["bars"][0];
    assert_eq!(first["ts_event"]["iso"], "2024-01-01T00:00:00.000000000Z");
    assert_eq!(first["open"], 100.0);
    assert_eq!(first["high"], 102.0);
    assert_eq!(first["low"], 99.0);
    assert_eq!(first["close"], 101.0);
    assert_eq!(first["volume"], 1000.0);
}

#[tokio::test]
async fn bars_honour_an_inclusive_from_and_exclusive_to() {
    let uri = "/bars/TCS.NSE?from=2024-01-01T00:01:00Z&to=2024-01-01T00:03:00Z";
    let (status, body) = get(app(), uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        unix_nanos(&body),
        vec![(T0 + MINUTE).to_string(), (T0 + 2 * MINUTE).to_string()]
    );
}

#[tokio::test]
async fn bars_accept_plain_dates_and_an_explicit_timeframe() {
    let (status, body) = get(app(), "/bars/TCS.NSE?tf=1m&from=2024-01-01&to=2024-01-02").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(unix_nanos(&body).len(), 5);
}

#[tokio::test]
async fn a_range_with_no_bars_is_an_empty_list() {
    let (status, body) = get(app(), "/bars/TCS.NSE?from=2030-01-01").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["bars"], json!([]));
}

#[tokio::test]
async fn the_same_request_gives_byte_identical_bodies() {
    let (_, first) = get(app(), "/bars/TCS.NSE?from=2024-01-01").await;
    let (_, second) = get(app(), "/bars/TCS.NSE?from=2024-01-01").await;
    // api_version and schema_version are constants; nothing here reads the clock.
    assert_eq!(first, second);
}

#[tokio::test]
async fn bars_for_an_unknown_instrument_are_a_404() {
    let (status, body) = get(app(), "/bars/NOPE.NSE").await;
    assert_error(status, &body, StatusCode::NOT_FOUND, "instrument_not_found");
}

#[tokio::test]
async fn a_timeframe_the_data_lacks_is_a_404_market_data_unavailable() {
    let (status, body) = get(app(), "/bars/TCS.NSE?tf=1d").await;
    assert_error(
        status,
        &body,
        StatusCode::NOT_FOUND,
        "market_data_unavailable",
    );
}

#[tokio::test]
async fn bad_bars_queries_are_422_envelopes_naming_the_field() {
    for (uri, field) in [
        ("/bars/TCS.NSE?tf=banana", "tf"),
        ("/bars/TCS.NSE?tf=0m", "tf"),
        ("/bars/TCS.NSE?from=yesterday", "from"),
        ("/bars/TCS.NSE?to=2024-13-45", "to"),
        ("/bars/TCS.NSE?from=2024-01-02&to=2024-01-01", "to"),
        ("/bars/TCS.NSE?from=2024-01-01&to=2024-01-01", "to"),
    ] {
        let (status, body) = get(app(), uri).await;
        assert_error(
            status,
            &body,
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_invalid_request",
        );
        assert_eq!(body["error"]["context"]["field"], json!(field), "{uri}");
    }
}

#[tokio::test]
async fn unknown_bars_query_parameters_and_malformed_ids_are_422() {
    for uri in ["/bars/TCS.NSE?limit=10", "/bars/TCS"] {
        let (status, body) = get(app(), uri).await;
        assert_error(
            status,
            &body,
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_invalid_request",
        );
    }
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("honba_rest_{}_{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn write_parquet(path: &Path, count: u64) {
    let schema = Arc::new(bar_schema());
    let col = |f: &dyn Fn(f64) -> f64| -> ArrayRef {
        Arc::new(Float64Array::from(
            (0..count).map(|i| f(i as f64)).collect::<Vec<_>>(),
        ))
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            (0..count)
                .map(|i| (T0 + i * MINUTE) as i64)
                .collect::<Vec<_>>(),
        )),
        col(&|i| 100.0 + i),
        col(&|i| 102.0 + i),
        col(&|i| 99.0 + i),
        col(&|i| 101.0 + i),
        col(&|_| 1000.0),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test]
async fn a_parquet_data_directory_serves_instruments_and_bars() {
    let dir = TempDir::new("serve");
    write_parquet(&dir.0.join("TCS.NSE.parquet"), 4);
    write_parquet(&dir.0.join("INFY.NSE.parquet"), 2);
    let state = AppState::from_parquet_dir(&dir.0).unwrap();
    let app = api_router_with(state);

    let (status, body) = get(app.clone(), "/instruments").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ids(&body), vec!["INFY.NSE", "TCS.NSE"]);

    let (status, body) = get(app.clone(), "/bars/TCS.NSE?from=2024-01-01T00:02:00Z").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        unix_nanos(&body),
        vec![(T0 + 2 * MINUTE).to_string(), (T0 + 3 * MINUTE).to_string()]
    );
    assert_eq!(body["data"]["bars"][0]["close"], 103.0);
}

fn quote_ids(body: &Value) -> Vec<String> {
    body["data"]["quotes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|q| {
            format!(
                "{}.{}",
                q["instrument_id"]["symbol"].as_str().unwrap(),
                q["instrument_id"]["exchange"].as_str().unwrap()
            )
        })
        .collect()
}

#[tokio::test]
async fn the_latest_quote_is_the_last_close_with_zero_sizes_and_both_timestamp_forms() {
    let (status, body) = get(app(), "/quotes?symbols=TCS&venue=NSE").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(quote_ids(&body), vec!["TCS.NSE"]);
    let quote = &body["data"]["quotes"][0];
    assert_eq!(quote["bid_price"], json!(105.0));
    assert_eq!(quote["ask_price"], json!(105.0));
    assert_eq!(quote["bid_size"], json!(0.0));
    assert_eq!(quote["ask_size"], json!(0.0));
    assert_eq!(quote["ts_event"]["iso"], "2024-01-01T00:04:00.000000000Z");
    assert_eq!(
        quote["ts_event"]["unix_nanos"],
        json!((T0 + 4 * MINUTE).to_string())
    );
    assert_eq!(quote["ts_init"], quote["ts_event"]);
}

#[tokio::test]
async fn symbols_expand_across_venues_in_id_order_and_keep_each_symbol_once() {
    let (status, body) = get(app(), "/quotes?symbols=TCS,INFY,TCS").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(quote_ids(&body), vec!["INFY.NSE", "TCS.BSE", "TCS.NSE"]);
}

#[tokio::test]
async fn as_of_returns_the_quote_known_at_that_time_and_never_a_later_one() {
    let (status, body) = get(
        app(),
        "/quotes?symbols=TCS&venue=NSE&as_of=2024-01-01T00:02:30Z",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["quotes"][0]["bid_price"], json!(103.0));
    let (_, exact) = get(
        app(),
        "/quotes?symbols=TCS&venue=NSE&as_of=2024-01-01T00:02:00Z",
    )
    .await;
    assert_eq!(exact["data"]["quotes"][0]["bid_price"], json!(103.0));
}

#[tokio::test]
async fn a_quote_before_the_first_bar_is_a_404_market_data_unavailable() {
    let (status, body) = get(app(), "/quotes?symbols=TCS&venue=NSE&as_of=2023-12-31").await;
    assert_error(
        status,
        &body,
        StatusCode::NOT_FOUND,
        "market_data_unavailable",
    );
}

#[tokio::test]
async fn quotes_are_deterministic() {
    let (_, first) = get(app(), "/quotes?symbols=TCS,INFY").await;
    let (_, second) = get(app(), "/quotes?symbols=TCS,INFY").await;
    assert_eq!(first["data"], second["data"]);
}

#[tokio::test]
async fn quote_requests_that_cannot_be_served_use_the_error_envelope() {
    let (status, body) = get(app(), "/quotes?symbols=NOPE").await;
    assert_error(status, &body, StatusCode::NOT_FOUND, "instrument_not_found");
    let (status, body) = get(app(), "/quotes?symbols=TCS&venue=MCX").await;
    assert_error(status, &body, StatusCode::NOT_FOUND, "instrument_not_found");
    for uri in [
        "/quotes",
        "/quotes?symbols=",
        "/quotes?symbols=TCS&as_of=soon",
        "/quotes?symbol=TCS",
    ] {
        let (status, body) = get(app(), uri).await;
        assert_error(
            status,
            &body,
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_invalid_request",
        );
    }
}

#[tokio::test]
async fn depth_is_a_404_market_data_unavailable_because_the_dataset_has_bars_only() {
    let (status, body) = get(app(), "/depth/TCS.NSE").await;
    assert_error(
        status,
        &body,
        StatusCode::NOT_FOUND,
        "market_data_unavailable",
    );
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("order book"));
    let (status, body) = get(app(), "/depth/TCS.NSE?depth=10").await;
    assert_error(
        status,
        &body,
        StatusCode::NOT_FOUND,
        "market_data_unavailable",
    );
}

#[tokio::test]
async fn depth_requests_are_validated_before_the_port_is_asked() {
    let (status, body) = get(app(), "/depth/NOPE.NSE").await;
    assert_error(status, &body, StatusCode::NOT_FOUND, "instrument_not_found");
    for uri in [
        "/depth/TCS",
        "/depth/TCS.NSE?depth=0",
        "/depth/TCS.NSE?depth=51",
        "/depth/TCS.NSE?levels=3",
    ] {
        let (status, body) = get(app(), uri).await;
        assert_error(
            status,
            &body,
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_invalid_request",
        );
    }
}

#[tokio::test]
async fn a_selection_over_the_row_cap_is_a_422_too_many_rows() {
    let cap = honba_api_rest::MAX_BAR_ROWS as u64;
    let app = || {
        let dataset = Dataset::from_slices(vec![slice("BIG", "NSE", cap + 1)]).unwrap();
        api_router_with(AppState::from_reader(DatasetReader::from_dataset(dataset)))
    };

    let (status, body) = get(app(), "/bars/BIG.NSE?tf=1m").await;
    assert_error(
        status,
        &body,
        StatusCode::UNPROCESSABLE_ENTITY,
        "validation_invalid_request",
    );
    assert_eq!(body["error"]["context"]["reason"], "too_many_rows");
    assert_eq!(body["error"]["context"]["limit"], json!(cap));

    // Narrowing the range below the cap succeeds: [T0, T0 + 10 min) is 10 bars.
    let (status, body) = get(app(), "/bars/BIG.NSE?tf=1m&to=2024-01-01T00:10:00Z").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["bars"].as_array().unwrap().len(), 10);
}
