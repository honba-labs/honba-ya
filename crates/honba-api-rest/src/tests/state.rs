//! Unit tests for the app state, the query extractor and the route table.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use honba_api::ENDPOINTS;
use honba_feed::DatasetReader;
use honba_ports::BarRequest;
use serde_json::Value;
use tower::util::ServiceExt;

use crate::{api_router, AppState};

async fn call(method: &str, uri: &str) -> (StatusCode, Vec<u8>) {
    let res = api_router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

#[tokio::test]
async fn the_default_state_serves_an_empty_catalogue() {
    let state = AppState::default();
    assert!(state
        .instruments
        .list_instruments()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn a_state_built_from_a_reader_shares_it_between_both_ports() {
    let state = AppState::from_reader(DatasetReader::from_dataset(Default::default()));
    assert!(state
        .instruments
        .list_instruments()
        .await
        .unwrap()
        .is_empty());
    let id = honba_messages::InstrumentId::new("X", honba_messages::Exchange::new("NSE"));
    let spec = honba_api::parse_timeframe("1m").unwrap();
    let request = BarRequest::new(id, spec, None, None).unwrap();
    assert!(state.bars.read_bars(&request).await.unwrap().is_empty());
}

#[test]
fn a_missing_data_directory_is_an_error_not_an_empty_state() {
    assert!(AppState::from_parquet_dir(std::path::Path::new("/nonexistent/honba")).is_err());
}

#[tokio::test]
async fn every_registry_endpoint_is_routed() {
    // An unrouted path is a bodyless 404 and a wrong method is a 405; a
    // handler's own 404 carries the JSON envelope.
    for (method, path) in ENDPOINTS {
        let uri = path.replace("{id}", "X.NSE");
        let (status, body) = call(method, &uri).await;
        assert_ne!(status, StatusCode::METHOD_NOT_ALLOWED, "{method} {path}");
        if status == StatusCode::NOT_FOUND {
            let json: Value = serde_json::from_slice(&body)
                .unwrap_or_else(|_| panic!("{method} {path} is not routed"));
            assert!(json["error"].is_object(), "{method} {path}");
        }
    }
}

#[tokio::test]
async fn an_unregistered_path_is_not_routed() {
    let (status, body) = call("GET", "/definitely/not/a/route").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["code"], "not_found");
}

#[tokio::test]
async fn an_unknown_query_parameter_is_a_422_envelope() {
    let (status, body) = call("GET", "/instruments?bogus=1").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["code"], "validation_invalid_request");
    assert!(json["data"].is_null());
}

#[tokio::test]
async fn a_reader_state_serves_quotes_and_reports_no_depth_from_the_same_reader() {
    let state = AppState::from_reader(DatasetReader::from_dataset(Default::default()));
    let id = honba_messages::InstrumentId::new("X", honba_messages::Exchange::new("NSE"));
    assert_eq!(state.quotes.read_quote(&id, None).await.unwrap(), None);
    assert!(matches!(
        state.depth.read_depth(&id, 5).await,
        Err(honba_ports::PortError::Unsupported(_))
    ));
}

#[test]
fn cloned_states_share_one_strategy_catalog() {
    let state = AppState::default();
    let clone = state.clone();
    assert!(std::sync::Arc::ptr_eq(&state.strategies, &clone.strategies));
    assert!(state.strategies.lock().unwrap().is_empty());
}

#[test]
fn the_strategy_catalog_capacity_can_be_replaced() {
    let state =
        AppState::default().with_strategy_catalog(honba_api::StrategyCatalog::with_limit(3));
    assert_eq!(state.strategies.lock().unwrap().limit(), 3);
}
