//! Unit tests for `honba-api` (ADR 007: one file per area).

mod cache;
mod endpoints;
mod ingest;
mod market;
mod run_id;
mod run_ports;
mod run_requests;
mod runs;
mod screener;
mod strategies;
mod stream_frame;

use serde_json::json;

use crate::{
    ApiResponse, BacktestMetrics, BacktestRequest, ErrorCode, ErrorDetail, ResponseEnvelope,
    RunStatus, API_VERSION,
};

#[test]
fn a_backtest_request_rejects_unknown_fields() {
    // A typo'd parameter must fail loudly rather than being ignored, or a
    // caller would run a backtest over the wrong universe.
    let raw = json!({"strategy": "sma", "univerze": "nifty50"});
    assert!(serde_json::from_value::<BacktestRequest>(raw).is_err());
}

#[test]
fn a_backtest_request_round_trips_through_json() {
    let req = BacktestRequest {
        strategy: Some("sma".into()),
        universe: Some("nifty50".into()),
        start: Some("2024-01-01".into()),
        end: Some("2025-01-01".into()),
        bar_spec: Some("1d".into()),
        initial_capital: Some(1_000_000.0),
        seed: Some(42),
        fill_model: None,
        commission_per_fill: None,
        slippage_bps: None,
    };
    let text = serde_json::to_string(&req).unwrap();
    let back: BacktestRequest = serde_json::from_str(&text).unwrap();
    assert_eq!(req, back);
}

#[test]
fn an_empty_backtest_request_is_accepted() {
    // Optional fields make this a compile step, not a run trigger; the run
    // endpoint is what rejects an incomplete request.
    let req: BacktestRequest = serde_json::from_value(json!({})).unwrap();
    assert!(req.strategy.is_none());
}

#[test]
fn envelope_carries_the_api_version_and_the_schema_version() {
    let env: ResponseEnvelope<BacktestMetrics> = ApiResponse::success(BacktestMetrics::default());
    assert_eq!(env.api_version.as_str(), API_VERSION);
    assert_eq!(env.schema_version, honba_messages::SCHEMA_VERSION);
}

#[test]
fn a_failing_envelope_round_trips_its_stable_code() {
    let env: ResponseEnvelope<BacktestMetrics> = ApiResponse::error(ErrorDetail::new(
        ErrorCode::RiskMaxNotionalExceeded,
        "too big",
    ));
    let text = serde_json::to_string(&env).unwrap();
    let back: ResponseEnvelope<BacktestMetrics> = serde_json::from_str(&text).unwrap();
    assert_eq!(back.error.unwrap().code, ErrorCode::RiskMaxNotionalExceeded);
}

#[test]
fn an_envelope_deserializes_for_a_payload_without_default() {
    // BacktestResponse does not implement Default; the envelope must not
    // demand it just because its `data` field tolerates absence.
    let env: ResponseEnvelope<crate::BacktestResponse> =
        ApiResponse::error(ErrorDetail::new(ErrorCode::Timeout, "slow"));
    let text = serde_json::to_string(&env).unwrap();
    let back: ResponseEnvelope<crate::BacktestResponse> = serde_json::from_str(&text).unwrap();
    assert_eq!(back.error.unwrap().code, ErrorCode::Timeout);
}

#[test]
fn run_status_serializes_in_snake_case() {
    assert_eq!(
        serde_json::to_value(RunStatus::Completed).unwrap(),
        json!("completed")
    );
    assert_eq!(
        serde_json::from_value::<RunStatus>(json!("running")).unwrap(),
        RunStatus::Running
    );
}

#[test]
fn api_error_codes_reach_callers_through_the_re_export() {
    // The codes moved to honba-messages (docs/archive/plan.md 4.2); this pins that the
    // honba-api re-export is the same type, not a copy.
    let via_api: crate::ErrorCode = ErrorCode::Timeout;
    let via_messages: honba_messages::ErrorCode = via_api;
    assert_eq!(via_messages, honba_messages::ErrorCode::Timeout);
}

mod capabilities;
mod unknown_fields;
mod verify;

/// A minimal verifiable strategy manifest shared by the run tests.
pub(crate) fn tests_manifest() -> honba_strategy::StrategyManifest {
    use honba_messages::{BarAggregation, Exchange, InstrumentId};
    use honba_strategy::{StrategyManifest, Subscriptions, TimeframeSpec, Universe};
    let tcs = InstrumentId::new("TCS", Exchange::new("NSE"));
    StrategyManifest::new(
        "sma",
        "sha256:abc",
        Universe::Explicit(vec![tcs.clone()]),
        TimeframeSpec::new(1, BarAggregation::Day),
    )
    .with_subscriptions(Subscriptions {
        instruments: vec![tcs],
        quotes: false,
        trades: false,
    })
    .with_warmup_bars(20)
}
