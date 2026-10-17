//! ADR 0017 wire changes through the public API: `error` on run responses and resolution
//! through serde. ADR 0012 rule 2: the additions are optional, so old payloads still parse.

use honba_api::{
    BacktestRequest, BacktestResponse, ErrorCode, ErrorDetail, RunId, RunIdGenerator, RunStatus,
    SweepRequest, SweepResponse,
};
use serde_json::json;

fn minted() -> RunId {
    RunIdGenerator::default()
        .next(1_700_000_000_000, [1; 10])
        .unwrap()
}

#[test]
fn a_response_without_error_omits_the_field() {
    let r = BacktestResponse {
        run_id: minted().to_string(),
        status: RunStatus::Pending,
        metrics: None,
        assumptions: None,
        validation: None,
        error: None,
    };
    let value = serde_json::to_value(&r).unwrap();
    assert!(value.get("error").is_none(), "{value}");
    assert_eq!(
        serde_json::from_value::<BacktestResponse>(value).unwrap(),
        r
    );
}

#[test]
fn a_failed_backtest_response_round_trips_its_error() {
    let error = ErrorDetail::new(ErrorCode::InternalError, "journal write failed")
        .with_context(json!({"reason": "journal_write"}));
    let r = BacktestResponse {
        run_id: minted().to_string(),
        status: RunStatus::Failed,
        metrics: None,
        assumptions: None,
        validation: None,
        error: Some(error.clone()),
    };
    let back: BacktestResponse = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
    assert_eq!(back.error, Some(error));
    assert_eq!(back.status, RunStatus::Failed);
}

#[test]
fn a_failed_sweep_response_round_trips_its_error() {
    let error = ErrorDetail::new(ErrorCode::InternalError, "panic")
        .with_context(json!({"reason": "panic"}));
    let r = SweepResponse {
        job_id: minted().to_string(),
        status: RunStatus::Failed,
        report: None,
        error: Some(error.clone()),
    };
    let value = serde_json::to_value(&r).unwrap();
    assert_eq!(value["error"]["code"], json!("internal_error"));
    assert_eq!(
        serde_json::from_value::<SweepResponse>(value)
            .unwrap()
            .error,
        Some(error)
    );
}

#[test]
fn payloads_from_before_the_error_field_still_parse() {
    let old = json!({"run_id": "r", "status": "completed"});
    assert!(serde_json::from_value::<BacktestResponse>(old)
        .unwrap()
        .error
        .is_none());
    let old = json!({"job_id": "j", "status": "running"});
    assert!(serde_json::from_value::<SweepResponse>(old)
        .unwrap()
        .error
        .is_none());
}

#[test]
fn a_cancelled_status_parses_on_a_response() {
    let r: BacktestResponse =
        serde_json::from_value(json!({"run_id": "r", "status": "cancelled"})).unwrap();
    assert_eq!(r.status, RunStatus::Cancelled);
}

#[test]
fn a_submit_body_resolves_through_serde() {
    let body = json!({
        "strategy": "sma", "universe": "nifty50",
        "start": "2024-01-01", "end": "2025-01-01", "seed": 42
    });
    let req: BacktestRequest = serde_json::from_value(body).unwrap();
    let resolved = req.resolve().unwrap();
    assert_eq!(resolved.bar_spec, "1d");
    assert_eq!(resolved.seed, 42);

    // The empty body parses (wire schema unchanged) and is refused by the resolver.
    let empty: BacktestRequest = serde_json::from_value(json!({})).unwrap();
    let err = empty.resolve().unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
}

#[test]
fn a_sweep_body_resolves_through_serde() {
    let body = json!({
        "strategy": "sma",
        "universe": "TCS.NSE",
        "start": "2024-01-01",
        "end": "2024-06-01",
        "bar_spec": "1d",
        "initial_capital": 1_000_000.0,
        "params": {"fast": [5, 20, 5]},
        "trials": 3,
        "seed": 9
    });
    let resolved = serde_json::from_value::<SweepRequest>(body)
        .unwrap()
        .resolve()
        .unwrap();
    assert_eq!((resolved.trials, resolved.seed), (3, 9));

    let zero = json!({
        "strategy": "sma",
        "universe": "TCS.NSE",
        "start": "2024-01-01",
        "end": "2024-06-01",
        "bar_spec": "1d",
        "initial_capital": 1_000_000.0,
        "params": {"a": [1, 2, 1]},
        "trials": 3,
        "seed": 0
    });
    let err = serde_json::from_value::<SweepRequest>(zero)
        .unwrap()
        .resolve()
        .unwrap_err();
    assert_eq!(err.context.unwrap()["field"], json!("seed"));
}

#[test]
fn a_run_id_survives_a_json_response_and_refuses_traversal() {
    let id = minted();
    let r = BacktestResponse {
        run_id: id.to_string(),
        status: RunStatus::Pending,
        metrics: None,
        assumptions: None,
        validation: None,
        error: None,
    };
    let back: BacktestResponse = serde_json::from_value(serde_json::to_value(r).unwrap()).unwrap();
    assert_eq!(RunId::parse(&back.run_id).unwrap(), id);
    for hostile in ["../x", "..%2F..%2Fetc", "%2e%2e%2f", "..\\x"] {
        assert!(RunId::parse(hostile).is_err());
    }
}
