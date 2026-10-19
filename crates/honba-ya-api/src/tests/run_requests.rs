//! ADR 0017 decision 6: `BacktestRequest::resolve` and `SweepRequest::resolve`.

use serde_json::json;

use crate::{
    BacktestRequest, DEFAULT_INITIAL_CAPITAL, ErrorCode, ErrorDetail, SweepRequest,
    unknown_strategy,
};

fn full() -> BacktestRequest {
    BacktestRequest {
        strategy: Some("sma".into()),
        universe: Some("nifty50".into()),
        start: Some("2024-01-01".into()),
        end: Some("2025-01-01".into()),
        bar_spec: None,
        initial_capital: None,
        seed: Some(42),
        fill_model: None,
        commission_per_fill: None,
        slippage_bps: None,
    }
}

fn assert_field(err: ErrorDetail, field: &str) {
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest, "{err}");
    assert_eq!(
        err.context.as_ref().unwrap()["field"],
        json!(field),
        "{err}"
    );
}

#[test]
fn backtest_request_resolve() {
    // `{}` fails on the first required field.
    assert!(BacktestRequest::default().resolve().is_err());

    let r = full().resolve().unwrap();
    assert_eq!(r.strategy, "sma");
    assert_eq!(r.universe, "nifty50");
    assert_eq!(r.bar_spec, "1d", "default bar_spec");
    assert_eq!(r.initial_capital, DEFAULT_INITIAL_CAPITAL);
    assert_eq!(r.initial_capital, 1_000_000.0);
    assert_eq!(r.seed, 42);
    assert!(r.start < r.end);
}

#[test]
fn backtest_resolve_keeps_explicit_optionals() {
    let mut req = full();
    req.bar_spec = Some("5m".into());
    req.initial_capital = Some(250_000.0);
    let r = req.resolve().unwrap();
    assert_eq!(r.bar_spec, "5m");
    assert_eq!(r.initial_capital, 250_000.0);
}

#[test]
fn backtest_resolve_carries_a_known_fill_model_and_defaults_to_none() {
    assert_eq!(
        full().resolve().unwrap().fill_model,
        None,
        "unset by default"
    );

    let mut req = full();
    req.fill_model = Some("l2".into());
    assert_eq!(req.resolve().unwrap().fill_model.as_deref(), Some("l2"));
}

#[test]
fn backtest_resolve_rejects_an_unknown_or_blank_fill_model() {
    let mut req = full();
    req.fill_model = Some("midpoint".into());
    let err = req.resolve().unwrap_err();
    assert_eq!(
        err.context.as_ref().unwrap()["reason"],
        json!("unknown_fill_model")
    );
    assert_field(err, "fill_model");

    let mut req = full();
    req.fill_model = Some("  ".into());
    assert_field(req.resolve().unwrap_err(), "fill_model");
}

#[test]
fn backtest_resolve_defaults_cost_knobs_to_zero_and_carries_explicit_ones() {
    let r = full().resolve().unwrap();
    assert_eq!(r.commission_per_fill, 0.0, "unset commission is free");
    assert_eq!(r.slippage_bps, 0.0, "unset slippage is none");

    let mut req = full();
    req.commission_per_fill = Some(20.0);
    req.slippage_bps = Some(5.0);
    let r = req.resolve().unwrap();
    assert_eq!(r.commission_per_fill, 20.0);
    assert_eq!(r.slippage_bps, 5.0);
}

#[test]
fn backtest_resolve_rejects_out_of_range_cost_knobs() {
    for bad in [
        -1.0,
        f64::NAN,
        f64::INFINITY,
        crate::MAX_COMMISSION_PER_FILL + 1.0,
    ] {
        let mut req = full();
        req.commission_per_fill = Some(bad);
        assert_field(req.resolve().unwrap_err(), "commission_per_fill");
    }

    for bad in [-1.0, f64::NAN, f64::INFINITY, crate::MAX_SLIPPAGE_BPS + 1.0] {
        let mut req = full();
        req.slippage_bps = Some(bad);
        assert_field(req.resolve().unwrap_err(), "slippage_bps");
    }
}

#[test]
fn backtest_resolve_missing_each_required_field() {
    type Clear = fn(&mut BacktestRequest);
    let cases: [(&str, Clear); 5] = [
        ("seed", |r| r.seed = None),
        ("strategy", |r| r.strategy = None),
        ("universe", |r| r.universe = None),
        ("start", |r| r.start = None),
        ("end", |r| r.end = None),
    ];
    for (field, clear) in cases {
        let mut req = full();
        clear(&mut req);
        assert_field(req.resolve().unwrap_err(), field);
    }
}

#[test]
fn backtest_resolve_rejects_zero_seed() {
    let mut req = full();
    req.seed = Some(0);
    let err = req.resolve().unwrap_err();
    assert_eq!(err.context.as_ref().unwrap()["reason"], json!("zero_seed"));
    assert_field(err, "seed");
}

#[test]
fn backtest_resolve_rejects_blank_and_invalid_values() {
    let mut req = full();
    req.strategy = Some("  ".into());
    assert_field(req.resolve().unwrap_err(), "strategy");

    let mut req = full();
    req.universe = Some("".into());
    assert_field(req.resolve().unwrap_err(), "universe");

    let mut req = full();
    req.start = Some("yesterday".into());
    assert_field(req.resolve().unwrap_err(), "start");

    let mut req = full();
    req.end = Some("2023-01-01".into());
    assert_field(req.resolve().unwrap_err(), "end");

    let mut req = full();
    req.bar_spec = Some("fast".into());
    assert_field(req.resolve().unwrap_err(), "bar_spec");

    for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let mut req = full();
        req.initial_capital = Some(bad);
        assert_field(req.resolve().unwrap_err(), "initial_capital");
    }
}

#[test]
fn backtest_resolve_error_never_names_a_path() {
    let mut req = full();
    req.universe = Some(String::new());
    let text = serde_json::to_string(&req.resolve().unwrap_err()).unwrap();
    assert!(!text.contains('/'), "{text}");
}

#[test]
fn unknown_strategy_is_a_strategy_field_error() {
    let err = unknown_strategy("sha256:nope");
    assert_field(err.clone(), "strategy");
    assert_eq!(err.context.unwrap()["reason"], json!("unknown_strategy"));
}

fn full_sweep() -> SweepRequest {
    SweepRequest {
        strategy: Some("sma".into()),
        universe: Some("TCS.NSE".into()),
        start: Some("2024-01-01".into()),
        end: Some("2024-06-01".into()),
        bar_spec: Some("1d".into()),
        initial_capital: Some(1_000_000.0),
        params: Some(json!({"fast": [5, 20, 5]})),
        trials: Some(10),
        seed: Some(7),
    }
}

#[test]
fn sweep_request_resolve() {
    assert!(SweepRequest::default().resolve().is_err());
    let r = full_sweep().resolve().unwrap();
    assert_eq!(r.strategy, "sma");
    assert_eq!(r.trials, 10);
    assert_eq!(r.seed, 7);
    assert_eq!(r.params, json!({"fast": [5, 20, 5]}));
}

#[test]
fn sweep_resolve_missing_each_required_field() {
    type Clear = fn(&mut SweepRequest);
    let cases: [(&str, Clear); 4] = [
        ("seed", |r| r.seed = None),
        ("strategy", |r| r.strategy = None),
        ("params", |r| r.params = None),
        ("trials", |r| r.trials = None),
    ];
    for (field, clear) in cases {
        let mut req = full_sweep();
        clear(&mut req);
        assert_field(req.resolve().unwrap_err(), field);
    }
}

#[test]
fn sweep_resolve_rejects_zero_seed_zero_trials_and_non_object_params() {
    let mut req = full_sweep();
    req.seed = Some(0);
    assert_field(req.resolve().unwrap_err(), "seed");

    let mut req = full_sweep();
    req.trials = Some(0);
    assert_field(req.resolve().unwrap_err(), "trials");

    for bad in [json!([1, 2]), json!({}), json!("x"), json!(null)] {
        let mut req = full_sweep();
        req.params = Some(bad);
        assert_field(req.resolve().unwrap_err(), "params");
    }
}
