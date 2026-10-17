//! ADR 0012: response DTOs are records (unknown fields ignored), request DTOs
//! are inputs (unknown fields rejected).

use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use crate::{
    BacktestMetrics, BacktestRequest, BacktestResponse, BarsQuery, BarsResponse, Capabilities,
    CapabilityManifest, CompiledStrategy, DepthLevel, DepthQuery, DepthResponse, InstrumentsQuery,
    InstrumentsResponse, OrdersRequest, OrdersResponse, PositionsResponse, QuotesQuery,
    QuotesResponse, StrategiesRequest, StrategiesResponse, SweepReportResponse, SweepRequest,
    SweepResponse, TradesResponse,
};

fn with_extra(mut value: Value) -> Value {
    value
        .as_object_mut()
        .expect("sample is an object")
        .insert("added_by_a_newer_server".into(), json!(1));
    value
}

fn tolerates_extra<T: DeserializeOwned + PartialEq + std::fmt::Debug>(sample: Value) {
    let plain: T = serde_json::from_value(sample.clone()).expect("sample parses");
    let extended: T = serde_json::from_value(with_extra(sample)).unwrap_or_else(|e| {
        panic!(
            "{} rejected an unknown field: {e}",
            std::any::type_name::<T>()
        )
    });
    assert_eq!(plain, extended, "the unknown field is dropped on parse");
}

fn rejects_extra<T: DeserializeOwned + std::fmt::Debug>(sample: Value) {
    serde_json::from_value::<T>(sample.clone()).expect("sample parses");
    assert!(
        serde_json::from_value::<T>(with_extra(sample)).is_err(),
        "{} accepted an unknown field",
        std::any::type_name::<T>()
    );
}

fn strategy_manifest() -> Value {
    json!({
        "api_version": "1.0.0",
        "name": "sma",
        "source_hash": "sha256:abc",
        "universe": {"explicit": [{"symbol": "TCS", "exchange": "NSE"}]},
        "subscriptions": {"instruments": [{"symbol": "TCS", "exchange": "NSE"}]},
        "driving_timeframe": {"interval": 1, "aggregation": "day"},
        "warmup_bars": 20
    })
}

fn compiled() -> Value {
    let manifest = serde_json::from_value(strategy_manifest()).unwrap();
    let mut catalog = crate::StrategyCatalog::default();
    serde_json::to_value(crate::compile_strategy(&mut catalog, manifest).unwrap()).unwrap()
}

fn manifest() -> Value {
    json!({
        "crates": [], "market_packs": [], "endpoints": [], "toolsets": [],
        "adapters": [], "features": {}
    })
}

#[test]
fn every_response_type_ignores_unknown_fields() {
    tolerates_extra::<CapabilityManifest>(manifest());
    tolerates_extra::<Capabilities>(json!({"capabilities": manifest()}));
    tolerates_extra::<InstrumentsResponse>(json!({"instruments": []}));
    tolerates_extra::<QuotesResponse>(json!({"quotes": []}));
    tolerates_extra::<BarsResponse>(json!({"bars": []}));
    tolerates_extra::<DepthLevel>(json!({"price": 1.0, "qty": 2.0}));
    tolerates_extra::<DepthResponse>(json!({"bids": [], "asks": []}));
    tolerates_extra::<StrategiesResponse>(json!({"strategies": []}));
    tolerates_extra::<StrategiesResponse>(json!({"strategies": [compiled()]}));
    tolerates_extra::<CompiledStrategy>(compiled());
    tolerates_extra::<BacktestMetrics>(json!({
        "trades": 1, "net_pnl": 0.0, "sharpe": 0.0, "max_drawdown": 0.0, "total_return": 0.0
    }));
    tolerates_extra::<BacktestResponse>(json!({"run_id": "r", "status": "pending"}));
    tolerates_extra::<SweepResponse>(json!({"job_id": "j", "status": "running"}));
    tolerates_extra::<SweepReportResponse>(json!({"ranked": []}));
    tolerates_extra::<OrdersResponse>(json!({"orders": []}));
    tolerates_extra::<PositionsResponse>(json!({"positions": []}));
    tolerates_extra::<TradesResponse>(json!({"trades": []}));
}

#[test]
fn every_request_type_rejects_unknown_fields() {
    rejects_extra::<InstrumentsQuery>(json!({}));
    rejects_extra::<QuotesQuery>(json!({}));
    rejects_extra::<BarsQuery>(json!({}));
    rejects_extra::<DepthQuery>(json!({}));
    rejects_extra::<StrategiesRequest>(json!({"manifest": strategy_manifest()}));
    rejects_extra::<BacktestRequest>(json!({}));
    rejects_extra::<SweepRequest>(json!({}));
    rejects_extra::<OrdersRequest>(json!({}));
}
