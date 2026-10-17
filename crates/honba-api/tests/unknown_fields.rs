//! ADR 0012 through the public API: a newer server's enveloped response with
//! extra fields (top level and nested) still parses, while a request body with a typo is refused.

use honba_api::{BacktestRequest, DepthResponse, ResponseEnvelope};
use serde_json::json;

#[test]
fn an_enveloped_response_from_a_newer_server_still_parses() {
    let wire = json!({
        "api_version": honba_api::API_VERSION,
        "schema_version": honba_messages::SCHEMA_VERSION,
        "data": {"bids": [{"price": 101.5, "qty": 10.0, "orders": 2}], "asks": [], "ts": 7},
    });
    let back: ResponseEnvelope<DepthResponse> = serde_json::from_value(wire).unwrap();
    assert_eq!(back.into_result().unwrap().bids[0].qty, 10.0);
}

#[test]
fn a_request_body_with_a_misspelt_field_is_refused() {
    let raw = json!({"strategy": "sma", "intial_capital": 1.0});
    let err = serde_json::from_value::<BacktestRequest>(raw).unwrap_err();
    assert!(err.to_string().contains("intial_capital"), "{err}");
}
