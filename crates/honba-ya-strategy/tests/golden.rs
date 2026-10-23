//! Golden-vector contract tests for `OrderIntent` (ADR 006).
//!
//! Reads the shared vectors in `schema/golden/order_intent.json`; the Python
//! tests read the same file. Invalid cases must be rejected on deserialize.

use std::path::PathBuf;

use honba_messages::SCHEMA_VERSION;
use honba_messages::{Exchange, InstrumentId, OrderId, OrderType, TimeInForce, UnixNanos};
use honba_strategy::OrderIntent;
use serde_json::Value;

fn golden() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../schema/golden/order_intent.json");
    let doc: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(doc["schema_version"], u64::from(SCHEMA_VERSION));
    assert_eq!(doc["type"], "OrderIntent");
    doc
}

fn nifty() -> InstrumentId {
    InstrumentId::new("NIFTY50", Exchange::new("NSE"))
}

fn expected(name: &str) -> OrderIntent {
    match name {
        "market_buy" => OrderIntent::market_buy(nifty(), 75.0),
        "limit_sell" => OrderIntent {
            time_in_force: TimeInForce::Gtc,
            ..OrderIntent::limit_sell(nifty(), 25.0, 22_100.5)
        },
        "stop_buy" => OrderIntent::stop_buy(nifty(), 75.0, 22_050.0),
        "stop_limit_sell" => OrderIntent {
            time_in_force: TimeInForce::Ioc,
            ..OrderIntent::stop_limit_sell(nifty(), 75.0, 21_950.0, 21_940.0)
        },
        "trailing_stop_sell_amount" => OrderIntent::trailing_stop_amount_sell(nifty(), 75.0, 50.0),
        "trailing_stop_buy_percent" => OrderIntent {
            time_in_force: TimeInForce::Gtc,
            ..OrderIntent::trailing_stop_percent_buy(nifty(), 75.0, 1.5)
        },
        other => panic!("no expected value for golden case {other}"),
    }
}

#[test]
fn order_intent_golden_cases_roundtrip() {
    let doc = golden();
    for case in doc["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let want = expected(name);
        assert_eq!(want.validate(), Ok(()), "{name}");
        let got: OrderIntent =
            serde_json::from_value(case["value"].clone()).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(got, want, "{name}: deserialized");
        assert_eq!(
            serde_json::to_value(&want).unwrap(),
            case["value"],
            "{name}: JSON"
        );
    }
}

#[test]
fn order_intent_invalid_cases_are_rejected() {
    let doc = golden();
    let invalid = doc["invalid"].as_array().unwrap();
    assert!(!invalid.is_empty());
    for case in invalid {
        let name = case["name"].as_str().unwrap();
        let res: Result<OrderIntent, _> = serde_json::from_value(case["value"].clone());
        assert!(res.is_err(), "{name}: invalid intent was accepted");
    }
}

#[test]
fn order_intent_invalid_text_is_rejected() {
    let doc = golden();
    let cases = doc["invalid_text"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let res: Result<OrderIntent, _> = serde_json::from_str(case["text"].as_str().unwrap());
        assert!(res.is_err(), "{name}: invalid text was accepted");
    }
}

#[test]
fn stop_limit_intent_becomes_order_with_both_prices() {
    let intent = OrderIntent::stop_limit_buy(nifty(), 75.0, 22_000.0, 22_010.0);
    let order = intent
        .into_order(OrderId::new("O-1"), UnixNanos::from_u64(5))
        .unwrap();
    assert_eq!(order.order_type(), OrderType::StopLimit);
    assert_eq!(order.price(), Some(22_010.0));
    assert_eq!(order.trigger_price(), Some(22_000.0));
}

#[test]
fn trailing_stop_intent_becomes_order_with_trail_and_no_prices() {
    let intent = OrderIntent::trailing_stop_amount_sell(nifty(), 75.0, 50.0);
    let order = intent
        .into_order(OrderId::new("O-2"), UnixNanos::from_u64(5))
        .unwrap();
    assert_eq!(order.order_type(), OrderType::TrailingStop);
    assert_eq!(order.price(), None);
    assert_eq!(order.trigger_price(), None);
    assert_eq!(order.trail_amount(), Some(50.0));
    assert_eq!(order.trail_percent(), None);
}
