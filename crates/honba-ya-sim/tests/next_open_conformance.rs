//! Cross-language vectors for the next-open simulator (ADR 0016).
//!
//! Reads `schema/conformance/next_open_sim.json`, generated from the Python
//! `NextOpenExecution` by `scripts/gen_next_open_vectors.py`; the Python test
//! `python/tests/integration/test_next_open_sim_conformance.py` replays the same file. Only the
//! chunks this implementation covers (`IMPLEMENTED_CHUNKS`) are replayed.

use std::path::PathBuf;

use honba_engine::{AlgoError, ExecutionEngine};
use honba_entities::{Currency, Money};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Exchange, InstrumentId, Order, OrderId,
    OrderSide, OrderType, PriceType, TimeInForce, UnixNanos,
};
use honba_sim::{FillCostFn, NextOpenSim};
use serde_json::Value;

/// Vector chunks (ADR 0016) the Rust simulator implements so far.
const IMPLEMENTED_CHUNKS: &[u64] = &[1, 2];

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/conformance/next_open_sim.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn iid(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("NSE"))
}

fn ts(v: &Value) -> UnixNanos {
    UnixNanos::from_u64(v.as_u64().unwrap())
}

fn bar(step: &Value) -> Bar {
    let t = ts(&step["ts"]);
    let bt = BarType::new(
        iid(step["symbol"].as_str().unwrap()),
        BarSpecification::new(1, BarAggregation::Day, PriceType::Last),
    );
    let px = |k: &str| step[k].as_f64().unwrap();
    Bar::new(
        bt,
        px("open"),
        px("high"),
        px("low"),
        px("close"),
        step["volume"].as_f64().unwrap_or(1000.0),
        t,
        t,
    )
}

/// JSON equality where `10` equals `10.0` (Python writes an int quantity as an int).
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same(p, q))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| same(v, w)))
        }
        _ => a == b,
    }
}

fn side(s: &str) -> OrderSide {
    if s == "buy" {
        OrderSide::Buy
    } else {
        OrderSide::Sell
    }
}

fn order(step: &Value) -> Order {
    let order_type = match step["type"].as_str().unwrap() {
        "market" => OrderType::Market,
        "limit" => OrderType::Limit,
        "stop_market" => OrderType::StopMarket,
        _ => OrderType::StopLimit,
    };
    let t = ts(&step["ts"]);
    let o = Order::new(
        OrderId::new(step["id"].as_str().unwrap()),
        iid(step["symbol"].as_str().unwrap()),
        side(step["side"].as_str().unwrap()),
        order_type,
        step["qty"].as_f64().unwrap(),
        step["price"].as_f64(),
        TimeInForce::Day,
        t,
        t,
    );
    match step["trigger"].as_f64() {
        Some(trigger) => o.with_trigger_price(trigger),
        None => o,
    }
}

fn side_str(s: OrderSide) -> &'static str {
    if s == OrderSide::Buy {
        "buy"
    } else {
        "sell"
    }
}

/// The vectors' test cost model: `flat + (notional * bps + 5000) / 10000` per side, in minor
/// units (the generator documents it; the real India schedule lives above `honba-sim`).
fn cost_fn(spec: &Value) -> FillCostFn {
    let get = |k: &str| spec[k].as_i64().unwrap_or(0);
    let (flat_buy, flat_sell) = (get("flat_buy"), get("flat_sell"));
    let (bps_buy, bps_sell) = (get("bps_buy"), get("bps_sell"));
    Box::new(move |side, qty, price| {
        let notional = Money::mul_qty(qty, price, Currency::Inr)
            .map_err(|e| AlgoError::Component(e.to_string()))?
            .minor();
        let (flat, bps) = if side == OrderSide::Buy {
            (flat_buy, bps_buy)
        } else {
            (flat_sell, bps_sell)
        };
        Ok(Money::new(
            flat + (notional * bps + 5000).div_euclid(10_000),
            Currency::Inr,
        ))
    })
}

fn build(config: &Value) -> NextOpenSim {
    assert_eq!(config["currency"], "INR");
    let cash = Money::new(config["cash"].as_i64().unwrap(), Currency::Inr);
    let mut sim = NextOpenSim::new(cash)
        .unwrap()
        .with_settlement_days(config["settlement_days"].as_i64().unwrap())
        .unwrap()
        .with_long_only(config["long_only"].as_bool().unwrap());
    if config.get("costs").is_some() {
        sim = sim.with_costs(cost_fn(&config["costs"]));
    }
    for (symbol, lot) in config["lot_sizes"].as_object().unwrap() {
        sim.set_lot_size(&iid(symbol), lot.as_f64().unwrap())
            .unwrap();
    }
    sim
}

fn run(sc: &Value) {
    let name = sc["name"].as_str().unwrap();
    let mut sim = build(&sc["config"]);
    let mut drains: Vec<Value> = Vec::new();
    let mut probes: Vec<Value> = Vec::new();
    for step in sc["steps"].as_array().unwrap() {
        let op = step["op"].as_str().unwrap();
        let result = match op {
            "bar" => sim.on_bar(&bar(step)),
            "open_session" => {
                let bars: Vec<Bar> = step["bars"].as_array().unwrap().iter().map(bar).collect();
                sim.open_session(ts(&step["ts"]), &bars)
            }
            "session_open" => {
                let bars: Vec<Bar> = step["bars"].as_array().unwrap().iter().map(bar).collect();
                sim.on_session_open(ts(&step["ts"]), &bars)
            }
            "set_settlement_days" => sim.set_settlement_days(step["days"].as_i64().unwrap()),
            "probe" => {
                probes.push(serde_json::json!({
                    "unsettled": sim.unsettled().minor(),
                    "available_cash": sim.available_cash().minor(),
                }));
                Ok(())
            }
            "submit" => ExecutionEngine::submit(&mut sim, order(step)),
            "cancel" => {
                ExecutionEngine::cancel(&mut sim, step["id"].as_str().unwrap(), ts(&step["now"]))
            }
            "drain" => {
                let fills: Vec<Value> = sim
                    .drain_fills()
                    .unwrap()
                    .iter()
                    .map(|f| {
                        serde_json::json!([
                            f.order_id().as_str(),
                            f.instrument_id().symbol(),
                            side_str(f.side()),
                            f.quantity(),
                            f.price(),
                            f.ts_event().as_u64(),
                            f.costs().minor(),
                        ])
                    })
                    .collect();
                let rejections: Vec<Value> = sim
                    .drain_rejections()
                    .unwrap()
                    .iter()
                    .map(|r| {
                        serde_json::json!([
                            r.order_id.as_str(),
                            r.instrument_id.symbol(),
                            side_str(r.side),
                            r.quantity,
                            r.reason,
                            r.ts.as_u64(),
                        ])
                    })
                    .collect();
                drains.push(serde_json::json!({"fills": fills, "rejections": rejections}));
                Ok(())
            }
            other => panic!("{name}: unknown op {other}"),
        };
        let expect_error = step["error"].as_bool().unwrap_or(false);
        assert_eq!(
            result.is_err(),
            expect_error,
            "{name}: step {step} error mismatch: {result:?}"
        );
    }

    let expect = &sc["expect"];
    let drains = Value::Array(drains);
    assert!(
        same(&drains, &expect["drains"]),
        "{name}: drained fills and rejections\n got {drains}\nwant {}",
        expect["drains"]
    );
    let fin = &expect["final"];
    assert_eq!(
        sim.cash().minor(),
        fin["cash"].as_i64().unwrap(),
        "{name}: cash"
    );
    assert_eq!(
        sim.fees().minor(),
        fin["fees"].as_i64().unwrap(),
        "{name}: fees"
    );
    assert_eq!(
        sim.traded_notional().minor(),
        fin["traded_notional"].as_i64().unwrap(),
        "{name}: traded notional"
    );
    assert_eq!(
        serde_json::json!(sim.working_orders()),
        fin["working"],
        "{name}: working orders"
    );
    if fin.get("unsettled").is_some() {
        assert_eq!(
            sim.unsettled().minor(),
            fin["unsettled"].as_i64().unwrap(),
            "{name}: unsettled"
        );
        assert_eq!(
            sim.available_cash().minor(),
            fin["available_cash"].as_i64().unwrap(),
            "{name}: available cash"
        );
    }
    if let Some(want) = fin.get("probes") {
        assert_eq!(&Value::Array(probes), want, "{name}: probes");
    }
    let mut positions = sim.positions();
    positions.sort_by(|a, b| a.0.cmp(&b.0));
    let positions: serde_json::Map<String, Value> = positions
        .iter()
        .map(|(id, q)| (id.symbol().to_string(), serde_json::json!(q)))
        .collect();
    assert_eq!(
        Value::Object(positions),
        fin["positions"],
        "{name}: positions"
    );
}

#[test]
fn next_open_vectors_match_the_python_reference() {
    let doc = fixture();
    assert_eq!(doc["fixture_version"], 1);
    let mut replayed = 0;
    for sc in doc["scenarios"].as_array().unwrap() {
        if IMPLEMENTED_CHUNKS.contains(&sc["chunk"].as_u64().unwrap()) {
            run(sc);
            replayed += 1;
        }
    }
    assert!(replayed > 0, "no scenario replayed");
}
