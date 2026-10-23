//! Cross-language vectors for the execution reject/cancel path.
//!
//! Reads `schema/conformance/order_rejections.json`; the Python test
//! `python/tests/integration/test_order_rejections_conformance.py` reads the same file.

use std::collections::HashSet;
use std::path::PathBuf;

use honba_engine::{Handler, Result};
use honba_messages::{Bar, Event, OrderSide, UnixNanos};
use honba_sim::{Behavior, ScriptedExecution};
use honba_strategy::{OrderIntent, Strategy, StrategyContext, StrategyRunner};
use honba_testing::VecFeed;
use honba_testing::fixtures::instrument;
use serde_json::{Value, json};

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/conformance/order_rejections.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// Submits the scenario's intents on the first bar event of each ts.
struct Scripted {
    name: String,
    submit: Value,
    done: HashSet<u64>,
}

impl Strategy for Scripted {
    fn name(&self) -> &str {
        &self.name
    }
    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, bar: &Bar) -> Result<()> {
        let ts = bar.ts_event().as_u64();
        if !self.done.insert(ts) {
            return Ok(());
        }
        let Some(intents) = self.submit.get(ts.to_string()) else {
            return Ok(());
        };
        for i in intents.as_array().unwrap() {
            let id = instrument(i[1].as_str().unwrap());
            let qty = i[2].as_f64().unwrap();
            ctx.submit(match i[0].as_str().unwrap() {
                "buy" => OrderIntent::market_buy(id, qty),
                _ => OrderIntent::market_sell(id, qty),
            });
        }
        Ok(())
    }
}

fn venue(script: &Value) -> ScriptedExecution {
    let mut exec = ScriptedExecution::new(10.0);
    for (id, spec) in script.as_object().unwrap() {
        let reason = spec["reason"].as_str().unwrap_or_default();
        let behavior = match spec["action"].as_str().unwrap() {
            "reject" => Behavior::reject(reason),
            "partial" => Behavior::partial(spec["filled"].as_f64().unwrap(), reason),
            "hold" => Behavior::Hold,
            other => panic!("unknown action {other}"),
        };
        exec = exec.with(id.clone(), behavior);
    }
    exec
}

#[test]
fn order_rejection_vectors() {
    let doc = fixture();
    let name = doc["strategy_name"].as_str().unwrap();
    for scenario in doc["scenarios"].as_array().unwrap() {
        let label = scenario["name"].as_str().unwrap();
        let strategy = Scripted {
            name: name.to_string(),
            submit: scenario["submit"].clone(),
            done: HashSet::new(),
        };
        let mut runner = StrategyRunner::new(strategy, venue(&scenario["venue"]));
        runner.on_start().unwrap();
        for e in scenario["events"].as_array().unwrap() {
            let (symbol, ts) = (e[1].as_str().unwrap(), e[2].as_u64().unwrap());
            let event: Event = VecFeed::bar(symbol, 10.0, ts).event().clone();
            runner.on_event(&event, UnixNanos::from_u64(ts)).unwrap();
            for c in scenario["cancels"].as_array().unwrap() {
                if c[0].as_u64() == Some(ts) {
                    runner.cancel(c[1].as_str().unwrap()).unwrap();
                }
            }
        }
        runner.on_stop().unwrap();

        let expect = &scenario["expect"];
        let rejections: Vec<Value> = runner
            .order_rejections()
            .iter()
            .map(|r| {
                json!([
                    r.order_id.as_str(),
                    r.instrument_id.symbol(),
                    if r.side == OrderSide::Buy {
                        "buy"
                    } else {
                        "sell"
                    },
                    r.quantity,
                    r.reason,
                    r.ts.as_u64(),
                    r.is_cancelled()
                ])
            })
            .collect();
        assert_eq!(
            Value::Array(rejections),
            expect["order_rejections"],
            "{label}: order_rejections"
        );
        let fills: Vec<Value> = runner
            .fills()
            .iter()
            .map(|f| json!([f.order_id().as_str(), f.quantity()]))
            .collect();
        assert_eq!(Value::Array(fills), expect["fills"], "{label}: fills");
        for (symbol, busy) in expect["busy"].as_object().unwrap() {
            assert_eq!(
                Value::Bool(runner.context().busy(&instrument(symbol))),
                *busy,
                "{label}: busy {symbol}"
            );
        }
        let positions = expect["positions"].as_object().unwrap();
        let held: Vec<String> = runner
            .context()
            .positions()
            .iter()
            .map(|(id, _)| id.symbol().to_string())
            .collect();
        assert_eq!(held.len(), positions.len(), "{label}: held instruments");
        for (symbol, qty) in positions {
            assert_eq!(
                runner.context().position(&instrument(symbol)),
                qty.as_f64().unwrap(),
                "{label}: position {symbol}"
            );
        }
    }
}
