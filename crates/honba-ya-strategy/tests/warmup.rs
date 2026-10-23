//! Cross-language vectors for the runner warm-up gate and the strategy manifest.
//!
//! Reads `schema/conformance/warmup_gate.json` and
//! `schema/conformance/strategy_manifest.json`; the Python test
//! `python/tests/integration/test_warmup_conformance.py` reads the same files.

use std::path::PathBuf;

use honba_engine::{ExecutionEngine, Handler, Result};
use honba_entities::Trade;
use honba_messages::{Bar, Event, InstrumentId, Order, UnixNanos};
use honba_strategy::{
    ManifestError, OrderIntent, Strategy, StrategyContext, StrategyManifest, StrategyRunner,
};
use honba_testing::VecFeed;
use honba_testing::fixtures::instrument;
use serde_json::Value;

fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/conformance")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// Buys 1 of the first instrument on start and 1 of the bar's instrument on every bar.
struct WarmupProbe {
    name: String,
    first: InstrumentId,
}

impl Strategy for WarmupProbe {
    fn name(&self) -> &str {
        &self.name
    }
    fn on_start(&mut self, ctx: &mut dyn StrategyContext) -> Result<()> {
        ctx.submit(OrderIntent::market_buy(self.first.clone(), 1.0));
        Ok(())
    }
    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, bar: &Bar) -> Result<()> {
        ctx.submit(OrderIntent::market_buy(
            bar.bar_type().instrument_id().clone(),
            1.0,
        ));
        Ok(())
    }
}

/// Accepts every order, never fills.
struct NeverFills;

impl ExecutionEngine for NeverFills {
    fn submit(&mut self, _order: Order) -> Result<()> {
        Ok(())
    }
    fn cancel(&mut self, _order_id: &str, _now: honba_messages::UnixNanos) -> Result<()> {
        Ok(())
    }
    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }
}

fn event(kind: &str, symbol: &str, ts: u64) -> Event {
    match kind {
        "bar" => VecFeed::bar(symbol, 1.0, ts).event().clone(),
        "quote" => VecFeed::quote(symbol, 1.0, 1.1, ts).event().clone(),
        other => panic!("unknown event kind {other}"),
    }
}

#[test]
fn warmup_gate_vectors() {
    let doc = fixture("warmup_gate.json");
    let name = doc["strategy_name"].as_str().unwrap().to_string();
    for scenario in doc["scenarios"].as_array().unwrap() {
        let label = scenario["name"].as_str().unwrap();
        let events = scenario["events"].as_array().unwrap();
        let first = instrument(events[0][1].as_str().unwrap());
        let warmup = u32::try_from(scenario["warmup_bars"].as_u64().unwrap()).unwrap();
        let strategy = WarmupProbe {
            name: name.clone(),
            first,
        };
        let mut runner = StrategyRunner::new(strategy, NeverFills).with_warmup_bars(warmup);
        runner.on_start().unwrap();
        for e in events {
            let (kind, symbol, ts) = (
                e[0].as_str().unwrap(),
                e[1].as_str().unwrap(),
                e[2].as_u64().unwrap(),
            );
            runner
                .on_event(&event(kind, symbol, ts), UnixNanos::from_u64(ts))
                .unwrap();
        }
        runner.on_stop().unwrap();

        let suppressed: Vec<Value> = runner
            .suppressed()
            .iter()
            .map(|s| serde_json::json!([s.ts_init.as_u64(), s.intent.instrument_id.symbol()]))
            .collect();
        assert_eq!(
            Value::Array(suppressed),
            scenario["suppressed"],
            "{label}: suppressed"
        );
        let submitted: Vec<Value> = runner
            .submitted()
            .iter()
            .map(|s| {
                serde_json::json!([
                    s.ts_init.as_u64(),
                    s.order_id.as_str(),
                    s.intent.instrument_id.symbol()
                ])
            })
            .collect();
        assert_eq!(
            Value::Array(submitted),
            scenario["submitted"],
            "{label}: submitted"
        );
        // Suppressed intents are released: only submitted (unfilled) orders stay busy.
        for e in events {
            let symbol = e[1].as_str().unwrap();
            let busy = scenario["submitted"]
                .as_array()
                .unwrap()
                .iter()
                .any(|s| s[2] == symbol);
            assert_eq!(
                runner.context().busy(&instrument(symbol)),
                busy,
                "{label}: busy {symbol}"
            );
        }
    }
}

fn error_code(e: &ManifestError) -> &'static str {
    match e {
        ManifestError::EmptyName => "empty_name",
        ManifestError::EmptySourceHash => "empty_source_hash",
        ManifestError::UnsupportedApiVersion { .. } => "unsupported_api_version",
        ManifestError::ZeroInterval => "zero_interval",
        ManifestError::NamedUniverseUnresolved => "named_universe_unresolved",
        _ => "unknown",
    }
}

#[test]
fn manifest_vectors() {
    let doc = fixture("strategy_manifest.json");
    for case in doc["cases"].as_array().unwrap() {
        let label = case["name"].as_str().unwrap();
        let parsed = serde_json::from_value::<StrategyManifest>(case["value"].clone());
        match case["error"].as_str() {
            Some("deserialize") => assert!(parsed.is_err(), "{label}: must not deserialize"),
            Some(code) => {
                let m = parsed.unwrap_or_else(|e| panic!("{label}: {e}"));
                let err = m.validate().expect_err(label);
                assert_eq!(error_code(&err), code, "{label}");
            }
            None => {
                let m = parsed.unwrap_or_else(|e| panic!("{label}: {e}"));
                assert_eq!(m.validate(), Ok(()), "{label}");
                // Serializes back to the shared shape (defaults filled, empty schedules omitted).
                let mut want = case["value"].clone();
                let subs = want["subscriptions"].as_object_mut().unwrap();
                subs.entry("quotes").or_insert(Value::Bool(false));
                subs.entry("trades").or_insert(Value::Bool(false));
                assert_eq!(serde_json::to_value(&m).unwrap(), want, "{label}");
            }
        }
    }
}
