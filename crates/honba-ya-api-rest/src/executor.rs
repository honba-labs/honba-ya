//! The [`RunExecutor`] behind `POST /backtests` and `POST /sweeps`.
//!
//! # Functionality gap (read before touching this file)
//!
//! This executor used to run on a synchronous event-kernel: `honba_engine`'s
//! `Engine`/`Handler`/`ExecutionEngine` traits pumped `Event`s through
//! handlers (a `Recorder`, a `Venue` wrapping `honba_sim`'s `BarFillEngine` or
//! `L2Sim`, a `JournalSink`, and `honba-nou-strategy`'s old `StrategyRunner`
//! which was itself built on `Handler`), and `honba_sweep::run` drove the
//! same kernel per trial for a parameter sweep.
//!
//! All three of `honba_engine`, `honba_sim` and `honba_sweep` are gone from
//! this workspace (not renamed — deleted; see `plan.md` Phase 1 pruning).
//! `honba-ya-backtest::sim` only ever absorbed part of `honba-ya-sim` (it has
//! `l2_book::OrderBook`, `latency`, `mis`, `slippage::SlippageModel`, but no
//! `BarFillEngine`/`FillCosts`/`SlippageKind`), and nothing absorbed
//! `honba_sweep` at all. More fundamentally, `honba-nou-strategy::StrategyRunner`
//! — the thing a ported engine would have to drive — was rewritten in the
//! same pruning pass to *not* depend on an `Engine`/`Handler` kernel any more:
//! it now turns a strategy's `OrderIntent`s into plain `OrderRequest`s and
//! hands them back to its host to submit (see
//! `honba-nou-strategy/src/runner.rs`'s module doc, which says in so many
//! words that `honba-engine` "has no replacement in `honba-core`/`honba-mon`").
//!
//! So restoring `POST /backtests` / `POST /sweeps` is not a `honba_messages`
//! rename, and not even a mechanical port to `honba-ya-backtest::sim` — it is
//! writing a new backtest host loop (bar/tick replay, order submission,
//! fill/event dispatch, journal recording) against `StrategyRunner`'s new
//! pull-based shape, plus a parameter-sweep driver with no source to port
//! from at all. That is a genuine feature-rebuild, out of scope for this
//! migration pass. Both routes are real/reachable (registered in
//! `auth.rs::ROUTE_SCOPES` under the `research` scope and wired to this
//! executor in `state.rs`), so the honest state is: they compile and the
//! service starts, but every backtest/sweep job now fails cleanly with
//! `ErrorCode::NotImplemented` instead of silently miscompiling against
//! invented stand-ins for the missing crates.
//!
//! The helpers below that do *not* depend on the dead engine/sim/sweep types
//! (`parse_universe`, `fill_model_from_name`, `parameter_grid_from_value`,
//! `assumptions`) are kept as the reusable, still-correct pieces of a future
//! real port. `merge_events` (which interleaved bars and ticks into the old
//! `Event` enum the deleted kernel pumped) had no honest replacement — there
//! is no event enum spanning both payload types any more — so it was dropped
//! rather than ported to something that only looked equivalent.

use std::collections::BTreeMap;
use std::sync::Arc;

use honba_api::{
    ErrorCode, ErrorDetail, JournalWriter, ResolvedBacktest, ResolvedRequest, ResultCache,
    RunExecutor, RunJob, RunOutcome, parse_instrument_id,
};
use honba_config::{AccountConfig, ExecutionConfig, FillModel};
use honba_core::rust_decimal::Decimal;
use honba_core::{BarReader, Currency, InstrumentId, PortError, TickReader};
use serde_json::{Value, json};

use crate::registry::StrategyRegistry;

/// Runs a resolved backtest or sweep job.
///
/// # Functionality gap
///
/// See the module doc: the synchronous kernel (`honba_engine`), the fill
/// simulators (`honba_sim`), and the sweep driver (`honba_sweep`) this used to
/// run on are gone with no replacement, so [`RunExecutor::execute`] below
/// refuses every job with `ErrorCode::NotImplemented` rather than guessing at
/// a substitute engine. The fields are kept (and still accepted via the
/// builder methods) so `state.rs`'s composition root keeps compiling and
/// gains them back for free once a real port lands.
pub struct BacktestExecutor {
    #[allow(dead_code)]
    bars: Arc<dyn BarReader + Send + Sync>,
    #[allow(dead_code)]
    registry: StrategyRegistry,
    #[allow(dead_code)]
    account: AccountConfig,
    #[allow(dead_code)]
    execution: ExecutionConfig,
    #[allow(dead_code)]
    cache: Option<ResultCache>,
    #[allow(dead_code)]
    ticks: Option<Arc<dyn TickReader + Send + Sync>>,
}

impl BacktestExecutor {
    /// An executor over `bars` running the strategies in `registry`, with the default
    /// account and execution config.
    pub fn new(bars: Arc<dyn BarReader + Send + Sync>, registry: StrategyRegistry) -> Self {
        Self {
            bars,
            registry,
            account: AccountConfig::default(),
            execution: ExecutionConfig::default(),
            cache: None,
            ticks: None,
        }
    }

    /// Overrides the account config (currency).
    #[must_use]
    pub fn with_account(mut self, account: AccountConfig) -> Self {
        self.account = account;
        self
    }

    /// Overrides the execution config (fill model).
    #[must_use]
    pub fn with_execution(mut self, execution: ExecutionConfig) -> Self {
        self.execution = execution;
        self
    }

    /// Attaches the quote/trade tick source `fill_model = l2` runs would match against.
    #[must_use]
    pub fn with_tick_reader(mut self, ticks: Arc<dyn TickReader + Send + Sync>) -> Self {
        self.ticks = Some(ticks);
        self
    }

    /// Attaches a result cache to this executor (ADR 0020, E4-S1).
    #[must_use]
    pub fn with_cache(mut self, cache: ResultCache) -> Self {
        self.cache = Some(cache);
        self
    }

    pub(crate) fn account_currency(&self) -> Result<Currency, ErrorDetail> {
        match self.account.currency.as_str() {
            "INR" => Ok(Currency::Inr),
            other => Err(ErrorDetail::new(
                ErrorCode::NotImplemented,
                format!("account currency {other:?} is not supported"),
            )),
        }
    }
}

impl RunExecutor for BacktestExecutor {
    fn execute(
        &self,
        job: RunJob,
        _journal: &mut dyn JournalWriter,
    ) -> Result<RunOutcome, ErrorDetail> {
        // See the module doc: there is no engine/sim/sweep kernel left to run this on.
        let (kind, _) = match &job.request {
            ResolvedRequest::Backtest(spec) => ("backtest", &spec.universe),
            ResolvedRequest::Sweep(spec) => ("sweep", &spec.universe),
        };
        Err(ErrorDetail::new(
            ErrorCode::NotImplemented,
            format!(
                "{kind} execution is unavailable: honba_engine/honba_sim/honba_sweep were \
                 removed from the workspace with no replacement kernel; see executor.rs's \
                 module doc"
            ),
        )
        .with_context(json!({"reason": "engine_kernel_removed", "run_kind": kind})))
    }
}

fn invalid(field: &str, reason: &str, message: impl Into<String>) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::ValidationInvalidRequest, message)
        .with_context(json!({"field": field, "reason": reason}))
}

/// Maps a request's `fill_model` name to the config enum, through serde so the accepted
/// spellings are the enum's own (pinned to `honba_api::FILL_MODELS` by a test).
#[allow(dead_code)]
fn fill_model_from_name(name: &str) -> Result<FillModel, ErrorDetail> {
    serde_json::from_value::<FillModel>(serde_json::Value::String(name.to_string())).map_err(|_| {
        invalid(
            "fill_model",
            "unknown_fill_model",
            format!("unknown fill_model {name:?}"),
        )
    })
}

#[allow(dead_code)]
fn port_error(e: &PortError) -> ErrorDetail {
    let code = match e {
        PortError::Unsupported(_) => ErrorCode::NotImplemented,
        PortError::NotFound(_) => ErrorCode::NotFound,
        PortError::Timeout => ErrorCode::Timeout,
        PortError::ConnectionFailed(_) | PortError::CalculationFailed(_) => {
            ErrorCode::InternalError
        }
    };
    ErrorDetail::new(code, format!("bar read failed: {e}"))
        .with_context(json!({"reason": "bar_read"}))
}

/// The one instrument of a universe. A list or a bare symbol is refused.
#[allow(dead_code)]
pub(crate) fn parse_universe(universe: &str) -> Result<InstrumentId, ErrorDetail> {
    let bad = |why: &str| {
        invalid(
            "universe",
            "unsupported_universe",
            format!("universe must be one SYMBOL.EXCHANGE instrument: {why}"),
        )
    };
    let text = universe.trim();
    if text.is_empty() || text.contains(',') || text.contains(char::is_whitespace) {
        return Err(bad("lists are not supported"));
    }
    parse_instrument_id(text).map_err(|_| bad("not SYMBOL.EXCHANGE"))
}

/// Annualisation that would be used for the Sharpe ratio, kept for the future real port.
#[allow(dead_code)]
const PERIODS_PER_YEAR: f64 = 252.0;

#[allow(dead_code)]
fn assumptions(spec: &ResolvedBacktest) -> Value {
    let mut not_modelled: Vec<&str> = Vec::new();
    if spec.commission_per_fill == Decimal::ZERO {
        not_modelled.push("transaction_costs");
    }
    if spec.slippage_bps == Decimal::ZERO {
        not_modelled.push("slippage");
    }
    not_modelled.extend([
        "taxes_and_levies",
        "market_impact",
        "partial_fills",
        "order_acknowledgement_latency",
        "mark_to_market_of_open_positions",
        "corporate_actions",
        "market_calendar",
    ]);
    json!({
        "not_modelled": not_modelled,
        "applied": {
            "commission_per_fill": spec.commission_per_fill,
            "slippage_bps": spec.slippage_bps,
        },
        "timing": "market orders fill at the close of the bar on which the strategy decided",
        "fill_model": "bar_fill",
        "metrics_basis": "closed_round_trips",
        "periods_per_year": PERIODS_PER_YEAR
    })
}

/// Converts a JSON parameter object (name -> [low, high, step]) into a Cartesian grid of trial params.
#[allow(dead_code)]
fn parameter_grid_from_value(params_obj: &Value) -> Result<Vec<BTreeMap<String, Value>>, String> {
    let obj = params_obj
        .as_object()
        .ok_or("params must be a JSON object")?;
    let mut space: BTreeMap<String, Vec<Value>> = BTreeMap::new();

    for (name, range_val) in obj {
        let range = range_val
            .as_array()
            .ok_or(format!("{name}: range must be an array"))?;
        if range.len() != 3 {
            return Err(format!("{name}: range must be [low, high, step]"));
        }
        let low = range[0]
            .as_f64()
            .ok_or(format!("{name}: low must be a number"))?;
        let high = range[1]
            .as_f64()
            .ok_or(format!("{name}: high must be a number"))?;
        let step = range[2]
            .as_f64()
            .ok_or(format!("{name}: step must be a number"))?;
        if low > high {
            return Err(format!("{name}: low cannot exceed high"));
        }
        if step <= 0.0 {
            return Err(format!("{name}: step must be positive"));
        }

        let is_int = low.fract() == 0.0 && high.fract() == 0.0 && step.fract() == 0.0;
        let mut values = Vec::new();
        let mut curr = low;
        while curr <= high + step * 1e-6 {
            if is_int {
                values.push(Value::from(curr.round() as i64));
            } else {
                values.push(Value::from((curr * 1e6).round() / 1e6));
            }
            curr += step;
        }
        if values.is_empty() {
            return Err(format!("{name}: range produced no values"));
        }
        space.insert(name.clone(), values);
    }

    if space.is_empty() {
        return Ok(Vec::new());
    }

    let mut grid: Vec<BTreeMap<String, Value>> = vec![BTreeMap::new()];
    for (name, values) in space {
        let mut next = Vec::new();
        for entry in &grid {
            for v in &values {
                let mut updated = entry.clone();
                updated.insert(name.clone(), v.clone());
                next.push(updated);
            }
        }
        grid = next;
    }
    Ok(grid)
}
