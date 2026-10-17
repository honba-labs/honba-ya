use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use honba_analytics::{EquityStats, RoundTrip, TradeStats};
use honba_api::{
    parse_instrument_id, parse_timeframe, BacktestMetrics, CacheKey, CachedResult, JournalWriter,
    ResolvedBacktest, ResolvedRequest, ResultCache, RunExecutor, RunJob, RunOutcome,
};
use honba_config::{AccountConfig, ExecutionConfig, FillModel};
use honba_engine::{
    AlgoError, Engine, EngineOutput, ExecutionEngine, Handler, Result as EngineResult,
};
use honba_entities::{Currency, ExecutionEvent, Money, Trade};
use honba_messages::{
    ErrorCode, ErrorDetail, Event, InstrumentId, Message, Order, OrderStatus, UnixNanos,
};
use honba_ports::{BarReader, BarRequest, PortError, TickReader, TickRequest};
use honba_sim::{BarFillEngine, FillCosts, L2Sim, SlippageKind, SlippageModel};
use honba_strategy::{DynStrategy, LedgerContext, StrategyRunner};
use serde_json::{json, Value};

use crate::registry::StrategyRegistry;
use honba_api::SweepReportResponse;
use honba_sweep::{
    run, CalmarFitness, Fitness, OmegaFitness, SharpeFitness, SortinoFitness, StrategySpec,
    SweepPlan, SweepReport, TrialParams,
};

/// A strategy factory that builds named strategies with parameter overrides.
#[allow(dead_code)]
struct CustomStrategyFactory {
    base_params: BTreeMap<String, Value>,
}

impl honba_sweep::StrategyFactory for CustomStrategyFactory {
    fn build(&self, seed: u64) -> Box<dyn honba_strategy::Strategy> {
        let _ = seed;
        // The registry knows how to build the named strategy with fixed parameters.
        // For sma_crossover, the parameters are fast, slow, quantity.
        // We'll use the registry to build the base strategy.
        // This factory is just a placeholder since the registry handles named strategies
        Box::new(honba_strategy::SmaCrossover::new(
            honba_messages::InstrumentId::new("DUMMY", honba_messages::Exchange::new("DUMMY")),
            1,
            2,
            1.0,
        ))
    }
}

/// Converts a JSON parameter object (name -> [low, high, step]) into a Cartesian grid of trial params.
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

/// Annualisation used for the Sharpe ratio; stated in the outcome's assumptions.
const PERIODS_PER_YEAR: f64 = 252.0;

/// Records appended between journal flushes, so a reader sees a growing prefix.
const FLUSH_EVERY: usize = 256;

/// Runs a resolved backtest with the synchronous kernel: `Engine` + `StrategyRunner` +
/// `BarFillEngine`, bars read through the [`BarReader`] port.
///
/// It owns no threads: a worker calls [`RunExecutor::execute`] on its own thread, which must
/// not be a tokio runtime thread (the bar read blocks on a private current-thread runtime).
pub struct BacktestExecutor {
    bars: Arc<dyn BarReader>,
    registry: StrategyRegistry,
    account: AccountConfig,
    execution: ExecutionConfig,
    cache: Option<ResultCache>,
    ticks: Option<Arc<dyn TickReader>>,
}

impl BacktestExecutor {
    /// An executor over `bars` running the strategies in `registry`, with the default
    /// account and execution config.
    pub fn new(bars: Arc<dyn BarReader>, registry: StrategyRegistry) -> Self {
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

    /// Attaches the quote/trade tick source `fill_model = l2` runs match against (ADR 0030).
    ///
    /// Without one, an `l2` run is refused with `reason = "l2_requires_ticks"`. A `bar_fill` run
    /// never reads it.
    #[must_use]
    pub fn with_tick_reader(mut self, ticks: Arc<dyn TickReader>) -> Self {
        self.ticks = Some(ticks);
        self
    }

    /// Attaches a result cache to this executor (ADR 0020, E4-S1).
    #[must_use]
    pub fn with_cache(mut self, cache: ResultCache) -> Self {
        self.cache = Some(cache);
        self
    }
}

fn invalid(field: &str, reason: &str, message: impl Into<String>) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::ValidationInvalidRequest, message)
        .with_context(json!({"field": field, "reason": reason}))
}

fn internal(reason: &str, message: impl Into<String>) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::InternalError, message).with_context(json!({"reason": reason}))
}

/// Maps a request's `fill_model` name to the config enum, through serde so the accepted
/// spellings are the enum's own (pinned to `honba_api::FILL_MODELS` by a test). Submit
/// validation already refused an unknown name, so this is defensive.
fn fill_model_from_name(name: &str) -> Result<FillModel, ErrorDetail> {
    serde_json::from_value::<FillModel>(serde_json::Value::String(name.to_string())).map_err(|_| {
        invalid(
            "fill_model",
            "unknown_fill_model",
            format!("unknown fill_model {name:?}"),
        )
    })
}

fn port_error(e: &PortError) -> ErrorDetail {
    let code = match e {
        PortError::Unsupported(_) => ErrorCode::Unsupported,
        PortError::InvalidRequest(_) => ErrorCode::ValidationInvalidRequest,
        PortError::Unavailable(_) | PortError::Timeout | PortError::Transport(_) => {
            ErrorCode::MarketDataUnavailable
        }
        _ => ErrorCode::InternalError,
    };
    ErrorDetail::new(code, format!("bar read failed: {e}"))
        .with_context(json!({"reason": "bar_read"}))
}

fn engine_error(e: &AlgoError) -> ErrorDetail {
    internal("engine", format!("backtest failed: {e}"))
}

/// The one instrument of a universe. A list or a bare symbol is refused.
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

// ---------------------------------------------------------------------------------------
// The tape: what the journal records, in kernel processing order.
// ---------------------------------------------------------------------------------------

#[derive(Default)]
struct Tape {
    records: Vec<Message>,
    now: UnixNanos,
}

type SharedTape = Arc<Mutex<Tape>>;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// First handler: records every message the engine dispatches.
struct Recorder(SharedTape);

impl Handler for Recorder {
    fn on_event(&mut self, event: &Event, ts_init: UnixNanos) -> EngineResult<EngineOutput> {
        let mut tape = lock(&self.0);
        tape.now = event.ts_event();
        // Ticks are replay input read from the tick store, not decisions or outcomes: they
        // advance the clock but stay out of the journal.
        if !matches!(event, Event::Quote(_) | Event::Trade(_)) {
            tape.records.push(Message::new(event.clone(), ts_init));
        }
        Ok(EngineOutput::None)
    }
}

/// The simulator behind the sink: bar fills, or the L2 matcher over ticks (ADR 0030).
#[derive(Clone)]
enum Venue {
    Bar(BarFillEngine),
    L2(Arc<Mutex<L2Sim>>),
}

impl Handler for Venue {
    fn on_start(&mut self) -> EngineResult<()> {
        match self {
            Self::Bar(e) => e.on_start(),
            Self::L2(e) => lock(e).on_start(),
        }
    }

    fn on_event(&mut self, event: &Event, ts_init: UnixNanos) -> EngineResult<EngineOutput> {
        match self {
            Self::Bar(e) => e.on_event(event, ts_init),
            Self::L2(e) => lock(e).on_event(event, ts_init),
        }
    }

    fn on_stop(&mut self) -> EngineResult<()> {
        match self {
            Self::Bar(e) => e.on_stop(),
            Self::L2(e) => lock(e).on_stop(),
        }
    }
}

impl ExecutionEngine for Venue {
    fn submit(&mut self, order: Order) -> EngineResult<()> {
        match self {
            Self::Bar(e) => e.submit(order),
            Self::L2(e) => lock(e).submit(order),
        }
    }

    fn cancel(&mut self, order_id: &str, now: UnixNanos) -> EngineResult<()> {
        match self {
            Self::Bar(e) => e.cancel(order_id, now),
            Self::L2(e) => lock(e).cancel(order_id, now),
        }
    }

    fn drain_events(&mut self) -> EngineResult<Vec<ExecutionEvent>> {
        match self {
            Self::Bar(e) => e.drain_events(),
            Self::L2(e) => lock(e).drain_events(),
        }
    }

    fn native_events(&self) -> bool {
        true
    }

    fn drain_fills(&mut self) -> EngineResult<Vec<Trade>> {
        match self {
            Self::Bar(e) => e.drain_fills(),
            Self::L2(e) => lock(e).drain_fills(),
        }
    }
}

/// The paper sink: a [`Venue`] that tees what the runner submits and what comes
/// back (as ADR 0019 wire events) onto the tape, and the fills onto the fill list.
#[derive(Clone)]
struct JournalSink {
    inner: Venue,
    tape: SharedTape,
    fills: Arc<Mutex<Vec<Trade>>>,
}

impl JournalSink {
    fn wire(&self, event: &ExecutionEvent, now: UnixNanos) -> Option<Event> {
        Some(match event {
            ExecutionEvent::Accepted {
                order_id,
                venue_order_id,
                ..
            } => Event::OrderAccepted {
                order_id: order_id.clone(),
                venue_order_id: venue_order_id.clone(),
                ts_event: now,
            },
            ExecutionEvent::Rejected {
                order_id, reason, ..
            } => Event::OrderRejected {
                order_id: order_id.clone(),
                reason: reason.clone(),
                ts_event: now,
            },
            ExecutionEvent::Fill {
                trade,
                complete: true,
                ..
            } => Event::OrderFilled {
                order_id: trade.order_id().clone(),
                last_qty: trade.quantity(),
                last_px: trade.price(),
                ts_event: now,
            },
            ExecutionEvent::Fill { trade, cum_qty, .. } => Event::OrderPartiallyFilled {
                order_id: trade.order_id().clone(),
                last_qty: trade.quantity(),
                last_px: trade.price(),
                cum_qty: *cum_qty,
                ts_event: now,
            },
            ExecutionEvent::CancelRequested { order_id, .. } => Event::OrderCancelRequested {
                order_id: order_id.clone(),
                ts_event: now,
            },
            ExecutionEvent::Cancelled { order_id, .. } => Event::OrderCancelled {
                order_id: order_id.clone(),
                ts_event: now,
            },
            ExecutionEvent::Expired { order_id, .. } => Event::OrderExpired {
                order_id: order_id.clone(),
                ts_event: now,
            },
            // `Submitted` is recorded as the order itself, at submit.
            _ => return None,
        })
    }
}

impl Handler for JournalSink {
    fn on_start(&mut self) -> EngineResult<()> {
        self.inner.on_start()
    }

    fn on_event(&mut self, event: &Event, ts_init: UnixNanos) -> EngineResult<EngineOutput> {
        self.inner.on_event(event, ts_init)
    }

    fn on_stop(&mut self) -> EngineResult<()> {
        self.inner.on_stop()
    }
}

impl ExecutionEngine for JournalSink {
    fn submit(&mut self, order: Order) -> EngineResult<()> {
        let now = lock(&self.tape).now;
        // Never earlier than the kernel clock, like `Engine`'s own `Order` record.
        let recorded = if order.ts_event() < now {
            let mut o = Order::new(
                order.order_id().clone(),
                order.instrument_id().clone(),
                order.side(),
                order.order_type(),
                order.quantity(),
                order.price(),
                order.time_in_force(),
                now,
                order.ts_init(),
            );
            if let Some(trigger) = order.trigger_price() {
                o = o.with_trigger_price(trigger);
            }
            o
        } else {
            order.clone()
        };
        lock(&self.tape).records.push(Message::new(
            Event::Order(recorded.with_status(OrderStatus::Submitted)),
            now,
        ));
        self.inner.submit(order)
    }

    fn cancel(&mut self, order_id: &str, now: UnixNanos) -> EngineResult<()> {
        self.inner.cancel(order_id, now)
    }

    fn drain_events(&mut self) -> EngineResult<Vec<ExecutionEvent>> {
        let events = self.inner.drain_events()?;
        let now = lock(&self.tape).now;
        for event in &events {
            if let ExecutionEvent::Fill { trade, .. } = event {
                lock(&self.fills).push(trade.clone());
            }
            if let Some(wire) = self.wire(event, now) {
                lock(&self.tape).records.push(Message::new(wire, now));
            }
        }
        Ok(events)
    }

    fn native_events(&self) -> bool {
        true
    }

    fn drain_fills(&mut self) -> EngineResult<Vec<Trade>> {
        self.inner.drain_fills()
    }
}

// ---------------------------------------------------------------------------------------
// Metrics.
// ---------------------------------------------------------------------------------------

/// Pairs fills two at a time (entry, exit) in fill order; a trailing open fill is dropped.
fn round_trips(fills: &[Trade]) -> Vec<RoundTrip> {
    fills
        .chunks(2)
        .filter_map(|pair| match pair {
            [entry, exit] => RoundTrip::from_fills(entry, exit).ok(),
            _ => None,
        })
        .collect()
}

fn metrics(fills: &[Trade], initial_capital: f64) -> Result<BacktestMetrics, ErrorDetail> {
    let trips = round_trips(fills);
    if trips.is_empty() {
        return Ok(BacktestMetrics::default());
    }
    let mut equity = initial_capital;
    let returns: Vec<f64> = trips
        .iter()
        .map(|trip| {
            let before = equity;
            equity += trip.net_pnl;
            if before == 0.0 {
                0.0
            } else {
                (equity - before) / before
            }
        })
        .collect();
    let analytics = |e: honba_analytics::AnalyticsError| internal("analytics", e.to_string());
    let trade_stats = TradeStats::from_round_trips(&trips).map_err(analytics)?;
    let equity_stats =
        EquityStats::from_returns(&returns, PERIODS_PER_YEAR, 0.0).map_err(analytics)?;
    Ok(BacktestMetrics {
        trades: trips.len() as u64,
        net_pnl: trade_stats.total_pnl,
        sharpe: equity_stats.sharpe.unwrap_or(0.0),
        max_drawdown: equity_stats.max_drawdown_pct,
        total_return: trade_stats.total_pnl / initial_capital,
    })
}

fn assumptions(spec: &ResolvedBacktest) -> Value {
    let mut not_modelled: Vec<&str> = Vec::new();
    if spec.commission_per_fill == 0.0 {
        not_modelled.push("transaction_costs");
    }
    if spec.slippage_bps == 0.0 {
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

// ---------------------------------------------------------------------------------------
// The run.
// ---------------------------------------------------------------------------------------

/// Bars and ticks in `ts_event` order: stable, and at equal time the ticks come first (the
/// Python `BacktestSession` rule, ADR 0030). `ticks` must be time-sorted.
pub(crate) fn merge_events(bars: Vec<honba_messages::Bar>, ticks: Vec<Event>) -> Vec<Event> {
    if ticks.is_empty() {
        return bars.into_iter().map(Event::Bar).collect();
    }
    let mut out = Vec::with_capacity(bars.len() + ticks.len());
    let mut ticks = ticks.into_iter().peekable();
    for bar in bars {
        while let Some(tick) = ticks.next_if(|t| t.ts_event() <= bar.ts_event()) {
            out.push(tick);
        }
        out.push(Event::Bar(bar));
    }
    out.extend(ticks);
    out
}

fn drain_tape(
    tape: &SharedTape,
    journal: &mut dyn JournalWriter,
    unflushed: &mut usize,
) -> Result<(), ErrorDetail> {
    let records = std::mem::take(&mut lock(tape).records);
    for record in &records {
        journal.append(record)?;
    }
    *unflushed += records.len();
    if *unflushed >= FLUSH_EVERY {
        journal.flush()?;
        *unflushed = 0;
    }
    Ok(())
}

impl BacktestExecutor {
    /// The registered name to run: the job's own id, or the name of its catalog IR.
    fn registered_name(&self, job: &RunJob) -> Result<&'static str, ErrorDetail> {
        [
            job.strategy_id.as_str(),
            job.strategy.manifest.name.as_str(),
        ]
        .into_iter()
        .find_map(|name| {
            if name == "sma_cross" {
                return Some("sma_crossover");
            }
            self.registry.names().into_iter().find(|n| *n == name)
        })
        .ok_or_else(|| {
            invalid(
                "strategy",
                "no_rust_implementation",
                "strategy has no registered Rust implementation",
            )
        })
    }

    pub(crate) fn account_currency(&self) -> Result<Currency, ErrorDetail> {
        match self.account.currency.as_str() {
            "INR" => Ok(Currency::Inr),
            other => Err(ErrorDetail::new(
                ErrorCode::Unsupported,
                format!("account currency {other:?} is not supported"),
            )),
        }
    }

    fn read_bars_for_sweep(
        &self,
        instrument: &InstrumentId,
        spec: &honba_api::ResolvedSweep,
    ) -> Result<Vec<honba_messages::Bar>, ErrorDetail> {
        let bar_spec = parse_timeframe(&spec.bar_spec)
            .map_err(|e| invalid("bar_spec", "invalid_bar_spec", e.message))?;
        let request = BarRequest::new(
            instrument.clone(),
            bar_spec,
            Some(spec.start),
            Some(spec.end),
        )
        .map_err(|e| port_error(&e))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|e| internal("runtime", format!("cannot start a reader runtime: {e}")))?;
        let bars = runtime
            .block_on(self.bars.read_bars(&request))
            .map_err(|e| port_error(&e))?;
        if bars.is_empty() {
            return Err(ErrorDetail::new(
                ErrorCode::MarketDataUnavailable,
                "no bars in the window",
            )
            .with_context(json!({"reason": "no_data"})));
        }
        Ok(bars)
    }

    fn read_bars(
        &self,
        instrument: &InstrumentId,
        spec: &ResolvedBacktest,
    ) -> Result<Vec<honba_messages::Bar>, ErrorDetail> {
        let bar_spec = parse_timeframe(&spec.bar_spec)
            .map_err(|e| invalid("bar_spec", "invalid_bar_spec", e.message))?;
        let request = BarRequest::new(
            instrument.clone(),
            bar_spec,
            Some(spec.start),
            Some(spec.end),
        )
        .map_err(|e| port_error(&e))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|e| internal("runtime", format!("cannot start a reader runtime: {e}")))?;
        let bars = runtime
            .block_on(self.bars.read_bars(&request))
            .map_err(|e| port_error(&e))?;
        if bars.is_empty() {
            return Err(ErrorDetail::new(
                ErrorCode::MarketDataUnavailable,
                "no bars in the window",
            )
            .with_context(json!({"reason": "no_data"})));
        }
        Ok(bars)
    }

    /// Quote and trade ticks of the window as replay events, quotes before trades at one
    /// instant. `l2_requires_ticks` when there is no source or nothing in the window.
    fn read_ticks(
        &self,
        instrument: &InstrumentId,
        spec: &ResolvedBacktest,
    ) -> Result<Vec<Event>, ErrorDetail> {
        let refused = |why: &str| {
            ErrorDetail::new(
                ErrorCode::Unsupported,
                format!("fill_model l2 requires quote/trade ticks; {why}"),
            )
            .with_context(json!({"reason": "l2_requires_ticks", "fill_model": "l2"}))
        };
        let Some(reader) = &self.ticks else {
            return Err(refused("this run's data source supplies bars only"));
        };
        let request = TickRequest::new(instrument.clone(), Some(spec.start), Some(spec.end))
            .map_err(|e| port_error(&e))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|e| internal("runtime", format!("cannot start a reader runtime: {e}")))?;
        let tick_err = |e: PortError| match e {
            PortError::Unsupported(_) => refused("the tick source holds no tick data"),
            other => ErrorDetail::new(
                port_error(&other).code,
                format!("tick read failed: {other}"),
            )
            .with_context(json!({"reason": "tick_read"})),
        };
        let quotes = runtime
            .block_on(reader.read_quotes(&request))
            .map_err(tick_err)?;
        let trades = runtime
            .block_on(reader.read_trades(&request))
            .map_err(tick_err)?;
        if quotes.is_empty() && trades.is_empty() {
            return Err(refused("the tick source has none in this run's window"));
        }
        let mut ticks: Vec<Event> = quotes
            .into_iter()
            .map(Event::Quote)
            .chain(trades.into_iter().map(Event::Trade))
            .collect();
        // Stable: at one instant the quotes (the book) stay ahead of the trades (the prints).
        ticks.sort_by_key(Event::ts_event);
        Ok(ticks)
    }

    fn backtest(
        &self,
        job: &RunJob,
        spec: &ResolvedBacktest,
        journal: &mut dyn JournalWriter,
    ) -> Result<RunOutcome, ErrorDetail> {
        if job.seed == 0 {
            return Err(invalid("seed", "zero_seed", "seed must be non-zero"));
        }
        let dataset_id = format!(
            "{}:{}:{}:{}",
            spec.universe,
            spec.bar_spec,
            spec.start.as_u64(),
            spec.end.as_u64()
        );
        let params = json!({
            "seed": job.seed,
            "initial_capital": spec.initial_capital,
            "commission_per_fill": spec.commission_per_fill,
            "slippage_bps": spec.slippage_bps,
        });
        let fill_model = match spec.fill_model.as_deref() {
            None => self.execution.fill_model,
            Some(name) => fill_model_from_name(name)?,
        };
        let cost_model_version = match fill_model {
            FillModel::BarFill => "bar_fill:v1",
            _ => "custom:v1",
        };
        let cache_key =
            CacheKey::compute(&job.strategy_id, &params, &dataset_id, cost_model_version);

        let is_l2 = matches!(fill_model, FillModel::L2);
        // The key names bars and a bar-fill model only; an l2 run also depends on its ticks,
        // so it neither reads nor writes the cache.
        let cache = if is_l2 { None } else { self.cache.as_ref() };
        if let Some(cache) = cache {
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(RunOutcome::Backtest {
                    metrics: cached.metrics,
                    assumptions: cached.assumptions,
                });
            }
        }
        if !is_l2 && !matches!(fill_model, FillModel::BarFill | FillModel::BarFillThrough | FillModel::None) {
            return Err(ErrorDetail::new(
                ErrorCode::Unsupported,
                "only the bar_fill, bar_fill_through, and none models are available",
            ));
        }
        let currency = self.account_currency()?;
        let name = self.registered_name(job)?;
        let instrument = parse_universe(&spec.universe)?;
        let ticks = if is_l2 {
            self.read_ticks(&instrument, spec)?
        } else {
            Vec::new()
        };
        let bars = self.read_bars(&instrument, spec)?;
        let strategy = self
            .registry
            .build(name, &instrument, job.seed)
            .ok_or_else(|| internal("registry", "registered strategy failed to build"))?;
        let cash = Money::from_major_f64(spec.initial_capital, currency)
            .map_err(|e| invalid("initial_capital", "not_representable", e.to_string()))?;

        let tape: SharedTape = Arc::default();
        let venue = if is_l2 {
            let sim = L2Sim::new(cash).map_err(|e| engine_error(&e))?;
            Venue::L2(Arc::new(Mutex::new(sim)))
        } else {
            let bar = if spec.commission_per_fill > 0.0 {
                let costs = FillCosts::new(spec.commission_per_fill, 0.0)
                    .map_err(|e| invalid("commission_per_fill", "out_of_range", e.to_string()))?;
                BarFillEngine::with_costs(costs)
            } else {
                BarFillEngine::new()
            }
            .with_currency(currency);
            let bar = if spec.slippage_bps > 0.0 {
                bar.with_slippage(SlippageModel::Static {
                    bps: spec.slippage_bps,
                    kind: SlippageKind::Adverse,
                })
            } else {
                bar
            };
            Venue::Bar(bar)
        };
        let sink = JournalSink {
            inner: venue,
            tape: Arc::clone(&tape),
            fills: Arc::default(),
        };
        let runner = StrategyRunner::with_context(
            DynStrategy::new(strategy),
            sink.clone(),
            LedgerContext::with_cash(cash),
        )
        .with_warmup_bars(job.strategy.warmup_bars);

        let mut engine = Engine::new();
        engine.add_handler(Recorder(Arc::clone(&tape)));
        engine.add_handler(sink.clone());
        engine.add_handler(runner);

        // Taken from the data and nothing else, like every `honba-sweep` trial.
        let ts_init = bars[0].ts_event();
        let mut unflushed = 0;
        engine.start().map_err(|e| engine_error(&e))?;
        for event in merge_events(bars, ticks) {
            engine.inject(Message::new(event, ts_init));
            while engine.pump().map_err(|e| engine_error(&e))? {}
            drain_tape(&tape, journal, &mut unflushed)?;
        }
        engine.finish().map_err(|e| engine_error(&e))?;
        drain_tape(&tape, journal, &mut unflushed)?;
        journal.flush()?;

        let fills = std::mem::take(&mut *lock(&sink.fills));
        let outcome = RunOutcome::Backtest {
            metrics: metrics(&fills, spec.initial_capital)?,
            assumptions: assumptions(spec),
        };
        if let Some(cache) = cache {
            if let RunOutcome::Backtest {
                ref metrics,
                ref assumptions,
            } = outcome
            {
                cache.insert(
                    cache_key,
                    CachedResult {
                        metrics: metrics.clone(),
                        assumptions: assumptions.clone(),
                        journal_path: None,
                    },
                );
            }
        }
        Ok(outcome)
    }
}

impl RunExecutor for BacktestExecutor {
    fn execute(
        &self,
        job: RunJob,
        _journal: &mut dyn JournalWriter,
    ) -> Result<RunOutcome, ErrorDetail> {
        match &job.request {
            ResolvedRequest::Backtest(spec) => self.backtest(&job, spec, _journal),
            ResolvedRequest::Sweep(spec) => self.sweep(&job, spec, _journal),
        }
    }
}

impl BacktestExecutor {
    fn sweep(
        &self,
        job: &RunJob,
        spec: &honba_api::ResolvedSweep,
        _journal: &mut dyn JournalWriter,
    ) -> Result<RunOutcome, ErrorDetail> {
        let _dataset_id = format!("{}:{}:{}:{}", spec.universe, "1d", 0, 0);
        let _cost_model_version = match self.execution.fill_model {
            FillModel::BarFill => "bar_fill:v1",
            _ => "custom:v1",
        };
        let fitness: Arc<dyn Fitness> = match spec.params.get("fitness").and_then(|v| v.as_str()) {
            Some("calmar") => Arc::new(CalmarFitness),
            Some("sortino") => Arc::new(SortinoFitness),
            Some("omega") => Arc::new(OmegaFitness),
            _ => Arc::new(SharpeFitness),
        };

        let grid = parameter_grid_from_value(&spec.params)
            .map_err(|e| invalid("params", "invalid_grid", e.to_string()))?;

        if grid.is_empty() {
            return Err(invalid(
                "params",
                "empty_grid",
                "parameter grid produced no trials",
            ));
        }

        if grid.len() != spec.trials as usize {
            return Err(invalid(
                "trials",
                "mismatch",
                format!(
                    "trials={} but parameter grid has {} combinations",
                    spec.trials,
                    grid.len()
                ),
            ));
        }

        let strategy_name = job.strategy_id.clone();
        let instrument = job
            .strategy
            .universe
            .instruments
            .first()
            .cloned()
            .unwrap_or_else(|| {
                honba_messages::InstrumentId::new("TCS", honba_messages::Exchange::new("NSE"))
            });
        let trials: Vec<TrialParams> = grid
            .into_iter()
            .enumerate()
            .map(|(i, params)| {
                let spec = match strategy_name.as_str() {
                    "sma_crossover" => {
                        let fast =
                            params.get("fast").and_then(|v| v.as_u64()).unwrap_or(3) as usize;
                        let slow =
                            params.get("slow").and_then(|v| v.as_u64()).unwrap_or(8) as usize;
                        let quantity = params
                            .get("quantity")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(10.0);
                        StrategySpec::SmaCrossover {
                            instrument: instrument.clone(),
                            fast,
                            slow,
                            quantity,
                        }
                    }
                    "rsi_reversal" => {
                        let period =
                            params.get("period").and_then(|v| v.as_u64()).unwrap_or(14) as usize;
                        let oversold = params
                            .get("oversold")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(30.0);
                        let overbought = params
                            .get("overbought")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(70.0);
                        let quantity = params
                            .get("quantity")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(10.0);
                        StrategySpec::RsiReversal {
                            instrument: instrument.clone(),
                            period,
                            oversold,
                            overbought,
                            quantity,
                        }
                    }
                    "buy_and_hold" => {
                        let quantity = params
                            .get("quantity")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(10.0);
                        StrategySpec::BuyAndHold {
                            instrument: instrument.clone(),
                            quantity,
                        }
                    }
                    _ => StrategySpec::Custom {
                        inner: Arc::new(CustomStrategyFactory {
                            base_params: params.clone(),
                        }),
                    },
                };
                TrialParams {
                    seed: job.seed + i as u64,
                    spec,
                }
            })
            .collect();

        let plan = SweepPlan::new(trials, fitness)
            .with_initial_cash(Money::from_major_f64(1_000_000.0, Currency::Inr).unwrap())
            .with_max_concurrency(4);

        // Sweep needs a Dataset, not just a BarReader. Build one from the bars.
        // Read the bars for the instrument and time range from the request.
        let bars = self.read_bars_for_sweep(&instrument, spec)?;
        let bar_spec = parse_timeframe(&spec.bar_spec)
            .map_err(|e| invalid("bar_spec", "invalid_bar_spec", e.message))?;
        let mut builder = honba_feed::ColumnarSliceBuilder::new(instrument.clone(), bar_spec);
        for bar in &bars {
            builder
                .push_bar(bar)
                .map_err(|e| internal("dataset", e.to_string()))?;
        }
        let slice = builder
            .finish()
            .map_err(|e| internal("dataset", e.to_string()))?;
        let dataset = honba_feed::Dataset::from_slices(vec![slice])
            .map_err(|e| internal("dataset", e.to_string()))?;

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|e| internal("runtime", format!("cannot start sweep runtime: {e}")))?;

        let report = rt
            .block_on(async { run(&plan, Arc::new(dataset)).await })
            .map_err(|e| internal("sweep", e.to_string()))?;

        let sweep_response = sweep_report_to_response(report);

        Ok(RunOutcome::Sweep {
            report: sweep_response,
        })
    }
}

fn sweep_report_to_response(report: SweepReport) -> SweepReportResponse {
    let ranked: Vec<serde_json::Value> = report
        .reports()
        .enumerate()
        .map(|(trial_id, r)| {
            let params_json = match &r.params.spec {
                StrategySpec::SmaCrossover { fast, slow, quantity, .. } => {
                    serde_json::json!({"fast": *fast, "slow": *slow, "quantity": *quantity})
                }
                StrategySpec::RsiReversal { period, oversold, overbought, quantity, .. } => {
                    serde_json::json!({"period": *period, "oversold": *oversold, "overbought": *overbought, "quantity": *quantity})
                }
                StrategySpec::BuyAndHold { quantity, .. } => {
                    serde_json::json!({"quantity": *quantity})
                }
                StrategySpec::Custom { .. } => serde_json::Value::Null,
            };
            serde_json::json!({
                "trial_id": trial_id,
                "params": params_json,
                "metrics": {
                    "fills": r.metrics.fills,
                    "round_trips": r.metrics.round_trips,
                    "trades": r.metrics.trades.map(|t| serde_json::to_value(t).unwrap()),
                    "equity": r.metrics.equity.map(|e| serde_json::to_value(e).unwrap()),
                },
            })
        })
        .collect();

    let best = ranked.first().cloned();

    SweepReportResponse {
        ranked,
        best,
        validation: None,
    }
}
