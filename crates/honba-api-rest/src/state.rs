//! Shared state of the REST API: the read ports the handlers depend on.
//!
//! Handlers see only the read ports owned by `honba-ports` ([`InstrumentMaster`],
//! [`BarReader`], [`QuoteReader`], [`DepthReader`]); which adapter answers is decided here, at the composition
//! root. This crate names `honba-feed` only to build those adapters.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use honba_api::StrategyCatalog;
use honba_entities::Currency;
use honba_feed::{DatasetReader, LiveQuoteStore, ReaderError};
use honba_messages::{ErrorDetail, TradingState};
use honba_ports::{BarReader, DepthReader, InstrumentMaster, QuoteReader};

use crate::{
    ApprovalQueue, BacktestExecutor, OrderLedger, PortfolioStore, ProfileStore, RunService,
    RunServiceConfig, RunStore, StrategyRegistry,
};

/// App state for REST API.
#[derive(Clone)]
pub struct AppState {
    /// Reference data: which instruments exist.
    pub instruments: Arc<dyn InstrumentMaster>,
    /// Stored historical bars.
    pub bars: Arc<dyn BarReader>,
    /// Latest top-of-book quotes.
    pub quotes: Arc<dyn QuoteReader>,
    /// Order-book depth.
    pub depth: Arc<dyn DepthReader>,
    /// Strategies compiled by `POST /strategies` in this process.
    ///
    /// Session-scoped and in memory: it is a plain pure value from `honba-api`, not a port,
    /// because it holds no external resource and nothing else implements it. Clones of the
    /// state share it.
    pub strategies: Arc<Mutex<StrategyCatalog>>,
    /// The run service behind `/backtests` and `/journals`; `None` until a journals root is
    /// configured ([`AppState::with_journals_dir`]).
    pub runs: Option<Arc<RunService>>,
    /// The per-user profile store behind `/profile`; `None` until a profiles directory is
    /// configured ([`AppState::with_profiles_dir`]).
    pub profiles: Option<Arc<ProfileStore>>,
    /// The per-user portfolio store behind `/portfolio`; `None` until a portfolios directory is
    /// configured ([`AppState::with_portfolios_dir`]).
    pub portfolios: Option<Arc<PortfolioStore>>,
    /// NDJSON file `PUT /trading-state` appends one record per change to; `None` until
    /// configured ([`AppState::with_operator_journal`]). The state is never restored from it.
    pub operator_journal: Option<PathBuf>,
    /// Account currency of the runs (journal fills carry no costs; they are zero in this).
    pub account_currency: Currency,
    /// The operator trading state the write routes evaluate against (ADR 0018 decision 7).
    ///
    /// Seeded by the composition root ([`AppState::with_trading_state`]) and changed at runtime
    /// by `PUT /trading-state` ([`AppState::set_trading_state`]). One cell shared by every
    /// clone of the state, so the order gate and `POST /positions/close` see a change on the
    /// next request. `Halted` refuses placements while cancel and close still pass.
    trading_state: Arc<RwLock<TradingState>>,
    /// Risk limits the order routes build their stage with (default: none). Set in process
    /// by the composition root ([`AppState::with_risk_limits`]).
    pub risk_limits: honba_risk::RiskLimits,
    /// Approval queue for order placements (E5-S4).
    ///
    /// When enabled, `POST /orders` is held here until approved by an operator.
    /// Cancel and modify bypass the queue.
    pub approval_queue: Option<Arc<ApprovalQueue>>,
    /// The write ledger every risk-approved placement lands in (E11-S7, ADR 0019
    /// decision 3). `GET /orders` reads it; a restart empties it.
    pub orders: OrderLedger,
    /// The embedded `NextOpenSim` approved orders are submitted to (ADR 0016).
    pub(crate) exec: crate::exec::ExecGateway,
    /// The writable store behind `quotes`, when the state was built by `from_reader`.
    live: Option<Arc<LiveQuoteStore>>,
    /// The quote/trade tick source `fill_model = l2` runs match against (ADR 0030); `None`
    /// refuses such runs with `l2_requires_ticks`.
    ticks: Option<Arc<dyn honba_ports::TickReader>>,
    /// Execution config the run service's executor is built with (default: bar fills).
    execution: honba_config::ExecutionConfig,
    /// The sending side of the bar ingest channel behind `POST /bars`; `None` until
    /// [`AppState::start_bar_ingest`] (or [`AppState::attach_bar_ingest`]) and again after
    /// [`AppState::stop_bar_ingest`]. Shared by every clone of the state.
    pub(crate) bar_ingest: Arc<Mutex<Option<tokio::sync::mpsc::Sender<honba_messages::Bar>>>>,
}

impl AppState {
    /// Creates state from any pair of port implementations.
    pub fn new(
        instruments: Arc<dyn InstrumentMaster>,
        bars: Arc<dyn BarReader>,
        quotes: Arc<dyn QuoteReader>,
        depth: Arc<dyn DepthReader>,
    ) -> Self {
        Self {
            instruments,
            bars,
            quotes,
            depth,
            strategies: Arc::default(),
            runs: None,
            profiles: None,
            portfolios: None,
            operator_journal: None,
            account_currency: Currency::Inr,
            trading_state: Arc::default(),
            risk_limits: honba_risk::RiskLimits::default(),
            approval_queue: None,
            orders: OrderLedger::default(),
            exec: crate::exec::ExecGateway::default_sim(),
            live: None,
            ticks: None,
            execution: honba_config::ExecutionConfig::default(),
            bar_ingest: Arc::default(),
        }
    }

    /// The writable live-quote store `quotes` reads, if this state has one.
    ///
    /// `from_reader` and `from_parquet_dir` build it; `AppState::new` with caller-supplied
    /// ports has none. The bar pump writes each bar here before advancing the market.
    pub fn live_quotes(&self) -> Option<&Arc<LiveQuoteStore>> {
        self.live.as_ref()
    }

    /// Enables the approval queue for order placements (E5-S4).
    ///
    /// When enabled, `POST /orders` is held in the queue until approved by an operator.
    /// Cancel and modify bypass the queue. `max_size` bounds the queue (default 100).
    pub fn with_approval_queue(mut self, max_size: usize) -> Self {
        self.approval_queue = Some(Arc::new(ApprovalQueue::new(max_size)));
        self
    }

    /// Replaces the embedded execution simulator (cash, costs, slippage, seed).
    pub fn with_execution_sim(self, sim: honba_sim::NextOpenSim) -> Self {
        self.with_execution_gateway(sim)
    }

    /// Plugs in the execution venue every order route submits to and cancels on (E3-S7).
    ///
    /// The default is a `NextOpenSim`. Events the venue reports (fills, rejects, cancels) update
    /// the order ledger through the same mapping as the simulator's; see [`ExecutionVenue`](crate::ExecutionVenue).
    pub fn with_execution_gateway(mut self, gateway: impl crate::ExecutionVenue) -> Self {
        self.exec = crate::exec::ExecGateway::new(gateway);
        self
    }

    /// Advances the market: feeds `bars` (ascending `ts_event`) to the embedded simulator.
    ///
    /// Market orders submitted earlier fill at the open of their instrument's first bar in a
    /// later session, and each fill, rejection or cancel updates the matching ledger row (so
    /// [`OrderLedger::position`] follows). This is the hook a live bar feed calls; event time is
    /// the bars' `ts_event`, never the wall clock. An out-of-order bar is an error.
    pub fn advance_market(&self, bars: &[honba_messages::Bar]) -> Result<(), ErrorDetail> {
        self.exec.advance(&self.orders, bars)
    }

    /// Sets the operator trading state the write routes evaluate against.
    ///
    /// This seeds the state: the cell is replaced, so call it while building the state, before
    /// cloning it. At runtime use [`AppState::set_trading_state`].
    pub fn with_trading_state(mut self, state: TradingState) -> Self {
        self.trading_state = Arc::new(RwLock::new(state));
        self
    }

    /// The operator trading state in force right now.
    pub fn trading_state(&self) -> TradingState {
        *self
            .trading_state
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Sets the operator trading state for every clone of this state and returns the previous
    /// one. Any transition is allowed; setting the current state again changes nothing.
    pub fn set_trading_state(&self, state: TradingState) -> TradingState {
        let mut guard = self
            .trading_state
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        std::mem::replace(&mut *guard, state)
    }

    /// Sets the risk limits the order routes enforce (ADR 0018), e.g. `price_collar_bps`.
    pub fn with_risk_limits(mut self, limits: honba_risk::RiskLimits) -> Self {
        self.risk_limits = limits;
        self
    }

    /// Sets the tick source L2 backtests read (call before [`AppState::with_journals_dir`]).
    #[must_use]
    pub fn with_tick_reader(mut self, ticks: Arc<dyn honba_ports::TickReader>) -> Self {
        self.ticks = Some(ticks);
        self
    }

    /// Sets the execution config (fill model) the backtest executor runs with (call before
    /// [`AppState::with_journals_dir`]). `fill_model = l2` needs [`AppState::with_tick_reader`].
    #[must_use]
    pub fn with_execution(mut self, execution: honba_config::ExecutionConfig) -> Self {
        self.execution = execution;
        self
    }

    /// Serves runs from `service` (tests inject a deterministic executor this way).
    pub fn with_run_service(mut self, service: RunService) -> Self {
        self.runs = Some(Arc::new(service));
        self
    }

    /// Starts the run service over the journals root `dir`: recovers and prunes it (blocking
    /// file I/O, once), then runs backtests over this state's bar reader and strategy catalog.
    ///
    /// Call it once at start-up, never from a request handler. `AppState::default()` has no
    /// journals root and so scans nothing.
    pub fn with_journals_dir(
        mut self,
        dir: &Path,
        config: RunServiceConfig,
    ) -> Result<Self, ErrorDetail> {
        let registry = StrategyRegistry::builtin();
        let mut executor = BacktestExecutor::new(self.bars.clone(), registry.clone())
            .with_execution(self.execution.clone());
        if let Some(ticks) = &self.ticks {
            executor = executor.with_tick_reader(ticks.clone());
        }
        self.account_currency = executor.account_currency()?;
        let service = RunService::start(
            Arc::new(RunStore::system(dir.to_path_buf())),
            Arc::new(executor),
            self.strategies.clone(),
            registry,
            config,
        );
        self.runs = Some(Arc::new(service));
        Ok(self)
    }

    /// Serves `/profile` from one JSON file per user under `dir` (created on first write).
    pub fn with_profiles_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.profiles = Some(Arc::new(ProfileStore::new(dir)));
        self
    }

    /// Serves `/portfolio` from one JSON file per user under `dir` (created on first write).
    ///
    /// Run one `honba serve` per portfolios directory: the store's lock is in-process only
    /// (ADR 0032 §5).
    pub fn with_portfolios_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.portfolios = Some(Arc::new(PortfolioStore::new(dir)));
        self
    }

    /// Appends one NDJSON record per `PUT /trading-state` change to `path` (created on first
    /// change). Off by default. The journal is a record, not a source of truth: the state is
    /// never restored from it at start-up (ADR 0015, explicit runtime).
    pub fn with_operator_journal(mut self, path: impl Into<PathBuf>) -> Self {
        self.operator_journal = Some(path.into());
        self
    }

    /// Graceful shutdown of the run service (ADR 0017 decision 5): stops admitting, cancels
    /// pending runs, waits out the grace period. Blocking; `None` when there is no service.
    pub fn shutdown_runs(&self) -> Option<crate::ShutdownReport> {
        self.runs.as_ref().map(|runs| runs.shutdown())
    }

    /// Replaces the strategy catalog (for a non-default capacity).
    pub fn with_strategy_catalog(mut self, catalog: StrategyCatalog) -> Self {
        self.strategies = Arc::new(Mutex::new(catalog));
        self
    }

    /// Creates state serving every port from one [`DatasetReader`], except `quotes`, which is
    /// a [`LiveQuoteStore`] over it so a live feed can refresh the latest quote.
    ///
    /// This is the constructor for tests: build a reader over an in-memory
    /// `Dataset` and no file or network is touched.
    pub fn from_reader(reader: DatasetReader) -> Self {
        let reader = Arc::new(reader);
        let live = Arc::new(LiveQuoteStore::new(reader.clone()));
        let mut state = Self::new(reader.clone(), reader.clone(), live.clone(), reader);
        state.live = Some(live);
        state
    }

    /// Creates state serving the `SYMBOL.EXCHANGE.parquet` files in `dir`.
    ///
    /// Blocks on I/O; call it once at start-up. A missing or unreadable
    /// directory is an error rather than an empty catalogue.
    pub fn from_parquet_dir(dir: &Path) -> Result<Self, ReaderError> {
        DatasetReader::from_parquet_dir(dir).map(Self::from_reader)
    }
}

impl Default for AppState {
    /// An empty catalogue: no instruments and no bars.
    fn default() -> Self {
        Self::from_reader(DatasetReader::from_dataset(Default::default()))
    }
}

impl fmt::Debug for AppState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppState").finish_non_exhaustive()
    }
}
