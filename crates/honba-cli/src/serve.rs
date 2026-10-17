//! `honba serve` – the REST API over a directory of Parquet bar files, with backtest runs
//! journaled under `--journals-dir`.
//!
//! The data is loaded once at start-up (a bad directory fails before anything
//! listens), then `honba-api-rest` serves it until ctrl-c. The first stdout line
//! is `listening on http://<addr>` with the address actually bound, so
//! `--addr 127.0.0.1:0` is usable by scripts and tests. Handlers read no clock.
//!
//! With `--bar-ingest`, `POST /bars` feeds a bounded queue drained by the bar pump (fills and
//! quotes follow). Shutdown drops the sender, waits for the pump to drain, and reports its
//! counts on stderr.
//!
//! With `--fill-model l2 --ticks <FILE|DIR>`, REST backtests match against the quote/trade ticks in
//! the file or store (format: ADR 0030, parsed in `ticks_file.rs` / `parquet_ticks.rs`) instead of
//! bars alone.
//!
//! With `--operator-journal <FILE>`, each `PUT /trading-state` change is appended to it as NDJSON
//! (ADR 0018 decision 7); off by default and never read back at start-up.

use anyhow::{Context, Result};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use std::time::Duration;

use honba_api_rest::{ApiConfig, AppState, RetentionPolicy, RunServiceConfig};
use honba_config::{ExecutionConfig, FillModel};
use honba_feed::{InMemoryTickReader, ParquetTickStore};
use honba_messages::TradingState;

/// Parses the `--trading-state` value; the spellings are the wire form (ADR 0018 decision 2).
pub(crate) fn parse_trading_state(raw: &str) -> Result<TradingState, String> {
    match raw {
        "active" => Ok(TradingState::Active),
        "reducing" => Ok(TradingState::Reducing),
        "halted" => Ok(TradingState::Halted),
        other => Err(format!(
            "unknown trading state {other:?}; expected active, reducing or halted"
        )),
    }
}

/// The stderr warning for an `--addr` that is not loopback, or `None` when it is.
pub(crate) fn non_loopback_warning(addr: SocketAddr) -> Option<String> {
    if addr.ip().is_loopback() {
        return None;
    }
    Some(format!(
        "warning: listening on {addr}, which is not loopback; the API has no auth or TLS, \
         so anyone who can reach it can read the data"
    ))
}

/// Where runs live and how many may run (ADR 0017 decisions 3 to 5), and where per-user
/// profiles live.
#[derive(clap::Args, Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunsArgs {
    /// Directory holding one sub-directory per run (manifest.json + events.ndjson)
    #[arg(long, default_value = "data/journals")]
    pub journals_dir: PathBuf,
    /// Directory holding one JSON profile per user (`/profile` routes); created on first write.
    /// Run one `honba serve` per profiles directory: there is no cross-process lock
    #[arg(long, default_value = "data/profiles")]
    pub profiles_dir: PathBuf,
    /// Directory holding one JSON portfolio per user (`/portfolio` routes); created on first
    /// write. Run one `honba serve` per portfolios directory: there is no cross-process lock
    #[arg(long, default_value = "data/portfolios")]
    pub portfolios_dir: PathBuf,
    /// NDJSON file each `PUT /trading-state` change is appended to (ADR 0018 decision 7).
    /// Off by default; never read back at start-up
    #[arg(long)]
    pub operator_journal: Option<PathBuf>,
    /// Worker threads, one backtest each (default: available parallelism)
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub max_concurrent_runs: Option<u64>,
    /// Most pending runs the queue holds before POST /backtests answers 429
    #[arg(long, alias = "max-queued", default_value_t = 64, value_parser = clap::value_parser!(u64).range(1..))]
    pub max_queued_runs: u64,
    /// Seconds a stopping server waits for running runs before cancelling them
    #[arg(long, default_value_t = 10)]
    pub shutdown_grace_secs: u64,
    /// Finished runs that always survive start-up retention
    #[arg(long, default_value_t = 1_000)]
    pub keep_runs: u64,
    /// Finished runs younger than this many days survive start-up retention
    #[arg(long, default_value_t = 30)]
    pub keep_days: u64,
}

/// Auth configuration (ADR 0023, E11-S8).
#[derive(clap::Args, Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthArgs {
    /// Path to tokens.toml file. When provided, Bearer token auth is enabled.
    /// The file is hot-reloaded on changes.
    #[arg(long)]
    pub tokens_file: Option<PathBuf>,
    /// Path to audit log file. If provided, auth decisions are logged here (NDJSON).
    /// Defaults to `audit.log` in the journals directory if `--tokens-file` is set.
    #[arg(long)]
    pub audit_log_file: Option<PathBuf>,
}

/// The live bar feed behind `POST /bars` (E3-S7 follow-up). Off by default: with it off the
/// route answers 503 `bar_ingest_disabled`.
#[derive(clap::Args, Debug, Clone, PartialEq, Eq)]
pub(crate) struct IngestArgs {
    /// Accept bars on POST /bars and feed them to the simulated market (fills, quotes)
    #[arg(long)]
    pub bar_ingest: bool,
    /// Bars the ingest queue holds before POST /bars answers 429
    #[arg(long, default_value_t = 1_024, value_parser = clap::value_parser!(u64).range(1..))]
    pub bar_ingest_capacity: u64,
}

/// Pre-trade risk limits for the write routes (ADR 0018 decision 8, collar addendum).
#[derive(clap::Args, Debug, Clone, PartialEq, Eq)]
pub(crate) struct RiskArgs {
    /// TOML file with a `[risk]` table, the same shape as a backtest config's `[risk]`
    /// (max_notional, max_participation, stale_after_ms, price_collar_bps, [risk.order_rate]).
    /// Default: no limits
    #[arg(long)]
    pub risk_config: Option<PathBuf>,
}

/// How REST backtests are filled and the tick source `fill_model = l2` needs (ADR 0030).
#[derive(clap::Args, Debug, Clone, PartialEq, Eq)]
pub(crate) struct FillArgs {
    /// Fill model for REST backtests: bar_fill, bar_fill_through or l2. Default: bar_fill.
    /// `l2` needs --ticks (refused at start-up otherwise)
    #[arg(long, value_parser = parse_fill_model)]
    pub fill_model: Option<FillModel>,
    /// NDJSON file (format v1, see ADR 0030) or Parquet tick store directory that
    /// `--fill-model l2` matches against. A file is validated at start-up; a directory is read
    /// through the Parquet tick store (`honba data ticks-import` builds one). A bad line is
    /// reported with its line number
    #[arg(long)]
    pub ticks: Option<PathBuf>,
}

/// The loaded tick source behind `--ticks`: an NDJSON file or a Parquet store directory.
pub(crate) enum TickSource {
    /// An NDJSON `--ticks <FILE>`.
    Ndjson(InMemoryTickReader),
    /// A `--ticks <DIR>` Parquet store.
    Parquet(ParquetTickStore),
}

/// Parses the `--fill-model` value; the spellings are the wire form.
pub(crate) fn parse_fill_model(raw: &str) -> Result<FillModel, String> {
    match raw {
        "bar_fill" => Ok(FillModel::BarFill),
        "bar_fill_through" => Ok(FillModel::BarFillThrough),
        "l2" => Ok(FillModel::L2),
        other => Err(format!(
            "unknown fill model {other:?}; expected bar_fill, bar_fill_through or l2"
        )),
    }
}

impl FillArgs {
    /// The execution config and the loaded tick source these flags spell. `l2` without
    /// `--ticks` is a start-up error, so the runtime `l2_requires_ticks` refusal is never the
    /// first the operator hears of it.
    pub(crate) fn resolve(&self) -> Result<(ExecutionConfig, Option<TickSource>)> {
        let fill_model = self.fill_model.unwrap_or_default();
        if fill_model == FillModel::L2 && self.ticks.is_none() {
            anyhow::bail!(
                "--fill-model l2 needs --ticks <FILE|DIR> (NDJSON or a Parquet tick store, see ADR 0030)"
            );
        }
        let ticks = match self.ticks.as_deref() {
            Some(path) if path.is_dir() => Some(TickSource::Parquet(
                ParquetTickStore::open(path)
                    .with_context(|| format!("reading --ticks store {}", path.display()))?,
            )),
            Some(path) => Some(TickSource::Ndjson(crate::ticks_file::load(path)?)),
            None => None,
        };
        let execution = ExecutionConfig {
            fill_model,
            ..ExecutionConfig::default()
        };
        Ok((execution, ticks))
    }
}

/// The file shape behind `--risk-config`: the `[risk]` table of `BacktestRunConfig`.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RiskFile {
    #[serde(default)]
    risk: honba_risk::RiskLimits,
}

/// Reads and validates a `--risk-config` file. Errors carry the path and name the field.
pub(crate) fn load_risk_limits(path: &Path) -> Result<honba_risk::RiskLimits> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading --risk-config {}", path.display()))?;
    let file: RiskFile = toml::from_str(&text)
        .with_context(|| format!("parsing --risk-config {}", path.display()))?;
    file.risk
        .validate()
        .with_context(|| format!("invalid [risk] in --risk-config {}", path.display()))?;
    Ok(file.risk)
}

impl RunsArgs {
    /// The run-service tunables these flags spell.
    pub(crate) fn service_config(&self) -> RunServiceConfig {
        let defaults = RunServiceConfig::default();
        RunServiceConfig {
            max_concurrent: self
                .max_concurrent_runs
                .map_or(defaults.max_concurrent, clamp_usize),
            max_queued: clamp_usize(self.max_queued_runs),
            shutdown_grace: Duration::from_secs(self.shutdown_grace_secs),
            retention: RetentionPolicy {
                keep_runs: clamp_usize(self.keep_runs),
                keep_days: self.keep_days,
            },
        }
    }
}

fn clamp_usize(n: u64) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

#[allow(clippy::too_many_arguments)] // one parameter per flag group of `honba serve`
pub fn run(
    data_dir: &Path,
    addr: SocketAddr,
    cors_origins: &[String],
    trading_state: TradingState,
    runs: &RunsArgs,
    auth: &AuthArgs,
    ingest: &IngestArgs,
    risk: &RiskArgs,
    fill: &FillArgs,
) -> Result<()> {
    let (execution, ticks) = fill.resolve()?;
    let risk_limits = match &risk.risk_config {
        Some(path) => load_risk_limits(path)?,
        None => honba_risk::RiskLimits::default(),
    };
    let mut config = ApiConfig::default()
        .with_cors_origins(cors_origins)
        .map_err(|e| anyhow::anyhow!(e))
        .context("invalid --cors-origin (cors)")?;

    if let Some(tokens_file) = &auth.tokens_file {
        config.auth.tokens_file = Some(tokens_file.clone());
        if let Some(audit_log_file) = &auth.audit_log_file {
            config.auth.audit_log_path = Some(audit_log_file.clone());
        } else if auth.tokens_file.is_some() {
            // Default audit log next to journals dir
            config.auth.audit_log_path = Some(runs.journals_dir.join("audit.log"));
        }
    }

    let mut state = AppState::from_parquet_dir(data_dir)
        .with_context(|| format!("loading data directory {}", data_dir.display()))?
        .with_trading_state(trading_state)
        .with_risk_limits(risk_limits)
        .with_execution(execution);
    if let Some(ticks) = ticks {
        let reader: std::sync::Arc<dyn honba_ports::TickReader> = match ticks {
            TickSource::Ndjson(reader) => std::sync::Arc::new(reader),
            TickSource::Parquet(store) => std::sync::Arc::new(store),
        };
        state = state.with_tick_reader(reader);
    }
    // Execution and ticks are set before the run service, which captures them.
    let mut state = state
        .with_profiles_dir(&runs.profiles_dir)
        .with_portfolios_dir(&runs.portfolios_dir);
    if let Some(journal) = &runs.operator_journal {
        state = state.with_operator_journal(journal);
    }
    let state = state
        .with_journals_dir(&runs.journals_dir, runs.service_config())
        .map_err(|e| anyhow::anyhow!("{}", e.message))
        .with_context(|| format!("opening journals directory {}", runs.journals_dir.display()))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting the async runtime")?;
    let served = runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding {addr}"))?;
        // Only warn about non-loopback if auth is NOT enabled
        if auth.tokens_file.is_none() {
            if let Some(warning) = non_loopback_warning(addr) {
                eprintln!("{warning}");
            }
        }
        // Started inside the runtime, before the first request can arrive.
        let pump = ingest
            .bar_ingest
            .then(|| state.start_bar_ingest(clamp_usize(ingest.bar_ingest_capacity)));
        println!("listening on http://{}", listener.local_addr()?);
        std::io::stdout().flush()?;
        let served =
            honba_api_rest::serve_with_config(listener, state.clone(), &config, stop_signal())
                .await
                .context("serving");
        // Drop the sender so the pump drains what is queued and ends, then report.
        state.stop_bar_ingest();
        if let Some(pump) = pump {
            match pump.await {
                Ok(report) => {
                    eprintln!("bar ingest: {} bars, {} errors", report.bars, report.errors)
                }
                Err(e) => eprintln!("bar ingest: pump task failed: {e}"),
            }
        }
        served
    });
    // Graceful run shutdown (ADR 0017 decision 5): cancel pending runs, wait the grace
    // period for running ones, cancel the rest. Blocking, so after the runtime stopped.
    if let Some(report) = state.shutdown_runs() {
        if report.cancelled_pending + report.cancelled_running > 0 {
            eprintln!(
                "cancelled {} pending and {} running runs",
                report.cancelled_pending, report.cancelled_running
            );
        }
    }
    served
}

/// Resolves on ctrl-c (SIGINT) or, on unix, SIGTERM.
async fn stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            },
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
