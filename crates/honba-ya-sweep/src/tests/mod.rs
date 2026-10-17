//! Unit tests for this crate, one file per area.

mod error;
mod fitness;
mod plan;
mod report;
mod trial_sink;

use honba_analytics::{EquityStats, TradeStats};
use honba_messages::{Exchange, InstrumentId};

use crate::{StrategySpec, TrialMetrics, TrialParams, TrialReport};

/// A placeholder instrument for tests where the instrument does not matter.
fn any_instrument() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("NSE"))
}

/// Buy-and-hold params for `seed`, the simplest distinct trial.
fn buy_and_hold_params(seed: u64) -> TrialParams {
    TrialParams {
        seed,
        spec: StrategySpec::BuyAndHold {
            instrument: any_instrument(),
            quantity: 10.0,
        },
    }
}

/// Trade statistics for `n` winning round trips.
fn trade_stats(n: usize) -> TradeStats {
    TradeStats {
        n_trades: n,
        n_wins: n,
        n_losses: 0,
        n_flat: 0,
        win_rate: 1.0,
        gross_profit: 10.0,
        gross_loss: 0.0,
        profit_factor: None,
        avg_win: Some(10.0),
        avg_loss: None,
        total_pnl: 10.0,
        expectancy: 10.0,
        total_fees: 0.0,
    }
}

/// Metrics for a trial whose Sharpe ratio is `sharpe`.
fn sharpe_metrics(sharpe: Option<f64>) -> TrialMetrics {
    TrialMetrics {
        fills: 4,
        round_trips: 2,
        trades: Some(trade_stats(2)),
        equity: Some(EquityStats {
            n_periods: 2,
            total_return: 0.05,
            annualized_return: 0.06,
            annualized_volatility: 0.1,
            sharpe,
            sortino: sharpe,
            max_drawdown: 0.01,
            max_drawdown_pct: 0.01,
            calmar: None,
        }),
    }
}

/// Metrics for a trial that produced no round trips and no return series.
fn no_equity_metrics() -> TrialMetrics {
    TrialMetrics {
        fills: 0,
        round_trips: 0,
        trades: None,
        equity: None,
    }
}

/// A report for `trial_id` whose Sharpe ratio is `sharpe`.
fn report(trial_id: usize, sharpe: Option<f64>) -> TrialReport {
    TrialReport {
        trial_id,
        params: buy_and_hold_params(trial_id as u64),
        metrics: sharpe_metrics(sharpe),
        audit: Vec::new(),
    }
}

/// A report for a trial that never traded.
fn no_equity_report(trial_id: usize) -> TrialReport {
    TrialReport {
        trial_id,
        params: buy_and_hold_params(trial_id as u64),
        metrics: no_equity_metrics(),
        audit: Vec::new(),
    }
}

/// Metrics for a trial with the given `sortino`, `calmar` and `profit_factor`; the other
/// stats are irrelevant to the fitnesses that read these three.
fn metrics_with(
    sortino: Option<f64>,
    calmar: Option<f64>,
    profit_factor: Option<f64>,
) -> TrialMetrics {
    let mut metrics = sharpe_metrics(sortino);
    if let Some(equity) = metrics.equity.as_mut() {
        equity.calmar = calmar;
    }
    if let Some(trades) = metrics.trades.as_mut() {
        trades.profit_factor = profit_factor;
    }
    metrics
}

/// A report for `trial_id` with the given `sortino`, `calmar` and `profit_factor`.
fn report_with(
    trial_id: usize,
    sortino: Option<f64>,
    calmar: Option<f64>,
    profit_factor: Option<f64>,
) -> TrialReport {
    TrialReport {
        trial_id,
        params: buy_and_hold_params(trial_id as u64),
        metrics: metrics_with(sortino, calmar, profit_factor),
        audit: Vec::new(),
    }
}
