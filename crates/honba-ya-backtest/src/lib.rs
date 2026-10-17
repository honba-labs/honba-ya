//! Event-driven backtester with NSE calendar.
use chrono::NaiveDate;
use rust_decimal::Decimal;

pub struct BacktestConfig {
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub starting_cash_inr: Decimal,
}

pub struct BacktestResult {
    pub final_equity_inr: Decimal,
    pub max_drawdown: Decimal,
    pub trades: u32,
}

pub async fn run(_cfg: BacktestConfig) -> anyhow::Result<BacktestResult> {
    anyhow::bail!("not implemented")
}
