//! Portfolio actor managing position tracking, FIFO tax-lot accounting, MTM valuation,
//! and integration with the double-entry ledger.

use honba_core::{FillReport, InstrumentId, Money, OrderSide, Price, Quantity};
use honba_kokoro_ledger::{Transaction, post_transaction};
use honba_portfolio::{
    apply_fill_to_account, HeldPosition, PortfolioState, PositionSide as PortfolioPositionSide,
    TaxSummary, AccountState, Marks, CorporateAction, compute_capital_gains,
};
use honba_persistence::SqliteAuditLedger;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::messages::{ActorStatus, PortfolioMsg, PortfolioUpdate, PortfolioSnapshot, PortfolioError, FeeBreakdown, PortfolioSettleResult};

/// Portfolio actor managing position tracking and tax-lot accounting.
pub struct PortfolioActor {
    /// Portfolio state per account
    portfolio: PortfolioState,
    /// Marks (last prices) for MTM valuation
    marks: Marks,
    /// Accumulated fills for tax summary
    fills_history: Vec<(AccountState, InstrumentId, PortfolioPositionSide, u64, Price, chrono::NaiveDate)>,
    /// Corporate actions to apply
    corporate_actions: Vec<CorporateAction>,
    /// Connected ledger for double-entry accounting
    audit_ledger: Option<Arc<SqliteAuditLedger>>,
    /// Actor status
    status: ActorStatus,
    /// Actor receiver
    rx: mpsc::Receiver<PortfolioMsg>,
    /// Base currency for the portfolio (from initial cash)
    base_currency: honba_core::Currency,
}

impl PortfolioActor {
    /// Create a new portfolio actor with initial cash.
    pub fn new(initial_cash: Money, rx: mpsc::Receiver<PortfolioMsg>) -> Self {
        let mut portfolio = PortfolioState::new(initial_cash.currency);
        let default_account = AccountState::new(initial_cash.currency, vec![]);
        portfolio.accounts.insert("default".to_string(), default_account);
        
        Self {
            portfolio,
            marks: Marks::new(),
            fills_history: Vec::new(),
            corporate_actions: Vec::new(),
            audit_ledger: None,
            status: ActorStatus::Starting,
            rx,
            base_currency: initial_cash.currency,
        }
    }

    /// Attach SEBI SQLite audit ledger.
    pub fn with_audit_ledger(mut self, audit_ledger: Arc<SqliteAuditLedger>) -> Self {
        self.audit_ledger = Some(audit_ledger);
        self
    }

    /// Run the portfolio actor event loop.
    pub async fn run(mut self) -> ActorStatus {
        info!("PortfolioActor starting");
        self.status = ActorStatus::Running;

        while let Some(msg) = self.rx.recv().await {
            match msg {
                PortfolioMsg::ProcessFill { fill, reply_to } => {
                    let result = self.process_fill(&fill);
                    let _ = reply_to.send(result);
                }
                PortfolioMsg::GetSnapshot { reply_to } => {
                    let snapshot = self.get_snapshot();
                    let _ = reply_to.send(snapshot);
                }
                PortfolioMsg::GetTaxSummary { period_start, period_end, reply_to } => {
                    let summary = self.get_tax_summary(period_start, period_end);
                    let _ = reply_to.send(summary);
                }
                PortfolioMsg::ApplyCorporateActions { actions, reply_to } => {
                    self.corporate_actions = actions;
                    let _ = reply_to.send(());
                }
                PortfolioMsg::SettleTrades { current_date, reply_to } => {
                    let result = self.settle_trades(current_date);
                    let _ = reply_to.send(result);
                }
                PortfolioMsg::Shutdown => {
                    info!("PortfolioActor received shutdown signal");
                    self.status = ActorStatus::Draining;
                    break;
                }
            }
        }

        self.status = ActorStatus::Stopped;
        info!("PortfolioActor stopped");
        self.status
    }

    /// Process a fill from the execution layer.
    fn process_fill(&mut self, fill: &FillReport) -> Result<PortfolioUpdate, PortfolioError> {
        // Convert FillReport to portfolio types
        let instrument = fill.instrument.clone();
        let side = match fill.side {
            OrderSide::Buy => PortfolioPositionSide::Long,
            OrderSide::Sell => PortfolioPositionSide::Short,
        };
        let qty = fill.quantity.0.try_into().unwrap_or(0);
        let price = fill.price;
        let fill_date = fill.timestamp.date_naive();

        let default_currency = self.base_currency;
        // Get or create default account
        if !self.portfolio.accounts.contains_key("default") {
            self.portfolio.accounts.insert("default".to_string(), AccountState::new(default_currency, vec![]));
        }

        // Apply fill to portfolio (FIFO tax-lot matching) - limit mutable borrow scope
        let realized_pnl = {
            let account = self.portfolio.accounts.get_mut("default").unwrap();
            apply_fill_to_account(account, instrument.clone(), side, qty, price)
        };

        // Calculate fees using the ledger's tax calculator
        let is_delivery = true; // TODO: determine from product type
        let fees = self.calculate_fees(fill, is_delivery);

        // Record in double-entry ledger
        self.record_fill_ledger(fill, &fees, &realized_pnl)?;

        // Add to fills history for tax summary - need account data again
        let fill_data = {
            let account = self.portfolio.accounts.get("default").unwrap();
            (account.clone(), instrument.clone(), side, qty, price, fill_date)
        };
        self.fills_history.push(fill_data);

        // Update marks for MTM
        self.marks.marks.insert(instrument.clone(), price);

        // Build response
        let positions: BTreeMap<String, Vec<HeldPosition>> = self.portfolio.accounts
            .iter()
            .map(|(k, v)| (k.clone(), v.positions.clone()))
            .collect();

        let cash = self.portfolio.accounts.get("default").unwrap().cash;

        Ok(PortfolioUpdate {
            positions,
            realized_pnl,
            cash,
            fees,
        })
    }

    /// Calculate Indian statutory fees for a fill.
    fn calculate_fees(&self, fill: &FillReport, is_delivery: bool) -> FeeBreakdown {
        let notional = fill.price.0 * fill.quantity.0;
        
        // STT
        let stt_rate = if is_delivery {
            Decimal::from(1i64) / Decimal::from(1000i64) // 0.1%
        } else {
            Decimal::from(1i64) / Decimal::from(4000i64) // 0.025% intraday sell only
        };
        let stt = if is_delivery || fill.side == OrderSide::Sell {
            (notional * stt_rate).floor()
        } else {
            Decimal::ZERO
        };

        // Exchange turnover charges (NSE: ₹2.97 per crore)
        let turnover = (notional * Decimal::from(297i64) / Decimal::from(10_000_000i64)).floor();

        // SEBI turnover charges (₹10 per crore)
        let sebi = (notional * Decimal::from(10i64) / Decimal::from(10_000_000i64)).floor();

        // Stamp duty (0.015% on buy)
        let stamp_duty = if fill.side == OrderSide::Buy {
            (notional * Decimal::from(15i64) / Decimal::from(100_000i64)).floor()
        } else {
            Decimal::ZERO
        };

        // Brokerage (placeholder - would come from broker)
        let brokerage = Decimal::ZERO;

        // GST (18% on brokerage + exchange + sebi)
        let gst_base = brokerage + turnover + sebi;
        let gst = (gst_base * Decimal::from(18i64) / Decimal::from(100i64)).floor();

        // DP charges (₹13.50 + GST on delivery sale)
        let dp_charges = if is_delivery && fill.side == OrderSide::Sell {
            let base = Decimal::from(1350i64) / Decimal::from(100i64); // ₹13.50 in paise
            let gst_rate = Decimal::from(18i64) / Decimal::from(100i64);
            let gst_dp = (base * gst_rate).floor();
            base + gst_dp
        } else {
            Decimal::ZERO
        };

        FeeBreakdown {
            stt: Money::inr(stt),
            gst: Money::inr(gst),
            stamp_duty: Money::inr(stamp_duty),
            sebi_turnover: Money::inr(sebi),
            dp_charges: Money::inr(dp_charges),
            brokerage: Money::inr(brokerage),
        }
    }

    /// Record fill in double-entry ledger.
    fn record_fill_ledger(
        &mut self,
        fill: &FillReport,
        fees: &FeeBreakdown,
        realized_pnl: &Money,
    ) -> Result<(), PortfolioError> {
        let tx = Transaction {
            id: fill.fill_id.clone(),
            timestamp: fill.timestamp,
            debits: BTreeMap::new(),
            credits: BTreeMap::new(),
            description: format!("Fill {:?} {} {}", fill.side, fill.quantity, fill.instrument),
        };

        // In a full implementation, this would create proper debit/credit entries
        // For now, validate the transaction structure
        post_transaction(&tx).map_err(|e| PortfolioError::LedgerError(e.to_string()))?;

        Ok(())
    }

    /// Get current portfolio snapshot.
    fn get_snapshot(&self) -> PortfolioSnapshot {
        let mut total_unrealized = Money::inr(Decimal::ZERO);
        let mut total_realized = Money::inr(Decimal::ZERO);
        let mut total_equity = Money::inr(Decimal::ZERO);
        let mut cash = Money::inr(Decimal::ZERO);

        for (_, account) in &self.portfolio.accounts {
            cash = Money {
                amount: cash.amount + account.cash.amount,
                currency: account.cash.currency,
            };

            for pos in &account.positions {
                if let Some(mark) = self.marks.marks.get(&pos.instrument) {
                    let market_value = mark.0 * pos.quantity.0;
                    let cost_basis = pos.entry_price.0 * pos.quantity.0;
                    let unrealized = market_value - cost_basis;
                    total_unrealized = Money {
                        amount: total_unrealized.amount + unrealized,
                        currency: honba_core::Currency::Inr,
                    };
                }
            }
        }

        // Get realized PnL from fills history
        for (account, instrument_id, side, fill_qty, fill_price, fill_date) in &self.fills_history {
            if let Some(idx) = account.positions.iter().position(|p| p.instrument == *instrument_id) {
                let pos = &account.positions[idx];
                let (pnl, _) = compute_capital_gains(
                    pos, *fill_qty, *fill_price, *fill_date,
                );
                total_realized = Money {
                    amount: total_realized.amount + pnl.amount,
                    currency: honba_core::Currency::Inr,
                };
            }
        }

        total_equity = Money {
            amount: cash.amount + total_unrealized.amount,
            currency: honba_core::Currency::Inr,
        };

        let positions: BTreeMap<String, Vec<HeldPosition>> = self.portfolio.accounts
            .iter()
            .map(|(k, v)| (k.clone(), v.positions.clone()))
            .collect();

        PortfolioSnapshot {
            total_equity,
            cash,
            unrealized_pnl: total_unrealized,
            realized_pnl: total_realized,
            positions,
        }
    }

    /// Get tax summary for a period.
    fn get_tax_summary(&self, period_start: chrono::NaiveDate, period_end: chrono::NaiveDate) -> TaxSummary {
        // Convert owned fills to references for the function
        let fills_refs: Vec<(&AccountState, InstrumentId, PortfolioPositionSide, u64, Price, chrono::NaiveDate)> = self.fills_history.iter()
            .map(|(acc, inst, side, qty, price, date)| {
                (&*acc, inst.clone(), *side, *qty, *price, *date)
            })
            .collect();
        
        TaxSummary::from_fills(
            &fills_refs,
            period_start,
            period_end,
            &self.corporate_actions,
        )
    }

    /// Perform T+1 settlement.
    fn settle_trades(&mut self, current_date: chrono::NaiveDate) -> PortfolioSettleResult {
        // Convert portfolio trades to settlement format
        // This is a simplified implementation
        PortfolioSettleResult {
            settled_count: 0,
            failed_count: 0,
            cash_flow: Money::inr(Decimal::ZERO),
            securities_moved: Vec::new(),
            errors: Vec::new(),
        }
    }
}

// Re-export honba-core Currency for internal use
use honba_core::Currency;