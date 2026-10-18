//! T+1 rolling settlement engine for Indian cash equity markets.
//!
//! SEBI transitioned Indian equity cash markets to mandatory T+1 rolling settlement on January 27, 2023.
//! Under T+1:
//! - Trades executed on trading day $T$ settle on the next exchange trading day $T+1$.
//! - Bought securities become fully settled and available for delivery/pledge on $T+1$.
//! - Sold proceeds settle into available cash balances on $T+1$ morning.

use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::collections::{BTreeMap, HashMap};

use honba_core::{InstrumentId, Quantity};

use crate::calendar::NseTradingCalendar;

/// An individual trade settlement obligation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementBatch {
    /// Scheduled settlement date ($T+1$)
    pub settlement_date: NaiveDate,
    /// Net cash balance change upon settlement (positive for sells, negative for buys)
    pub net_cash: Decimal,
    /// Net securities quantity changes per instrument
    pub securities: HashMap<InstrumentId, i64>,
}

/// Ledger tracking settled and pending T+1 cash and securities balances.
#[derive(Debug, Clone, Default)]
pub struct SettlementLedger {
    settled_cash: Decimal,
    settled_securities: HashMap<InstrumentId, u64>,
    /// Pending settlement batches keyed by settlement date
    pending_batches: BTreeMap<NaiveDate, Vec<SettlementBatch>>,
}

impl SettlementLedger {
    /// Create a ledger with initial settled cash balance.
    pub fn new(initial_cash: Decimal) -> Self {
        Self {
            settled_cash: initial_cash,
            settled_securities: HashMap::new(),
            pending_batches: BTreeMap::new(),
        }
    }

    /// Current settled cash balance.
    pub fn settled_cash(&self) -> Decimal {
        self.settled_cash
    }

    /// Settled securities holding for an instrument.
    pub fn settled_position(&self, instrument: &InstrumentId) -> u64 {
        self.settled_securities
            .get(instrument)
            .copied()
            .unwrap_or(0)
    }

    /// Record a delivery buy execution on trade date $T$.
    ///
    /// The purchase cost is blocked immediately, and shares are queued for delivery on $T+1$.
    pub fn record_delivery_buy(
        &mut self,
        instrument: InstrumentId,
        quantity: Quantity,
        total_cost_including_fees: Decimal,
        trade_date: NaiveDate,
        calendar: &NseTradingCalendar,
    ) {
        let settlement_date = calendar.next_trading_day(trade_date);
        let qty_i64 = quantity.0.to_i64().unwrap_or(0);
        let mut securities = HashMap::new();
        securities.insert(instrument, qty_i64);

        let batch = SettlementBatch {
            settlement_date,
            net_cash: -total_cost_including_fees,
            securities,
        };
        self.pending_batches
            .entry(settlement_date)
            .or_default()
            .push(batch);
    }

    /// Record a delivery sell execution on trade date $T$.
    ///
    /// Shares are debited, and net proceeds (trade value minus statutory fees) settle on $T+1$.
    pub fn record_delivery_sell(
        &mut self,
        instrument: InstrumentId,
        quantity: Quantity,
        net_proceeds_after_fees: Decimal,
        trade_date: NaiveDate,
        calendar: &NseTradingCalendar,
    ) {
        let settlement_date = calendar.next_trading_day(trade_date);
        let qty_i64 = quantity.0.to_i64().unwrap_or(0);
        let mut securities = HashMap::new();
        securities.insert(instrument, -qty_i64);

        let batch = SettlementBatch {
            settlement_date,
            net_cash: net_proceeds_after_fees,
            securities,
        };
        self.pending_batches
            .entry(settlement_date)
            .or_default()
            .push(batch);
    }

    /// Advance settlement clock to `current_date`, clearing all batches due on or before `current_date`.
    pub fn settle_up_to(&mut self, current_date: NaiveDate) {
        let due_dates: Vec<NaiveDate> = self
            .pending_batches
            .range(..=current_date)
            .map(|(d, _)| *d)
            .collect();

        for d in due_dates {
            if let Some(batches) = self.pending_batches.remove(&d) {
                for batch in batches {
                    self.settled_cash += batch.net_cash;
                    for (inst, qty_change) in batch.securities {
                        let entry = self.settled_securities.entry(inst).or_insert(0);
                        if qty_change >= 0 {
                            *entry += qty_change as u64;
                        } else {
                            let debit = (-qty_change) as u64;
                            *entry = entry.saturating_sub(debit);
                        }
                    }
                }
            }
        }
    }

    /// Compute total portfolio equity = settled cash + mark-to-market value of settled securities.
    pub fn total_equity<F>(&self, mark_to_market_fn: F) -> Decimal
    where
        F: Fn(&InstrumentId) -> Decimal,
    {
        let mut equity = self.settled_cash;
        for (inst, qty) in &self.settled_securities {
            if *qty > 0 {
                let price = mark_to_market_fn(inst);
                equity += price * Decimal::from(*qty);
            }
        }
        equity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use honba_core::Exchange;
    use rust_decimal_macros::dec;

    #[test]
    fn test_t_plus_one_rolling_settlement() {
        let calendar = NseTradingCalendar::new();
        let mut ledger = SettlementLedger::new(dec!(500_000.00));
        let inst = InstrumentId::new(Exchange::Nse, "INFY");

        // Wednesday, Jan 15, 2025: Buy 100 INFY at ₹1800 + ₹200 fees = ₹180,200
        let trade_date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        ledger.record_delivery_buy(
            inst.clone(),
            Quantity::from_u64(100),
            dec!(180_200.00),
            trade_date,
            &calendar,
        );

        // On trade date T, settled cash and securities are unchanged
        assert_eq!(ledger.settled_cash(), dec!(500_000.00));
        assert_eq!(ledger.settled_position(&inst), 0);

        // Advance to Thursday, Jan 16, 2025 ($T+1$ settlement date)
        let t_plus_one = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        ledger.settle_up_to(t_plus_one);

        // Now cash is debited and shares are credited
        assert_eq!(ledger.settled_cash(), dec!(319_800.00));
        assert_eq!(ledger.settled_position(&inst), 100);
    }

    #[test]
    fn test_weekend_settlement_rolls_to_monday() {
        let calendar = NseTradingCalendar::new();
        let mut ledger = SettlementLedger::new(dec!(100_000.00));
        let inst = InstrumentId::new(Exchange::Nse, "TCS");

        // Friday, Jan 17, 2025: Sell 10 TCS netting ₹40,000
        let friday = NaiveDate::from_ymd_opt(2025, 1, 17).unwrap();
        ledger.record_delivery_sell(
            inst.clone(),
            Quantity::from_u64(10),
            dec!(40_000.00),
            friday,
            &calendar,
        );

        // On Saturday Jan 18, not settled
        ledger.settle_up_to(NaiveDate::from_ymd_opt(2025, 1, 18).unwrap());
        assert_eq!(ledger.settled_cash(), dec!(100_000.00));

        // On Monday Jan 20 ($T+1$ trading day), settles!
        let monday = NaiveDate::from_ymd_opt(2025, 1, 20).unwrap();
        ledger.settle_up_to(monday);
        assert_eq!(ledger.settled_cash(), dec!(140_000.00));
    }
}
