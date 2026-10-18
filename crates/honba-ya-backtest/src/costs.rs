//! Exact Indian equities transaction cost and statutory tax modeling using `rust_decimal::Decimal`.
//!
//! Models all SEBI and exchange statutory levies:
//! - STT (Securities Transaction Tax)
//! - Stamp Duty
//! - Exchange Turnover Charges (NSE/BSE)
//! - SEBI Turnover Fee (₹10 per crore)
//! - GST (18% on brokerage + exchange + SEBI fees)
//! - DP Charges (flat fee per scrip per day on delivery sell)
//! - Brokerage (configurable flat or percentage with cap)

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

use honba_core::OrderSide;

/// Product type for Indian equity orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum ProductType {
    /// Cash delivery (CNC - Cash and Carry)
    #[default]
    Delivery,
    /// Intraday equity (MIS - Margin Intraday Square-off)
    Intraday,
    /// Equity futures
    Futures,
    /// Equity options (STT on sell premium)
    Options,
}

/// Itemized fee breakdown for a single transaction or aggregated session.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct IndianFeeBreakdown {
    /// Broker commission/brokerage
    pub brokerage: Decimal,
    /// Securities Transaction Tax (STT)
    pub stt: Decimal,
    /// State stamp duty
    pub stamp_duty: Decimal,
    /// Exchange transaction / turnover charges
    pub exchange_turnover: Decimal,
    /// SEBI regulatory turnover fee
    pub sebi_turnover: Decimal,
    /// Goods and Services Tax (18%)
    pub gst: Decimal,
    /// Depository Participant charges (applicable on delivery sell debit)
    pub dp_charge: Decimal,
}

impl IndianFeeBreakdown {
    /// Total aggregate transaction cost in INR.
    pub fn total(&self) -> Decimal {
        self.brokerage
            + self.stt
            + self.stamp_duty
            + self.exchange_turnover
            + self.sebi_turnover
            + self.gst
            + self.dp_charge
    }

    /// Accumulate another fee breakdown into self.
    pub fn add_assign(&mut self, other: &IndianFeeBreakdown) {
        self.brokerage += other.brokerage;
        self.stt += other.stt;
        self.stamp_duty += other.stamp_duty;
        self.exchange_turnover += other.exchange_turnover;
        self.sebi_turnover += other.sebi_turnover;
        self.gst += other.gst;
        self.dp_charge += other.dp_charge;
    }
}

/// Configuration parameters for Indian brokerage and tax schedules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndianCostConfig {
    /// Brokerage rate fraction (e.g., 0.0003 for 0.03%)
    pub brokerage_pct: Decimal,
    /// Maximum brokerage cap per executed order in INR (e.g., ₹20)
    pub brokerage_cap: Decimal,
    /// Flat DP charge per scrip per day on debit (e.g., ₹13.50)
    pub dp_charge_flat: Decimal,
    /// GST rate (standard 18% = 0.18)
    pub gst_rate: Decimal,
}

impl Default for IndianCostConfig {
    fn default() -> Self {
        Self {
            brokerage_pct: dec!(0.0003), // 0.03%
            brokerage_cap: dec!(20.00),  // ₹20 cap
            dp_charge_flat: dec!(13.50), // ₹13.50 + 18% GST = ₹15.93
            gst_rate: dec!(0.18),        // 18%
        }
    }
}

/// Exact Indian statutory cost calculator.
#[derive(Debug, Clone, Default)]
pub struct IndianCostModel {
    config: IndianCostConfig,
}

impl IndianCostModel {
    /// Create a new cost model with default discount broker schedule.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with custom configuration.
    pub fn with_config(config: IndianCostConfig) -> Self {
        Self { config }
    }

    /// Calculate exact itemized statutory fees for an order fill.
    ///
    /// Rates per SEBI and Indian Exchange regulations:
    /// - **STT**:
    ///   - Delivery: 0.1% on buy & 0.1% on sell (dec!(0.001))
    ///   - Intraday: 0.025% on sell (dec!(0.00025)), 0 on buy
    ///   - Futures: 0.0125% on sell (dec!(0.000125))
    ///   - Options: 0.0625% on sell (dec!(0.000625))
    /// - **Stamp Duty**:
    ///   - Delivery: 0.015% on buy (dec!(0.00015)), 0 on sell
    ///   - Intraday: 0.003% on buy (dec!(0.00003)), 0 on sell
    /// - **Exchange Turnover**:
    ///   - NSE cash: 0.00297% (dec!(0.0000297))
    /// - **SEBI Turnover Fee**:
    ///   - ₹10 per crore (dec!(0.000001))
    /// - **GST**:
    ///   - 18% on (brokerage + exchange charges + SEBI charges)
    /// - **DP Charges**:
    ///   - Flat per debit day (e.g. ₹13.50 + 18% GST) on delivery sell
    pub fn compute_fees(
        &self,
        side: OrderSide,
        product: ProductType,
        notional: Decimal,
        is_first_sell_of_day_for_scrip: bool,
    ) -> IndianFeeBreakdown {
        if notional <= Decimal::ZERO {
            return IndianFeeBreakdown::default();
        }

        let is_buy = side == OrderSide::Buy;

        // 1. Brokerage
        let raw_brokerage = notional * self.config.brokerage_pct;
        let brokerage = raw_brokerage.min(self.config.brokerage_cap);

        // 2. Securities Transaction Tax (STT)
        let stt = match product {
            ProductType::Delivery => notional * dec!(0.001), // 0.1% both buy & sell
            ProductType::Intraday => {
                if !is_buy {
                    notional * dec!(0.00025) // 0.025% on sell only
                } else {
                    Decimal::ZERO
                }
            }
            ProductType::Futures => {
                if !is_buy {
                    notional * dec!(0.000125) // 0.0125% on sell
                } else {
                    Decimal::ZERO
                }
            }
            ProductType::Options => {
                if !is_buy {
                    notional * dec!(0.000625) // 0.0625% on sell premium
                } else {
                    Decimal::ZERO
                }
            }
        };

        // 3. Stamp Duty (applies on BUY side only per Indian Stamp Act amendment)
        let stamp_duty = if is_buy {
            match product {
                ProductType::Delivery => notional * dec!(0.00015), // 0.015%
                ProductType::Intraday => notional * dec!(0.00003), // 0.003%
                ProductType::Futures => notional * dec!(0.00002),  // 0.002%
                ProductType::Options => notional * dec!(0.00003),  // 0.003%
            }
        } else {
            Decimal::ZERO
        };

        // 4. Exchange Turnover Charges (NSE Cash: 0.00297%)
        let exchange_turnover = notional * dec!(0.0000297);

        // 5. SEBI Turnover Fee (₹10 per crore = ₹0.000001 per rupee of turnover)
        let sebi_turnover = notional * dec!(0.000001);

        // 6. DP Charges (flat debit fee on delivery sell)
        let dp_charge =
            if !is_buy && product == ProductType::Delivery && is_first_sell_of_day_for_scrip {
                self.config.dp_charge_flat
            } else {
                Decimal::ZERO
            };

        // 7. GST: 18% on (brokerage + exchange charges + SEBI charges + DP charge)
        let gst_taxable = brokerage + exchange_turnover + sebi_turnover + dp_charge;
        let gst = gst_taxable * self.config.gst_rate;

        IndianFeeBreakdown {
            brokerage: round_inr(brokerage),
            stt: round_inr(stt),
            stamp_duty: round_inr(stamp_duty),
            exchange_turnover: round_inr(exchange_turnover),
            sebi_turnover: round_inr(sebi_turnover),
            gst: round_inr(gst),
            dp_charge: round_inr(dp_charge),
        }
    }
}

/// Helper to round amounts to two decimal places (paise precision in INR).
fn round_inr(amount: Decimal) -> Decimal {
    amount.round_dp(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delivery_buy_costs() {
        let model = IndianCostModel::new();
        let notional = dec!(100_000.00); // ₹1 Lakh
        let fees = model.compute_fees(OrderSide::Buy, ProductType::Delivery, notional, false);

        // Brokerage: 0.03% of 100k = ₹30 capped at ₹20
        assert_eq!(fees.brokerage, dec!(20.00));
        // STT: 0.1% of 100k = ₹100
        assert_eq!(fees.stt, dec!(100.00));
        // Stamp Duty: 0.015% of 100k = ₹15
        assert_eq!(fees.stamp_duty, dec!(15.00));
        // Exchange fee: 0.00297% of 100k = ₹2.97
        assert_eq!(fees.exchange_turnover, dec!(2.97));
        // SEBI fee: ₹0.10
        assert_eq!(fees.sebi_turnover, dec!(0.10));
        // DP charge: ₹0 on buy
        assert_eq!(fees.dp_charge, dec!(0.00));
        // GST: 18% of (20 + 2.97 + 0.10) = 18% of 23.07 = ₹4.15
        assert_eq!(fees.gst, dec!(4.15));
        assert!(fees.total() > dec!(142.00));
    }

    #[test]
    fn test_delivery_sell_costs_with_dp_charge() {
        let model = IndianCostModel::new();
        let notional = dec!(100_000.00);
        let fees = model.compute_fees(OrderSide::Sell, ProductType::Delivery, notional, true);

        // STT on sell: ₹100
        assert_eq!(fees.stt, dec!(100.00));
        // Stamp duty: ₹0 on sell
        assert_eq!(fees.stamp_duty, dec!(0.00));
        // DP charge on first sell of day: ₹13.50
        assert_eq!(fees.dp_charge, dec!(13.50));
    }

    #[test]
    fn test_intraday_buy_has_no_stt() {
        let model = IndianCostModel::new();
        let notional = dec!(50_000.00);
        let fees = model.compute_fees(OrderSide::Buy, ProductType::Intraday, notional, false);

        assert_eq!(fees.stt, dec!(0.00));
        // Stamp duty: 0.003% of 50k = ₹1.50
        assert_eq!(fees.stamp_duty, dec!(1.50));
    }
}
