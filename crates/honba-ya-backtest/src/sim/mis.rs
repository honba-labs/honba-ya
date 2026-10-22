//! Intraday MIS (Margin Intraday Square-off) 15:20 IST auto-square-off engine.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashSet;

use honba_core::{InstrumentId, OrderSide, Price};
use honba_mon::RiskRefusal;

use crate::calendar::NseTradingCalendar;

/// Square-off result for an order/trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SquareOffResult {
    /// Order is within square-off window, no action needed
    NoAction,
    /// Order will be square-off forced at 15:20 IST
    ForcedSquareOff {
        /// Reason for square-off
        reason: String,
        /// Estimated square-off price (mid-price at 15:20)
        square_off_price: Price,
    },
    /// Order already square-off eligible (e.g. closing position)
    AlreadyClosed {
        /// Net position change
        net_position_change: i64,
    },
}

/// MIS square-off manager for intraday risk control.
///
/// Enforces SEBI/MARGIN INTRADAY SQUARE-OFF at 15:20 IST where all
/// open intraday (MIS) positions are forcibly squared off, squaring off
/// buy and sell positions at the prevailing market price.
#[derive(Debug, Clone)]
pub struct MisSquareOffManager {
    /// NSE trading calendar for holiday checks
    calendar: NseTradingCalendar,
    /// Instruments that have been square-off eligible today
    square_off_eligible: HashSet<InstrumentId>,
    /// Track square-off timestamps per instrument
    square_off_timestamps: HashSet<(InstrumentId, DateTime<Utc>)>,
}

impl MisSquareOffManager {
    /// Create a new MIS square-off manager.
    pub fn new(calendar: NseTradingCalendar) -> Self {
        Self {
            calendar,
            square_off_eligible: HashSet::new(),
            square_off_timestamps: HashSet::new(),
        }
    }

    /// Check if an instrument is eligible for square-off today.
    pub fn is_eligible(&self, instrument: &InstrumentId) -> bool {
        self.square_off_eligible.contains(instrument)
    }

    /// Mark an instrument as square-off eligible for today.
    pub fn mark_eligible(&mut self, instrument: InstrumentId) {
        self.square_off_eligible.insert(instrument);
    }

    /// Check if square-off has been triggered for an instrument at the given time.
    pub fn check_square_off_time(
        &self,
        instrument: &InstrumentId,
        timestamp: DateTime<Utc>,
    ) -> Option<SquareOffResult> {
        // Check if we're past 15:20 IST
        let ist_time = timestamp.with_timezone(&self.calendar.ist_offset());
        let ist_time_only = ist_time.time();

        // NSE market closes at 15:30 IST, square-off at 15:20 IST
        let square_off_time = chrono::NaiveTime::from_hms_opt(15, 20, 0).unwrap();
        let market_close_time = chrono::NaiveTime::from_hms_opt(15, 30, 0).unwrap();

        if ist_time_only >= square_off_time && ist_time_only < market_close_time {
            // Square-off window active
            let reason = format!(
                "MIS square-off at 15:20 IST; time is {:}",
                ist_time.format("%H:%M:%S")
            );
            Some(SquareOffResult::ForcedSquareOff {
                reason,
                square_off_price: Price(Decimal::ZERO), // Would be mid-price at square-off
            })
        } else if ist_time_only >= market_close_time {
            // Market closed
            Some(SquareOffResult::ForcedSquareOff {
                reason: "NSE market close at 15:30 IST".to_string(),
                square_off_price: Price(Decimal::ZERO),
            })
        } else {
            None // Outside square-off window
        }
    }

    /// Process a trade and determine if square-off action is needed.
    pub fn process_trade(
        &mut self,
        instrument: InstrumentId,
        side: OrderSide,
        quantity: i64,
        timestamp: DateTime<Utc>,
    ) -> Result<Option<SquareOffResult>, RiskRefusal> {
        // Check if this is a closing order (reduces existing position)
        // If it reduces or eliminates a position, no square-off needed
        // If it increases an open MIS position and we're past square-off time, refuse

        let square_off_result = self.check_square_off_time(&instrument, timestamp);

        if let Some(result) = square_off_result {
            // Square-off is active - refuse new orders that increase MIS position
            // (or force square-off depending on policy)
            Ok(Some(result))
        } else {
            // Update eligibility tracking
            self.mark_eligible(instrument);
            Ok(None)
        }
    }

    /// Get square-off eligibility per instrument.
    pub fn eligibility_status(&self) -> &HashSet<InstrumentId> {
        &self.square_off_eligible
    }
}