//! Intraday MIS (Margin Intraday Square-off) auto-square-off engine.
//!
//! The square-off time is derived from the trading calendar's session
//! window (configurable offset before session close) rather than hardcoded
//! to 15:20 IST, supporting any exchange venue (Phase 4.4 items 2 & 4).

use chrono::{DateTime, Timelike, Utc};
use rust_decimal::Decimal;
use std::collections::HashSet;
use std::sync::Arc;

use honba_core::{InstrumentId, OrderSide, Price, TradingCalendar};
use honba_mon::RiskRefusal;

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
/// Enforces intraday square-off at a configurable offset before session
/// close (e.g. 10 minutes prior to 15:30 IST → 15:20 IST for NSE) where
/// all open intraday (MIS) positions are forcibly squared off.
///
/// The square-off time is derived from the trading calendar's session
/// window rather than hardcoded, supporting any exchange venue
/// (Phase 4.4 items 2 & 4).
#[derive(Clone)]
pub struct MisSquareOffManager {
    /// Object-safe trading calendar for session times and holiday checks.
    calendar: Arc<dyn TradingCalendar>,
    /// Minutes before session close at which square-off is triggered
    /// (default 10 minutes, i.e. 15:20 IST for a 15:30 IST close).
    square_off_offset_minutes: i64,
    /// Instruments that have been square-off eligible today
    square_off_eligible: HashSet<InstrumentId>,
}

impl std::fmt::Debug for MisSquareOffManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MisSquareOffManager")
            .field("square_off_offset_minutes", &self.square_off_offset_minutes)
            .field("square_off_eligible", &self.square_off_eligible)
            .field("calendar", &format_args!("Arc<dyn TradingCalendar>"))
            .finish()
    }
}

impl MisSquareOffManager {
    /// Create a new MIS square-off manager with a 10-minute pre-close
    /// square-off offset (15:20 IST for NSE's 15:30 IST close).
    pub fn new(calendar: Arc<dyn TradingCalendar>) -> Self {
        Self {
            calendar,
            square_off_offset_minutes: 10,
            square_off_eligible: HashSet::new(),
        }
    }

    /// Create a new MIS square-off manager with a custom offset before
    /// session close.
    pub fn with_offset(calendar: Arc<dyn TradingCalendar>, square_off_offset_minutes: i64) -> Self {
        Self {
            calendar,
            square_off_offset_minutes,
            square_off_eligible: HashSet::new(),
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
        _instrument: &InstrumentId,
        timestamp: DateTime<Utc>,
    ) -> Option<SquareOffResult> {
        let local_time = timestamp.with_timezone(&self.calendar.timezone_offset());
        let local_date = local_time.date_naive();
        let local_time_only = local_time.time();

        let session = self.calendar.session_for_date(local_date)?;
        let close = session.close;

        // Square-off starts `offset` minutes before session close.
        // NaiveTime has no checked_sub_signed, so work in seconds from midnight.
        let close_secs = close.num_seconds_from_midnight();
        let offset_secs: u32 = (self.square_off_offset_minutes * 60) as u32;
        let square_off_start_secs = close_secs.saturating_sub(offset_secs);
        let square_off_start =
            chrono::NaiveTime::from_num_seconds_from_midnight_opt(square_off_start_secs, 0)
                .unwrap_or(close);

        if local_time_only >= square_off_start && local_time_only < close {
            // Square-off window active
            let reason = format!(
                "MIS square-off {} minutes before close ({}); time is {:}",
                self.square_off_offset_minutes,
                close.format("%H:%M"),
                local_time.format("%H:%M:%S")
            );
            Some(SquareOffResult::ForcedSquareOff {
                reason,
                square_off_price: Price(Decimal::ZERO),
            })
        } else if local_time_only >= close {
            // Market closed
            Some(SquareOffResult::ForcedSquareOff {
                reason: format!("market close at {}", close.format("%H:%M")),
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
        _side: OrderSide,
        _quantity: i64,
        timestamp: DateTime<Utc>,
    ) -> Result<Option<SquareOffResult>, Box<RiskRefusal>> {
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
