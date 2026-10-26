//! National Stock Exchange of India (NSE) trading calendar and session rules.
//!
//! Models:
//! - Regular market hours (09:15 to 15:30 IST)
//! - Weekend exclusion (Saturdays & Sundays)
//! - Declared exchange holidays (Republic Day, Diwali, Independence Day, etc.)
//! - Special trading sessions such as annual Diwali **Muhurat trading** (18:15 to 19:15 IST)

use chrono::{DateTime, Datelike, Duration, FixedOffset, NaiveDate, NaiveTime, Utc, Weekday};
use honba_core::TradingCalendar;
use std::collections::HashSet;

/// Session window on a trading date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionWindow {
    /// Session opening time (IST)
    pub open: NaiveTime,
    /// Session closing time (IST)
    pub close: NaiveTime,
}

impl SessionWindow {
    /// Standard NSE cash equity continuous trading session: 09:15 to 15:30 IST.
    pub fn regular() -> Self {
        Self {
            open: NaiveTime::from_hms_opt(9, 15, 0).unwrap(),
            close: NaiveTime::from_hms_opt(15, 30, 0).unwrap(),
        }
    }

    /// Special Diwali Muhurat trading session (1 hour in the evening): 18:15 to 19:15 IST.
    pub fn muhurat() -> Self {
        Self {
            open: NaiveTime::from_hms_opt(18, 15, 0).unwrap(),
            close: NaiveTime::from_hms_opt(19, 15, 0).unwrap(),
        }
    }

    /// Check if an IST time falls within this session window.
    pub fn contains(&self, time: NaiveTime) -> bool {
        time >= self.open && time <= self.close
    }
}

/// NSE trading calendar managing holidays, trading days, and special sessions.
#[derive(Debug, Clone)]
pub struct NseTradingCalendar {
    holidays: HashSet<NaiveDate>,
    muhurat_dates: HashSet<NaiveDate>,
    ist_offset: FixedOffset,
}

impl Default for NseTradingCalendar {
    fn default() -> Self {
        Self::new()
    }
}

impl NseTradingCalendar {
    /// Create a new NSE trading calendar pre-loaded with standard national holidays and Muhurat dates.
    pub fn new() -> Self {
        let mut cal = Self {
            holidays: HashSet::new(),
            muhurat_dates: HashSet::new(),
            ist_offset: FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap(), // UTC+05:30
        };
        cal.seed_standard_holidays();
        cal
    }

    /// Add a market holiday.
    pub fn add_holiday(&mut self, date: NaiveDate) {
        self.holidays.insert(date);
    }

    /// Add a special Muhurat trading session date.
    pub fn add_muhurat_session(&mut self, date: NaiveDate) {
        self.muhurat_dates.insert(date);
    }

    /// Check if a date is a declared market holiday.
    pub fn is_holiday(&self, date: NaiveDate) -> bool {
        self.holidays.contains(&date)
    }

    /// Check if a date hosts an annual Diwali Muhurat trading session.
    pub fn is_muhurat_session(&self, date: NaiveDate) -> bool {
        self.muhurat_dates.contains(&date)
    }

    /// Check if a date is a recognized NSE trading day.
    ///
    /// Regular weekdays that are not declared holidays, or special Muhurat session dates.
    pub fn is_trading_day(&self, date: NaiveDate) -> bool {
        if self.is_muhurat_session(date) {
            return true;
        }
        let weekday = date.weekday();
        if weekday == Weekday::Sat || weekday == Weekday::Sun {
            return false;
        }
        !self.is_holiday(date)
    }

    /// Get the applicable session window for a date.
    pub fn session_for_date(&self, date: NaiveDate) -> Option<SessionWindow> {
        if self.is_muhurat_session(date) {
            Some(SessionWindow::muhurat())
        } else if self.is_trading_day(date) {
            Some(SessionWindow::regular())
        } else {
            None
        }
    }

    /// Find the next trading day strictly after `date`.
    pub fn next_trading_day(&self, mut date: NaiveDate) -> NaiveDate {
        loop {
            date += Duration::days(1);
            if self.is_trading_day(date) {
                return date;
            }
        }
    }

    /// IST offset (UTC+05:30) used to convert UTC timestamps for session checks.
    pub fn ist_offset(&self) -> FixedOffset {
        self.ist_offset
    }

    /// Every declared holiday date, as a flat list. This is the real NSE
    /// holiday *data* this calendar owns; it gets injected into
    /// `honba_portfolio::ClearingCalendar::with_holidays` so that crate
    /// never duplicates or hardcodes its own (potentially diverging) copy
    /// of the same table. Muhurat session dates are intentionally
    /// excluded: they are trading days (`is_trading_day` returns `true`
    /// for them), not holidays.
    pub fn holidays(&self) -> Vec<NaiveDate> {
        self.holidays.iter().copied().collect()
    }

    /// Check if a UTC timestamp falls within open NSE market hours.
    pub fn is_market_open(&self, utc_time: DateTime<Utc>) -> bool {
        let ist_time = utc_time.with_timezone(&self.ist_offset);
        let date = ist_time.date_naive();
        if let Some(session) = self.session_for_date(date) {
            session.contains(ist_time.time())
        } else {
            false
        }
    }

    fn seed_standard_holidays(&mut self) {
        // Standard declared NSE holidays across recent years
        let holidays = [
            // 2025
            NaiveDate::from_ymd_opt(2025, 1, 26).unwrap(), // Republic Day
            NaiveDate::from_ymd_opt(2025, 2, 26).unwrap(), // Mahashivratri
            NaiveDate::from_ymd_opt(2025, 3, 14).unwrap(), // Holi
            NaiveDate::from_ymd_opt(2025, 3, 31).unwrap(), // Id-Ul-Fitr
            NaiveDate::from_ymd_opt(2025, 4, 10).unwrap(), // Mahavir Jayanti
            NaiveDate::from_ymd_opt(2025, 4, 14).unwrap(), // Dr. Ambedkar Jayanti
            NaiveDate::from_ymd_opt(2025, 4, 18).unwrap(), // Good Friday
            NaiveDate::from_ymd_opt(2025, 5, 1).unwrap(),  // Maharashtra Day
            NaiveDate::from_ymd_opt(2025, 8, 15).unwrap(), // Independence Day
            NaiveDate::from_ymd_opt(2025, 8, 27).unwrap(), // Ganesh Chaturthi
            NaiveDate::from_ymd_opt(2025, 10, 2).unwrap(), // Mahatma Gandhi Jayanti
            NaiveDate::from_ymd_opt(2025, 10, 21).unwrap(), // Diwali Laxmi Pujan (Muhurat session evening)
            NaiveDate::from_ymd_opt(2025, 10, 22).unwrap(), // Diwali Balipratipada
            NaiveDate::from_ymd_opt(2025, 11, 5).unwrap(),  // Prakash Gurpurb
            NaiveDate::from_ymd_opt(2025, 12, 25).unwrap(), // Christmas
            // 2026
            NaiveDate::from_ymd_opt(2026, 1, 26).unwrap(), // Republic Day
            NaiveDate::from_ymd_opt(2026, 3, 3).unwrap(),  // Holi
            NaiveDate::from_ymd_opt(2026, 4, 3).unwrap(),  // Good Friday
            NaiveDate::from_ymd_opt(2026, 4, 14).unwrap(), // Ambedkar Jayanti
            NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),  // Maharashtra Day
            NaiveDate::from_ymd_opt(2026, 8, 15).unwrap(), // Independence Day
            NaiveDate::from_ymd_opt(2026, 10, 2).unwrap(), // Gandhi Jayanti
            NaiveDate::from_ymd_opt(2026, 11, 8).unwrap(), // Diwali Laxmi Pujan (Muhurat)
            NaiveDate::from_ymd_opt(2026, 12, 25).unwrap(), // Christmas
        ];

        for h in holidays {
            self.holidays.insert(h);
        }

        // Register special Muhurat sessions
        self.muhurat_dates
            .insert(NaiveDate::from_ymd_opt(2025, 10, 21).unwrap());
        self.muhurat_dates
            .insert(NaiveDate::from_ymd_opt(2026, 11, 8).unwrap());
    }
}

/// `NseTradingCalendar` implements the canonical object-safe
/// [`honba_core::TradingCalendar`] port, allowing the backtest engine and
/// enhancer optimizers to be decoupled from the concrete NSE venue.
impl TradingCalendar for NseTradingCalendar {
    fn is_trading_day(&self, date: NaiveDate) -> bool {
        self.is_trading_day(date)
    }

    fn is_holiday(&self, date: NaiveDate) -> bool {
        self.is_holiday(date)
    }

    fn session_for_date(&self, date: NaiveDate) -> Option<honba_core::SessionWindow> {
        self.session_for_date(date)
            .map(|sw| honba_core::SessionWindow {
                open: sw.open,
                close: sw.close,
            })
    }

    fn holidays(&self) -> Vec<NaiveDate> {
        self.holidays()
    }

    fn timezone_offset(&self) -> FixedOffset {
        self.ist_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regular_weekday_is_trading_day() {
        let cal = NseTradingCalendar::new();
        // Wednesday, Jan 15, 2025
        let d = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        assert!(cal.is_trading_day(d));
        let session = cal.session_for_date(d).unwrap();
        assert_eq!(session, SessionWindow::regular());
    }

    #[test]
    fn test_weekend_and_holidays_excluded() {
        let cal = NseTradingCalendar::new();
        // Saturday, Jan 18, 2025
        let sat = NaiveDate::from_ymd_opt(2025, 1, 18).unwrap();
        assert!(!cal.is_trading_day(sat));

        // Independence Day: Aug 15, 2025 (Friday)
        let holiday = NaiveDate::from_ymd_opt(2025, 8, 15).unwrap();
        assert!(cal.is_holiday(holiday));
        assert!(!cal.is_trading_day(holiday));

        // Next trading day after Thursday Aug 14 is Monday Aug 18
        let aug14 = NaiveDate::from_ymd_opt(2025, 8, 14).unwrap();
        let next = cal.next_trading_day(aug14);
        assert_eq!(next, NaiveDate::from_ymd_opt(2025, 8, 18).unwrap());
    }

    #[test]
    fn test_muhurat_trading_session() {
        let cal = NseTradingCalendar::new();
        let diwali = NaiveDate::from_ymd_opt(2025, 10, 21).unwrap();
        assert!(cal.is_muhurat_session(diwali));
        assert!(cal.is_trading_day(diwali));

        let session = cal.session_for_date(diwali).unwrap();
        assert_eq!(session, SessionWindow::muhurat());
        assert_eq!(session.open, NaiveTime::from_hms_opt(18, 15, 0).unwrap());
        assert_eq!(session.close, NaiveTime::from_hms_opt(19, 15, 0).unwrap());
    }
}
