//! Top-of-book quote/trade matching (ADR 0030): [`L2Sim`] over an [`L2Book`].
//!
//! An opt-in [`honba_engine::ExecutionEngine`] next to the L1 simulators; it never changes
//! them (ADR 0025 decisions 4-5). The stream is `QuoteTick` (the book) plus `TradeTick`
//! (prints), at nanosecond event time, with no wall clock.
//!
//! Semantics (v1):
//!
//! - **Market**: fills at the opposite touch, up to the touch size; a remainder keeps working
//!   and fills against later quotes (cancelled under IOC).
//! - **Marketable limit**: walks the touch only (one level) at the touch price; the remainder
//!   rests at its limit (cancelled under IOC). Consumed touch size is gone until the next quote.
//! - **Resting limit**: fills at its limit when a `TradeTick` prints through it (buy:
//!   `price <= limit`, sell: `price >= limit`), FIFO by submission, sharing the print's size. A
//!   new quote that crosses a resting limit fills it at the touch.
//! - **Stop / stop-limit**: armed until a `TradeTick` crosses `trigger_price` (buy:
//!   `price >= trigger`, sell: `price <= trigger`), then behaves as a market / limit order. A
//!   stop is not triggered by a print that predates its submission. Trailing stops are refused
//!   (`unsupported_order_type`), as in the L1 simulator.
//! - **IOC / FOK**: judged once, against the book at submit (or trigger) time. FOK is
//!   all-or-nothing against the touch size, else rejected `fok_unfillable`.
//! - **Day**: expires at the session close (the profile calendar's close as a UTC time of day,
//!   default 23:59:00 UTC like [`crate::margin::default_session_close_ts`]) with an `Expired` at
//!   the close instant; `GTC`/`GTD` never expire (`GTD` carries no date). Contract expiry
//!   (`ExpiryRules`) is derivative-specific and not used.
//! - **Cancel** (ADR 0019 decision 6): `CancelRequested` and `Cancelled` are both stamped `now`.
//!
//! Costs, lots, long-only sell caps, funding cuts and MIS tracking follow
//! [`crate::NextOpenSim`]; there is no settlement cycle (cash moves at the fill). The
//! [`crate::depth_survival_discount`] stub stays unwired: matching here is instantaneous at
//! event time (latency only shifts the fill timestamp), so no in-flight interval exists to
//! discount the quote size by; ADR 0030 open item.

mod book;
mod sim;

pub use book::L2Book;
pub use sim::L2Sim;
