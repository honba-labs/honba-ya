//! The [`StrategyContext`] port: a strategy's only view of the world (ADR 008).
//!
//! A strategy reads the clock, its positions, cash and instrument metadata,
//! and submits [`OrderIntent`]s through the context. It never touches the
//! execution engine, I/O or the wall clock, so the same strategy runs
//! unchanged in backtest, paper and live. Mirrors the Python
//! `honba.strategies.context.StrategyContext` ABC.

use std::collections::BTreeMap;

use honba_core::chrono::{DateTime, Utc};
use honba_core::{Currency, FillReport, InstrumentId, Money, OrderSide, Price, Quantity};
use rust_decimal::Decimal;

use crate::error::{Result, StrategyError};
use crate::intent::OrderIntent;

/// What a strategy may read and do. Implementations are supplied by the
/// runner; every [`Strategy`](crate::Strategy) hook receives one as
/// `&mut dyn StrategyContext`.
pub trait StrategyContext {
    /// The `ts_init` of the event being processed; zero before the first event.
    fn now(&self) -> DateTime<Utc>;

    /// Net signed quantity held (positive long, negative short), updated from fills.
    fn position(&self, instrument_id: &InstrumentId) -> Decimal;

    /// Every non-flat position, ordered by instrument id (symbol, then exchange).
    fn positions(&self) -> Vec<(InstrumentId, Decimal)>;

    /// Initial cash plus the net cash flow of all fills (buys debit
    /// notional plus costs, sells credit notional minus costs), in minor units.
    fn cash(&self) -> Money;

    /// `true` while an intent submitted for `instrument_id` is not fully
    /// filled or rejected.
    fn busy(&self, instrument_id: &InstrumentId) -> bool;

    /// Instrument metadata (lot and tick size), if the run knows it.
    fn instrument(&self, instrument_id: &InstrumentId) -> Option<&InstrumentInfo>;

    /// Queues an intent; the runner turns it into an order after the current
    /// hook returns.
    fn submit(&mut self, intent: OrderIntent);
}

/// Instrument metadata (lot and tick size).
///
/// No `Default` derive: `honba_core::InstrumentId` has no default value (an
/// instrument always names a concrete exchange and symbol).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstrumentInfo {
    /// The instrument this metadata describes.
    pub id: InstrumentId,
    /// The exchange-mandated lot size (orders must be a multiple of this).
    pub lot_size: Quantity,
    /// The exchange-mandated minimum price increment.
    pub tick_size: Price,
}

/// Unfilled quantity below this counts as filled (float noise from partial fills).
const EPSILON: Decimal = Decimal::ZERO;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Pending {
    buy: Decimal,
    sell: Decimal,
}

/// The reference [`StrategyContext`]: a deterministic in-memory ledger.
///
/// The runner sets the clock, applies fills and releases rejected intents;
/// the strategy reads it and submits intents, which the runner drains. It is
/// pure (no I/O, no wall clock), so backtest, paper and live share it.
/// Mirrors the Python `honba.strategies.context.LedgerContext`.
#[derive(Clone, Debug)]
pub struct LedgerContext {
    now: DateTime<Utc>,
    cash: Money,
    currency: Currency,
    positions: BTreeMap<InstrumentId, Decimal>,
    pending: BTreeMap<InstrumentId, Pending>,
    instruments: BTreeMap<InstrumentId, InstrumentInfo>,
    outbox: Vec<OrderIntent>,
}

impl Default for LedgerContext {
    fn default() -> Self {
        Self {
            now: DateTime::<Utc>::MIN_UTC,
            cash: Money::zero_inr(),
            currency: Currency::Inr,
            positions: Default::default(),
            pending: Default::default(),
            instruments: Default::default(),
            outbox: Default::default(),
        }
    }
}

impl LedgerContext {
    /// An empty ledger with zero cash, in the given currency.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty ledger starting with `cash`.
    pub fn with_cash(cash: Money) -> Self {
        Self {
            currency: cash.currency,
            cash,
            ..Self::default()
        }
    }

    /// The currency this ledger settles in.
    pub fn currency(&self) -> Currency {
        self.currency
    }

    /// Sets the clock to the `ts_init` of the event about to be processed.
    pub fn set_now(&mut self, ts_init: DateTime<Utc>) {
        self.now = ts_init;
    }

    /// Registers instrument metadata for [`StrategyContext::instrument`].
    pub fn add_instrument(&mut self, instrument: InstrumentInfo) {
        self.instruments.insert(instrument.id.clone(), instrument);
    }

    /// Sets the signed position of `instrument_id` without booking a fill: no cash moves.
    /// For runs that start from an existing portfolio (for example a reduce-only run).
    pub fn seed_position(&mut self, instrument_id: &InstrumentId, quantity: Decimal) {
        self.positions.insert(instrument_id.clone(), quantity);
    }

    /// Returns and clears submitted intents, in submission order.
    pub fn drain_intents(&mut self) -> Vec<OrderIntent> {
        std::mem::take(&mut self.outbox)
    }

    /// Books a fill: position, cash (notional plus costs) and the pending
    /// quantity of the instrument.
    ///
    /// The notional rounds to minor units once, at the point of booking, and
    /// the costs are already integer `Money` (ADR 0011), so cash accumulates
    /// integers. Booking is all-or-nothing: a fill whose notional cannot be
    /// represented, whose costs are in another currency, or that would
    /// overflow the balance is refused with an error and leaves the ledger
    /// untouched, rather than moving the position without the cash.
    pub fn apply_fill(&mut self, fill: &FillReport) -> Result<()> {
        if fill.fee.currency != self.currency {
            return Err(StrategyError::other(format!(
                "fill fee currency {:?} does not match ledger currency {:?}",
                fill.fee.currency, self.currency
            )));
        }
        let notional = (fill.price.0 * fill.quantity.0).min(Decimal::MAX);
        let costs = fill.fee.amount;

        let delta = if fill.side == OrderSide::Buy {
            -(notional + costs)
        } else {
            notional - costs
        };
        self.cash = Money::new(self.cash.amount + delta, self.cash.currency);

        let signed = if fill.side == OrderSide::Buy {
            fill.quantity.0
        } else {
            -fill.quantity.0
        };
        *self
            .positions
            .entry(fill.instrument.clone())
            .or_insert(Decimal::ZERO) += signed;
        self.reduce_pending(&fill.instrument, fill.side, fill.quantity.0);
        Ok(())
    }

    /// An intent was rejected or its order cancelled unfilled: it no longer
    /// counts towards [`StrategyContext::busy`].
    ///
    /// An intent that fails [`OrderIntent::validate`] never counted (see
    /// `submit`), so releasing it is a no-op.
    pub fn release(&mut self, intent: &OrderIntent) {
        if intent.validate().is_err() {
            return;
        }
        self.reduce_pending(&intent.instrument_id, intent.side, intent.quantity.0);
    }

    /// Releases `quantity` of the pending `side` quantity of `instrument_id`:
    /// the unfilled remainder of an order the execution port rejected or
    /// cancelled. A non-positive or non-finite quantity is a no-op.
    pub fn release_remainder(
        &mut self,
        instrument_id: &InstrumentId,
        side: OrderSide,
        quantity: Decimal,
    ) {
        if quantity > Decimal::ZERO {
            self.reduce_pending(instrument_id, side, quantity);
        }
    }

    fn reduce_pending(&mut self, instrument_id: &InstrumentId, side: OrderSide, qty: Decimal) {
        let Some(p) = self.pending.get_mut(instrument_id) else {
            return;
        };
        let slot = if side == OrderSide::Buy {
            &mut p.buy
        } else {
            &mut p.sell
        };
        let left = *slot - qty;
        *slot = if left > EPSILON { left } else { Decimal::ZERO };
        if *p == Pending::default() {
            self.pending.remove(instrument_id);
        }
    }
}

impl StrategyContext for LedgerContext {
    fn now(&self) -> DateTime<Utc> {
        self.now
    }

    fn position(&self, instrument_id: &InstrumentId) -> Decimal {
        self.positions.get(instrument_id).copied().unwrap_or(Decimal::ZERO)
    }

    fn positions(&self) -> Vec<(InstrumentId, Decimal)> {
        self.positions
            .iter()
            .filter(|(_, q)| **q != Decimal::ZERO)
            .map(|(id, q)| (id.clone(), *q))
            .collect()
    }

    fn cash(&self) -> Money {
        self.cash
    }

    fn busy(&self, instrument_id: &InstrumentId) -> bool {
        self.pending
            .get(instrument_id)
            .is_some_and(|p| p.buy > Decimal::ZERO || p.sell > Decimal::ZERO)
    }

    fn instrument(&self, instrument_id: &InstrumentId) -> Option<&InstrumentInfo> {
        self.instruments.get(instrument_id)
    }

    fn submit(&mut self, intent: OrderIntent) {
        // An invalid intent is still queued so the runner can reject it, but
        // it must not touch the pending ledger (a NaN would corrupt it).
        if intent.validate().is_ok() {
            let p = self
                .pending
                .entry(intent.instrument_id.clone())
                .or_default();
            if intent.side == OrderSide::Buy {
                p.buy += intent.quantity.0;
            } else {
                p.sell += intent.quantity.0;
            }
        }
        self.outbox.push(intent);
    }
}

// A `CacheContext` implementation used to live here, backed directly by
// `honba_engine::StateCache` behind a `#[cfg(feature = "engine")]` gate.
// `honba-engine` has been fully removed from this crate's dependency graph
// (see Cargo.toml) and there is no `"engine"` feature left to gate it on, so
// that type could never be constructed; it has been deleted rather than kept
// as permanently-dead code. `LedgerContext` is the one `StrategyContext`
// implementation this crate provides now.