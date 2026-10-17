//! [`L2Sim`]: the ADR 0030 top-of-book matching engine.

use chrono::Timelike;
use honba_engine::{
    AlgoError, EngineOutput, ExecutionEngine, Handler, LegacyDrains, OrderRejection, Result,
};
use honba_entities::{Currency, ExecutionEvent, Money, Trade};
use honba_market::costs::{CostSchedule, MarketSegment};
use honba_market::settlement::MarginModel;
use honba_market::MarketProfile;
use honba_messages::{
    Event, InstrumentId, Order, OrderId, OrderSide, OrderType, QuoteTick, TimeInForce, TradeTick,
    UnixNanos,
};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::Arc;

use super::book::L2Book;
use crate::cost::ScheduleFillCost;
use crate::fill_model::{FillModel, NoSlippage, SlippageModel};
use crate::latency::{LatencyModel, NoLatency};
use crate::margin::MisPositions;
use crate::next_open::FillCostFn;

/// Quantities closer than this are equal (float residue from fractional fills).
const QTY_EPS: f64 = 1e-9;
const NS_PER_DAY: u64 = 86_400_000_000_000;
/// 23:59:00 UTC in nanoseconds into the day (the L1 default session close).
const DEFAULT_CLOSE_NS: u64 = 86_340_000_000_000;

fn err(msg: String) -> AlgoError {
    AlgoError::Component(msg)
}

/// One working order.
struct Working {
    order: Order,
    remaining: f64,
    filled: f64,
    /// Limit price (Limit and StopLimit).
    limit: Option<f64>,
    /// Stop price while the stop is armed.
    armed: Option<f64>,
    /// Day expiry instant.
    expire_at: Option<u64>,
}

/// An [`ExecutionEngine`] matching orders against a top-of-book `QuoteTick` / `TradeTick` stream.
///
/// See the [module docs](crate::l2) for the matching rules. Events come out of one ordered queue
/// ([`ExecutionEngine::drain_events`]). Like [`crate::NextOpenSim`] it never acknowledges.
pub struct L2Sim {
    cash: i64,
    currency: Currency,
    fees: i64,
    traded_notional: i64,
    cost_fn: FillCostFn,
    fill_model: Arc<dyn FillModel>,
    latency_model: Arc<dyn LatencyModel>,
    rng: StdRng,
    long_only: bool,
    lot_sizes: Vec<(InstrumentId, f64)>,
    positions: Vec<(InstrumentId, f64)>,
    mis: MisPositions,
    margin: Option<Arc<dyn MarginModel>>,
    /// Nanoseconds into the UTC day at which Day orders expire.
    close_ns: u64,
    books: Vec<(InstrumentId, L2Book)>,
    now: u64,
    working: Vec<Working>,
    events: Vec<ExecutionEvent>,
    legacy: LegacyDrains,
}

impl L2Sim {
    /// Creates a simulator holding `cash` (not negative), long only.
    pub fn new(cash: Money) -> Result<Self> {
        if cash.minor() < 0 {
            return Err(err("cash must be >= 0".into()));
        }
        Ok(Self {
            cash: cash.minor(),
            currency: cash.currency(),
            fees: 0,
            traded_notional: 0,
            cost_fn: Box::new(|_, _, _| Ok(Money::new(0, Currency::Inr))),
            fill_model: Arc::new(NoSlippage),
            latency_model: Arc::new(NoLatency),
            rng: StdRng::seed_from_u64(0),
            long_only: true,
            lot_sizes: Vec::new(),
            positions: Vec::new(),
            mis: MisPositions::new(),
            margin: None,
            close_ns: DEFAULT_CLOSE_NS,
            books: Vec::new(),
            now: 0,
            working: Vec::new(),
            events: Vec::new(),
            legacy: LegacyDrains::new(),
        })
    }

    /// Sets the fill cost function (default: no costs).
    #[must_use]
    pub fn with_costs(mut self, cost_fn: FillCostFn) -> Self {
        self.cost_fn = cost_fn;
        self
    }

    /// Charges each fill from a market [`CostSchedule`] (see [`ScheduleFillCost`]).
    #[must_use]
    pub fn with_schedule(
        mut self,
        schedule: Arc<dyn CostSchedule>,
        segment: MarketSegment,
    ) -> Self {
        self.cost_fn = ScheduleFillCost::new(schedule, segment, self.currency).into_fn();
        self
    }

    /// Sets whether a sell is capped at the position held (default true).
    #[must_use]
    pub fn with_long_only(mut self, long_only: bool) -> Self {
        self.long_only = long_only;
        self
    }

    /// Installs the fill model applied to aggressive (touch) fills; resting limit fills print
    /// at their limit and draw nothing. Default [`NoSlippage`].
    #[must_use]
    pub fn with_fill_model(mut self, model: Arc<dyn FillModel>) -> Self {
        self.fill_model = model;
        self
    }

    /// Installs the latency model that delays each fill's timestamp (default [`NoLatency`]).
    #[must_use]
    pub fn with_latency_model(mut self, model: Arc<dyn LatencyModel>) -> Self {
        self.latency_model = model;
        self
    }

    /// Seeds the simulator's random stream.
    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.rng = StdRng::seed_from_u64(seed);
        self
    }

    /// Installs a [`SlippageModel`], seeding the simulator from its seed when it has one.
    #[must_use]
    pub fn with_slippage(self, model: SlippageModel) -> Self {
        let seed = model.seed();
        let sim = self.with_fill_model(Arc::new(model));
        match seed {
            Some(seed) => sim.with_seed(seed),
            None => sim,
        }
    }

    /// Installs the active market's [`MarginModel`], queried through [`Self::margin_query`].
    #[must_use]
    pub fn with_margin_model(mut self, model: Arc<dyn MarginModel>) -> Self {
        self.margin = Some(model);
        self
    }

    /// A [`crate::MarginQuery`] over the installed margin model, if any.
    pub fn margin_query(&self) -> Option<crate::margin::MarginQuery<'_>> {
        self.margin
            .as_ref()
            .map(|m| crate::margin::MarginQuery::new(m.as_ref()))
    }

    /// Takes the Day-order expiry time from `profile`'s calendar session close, read as a UTC
    /// time of day (the same convention as [`crate::margin::default_session_close_ts`]).
    #[must_use]
    pub fn with_market_profile(mut self, profile: &dyn MarketProfile) -> Self {
        let close = profile.calendar().session().close();
        self.close_ns = u64::from(close.num_seconds_from_midnight()) * 1_000_000_000
            + u64::from(close.nanosecond());
        self
    }

    /// Sets the quantity step a funding cut floors to for `instrument` (default 1).
    pub fn set_lot_size(&mut self, instrument: &InstrumentId, lot_size: f64) -> Result<()> {
        if !(lot_size.is_finite() && lot_size > 0.0) {
            return Err(err(format!("lot_size must be positive, got {lot_size}")));
        }
        match self.lot_sizes.iter_mut().find(|(i, _)| i == instrument) {
            Some(entry) => entry.1 = lot_size,
            None => self.lot_sizes.push((instrument.clone(), lot_size)),
        }
        Ok(())
    }

    /// Booked cash.
    pub fn cash(&self) -> Money {
        Money::new(self.cash, self.currency)
    }

    /// Transaction costs paid so far.
    pub fn fees(&self) -> Money {
        Money::new(self.fees, self.currency)
    }

    /// Sum of the notional of every fill.
    pub fn traded_notional(&self) -> Money {
        Money::new(self.traded_notional, self.currency)
    }

    /// The net signed position in `instrument` (0 when flat).
    pub fn position(&self, instrument: &InstrumentId) -> f64 {
        self.positions
            .iter()
            .find(|(i, _)| i == instrument)
            .map_or(0.0, |(_, q)| *q)
    }

    /// Every non-flat position, in the order first opened.
    pub fn positions(&self) -> Vec<(InstrumentId, f64)> {
        self.positions.clone()
    }

    /// Seeds or overwrites the net signed position in `instrument`.
    pub fn set_position(&mut self, instrument: &InstrumentId, quantity: f64) -> Result<()> {
        if !quantity.is_finite() {
            return Err(err(format!("position must be finite, got {quantity}")));
        }
        let flat = quantity.abs() <= QTY_EPS;
        match self.positions.iter().position(|(i, _)| i == instrument) {
            Some(n) if flat => {
                self.positions.remove(n);
            }
            Some(n) => self.positions[n].1 = quantity,
            None if !flat => self.positions.push((instrument.clone(), quantity)),
            None => {}
        }
        Ok(())
    }

    /// The instruments currently held under an intraday (MIS) product.
    pub fn mis_positions(&self) -> &[InstrumentId] {
        self.mis.intraday()
    }

    /// The current top of book of `instrument`, if a quote has arrived.
    pub fn book(&self, instrument: &InstrumentId) -> Option<L2Book> {
        self.books
            .iter()
            .find(|(i, _)| i == instrument)
            .map(|(_, b)| *b)
    }

    /// Ids of orders not yet filled, rejected, cancelled or expired, in submission order.
    pub fn working_orders(&self) -> Vec<String> {
        self.working
            .iter()
            .map(|w| w.order.order_id().as_str().to_string())
            .collect()
    }

    /// Observes one quote: updates the book, then matches working orders in time priority.
    pub fn on_quote(&mut self, q: &QuoteTick) -> Result<()> {
        let ts = q.ts_event().as_u64();
        self.advance(ts)?;
        let book = L2Book::from_quote(q);
        match self.books.iter_mut().find(|(i, _)| i == q.instrument_id()) {
            Some(entry) => entry.1 = book,
            None => self.books.push((q.instrument_id().clone(), book)),
        }
        let mut i = 0;
        while i < self.working.len() {
            let w = &self.working[i];
            let live = w.armed.is_none() && w.order.instrument_id() == q.instrument_id();
            if live && self.aggress(i, ts, false)? {
                continue;
            }
            i += 1;
        }
        Ok(())
    }

    /// Observes one print: fills resting limits it trades through (FIFO, sharing its size),
    /// then triggers the stops it crosses.
    pub fn on_trade(&mut self, t: &TradeTick) -> Result<()> {
        let ts = t.ts_event().as_u64();
        self.advance(ts)?;
        let px = t.price();
        let mut left = t.size();
        let mut i = 0;
        while i < self.working.len() {
            let w = &self.working[i];
            let through = w.armed.is_none()
                && w.order.instrument_id() == t.instrument_id()
                && match (w.limit, w.order.side()) {
                    (Some(l), OrderSide::Buy) => px <= l,
                    (Some(l), OrderSide::Sell) => px >= l,
                    _ => false,
                };
            if through && left > QTY_EPS {
                let limit = w.limit.unwrap_or(px);
                let (filled, removed) = self.execute(i, left, limit, ts, true)?;
                left -= filled;
                if removed {
                    continue;
                }
            }
            i += 1;
        }
        let mut i = 0;
        while i < self.working.len() {
            let w = &self.working[i];
            let hit = match (w.armed, w.order.side()) {
                (Some(trig), OrderSide::Buy) => px >= trig,
                (Some(trig), OrderSide::Sell) => px <= trig,
                _ => false,
            } && w.order.instrument_id() == t.instrument_id();
            if hit {
                self.working[i].armed = None;
                if self.aggress(i, ts, true)? {
                    continue;
                }
            }
            i += 1;
        }
        Ok(())
    }

    /// Closes the session at `ts`: every working Day order expires, stamped `ts`.
    pub fn on_session_close(&mut self, ts: UnixNanos) -> Result<()> {
        let ts = ts.as_u64();
        self.now = self.now.max(ts);
        let mut i = 0;
        while i < self.working.len() {
            if self.working[i].order.time_in_force() == TimeInForce::Day {
                self.expire(i, ts);
            } else {
                i += 1;
            }
        }
        Ok(())
    }

    /// Moves event time to `ts` with no market event, expiring Day orders whose close has
    /// passed (each stamped at its close instant).
    pub fn advance_to(&mut self, ts: UnixNanos) -> Result<()> {
        self.advance(ts.as_u64())
    }

    /// Moves event time to `ts`, expiring Day orders whose close has passed.
    fn advance(&mut self, ts: u64) -> Result<()> {
        let mut i = 0;
        while i < self.working.len() {
            match self.working[i].expire_at {
                Some(at) if at <= ts => {
                    self.expire(i, at);
                }
                _ => i += 1,
            }
        }
        self.now = self.now.max(ts);
        Ok(())
    }

    fn expire(&mut self, idx: usize, ts: u64) {
        let w = self.working.remove(idx);
        self.events.push(ExecutionEvent::Expired {
            order_id: w.order.order_id().clone(),
            instrument_id: w.order.instrument_id().clone(),
            side: w.order.side(),
            quantity: w.remaining,
            venue_order_id: None,
            ts: UnixNanos::from_u64(ts),
        });
    }

    fn close_after(&self, now: u64) -> u64 {
        let candidate = now / NS_PER_DAY * NS_PER_DAY + self.close_ns;
        if candidate < now {
            candidate + NS_PER_DAY
        } else {
            candidate
        }
    }

    /// Matches working order `idx` against the touch. `first` marks the order's first look at
    /// the book (submit or trigger), where IOC / FOK are decided. Returns whether the order
    /// left the working set.
    fn aggress(&mut self, idx: usize, ts: u64, first: bool) -> Result<bool> {
        let w = &self.working[idx];
        let (side, tif, remaining) = (w.order.side(), w.order.time_in_force(), w.remaining);
        let instrument = w.order.instrument_id().clone();
        let touch = self
            .book(&instrument)
            .and_then(|b| b.touch(side))
            .filter(|(px, _)| match (w.limit, side) {
                (None, _) => true,
                (Some(l), OrderSide::Buy) => *px <= l,
                (Some(l), _) => *px >= l,
            });
        if first
            && tif == TimeInForce::Fok
            && touch.map_or(true, |(_, sz)| sz + QTY_EPS < remaining)
        {
            let w = self.working.remove(idx);
            self.push_rejection(&w.order, w.remaining, "fok_unfillable", ts);
            return Ok(true);
        }
        if let Some((px, sz)) = touch {
            let (filled, removed) = self.execute(idx, sz.min(remaining), px, ts, false)?;
            if let Some((_, book)) = self.books.iter_mut().find(|(i, _)| *i == instrument) {
                book.consume(side, filled);
            }
            if removed {
                return Ok(true);
            }
        }
        if first && tif == TimeInForce::Ioc {
            let w = self.working.remove(idx);
            self.events.push(ExecutionEvent::Cancelled {
                order_id: w.order.order_id().clone(),
                instrument_id: w.order.instrument_id().clone(),
                side: w.order.side(),
                quantity: w.remaining,
                venue_order_id: None,
                ts: UnixNanos::from_u64(ts),
            });
            return Ok(true);
        }
        Ok(false)
    }

    /// Fills up to `avail` of order `idx` at `base` (a touch price, or the limit when
    /// `passive`), applying the long-only cap, the funding cut, slippage (aggressive fills,
    /// clamped to the limit), costs and latency. Returns `(filled quantity, left the book)`.
    fn execute(
        &mut self,
        idx: usize,
        avail: f64,
        base: f64,
        ts: u64,
        passive: bool,
    ) -> Result<(f64, bool)> {
        let order = self.working[idx].order.clone();
        let limit = self.working[idx].limit;
        let side = order.side();
        let mut qty = avail.min(self.working[idx].remaining);
        let mut cut: Option<&str> = None;
        if self.long_only && side == OrderSide::Sell {
            let capped = 0.0_f64.max(qty.min(self.position(order.instrument_id())));
            if capped < qty {
                cut = Some("no_position");
                qty = capped;
            }
        }
        let mut px = if passive {
            base
        } else {
            self.fill_model.price(side, base, &mut self.rng)
        };
        if let Some(l) = limit {
            px = if side == OrderSide::Buy {
                px.min(l)
            } else {
                px.max(l)
            };
        }
        if side == OrderSide::Buy && qty > 0.0 && self.buy_cost(qty, px)? > self.cash {
            let lot = self.lot_size(order.instrument_id());
            qty = self.affordable(qty, px, self.cash, lot)?;
            cut = Some("insufficient_funds");
        }
        if qty > 0.0 {
            let notional = self.notional(qty, px)?;
            let cost = self.costs(side, qty, px)?;
            let delta = if side == OrderSide::Buy {
                -notional
                    .checked_add(cost)
                    .ok_or_else(|| err("buy cost overflow".into()))?
            } else {
                notional
                    .checked_sub(cost)
                    .ok_or_else(|| err("proceeds overflow".into()))?
            };
            let cash = self
                .cash
                .checked_add(delta)
                .ok_or_else(|| err("cash overflow".into()))?;
            let w = &mut self.working[idx];
            w.remaining -= qty;
            w.filled += qty;
            let (cum, complete) = (w.filled, w.remaining <= QTY_EPS);
            self.cash = cash;
            self.book_fill(&order, px, qty, notional, cost, cum, complete, ts)?;
        }
        let remaining = self.working[idx].remaining;
        if let Some(reason) = cut {
            self.working.remove(idx);
            self.push_rejection(&order, remaining, reason, ts);
            return Ok((qty, true));
        }
        if remaining <= QTY_EPS {
            self.working.remove(idx);
            return Ok((qty, true));
        }
        Ok((qty, false))
    }

    #[allow(clippy::too_many_arguments)]
    fn book_fill(
        &mut self,
        order: &Order,
        price: f64,
        qty: f64,
        notional: i64,
        cost: i64,
        cum_qty: f64,
        complete: bool,
        ts: u64,
    ) -> Result<()> {
        let signed = if order.side() == OrderSide::Buy {
            qty
        } else {
            -qty
        };
        let held = self.position(order.instrument_id()) + signed;
        self.positions.retain(|(i, _)| i != order.instrument_id());
        if held.abs() > QTY_EPS {
            self.positions.push((order.instrument_id().clone(), held));
        }
        self.mis.apply(order.instrument_id(), order.product(), held);
        self.fees = self
            .fees
            .checked_add(cost)
            .ok_or_else(|| err("fees overflow".into()))?;
        self.traded_notional = self
            .traded_notional
            .checked_add(notional)
            .ok_or_else(|| err("traded notional overflow".into()))?;
        let delay_ns = self
            .latency_model
            .total_delay_ms(&mut self.rng)
            .saturating_mul(1_000_000);
        let at = UnixNanos::from_u64(ts.saturating_add(delay_ns));
        let trade = Trade::new(
            OrderId::new(order.order_id().as_str()),
            order.instrument_id().clone(),
            order.side(),
            qty,
            price,
            self.currency,
            at,
            at,
        )
        .with_costs(Money::new(cost, self.currency));
        self.events.push(ExecutionEvent::Fill {
            trade,
            cum_qty,
            complete,
            venue_order_id: None,
        });
        Ok(())
    }

    fn notional(&self, qty: f64, price: f64) -> Result<i64> {
        Money::mul_qty(qty, price, self.currency)
            .map(|m| m.minor())
            .map_err(|e| err(format!("notional: {e}")))
    }

    fn costs(&self, side: OrderSide, qty: f64, price: f64) -> Result<i64> {
        let cost = (self.cost_fn)(side, qty, price)?;
        if cost.minor() == 0 {
            return Ok(0);
        }
        if cost.currency() != self.currency {
            return Err(err(format!(
                "fill cost currency {:?} differs from the cash currency {:?}",
                cost.currency(),
                self.currency
            )));
        }
        if cost.minor() < 0 {
            return Err(err(format!(
                "fill cost must not be negative, got {}",
                cost.minor()
            )));
        }
        Ok(cost.minor())
    }

    fn buy_cost(&self, qty: f64, px: f64) -> Result<i64> {
        self.notional(qty, px)?
            .checked_add(self.costs(OrderSide::Buy, qty, px)?)
            .ok_or_else(|| err("buy cost overflow".into()))
    }

    fn lot_size(&self, instrument: &InstrumentId) -> f64 {
        self.lot_sizes
            .iter()
            .find(|(i, _)| i == instrument)
            .map_or(1.0, |(_, l)| *l)
    }

    /// Largest whole number of lots (<= `want`) whose notional and cost fit `available`.
    fn affordable(&self, want: f64, px: f64, available: i64, lot: f64) -> Result<f64> {
        if px <= 0.0 || available <= 0 {
            return Ok(0.0);
        }
        let avail_major = Money::new(available, self.currency).to_major_f64();
        let hi = (want / lot + QTY_EPS)
            .floor()
            .min((avail_major / px / lot + QTY_EPS).floor());
        let (mut lo, mut hi) = (0_i64, hi as i64);
        while lo < hi {
            let mid = (lo + hi + 1) / 2;
            if self.buy_cost(mid as f64 * lot, px)? <= available {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        Ok(lo as f64 * lot)
    }

    fn push_rejection(&mut self, order: &Order, qty: f64, reason: &str, ts: impl Into<u64>) {
        self.events.push(ExecutionEvent::Rejected {
            order_id: order.order_id().clone(),
            instrument_id: order.instrument_id().clone(),
            side: order.side(),
            quantity: qty,
            reason: reason.to_string(),
            venue_order_id: None,
            ts: UnixNanos::from_u64(ts.into()),
        });
    }
}

impl ExecutionEngine for L2Sim {
    fn submit(&mut self, order: Order) -> Result<()> {
        let id = order.order_id().as_str();
        if order.side() == OrderSide::NoOrderSide {
            return Err(err(format!(
                "order {id} has no side: it must be buy or sell"
            )));
        }
        if self
            .working
            .iter()
            .any(|w| w.order.order_id().as_str() == id)
        {
            return Err(err(format!("order id {id} is already working")));
        }
        let ts = order.ts_event().as_u64().max(self.now);
        self.advance(ts)?;
        let qty = order.quantity();
        let (limit, armed) = match order.order_type() {
            OrderType::Market => (None, None),
            OrderType::Limit => (order.price(), None),
            OrderType::StopMarket => (None, order.trigger_price()),
            OrderType::StopLimit => (order.price(), order.trigger_price()),
            _ => {
                self.push_rejection(&order, qty, "unsupported_order_type", ts);
                return Ok(());
            }
        };
        let usable = |p: Option<f64>| p.is_some_and(|p| p.is_finite() && p > 0.0);
        let needs_limit = matches!(order.order_type(), OrderType::Limit | OrderType::StopLimit);
        let needs_stop = matches!(
            order.order_type(),
            OrderType::StopMarket | OrderType::StopLimit
        );
        if !(qty.is_finite() && qty > 0.0)
            || (needs_limit && !usable(limit))
            || (needs_stop && !usable(armed))
        {
            self.push_rejection(&order, qty, "invalid_order", ts);
            return Ok(());
        }
        let expire_at = (order.time_in_force() == TimeInForce::Day).then(|| self.close_after(ts));
        self.working.push(Working {
            order,
            remaining: qty,
            filled: 0.0,
            limit,
            armed,
            expire_at,
        });
        if armed.is_none() {
            let idx = self.working.len() - 1;
            self.aggress(idx, ts, true)?;
        }
        Ok(())
    }

    fn cancel(&mut self, order_id: &str, now: UnixNanos) -> Result<()> {
        if let Some(i) = self
            .working
            .iter()
            .position(|w| w.order.order_id().as_str() == order_id)
        {
            let w = self.working.remove(i);
            // ADR 0019 decision 6, L2 v1: both stamped `now`.
            self.events.push(ExecutionEvent::CancelRequested {
                order_id: w.order.order_id().clone(),
                ts: now,
            });
            self.events.push(ExecutionEvent::Cancelled {
                order_id: w.order.order_id().clone(),
                instrument_id: w.order.instrument_id().clone(),
                side: w.order.side(),
                quantity: w.remaining,
                venue_order_id: None,
                ts: now,
            });
        }
        Ok(())
    }

    fn drain_events(&mut self) -> Result<Vec<ExecutionEvent>> {
        Ok(std::mem::take(&mut self.events))
    }

    fn native_events(&self) -> bool {
        true
    }

    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        let events = std::mem::take(&mut self.events);
        self.legacy.absorb(events);
        Ok(self.legacy.take_fills())
    }

    fn drain_rejections(&mut self) -> Result<Vec<OrderRejection>> {
        let events = std::mem::take(&mut self.events);
        self.legacy.absorb(events);
        Ok(self.legacy.take_rejections())
    }
}

impl Handler for L2Sim {
    fn on_event(&mut self, event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        match event {
            Event::Quote(q) => self.on_quote(q)?,
            Event::Trade(t) => self.on_trade(t)?,
            _ => {}
        }
        Ok(EngineOutput::None)
    }
}
