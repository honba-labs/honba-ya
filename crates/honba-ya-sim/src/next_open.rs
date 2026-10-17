//! Deterministic multi-instrument simulator that fills market orders at the next bar open.
//!
//! Rust port of `honba.backtest.simulated.NextOpenExecution` (ADR 0016); the Python class is the
//! reference and `schema/conformance/next_open_sim.json` pins the two together. Implemented
//! (chunks 1 and 2): market orders, fill at the open of the instrument's first bar in a later
//! session, cancel, the `unsupported_order_type`, `no_position` and `insufficient_funds`
//! rejections, integer-minor-unit cash, long-only sell caps, lot-sized and cost-aware funding
//! cuts, the settlement cycle (receivables, available cash, buys waiting for pending proceeds),
//! the injected fill-cost hook ([`FillCostFn`]) and the session-open event path
//! ([`NextOpenSim::on_session_open`]).

use honba_engine::{
    AlgoError, EngineOutput, ExecutionEngine, Handler, LegacyDrains, OrderRejection, Result,
};
use honba_entities::{Currency, ExecutionEvent, Money, Trade};
use honba_market::costs::{CostSchedule, MarketSegment};
use honba_market::settlement::MarginModel;
use honba_messages::{Bar, Event, InstrumentId, Order, OrderId, OrderSide, OrderType, UnixNanos};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::Arc;

use crate::cost::ScheduleFillCost;
use crate::fill_model::{FillModel, NoSlippage, SlippageModel};
use crate::latency::{LatencyModel, NoLatency};
use crate::margin::{square_off_order, MisPositions};

/// Quantities closer than this are equal (float residue from fractional fills).
const QTY_EPS: f64 = 1e-9;

/// `(side, quantity, price) -> cost` of one fill, in the simulator's currency and never
/// negative (a negative value is an error that leaves the order working). Injected so a caller
/// can price fills (see [`NextOpenSim::with_schedule`] and [`ScheduleFillCost`]) without the simulator
/// hard-coding a market; the Python counterpart is `FillCostFn`.
pub type FillCostFn = Box<dyn Fn(OrderSide, f64, f64) -> Result<Money> + Send>;

struct Working {
    order: Order,
    /// Index of the session the order was submitted in (-1 before the first).
    session: i64,
    /// First session the order was eligible and considered for funding.
    first_try: Option<i64>,
}

struct Open {
    instrument: InstrumentId,
    price: f64,
    ts: UnixNanos,
}

/// An [`ExecutionEngine`] filling market orders at the next session's open.
///
/// A session is one point on the driving clock with an increasing key (the bar's `ts_event`).
/// An order submitted in session *k* fills at the open of its instrument's first usable bar in a
/// later session and waits while the instrument does not print. Within one opening, sells fill
/// before buys, each side in submission order. All cash is integer minor units (ADR 0011).
///
/// Events come out of one ordered queue ([`ExecutionEngine::drain_events`]): a fill that
/// leaves a remainder is followed by the remainder's `Rejected`. It never acknowledges (L1).
pub struct NextOpenSim {
    cash: i64,
    currency: Currency,
    fees: i64,
    traded_notional: i64,
    settlement_days: i64,
    /// `(session the proceeds become available, amount in minor units)`.
    receivables: Vec<(i64, i64)>,
    cost_fn: FillCostFn,
    fill_model: Arc<dyn FillModel>,
    latency_model: Arc<dyn LatencyModel>,
    rng: StdRng,
    long_only: bool,
    lot_sizes: Vec<(InstrumentId, f64)>,
    positions: Vec<(InstrumentId, f64)>,
    /// Instruments held under an intraday (MIS) product, squared off at the session close.
    mis: MisPositions,
    /// The active market's margin model, for [`Self::margin_query`].
    margin: Option<Arc<dyn MarginModel>>,
    /// Flatten held intraday positions at [`Self::on_session_close`] (default true).
    auto_square_off_mis: bool,
    session: i64,
    session_ts: Option<u64>,
    /// The current session came from a session-open event (see [`Self::on_session_open`]).
    from_open: bool,
    opened: Vec<InstrumentId>,
    working: Vec<Working>,
    events: Vec<ExecutionEvent>,
    legacy: LegacyDrains,
}

fn err(msg: String) -> AlgoError {
    AlgoError::Component(msg)
}

fn usable_open(price: f64) -> bool {
    price.is_finite() && price > 0.0
}

impl NextOpenSim {
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
            settlement_days: 0,
            receivables: Vec::new(),
            cost_fn: Box::new(|_, _, _| Ok(Money::new(0, Currency::Inr))),
            fill_model: Arc::new(NoSlippage),
            latency_model: Arc::new(NoLatency),
            // Seed 0, not entropy: an unseeded simulator is still reproducible.
            rng: StdRng::seed_from_u64(0),
            long_only: true,
            lot_sizes: Vec::new(),
            positions: Vec::new(),
            mis: MisPositions::new(),
            margin: None,
            auto_square_off_mis: true,
            session: -1,
            session_ts: None,
            from_open: false,
            opened: Vec::new(),
            working: Vec::new(),
            events: Vec::new(),
            legacy: LegacyDrains::new(),
        })
    }

    /// Sets the fill cost function (default: no costs). Funding cuts include its cost, so it
    /// should be non-decreasing in quantity.
    #[must_use]
    pub fn with_costs(mut self, cost_fn: FillCostFn) -> Self {
        self.cost_fn = cost_fn;
        self
    }

    /// Charges each fill from a market [`CostSchedule`], itemized per named charge.
    ///
    /// The schedule is evaluated on `abs(quantity * price)` for `side`, and every charge is
    /// rounded to minor units once before the legs are summed ([`ScheduleFillCost`]); funding
    /// cuts therefore include the cost. Wires into the same hook as [`Self::with_costs`].
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

    /// Installs the slippage model applied to every fill (default [`NoSlippage`]).
    ///
    /// A stochastic model consumes the simulator's seeded stream; pair it with
    /// [`Self::with_seed`] or use [`Self::with_slippage`] so a run repeats.
    #[must_use]
    pub fn with_fill_model(mut self, model: Arc<dyn FillModel>) -> Self {
        self.fill_model = model;
        self
    }

    /// Installs the latency model that delays each fill's timestamp (default
    /// [`NoLatency`]).
    #[must_use]
    pub fn with_latency_model(mut self, model: Arc<dyn LatencyModel>) -> Self {
        self.latency_model = model;
        self
    }

    /// Seeds the simulator's random stream. Two runs with the same seed, orders
    /// and bars book byte-identical fills.
    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.rng = StdRng::seed_from_u64(seed);
        self
    }

    /// Installs a [`SlippageModel`], seeding the simulator from its seed when it
    /// has one.
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

    /// The installed margin model, if any.
    pub fn margin_model(&self) -> Option<Arc<dyn MarginModel>> {
        self.margin.clone()
    }

    /// A [`crate::MarginQuery`] over the installed margin model, if any.
    pub fn margin_query(&self) -> Option<crate::margin::MarginQuery<'_>> {
        self.margin
            .as_ref()
            .map(|m| crate::margin::MarginQuery::new(m.as_ref()))
    }

    /// Sets whether a held intraday (MIS) position is flattened at the session close
    /// (default true). With it off, a held MIS position at [`Self::on_session_close`] is an
    /// error instead.
    #[must_use]
    pub fn with_auto_square_off_mis(mut self, on: bool) -> Self {
        self.auto_square_off_mis = on;
        self
    }

    /// The instruments currently held under an intraday (MIS) product.
    pub fn mis_positions(&self) -> &[InstrumentId] {
        self.mis.intraday()
    }

    /// Sets the settlement cycle: sale proceeds become available that many sessions after the
    /// sale (default 0). Errors when negative.
    pub fn with_settlement_days(mut self, days: i64) -> Result<Self> {
        self.set_settlement_days(days)?;
        Ok(self)
    }

    /// Changes the settlement cycle. Errors (state unchanged) when `days` is negative or the
    /// first session has already opened.
    pub fn set_settlement_days(&mut self, days: i64) -> Result<()> {
        if days < 0 {
            return Err(err("settlement_days must be >= 0".into()));
        }
        if self.session_ts.is_some() {
            return Err(err(
                "settlement_days can only change before the first session".into(),
            ));
        }
        self.settlement_days = days;
        Ok(())
    }

    /// The settlement cycle in sessions.
    pub fn settlement_days(&self) -> i64 {
        self.settlement_days
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

    /// Sale proceeds booked but not yet available.
    pub fn unsettled(&self) -> Money {
        Money::new(self.unsettled_minor(), self.currency)
    }

    /// Cash that may fund a buy now: booked cash less unsettled sale proceeds.
    pub fn available_cash(&self) -> Money {
        Money::new(self.cash - self.unsettled_minor(), self.currency)
    }

    /// Pending sale proceeds as `(session index they become available, amount)`.
    pub fn receivables(&self) -> Vec<(i64, Money)> {
        self.receivables
            .iter()
            .map(|&(due, a)| (due, Money::new(a, self.currency)))
            .collect()
    }

    fn unsettled_minor(&self) -> i64 {
        self.receivables
            .iter()
            .filter(|(due, _)| *due > self.session)
            .map(|(_, a)| a)
            .sum()
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

    /// The key of the current session (`None` before the first one opens).
    pub fn session_ts(&self) -> Option<u64> {
        self.session_ts
    }

    /// Seeds or overwrites the net signed position in `instrument` (a starting holding; the
    /// Python `positions[iid] = qty`). A value within the quantity epsilon of zero removes it;
    /// a non-finite value is an error with the state unchanged. An existing entry keeps its
    /// place in [`Self::positions`].
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

    /// Every non-flat position, in the order first opened.
    pub fn positions(&self) -> Vec<(InstrumentId, f64)> {
        self.positions.clone()
    }

    /// Ids of orders not yet filled, rejected or cancelled, in submission order.
    pub fn working_orders(&self) -> Vec<String> {
        self.working
            .iter()
            .map(|w| w.order.order_id().as_str().to_string())
            .collect()
    }

    /// Starts session `ts` and fills eligible orders at the opens of `bars`.
    ///
    /// Errors if `ts` does not follow the current session; state is unchanged then.
    pub fn open_session(&mut self, ts: UnixNanos, bars: &[Bar]) -> Result<()> {
        let ts = ts.as_u64();
        if let Some(cur) = self.session_ts {
            if ts <= cur {
                return Err(err(format!("session {ts} does not follow session {cur}")));
            }
        }
        self.session += 1;
        self.session_ts = Some(ts);
        self.opened.clear();
        let session = self.session;
        self.receivables.retain(|(due, _)| *due > session);
        self.fill_at(bars)
    }

    /// The session-open event (Python `SessionOpen`): opens session `ts` like
    /// [`Self::open_session`], then switches to the lenient bar rules for good.
    ///
    /// The key `ts` need not be a bar timestamp, so afterwards a bar never opens or orders a
    /// session: it only opens an instrument the event left out, when it carries the session's
    /// `ts`; every other bar is ignored. When opening fails (bad key, or an error while filling)
    /// the mode does not change.
    pub fn on_session_open(&mut self, ts: UnixNanos, bars: &[Bar]) -> Result<()> {
        self.open_session(ts, bars)?;
        self.from_open = true;
        Ok(())
    }

    /// Closes the current session at `ts`, flattening held intraday (MIS) positions at the
    /// close of their bar in `bars`.
    ///
    /// With [`Self::with_auto_square_off_mis`] on (the default) every instrument marked
    /// intraday is closed with the opposite [`square_off_order`] at its bar's close, so the run
    /// ends flat: the fill and the cash it moves surface through the usual drains and the
    /// equity/PnL reflects the square-off. With it off, a held MIS position is an error instead
    /// (state unchanged). A CNC (delivery) position is never squared off.
    ///
    /// `ts` must not precede the current session, and each held intraday instrument needs a bar
    /// in `bars` to price the close. When no market calendar is wired, use
    /// [`crate::margin::default_session_close_ts`] (23:59:00 UTC) for `ts`.
    pub fn on_session_close(&mut self, ts: UnixNanos, bars: &[Bar]) -> Result<()> {
        let ts_u = ts.as_u64();
        if let Some(cur) = self.session_ts {
            if ts_u < cur {
                return Err(err(format!("session close {ts_u} is before session {cur}")));
            }
        }
        let mut closes: Vec<(InstrumentId, f64)> = Vec::new();
        for b in bars {
            let id = b.bar_type().instrument_id();
            if usable_open(b.close()) && !closes.iter().any(|(i, _)| i == id) {
                closes.push((id.clone(), b.close()));
            }
        }
        let held: Vec<InstrumentId> = self.mis.intraday().to_vec();
        if !held.is_empty() && !self.auto_square_off_mis {
            return Err(err(format!(
                "{} intraday position(s) held at session close {ts_u} and auto square-off is off",
                held.len()
            )));
        }
        for (seq, instrument) in held.iter().enumerate() {
            let position = self.position(instrument);
            if position.abs() <= QTY_EPS {
                self.mis.clear(instrument);
                continue;
            }
            let Some(price) = closes
                .iter()
                .find(|(i, _)| i == instrument)
                .map(|(_, p)| *p)
            else {
                return Err(err(format!(
                    "no session-close price for {} to square off its MIS position",
                    instrument.symbol()
                )));
            };
            self.square_off(instrument, position, price, ts_u, seq)?;
        }
        self.session_ts = Some(ts_u);
        if let Some((instrument, qty)) = self.mis.intraday().iter().find_map(|i| {
            let q = self.position(i);
            (q.abs() > QTY_EPS).then(|| (i.clone(), q))
        }) {
            return Err(err(format!(
                "intraday position in {} ({qty}) is not flat after square-off",
                instrument.symbol()
            )));
        }
        Ok(())
    }

    /// Books the session-close fill that flattens `position` in `instrument` at `price`.
    fn square_off(
        &mut self,
        instrument: &InstrumentId,
        position: f64,
        price: f64,
        ts: u64,
        seq: usize,
    ) -> Result<()> {
        let order = square_off_order(
            &format!("__square_off_{seq}"),
            instrument,
            position,
            UnixNanos::from_u64(ts),
        );
        let qty = position.abs();
        let notional = self.notional(qty, price)?;
        let cost = self.costs(order.side(), qty, price)?;
        let delta = if order.side() == OrderSide::Buy {
            -(notional
                .checked_add(cost)
                .ok_or_else(|| err("square-off cost overflow".into()))?)
        } else {
            notional
                .checked_sub(cost)
                .ok_or_else(|| err("square-off proceeds overflow".into()))?
        };
        self.cash = self
            .cash
            .checked_add(delta)
            .ok_or_else(|| err("cash overflow".into()))?;
        let open = Open {
            instrument: instrument.clone(),
            price,
            ts: UnixNanos::from_u64(ts),
        };
        self.book(&order, &open, price, qty, notional, cost, true)
    }

    /// Observes one bar: it opens a session when its ts is later than the current one, opens its
    /// own instrument when it shares the ts, and is an error when earlier or repeated.
    pub fn on_bar(&mut self, bar: &Bar) -> Result<()> {
        let ts = bar.ts_event().as_u64();
        if self.from_open {
            if Some(ts) == self.session_ts && !self.opened.contains(bar.bar_type().instrument_id())
            {
                return self.fill_at(std::slice::from_ref(bar));
            }
            return Ok(());
        }
        match self.session_ts {
            Some(cur) if ts < cur => Err(err(format!(
                "non-monotonic bar: ts {ts} is before session {cur}"
            ))),
            Some(cur) if ts == cur => {
                if self.opened.contains(bar.bar_type().instrument_id()) {
                    return Err(err(format!(
                        "duplicate bar for {} at ts {ts} in session {cur}",
                        bar.bar_type().instrument_id().symbol()
                    )));
                }
                self.fill_at(std::slice::from_ref(bar))
            }
            _ => self.open_session(bar.ts_event(), std::slice::from_ref(bar)),
        }
    }

    fn now(&self) -> UnixNanos {
        UnixNanos::from_u64(self.session_ts.unwrap_or(0))
    }

    fn fill_at(&mut self, bars: &[Bar]) -> Result<()> {
        let mut opens: Vec<Open> = Vec::new();
        for b in bars {
            let id = b.bar_type().instrument_id();
            if !self.opened.contains(id)
                && usable_open(b.open())
                && !opens.iter().any(|o| &o.instrument == id)
            {
                opens.push(Open {
                    instrument: id.clone(),
                    price: b.open(),
                    ts: b.ts_event(),
                });
            }
        }
        self.opened
            .extend(opens.iter().map(|o| o.instrument.clone()));
        let eligible: Vec<(String, usize)> = self
            .working
            .iter()
            .filter(|w| w.session < self.session)
            .filter_map(|w| {
                opens
                    .iter()
                    .position(|o| &o.instrument == w.order.instrument_id())
                    .map(|i| (w.order.order_id().as_str().to_string(), i))
            })
            .collect();
        for side in [OrderSide::Sell, OrderSide::Buy] {
            for (id, i) in &eligible {
                let Some(idx) = self
                    .working
                    .iter()
                    .position(|w| w.order.order_id().as_str() == id)
                else {
                    continue;
                };
                if self.working[idx].order.side() != side {
                    continue;
                }
                if side == OrderSide::Sell {
                    self.fill_sell(idx, &opens[*i])?;
                } else {
                    self.fill_buy(idx, &opens[*i])?;
                }
            }
        }
        Ok(())
    }

    fn notional(&self, qty: f64, price: f64) -> Result<i64> {
        Money::mul_qty(qty, price, self.currency)
            .map(|m| m.minor())
            .map_err(|e| err(format!("notional: {e}")))
    }

    /// Cost of one fill in minor units; errors when negative or in another currency (a zero
    /// cost is currency-neutral).
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

    /// Notional plus cost of buying `qty` at `px`.
    fn buy_cost(&self, qty: f64, px: f64) -> Result<i64> {
        self.notional(qty, px)?
            .checked_add(self.costs(OrderSide::Buy, qty, px)?)
            .ok_or_else(|| err("buy cost overflow".into()))
    }

    fn fill_sell(&mut self, idx: usize, open: &Open) -> Result<()> {
        let order = self.working[idx].order.clone();
        let mut want = order.quantity();
        let mut qty = want;
        if self.long_only {
            let held = self.position(order.instrument_id());
            qty = 0.0_f64.max(want.min(held));
            if 0.0 < qty && qty < want && want - qty <= QTY_EPS {
                want = qty; // a hair over the position is float residue: sell it all
            }
        }
        // One slippage draw per order: the same price funds and books the fill.
        let px = self
            .fill_model
            .price(order.side(), open.price, &mut self.rng);
        let (notional, cost) = if qty > 0.0 {
            // before dequeuing: a failure leaves the order working
            (
                self.notional(qty, px)?,
                self.costs(OrderSide::Sell, qty, px)?,
            )
        } else {
            (0, 0)
        };
        let proceeds = notional
            .checked_sub(cost)
            .ok_or_else(|| err("proceeds overflow".into()))?;
        let cash = self
            .cash
            .checked_add(proceeds)
            .ok_or_else(|| err("cash overflow".into()))?;
        self.working.remove(idx);
        // The fill, then the remainder's rejection: one queue, in that order.
        let booked = if qty > 0.0 {
            self.cash = cash;
            self.receivables
                .push((self.session + self.settlement_days, proceeds));
            self.book(&order, open, px, qty, notional, cost, qty >= want)
        } else {
            Ok(())
        };
        if qty < want {
            self.reject(&order, want - qty, "no_position");
        }
        booked
    }

    fn fill_buy(&mut self, idx: usize, open: &Open) -> Result<()> {
        let session = self.session;
        let first_try = *self.working[idx].first_try.get_or_insert(session);
        let order = self.working[idx].order.clone();
        let want = order.quantity();
        // One slippage draw per order: the same price funds and books the fill.
        let px = self
            .fill_model
            .price(order.side(), open.price, &mut self.rng);
        let available = self.cash - self.unsettled_minor();
        let qty = if self.buy_cost(want, px)? <= available {
            want
        } else if self.unsettled_minor() > 0 && session - first_try < self.settlement_days {
            return Ok(()); // wait for pending sale proceeds to settle
        } else {
            self.affordable(want, px, available, self.lot_size(order.instrument_id()))?
        };
        let (notional, cost) = if qty > 0.0 {
            // before dequeuing: a failure leaves the order working
            (
                self.notional(qty, px)?,
                self.costs(OrderSide::Buy, qty, px)?,
            )
        } else {
            (0, 0)
        };
        let outlay = notional
            .checked_add(cost)
            .and_then(|o| self.cash.checked_sub(o))
            .ok_or_else(|| err("cash overflow".into()))?;
        self.working.remove(idx);
        // The fill, then the remainder's rejection: one queue, in that order.
        let booked = if qty > 0.0 {
            self.cash = outlay;
            self.book(&order, open, px, qty, notional, cost, qty >= want)
        } else {
            Ok(())
        };
        if qty < want {
            self.reject(&order, want - qty, "insufficient_funds");
        }
        booked
    }

    fn lot_size(&self, instrument: &InstrumentId) -> f64 {
        self.lot_sizes
            .iter()
            .find(|(i, _)| i == instrument)
            .map_or(1.0, |(_, l)| *l)
    }

    /// Largest whole number of lots (<= `want`) whose notional fits the cash, by bisection.
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

    #[allow(clippy::too_many_arguments)]
    fn book(
        &mut self,
        order: &Order,
        open: &Open,
        price: f64,
        qty: f64,
        notional: i64,
        cost: i64,
        complete: bool,
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
        let ts = UnixNanos::from_u64(open.ts.as_u64().saturating_add(delay_ns));
        let trade = Trade::new(
            OrderId::new(order.order_id().as_str()),
            order.instrument_id().clone(),
            order.side(),
            qty,
            price,
            self.currency,
            ts,
            ts,
        )
        .with_costs(Money::new(cost, self.currency));
        // One fill per order: its cumulative quantity is its own.
        self.events.push(ExecutionEvent::Fill {
            trade,
            cum_qty: qty,
            complete,
            venue_order_id: None,
        });
        Ok(())
    }

    fn reject(&mut self, order: &Order, qty: f64, reason: &str) {
        let ts = self.now();
        self.push_rejection(order, qty, reason, ts);
    }

    fn push_rejection(&mut self, order: &Order, qty: f64, reason: &str, ts: UnixNanos) {
        self.events.push(ExecutionEvent::Rejected {
            order_id: order.order_id().clone(),
            instrument_id: order.instrument_id().clone(),
            side: order.side(),
            quantity: qty,
            reason: reason.to_string(),
            venue_order_id: None,
            ts,
        });
    }
}

impl ExecutionEngine for NextOpenSim {
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
        if order.order_type() != OrderType::Market {
            let (qty, ts) = (order.quantity(), order.ts_event());
            self.push_rejection(&order, qty, "unsupported_order_type", ts);
            return Ok(());
        }
        self.working.push(Working {
            order,
            session: self.session,
            first_try: None,
        });
        Ok(())
    }

    fn cancel(&mut self, order_id: &str, now: UnixNanos) -> Result<()> {
        if let Some(i) = self
            .working
            .iter()
            .position(|w| w.order.order_id().as_str() == order_id)
        {
            let w = self.working.remove(i);
            self.events.push(ExecutionEvent::Cancelled {
                order_id: w.order.order_id().clone(),
                instrument_id: w.order.instrument_id().clone(),
                side: w.order.side(),
                quantity: w.order.quantity(),
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

impl Handler for NextOpenSim {
    fn on_event(&mut self, event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        if let Event::Bar(b) = event {
            self.on_bar(b)?;
        }
        Ok(EngineOutput::None)
    }
}
