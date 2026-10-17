//! Execution engine that fills at the most recent bar's close.

use std::sync::{Arc, Mutex};

use honba_engine::{AlgoError, ExecutionEngine, Handler, LegacyDrains, OrderRejection, Result};
use honba_entities::{Currency, ExecutionEvent, Money, Trade};
use honba_market::costs::{CostSchedule, MarketSegment};
use honba_market::settlement::MarginModel;
use honba_messages::{
    Event, InstrumentId, Order, OrderId, OrderSide, OrderType, Product, UnixNanos,
};
use rand::rngs::StdRng;
use rand::SeedableRng;

use crate::cost::ScheduleFillCost;
use crate::fill_model::{FillModel, NoSlippage, SlippageModel};
use crate::latency::{LatencyModel, NoLatency};
use crate::margin::{MisPositions, QTY_EPS};

/// Largest accepted flat cost per fill.
pub const MAX_FLAT_COST: f64 = 1e9;

/// Largest accepted proportional cost, in basis points of the fill notional
/// (10 000 bps = 100%).
pub const MAX_COST_BPS: f64 = 10_000.0;

/// Why a [`FillCosts`] could not be built.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum FillCostsError {
    /// The flat cost is not a finite number in `[0, MAX_FLAT_COST]`.
    #[error("flat fill cost must be finite and within 0..={MAX_FLAT_COST}, got {0}")]
    InvalidFlat(f64),
    /// The proportional cost is not a finite number in `[0, MAX_COST_BPS]`.
    #[error("fill cost bps must be finite and within 0..={MAX_COST_BPS}, got {0}")]
    InvalidBps(f64),
}

/// Per-fill transaction costs charged by [`BarFillEngine`] (ADR 008).
///
/// The cost of a fill is the flat component plus the proportional component of
/// the notional, each **rounded to minor units before summation** (ADR 0011):
/// costs are computed per leg and each leg rounds once, so the reported total
/// is one a broker's ledger can reproduce. The cost is never negative and not
/// signed by side: `Trade::costs` carries it, a buy debits `quantity * price +
/// costs` and a sell credits `quantity * price - costs`. The default is no
/// costs.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct FillCosts {
    flat: f64,
    bps: f64,
}

impl FillCosts {
    /// Validates and builds the costs: `flat` in `[0, MAX_FLAT_COST]` and
    /// `bps` in `[0, MAX_COST_BPS]`, both finite.
    pub fn new(flat: f64, bps: f64) -> std::result::Result<Self, FillCostsError> {
        if !(flat.is_finite() && (0.0..=MAX_FLAT_COST).contains(&flat)) {
            return Err(FillCostsError::InvalidFlat(flat));
        }
        if !(bps.is_finite() && (0.0..=MAX_COST_BPS).contains(&bps)) {
            return Err(FillCostsError::InvalidBps(bps));
        }
        Ok(Self { flat, bps })
    }

    /// The flat cost per fill.
    pub fn flat(&self) -> f64 {
        self.flat
    }

    /// The proportional cost in basis points of the fill notional.
    pub fn bps(&self) -> f64 {
        self.bps
    }

    /// The cost of one fill: each leg rounded to minor units once, then summed.
    ///
    /// A leg that cannot be represented (a non-finite or i64-overflowing
    /// notional) is an error, never a zero cost: a free fill would round in the
    /// reporter's favour (ADR 0011).
    fn of(&self, quantity: f64, price: f64, currency: Currency) -> Result<Money> {
        let leg_error =
            |e: honba_entities::MoneyError| AlgoError::Component(format!("fill cost: {e}"));
        let flat = Money::from_major_f64(self.flat, currency).map_err(leg_error)?;
        let bps = Money::from_major_f64(quantity * price * self.bps / 10_000.0, currency)
            .map_err(leg_error)?;
        (flat + bps).map_err(|e| AlgoError::Component(format!("fill cost: {e}")))
    }
}

/// The per-fill cost model of a [`BarFillEngine`].
///
/// The default is a side-agnostic flat + bps [`FillCosts`]; a market
/// [`CostSchedule`] may be installed instead, which sees the fill's side.
#[derive(Clone)]
enum CostModel {
    /// A side-agnostic flat + bps cost.
    Flat(FillCosts),
    /// A side-aware market cost schedule bound to one segment.
    Schedule {
        schedule: Arc<dyn CostSchedule>,
        segment: MarketSegment,
    },
}

impl CostModel {
    /// The cost of one fill in `currency`.
    fn of(&self, side: OrderSide, quantity: f64, price: f64, currency: Currency) -> Result<Money> {
        match self {
            CostModel::Flat(costs) => costs.of(quantity, price, currency),
            CostModel::Schedule { schedule, segment } => {
                ScheduleFillCost::new(Arc::clone(schedule), segment.clone(), currency)
                    .total(side, quantity, price)
            }
        }
    }
}

#[derive(Clone)]
struct RestingTrailingStop {
    order: Order,
    peak: f64,
    trough: f64,
}

/// Net signed position in `instrument` from a position list.
fn position_of(positions: &[(InstrumentId, f64)], instrument: &InstrumentId) -> f64 {
    positions
        .iter()
        .find(|(i, _)| i == instrument)
        .map_or(0.0, |(_, q)| *q)
}

/// Applies a fill's signed delta to the position list and refreshes the intraday marks.
fn apply_position(
    positions: &mut Vec<(InstrumentId, f64)>,
    mis: &mut MisPositions,
    instrument: &InstrumentId,
    product: Option<Product>,
    signed_delta: f64,
) {
    let held = position_of(positions, instrument) + signed_delta;
    positions.retain(|(i, _)| i != instrument);
    if held.abs() > QTY_EPS {
        positions.push((instrument.clone(), held));
    }
    mis.apply(instrument, product, held);
}

struct Inner {
    last_price: Option<f64>,
    resting_trailing: Vec<RestingTrailingStop>,
    /// Net signed positions, used to hold CNC overnight and square off MIS at the close.
    positions: Vec<(InstrumentId, f64)>,
    /// Instruments held under an intraday (MIS) product.
    mis: MisPositions,
    events: Vec<ExecutionEvent>,
    legacy: LegacyDrains,
    next_ts: u64,
    rng: StdRng,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            last_price: None,
            resting_trailing: Vec::new(),
            positions: Vec::new(),
            mis: MisPositions::new(),
            events: Vec::new(),
            legacy: LegacyDrains::default(),
            next_ts: 0,
            // Seed 0, not entropy: an unseeded engine is still reproducible.
            rng: StdRng::seed_from_u64(0),
        }
    }
}

/// An execution engine that records the most recent bar close (as a
/// [`Handler`]) and fills every order at that price (as an
/// [`ExecutionEngine`]). Each order produces one complete
/// [`ExecutionEvent::Fill`]; it never acknowledges (L1).
///
/// Cheap to clone — clones share the same underlying state. Register one
/// clone with the engine to observe bars, and pass another to the runner to
/// execute orders.
///
/// ```
/// use honba_sim::BarFillEngine;
/// use honba_engine::{ExecutionEngine, Handler};
/// use honba_messages::{
///     Bar, BarAggregation, BarSpecification, BarType, Event, InstrumentId,
///     Order, OrderId, OrderSide, OrderType, PriceType, TimeInForce,
///     UnixNanos, Exchange,
/// };
///
/// let mut exec = BarFillEngine::new();
/// let bt = BarType::new(
///     InstrumentId::new("X", Exchange::new("TEST")),
///     BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
/// );
/// let t = UnixNanos::from_u64(2);
/// let bar = Bar::new(bt, 100.0, 102.0, 99.0, 101.0, 1000.0, t, t);
/// exec.on_event(&Event::Bar(bar), t).unwrap();
///
/// // Last price observed from bar close is 101. Submit and drain.
/// let order = Order::new(
///     OrderId::new("O-1"),
///     InstrumentId::new("X", Exchange::new("TEST")),
///     OrderSide::Buy, OrderType::Market, 5.0, None, TimeInForce::Day,
///     UnixNanos::from_u64(2), UnixNanos::from_u64(2),
/// );
/// exec.submit(order).unwrap();
/// let fills = exec.drain_fills().unwrap();
/// assert_eq!(fills.len(), 1);
/// assert_eq!(fills[0].price(), 101.0);
/// ```
#[derive(Clone)]
pub struct BarFillEngine {
    inner: Arc<Mutex<Inner>>,
    costs: CostModel,
    /// The currency costs settle in. An `Order` carries no currency (it is a
    /// market instruction, not a ledger entry), so the engine declares which
    /// currency its fills cost in — INR for the Indian market by default.
    currency: Currency,
    /// Applies slippage to each fill (default [`NoSlippage`]).
    fill_model: Arc<dyn FillModel>,
    /// Delays each fill's timestamp (default [`NoLatency`]).
    latency_model: Arc<dyn LatencyModel>,
    /// The active market's margin model, for [`Self::margin_query`].
    margin_model: Option<Arc<dyn MarginModel>>,
    /// Flatten held intraday positions at [`Self::on_session_close`] (default true).
    auto_square_off_mis: bool,
}

impl Default for BarFillEngine {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            costs: CostModel::Flat(FillCosts::default()),
            currency: Currency::Inr,
            fill_model: Arc::new(NoSlippage),
            latency_model: Arc::new(NoLatency),
            margin_model: None,
            auto_square_off_mis: true,
        }
    }
}

impl BarFillEngine {
    /// Creates an engine with no observed price and no fill costs.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an engine that charges `costs` on every fill.
    pub fn with_costs(costs: FillCosts) -> Self {
        Self {
            costs: CostModel::Flat(costs),
            ..Self::default()
        }
    }

    /// Creates an engine that charges `schedule` (bound to `segment`) on every fill.
    ///
    /// The schedule sees the fill's side; each named charge is rounded to minor
    /// units once before the legs are summed ([`ScheduleFillCost`]).
    pub fn with_schedule(schedule: Arc<dyn CostSchedule>, segment: MarketSegment) -> Self {
        Self {
            costs: CostModel::Schedule { schedule, segment },
            ..Self::default()
        }
    }

    /// Sets the settlement currency costs are charged in.
    pub fn with_currency(mut self, currency: Currency) -> Self {
        self.currency = currency;
        self
    }

    /// Installs the slippage model applied to every fill (default [`NoSlippage`]).
    ///
    /// A stochastic model consumes the engine's seeded stream; pair it with
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

    /// Seeds the engine's random stream. Two runs with the same seed, orders and
    /// bars book byte-identical fills.
    #[must_use]
    pub fn with_seed(self, seed: u64) -> Self {
        self.inner.lock().unwrap().rng = StdRng::seed_from_u64(seed);
        self
    }

    /// Installs a [`SlippageModel`], seeding the engine from its seed when it
    /// has one.
    #[must_use]
    pub fn with_slippage(self, model: SlippageModel) -> Self {
        let seed = model.seed();
        let engine = self.with_fill_model(Arc::new(model));
        match seed {
            Some(seed) => engine.with_seed(seed),
            None => engine,
        }
    }

    /// Installs the active market's [`MarginModel`], queried through
    /// [`Self::margin_query`].
    #[must_use]
    pub fn with_margin_model(mut self, model: Arc<dyn MarginModel>) -> Self {
        self.margin_model = Some(model);
        self
    }

    /// The installed margin model, if any.
    pub fn margin_model(&self) -> Option<Arc<dyn MarginModel>> {
        self.margin_model.clone()
    }

    /// A [`crate::MarginQuery`] over the installed margin model, if any.
    pub fn margin_query(&self) -> Option<crate::margin::MarginQuery<'_>> {
        self.margin_model
            .as_ref()
            .map(|m| crate::margin::MarginQuery::new(m.as_ref()))
    }

    /// Sets whether a held intraday (MIS) position is flattened at the session
    /// close (default true). With it off, a held MIS position at
    /// [`Self::on_session_close`] is an error instead.
    #[must_use]
    pub fn with_auto_square_off_mis(mut self, on: bool) -> Self {
        self.auto_square_off_mis = on;
        self
    }

    /// The net signed position in `instrument` (0 when flat).
    pub fn position(&self, instrument: &InstrumentId) -> f64 {
        position_of(&self.inner.lock().unwrap().positions, instrument)
    }

    /// Every non-flat position, in the order first opened.
    pub fn positions(&self) -> Vec<(InstrumentId, f64)> {
        self.inner.lock().unwrap().positions.clone()
    }

    /// Closes the current session at `ts`, flattening held intraday (MIS) positions at the
    /// last observed bar close.
    ///
    /// With [`Self::with_auto_square_off_mis`] on (the default) every instrument marked
    /// intraday is closed with the opposite [`crate::square_off_order`] at the engine's last
    /// observed price, so the run ends flat; the fill surfaces through the usual drains. With
    /// it off, a held MIS position is an error instead (state unchanged). A CNC (delivery)
    /// position is never squared off. When no market calendar is wired, use
    /// [`crate::margin::default_session_close_ts`] (23:59:00 UTC) for `ts`.
    pub fn on_session_close(&mut self, ts: UnixNanos) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let inner = &mut *inner;
        let held: Vec<InstrumentId> = inner.mis.intraday().to_vec();
        if !held.is_empty() && !self.auto_square_off_mis {
            return Err(AlgoError::Component(format!(
                "{} intraday position(s) held at session close {} and auto square-off is off",
                held.len(),
                ts.as_u64()
            )));
        }
        let price = match inner.last_price {
            Some(price) => price,
            None if held.is_empty() => {
                inner.next_ts = inner.next_ts.max(ts.as_u64());
                return Ok(());
            }
            None => {
                return Err(AlgoError::Component(
                    "no observed price to square off an MIS position".into(),
                ))
            }
        };
        for (seq, instrument) in held.iter().enumerate() {
            let position = position_of(&inner.positions, instrument);
            if position.abs() <= QTY_EPS {
                inner.mis.clear(instrument);
                continue;
            }
            let side = if position > 0.0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };
            let qty = position.abs();
            let costs = self.costs.of(side, qty, price, self.currency)?;
            let base = inner.next_ts.max(ts.as_u64());
            let fill_ts = UnixNanos::from_u64(base);
            inner.next_ts = base + 1;
            let trade = Trade::new(
                OrderId::new(format!("__square_off_{seq}")),
                instrument.clone(),
                side,
                qty,
                price,
                self.currency,
                fill_ts,
                fill_ts,
            )
            .with_costs(costs);
            inner.events.push(ExecutionEvent::Fill {
                trade,
                cum_qty: qty,
                complete: true,
                venue_order_id: None,
            });
            apply_position(
                &mut inner.positions,
                &mut inner.mis,
                instrument,
                None,
                if side == OrderSide::Buy { qty } else { -qty },
            );
        }
        if let Some((instrument, qty)) = inner.mis.intraday().iter().find_map(|i| {
            let q = position_of(&inner.positions, i);
            (q.abs() > QTY_EPS).then(|| (i.clone(), q))
        }) {
            return Err(AlgoError::Component(format!(
                "intraday position in {} ({qty}) is not flat after square-off",
                instrument.symbol()
            )));
        }
        Ok(())
    }

    /// The settlement currency.
    pub fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the most recent observed close, if any.
    pub fn last_price(&self) -> Option<f64> {
        self.inner.lock().unwrap().last_price
    }
}

impl Handler for BarFillEngine {
    fn on_event(
        &mut self,
        event: &Event,
        _ts_init: UnixNanos,
    ) -> Result<honba_engine::EngineOutput> {
        if let Event::Bar(b) = event {
            let mut inner = self.inner.lock().unwrap();
            let inner = &mut *inner;
            inner.last_price = Some(b.close());
            let iid = b.bar_type().instrument_id();
            let mut triggered = Vec::new();
            inner.resting_trailing.retain_mut(|stop| {
                if stop.order.instrument_id() != iid {
                    return true;
                }
                match stop.order.side() {
                    OrderSide::Sell => {
                        stop.peak = stop.peak.max(b.high());
                        let trigger = if let Some(amt) = stop.order.trail_amount() {
                            stop.peak - amt
                        } else if let Some(pct) = stop.order.trail_percent() {
                            stop.peak * (1.0 - pct / 100.0)
                        } else {
                            stop.peak
                        };
                        if b.low() <= trigger {
                            let px = if b.open() <= trigger {
                                b.open()
                            } else {
                                trigger
                            };
                            triggered.push((stop.order.clone(), px));
                            false
                        } else {
                            true
                        }
                    }
                    OrderSide::Buy => {
                        stop.trough = stop.trough.min(b.low());
                        let trigger = if let Some(amt) = stop.order.trail_amount() {
                            stop.trough + amt
                        } else if let Some(pct) = stop.order.trail_percent() {
                            stop.trough * (1.0 + pct / 100.0)
                        } else {
                            stop.trough
                        };
                        if b.high() >= trigger {
                            let px = if b.open() >= trigger {
                                b.open()
                            } else {
                                trigger
                            };
                            triggered.push((stop.order.clone(), px));
                            false
                        } else {
                            true
                        }
                    }
                    OrderSide::NoOrderSide | _ => false,
                }
            });
            for (order, price) in triggered {
                let price = self.fill_model.price(order.side(), price, &mut inner.rng);
                let costs = self
                    .costs
                    .of(order.side(), order.quantity(), price, self.currency)?;
                let base = inner.next_ts.max(b.ts_event().as_u64());
                let delay_ns = self
                    .latency_model
                    .total_delay_ms(&mut inner.rng)
                    .saturating_mul(1_000_000);
                let ts = UnixNanos::from_u64(base.saturating_add(delay_ns));
                inner.next_ts = ts.as_u64() + 1;
                let trade = Trade::new(
                    OrderId::new(order.order_id().as_str()),
                    order.instrument_id().clone(),
                    order.side(),
                    order.quantity(),
                    price,
                    self.currency,
                    ts,
                    ts,
                )
                .with_costs(costs);
                inner.events.push(ExecutionEvent::Fill {
                    trade,
                    cum_qty: order.quantity(),
                    complete: true,
                    venue_order_id: None,
                });
                let signed = if order.side() == OrderSide::Buy {
                    order.quantity()
                } else {
                    -order.quantity()
                };
                apply_position(
                    &mut inner.positions,
                    &mut inner.mis,
                    order.instrument_id(),
                    order.product(),
                    signed,
                );
            }
        }
        Ok(honba_engine::EngineOutput::None)
    }
}

impl ExecutionEngine for BarFillEngine {
    fn submit(&mut self, order: Order) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let inner = &mut *inner;
        if order.order_type() == OrderType::TrailingStop {
            let ref_px = inner.last_price.unwrap_or(0.0);
            inner.resting_trailing.push(RestingTrailingStop {
                order,
                peak: ref_px,
                trough: ref_px,
            });
            return Ok(());
        }
        let desired = inner.last_price.unwrap_or(0.0);
        let price = self.fill_model.price(order.side(), desired, &mut inner.rng);
        // Costs first: a fill whose cost cannot be represented is refused
        // before it consumes a timestamp or reaches the fill buffer.
        let costs = self
            .costs
            .of(order.side(), order.quantity(), price, self.currency)?;
        let base = inner.next_ts.max(order.ts_event().as_u64());
        let delay_ns = self
            .latency_model
            .total_delay_ms(&mut inner.rng)
            .saturating_mul(1_000_000);
        let ts = UnixNanos::from_u64(base.saturating_add(delay_ns));
        inner.next_ts = ts.as_u64() + 1;
        let trade = Trade::new(
            OrderId::new(order.order_id().as_str()),
            order.instrument_id().clone(),
            order.side(),
            order.quantity(),
            price,
            self.currency,
            ts,
            ts,
        )
        .with_costs(costs);
        inner.events.push(ExecutionEvent::Fill {
            trade,
            cum_qty: order.quantity(),
            complete: true,
            venue_order_id: None,
        });
        let signed = if order.side() == OrderSide::Buy {
            order.quantity()
        } else {
            -order.quantity()
        };
        apply_position(
            &mut inner.positions,
            &mut inner.mis,
            order.instrument_id(),
            order.product(),
            signed,
        );
        Ok(())
    }

    fn cancel(&mut self, order_id: &str, _now: UnixNanos) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .resting_trailing
            .retain(|s| s.order.order_id().as_str() != order_id);
        Ok(())
    }

    fn drain_events(&mut self) -> Result<Vec<ExecutionEvent>> {
        Ok(std::mem::take(&mut self.inner.lock().unwrap().events))
    }

    fn native_events(&self) -> bool {
        true
    }

    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        let mut inner = self.inner.lock().unwrap();
        let events = std::mem::take(&mut inner.events);
        inner.legacy.absorb(events);
        Ok(inner.legacy.take_fills())
    }

    fn drain_rejections(&mut self) -> Result<Vec<OrderRejection>> {
        let mut inner = self.inner.lock().unwrap();
        let events = std::mem::take(&mut inner.events);
        inner.legacy.absorb(events);
        Ok(inner.legacy.take_rejections())
    }
}
