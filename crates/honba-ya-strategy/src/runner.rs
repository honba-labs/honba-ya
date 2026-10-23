//! The `StrategyRunner`: glues a strategy to a pre-trade risk gate.
//!
//! `honba-engine` (the event-kernel `ExecutionEngine`/`Handler` abstraction
//! this runner used to be built on) has been removed from this crate's
//! dependency graph entirely — it is not an `honba-nou-strategy`
//! dependency any more (see `Cargo.toml`), and has no replacement in
//! `honba-core`/`honba-mon`. Per `AGENTS.md` §2.1/§2.5 the domain layer stays
//! zero-I/O and depends only on abstract contracts, so this runner does not
//! talk to an execution gateway itself: it turns a [`Strategy`]'s submitted
//! [`OrderIntent`]s into [`OrderRequest`]s (after the warm-up gate and an
//! optional [`RiskEngine`] pre-trade check) for the host — an actor runtime,
//! a backtester, anything implementing
//! [`ExecutionGateway`](honba_core::ports::ExecutionGateway) — to actually
//! submit, and reacts to the [`FillReport`]s and [`OrderEvent`]s that host
//! hands back via [`StrategyRunner::on_fill`] and
//! [`StrategyRunner::on_order_event`].

use std::collections::HashMap;

use honba_core::chrono::{DateTime, Utc};
use honba_core::{
    FillReport, InstrumentId, MarketEvent, OrderEvent, OrderRequest, OrderSide, Quantity,
    TradingState,
};
use honba_mon::RiskEngine;
use rust_decimal::Decimal;

use crate::context::LedgerContext;
use crate::error::{Result, StrategyError};
use crate::intent::{IntentError, OrderIntent};
use crate::strategy::{Strategy, StrategyAdapter};

/// An intent the runner turned into an [`OrderRequest`] and queued for the
/// host to submit.
#[derive(Clone, Debug, PartialEq)]
pub struct SubmittedIntent {
    /// The `ts_init` of the event during which it was submitted.
    pub ts_init: DateTime<Utc>,
    /// The intent, as the strategy submitted it.
    pub intent: OrderIntent,
    /// The order request it became; `request.tag` is this strategy's own
    /// `"{name}-{n}"` id, used to correlate later fills and order events.
    pub request: OrderRequest,
}

/// A valid intent the warm-up gate released instead of submitting.
#[derive(Clone, Debug, PartialEq)]
pub struct SuppressedIntent {
    /// The `ts_init` of the event during which it was drained.
    pub ts_init: DateTime<Utc>,
    /// The intent, as the strategy submitted it.
    pub intent: OrderIntent,
}

/// An intent the runner refused to turn into an order because it broke an
/// [`OrderIntent`] invariant.
#[derive(Clone, Debug, PartialEq)]
pub struct IntentRejection {
    /// The rejected intent, as the strategy emitted it.
    pub intent: OrderIntent,
    /// Which invariant it broke.
    pub error: IntentError,
    /// The `ts_init` of the event during which it was emitted.
    pub ts_init: DateTime<Utc>,
}

/// A valid, risk-approved order that was refused, rejected, cancelled, or
/// expired after submission; the unfilled remainder has been released in the
/// context.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderRejection {
    /// This strategy's own `"{name}-{n}"` id for the order.
    pub order_id: String,
    /// The instrument it was on.
    pub instrument_id: InstrumentId,
    /// Which side it was on.
    pub side: OrderSide,
    /// The quantity released (the unfilled remainder).
    pub quantity: Quantity,
    /// Why ("risk_refused", the venue's reject reason, "cancelled", ...).
    pub reason: String,
}

/// What the runner still tracks about an order it has submitted: enough to
/// release the right remainder when it is rejected or cancelled, since
/// `honba_core::OrderEvent::{Rejected,Cancelled}` carry only the order id.
#[derive(Clone, Debug, PartialEq)]
struct OpenOrder {
    instrument_id: InstrumentId,
    side: OrderSide,
    remaining: Decimal,
}

/// Wraps a [`Strategy`] with its [`LedgerContext`] and an optional
/// [`RiskEngine`] pre-trade gate (ADR 008; the Python `StrategyRunner`
/// follows the same steps), without owning any execution gateway itself:
///
/// 1. [`Self::on_event`] sets the context clock to the event's `ts_init` and
///    dispatches a bar, quote or trade event to its hook (other events reach
///    no hook).
/// 2. It then drains every [`OrderIntent`] submitted since the last drain,
///    including those from [`Strategy::on_start`] and the previous event's
///    [`Strategy::on_fill`].
/// 3. Each is validated, converted into an [`OrderRequest`] tagged
///    `"{name}-{n}"`, and passed through the [`RiskEngine`] if one is
///    attached. An intent that breaks the [`OrderIntent`] invariants is
///    never submitted: it is released in the context, recorded as an
///    [`IntentRejection`] (see [`Self::rejections`]) and reported through
///    [`Strategy::on_intent_rejected`]; the run continues. A risk refusal is
///    also released and recorded (see [`Self::order_rejections`]), but
///    — being a different error shape to [`IntentError`] — is not reported
///    through [`Strategy::on_intent_rejected`].
/// 4. Everything that survives is queued in [`Self::drain_orders`] for the
///    host to actually submit.
/// 5. The host calls [`Self::on_fill`] for every [`FillReport`] and
///    [`Self::on_order_event`] for every other [`OrderEvent`] it receives
///    back; a fill is booked in the context then passed to
///    [`Strategy::on_fill`], while a rejection or cancellation releases the
///    unfilled remainder of the order it names.
///
/// Intents submitted in [`Strategy::on_stop`] are discarded.
///
/// **Warm-up gate** ([`StrategyManifest::warmup_bars`](crate::StrategyManifest),
/// set with [`Self::with_warmup_bars`]). A *driving bar* is a
/// [`MarketEvent::Bar`] whose `ts_init` differs from the previous bar
/// event's, so bars of several instruments at one time count once. While at
/// most `warmup_bars` driving bars have been seen (and before the first
/// one), the strategy still receives every event, but each valid intent is
/// released in the context and recorded as a [`SuppressedIntent`] (see
/// [`Self::suppressed`]) instead of becoming an order. Invalid intents are
/// still rejected as usual.
pub struct StrategyRunner<S: Strategy> {
    adapter: StrategyAdapter<S>,
    order_seq: u64,
    submitted: Vec<SubmittedIntent>,
    fills: Vec<FillReport>,
    rejections: Vec<IntentRejection>,
    order_rejections: Vec<OrderRejection>,
    warmup_bars: u32,
    bars_seen: u64,
    last_bar_ts: Option<DateTime<Utc>>,
    /// `ts_init` of the latest event processed.
    now: DateTime<Utc>,
    suppressed: Vec<SuppressedIntent>,
    /// Orders submitted and not yet fully filled/released, keyed by this
    /// runner's own `"{name}-{n}"` id.
    open_orders: HashMap<String, OpenOrder>,
    released: HashMap<InstrumentId, Decimal>,
    risk: Option<RiskEngine>,
    trading_state: TradingState,
    outbox: Vec<OrderRequest>,
}

impl<S: Strategy> StrategyRunner<S> {
    /// Creates a runner from a strategy, with an empty [`LedgerContext`] and
    /// no risk gate.
    pub fn new(strategy: S) -> Self {
        Self::with_context(strategy, LedgerContext::new())
    }

    /// Creates a runner whose strategy reads and writes `ctx` (initial cash,
    /// instrument metadata).
    pub fn with_context(strategy: S, ctx: LedgerContext) -> Self {
        Self {
            adapter: StrategyAdapter::with_context(strategy, ctx),
            order_seq: 0,
            submitted: Vec::new(),
            fills: Vec::new(),
            rejections: Vec::new(),
            order_rejections: Vec::new(),
            warmup_bars: 0,
            bars_seen: 0,
            last_bar_ts: None,
            now: DateTime::<Utc>::MIN_UTC,
            suppressed: Vec::new(),
            open_orders: HashMap::new(),
            released: HashMap::new(),
            risk: None,
            trading_state: TradingState::Active,
            outbox: Vec::new(),
        }
    }

    /// Attaches a pre-trade [`RiskEngine`]: every order this runner queues
    /// is checked against it first, and a refusal is recorded (see
    /// [`Self::order_rejections`]) instead of being queued. Without one, the
    /// runner still applies the warm-up gate but performs no risk check.
    #[must_use]
    pub fn with_risk(mut self, risk: RiskEngine) -> Self {
        self.risk = Some(risk);
        self
    }

    /// Suppresses orders until `warmup_bars` driving bars have been seen.
    #[must_use]
    pub fn with_warmup_bars(mut self, warmup_bars: u32) -> Self {
        self.warmup_bars = warmup_bars;
        self
    }

    /// The number of warm-up bars this runner enforces.
    pub fn warmup_bars(&self) -> u32 {
        self.warmup_bars
    }

    /// True while orders are suppressed: at most `warmup_bars` driving bars seen.
    pub fn warming_up(&self) -> bool {
        self.warmup_bars > 0 && self.bars_seen <= u64::from(self.warmup_bars)
    }

    /// Returns every intent the warm-up gate released, in order.
    pub fn suppressed(&self) -> &[SuppressedIntent] {
        &self.suppressed
    }

    /// Returns the strategy's context (clock, positions, cash).
    pub fn context(&self) -> &LedgerContext {
        self.adapter.context()
    }

    /// Returns every intent submitted as an order, in order.
    pub fn submitted(&self) -> &[SubmittedIntent] {
        &self.submitted
    }

    /// Returns a shared reference to the wrapped strategy.
    pub fn strategy(&self) -> &S {
        self.adapter.inner()
    }

    /// Returns a mutable reference to the wrapped strategy.
    pub fn strategy_mut(&mut self) -> &mut S {
        self.adapter.inner_mut()
    }

    /// Returns a shared reference to the attached risk engine, if any.
    pub fn risk_engine(&self) -> Option<&RiskEngine> {
        self.risk.as_ref()
    }

    /// Returns all fills booked during the run.
    pub fn fills(&self) -> &[FillReport] {
        &self.fills
    }

    /// Returns every intent rejected during the run, in emission order.
    pub fn rejections(&self) -> &[IntentRejection] {
        &self.rejections
    }

    /// Returns every order (or part of one) that was refused, rejected or
    /// cancelled, in the order it was recorded. Each was released in the
    /// context.
    pub fn order_rejections(&self) -> &[OrderRejection] {
        &self.order_rejections
    }

    /// The trading state the runner enforces: [`TradingState::Active`] until
    /// [`Self::on_trading_state`] reports a change.
    pub fn trading_state(&self) -> &TradingState {
        &self.trading_state
    }

    /// Records a trading-state change (e.g. a global or per-algo halt).
    pub fn on_trading_state(&mut self, state: TradingState) {
        self.trading_state = state;
    }

    /// The total quantity released for `instrument_id` so far: the unfilled
    /// remainders of its rejected, cancelled and expired orders.
    pub fn released_quantity(&self, instrument_id: &InstrumentId) -> Decimal {
        self.released
            .get(instrument_id)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    /// Takes every [`OrderRequest`] queued since the last drain, in
    /// submission order, for the host to actually submit through its own
    /// execution gateway.
    pub fn drain_orders(&mut self) -> Vec<OrderRequest> {
        std::mem::take(&mut self.outbox)
    }

    /// Consumes the runner, returning the strategy and the fills it booked.
    pub fn into_parts(self) -> (S, Vec<FillReport>) {
        (self.adapter.into_inner(), self.fills)
    }

    /// Calls [`Strategy::on_start`]. Intents it submits stay queued in the
    /// context and are processed with the first event, alongside that
    /// event's own intents (same timestamp, same risk check) — mirroring the
    /// pre-migration runner's documented behaviour.
    pub fn on_start(&mut self) -> Result<()> {
        self.adapter.on_start()
    }

    /// Sets the clock to `ts_init`, dispatches `event` to the strategy, and
    /// queues (in [`Self::drain_orders`]) whatever intents it submits, after
    /// the warm-up gate and risk check.
    pub fn on_event(&mut self, event: &MarketEvent, ts_init: DateTime<Utc>) -> Result<()> {
        self.now = ts_init;
        if matches!(event, MarketEvent::Bar(_)) && self.last_bar_ts != Some(ts_init) {
            self.bars_seen += 1;
            self.last_bar_ts = Some(ts_init);
        }

        self.adapter.on_event(event, ts_init)?;
        self.drain_and_submit(ts_init)
    }

    /// Calls [`Strategy::on_stop`]. Intents it submits are discarded (ADR
    /// 008: the run is over).
    pub fn on_stop(&mut self) -> Result<()> {
        self.adapter.on_stop()?;
        self.adapter.drain_intents();
        Ok(())
    }

    /// Books `fill` in the context, updates the matching [`OpenOrder`]'s
    /// remaining quantity, records it with the attached [`RiskEngine`] (if
    /// any), and calls [`Strategy::on_fill`].
    pub fn on_fill(&mut self, fill: &FillReport) -> Result<()> {
        if let Some(open) = self.open_orders.get_mut(fill.order_id.as_str()) {
            open.remaining = (open.remaining - fill.quantity.0).max(Decimal::ZERO);
            if open.remaining.is_zero() {
                self.open_orders.remove(fill.order_id.as_str());
            }
        }
        if let Some(risk) = self.risk.as_mut() {
            risk.record_fill(fill);
        }
        let (strategy, ctx) = self.adapter.parts_mut();
        ctx.apply_fill(fill)?;
        strategy.on_fill(ctx, fill)?;
        self.fills.push(fill.clone());
        Ok(())
    }

    /// Handles a post-submission [`OrderEvent`] other than a fill: an
    /// acknowledgement moves nothing; a rejection or cancellation releases
    /// the order's unfilled remainder in the context and records an
    /// [`OrderRejection`].
    pub fn on_order_event(&mut self, event: &OrderEvent) -> Result<()> {
        let (order_id, reason) = match event {
            OrderEvent::Acked(_) | OrderEvent::Filled(_) => return Ok(()),
            OrderEvent::Rejected { order_id, reason } => (order_id.clone(), format!("{reason:?}")),
            OrderEvent::Cancelled { order_id } => (order_id.clone(), "cancelled".to_string()),
        };
        let Some(open) = self.open_orders.remove(&order_id) else {
            // Unknown or already-finished order: nothing left to release.
            return Ok(());
        };
        if open.remaining > Decimal::ZERO {
            self.adapter.parts_mut().1.release_remainder(
                &open.instrument_id,
                open.side,
                open.remaining,
            );
            *self
                .released
                .entry(open.instrument_id.clone())
                .or_insert(Decimal::ZERO) += open.remaining;
        }
        self.order_rejections.push(OrderRejection {
            order_id,
            instrument_id: open.instrument_id,
            side: open.side,
            quantity: Quantity::new(open.remaining),
            reason,
        });
        Ok(())
    }

    /// Validates and queues the intents the strategy submitted during the
    /// current event, as described in the type's docs. An error is terminal
    /// for the call; any intent not yet processed when it happens is
    /// released first, so the context never keeps an instrument busy for an
    /// order that was never queued.
    fn drain_and_submit(&mut self, ts_init: DateTime<Utc>) -> Result<()> {
        let mut intents = self.adapter.drain_intents().into_iter();
        while let Some(intent) = intents.next() {
            if let Err(error) = intent.validate() {
                let (strategy, ctx) = self.adapter.parts_mut();
                ctx.release(&intent);
                let hook = strategy.on_intent_rejected(ctx, &intent, &error);
                self.rejections.push(IntentRejection {
                    intent,
                    error,
                    ts_init,
                });
                hook?;
                continue;
            }
            if self.warming_up() {
                self.adapter.parts_mut().1.release(&intent);
                self.suppressed.push(SuppressedIntent { ts_init, intent });
                continue;
            }

            let name = self.adapter.inner().name().to_string();
            let order_id = format!("{name}-{}", self.order_seq);
            self.order_seq += 1;

            let mut request = match intent.clone().into_order(name) {
                Ok(request) => request,
                Err(e) => {
                    self.adapter.parts_mut().1.release(&intent);
                    for remaining in intents {
                        self.adapter.parts_mut().1.release(&remaining);
                    }
                    return Err(StrategyError::from(e));
                }
            };
            request.tag = Some(order_id.clone());

            if let Some(refusal) = self
                .risk
                .as_mut()
                .and_then(|risk| risk.check_order(&request, ts_init).err())
            {
                self.adapter.parts_mut().1.release(&intent);
                self.order_rejections.push(OrderRejection {
                    order_id,
                    instrument_id: intent.instrument_id.clone(),
                    side: intent.side,
                    quantity: intent.quantity,
                    reason: refusal.to_string(),
                });
                continue;
            }

            self.open_orders.insert(
                order_id.clone(),
                OpenOrder {
                    instrument_id: intent.instrument_id.clone(),
                    side: intent.side,
                    remaining: intent.quantity.0,
                },
            );
            self.outbox.push(request.clone());
            self.submitted.push(SubmittedIntent {
                ts_init,
                intent,
                request,
            });
        }
        Ok(())
    }
}
