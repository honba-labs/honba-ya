//! The event loop.

use std::collections::HashMap;

use honba_entities::{ExecutionEvent, Trade};
use honba_messages::{
    ErrorCode, Event, InstrumentId, Message, Order, OrderEventKind, OrderId, OrderSide, OrderState,
    OrderStatus, UnixNanos, VenueOrderId,
};
use honba_risk::{
    check_state, DurableRiskState, FillFingerprint, FillLedger, InFlightOrder, RiskCheck,
    RiskDecision, RiskLimits, RiskRequest, RiskStage,
};

use crate::audit::{AuditKind, AuditLog, AuditRecord};
use crate::cache::StateCache;
use crate::clock::Clock;
use crate::data::DataFeed;
use crate::error::{AlgoError, Result};
use crate::execution::{ExecutionEngine, LegacyPortEvents};
use crate::handler::{EngineOutput, Handler};
use crate::queue::EventQueue;
use crate::state::TradingState;

/// Default number of events pulled from the feed before dispatching.
///
/// Larger values reorder more aggressively within a window; smaller values
/// reduce latency for live streams. Set to `1` when the feed guarantees
/// non-decreasing `ts_event` and you want immediate dispatch.
pub const DEFAULT_BATCH_SIZE: usize = 1024;

pub use crate::cache::TrackedOrder;

/// Drives events from a [`DataFeed`] through a set of [`Handler`]s.
///
/// An engine belongs to one thread: it holds the clock, the queue, the
/// handlers, the execution sink and the audit log, and shares none of them.
/// Drive it from a single thread, and to hand work to another, move the whole
/// engine rather than its parts.
pub struct Engine {
    clock: Clock,
    queue: EventQueue,
    handlers: Vec<Box<dyn Handler>>,
    batch_size: usize,
    execution: Option<Box<dyn ExecutionEngine>>,
    trading_state: TradingState,
    audit: AuditLog,
    /// Every execution event acknowledged and not yet drained, in order.
    observed: Vec<ExecutionEvent>,
    cache: StateCache,
    orders: HashMap<String, TrackedOrder>,
    positions: HashMap<InstrumentId, f64>,
    /// Last bar close or trade price per instrument: the stage's `reference_price`.
    last_px: HashMap<InstrumentId, f64>,
    /// Last quoted `(bid, ask)` per instrument: the price collar's `expected_price` source.
    last_quote: HashMap<InstrumentId, (f64, f64)>,
    /// Last market-data feed timestamp per instrument.
    last_feed_ts: HashMap<InstrumentId, UnixNanos>,
    risk: Option<RiskStage>,
    fill_ledger: FillLedger,
    started: bool,
    finished: bool,
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    /// Creates an engine with the default batch size.
    pub fn new() -> Self {
        Self {
            clock: Clock::default(),
            queue: EventQueue::new(),
            handlers: Vec::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            execution: None,
            trading_state: TradingState::Active,
            audit: AuditLog::new(),
            observed: Vec::new(),
            cache: StateCache::new(),
            orders: HashMap::new(),
            positions: HashMap::new(),
            last_px: HashMap::new(),
            last_quote: HashMap::new(),
            last_feed_ts: HashMap::new(),
            risk: None,
            fill_ledger: FillLedger::new(),
            started: false,
            finished: false,
        }
    }

    /// Sets the batch size.
    ///
    /// The feed is read up to `batch_size` events at a time, then the queue
    /// is drained in `ts_event` order before the next batch is pulled. Use
    /// `1` for a strictly-ordered feed that must dispatch immediately.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        assert!(batch_size > 0, "batch_size must be positive");
        self.batch_size = batch_size;
        self
    }

    /// Puts `stage` in front of the execution sink: every order the engine submits passes it,
    /// and a refusal is audited as [`AuditKind::RiskRefused`] then
    /// [`AuditKind::OrderRejected`] and never reaches the sink (ADR 0018 decisions 6-7).
    ///
    /// Without a stage the engine still applies the state rules (`Halted`, reduce-only). A run
    /// holds at most one stage: see [`Handler::holds_risk_stage`].
    pub fn with_risk(mut self, stage: RiskStage) -> Self {
        self.risk = Some(stage);
        self
    }

    /// The live-run guard (ADR 0018 decision 8): `Ok` only when the engine's stage was built
    /// with both `max_notional` and `order_rate`. An assembler building a non-simulated run
    /// calls this before starting; an engine without a stage fails like one without limits.
    pub fn require_live_limits(&self) -> Result<()> {
        let limits = match &self.risk {
            Some(stage) => stage.limits().clone(),
            None => RiskLimits::default(),
        };
        limits.require_live().map_err(AlgoError::from)
    }

    /// Seeds the position map (net signed quantity per instrument) that fills
    /// then move (ADR 0018 decision 7). A later entry for the same instrument
    /// replaces the earlier one.
    pub fn with_positions(mut self, seed: impl IntoIterator<Item = (InstrumentId, f64)>) -> Self {
        let items: Vec<(InstrumentId, f64)> = seed.into_iter().collect();
        self.positions.extend(items.iter().cloned());
        for (inst, qty) in items {
            self.cache.seed_position(inst, qty);
        }
        self
    }

    /// The net signed position in `instrument` (0 when never traded).
    pub fn position(&self, instrument: &InstrumentId) -> f64 {
        self.positions.get(instrument).copied().unwrap_or(0.0)
    }

    /// The tracked state of order `order_id`, if the engine submitted or
    /// refused it.
    pub fn order(&self, order_id: &str) -> Option<&TrackedOrder> {
        self.orders.get(order_id)
    }

    /// The signed open quantity (`+` buy, `-` sell) of the working orders on
    /// `instrument` and `side`: the sum of `quantity - filled_qty` over orders
    /// that are `Submitted`, `Accepted` or `PartiallyFilled` (ADR 0019
    /// decision 5). A reduce-only check uses `position + working_exposure`.
    pub fn working_exposure(&self, instrument: &InstrumentId, side: OrderSide) -> f64 {
        let sign = match side {
            OrderSide::Buy => 1.0,
            OrderSide::Sell => -1.0,
            _ => return 0.0,
        };
        sign * self
            .orders
            .values()
            .filter(|o| o.is_working() && o.side == side && &o.instrument_id == instrument)
            .map(|o| o.state.quantity - o.state.filled_qty)
            .sum::<f64>()
    }

    /// Returns the current clock value.
    pub fn now(&self) -> UnixNanos {
        self.clock.now()
    }

    /// Registers a handler.
    pub fn add_handler<H: Handler + 'static>(&mut self, handler: H) {
        self.handlers.push(Box::new(handler));
    }

    /// Attaches an execution sink. [`EngineOutput::Orders`] / `Cancels` are routed here.
    ///
    /// A sink that implements only the legacy drains
    /// ([`ExecutionEngine::native_events`] is `false`) is wrapped in a
    /// [`LegacyPortEvents`], so its partial fills are reported correctly.
    /// Replacing a sink drops the old one; the events already acknowledged
    /// stay available through [`Engine::drain_events`].
    pub fn set_execution(&mut self, execution: Box<dyn ExecutionEngine>) {
        let execution: Box<dyn ExecutionEngine> = if execution.native_events() {
            execution
        } else {
            Box::new(LegacyPortEvents::new(execution))
        };
        self.execution = Some(execution);
    }

    /// Returns the attached execution sink, if any.
    pub fn execution(&self) -> Option<&dyn ExecutionEngine> {
        self.execution.as_deref()
    }

    /// Returns the current trading state.
    pub fn trading_state(&self) -> TradingState {
        self.trading_state
    }

    /// Returns the audit trail, oldest record first.
    pub fn audit(&self) -> &[AuditRecord] {
        self.audit.records()
    }

    /// Returns the complete audit log.
    pub fn audit_log(&self) -> &AuditLog {
        &self.audit
    }

    /// Returns the engine's idempotent fill ledger.
    pub fn fill_ledger(&self) -> &FillLedger {
        &self.fill_ledger
    }

    /// Returns a mutable reference to the engine's idempotent fill ledger.
    pub fn fill_ledger_mut(&mut self) -> &mut FillLedger {
        &mut self.fill_ledger
    }

    /// Sets or replaces the engine's fill ledger (e.g. restored from durable state).
    pub fn with_fill_ledger(mut self, ledger: FillLedger) -> Self {
        self.fill_ledger = ledger;
        self
    }

    /// Exports current positions, in-flight orders, and fill ledger as [`DurableRiskState`].
    pub fn export_durable_risk(&self) -> DurableRiskState {
        let mut state = DurableRiskState::new();
        state.fill_ledger = self.fill_ledger.clone();
        state.last_updated_ns = self.clock.now().as_u64();
        for (inst, &qty) in &self.positions {
            state.set_position(inst, qty);
        }
        for (id, tracked) in &self.orders {
            if matches!(
                tracked.state.status,
                OrderStatus::Submitted | OrderStatus::Accepted | OrderStatus::PartiallyFilled
            ) {
                state.add_in_flight(InFlightOrder::new(
                    OrderId::new(id),
                    tracked.instrument_id.clone(),
                    tracked.side,
                    tracked.state.quantity,
                    None,
                    tracked.venue_order_id.clone(),
                    self.clock.now(),
                ));
            }
        }
        state
    }

    /// Restores positions, in-flight orders, and fill ledger from [`DurableRiskState`].
    pub fn with_durable_risk(mut self, state: DurableRiskState) -> Self {
        self.fill_ledger = state.fill_ledger;
        for (key, qty) in state.positions {
            let parts: Vec<&str> = key.split(".").collect();
            if parts.len() == 2 {
                let inst = InstrumentId::new(parts[0], honba_messages::Exchange::new(parts[1]));
                self.positions.insert(inst.clone(), qty);
                self.cache.seed_position(inst, qty);
            }
        }
        self
    }

    /// Returns the engine's state cache (orders, positions, instruments, and market data).
    pub fn cache(&self) -> &StateCache {
        &self.cache
    }

    /// Returns a mutable reference to the engine's state cache.
    pub fn cache_mut(&mut self) -> &mut StateCache {
        &mut self.cache
    }

    /// Reconciles broker state against the engine's cache and returns the report.
    pub fn reconcile(
        &self,
        snapshot: &crate::reconciliation::BrokerSnapshot,
    ) -> crate::reconciliation::ReconciliationReport {
        crate::reconciliation::Reconciler::reconcile(self.cache(), snapshot, self.clock.now())
    }

    /// Reconciles broker state and injects synthetic events into the event queue.
    pub fn reconcile_and_inject(
        &mut self,
        snapshot: &crate::reconciliation::BrokerSnapshot,
    ) -> crate::reconciliation::ReconciliationReport {
        let report = self.reconcile(snapshot);
        let now = self.clock.now();
        report.apply_to_cache(&mut self.cache);
        for fill in &report.missed_fills {
            let signed = match fill.side {
                OrderSide::Sell => -fill.quantity,
                _ => fill.quantity,
            };
            *self
                .positions
                .entry(fill.instrument_id.clone())
                .or_insert(0.0) += signed;
            if let Some(tracked) = self.orders.get_mut(fill.order_id.as_str()) {
                let _ = tracked.state.apply(&honba_messages::OrderEvent::Fill {
                    last_qty: fill.quantity,
                    complete: fill.completes_order,
                });
            }
        }
        for stale in &report.stale_orders {
            if let Some(tracked) = self.orders.get_mut(stale.order_id.as_str()) {
                let _ = tracked.state.apply(&honba_messages::OrderEvent::Cancelled);
            }
        }
        for event in &report.synthetic_events {
            self.inject(Message::new(event.clone(), now));
        }
        report
    }

    /// Injects a message into the queue, exactly as a feed would.
    ///
    /// Used by tests and by the async shell; does not advance the clock.
    pub fn inject(&mut self, msg: Message) {
        self.queue.push(msg);
    }

    /// Number of messages waiting in the queue.
    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    /// Runs `on_start` for every handler. Idempotent: a second call is a no-op.
    ///
    /// [`Engine::run`] calls this for you. A driver that injects messages
    /// incrementally — the async shell, or anything else that owns its own
    /// loop — calls it instead of `run`, then dispatches with
    /// [`Engine::pump`] and closes the lifecycle with [`Engine::finish`].
    ///
    /// Each handler's `on_start` runs at most once: the lifecycle is marked
    /// started before the handlers are called, so a handler that fails
    /// half-way through the fan-out is not started a second time by a retry.
    pub fn start(&mut self) -> Result<()> {
        if self.started {
            return Ok(());
        }
        let stages = usize::from(self.risk.is_some())
            + self
                .handlers
                .iter()
                .filter(|h| h.holds_risk_stage())
                .count();
        if stages > 1 {
            return Err(AlgoError::DuplicateRiskStage);
        }
        self.started = true;
        for h in &mut self.handlers {
            h.on_start()?;
        }
        Ok(())
    }

    /// Dispatches one queued message, if any. Returns false when the queue is empty.
    ///
    /// This is the one place a queued message becomes a handler call: the
    /// clock is advanced to the message's `ts_event`, the dispatch is audited,
    /// and each handler in registration order is asked for an
    /// [`EngineOutput`] whose commands are applied before the next handler
    /// runs. After every handler the execution sink is drained through
    /// [`Engine::acknowledge_events`], which turns each event back into a
    /// queued lifecycle message, so a caller that pumps until `false` closes
    /// the command/ack loop in one queue.
    ///
    /// Errors are returned verbatim: an event earlier than one already
    /// dispatched yields [`AlgoError::ClockRegression`].
    pub fn pump(&mut self) -> Result<bool> {
        let Some(msg) = self.queue.pop() else {
            return Ok(false);
        };
        let ts_event = msg.event().ts_event();
        self.clock.advance_to(ts_event)?;
        self.audit.record(AuditKind::EventDispatched {
            ts_event: ts_event.as_u64(),
        });
        self.observe_price(msg.event());
        for i in 0..self.handlers.len() {
            let output = self.handlers[i].on_event(msg.event(), msg.ts_init());
            // Merge first, even on error: what the handler already refused stays audited.
            self.merge_handler_audit(i);
            let output = output?;
            self.apply_output(output)?;
            self.acknowledge_events()?;
        }
        Ok(true)
    }

    /// Appends the records handler `i` reports through [`Handler::drain_audit`] to the audit
    /// log, in the order given, so a refusal the handler made itself is in the engine's trail.
    fn merge_handler_audit(&mut self, i: usize) {
        for kind in self.handlers[i].drain_audit() {
            self.audit.record(kind);
        }
    }

    /// Remembers the last bar close or trade price per instrument for the risk request.
    fn observe_price(&mut self, event: &Event) {
        if event.is_market_data() {
            self.cache.apply_event(event);
        }
        match event {
            Event::Quote(quote) => {
                self.last_feed_ts
                    .insert(quote.instrument_id().clone(), quote.ts_event());
                self.last_quote.insert(
                    quote.instrument_id().clone(),
                    (quote.bid_price(), quote.ask_price()),
                );
            }
            Event::Bar(bar) => {
                self.last_feed_ts
                    .insert(bar.bar_type().instrument_id().clone(), bar.ts_event());
                self.last_px
                    .insert(bar.bar_type().instrument_id().clone(), bar.close());
            }
            Event::Trade(tick) => {
                self.last_feed_ts
                    .insert(tick.instrument_id().clone(), tick.ts_event());
                self.last_px
                    .insert(tick.instrument_id().clone(), tick.price());
            }
            _ => {}
        }
    }

    /// Drains the queue via [`Engine::pump`], then runs `on_stop` for every handler. Idempotent.
    ///
    /// [`Engine::run`] calls this for you. A driver that injected messages
    /// incrementally calls it instead: draining before stopping is what makes
    /// the audit trail complete, because a message that was accepted into the
    /// queue is dispatched before any handler is told the run is over.
    ///
    /// Each handler's `on_stop` runs at most once, on the same terms as
    /// [`Engine::start`]: a handler that fails half-way through the fan-out is
    /// not stopped a second time by a retry. The queue is drained either way.
    pub fn finish(&mut self) -> Result<()> {
        while self.pump()? {}
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        for h in &mut self.handlers {
            h.on_stop()?;
        }
        Ok(())
    }

    /// Sets the trading state, honouring [`TradingState::can_transition_to`], and records
    /// [`AuditKind::StateChanged`] when it actually changes. Returns the previous state.
    ///
    /// A transition to the state the engine is already in is not a change and
    /// records nothing; a transition [`TradingState::can_transition_to`]
    /// refuses leaves the state alone and records nothing. Both are still
    /// reported through the returned previous state.
    ///
    /// This is the only implementation of the state transition: a handler's
    /// [`EngineOutput::StateChange`] and the async shell's operator command
    /// both go through it, so they cannot drift apart.
    pub fn set_trading_state(&mut self, next: TradingState) -> TradingState {
        let previous = self.trading_state;
        if previous != next && previous.can_transition_to(next) {
            self.trading_state = next;
            self.audit.record(AuditKind::StateChanged {
                from: previous,
                to: next,
            });
            for i in 0..self.handlers.len() {
                self.handlers[i].on_trading_state(next);
                self.merge_handler_audit(i);
            }
        }
        previous
    }

    /// Drains every execution event acknowledged since the last call, in the
    /// one queue order (ADR 0019 decision 4): the submitter's `Submitted` and
    /// `CancelRequested`, pre-gate `Rejected`, and the sink's events.
    ///
    /// Events the engine already acknowledged during a run come first, then
    /// whatever the sink has produced since, which this call acknowledges
    /// (and so enqueues as lifecycle messages). Calling it twice in a row
    /// returns an empty second batch.
    pub fn drain_events(&mut self) -> Result<Vec<ExecutionEvent>> {
        self.acknowledge_events()?;
        Ok(std::mem::take(&mut self.observed))
    }

    /// Legacy: drains the fills acknowledged since the last call, in
    /// production order, leaving every other event for
    /// [`Engine::drain_events`]. Kept until 0.3.0 (ADR 0019 decision 4).
    pub fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        self.acknowledge_events()?;
        let mut fills = Vec::new();
        self.observed.retain(|ev| match ev {
            ExecutionEvent::Fill { trade, .. } => {
                fills.push(trade.clone());
                false
            }
            _ => true,
        });
        Ok(fills)
    }

    /// Drains the execution sink and applies each event to its order's
    /// [`OrderState`] (ADR 0019 decision 5).
    ///
    /// A legal transition produces exactly one lifecycle message on the
    /// queue, stamped with the kernel clock; a fill also moves the position
    /// map. A duplicate terminal event is a no-op. An illegal one is recorded
    /// as [`AuditKind::IllegalTransition`] and changes nothing, except that an
    /// illegal fill is still booked into the position map: money moved at the
    /// venue. Never panics on venue input.
    pub fn acknowledge_events(&mut self) -> Result<()> {
        let Some(execution) = self.execution.as_deref_mut() else {
            return Ok(());
        };
        for ev in execution.drain_events()? {
            self.translate(ev);
        }
        Ok(())
    }

    /// Applies one event to the order store and emits its message.
    fn translate(&mut self, ev: ExecutionEvent) {
        let order_id = ev.order_id().as_str().to_string();
        let now = self.clock.now();
        let kind = ev.order_event().kind();
        if let ExecutionEvent::Fill { trade, .. } = &ev {
            if !self.fill_ledger.record(trade) {
                let order_id = trade.order_id().as_str().to_string();
                let fingerprint = FillFingerprint::from_trade(trade).as_str().to_string();
                self.audit.record(AuditKind::DuplicateFillIgnored {
                    order_id,
                    fingerprint,
                });
                return;
            }
            let signed = match trade.side() {
                OrderSide::Sell => -trade.quantity(),
                _ => trade.quantity(),
            };
            *self
                .positions
                .entry(trade.instrument_id().clone())
                .or_insert(0.0) += signed;
            self.cache.seed_position(
                trade.instrument_id().clone(),
                self.positions[trade.instrument_id()],
            );
        }
        let Some(tracked) = self.orders.get_mut(&order_id) else {
            // Nothing was sent under this id: every event is illegal for it.
            self.audit.record(AuditKind::IllegalTransition {
                order_id,
                error: honba_messages::IllegalTransition::Transition {
                    status: OrderStatus::Initialized,
                    cancel_requested: false,
                    event: kind,
                },
            });
            self.observed.push(ev);
            return;
        };
        if let Some(received) = venue_order_id(&ev) {
            match &tracked.venue_order_id {
                None => tracked.venue_order_id = Some(received.clone()),
                Some(recorded) if recorded != received => {
                    let recorded = recorded.clone();
                    self.audit.record(AuditKind::VenueOrderIdDrift {
                        order_id: order_id.clone(),
                        recorded,
                        received: received.clone(),
                    });
                }
                Some(_) => {}
            }
        }
        let from = tracked.state.status;
        match tracked.state.apply(&ev.order_event()) {
            Err(error) => {
                self.audit
                    .record(AuditKind::IllegalTransition { order_id, error });
            }
            Ok(false) => {}
            Ok(true) => {
                let state = tracked.state.clone();
                let message = self.message_for(&ev, &state, now);
                match &ev {
                    ExecutionEvent::Fill { trade, .. } => {
                        self.audit.record(AuditKind::FillProduced {
                            order_id,
                            quantity: trade.quantity(),
                            price: trade.price(),
                            ts_event: now.as_u64(),
                        });
                    }
                    ExecutionEvent::Submitted { .. } | ExecutionEvent::CancelRequested { .. } => {}
                    ExecutionEvent::Rejected { .. } if from == OrderStatus::Initialized => {}
                    _ => {
                        self.audit.record(AuditKind::OrderLifecycle {
                            order_id,
                            event: kind,
                            ts_event: now.as_u64(),
                        });
                    }
                }
                if let Some(message) = message {
                    self.queue.push_event(message, now);
                }
            }
        }
        if let Some(t) = self.orders.get(ev.order_id().as_str()) {
            self.cache
                .seed_order(ev.order_id().as_str().to_string(), t.clone());
        }
        self.observed.push(ev);
    }

    /// The wire message for a legal transition; `Submitted` is built by
    /// [`Engine::submit`], which has the order.
    fn message_for(
        &self,
        ev: &ExecutionEvent,
        state: &OrderState,
        now: UnixNanos,
    ) -> Option<Event> {
        let order_id = ev.order_id().clone();
        Some(match ev {
            ExecutionEvent::Submitted { .. } => return None,
            ExecutionEvent::Accepted { venue_order_id, .. } => Event::OrderAccepted {
                order_id,
                venue_order_id: venue_order_id.clone(),
                ts_event: now,
            },
            ExecutionEvent::Rejected { reason, .. } => Event::OrderRejected {
                order_id,
                reason: reason.clone(),
                ts_event: now,
            },
            ExecutionEvent::Fill { trade, .. } if state.status == OrderStatus::Filled => {
                Event::OrderFilled {
                    order_id,
                    last_qty: trade.quantity(),
                    last_px: trade.price(),
                    ts_event: now,
                }
            }
            ExecutionEvent::Fill { trade, .. } => Event::OrderPartiallyFilled {
                order_id,
                last_qty: trade.quantity(),
                last_px: trade.price(),
                cum_qty: state.filled_qty,
                ts_event: now,
            },
            ExecutionEvent::CancelRequested { .. } => Event::OrderCancelRequested {
                order_id,
                ts_event: now,
            },
            ExecutionEvent::Cancelled { .. } => Event::OrderCancelled {
                order_id,
                ts_event: now,
            },
            ExecutionEvent::Expired { .. } => Event::OrderExpired {
                order_id,
                ts_event: now,
            },
            _ => return None,
        })
    }

    /// Refuses `order` before it reaches the sink (ADR 0019 decision 5):
    /// `Initialized -> Rejected`, audited, and an `ExecutionEvent::Rejected`
    /// on the one event queue.
    fn refuse(&mut self, order: &Order, code: ErrorCode) {
        let order_id = order.order_id().as_str().to_string();
        let reason = code.as_str().to_string();
        self.audit.record(AuditKind::OrderRejected {
            order_id: order_id.clone(),
            reason: reason.clone(),
        });
        let tracked = TrackedOrder {
            state: OrderState::new(),
            instrument_id: order.instrument_id().clone(),
            side: order.side(),
            venue_order_id: None,
        };
        self.cache.seed_order(order_id.clone(), tracked.clone());
        self.orders.insert(order_id, tracked);
        self.translate(ExecutionEvent::Rejected {
            order_id: order.order_id().clone(),
            instrument_id: order.instrument_id().clone(),
            side: order.side(),
            quantity: order.quantity(),
            reason,
            venue_order_id: None,
            ts: self.clock.now(),
        });
    }

    /// The touch a market order would cross (ask for a buy, bid for a sell) from the latest
    /// quote; `None` for priced orders or before any quote (the collar then skips).
    fn expected_price(&self, order: &Order) -> Option<f64> {
        if order.price().is_some() {
            return None;
        }
        let (bid, ask) = *self.last_quote.get(order.instrument_id())?;
        match order.side() {
            OrderSide::Buy => Some(ask),
            OrderSide::Sell => Some(bid),
            _ => None,
        }
    }

    /// Runs the stage (or, without one, the state rules) over `order`.
    fn risk_check(&mut self, order: &Order) -> Option<honba_risk::RiskRefusal> {
        let instrument = order.instrument_id();
        let request = RiskRequest {
            order_id: order.order_id().clone(),
            instrument_id: instrument.clone(),
            side: order.side(),
            quantity: order.quantity(),
            price: order.price(),
            trigger_price: order.trigger_price(),
            reference_price: self.last_px.get(instrument).copied(),
            adv: None,
            position: self.position(instrument) + self.working_exposure(instrument, order.side()),
            trading_state: self.trading_state,
            ts: self.clock.now(),
            last_feed_ts: self.last_feed_ts.get(instrument).copied(),
            expected_price: self.expected_price(order),
        };
        match self.risk.as_mut() {
            Some(stage) => match stage.check(&request) {
                RiskDecision::Approved => None,
                RiskDecision::Refused(refusal) => Some(refusal),
            },
            None => check_state(&request),
        }
    }

    fn submit(&mut self, order: Order) -> Result<()> {
        let order_id = order.order_id().as_str().to_string();
        // Whatever the sink produced earlier is ahead of this order.
        self.acknowledge_events()?;
        if let Some(existing) = self.orders.get(&order_id) {
            // An id is submitted once per run; reusing it is a submitter bug.
            let error = honba_messages::IllegalTransition::Transition {
                status: existing.state.status,
                cancel_requested: existing.state.cancel_requested,
                event: OrderEventKind::Submitted,
            };
            self.audit
                .record(AuditKind::IllegalTransition { order_id, error });
            return Ok(());
        }
        if self.execution.is_none() {
            self.refuse(&order, ErrorCode::OrderExecutionUnavailable);
            return Ok(());
        }
        if let Some(refusal) = self.risk_check(&order) {
            self.audit.record(AuditKind::RiskRefused {
                order_id,
                refusal: refusal.clone(),
            });
            self.refuse(&order, refusal.error_code());
            return Ok(());
        }
        let order_id = order.order_id().as_str().to_string();
        let instrument = order.instrument_id().to_string();
        let side = side_label(order.side());
        let submitted = self.submitted_message(&order);
        let execution = self
            .execution
            .as_deref_mut()
            .expect("checked above: an execution is attached");
        execution.submit(order.clone())?;
        self.audit.record(AuditKind::OrderSubmitted {
            order_id: order_id.clone(),
            instrument,
            side,
        });
        let tracked = TrackedOrder {
            state: OrderState::new(),
            instrument_id: order.instrument_id().clone(),
            side: order.side(),
            venue_order_id: None,
        };
        self.cache.seed_order(order_id.clone(), tracked.clone());
        self.orders.insert(order_id, tracked);
        // Enqueued ahead of anything the sink can drain for this order.
        self.queue.push_event(submitted, self.clock.now());
        self.translate(ExecutionEvent::Submitted {
            order_id: order.order_id().clone(),
            instrument_id: order.instrument_id().clone(),
            side: order.side(),
            quantity: order.quantity(),
            ts: self.clock.now(),
        });
        self.acknowledge_events()
    }

    /// `Event::Order` for `order` as submitted, never earlier than the clock
    /// (a message earlier than the clock could not be dispatched).
    fn submitted_message(&self, order: &Order) -> Event {
        let now = self.clock.now();
        let order = if order.ts_event() < now {
            let mut o = Order::new(
                order.order_id().clone(),
                order.instrument_id().clone(),
                order.side(),
                order.order_type(),
                order.quantity(),
                order.price(),
                order.time_in_force(),
                now,
                order.ts_init(),
            );
            if let Some(trigger) = order.trigger_price() {
                o = o.with_trigger_price(trigger);
            }
            o
        } else {
            order.clone()
        };
        Event::Order(order.with_status(OrderStatus::Submitted))
    }

    /// Requests a cancel (ADR 0019 decision 6): a no-op that emits nothing for
    /// an unknown, un-submitted or terminal order, or one whose cancel is
    /// already pending; otherwise `CancelRequested` once, then the sink's
    /// cancel at the engine time.
    fn cancel(&mut self, id: OrderId) -> Result<()> {
        // A fill the sink already produced beats this cancel.
        self.acknowledge_events()?;
        let order_id = id.as_str().to_string();
        let pending = match self.orders.get(&order_id) {
            Some(o) => !o.is_working() || o.state.cancel_requested,
            None => true,
        };
        if pending {
            return Ok(());
        }
        let now = self.clock.now();
        self.audit.record(AuditKind::CancelRequested {
            order_id: order_id.clone(),
        });
        self.translate(ExecutionEvent::CancelRequested {
            order_id: id,
            ts: now,
        });
        if let Some(execution) = self.execution.as_deref_mut() {
            execution.cancel(&order_id, now)?;
        }
        self.acknowledge_events()
    }

    fn apply_output(&mut self, output: EngineOutput) -> Result<()> {
        match output {
            EngineOutput::None => {}
            EngineOutput::Orders(orders) => {
                for order in orders {
                    self.submit(order)?;
                }
            }
            EngineOutput::Cancels(ids) => {
                for id in ids {
                    self.cancel(id)?;
                }
            }
            EngineOutput::StateChange(next) => {
                self.set_trading_state(next);
            }
        }
        Ok(())
    }

    /// Runs the engine to completion against the given feed.
    ///
    /// Each iteration pulls up to `batch_size` events from the feed into the
    /// queue, then dispatches the earliest one through [`Engine::pump`]: the
    /// clock advances to the message's `ts_event`, the dispatch is audited, and
    /// each handler in registration order is asked for an [`EngineOutput`]
    /// whose commands are applied before the next handler runs.
    ///
    /// After every handler the engine drains the execution sink and turns each
    /// event into a lifecycle message (`order_partially_filled`,
    /// `order_filled`, `order_cancelled`, ...) stamped with the kernel clock,
    /// so the command/ack loop is closed in one queue and a replay stays
    /// deterministic. That loop needs no iteration cap: `drain_events` takes
    /// the sink's buffer, so a handler that submits nothing produces no events
    /// and therefore no new messages.
    ///
    /// If the feed produces events earlier than ones already dispatched, the
    /// clock returns [`AlgoError::ClockRegression`]
    /// and the run aborts.
    ///
    /// This is [`Engine::start`], the pull/dispatch loop, then
    /// [`Engine::finish`], and nothing else: the three phases are the same
    /// public steps a driver that owns its own loop takes, so a run driven by
    /// `run` and a run driven by `start`/`pump`/`finish` dispatch identically.
    pub fn run(&mut self, feed: &mut dyn DataFeed) -> Result<()> {
        self.start()?;

        loop {
            for _ in 0..self.batch_size {
                match feed.next()? {
                    Some(msg) => self.queue.push(msg),
                    None => break,
                }
            }

            if !self.pump()? {
                break;
            }
        }

        self.finish()
    }
}

fn venue_order_id(ev: &ExecutionEvent) -> Option<&VenueOrderId> {
    match ev {
        ExecutionEvent::Accepted { venue_order_id, .. }
        | ExecutionEvent::Rejected { venue_order_id, .. }
        | ExecutionEvent::Fill { venue_order_id, .. }
        | ExecutionEvent::Cancelled { venue_order_id, .. }
        | ExecutionEvent::Expired { venue_order_id, .. } => venue_order_id.as_ref(),
        _ => None,
    }
}

fn side_label(side: OrderSide) -> String {
    match side {
        OrderSide::Buy => "buy".to_string(),
        OrderSide::Sell => "sell".to_string(),
        OrderSide::NoOrderSide => "no_order_side".to_string(),
        _ => "other".to_string(),
    }
}
