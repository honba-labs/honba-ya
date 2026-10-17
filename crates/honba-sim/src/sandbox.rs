//! Sandbox execution adapter: paper trading on a live-style market-data feed.
//!
//! The sandbox wraps the same matching core the backtester drives
//! ([`BarFillEngine`](crate::BarFillEngine) or [`NextOpenSim`](crate::NextOpenSim)) behind a
//! live-feed seam, so an order takes exactly the same route through the order FSM
//! ([`OrderState`] over [`ExecutionEvent`]s) whether it is backtested or paper-traded online.
//! Its ledger — [`SandboxAdapter::virtual_funds`] and
//! [`SandboxAdapter::virtual_positions`] — is held here, isolated from any real broker
//! adapter: nothing in this type touches another adapter's account.
//!
//! The feed seam is [`LiveFeed`], the synchronous stand-in for
//! `honba_ports::MarketDataConnector`. Wiring the async connector (and a real
//! `ExecutionGateway`) is deliberately out of scope: it needs the port-bridge ADR noted as D10
//! in the roadmap before the two sides are joined.

use std::collections::{HashMap, VecDeque};

use honba_engine::{AlgoError, ExecutionEngine, Handler, Result};
use honba_entities::{Currency, ExecutionEvent, Money, Trade};
use honba_messages::{
    Bar, Event, InstrumentId, Order, OrderEvent, OrderSide, OrderState, UnixNanos,
};

/// Quantities closer than this are flat (float residue from fractional fills).
const QTY_EPS: f64 = 1e-9;

/// A synchronous, pull-based market-data source.
///
/// The sandbox's seam to a live session: the synchronous stand-in for
/// `honba_ports::MarketDataConnector`. It yields the same L0 [`Event`]s the kernel orders, one
/// at a time; `None` means the stream is exhausted. A live implementation is driven by the async
/// connector behind a runtime, but the seam stays synchronous so the sandbox runs in the same
/// deterministic kernel as a backtest.
pub trait LiveFeed {
    /// The next event, or `None` when the feed is exhausted.
    fn next_event(&mut self) -> Option<Event>;
}

/// A [`LiveFeed`] that replays a fixed list of events, in order.
///
/// Offline and deterministic: the feed a test drives a sandbox with over a known market slice.
#[derive(Clone, Debug, Default)]
pub struct SimulatedFeed {
    events: VecDeque<Event>,
}

impl SimulatedFeed {
    /// Creates a feed that replays `events` from the front.
    pub fn new(events: Vec<Event>) -> Self {
        Self {
            events: events.into(),
        }
    }

    /// Creates a feed that replays `bars` as [`Event::Bar`]s, in order.
    pub fn from_bars(bars: Vec<Bar>) -> Self {
        Self::new(bars.into_iter().map(Event::Bar).collect())
    }

    /// Whether the feed has no events left.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

impl LiveFeed for SimulatedFeed {
    fn next_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }
}

/// Paper-trades orders on a live-style feed through the same matching core and order FSM a
/// backtest uses, holding a virtual ledger isolated from any real adapter.
///
/// Submit an order with [`SandboxAdapter::place_order`]; it is recorded as `Submitted` in an
/// [`OrderState`], routed to the wrapped core, and every event the core produces is applied to
/// that state and, for a fill, to the sandbox's own funds and positions. The wrapped core is the
/// same one a backtest drives, so the fills are identical (see the `sandbox_vs_backtest_same_fills`
/// integration test).
pub struct SandboxAdapter<C> {
    feed: Box<dyn LiveFeed>,
    core: C,
    currency: Currency,
    funds: Money,
    positions: Vec<(InstrumentId, f64)>,
    states: HashMap<String, OrderState>,
    events: Vec<ExecutionEvent>,
}

impl<C: ExecutionEngine> SandboxAdapter<C> {
    /// Creates a sandbox over `feed`, routing orders to `core`, starting with `starting_funds`.
    ///
    /// The virtual ledger's currency is the funds' currency; the core must settle fills in it.
    pub fn new(feed: impl LiveFeed + 'static, core: C, starting_funds: Money) -> Self {
        Self {
            feed: Box::new(feed),
            core,
            currency: starting_funds.currency(),
            funds: starting_funds,
            positions: Vec::new(),
            states: HashMap::new(),
            events: Vec::new(),
        }
    }

    /// Routes `order` to the core through the order FSM and returns its id.
    ///
    /// The order is recorded as `Submitted` in an [`OrderState`], submitted to the same matching
    /// core a backtest drives, and every event the core produces is applied to that state and,
    /// when it is a fill, to the sandbox's virtual ledger. Drains with [`Self::drain_events`].
    pub fn place_order(&mut self, order: Order) -> Result<String> {
        let id = order.order_id().as_str().to_string();
        let quantity = order.quantity();
        let mut state = OrderState::new();
        state
            .apply(&OrderEvent::Submitted { quantity })
            .map_err(|e| component(format!("sandbox submit {id}: {e}")))?;
        self.states.insert(id.clone(), state);
        self.core.submit(order)?;
        self.absorb()?;
        Ok(id)
    }

    /// Cancels a working order at `now`, through the core and the same FSM.
    pub fn cancel(&mut self, order_id: &str, now: UnixNanos) -> Result<()> {
        self.core.cancel(order_id, now)?;
        self.absorb()
    }

    /// Drains the execution events produced since the last call, in production order.
    pub fn drain_events(&mut self) -> Vec<ExecutionEvent> {
        std::mem::take(&mut self.events)
    }

    /// The isolated virtual funds.
    pub fn virtual_funds(&self) -> Money {
        self.funds
    }

    /// The isolated virtual positions, net signed, in the order first opened.
    pub fn virtual_positions(&self) -> &[(InstrumentId, f64)] {
        &self.positions
    }

    /// The FSM state of an order the sandbox has seen.
    pub fn order_state(&self, order_id: &str) -> Option<&OrderState> {
        self.states.get(order_id)
    }

    /// The currency the virtual ledger settles in.
    pub fn currency(&self) -> Currency {
        self.currency
    }

    /// The wrapped matching core, for inspecting whatever it tracks beyond the ledger.
    pub fn core(&self) -> &C {
        &self.core
    }

    /// Moves the core's new events through the FSM and the virtual ledger.
    fn absorb(&mut self) -> Result<()> {
        for event in self.core.drain_events()? {
            let id = event.order_id().as_str().to_string();
            if let Some(state) = self.states.get_mut(&id) {
                state
                    .apply(&event.order_event())
                    .map_err(|e| component(format!("sandbox fsm {id}: {e}")))?;
            }
            if let ExecutionEvent::Fill { trade, .. } = &event {
                self.book(trade)?;
            }
            self.events.push(event);
        }
        Ok(())
    }

    /// Applies one fill to the virtual funds and positions.
    ///
    /// A buy debits `quantity * price + costs`; a sell credits `quantity * price - costs`, each
    /// notional rounded to minor units once ([`Money::mul_qty`]).
    fn book(&mut self, trade: &Trade) -> Result<()> {
        let notional = Money::mul_qty(trade.quantity(), trade.price(), self.currency)
            .map_err(|e| component(format!("sandbox notional: {e}")))?;
        let delta = match trade.side() {
            OrderSide::Buy => (notional + trade.costs())
                .map(|m| m.neg())
                .map_err(|e| component(format!("sandbox funds: {e}")))?,
            OrderSide::Sell => {
                (notional - trade.costs()).map_err(|e| component(format!("sandbox funds: {e}")))?
            }
            _ => return Err(component("sandbox fill has no side".into())),
        };
        self.funds = (self.funds + delta).map_err(|e| component(format!("sandbox funds: {e}")))?;
        let signed = if trade.side() == OrderSide::Buy {
            trade.quantity()
        } else {
            -trade.quantity()
        };
        apply_position(&mut self.positions, trade.instrument_id(), signed);
        Ok(())
    }
}

impl<C: ExecutionEngine + Handler> SandboxAdapter<C> {
    /// Advances the feed by one event, letting the core observe it.
    ///
    /// Returns `false` when the feed is exhausted. A bar reaches the resting-order logic at the
    /// same point it would in a backtest that feeds the same slice to the same core, and any fill
    /// it triggers moves through the FSM and the ledger.
    pub fn advance(&mut self) -> Result<bool> {
        let Some(event) = self.feed.next_event() else {
            return Ok(false);
        };
        let ts = event.ts_event();
        self.core.on_event(&event, ts)?;
        self.absorb()?;
        Ok(true)
    }
}

fn component(msg: String) -> AlgoError {
    AlgoError::Component(msg)
}

/// Applies a signed fill delta to a net-position list, dropping a flat entry.
fn apply_position(positions: &mut Vec<(InstrumentId, f64)>, instrument: &InstrumentId, delta: f64) {
    let held = positions
        .iter()
        .find(|(i, _)| i == instrument)
        .map_or(0.0, |(_, q)| *q)
        + delta;
    positions.retain(|(i, _)| i != instrument);
    if held.abs() > QTY_EPS {
        positions.push((instrument.clone(), held));
    }
}
