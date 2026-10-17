//! Unit tests for this crate, one file per area.

mod bar_fill;
mod execution_events;
mod fill_model;
mod l2;
mod latency;
mod lot;
mod margin;
mod next_open;
mod next_open_costs;
mod next_open_session_open;
mod next_open_settlement;
mod paper;
mod sandbox;
mod scripted;
mod target;

use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId, Order, OrderId,
    OrderSide, OrderType, PriceType, TimeInForce, UnixNanos,
};

/// A test cost schedule with two legs whose amounts are chosen so that rounding each leg once
/// differs from rounding the sum: a leg of `0.006` major units is one minor unit.
///
/// The "main" leg is side-aware (buy `0.006`, sell `0.016`) and the "fixed" leg is always
/// `0.006`, so a buy costs `1 + 1 = 2` minor units and a sell `2 + 1 = 3`.
pub(crate) struct TwoLegSchedule;

impl honba_market::CostSchedule for TwoLegSchedule {
    fn compute_costs(
        &self,
        _segment: &honba_market::MarketSegment,
        side: OrderSide,
        _notional: f64,
    ) -> honba_market::FeeBreakdown {
        let main = if side == OrderSide::Buy { 0.006 } else { 0.016 };
        honba_market::FeeBreakdown::from_charges(vec![
            honba_market::Charge::new("main", main),
            honba_market::Charge::new("fixed", 0.006),
        ])
    }
}

/// A placeholder instrument for tests where the instrument does not matter.
fn any_instrument() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("TEST"))
}

/// A one-minute bar event for [`any_instrument`] closing at `close`, at `ts`.
fn bar_event(close: f64, ts: u64) -> Event {
    let bt = BarType::new(
        any_instrument(),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    let t = UnixNanos::from_u64(ts);
    Event::Bar(Bar::new(bt, close, close, close, close, 1.0, t, t))
}

/// A day order for [`any_instrument`] stamped at `ts`.
pub(crate) fn order(
    id: &str,
    side: OrderSide,
    order_type: OrderType,
    qty: f64,
    price: Option<f64>,
    ts: u64,
) -> Order {
    let t = UnixNanos::from_u64(ts);
    Order::new(
        OrderId::new(id),
        any_instrument(),
        side,
        order_type,
        qty,
        price,
        TimeInForce::Day,
        t,
        t,
    )
}

fn market(id: &str, side: OrderSide, qty: f64, ts: u64) -> Order {
    order(id, side, OrderType::Market, qty, None, ts)
}

fn limit(id: &str, side: OrderSide, qty: f64, price: f64, ts: u64) -> Order {
    order(id, side, OrderType::Limit, qty, Some(price), ts)
}

/// A day market order for `symbol` on the `TEST` exchange stamped at `ts`.
fn market_for(id: &str, symbol: &str, side: OrderSide, qty: f64, ts: u64) -> Order {
    let t = UnixNanos::from_u64(ts);
    Order::new(
        OrderId::new(id),
        InstrumentId::new(symbol, Exchange::new("TEST")),
        side,
        OrderType::Market,
        qty,
        None,
        TimeInForce::Day,
        t,
        t,
    )
}
