//! Unit tests for `crate::sandbox`: the virtual ledger is isolated from a real adapter.

use honba_entities::{Currency, Money};
use honba_messages::{InstrumentId, Order, OrderSide, OrderStatus};

use super::{any_instrument, bar_event, market};
use crate::{BarFillEngine, SandboxAdapter, SimulatedFeed};

/// A stand-in for a real broker adapter: its own ledger, and a venue that fills
/// an order at a different price with a fee. It exists only to show that the
/// sandbox's virtual ledger is separate from it.
struct RealLikeAdapter {
    funds: Money,
    positions: Vec<(InstrumentId, f64)>,
}

impl RealLikeAdapter {
    fn new(funds: Money) -> Self {
        Self {
            funds,
            positions: Vec::new(),
        }
    }

    fn route(&mut self, order: &Order, fill_price: f64, fee: f64) {
        let currency = self.funds.currency();
        let notional = Money::from_major_f64(order.quantity() * fill_price, currency).unwrap();
        let fee = Money::from_major_f64(fee, currency).unwrap();
        let delta = match order.side() {
            OrderSide::Buy => (notional + fee).unwrap().neg(),
            _ => (notional - fee).unwrap(),
        };
        self.funds = (self.funds + delta).unwrap();
        apply(
            &mut self.positions,
            order.instrument_id(),
            if order.side() == OrderSide::Buy {
                order.quantity()
            } else {
                -order.quantity()
            },
        );
    }
}

fn apply(positions: &mut Vec<(InstrumentId, f64)>, instrument: &InstrumentId, delta: f64) {
    let held = positions
        .iter()
        .find(|(i, _)| i == instrument)
        .map_or(0.0, |(_, q)| *q)
        + delta;
    positions.retain(|(i, _)| i != instrument);
    if held.abs() > 1e-9 {
        positions.push((instrument.clone(), held));
    }
}

#[test]
fn virtual_funds_isolated() {
    let start = Money::new(10_000_000, Currency::Inr); // 1,00,000.00

    // Sandbox: fill the buy at the observed bar close (50.0), no costs.
    let mut sandbox = SandboxAdapter::new(
        SimulatedFeed::new(vec![bar_event(50.0, 1)]),
        BarFillEngine::new(),
        start,
    );
    assert!(sandbox.advance().unwrap());
    let id = sandbox
        .place_order(market("o1", OrderSide::Buy, 100.0, 1))
        .unwrap();
    assert_eq!(id, "o1");

    // The real-like adapter sees the same order, at its own venue price plus a fee.
    let mut real = RealLikeAdapter::new(start);
    real.route(&market("o1", OrderSide::Buy, 100.0, 1), 51.0, 20.0);

    // Same order, two ledgers: the sandbox fills at the bar close, the real
    // adapter at its venue price plus a fee; the two must not agree.
    assert_eq!(
        sandbox.virtual_funds(),
        Money::new(9_500_000, Currency::Inr)
    );
    assert_eq!(real.funds, Money::new(9_488_000, Currency::Inr));
    assert_ne!(sandbox.virtual_funds(), real.funds);

    // Isolation: the sandbox's activity never touched the real adapter.
    assert_eq!(real.funds, Money::new(9_488_000, Currency::Inr));
    assert_eq!(real.positions, vec![(any_instrument(), 100.0)]);
    assert_eq!(
        sandbox.virtual_positions(),
        [(any_instrument(), 100.0)].as_slice()
    );
    assert_eq!(
        sandbox.order_state("o1").unwrap().status,
        OrderStatus::Filled
    );
}
