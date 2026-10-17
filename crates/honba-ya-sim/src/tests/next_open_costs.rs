//! Unit tests for the fill-cost hook of `crate::next_open` (ADR 0016, chunk 2).

use honba_engine::{AlgoError, ExecutionEngine};
use honba_entities::{Currency, Money};
use honba_messages::{Event, OrderSide};

use super::{any_instrument, bar_event, market};
use crate::{FillCostFn, NextOpenSim};

fn inr(rupees: i64) -> Money {
    Money::new(rupees * 100, Currency::Inr)
}

fn minor(m: i64) -> Money {
    Money::new(m, Currency::Inr)
}

fn feed(sim: &mut NextOpenSim, open: f64, ts: u64) {
    let Event::Bar(b) = bar_event(open, ts) else {
        unreachable!()
    };
    sim.on_bar(&b).unwrap();
}

fn flat(cost: i64) -> FillCostFn {
    Box::new(move |_, _, _| Ok(minor(cost)))
}

/// Cost of 1% of the notional (minor units, half away from zero).
fn one_percent() -> FillCostFn {
    Box::new(|_, qty, price| {
        let notional = Money::mul_qty(qty, price, Currency::Inr)
            .map_err(|e| AlgoError::Component(e.to_string()))?;
        Ok(minor((notional.minor() + 50) / 100))
    })
}

#[test]
fn the_default_cost_is_zero() {
    let mut s = NextOpenSim::new(inr(10_000)).unwrap();
    feed(&mut s, 100.0, 1);
    s.submit(market("B", OrderSide::Buy, 1.0, 1)).unwrap();
    feed(&mut s, 100.0, 2);
    assert_eq!(s.fees(), inr(0));
    assert_eq!(s.drain_fills().unwrap()[0].costs(), inr(0));
}

#[test]
fn a_buy_pays_notional_plus_cost_and_fees_accumulate() {
    let mut s = NextOpenSim::new(inr(10_000)).unwrap().with_costs(flat(250));
    feed(&mut s, 100.0, 1);
    s.submit(market("B", OrderSide::Buy, 10.0, 1)).unwrap();
    feed(&mut s, 100.0, 2);
    s.submit(market("S", OrderSide::Sell, 10.0, 2)).unwrap();
    assert_eq!(s.cash(), minor(1_000_000 - 100_000 - 250));
    feed(&mut s, 100.0, 3);
    // the sell credits notional less cost
    assert_eq!(s.cash(), minor(1_000_000 - 100_000 - 250 + 100_000 - 250));
    assert_eq!(s.fees(), minor(500));
    let fills = s.drain_fills().unwrap();
    assert_eq!(fills.len(), 2);
    assert!(fills.iter().all(|f| f.costs() == minor(250)));
}

#[test]
fn sale_proceeds_pending_are_net_of_cost() {
    let mut s = NextOpenSim::new(inr(10_000))
        .unwrap()
        .with_settlement_days(1)
        .unwrap()
        .with_costs(flat(250));
    feed(&mut s, 100.0, 1);
    s.submit(market("B", OrderSide::Buy, 10.0, 1)).unwrap();
    feed(&mut s, 100.0, 2);
    s.submit(market("S", OrderSide::Sell, 10.0, 2)).unwrap();
    feed(&mut s, 100.0, 3);
    assert_eq!(s.unsettled(), minor(100_000 - 250));
}

#[test]
fn funding_includes_the_cost_so_the_cut_is_cost_aware() {
    // 1000 cash, price 100, flat cost 10: 10 units cost 1010, 9 units cost 910.
    let mut s = NextOpenSim::new(inr(1_000))
        .unwrap()
        .with_costs(flat(1_000));
    feed(&mut s, 100.0, 1);
    s.submit(market("B", OrderSide::Buy, 10.0, 1)).unwrap();
    feed(&mut s, 100.0, 2);
    assert_eq!(s.position(&any_instrument()), 9.0);
    assert_eq!(s.cash(), minor(100_000 - 90_000 - 1_000));
    let rej = s.drain_rejections().unwrap();
    assert_eq!(
        (rej[0].reason.as_str(), rej[0].quantity),
        ("insufficient_funds", 1.0)
    );
}

#[test]
fn a_quantity_dependent_cost_shrinks_the_cut() {
    // 1% cost: n units cost 100n * 1.01; with 1000 cash 9 units fit (909), 10 do not (1010).
    let mut s = NextOpenSim::new(inr(1_000))
        .unwrap()
        .with_costs(one_percent());
    feed(&mut s, 100.0, 1);
    s.submit(market("B", OrderSide::Buy, 20.0, 1)).unwrap();
    feed(&mut s, 100.0, 2);
    assert_eq!(s.position(&any_instrument()), 9.0);
    assert_eq!(s.fees(), minor(900));
}

#[test]
fn a_cost_alone_beyond_cash_rejects_the_whole_buy() {
    let mut s = NextOpenSim::new(inr(100))
        .unwrap()
        .with_costs(flat(1_000_000));
    feed(&mut s, 10.0, 1);
    s.submit(market("B", OrderSide::Buy, 1.0, 1)).unwrap();
    feed(&mut s, 10.0, 2);
    assert!(s.drain_fills().unwrap().is_empty());
    assert_eq!(s.drain_rejections().unwrap().len(), 1);
    assert_eq!(s.cash(), inr(100));
}

#[test]
fn a_negative_buy_cost_errors_and_leaves_the_order_working() {
    let mut s = NextOpenSim::new(inr(10_000)).unwrap().with_costs(flat(-1));
    feed(&mut s, 100.0, 1);
    s.submit(market("B", OrderSide::Buy, 1.0, 1)).unwrap();
    let Event::Bar(b) = bar_event(100.0, 2) else {
        unreachable!()
    };
    assert!(s.on_bar(&b).is_err());
    assert_eq!(s.working_orders(), vec!["B".to_string()]);
    assert_eq!((s.cash(), s.fees()), (inr(10_000), inr(0)));
    assert!(s.drain_fills().unwrap().is_empty());
    assert!(s.drain_rejections().unwrap().is_empty());
}

#[test]
fn a_negative_sell_cost_errors_and_leaves_the_order_and_position_alone() {
    let mut s = NextOpenSim::new(inr(10_000))
        .unwrap()
        .with_costs(Box::new(|side, _, _| {
            Ok(minor(if side == OrderSide::Sell { -5 } else { 0 }))
        }));
    feed(&mut s, 100.0, 1);
    s.submit(market("B", OrderSide::Buy, 2.0, 1)).unwrap();
    feed(&mut s, 100.0, 2);
    s.submit(market("S", OrderSide::Sell, 2.0, 2)).unwrap();
    let Event::Bar(b) = bar_event(100.0, 3) else {
        unreachable!()
    };
    assert!(s.on_bar(&b).is_err());
    assert_eq!(s.working_orders(), vec!["S".to_string()]);
    assert_eq!(s.position(&any_instrument()), 2.0);
    assert_eq!(s.cash(), inr(10_000 - 200));
}

#[test]
fn a_cost_in_another_currency_is_an_error_but_a_zero_one_is_neutral() {
    let mut s = NextOpenSim::new(inr(10_000))
        .unwrap()
        .with_costs(Box::new(|_, _, _| Ok(Money::new(0, Currency::Usd))));
    feed(&mut s, 100.0, 1);
    s.submit(market("B", OrderSide::Buy, 1.0, 1)).unwrap();
    feed(&mut s, 100.0, 2);
    assert_eq!(s.fees(), inr(0));

    let mut t = NextOpenSim::new(inr(10_000))
        .unwrap()
        .with_costs(Box::new(|_, _, _| Ok(Money::new(5, Currency::Usd))));
    feed(&mut t, 100.0, 1);
    t.submit(market("B", OrderSide::Buy, 1.0, 1)).unwrap();
    let Event::Bar(b) = bar_event(100.0, 2) else {
        unreachable!()
    };
    assert!(t.on_bar(&b).is_err());
    assert_eq!(t.working_orders().len(), 1);
}

#[test]
fn a_failing_cost_function_propagates_its_error() {
    let mut s = NextOpenSim::new(inr(10_000))
        .unwrap()
        .with_costs(Box::new(|_, _, _| Err(AlgoError::Component("boom".into()))));
    feed(&mut s, 100.0, 1);
    s.submit(market("B", OrderSide::Buy, 1.0, 1)).unwrap();
    let Event::Bar(b) = bar_event(100.0, 2) else {
        unreachable!()
    };
    assert!(s.on_bar(&b).is_err());
    assert_eq!(s.working_orders().len(), 1);
}

#[test]
fn with_schedule_rounds_each_leg_once_and_charges_by_side() {
    use std::sync::Arc;

    use honba_market::MarketSegment;

    use super::TwoLegSchedule;

    let mut s = NextOpenSim::new(inr(10_000))
        .unwrap()
        .with_schedule(Arc::new(TwoLegSchedule), MarketSegment::from("equity"));
    s.set_position(&any_instrument(), 1.0).unwrap();
    feed(&mut s, 100.0, 1);
    s.submit(market("B", OrderSide::Buy, 1.0, 1)).unwrap();
    s.submit(market("S", OrderSide::Sell, 1.0, 1)).unwrap();
    feed(&mut s, 100.0, 2);
    let fills = s.drain_fills().unwrap();
    let cost_of = |side: OrderSide| fills.iter().find(|f| f.side() == side).unwrap().costs();
    // round(0.006) + round(0.006) = 1 + 1 = 2 minor units, not round(0.012) = 1.
    assert_eq!(cost_of(OrderSide::Buy), minor(2));
    // round(0.016) + round(0.006) = 2 + 1 = 3.
    assert_eq!(cost_of(OrderSide::Sell), minor(3));
    assert_eq!(s.fees(), minor(5));
}

#[test]
fn schedule_fill_cost_exposes_the_breakdown_and_the_rounded_total() {
    use std::sync::Arc;

    use crate::cost::ScheduleFillCost;
    use honba_market::MarketSegment;

    use super::TwoLegSchedule;

    let cost = ScheduleFillCost::new(
        Arc::new(TwoLegSchedule),
        MarketSegment::from("equity"),
        Currency::Inr,
    );
    assert_eq!(cost.breakdown(OrderSide::Buy, 1.0, 100.0).charges.len(), 2);
    assert_eq!(cost.total(OrderSide::Buy, 1.0, 100.0).unwrap(), minor(2));
    assert_eq!(cost.total(OrderSide::Sell, 1.0, 100.0).unwrap(), minor(3));
    assert_eq!(cost.currency(), Currency::Inr);
    assert_eq!(cost.segment().as_str(), "equity");
}
