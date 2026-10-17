//! Unit tests for `crate::bar_fill`.

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::Trade;
use honba_messages::{OrderSide, OrderType, UnixNanos};

use super::{any_instrument, bar_event, limit, market, order};
use crate::BarFillEngine;

fn observe(exec: &mut BarFillEngine, close: f64, ts: u64) {
    exec.on_event(&bar_event(close, ts), UnixNanos::from_u64(ts))
        .unwrap();
}

#[test]
fn new_engine_has_no_price_and_no_fills() {
    let mut exec = BarFillEngine::new();
    assert_eq!(exec.last_price(), None);
    assert!(exec.drain_fills().unwrap().is_empty());
}

#[test]
fn tracks_the_latest_bar_close() {
    let mut exec = BarFillEngine::new();
    observe(&mut exec, 100.0, 1);
    observe(&mut exec, 101.5, 2);
    assert_eq!(exec.last_price(), Some(101.5));
}

#[test]
fn market_buy_fills_full_quantity_at_last_close() {
    let mut exec = BarFillEngine::new();
    observe(&mut exec, 101.0, 5);
    exec.submit(market("O-1", OrderSide::Buy, 3.0, 5)).unwrap();

    let fills = exec.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    let f = &fills[0];
    assert_eq!(f.order_id().as_str(), "O-1");
    assert_eq!(f.instrument_id(), &any_instrument());
    assert_eq!(f.side(), OrderSide::Buy);
    assert_eq!(f.quantity(), 3.0);
    assert_eq!(f.price(), 101.0);
    assert_eq!(f.ts_event(), UnixNanos::from_u64(5));
}

#[test]
fn market_sell_keeps_its_side() {
    let mut exec = BarFillEngine::new();
    observe(&mut exec, 50.0, 1);
    exec.submit(market("O-1", OrderSide::Sell, 2.0, 1)).unwrap();
    let fills = exec.drain_fills().unwrap();
    assert_eq!(fills[0].side(), OrderSide::Sell);
    assert_eq!(fills[0].notional(), 100.0);
}

#[test]
fn marketable_limit_buy_fills_at_last_close() {
    let mut exec = BarFillEngine::new();
    observe(&mut exec, 101.0, 1);
    exec.submit(limit("O-1", OrderSide::Buy, 1.0, 105.0, 1))
        .unwrap();
    let fills = exec.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].price(), 101.0);
}

#[test]
#[ignore = "known gap: ADR 006 (BarFillEngine ignores limit prices)"]
fn non_marketable_limit_buy_is_not_filled() {
    let mut exec = BarFillEngine::new();
    observe(&mut exec, 101.0, 1);
    exec.submit(limit("O-1", OrderSide::Buy, 1.0, 95.0, 1))
        .unwrap();
    assert!(exec.drain_fills().unwrap().is_empty());
}

#[test]
#[ignore = "known gap: ADR 006 (BarFillEngine ignores stop trigger prices)"]
fn stop_buy_is_not_filled_before_its_trigger() {
    let mut exec = BarFillEngine::new();
    observe(&mut exec, 101.0, 1);
    let stop =
        order("O-1", OrderSide::Buy, OrderType::StopMarket, 1.0, None, 1).with_trigger_price(110.0);
    exec.submit(stop).unwrap();
    assert!(exec.drain_fills().unwrap().is_empty());
}

#[test]
#[ignore = "known gap: BarFillEngine has no reject path; before any bar it fills at price 0.0"]
fn order_before_any_bar_is_not_filled_at_zero() {
    let mut exec = BarFillEngine::new();
    exec.submit(market("O-1", OrderSide::Buy, 1.0, 1)).unwrap();
    assert!(exec.drain_fills().unwrap().iter().all(|f| f.price() > 0.0));
}

#[test]
fn fill_timestamps_are_strictly_increasing_and_never_before_the_order() {
    let mut exec = BarFillEngine::new();
    observe(&mut exec, 10.0, 1);
    exec.submit(market("O-1", OrderSide::Buy, 1.0, 7)).unwrap();
    exec.submit(market("O-2", OrderSide::Sell, 1.0, 7)).unwrap();
    exec.submit(market("O-3", OrderSide::Buy, 1.0, 3)).unwrap();
    let ts: Vec<u64> = exec
        .drain_fills()
        .unwrap()
        .iter()
        .map(|f| f.ts_event().as_u64())
        .collect();
    assert_eq!(ts, [7, 8, 9]);
}

#[test]
fn drain_empties_the_fill_buffer() {
    let mut exec = BarFillEngine::new();
    observe(&mut exec, 10.0, 1);
    exec.submit(market("O-1", OrderSide::Buy, 1.0, 1)).unwrap();
    assert_eq!(exec.drain_fills().unwrap().len(), 1);
    assert!(exec.drain_fills().unwrap().is_empty());
}

#[test]
fn a_schedule_cost_model_charges_by_side_per_fill() {
    use std::sync::Arc;

    use honba_market::MarketSegment;

    use super::TwoLegSchedule;

    let mut exec =
        BarFillEngine::with_schedule(Arc::new(TwoLegSchedule), MarketSegment::from("equity"));
    observe(&mut exec, 100.0, 1);
    exec.submit(market("B", OrderSide::Buy, 1.0, 1)).unwrap();
    exec.submit(market("S", OrderSide::Sell, 1.0, 2)).unwrap();
    let fills = exec.drain_fills().unwrap();
    // buy: 0.006 + 0.006 rounds per leg to 2 minor units; sell: 0.016 + 0.006 to 3.
    assert_eq!(fills[0].costs().minor(), 2);
    assert_eq!(fills[1].costs().minor(), 3);
}

#[test]
fn clones_share_prices_and_fills() {
    let mut observer = BarFillEngine::new();
    let mut executor = observer.clone();
    observe(&mut observer, 42.0, 1);
    assert_eq!(executor.last_price(), Some(42.0));
    executor
        .submit(market("O-1", OrderSide::Buy, 1.0, 1))
        .unwrap();
    assert_eq!(observer.drain_fills().unwrap().len(), 1);
    assert!(executor.drain_fills().unwrap().is_empty());
}

#[test]
fn cancel_is_a_no_op() {
    let mut exec = BarFillEngine::new();
    observe(&mut exec, 10.0, 1);
    exec.submit(market("O-1", OrderSide::Buy, 1.0, 1)).unwrap();
    exec.cancel("O-1", UnixNanos::from_u64(1)).unwrap();
    assert_eq!(exec.drain_fills().unwrap().len(), 1);
}

#[test]
fn identical_inputs_give_identical_fills() {
    fn run() -> Vec<Trade> {
        let mut exec = BarFillEngine::new();
        for (i, close) in [100.0, 101.0, 99.5, 102.0].into_iter().enumerate() {
            let ts = i as u64 + 1;
            observe(&mut exec, close, ts);
            let side = if i % 2 == 0 {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            exec.submit(market(&format!("O-{ts}"), side, 1.0, ts))
                .unwrap();
        }
        exec.drain_fills().unwrap()
    }
    let a = run();
    assert_eq!(a.len(), 4);
    assert_eq!(a, run());
}

// ---- fill costs (ADR 008): cost = flat + quantity * price * bps / 10_000 ----

use crate::bar_fill::{MAX_COST_BPS, MAX_FLAT_COST};
use crate::{FillCosts, FillCostsError};

fn costed(flat: f64, bps: f64) -> BarFillEngine {
    BarFillEngine::with_costs(FillCosts::new(flat, bps).unwrap())
}

#[test]
fn default_engine_charges_no_costs() {
    let mut exec = BarFillEngine::new();
    observe(&mut exec, 101.0, 1);
    exec.submit(market("O-1", OrderSide::Buy, 3.0, 1)).unwrap();
    assert_eq!(exec.drain_fills().unwrap()[0].costs().minor(), 0);
    assert_eq!(FillCosts::default(), FillCosts::new(0.0, 0.0).unwrap());
}

#[test]
fn flat_and_proportional_costs_add_up_per_fill() {
    // notional 10 * 100 = 1000; 10 bps of it is 1.0; plus the flat 20.
    let mut exec = costed(20.0, 10.0);
    observe(&mut exec, 100.0, 1);
    exec.submit(market("O-1", OrderSide::Buy, 10.0, 1)).unwrap();
    exec.submit(market("O-2", OrderSide::Sell, 10.0, 1))
        .unwrap();
    let fills = exec.drain_fills().unwrap();
    // Costs are an amount, never signed by side: the ledger applies the sign.
    // 20.00 flat + 10 bps of 1,000.00 = 1.00, each leg rounded once: 2100 minor units.
    assert_eq!(fills[0].costs().minor(), 2100);
    assert_eq!(fills[1].costs().minor(), 2100);
    assert_eq!(fills[0].price(), 100.0);
}

#[test]
fn flat_only_and_bps_only_costs() {
    let mut flat = costed(2.5, 0.0);
    observe(&mut flat, 100.0, 1);
    flat.submit(market("O-1", OrderSide::Buy, 3.0, 1)).unwrap();
    assert_eq!(flat.drain_fills().unwrap()[0].costs().minor(), 250);

    let mut bps = costed(0.0, 625.0);
    observe(&mut bps, 64.0, 1);
    bps.submit(market("O-1", OrderSide::Sell, 1.0, 1)).unwrap();
    // 625 bps of 64.00 = 4.00: 400 minor units.
    assert_eq!(bps.drain_fills().unwrap()[0].costs().minor(), 400);
}

#[test]
fn fill_costs_reject_invalid_values_with_typed_errors() {
    assert_eq!(
        FillCosts::new(-0.01, 0.0),
        Err(FillCostsError::InvalidFlat(-0.01))
    );
    assert!(matches!(
        FillCosts::new(f64::NAN, 0.0),
        Err(FillCostsError::InvalidFlat(_))
    ));
    assert!(matches!(
        FillCosts::new(f64::INFINITY, 0.0),
        Err(FillCostsError::InvalidFlat(_))
    ));
    assert!(matches!(
        FillCosts::new(MAX_FLAT_COST + 1.0, 0.0),
        Err(FillCostsError::InvalidFlat(_))
    ));
    assert_eq!(
        FillCosts::new(0.0, -1.0),
        Err(FillCostsError::InvalidBps(-1.0))
    );
    assert!(matches!(
        FillCosts::new(0.0, f64::NAN),
        Err(FillCostsError::InvalidBps(_))
    ));
    assert!(matches!(
        FillCosts::new(0.0, MAX_COST_BPS + 1.0),
        Err(FillCostsError::InvalidBps(_))
    ));
    // The caps themselves are allowed.
    assert!(FillCosts::new(MAX_FLAT_COST, MAX_COST_BPS).is_ok());
    assert!(FillCostsError::InvalidBps(-1.0).to_string().contains("bps"));
}

#[test]
fn each_cost_leg_rounds_once_before_the_legs_are_summed() {
    // ADR 0011: flat 0.005 rounds to 1 minor unit and 1 bps of 50.00 (0.005) rounds
    // to 1 minor unit: 2 minor units. Summing first (0.01) and rounding once would charge
    // 1 minor unit, a total the broker's per-leg ledger cannot reproduce.
    let mut exec = costed(0.005, 1.0);
    observe(&mut exec, 50.0, 1);
    exec.submit(market("O-1", OrderSide::Buy, 1.0, 1)).unwrap();
    assert_eq!(exec.drain_fills().unwrap()[0].costs().minor(), 2);
}

#[test]
fn a_cost_that_cannot_be_represented_fails_the_fill_instead_of_charging_zero() {
    // A notional so large its bps leg overflows i64 minor units must not settle as a
    // free fill: zero costs would round in the reporter's favour.
    let mut exec = costed(0.0, MAX_COST_BPS);
    observe(&mut exec, 1e10, 1);
    let err = exec
        .submit(market("O-1", OrderSide::Buy, 1e10, 1))
        .unwrap_err();
    assert!(err.to_string().contains("cost"), "{err}");
    assert!(exec.drain_fills().unwrap().is_empty());
}

#[test]
fn trailing_stop_sell_rests_and_triggers_on_breach() {
    use honba_messages::{Bar, BarAggregation, BarSpecification, BarType, Event, PriceType};

    let mut exec = BarFillEngine::new();
    observe(&mut exec, 100.0, 1);

    // Trailing stop sell with trail amount 5.0
    let ts_order = order(
        "TS-1",
        OrderSide::Sell,
        OrderType::TrailingStop,
        10.0,
        None,
        1,
    )
    .with_trail_amount(5.0);
    exec.submit(ts_order).unwrap();
    // Initially rests, no fill yet
    assert!(exec.drain_fills().unwrap().is_empty());

    let bt = BarType::new(
        any_instrument(),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );

    // Bar 2: high moves up to 110.0, low is 106.0. Peak becomes 110.0, trigger is 105.0.
    // Low 106.0 does not breach 105.0.
    let t2 = UnixNanos::from_u64(2);
    let bar2 = Bar::new(bt.clone(), 107.0, 110.0, 106.0, 108.0, 1.0, t2, t2);
    exec.on_event(&Event::Bar(bar2), t2).unwrap();
    assert!(exec.drain_fills().unwrap().is_empty());

    // Bar 3: drops to 104.0, breaching 105.0!
    let t3 = UnixNanos::from_u64(3);
    let bar3 = Bar::new(bt, 107.0, 107.0, 104.0, 105.0, 1.0, t3, t3);
    exec.on_event(&Event::Bar(bar3), t3).unwrap();
    let fills = exec.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].price(), 105.0);
    assert_eq!(fills[0].quantity(), 10.0);
    assert_eq!(fills[0].side(), OrderSide::Sell);
}
