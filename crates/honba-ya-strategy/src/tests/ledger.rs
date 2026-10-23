//! `LedgerContext`: the reference `StrategyContext` (ADR 008). Mirrors
//! `python/tests/unit/test_ledger_context.py`.

use honba_core::chrono::{DateTime, Utc};
use honba_core::uuid::Uuid;
use honba_core::{Currency, Exchange, FillReport, InstrumentId, Money, OrderSide, Price, Quantity};
use honba_testing::fixtures::{exact_decimal, utc_nanos};
use rust_decimal::Decimal;

use crate::context::InstrumentInfo;
use crate::{LedgerContext, OrderIntent, StrategyContext};

fn id(symbol: &str, exchange: &str) -> InstrumentId {
    let ex = match exchange {
        "BSE" => Exchange::Bse,
        _ => Exchange::Nse,
    };
    InstrumentId::new(ex, symbol)
}

fn q(val: f64) -> Quantity {
    Quantity::new(exact_decimal(val))
}

fn p(val: f64) -> Price {
    Price::new(exact_decimal(val))
}

fn fill(
    instrument_id: &InstrumentId,
    side: OrderSide,
    qty: f64,
    px: f64,
    costs: f64,
) -> FillReport {
    FillReport::new(
        "F-1",
        "O-1",
        Uuid::nil(),
        instrument_id.clone(),
        side,
        p(px),
        q(qty),
        "test",
        utc_nanos(1),
    )
    .with_fee(Money::new(exact_decimal(costs), Currency::Inr), None)
}

#[test]
fn starts_empty() {
    let ctx = LedgerContext::new();
    let nifty = id("NIFTY50", "NSE");
    assert_eq!(ctx.now(), DateTime::<Utc>::MIN_UTC);
    assert_eq!(ctx.cash().amount, Decimal::ZERO);
    assert!(ctx.positions().is_empty());
    assert_eq!(ctx.position(&nifty), Decimal::ZERO);
    assert!(!ctx.busy(&nifty));
    assert!(ctx.instrument(&nifty).is_none());
}

#[test]
fn clock_is_set_by_the_runner() {
    let mut ctx = LedgerContext::new();
    let ts = utc_nanos(1_700_000_000_000_000_000);
    ctx.set_now(ts);
    assert_eq!(ctx.now(), ts);
}

#[test]
fn instrument_lookup() {
    let nifty = id("NIFTY50", "NSE");
    let meta = InstrumentInfo {
        id: nifty.clone(),
        lot_size: q(75.0),
        tick_size: p(0.05),
    };
    let mut ctx = LedgerContext::new();
    ctx.add_instrument(meta.clone());
    assert_eq!(ctx.instrument(&nifty), Some(&meta));
}

#[test]
fn submit_queues_in_order_and_marks_busy_until_filled() {
    let (nifty, infy) = (id("NIFTY50", "NSE"), id("INFY", "NSE"));
    let mut ctx = LedgerContext::new();
    let first = OrderIntent::market_buy(nifty.clone(), q(10.0));
    let second = OrderIntent::limit_sell(infy.clone(), q(2.0), p(1500.0));
    ctx.submit(first.clone());
    ctx.submit(second.clone());
    assert!(ctx.busy(&nifty) && ctx.busy(&infy));
    assert_eq!(ctx.drain_intents(), vec![first, second]);
    assert!(ctx.drain_intents().is_empty());
    assert!(ctx.busy(&nifty), "drained is not filled");
    ctx.apply_fill(&fill(&nifty, OrderSide::Buy, 4.0, 100.0, 0.0))
        .unwrap();
    assert!(ctx.busy(&nifty), "partial fill");
    ctx.apply_fill(&fill(&nifty, OrderSide::Buy, 6.0, 100.0, 0.0))
        .unwrap();
    assert!(!ctx.busy(&nifty));
}

#[test]
fn release_clears_pending_for_a_rejected_intent() {
    let nifty = id("NIFTY50", "NSE");
    let mut ctx = LedgerContext::new();
    let intent = OrderIntent::market_sell(nifty.clone(), q(5.0));
    ctx.submit(intent.clone());
    ctx.release(&intent);
    assert!(!ctx.busy(&nifty));
}

#[test]
fn release_remainder_frees_only_the_unfilled_part() {
    let nifty = id("NIFTY50", "NSE");
    let mut ctx = LedgerContext::new();
    ctx.submit(OrderIntent::market_buy(nifty.clone(), q(10.0)));
    ctx.release_remainder(&nifty, OrderSide::Buy, Decimal::from(4));
    assert!(ctx.busy(&nifty), "6 still pending");
    ctx.release_remainder(&nifty, OrderSide::Buy, Decimal::from(6));
    assert!(!ctx.busy(&nifty));
    ctx.release_remainder(&nifty, OrderSide::Buy, Decimal::from(-1));
}

#[test]
fn fills_move_position_and_cash_including_costs() {
    let nifty = id("NIFTY50", "NSE");
    let mut ctx = LedgerContext::with_cash(Money::new(Decimal::from(1_000), Currency::Inr));
    ctx.apply_fill(&fill(&nifty, OrderSide::Buy, 3.0, 100.0, 1.5))
        .unwrap();
    assert_eq!(ctx.position(&nifty), Decimal::from(3));
    assert_eq!(ctx.cash().amount, exact_decimal(1_000.0 - 300.0 - 1.5));
    ctx.apply_fill(&fill(&nifty, OrderSide::Sell, 5.0, 110.0, 2.0))
        .unwrap();
    assert_eq!(ctx.position(&nifty), Decimal::from(-2), "signed: short 2");
    assert_eq!(ctx.cash().amount, exact_decimal(698.5 + 550.0 - 2.0));
}

#[test]
fn positions_lists_non_flat_ordered_by_instrument_id() {
    let (nifty, infy) = (id("NIFTY50", "NSE"), id("INFY", "NSE"));
    let (acme_bse, acme_nse) = (id("ACME", "BSE"), id("ACME", "NSE"));
    let mut ctx = LedgerContext::new();
    for i in [&nifty, &acme_nse, &infy, &acme_bse] {
        ctx.apply_fill(&fill(i, OrderSide::Buy, 1.0, 10.0, 0.0))
            .unwrap();
    }
    ctx.apply_fill(&fill(&infy, OrderSide::Sell, 1.0, 10.0, 0.0))
        .unwrap();
    let mut expected = vec![
        (acme_bse, Decimal::ONE),
        (acme_nse, Decimal::ONE),
        (nifty, Decimal::ONE),
    ];
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(ctx.positions(), expected);
}

#[test]
fn invalid_intent_does_not_touch_pending_state() {
    let mut ctx = LedgerContext::new();
    let nifty = id("NIFTY50", "NSE");
    ctx.submit(OrderIntent::market_buy(nifty.clone(), q(10.0)));
    for bad in [Decimal::ZERO, Decimal::from(-1)] {
        let intent = OrderIntent::market_buy(nifty.clone(), Quantity::new(bad));
        ctx.submit(intent.clone());
        assert!(ctx.busy(&nifty), "pending wiped by quantity {bad}");
        ctx.release(&intent);
        assert!(ctx.busy(&nifty), "pending wiped by release of {bad}");
    }
    ctx.apply_fill(&fill(&nifty, OrderSide::Buy, 10.0, 1.0, 0.0))
        .unwrap();
    assert!(!ctx.busy(&nifty));
    assert_eq!(ctx.drain_intents().len(), 3);
}

#[test]
fn a_fill_whose_costs_are_in_another_currency_is_refused_whole() {
    let nifty = id("NIFTY50", "NSE");
    let mut ctx = LedgerContext::with_cash(Money::new(Decimal::from(100_000), Currency::Inr));
    ctx.submit(OrderIntent::market_buy(nifty.clone(), q(3.0)));
    let usd_costs = FillReport::new(
        "F-1",
        "O-1",
        Uuid::nil(),
        nifty.clone(),
        OrderSide::Buy,
        p(100.0),
        q(3.0),
        "test",
        utc_nanos(1),
    )
    .with_fee(Money::new(Decimal::from(150), Currency::Usd), None);
    assert!(ctx.apply_fill(&usd_costs).is_err());
    assert_eq!(
        ctx.cash(),
        Money::new(Decimal::from(100_000), Currency::Inr)
    );
    assert_eq!(ctx.position(&nifty), Decimal::ZERO);
    assert!(ctx.busy(&nifty), "the refused fill did not fill the intent");
}

#[test]
fn releasing_one_order_leaves_a_second_order_on_the_instrument_pending() {
    let nifty = id("NIFTY50", "NSE");
    let mut ctx = LedgerContext::new();
    ctx.submit(OrderIntent::market_buy(nifty.clone(), q(3.0)));
    ctx.submit(OrderIntent::market_buy(nifty.clone(), q(2.0)));
    ctx.release_remainder(&nifty, OrderSide::Buy, Decimal::from(3));
    assert!(ctx.busy(&nifty), "2 still pending");
    ctx.release_remainder(&nifty, OrderSide::Buy, Decimal::from(2));
    assert!(!ctx.busy(&nifty));
}
