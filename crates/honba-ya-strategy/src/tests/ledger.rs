//! `LedgerContext`: the reference `StrategyContext` (ADR 008). Mirrors
//! `python/tests/unit/test_ledger_context.py`.

use honba_entities::{Currency, Instrument, InstrumentKind, Money, Trade};
use honba_messages::{Exchange, InstrumentId, OrderSide, UnixNanos};

use crate::{LedgerContext, OrderIntent, StrategyContext};

fn id(symbol: &str, exchange: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new(exchange))
}

fn fill(instrument_id: &InstrumentId, side: OrderSide, qty: f64, px: f64, costs: f64) -> Trade {
    Trade::new(
        "O-1".into(),
        instrument_id.clone(),
        side,
        qty,
        px,
        Currency::Inr,
        UnixNanos::from_u64(1),
        UnixNanos::from_u64(1),
    )
    .with_costs(Money::from_major_f64(costs, Currency::Inr).unwrap())
}

#[test]
fn starts_empty() {
    let ctx = LedgerContext::new();
    let nifty = id("NIFTY50", "NSE");
    assert_eq!(ctx.now(), UnixNanos::from_u64(0));
    assert_eq!(ctx.cash().minor(), 0);
    assert!(ctx.positions().is_empty());
    assert_eq!(ctx.position(&nifty), 0.0);
    assert!(!ctx.busy(&nifty));
    assert!(ctx.instrument(&nifty).is_none());
}

#[test]
fn clock_is_set_by_the_runner() {
    let mut ctx = LedgerContext::new();
    ctx.set_now(UnixNanos::from_u64(1_700_000_000_000_000_000));
    assert_eq!(ctx.now(), UnixNanos::from_u64(1_700_000_000_000_000_000));
}

#[test]
fn instrument_lookup() {
    let nifty = id("NIFTY50", "NSE");
    let meta = Instrument::new(
        nifty.clone(),
        InstrumentKind::Index,
        Currency::Inr,
        75.0,
        0.05,
    );
    let mut ctx = LedgerContext::new();
    ctx.add_instrument(meta.clone());
    assert_eq!(ctx.instrument(&nifty), Some(&meta));
}

#[test]
fn submit_queues_in_order_and_marks_busy_until_filled() {
    let (nifty, infy) = (id("NIFTY50", "NSE"), id("INFY", "NSE"));
    let mut ctx = LedgerContext::new();
    let first = OrderIntent::market_buy(nifty.clone(), 10.0);
    let second = OrderIntent::limit_sell(infy.clone(), 2.0, 1500.0);
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
    let intent = OrderIntent::market_sell(nifty.clone(), 5.0);
    ctx.submit(intent.clone());
    ctx.release(&intent);
    assert!(!ctx.busy(&nifty));
}

#[test]
fn release_remainder_frees_only_the_unfilled_part() {
    let nifty = id("NIFTY50", "NSE");
    let mut ctx = LedgerContext::new();
    ctx.submit(OrderIntent::market_buy(nifty.clone(), 10.0));
    ctx.release_remainder(&nifty, OrderSide::Buy, 4.0);
    assert!(ctx.busy(&nifty), "6 still pending");
    ctx.release_remainder(&nifty, OrderSide::Buy, 6.0);
    assert!(!ctx.busy(&nifty));
    ctx.release_remainder(&nifty, OrderSide::Buy, f64::NAN);
    ctx.release_remainder(&nifty, OrderSide::Buy, -1.0);
}

#[test]
fn fills_move_position_and_cash_including_costs() {
    let nifty = id("NIFTY50", "NSE");
    let mut ctx = LedgerContext::with_cash(Money::from_major_f64(1_000.0, Currency::Inr).unwrap());
    ctx.apply_fill(&fill(&nifty, OrderSide::Buy, 3.0, 100.0, 1.5))
        .unwrap();
    assert_eq!(ctx.position(&nifty), 3.0);
    assert_eq!(ctx.cash().minor(), 100_000 - 30_000 - 150);
    ctx.apply_fill(&fill(&nifty, OrderSide::Sell, 5.0, 110.0, 2.0))
        .unwrap();
    assert_eq!(ctx.position(&nifty), -2.0, "signed: short 2");
    assert_eq!(ctx.cash().minor(), 69_850 + 55_000 - 200);
}

#[test]
fn positions_lists_non_flat_ordered_by_symbol_then_exchange() {
    let (nifty, infy) = (id("NIFTY50", "NSE"), id("INFY", "NSE"));
    let (acme_bse, acme_nse) = (id("ACME", "BSE"), id("ACME", "NSE"));
    let mut ctx = LedgerContext::new();
    for i in [&nifty, &acme_nse, &infy, &acme_bse] {
        ctx.apply_fill(&fill(i, OrderSide::Buy, 1.0, 10.0, 0.0))
            .unwrap();
    }
    ctx.apply_fill(&fill(&infy, OrderSide::Sell, 1.0, 10.0, 0.0))
        .unwrap();
    assert_eq!(
        ctx.positions(),
        vec![(acme_bse, 1.0), (acme_nse, 1.0), (nifty, 1.0)]
    );
}

#[test]
fn invalid_intent_does_not_touch_pending_state() {
    let mut ctx = LedgerContext::new();
    let nifty = id("NIFTY50", "NSE");
    ctx.submit(OrderIntent::market_buy(nifty.clone(), 10.0));
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
        let intent = OrderIntent::market_buy(nifty.clone(), bad);
        ctx.submit(intent.clone());
        assert!(ctx.busy(&nifty), "pending wiped by quantity {bad}");
        // The runner releases rejected intents; that must not corrupt it either.
        ctx.release(&intent);
        assert!(ctx.busy(&nifty), "pending wiped by release of {bad}");
    }
    // The valid 10 is still exactly what is pending: a 10-lot fill clears it.
    ctx.apply_fill(&fill(&nifty, OrderSide::Buy, 10.0, 1.0, 0.0))
        .unwrap();
    assert!(!ctx.busy(&nifty));
    // Invalid intents are still handed to the runner to be rejected.
    assert_eq!(ctx.drain_intents().len(), 5);
}

#[test]
fn a_fill_whose_costs_are_in_another_currency_is_refused_whole() {
    // Previously the cash leg was silently skipped while the position moved:
    // a ledger that drops money is worse than one that refuses the fill.
    let nifty = id("NIFTY50", "NSE");
    let mut ctx = LedgerContext::with_cash(Money::new(100_000, Currency::Inr));
    ctx.submit(OrderIntent::market_buy(nifty.clone(), 3.0));
    let usd_costs = Trade::new(
        "O-1".into(),
        nifty.clone(),
        OrderSide::Buy,
        3.0,
        100.0,
        Currency::Usd,
        UnixNanos::from_u64(1),
        UnixNanos::from_u64(1),
    )
    .with_costs(Money::new(150, Currency::Usd));
    assert!(ctx.apply_fill(&usd_costs).is_err());
    assert_eq!(ctx.cash(), Money::new(100_000, Currency::Inr));
    assert_eq!(ctx.position(&nifty), 0.0);
    assert!(ctx.busy(&nifty), "the refused fill did not fill the intent");
}

#[test]
fn a_fill_whose_notional_cannot_be_represented_is_refused_whole() {
    let nifty = id("NIFTY50", "NSE");
    let mut ctx = LedgerContext::with_cash(Money::new(100_000, Currency::Inr));
    assert!(
        ctx.apply_fill(&fill(&nifty, OrderSide::Buy, 1e12, 1e12, 0.0))
            .is_err()
    );
    assert_eq!(ctx.cash(), Money::new(100_000, Currency::Inr));
    assert_eq!(ctx.position(&nifty), 0.0);
}

#[test]
fn notional_rounds_once_per_fill_to_minor_units() {
    // 3 * 33.333 = 99.999: one rounding per fill to 10000 minor units, debited once.
    let nifty = id("NIFTY50", "NSE");
    let mut ctx = LedgerContext::with_cash(Money::new(100_000, Currency::Inr));
    ctx.apply_fill(&fill(&nifty, OrderSide::Buy, 3.0, 33.333, 0.0))
        .unwrap();
    assert_eq!(ctx.cash().minor(), 100_000 - 10_000);
}

#[test]
fn releasing_one_order_leaves_a_second_order_on_the_instrument_pending() {
    let nifty = id("NIFTY50", "NSE");
    let mut ctx = LedgerContext::new();
    ctx.submit(OrderIntent::market_buy(nifty.clone(), 3.0));
    ctx.submit(OrderIntent::market_buy(nifty.clone(), 2.0));
    ctx.release_remainder(&nifty, OrderSide::Buy, 3.0);
    assert!(ctx.busy(&nifty), "2 still pending");
    ctx.release_remainder(&nifty, OrderSide::Buy, 2.0);
    assert!(!ctx.busy(&nifty));
}
