//! The `StrategyContext` port is object-safe and usable through `&mut dyn`.

use chrono::{DateTime, Utc};
use honba_core::{Currency, Exchange, InstrumentId, Money, Price, Quantity};
use honba_testing::fixtures::utc_nanos;
use rust_decimal::Decimal;

use super::any_instrument;
use crate::context::InstrumentInfo;
use crate::{LedgerContext, OrderIntent, StrategyContext};

/// A hand-written context: proves the trait can be implemented outside the
/// reference ledger and used as a trait object, as a live runner would.
#[derive(Default)]
struct FixedContext {
    submitted: Vec<OrderIntent>,
}

impl StrategyContext for FixedContext {
    fn now(&self) -> DateTime<Utc> {
        utc_nanos(42)
    }
    fn position(&self, _instrument_id: &InstrumentId) -> Decimal {
        Decimal::from(3)
    }
    fn positions(&self) -> Vec<(InstrumentId, Decimal)> {
        vec![(any_instrument(), Decimal::from(3))]
    }
    fn cash(&self) -> Money {
        Money::new(Decimal::from(10_000), Currency::Inr)
    }
    fn busy(&self, _instrument_id: &InstrumentId) -> bool {
        !self.submitted.is_empty()
    }
    fn instrument(&self, _instrument_id: &InstrumentId) -> Option<&InstrumentInfo> {
        None
    }
    fn submit(&mut self, intent: OrderIntent) {
        self.submitted.push(intent);
    }
}

fn buy_one(ctx: &mut dyn StrategyContext) {
    let id = any_instrument();
    if !ctx.busy(&id) && ctx.instrument(&id).is_none() {
        ctx.submit(OrderIntent::market_buy(id, Quantity::from_u64(1)));
    }
}

#[test]
fn a_custom_context_works_as_a_trait_object() {
    let mut ctx = FixedContext::default();
    buy_one(&mut ctx);
    buy_one(&mut ctx); // busy now: no second intent
    assert_eq!(ctx.submitted.len(), 1);
    let view: &dyn StrategyContext = &ctx;
    assert_eq!(view.now(), utc_nanos(42));
    assert_eq!(view.position(&any_instrument()), Decimal::from(3));
    assert_eq!(view.positions(), vec![(any_instrument(), Decimal::from(3))]);
    assert_eq!(view.cash().amount, Decimal::from(10_000));
}

#[test]
fn seed_position_sets_the_position_without_touching_cash() {
    let id = any_instrument();
    let mut ctx = LedgerContext::with_cash(Money::new(Decimal::from(500), Currency::Inr));
    ctx.seed_position(&id, Decimal::from(100));
    assert_eq!(ctx.position(&id), Decimal::from(100));
    assert_eq!(ctx.positions(), vec![(id.clone(), Decimal::from(100))]);
    assert_eq!(ctx.cash(), Money::new(Decimal::from(500), Currency::Inr));
    ctx.seed_position(&id, Decimal::ZERO);
    assert!(ctx.positions().is_empty());
}

#[test]
fn ledger_context_polymorphic_reads_and_submissions() {
    let id = InstrumentId::new(Exchange::Nse, "INFY");
    let instrument = InstrumentInfo {
        id: id.clone(),
        lot_size: Quantity::from_u64(1),
        tick_size: Price::new(Decimal::new(5, 2)),
    };

    let mut ctx = LedgerContext::with_cash(Money::new(Decimal::from(50_000), Currency::Inr));
    ctx.set_now(utc_nanos(100));
    ctx.add_instrument(instrument.clone());
    ctx.seed_position(&id, Decimal::from(10));
    ctx.submit(OrderIntent::market_buy(id.clone(), Quantity::from_u64(5)));

    let view: &dyn StrategyContext = &ctx;
    assert_eq!(view.now(), utc_nanos(100));
    assert_eq!(view.position(&id), Decimal::from(10));
    assert_eq!(view.positions(), vec![(id.clone(), Decimal::from(10))]);
    assert_eq!(
        view.cash(),
        Money::new(Decimal::from(50_000), Currency::Inr)
    );
    assert!(view.busy(&id));
    assert_eq!(view.instrument(&id), Some(&instrument));

    let intents = ctx.drain_intents();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].quantity, Quantity::from_u64(5));
}
