//! The `StrategyContext` port is object-safe and usable through `&mut dyn`.

use honba_entities::{Currency, Instrument, Money};
use honba_messages::{InstrumentId, UnixNanos};

use super::any_instrument;
use crate::{OrderIntent, StrategyContext};

/// A hand-written context: proves the trait can be implemented outside the
/// reference ledger and used as a trait object, as a live runner would.
#[derive(Default)]
struct FixedContext {
    submitted: Vec<OrderIntent>,
}

impl StrategyContext for FixedContext {
    fn now(&self) -> UnixNanos {
        UnixNanos::from_u64(42)
    }
    fn position(&self, _instrument_id: &InstrumentId) -> f64 {
        3.0
    }
    fn positions(&self) -> Vec<(InstrumentId, f64)> {
        vec![(any_instrument(), 3.0)]
    }
    fn cash(&self) -> Money {
        Money::new(10_000, Currency::Inr)
    }
    fn busy(&self, _instrument_id: &InstrumentId) -> bool {
        !self.submitted.is_empty()
    }
    fn instrument(&self, _instrument_id: &InstrumentId) -> Option<&Instrument> {
        None
    }
    fn submit(&mut self, intent: OrderIntent) {
        self.submitted.push(intent);
    }
}

fn buy_one(ctx: &mut dyn StrategyContext) {
    let id = any_instrument();
    if !ctx.busy(&id) && ctx.instrument(&id).is_none() {
        ctx.submit(OrderIntent::market_buy(id, 1.0));
    }
}

#[test]
fn a_custom_context_works_as_a_trait_object() {
    let mut ctx = FixedContext::default();
    buy_one(&mut ctx);
    buy_one(&mut ctx); // busy now: no second intent
    assert_eq!(ctx.submitted.len(), 1);
    let view: &dyn StrategyContext = &ctx;
    assert_eq!(view.now(), UnixNanos::from_u64(42));
    assert_eq!(view.position(&any_instrument()), 3.0);
    assert_eq!(view.positions(), vec![(any_instrument(), 3.0)]);
    assert_eq!(view.cash().minor(), 10000);
}

#[test]
fn seed_position_sets_the_position_without_touching_cash() {
    let id = any_instrument();
    let mut ctx = crate::LedgerContext::with_cash(Money::new(500, Currency::Inr));
    ctx.seed_position(&id, 100.0);
    assert_eq!(ctx.position(&id), 100.0);
    assert_eq!(ctx.positions(), vec![(id.clone(), 100.0)]);
    assert_eq!(ctx.cash(), Money::new(500, Currency::Inr));
    ctx.seed_position(&id, 0.0);
    assert!(ctx.positions().is_empty());
}

#[test]
fn context_reads_from_cache() {
    use honba_engine::{StateCache, TrackedOrder};
    use honba_entities::{Currency, Instrument, InstrumentKind};
    use honba_messages::{Exchange, InstrumentId, OrderSide, OrderState, OrderStatus, UnixNanos};

    let id = InstrumentId::new("INFY", Exchange::new("NSE"));
    let instrument = Instrument::new(id.clone(), InstrumentKind::Equity, Currency::Inr, 1.0, 0.05);

    let mut cache = StateCache::new()
        .with_positions([(id.clone(), 10.0)])
        .with_instruments([instrument.clone()]);

    let mut tracked = TrackedOrder {
        state: OrderState::new(),
        instrument_id: id.clone(),
        side: OrderSide::Buy,
        venue_order_id: None,
    };
    tracked.state.status = OrderStatus::Submitted;
    cache.seed_order("ORD-1".to_string(), tracked);

    let mut ctx = crate::CacheContext::new(&cache, Money::new(50_000, Currency::Inr));
    ctx.set_now(UnixNanos::from_u64(100));

    // Polymorphic read via &dyn StrategyContext
    let view: &dyn StrategyContext = &ctx;
    assert_eq!(view.now(), UnixNanos::from_u64(100));
    assert_eq!(view.position(&id), 10.0);
    assert_eq!(view.positions(), vec![(id.clone(), 10.0)]);
    assert_eq!(view.cash(), Money::new(50_000, Currency::Inr));
    assert!(view.busy(&id));
    assert_eq!(view.instrument(&id), Some(&instrument));

    // Submit intent through context
    ctx.submit(crate::OrderIntent::market_buy(id.clone(), 5.0));
    let intents = ctx.drain_intents();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].quantity, 5.0);
}
