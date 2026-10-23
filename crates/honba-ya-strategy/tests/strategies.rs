//! Behavioural tests for reference strategies.

use honba_engine::Engine;
use honba_messages::{OrderSide, UnixNanos};
use honba_strategy::{
    BuyAndHold, LedgerContext, OrderIntent, RsiReversal, SmaCrossover, Strategy, StrategyAdapter,
};
use honba_testing::VecFeed;
use honba_testing::fixtures::{any_instrument, flat_bar};

// --- BuyAndHold ---

#[test]
fn buy_and_hold_emits_one_buy() {
    let id = any_instrument();
    let mut s = BuyAndHold::new(id, 10.0);
    let mut ctx = LedgerContext::new();

    let b = flat_bar("X", 1.0, 1);
    s.on_bar(&mut ctx, &b).unwrap();
    s.on_bar(&mut ctx, &b).unwrap();
    s.on_bar(&mut ctx, &b).unwrap();

    let intents = ctx.drain_intents();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].side, OrderSide::Buy);
    assert_eq!(intents[0].quantity, 10.0);
    assert!(s.has_bought());

    // Second drain is empty.
    assert!(ctx.drain_intents().is_empty());
}

// --- SmaCrossover ---

#[test]
fn sma_crossover_emits_buy_on_cross_up() {
    let id = any_instrument();
    let mut s = SmaCrossover::new(id, 2, 5, 1.0);
    let mut ctx = LedgerContext::new();

    // Flat then rise: fast SMA crosses above slow.
    let closes = [1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0];
    for (i, c) in closes.iter().enumerate() {
        s.on_bar(&mut ctx, &flat_bar("X", *c, i as u64 + 1))
            .unwrap();
    }

    let intents = ctx.drain_intents();
    assert!(!intents.is_empty(), "expected at least one intent");
    assert_eq!(intents[0].side, OrderSide::Buy);
}

#[test]
fn sma_crossover_emits_sell_on_cross_down() {
    let id = any_instrument();
    let mut s = SmaCrossover::new(id, 2, 5, 1.0);
    let mut ctx = LedgerContext::new();

    // Rise then fall.
    let closes = [1.0, 2.0, 3.0, 4.0, 5.0, 5.0, 1.0, 1.0, 1.0];
    for (i, c) in closes.iter().enumerate() {
        s.on_bar(&mut ctx, &flat_bar("X", *c, i as u64 + 1))
            .unwrap();
    }

    let intents = ctx.drain_intents();
    assert!(
        intents.iter().any(|i| i.side == OrderSide::Sell),
        "expected a sell intent, got {intents:?}"
    );
}

#[test]
#[should_panic(expected = "fast period must be less than slow period")]
fn sma_crossover_rejects_bad_periods() {
    let _ = SmaCrossover::new(any_instrument(), 20, 5, 1.0);
}

// --- RsiReversal ---

#[test]
fn rsi_reversal_buys_when_oversold() {
    let id = any_instrument();
    let mut s = RsiReversal::new(id, 3, 30.0, 70.0, 1.0);
    let mut ctx = LedgerContext::new();

    // Monotonic decline drives RSI to 0.
    for (i, c) in [10.0, 9.0, 8.0, 7.0, 6.0, 5.0].iter().enumerate() {
        s.on_bar(&mut ctx, &flat_bar("X", *c, i as u64 + 1))
            .unwrap();
    }

    let intents = ctx.drain_intents();
    assert!(
        intents.iter().any(|i| i.side == OrderSide::Buy),
        "expected a buy intent, got {intents:?}"
    );
}

#[test]
fn rsi_reversal_sells_when_overbought() {
    let id = any_instrument();
    let mut s = RsiReversal::new(id, 3, 30.0, 70.0, 1.0);
    let mut ctx = LedgerContext::new();

    // Monotonic rise drives RSI to 100.
    for (i, c) in [1.0, 2.0, 3.0, 4.0, 5.0, 6.0].iter().enumerate() {
        s.on_bar(&mut ctx, &flat_bar("X", *c, i as u64 + 1))
            .unwrap();
    }

    let intents = ctx.drain_intents();
    assert!(
        intents.iter().any(|i| i.side == OrderSide::Sell),
        "expected a sell intent, got {intents:?}"
    );
}

#[test]
fn rsi_reversal_does_not_repeat_in_zone() {
    let id = any_instrument();
    let mut s = RsiReversal::new(id, 3, 30.0, 70.0, 1.0);
    let mut ctx = LedgerContext::new();

    // Deep decline, all below oversold.
    for (i, c) in [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0].iter().enumerate() {
        s.on_bar(&mut ctx, &flat_bar("X", *c, i as u64 + 1))
            .unwrap();
    }

    let intents = ctx.drain_intents();
    let buys = intents.iter().filter(|i| i.side == OrderSide::Buy).count();
    assert_eq!(buys, 1, "expected exactly one buy, got {buys}");
}

// --- Integration with Engine ---

#[test]
fn strategy_runs_through_engine() {
    let id = any_instrument();
    let adapter = StrategyAdapter::new(BuyAndHold::new(id, 5.0));

    let mut engine = Engine::new();
    engine.add_handler(adapter);

    let mut feed = VecFeed::new(vec![
        VecFeed::bar("X", 1.0, 1),
        VecFeed::bar("X", 2.0, 2),
        VecFeed::bar("X", 3.0, 3),
    ]);
    engine.run(&mut feed).unwrap();
    // Run completed; strategy's intents were drained by the adapter internally
    // only if the runner calls drain_intents. Since the engine doesn't, the
    // intents remain in the adapter. We're just checking the run didn't fail.
}

// --- OrderIntent ---

#[test]
fn intent_into_order_preserves_fields() {
    use honba_messages::{OrderId, OrderType};

    let id = any_instrument();
    let intent = OrderIntent::market_buy(id.clone(), 75.0);
    let order = intent
        .into_order(OrderId::new("O-1"), UnixNanos::from_u64(100))
        .unwrap();

    assert_eq!(order.order_id().as_str(), "O-1");
    assert_eq!(order.instrument_id(), &id);
    assert_eq!(order.side(), OrderSide::Buy);
    assert_eq!(order.order_type(), OrderType::Market);
    assert_eq!(order.quantity(), 75.0);
    assert_eq!(order.ts_event().as_u64(), 100);
}

#[test]
fn limit_intent_carries_price() {
    let id = any_instrument();
    let intent = OrderIntent::limit_buy(id, 10.0, 22_000.0);
    assert_eq!(intent.price, Some(22_000.0));
}
