//! Behavioural tests for reference strategies.

use honba_core::{OrderSide, OrderType, Price, Quantity};
use honba_strategy::{
    BuyAndHold, LedgerContext, OrderIntent, RsiReversal, SmaCrossover, Strategy, StrategyRunner,
};
use honba_testing::fixtures::{any_instrument, flat_bar, utc_nanos};

// --- BuyAndHold ---

#[test]
fn buy_and_hold_emits_one_buy() {
    let id = any_instrument();
    let mut s = BuyAndHold::new(id, Quantity::from_u64(10));
    let mut ctx = LedgerContext::new();

    let b = flat_bar("X", 1.0, 1);
    s.on_bar(&mut ctx, &b).unwrap();
    s.on_bar(&mut ctx, &b).unwrap();
    s.on_bar(&mut ctx, &b).unwrap();

    let intents = ctx.drain_intents();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].side, OrderSide::Buy);
    assert_eq!(intents[0].quantity, Quantity::from_u64(10));
    assert!(s.has_bought());

    // Second drain is empty.
    assert!(ctx.drain_intents().is_empty());
}

// --- SmaCrossover ---

#[test]
fn sma_crossover_emits_buy_on_cross_up() {
    let id = any_instrument();
    let mut s = SmaCrossover::new(id, 2, 5, Quantity::from_u64(1));
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
    let mut s = SmaCrossover::new(id, 2, 5, Quantity::from_u64(1));
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
    let _ = SmaCrossover::new(any_instrument(), 20, 5, Quantity::from_u64(1));
}

// --- RsiReversal ---

#[test]
fn rsi_reversal_buys_when_oversold() {
    let id = any_instrument();
    let mut s = RsiReversal::new(id, 3, 30.0, 70.0, Quantity::from_u64(1));
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
    let mut s = RsiReversal::new(id, 3, 30.0, 70.0, Quantity::from_u64(1));
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
    let mut s = RsiReversal::new(id, 3, 30.0, 70.0, Quantity::from_u64(1));
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

// --- Integration with StrategyRunner ---

#[test]
fn strategy_runs_through_runner() {
    let id = any_instrument();
    let mut runner = StrategyRunner::new(BuyAndHold::new(id, Quantity::from_u64(5)));
    runner.on_start().unwrap();
    runner
        .on_event(
            &honba_core::MarketEvent::Bar(flat_bar("X", 1.0, 1)),
            utc_nanos(1),
        )
        .unwrap();
    runner.on_stop().unwrap();
}

// --- OrderIntent ---

#[test]
fn intent_into_order_preserves_fields() {
    let id = any_instrument();
    let intent = OrderIntent::market_buy(id.clone(), Quantity::from_u64(75));
    let order = intent.into_order("algo-1").unwrap();

    assert_eq!(order.instrument, id);
    assert_eq!(order.side, OrderSide::Buy);
    assert_eq!(order.order_type, OrderType::Market);
    assert_eq!(order.quantity, Quantity::from_u64(75));
}

#[test]
fn limit_intent_carries_price() {
    let id = any_instrument();
    let intent = OrderIntent::limit_buy(
        id,
        Quantity::from_u64(10),
        Price::from_rupees("22000.00").unwrap(),
    );
    assert_eq!(
        intent.order_type,
        OrderType::Limit(Price::from_rupees("22000.00").unwrap())
    );
}
