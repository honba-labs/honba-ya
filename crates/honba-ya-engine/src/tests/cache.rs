//! Unit tests for [`crate::cache::StateCache`].

use honba_entities::{Currency, Instrument, InstrumentKind};
use honba_messages::{
    AggressorSide, Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId,
    Order, OrderId, OrderSide, OrderStatus, OrderType, PriceType, QuoteTick, TimeInForce, TradeId,
    TradeTick, UnixNanos, VenueOrderId,
};

use crate::cache::{CacheQuery, StateCache};

fn ts(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn inst(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("NSE"))
}

fn bar(symbol: &str, close: f64, t: u64) -> Bar {
    let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let bt = BarType::new(inst(symbol), spec);
    Bar::new(
        bt,
        close - 1.0,
        close + 1.0,
        close - 2.0,
        close,
        100.0,
        ts(t),
        ts(t),
    )
}

fn quote(symbol: &str, bid: f64, ask: f64, t: u64) -> QuoteTick {
    QuoteTick::new(inst(symbol), bid, ask, 10.0, 10.0, ts(t), ts(t))
}

fn trade_tick(symbol: &str, price: f64, t: u64) -> TradeTick {
    TradeTick::new(
        inst(symbol),
        price,
        5.0,
        AggressorSide::Buyer,
        TradeId::new("T1"),
        ts(t),
        ts(t),
    )
}

fn market_order(id: &str, symbol: &str, side: OrderSide, qty: f64, t: u64) -> Order {
    Order::new(
        OrderId::new(id),
        inst(symbol),
        side,
        OrderType::Market,
        qty,
        None,
        TimeInForce::Day,
        ts(t),
        ts(t),
    )
}

#[test]
fn cache_apply_event() {
    let mut cache = StateCache::new();
    let symbol = "INFY";
    let instrument = inst(symbol);

    // Initial state
    assert_eq!(cache.position(&instrument), 0.0);
    assert!(cache.last_quote(&instrument).is_none());
    assert!(cache.last_bar(&instrument).is_none());
    assert!(cache.last_price(&instrument).is_none());

    // 1. Quote event
    let q = quote(symbol, 1400.0, 1402.0, 10);
    cache.apply_event(&Event::Quote(q.clone()));
    assert_eq!(cache.last_quote(&instrument), Some(&q));
    assert_eq!(cache.last_price(&instrument), Some(1401.0));
    assert_eq!(cache.last_ts(), ts(10));

    // 2. Bar event
    let b = bar(symbol, 1405.0, 20);
    cache.apply_event(&Event::Bar(b.clone()));
    assert_eq!(cache.last_bar(&instrument), Some(&b));
    assert_eq!(cache.last_price(&instrument), Some(1405.0));
    assert_eq!(cache.last_ts(), ts(20));

    // 3. Trade event
    let tt = trade_tick(symbol, 1406.0, 30);
    cache.apply_event(&Event::Trade(tt));
    assert_eq!(cache.last_price(&instrument), Some(1406.0));

    // 4. Order submitted
    let ord = market_order("ORD-1", symbol, OrderSide::Buy, 10.0, 40);
    cache.apply_event(&Event::Order(ord));
    assert_eq!(cache.open_orders().len(), 1);
    let tracked = cache.order("ORD-1").expect("ORD-1 tracked");
    assert_eq!(tracked.state.status, OrderStatus::Submitted);
    assert_eq!(tracked.state.quantity, 10.0);
    assert_eq!(tracked.side, OrderSide::Buy);
    assert!(tracked.is_working());

    // 5. Order accepted with venue order id
    let v_id = VenueOrderId::new("V-999");
    cache.apply_event(&Event::OrderAccepted {
        order_id: OrderId::new("ORD-1"),
        venue_order_id: Some(v_id.clone()),
        ts_event: ts(45),
    });
    let tracked = cache.order("ORD-1").unwrap();
    assert_eq!(tracked.state.status, OrderStatus::Accepted);
    assert_eq!(tracked.venue_order_id, Some(v_id));
    assert!(tracked.is_working());

    // 6. Partial fill
    cache.apply_event(&Event::OrderPartiallyFilled {
        order_id: OrderId::new("ORD-1"),
        last_qty: 4.0,
        cum_qty: 4.0,
        last_px: 1407.0,
        ts_event: ts(50),
    });
    let tracked = cache.order("ORD-1").unwrap();
    assert_eq!(tracked.state.status, OrderStatus::PartiallyFilled);
    assert_eq!(tracked.state.filled_qty, 4.0);
    assert_eq!(cache.position(&instrument), 4.0);
    assert_eq!(cache.last_price(&instrument), Some(1407.0));
    assert!(tracked.is_working());

    // 7. Complete fill
    cache.apply_event(&Event::OrderFilled {
        order_id: OrderId::new("ORD-1"),
        last_qty: 6.0,
        last_px: 1408.0,
        ts_event: ts(55),
    });
    let tracked = cache.order("ORD-1").unwrap();
    assert_eq!(tracked.state.status, OrderStatus::Filled);
    assert_eq!(tracked.state.filled_qty, 10.0);
    assert_eq!(cache.position(&instrument), 10.0);
    assert!(!tracked.is_working());
    assert_eq!(cache.open_orders().len(), 0);

    // 8. Sell order with cancellation
    let ord2 = market_order("ORD-2", symbol, OrderSide::Sell, 5.0, 60);
    cache.apply_event(&Event::Order(ord2));
    assert_eq!(cache.open_orders().len(), 1);
    cache.apply_event(&Event::OrderCancelled {
        order_id: OrderId::new("ORD-2"),
        ts_event: ts(65),
    });
    let tracked2 = cache.order("ORD-2").unwrap();
    assert_eq!(tracked2.state.status, OrderStatus::Cancelled);
    assert!(!tracked2.is_working());
    assert_eq!(cache.open_orders().len(), 0);
    assert_eq!(cache.position(&instrument), 10.0);
}

#[test]
fn cache_seeds_and_instruments() {
    let instrument = Instrument::new(
        inst("TCS"),
        InstrumentKind::Equity,
        Currency::Inr,
        1.0,
        0.05,
    );
    let id = instrument.id().clone();

    let cache = StateCache::new()
        .with_positions([(id.clone(), 15.0)])
        .with_instruments([instrument.clone()]);

    assert_eq!(cache.position(&id), 15.0);
    assert_eq!(cache.instrument(&id), Some(&instrument));
    assert_eq!(cache.instruments().len(), 1);
}
