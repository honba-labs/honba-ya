//! Unit tests for `crate::l2` (ADR 0030): the top-of-book quote/trade matching core.

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::{Currency, ExecutionEvent, Money};
use honba_messages::{
    AggressorSide, Event, Order, OrderId, OrderSide, OrderType, QuoteTick, TimeInForce, TradeId,
    TradeTick, UnixNanos,
};

use super::{any_instrument, bar_event, market};
use crate::{L2Sim, NextOpenSim, SlippageKind, SlippageModel};

fn inr(rupees: i64) -> Money {
    Money::new(rupees * 100, Currency::Inr)
}

fn sim() -> L2Sim {
    L2Sim::new(inr(1_000_000)).unwrap()
}

fn t(ns: u64) -> UnixNanos {
    UnixNanos::from_u64(ns)
}

fn ord(id: &str, side: OrderSide, ty: OrderType, qty: f64, tif: TimeInForce, ts: u64) -> Order {
    Order::new(
        OrderId::new(id),
        any_instrument(),
        side,
        ty,
        qty,
        None,
        tif,
        t(ts),
        t(ts),
    )
}

fn lim(id: &str, side: OrderSide, qty: f64, px: f64, tif: TimeInForce, ts: u64) -> Order {
    let o = ord(id, side, OrderType::Limit, qty, tif, ts);
    Order::new(
        o.order_id().clone(),
        any_instrument(),
        side,
        OrderType::Limit,
        qty,
        Some(px),
        tif,
        t(ts),
        t(ts),
    )
}

fn stop(id: &str, side: OrderSide, qty: f64, trigger: f64, limit: Option<f64>, ts: u64) -> Order {
    let ty = if limit.is_some() {
        OrderType::StopLimit
    } else {
        OrderType::StopMarket
    };
    Order::new(
        OrderId::new(id),
        any_instrument(),
        side,
        ty,
        qty,
        limit,
        TimeInForce::Day,
        t(ts),
        t(ts),
    )
    .with_trigger_price(trigger)
}

fn quote(s: &mut L2Sim, bid: f64, ask: f64, bs: f64, asz: f64, ts: u64) {
    let q = QuoteTick::new(any_instrument(), bid, ask, bs, asz, t(ts), t(ts));
    s.on_quote(&q).unwrap();
}

fn trade(s: &mut L2Sim, px: f64, size: f64, ts: u64) {
    let tt = TradeTick::new(
        any_instrument(),
        px,
        size,
        AggressorSide::Buyer,
        TradeId::new(format!("T-{ts}")),
        t(ts),
        t(ts),
    );
    s.on_trade(&tt).unwrap();
}

/// `(order id, qty, price, ts)` of every fill, in order.
fn fills(events: &[ExecutionEvent]) -> Vec<(String, f64, f64, u64)> {
    events
        .iter()
        .filter_map(|e| match e {
            ExecutionEvent::Fill { trade, .. } => Some((
                trade.order_id().as_str().to_string(),
                trade.quantity(),
                trade.price(),
                trade.ts_event().as_u64(),
            )),
            _ => None,
        })
        .collect()
}

fn reasons(events: &[ExecutionEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            ExecutionEvent::Rejected { reason, .. } => Some(reason.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn negative_cash_is_refused() {
    assert!(L2Sim::new(Money::new(-1, Currency::Inr)).is_err());
}

#[test]
fn market_orders_fill_at_the_opposite_touch() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 100.0, 100.0, 1);
    s.set_position(&any_instrument(), 5.0).unwrap();
    s.submit(market("B", OrderSide::Buy, 10.0, 2)).unwrap();
    s.submit(market("S", OrderSide::Sell, 5.0, 2)).unwrap();
    let ev = s.drain_events().unwrap();
    assert_eq!(
        fills(&ev),
        vec![("B".into(), 10.0, 101.0, 2), ("S".into(), 5.0, 99.0, 2)]
    );
    assert_eq!(s.position(&any_instrument()), 10.0);
}

#[test]
fn market_partial_fill_by_top_size_then_rest_on_next_quote() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 4.0, 1);
    s.submit(market("B", OrderSide::Buy, 10.0, 2)).unwrap();
    let ev = s.drain_events().unwrap();
    assert_eq!(fills(&ev), vec![("B".into(), 4.0, 101.0, 2)]);
    assert!(
        matches!(&ev[0], ExecutionEvent::Fill { complete: false, cum_qty, .. } if *cum_qty == 4.0)
    );
    assert_eq!(s.working_orders(), vec!["B"]);
    // The consumed size is gone until the book updates.
    quote(&mut s, 99.0, 102.0, 10.0, 20.0, 3);
    let ev = s.drain_events().unwrap();
    assert_eq!(fills(&ev), vec![("B".into(), 6.0, 102.0, 3)]);
    assert!(
        matches!(&ev[0], ExecutionEvent::Fill { complete: true, cum_qty, .. } if *cum_qty == 10.0)
    );
    assert!(s.working_orders().is_empty());
}

#[test]
fn consumed_top_size_is_not_reused_by_a_second_order() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 6.0, 1);
    s.submit(market("A", OrderSide::Buy, 5.0, 2)).unwrap();
    s.submit(market("B", OrderSide::Buy, 5.0, 2)).unwrap();
    let ev = s.drain_events().unwrap();
    assert_eq!(
        fills(&ev),
        vec![("A".into(), 5.0, 101.0, 2), ("B".into(), 1.0, 101.0, 2)]
    );
}

#[test]
fn market_order_waits_for_the_first_quote() {
    let mut s = sim();
    s.submit(market("B", OrderSide::Buy, 3.0, 1)).unwrap();
    assert!(s.drain_events().unwrap().is_empty());
    quote(&mut s, 99.0, 101.0, 10.0, 10.0, 5);
    assert_eq!(
        fills(&s.drain_events().unwrap()),
        vec![("B".into(), 3.0, 101.0, 5)]
    );
}

#[test]
fn limit_fills_on_trade_through() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 10.0, 1);
    s.submit(lim("L", OrderSide::Buy, 10.0, 100.0, TimeInForce::Day, 2))
        .unwrap();
    assert!(s.drain_events().unwrap().is_empty()); // not marketable: rests
    trade(&mut s, 100.5, 50.0, 3); // above the limit: no fill
    assert!(s.drain_events().unwrap().is_empty());
    trade(&mut s, 100.0, 50.0, 4); // at the limit: fills at the limit
    assert_eq!(
        fills(&s.drain_events().unwrap()),
        vec![("L".into(), 10.0, 100.0, 4)]
    );
    assert_eq!(s.cash(), inr(1_000_000 - 1_000));
}

#[test]
fn sell_limit_fills_when_a_trade_prints_at_or_above() {
    let mut s = sim();
    s.set_position(&any_instrument(), 10.0).unwrap();
    quote(&mut s, 99.0, 101.0, 10.0, 10.0, 1);
    s.submit(lim("L", OrderSide::Sell, 10.0, 102.0, TimeInForce::Gtc, 2))
        .unwrap();
    trade(&mut s, 101.9, 50.0, 3);
    assert!(s.drain_events().unwrap().is_empty());
    trade(&mut s, 103.0, 4.0, 4); // through the limit, only 4 printed
    let ev = s.drain_events().unwrap();
    assert_eq!(fills(&ev), vec![("L".into(), 4.0, 102.0, 4)]);
    assert_eq!(s.working_orders(), vec!["L"]);
}

#[test]
fn marketable_limit_walks_the_touch_then_rests() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 4.0, 1);
    s.submit(lim("L", OrderSide::Buy, 10.0, 102.0, TimeInForce::Day, 2))
        .unwrap();
    assert_eq!(
        fills(&s.drain_events().unwrap()),
        vec![("L".into(), 4.0, 101.0, 2)]
    );
    assert_eq!(s.working_orders(), vec!["L"]);
    trade(&mut s, 102.0, 50.0, 3); // the remainder rests at its limit
    assert_eq!(
        fills(&s.drain_events().unwrap()),
        vec![("L".into(), 6.0, 102.0, 3)]
    );
}

#[test]
fn a_resting_limit_that_a_new_quote_crosses_fills_at_the_touch() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 10.0, 1);
    s.submit(lim("L", OrderSide::Buy, 5.0, 100.0, TimeInForce::Day, 2))
        .unwrap();
    quote(&mut s, 97.0, 99.5, 10.0, 10.0, 3);
    assert_eq!(
        fills(&s.drain_events().unwrap()),
        vec![("L".into(), 5.0, 99.5, 3)]
    );
}

#[test]
fn ioc_fills_what_it_can_and_cancels_the_rest() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 4.0, 1);
    s.submit(lim("I", OrderSide::Buy, 10.0, 101.0, TimeInForce::Ioc, 2))
        .unwrap();
    let ev = s.drain_events().unwrap();
    assert_eq!(fills(&ev), vec![("I".into(), 4.0, 101.0, 2)]);
    assert!(matches!(
        ev.last().unwrap(),
        ExecutionEvent::Cancelled { quantity, ts, .. } if *quantity == 6.0 && ts.as_u64() == 2
    ));
    assert!(s.working_orders().is_empty());
}

#[test]
fn ioc_that_is_not_marketable_cancels_whole() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 10.0, 1);
    s.submit(lim("I", OrderSide::Buy, 5.0, 100.0, TimeInForce::Ioc, 2))
        .unwrap();
    let ev = s.drain_events().unwrap();
    assert!(fills(&ev).is_empty());
    assert!(matches!(&ev[..], [ExecutionEvent::Cancelled { quantity, .. }] if *quantity == 5.0));
}

#[test]
fn fok_is_all_or_nothing_against_the_current_size() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 4.0, 1);
    s.submit(lim("F1", OrderSide::Buy, 10.0, 101.0, TimeInForce::Fok, 2))
        .unwrap();
    let ev = s.drain_events().unwrap();
    assert!(fills(&ev).is_empty());
    assert_eq!(reasons(&ev), vec!["fok_unfillable"]);
    s.submit(lim("F2", OrderSide::Buy, 4.0, 101.0, TimeInForce::Fok, 3))
        .unwrap();
    assert_eq!(
        fills(&s.drain_events().unwrap()),
        vec![("F2".into(), 4.0, 101.0, 3)]
    );
    assert!(s.working_orders().is_empty());
}

#[test]
fn stop_market_triggers_on_a_trade_and_fills_at_the_touch() {
    let mut s = sim();
    quote(&mut s, 104.0, 106.0, 10.0, 10.0, 1);
    s.submit(stop("S", OrderSide::Buy, 5.0, 105.0, None, 2))
        .unwrap();
    trade(&mut s, 104.9, 1.0, 3);
    assert!(s.drain_events().unwrap().is_empty());
    trade(&mut s, 105.0, 1.0, 4);
    assert_eq!(
        fills(&s.drain_events().unwrap()),
        vec![("S".into(), 5.0, 106.0, 4)]
    );
}

#[test]
fn sell_stop_limit_triggers_then_behaves_as_a_limit() {
    let mut s = sim();
    s.set_position(&any_instrument(), 10.0).unwrap();
    quote(&mut s, 94.0, 95.5, 10.0, 10.0, 1);
    s.submit(stop("S", OrderSide::Sell, 10.0, 95.0, Some(96.0), 2))
        .unwrap();
    trade(&mut s, 95.0, 1.0, 3); // triggers; limit 96 is above the bid 94: rests
    assert!(s.drain_events().unwrap().is_empty());
    assert_eq!(s.working_orders(), vec!["S"]);
    trade(&mut s, 96.0, 100.0, 4); // prints through the sell limit
    assert_eq!(
        fills(&s.drain_events().unwrap()),
        vec![("S".into(), 10.0, 96.0, 4)]
    );
}

#[test]
fn a_trailing_stop_is_rejected() {
    let mut s = sim();
    s.submit(ord(
        "T",
        OrderSide::Sell,
        OrderType::TrailingStop,
        1.0,
        TimeInForce::Day,
        1,
    ))
    .unwrap();
    assert_eq!(
        reasons(&s.drain_events().unwrap()),
        vec!["unsupported_order_type"]
    );
}

#[test]
fn time_priority_is_fifo_at_the_touch() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 10.0, 1);
    s.submit(lim("A", OrderSide::Buy, 5.0, 100.0, TimeInForce::Day, 2))
        .unwrap();
    s.submit(lim("B", OrderSide::Buy, 5.0, 100.0, TimeInForce::Day, 3))
        .unwrap();
    trade(&mut s, 100.0, 5.0, 4);
    assert_eq!(
        fills(&s.drain_events().unwrap()),
        vec![("A".into(), 5.0, 100.0, 4)]
    );
    trade(&mut s, 100.0, 7.0, 5);
    assert_eq!(
        fills(&s.drain_events().unwrap()),
        vec![("B".into(), 5.0, 100.0, 5)]
    );
}

#[test]
fn cancel_is_requested_and_confirmed_at_the_cancel_time() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 10.0, 1);
    s.submit(lim("L", OrderSide::Buy, 5.0, 100.0, TimeInForce::Day, 2))
        .unwrap();
    s.cancel("L", t(50)).unwrap();
    let ev = s.drain_events().unwrap();
    assert!(matches!(&ev[0], ExecutionEvent::CancelRequested { ts, .. } if ts.as_u64() == 50));
    assert!(
        matches!(&ev[1], ExecutionEvent::Cancelled { ts, quantity, .. }
        if ts.as_u64() == 50 && *quantity == 5.0)
    );
    assert_eq!(ev.len(), 2);
    trade(&mut s, 100.0, 9.0, 60);
    assert!(s.drain_events().unwrap().is_empty());
    s.cancel("L", t(70)).unwrap(); // already gone: no events
    assert!(s.drain_events().unwrap().is_empty());
}

#[test]
fn day_orders_expire_at_the_session_close_gtc_do_not() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 10.0, 1_000_000_000);
    s.submit(lim(
        "D",
        OrderSide::Buy,
        5.0,
        100.0,
        TimeInForce::Day,
        1_000_000_001,
    ))
    .unwrap();
    s.submit(lim(
        "G",
        OrderSide::Buy,
        5.0,
        100.0,
        TimeInForce::Gtc,
        1_000_000_001,
    ))
    .unwrap();
    // 23:59:00 UTC on day 0 is the default close.
    let close = 86_340_000_000_000_u64;
    trade(&mut s, 200.0, 1.0, close - 1); // before the close: still working
    assert!(s.drain_events().unwrap().is_empty());
    trade(&mut s, 100.0, 100.0, close + 1);
    let ev = s.drain_events().unwrap();
    assert!(
        matches!(&ev[0], ExecutionEvent::Expired { order_id, ts, quantity, .. }
        if order_id.as_str() == "D" && ts.as_u64() == close && *quantity == 5.0)
    );
    // The Day order is gone before the print; the GTC one fills on it.
    assert_eq!(fills(&ev), vec![("G".into(), 5.0, 100.0, close + 1)]);
}

#[test]
fn explicit_session_close_expires_day_orders() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 10.0, 1);
    s.submit(lim("D", OrderSide::Buy, 5.0, 100.0, TimeInForce::Day, 2))
        .unwrap();
    s.on_session_close(t(900)).unwrap();
    let ev = s.drain_events().unwrap();
    assert!(matches!(&ev[..], [ExecutionEvent::Expired { ts, .. }] if ts.as_u64() == 900));
}

#[test]
fn insufficient_cash_cuts_the_fill_and_rejects_the_rest() {
    let mut s = L2Sim::new(inr(505)).unwrap();
    quote(&mut s, 99.0, 100.0, 10.0, 100.0, 1);
    s.submit(market("B", OrderSide::Buy, 10.0, 2)).unwrap();
    let ev = s.drain_events().unwrap();
    assert_eq!(fills(&ev), vec![("B".into(), 5.0, 100.0, 2)]);
    assert_eq!(reasons(&ev), vec!["insufficient_funds"]);
    assert!(s.working_orders().is_empty());
}

#[test]
fn selling_more_than_held_is_capped_when_long_only() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 10.0, 1);
    s.submit(market("S", OrderSide::Sell, 5.0, 2)).unwrap();
    assert_eq!(reasons(&s.drain_events().unwrap()), vec!["no_position"]);
}

fn run_stream(seed: u64) -> String {
    let mut s = L2Sim::new(inr(1_000_000))
        .unwrap()
        .with_slippage(SlippageModel::Seed { bps: 5.0, seed });
    let _ = seed;
    let mut out = String::new();
    for i in 0..20_u64 {
        let base = 100.0 + (i % 5) as f64;
        quote(&mut s, base - 0.5, base + 0.5, 3.0, 3.0, 10 + i * 10);
        if i % 2 == 0 {
            s.submit(market(&format!("M{i}"), OrderSide::Buy, 4.0, 11 + i * 10))
                .unwrap();
        } else {
            s.submit(lim(
                &format!("L{i}"),
                OrderSide::Buy,
                2.0,
                base - 1.0,
                TimeInForce::Day,
                11 + i * 10,
            ))
            .unwrap();
        }
        trade(&mut s, base - 1.0, 1.5, 15 + i * 10);
        out.push_str(&format!("{:?}\n", s.drain_events().unwrap()));
    }
    out
}

#[test]
fn the_same_stream_twice_gives_identical_events() {
    assert_eq!(run_stream(7), run_stream(7));
    assert!(run_stream(7).contains("Fill"));
    assert_ne!(run_stream(7), run_stream(8));
}

#[test]
fn static_slippage_never_beats_the_limit_price() {
    let mut s = sim().with_slippage(SlippageModel::Static {
        bps: 1_000.0,
        kind: SlippageKind::Adverse,
    });
    quote(&mut s, 99.0, 101.0, 10.0, 10.0, 1);
    s.submit(lim("L", OrderSide::Buy, 1.0, 102.0, TimeInForce::Ioc, 2))
        .unwrap();
    assert_eq!(fills(&s.drain_events().unwrap())[0].2, 102.0);
}

#[test]
fn the_handler_path_drives_quotes_and_trades() {
    let mut s = sim();
    let q = Event::Quote(QuoteTick::new(
        any_instrument(),
        99.0,
        101.0,
        10.0,
        10.0,
        t(1),
        t(1),
    ));
    s.on_event(&q, t(1)).unwrap();
    s.submit(lim("L", OrderSide::Buy, 1.0, 100.0, TimeInForce::Day, 2))
        .unwrap();
    let tr = Event::Trade(TradeTick::new(
        any_instrument(),
        99.5,
        5.0,
        AggressorSide::Seller,
        TradeId::new("T1"),
        t(3),
        t(3),
    ));
    s.on_event(&tr, t(3)).unwrap();
    assert_eq!(s.drain_fills().unwrap().len(), 1);
}

#[test]
fn l1_and_l2_agree_on_marketable_orders_over_a_shared_stream() {
    let mut l1 = NextOpenSim::new(inr(1_000_000)).unwrap();
    let mut l2 = sim();
    l1.submit(market("O", OrderSide::Buy, 7.0, 1)).unwrap();
    l2.submit(market("O", OrderSide::Buy, 7.0, 1)).unwrap();
    l1.on_event(&bar_event(250.0, 5), t(5)).unwrap();
    quote(&mut l2, 250.0, 250.0, 100.0, 100.0, 5);
    assert_eq!(
        fills(&l1.drain_events().unwrap()),
        fills(&l2.drain_events().unwrap())
    );
    assert_eq!(l1.cash(), l2.cash());
    assert_eq!(
        l1.position(&any_instrument()),
        l2.position(&any_instrument())
    );
}

#[test]
fn l1_and_l2_agree_on_a_buy_then_sell_round_trip() {
    let mut l1 = NextOpenSim::new(inr(1_000_000)).unwrap();
    let mut l2 = sim();
    let (mut f1, mut f2) = (Vec::new(), Vec::new());
    for (n, (px, ts)) in [(100.0, 10_u64), (103.0, 20), (101.0, 30)]
        .iter()
        .enumerate()
    {
        l1.on_event(&bar_event(*px, *ts), t(*ts)).unwrap();
        quote(&mut l2, *px, *px, 1_000.0, 1_000.0, *ts);
        f1.extend(fills(&l1.drain_events().unwrap()));
        f2.extend(fills(&l2.drain_events().unwrap()));
        // Submit between quotes, before the next one prints: both fill on the next quote.
        match n {
            0 => {
                l1.submit(market("B", OrderSide::Buy, 5.0, *ts)).unwrap();
                l2.submit(lim("B", OrderSide::Buy, 5.0, 1e9, TimeInForce::Gtc, *ts))
                    .unwrap();
            }
            1 => {
                l1.submit(market("S", OrderSide::Sell, 5.0, *ts)).unwrap();
                l2.submit(lim("S", OrderSide::Sell, 5.0, 1e-9, TimeInForce::Gtc, *ts))
                    .unwrap();
            }
            _ => {}
        }
    }
    // L2 sees the standing locked book, so its orders fill at the quote that stood when they
    // were submitted; same orders, quantities and sides, and the same final position.
    assert_eq!(
        f1.iter().map(|f| (f.0.clone(), f.1)).collect::<Vec<_>>(),
        f2.iter().map(|f| (f.0.clone(), f.1)).collect::<Vec<_>>()
    );
    assert_eq!(
        l1.position(&any_instrument()),
        l2.position(&any_instrument())
    );
}

#[test]
fn advance_to_expires_day_orders_past_the_close_without_an_event() {
    let mut s = sim();
    quote(&mut s, 99.0, 101.0, 10.0, 10.0, 1);
    s.submit(lim("D", OrderSide::Buy, 5.0, 100.0, TimeInForce::Day, 2))
        .unwrap();
    let close = 86_340_000_000_000_u64;
    s.advance_to(t(close - 1)).unwrap();
    assert_eq!(s.working_orders(), vec!["D"]);
    s.advance_to(t(close + 5)).unwrap();
    let ev = s.drain_events().unwrap();
    assert!(matches!(&ev[..], [ExecutionEvent::Expired { ts, .. }] if ts.as_u64() == close));
    assert!(s.working_orders().is_empty());
}
