//! The Fill -> Trade mapping of the journal routes (ADR 0017 decision 7).

use honba_entities::Currency;
use honba_messages::{
    ErrorCode, Event, Exchange, InstrumentId, Message, Order, OrderId, OrderSide, OrderType,
    QuoteTick, TimeInForce, UnixNanos,
};

use crate::trades::trades_from_journal;

fn tcs() -> InstrumentId {
    InstrumentId::new("TCS", Exchange::new("NSE"))
}

fn ns(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn order(id: &str, side: OrderSide) -> Message {
    let o = Order::new(
        OrderId::new(id),
        tcs(),
        side,
        OrderType::Market,
        10.0,
        None,
        TimeInForce::Day,
        ns(5),
        ns(5),
    );
    Message::new(Event::Order(o), ns(5))
}

fn partial(id: &str, qty: f64, px: f64, cum: f64, ts: u64, init: u64) -> Message {
    Message::new(
        Event::OrderPartiallyFilled {
            order_id: OrderId::new(id),
            last_qty: qty,
            last_px: px,
            cum_qty: cum,
            ts_event: ns(ts),
        },
        ns(init),
    )
}

fn filled(id: &str, qty: f64, px: f64, ts: u64, init: u64) -> Message {
    Message::new(
        Event::OrderFilled {
            order_id: OrderId::new(id),
            last_qty: qty,
            last_px: px,
            ts_event: ns(ts),
        },
        ns(init),
    )
}

#[test]
fn an_empty_journal_has_no_trades() {
    assert!(trades_from_journal(&[], Currency::Inr).unwrap().is_empty());
}

#[test]
fn each_fill_record_is_one_trade_in_journal_order() {
    let records = vec![
        order("O-1", OrderSide::Buy),
        order("O-2", OrderSide::Sell),
        partial("O-1", 4.0, 100.0, 4.0, 10, 11),
        filled("O-2", 10.0, 102.5, 12, 13),
        filled("O-1", 6.0, 101.0, 20, 21),
    ];
    let trades = trades_from_journal(&records, Currency::Inr).unwrap();
    assert_eq!(trades.len(), 3);
    assert_eq!(trades[0].order_id().as_str(), "O-1");
    assert_eq!(trades[0].side(), OrderSide::Buy);
    assert_eq!(trades[0].instrument_id(), &tcs());
    assert_eq!((trades[0].quantity(), trades[0].price()), (4.0, 100.0));
    assert_eq!(trades[1].order_id().as_str(), "O-2");
    assert_eq!(trades[1].side(), OrderSide::Sell);
    assert_eq!((trades[1].quantity(), trades[1].price()), (10.0, 102.5));
    assert_eq!(trades[2].order_id().as_str(), "O-1");
    assert_eq!(trades[2].quantity(), 6.0);
}

#[test]
fn timestamps_come_from_the_event_and_the_envelope() {
    let records = vec![
        order("O-1", OrderSide::Buy),
        filled("O-1", 10.0, 100.0, 77, 88),
    ];
    let trade = &trades_from_journal(&records, Currency::Inr).unwrap()[0];
    assert_eq!(trade.ts_event(), ns(77));
    assert_eq!(trade.ts_init(), ns(88));
}

#[test]
fn costs_are_zero_in_the_account_currency() {
    let records = vec![
        order("O-1", OrderSide::Buy),
        filled("O-1", 10.0, 100.0, 1, 2),
    ];
    let trade = &trades_from_journal(&records, Currency::Inr).unwrap()[0];
    assert_eq!(trade.costs().minor(), 0);
    assert_eq!(trade.costs().currency(), Currency::Inr);
}

#[test]
fn records_that_are_not_fills_are_skipped() {
    let quote = QuoteTick::new(tcs(), 100.0, 101.0, 10.0, 20.0, ns(1), ns(1));
    let records = vec![
        Message::new(Event::Quote(quote), ns(1)),
        order("O-1", OrderSide::Buy),
        Message::new(
            Event::OrderAccepted {
                order_id: OrderId::new("O-1"),
                venue_order_id: None,
                ts_event: ns(2),
            },
            ns(2),
        ),
        Message::new(
            Event::OrderCancelled {
                order_id: OrderId::new("O-1"),
                ts_event: ns(3),
            },
            ns(3),
        ),
    ];
    assert!(trades_from_journal(&records, Currency::Inr)
        .unwrap()
        .is_empty());
}

#[test]
fn a_fill_without_its_order_record_is_an_internal_error() {
    let records = vec![filled("O-9", 1.0, 1.0, 1, 2)];
    let err = trades_from_journal(&records, Currency::Inr).unwrap_err();
    assert_eq!(err.code, ErrorCode::InternalError);
    assert_eq!(err.context.unwrap()["reason"], "journal_orphan_fill");
}

#[test]
fn an_order_record_after_its_fill_does_not_resolve_it() {
    let records = vec![filled("O-1", 1.0, 1.0, 1, 2), order("O-1", OrderSide::Buy)];
    let err = trades_from_journal(&records, Currency::Inr).unwrap_err();
    assert_eq!(err.context.unwrap()["reason"], "journal_orphan_fill");
}
