//! The journal `Event::Fill` -> `Trade` mapping (ADR 0017 decision 7).
//!
//! The old `honba_messages` journal model needed an `Order` record to resolve a fill's
//! instrument and side, and treated an unresolved fill as `journal_orphan_fill`
//! (`internal_error`). The current `honba_api::wire::Event::Fill` carries a complete
//! [`honba_core::FillReport`] directly, so there is nothing left to resolve and no orphan-fill
//! failure mode; these tests cover the simpler mapping instead.

use chrono::{TimeZone, Utc};
use honba_api::wire::QuoteTick;
use honba_api::{Event, Message};
use honba_core::rust_decimal::Decimal;
use honba_core::uuid::Uuid;
use honba_core::{Currency, Exchange, FillReport, InstrumentId, OrderSide, Price, Quantity};

use crate::trades::trades_from_journal;

fn tcs() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "TCS")
}

fn ts(n: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(n, 0).unwrap()
}

fn fill(order_id: &str, side: OrderSide, qty: u64, px: u64, at: i64) -> Message {
    let trade = FillReport::new(
        format!("F-{order_id}"),
        order_id,
        Uuid::new_v4(),
        tcs(),
        side,
        Price::new(Decimal::from(px)),
        Quantity::from_u64(qty),
        "ALGO1",
        ts(at),
    );
    Message::new(Event::Fill(trade), ts(at))
}

#[test]
fn an_empty_journal_has_no_trades() {
    assert!(trades_from_journal(&[], Currency::Inr).unwrap().is_empty());
}

#[test]
fn each_fill_record_is_one_trade_in_journal_order() {
    let records = vec![
        fill("O-1", OrderSide::Buy, 4, 100, 10),
        fill("O-2", OrderSide::Sell, 10, 102, 12),
        fill("O-1", OrderSide::Buy, 6, 101, 20),
    ];
    let trades = trades_from_journal(&records, Currency::Inr).unwrap();
    assert_eq!(trades.len(), 3);
    assert_eq!(trades[0].order_id, "O-1");
    assert_eq!(trades[0].side, OrderSide::Buy);
    assert_eq!(trades[0].instrument, tcs());
    assert_eq!(trades[0].quantity, Quantity::from_u64(4));
    assert_eq!(trades[0].price, Price::new(Decimal::from(100)));
    assert_eq!(trades[1].order_id, "O-2");
    assert_eq!(trades[1].side, OrderSide::Sell);
    assert_eq!(trades[2].order_id, "O-1");
    assert_eq!(trades[2].quantity, Quantity::from_u64(6));
}

#[test]
fn timestamps_come_from_the_fill_itself() {
    let records = vec![fill("O-1", OrderSide::Buy, 10, 100, 77)];
    let trade = &trades_from_journal(&records, Currency::Inr).unwrap()[0];
    assert_eq!(trade.timestamp, ts(77));
}

#[test]
fn records_that_are_not_fills_are_skipped() {
    let quote = QuoteTick::new(
        tcs(),
        Decimal::from(100),
        Decimal::from(101),
        Decimal::from(10),
        Decimal::from(20),
        ts(1),
        ts(1),
    );
    let records = vec![
        Message::new(Event::Quote(quote), ts(1)),
        fill("O-1", OrderSide::Buy, 10, 100, 2),
    ];
    let trades = trades_from_journal(&records, Currency::Inr).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].order_id, "O-1");
}
