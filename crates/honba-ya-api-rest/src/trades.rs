//! The journal to `TradesResponse` mapping (ADR 0017 decision 7), pure.

use std::collections::HashMap;

use honba_entities::{Currency, Trade};
use honba_messages::{
    ErrorCode, ErrorDetail, Event, InstrumentId, Message, OrderId, OrderSide, UnixNanos,
};
use serde_json::json;

fn internal(reason: &str, message: String) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::InternalError, message).with_context(json!({"reason": reason}))
}

/// One [`Trade`] per `order_partially_filled` / `order_filled` record, in journal order.
///
/// `instrument_id` and `side` come from the run's earlier `order` record with the same
/// `order_id`; a fill without one is `internal_error` / `journal_orphan_fill`. `costs` are
/// zero in `currency` because no fill event carries costs before E4-S1.
pub(crate) fn trades_from_journal(
    records: &[Message],
    currency: Currency,
) -> Result<Vec<Trade>, ErrorDetail> {
    let mut orders: HashMap<&OrderId, (&InstrumentId, OrderSide)> = HashMap::new();
    let mut trades = Vec::new();
    for record in records {
        let (order_id, qty, px, ts_event): (&OrderId, f64, f64, UnixNanos) = match record.event() {
            Event::Order(o) => {
                orders
                    .entry(o.order_id())
                    .or_insert((o.instrument_id(), o.side()));
                continue;
            }
            Event::OrderPartiallyFilled {
                order_id,
                last_qty,
                last_px,
                ts_event,
                ..
            }
            | Event::OrderFilled {
                order_id,
                last_qty,
                last_px,
                ts_event,
            } => (order_id, *last_qty, *last_px, *ts_event),
            _ => continue,
        };
        let Some((instrument, side)) = orders.get(order_id) else {
            return Err(internal(
                "journal_orphan_fill",
                format!("journal fill for order {order_id} has no order record"),
            ));
        };
        let trade = Trade::new(
            order_id.clone(),
            (*instrument).clone(),
            *side,
            qty,
            px,
            currency,
            ts_event,
            record.ts_init(),
        );
        trade
            .validate()
            .map_err(|e| internal("journal_corrupt", format!("journal fill invalid: {e}")))?;
        trades.push(trade);
    }
    Ok(trades)
}
