//! The journal `Event::Fill` -> `Trade` mapping (ADR 0017 decision 7), pure.

use honba_api::wire::Trade;
use honba_api::{ErrorDetail, Event, Message};
use honba_core::Currency;

/// One [`Trade`] per `Event::Fill` record, in journal order.
///
/// A fill record ([`honba_core::FillReport`], aliased [`Trade`] on the wire) already carries
/// its own instrument, side, price, quantity and timestamp, so — unlike the old
/// `honba_messages` journal event model this replaced — no correlation with an earlier order
/// record is needed to resolve a fill's context.
pub(crate) fn trades_from_journal(
    records: &[Message],
    _currency: Currency,
) -> Result<Vec<Trade>, ErrorDetail> {
    Ok(records
        .iter()
        .filter_map(|record| match &record.event {
            Event::Fill(trade) => Some(trade.clone()),
            _ => None,
        })
        .collect())
}
