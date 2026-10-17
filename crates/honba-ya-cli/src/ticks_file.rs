//! The `--ticks` file: versioned NDJSON of quote and trade ticks (ADR 0030, "Tick file format").
//!
//! Line 1 is the header `{"honba_ticks":1}`. Every later non-blank line is one tick:
//!
//! ```text
//! {"kind":"quote","symbol":"TCS","exchange":"NSE","ts_event":1704067200000000000,"bid_price":99.9,"ask_price":100.1,"bid_size":500,"ask_size":400}
//! {"kind":"trade","symbol":"TCS","exchange":"NSE","ts_event":1704067200000000000,"price":100.0,"size":50,"aggressor":"buyer","trade_id":"T-1"}
//! ```
//!
//! `ts_event` is unix nanoseconds as a JSON integer; `ts_init` is optional and defaults to
//! `ts_event`; `aggressor` is `buyer`, `seller` or `no_aggressor`. Unknown fields are refused.
//! Ticks may come in any order (the reader sorts, stable for ties). Every error names its line.

use anyhow::{bail, Context, Result};
use honba_feed::InMemoryTickReader;
use honba_messages::{AggressorSide, Exchange, InstrumentId, QuoteTick, TradeTick, UnixNanos};
use serde::Deserialize;
use std::path::Path;

/// The only format version this build reads.
pub(crate) const VERSION: u64 = 1;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    honba_ticks: u64,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Line {
    Quote {
        symbol: String,
        exchange: String,
        ts_event: u64,
        ts_init: Option<u64>,
        bid_price: f64,
        ask_price: f64,
        bid_size: f64,
        ask_size: f64,
    },
    Trade {
        symbol: String,
        exchange: String,
        ts_event: u64,
        ts_init: Option<u64>,
        price: f64,
        size: f64,
        aggressor: AggressorSide,
        trade_id: String,
    },
}

fn instrument(symbol: String, exchange: String) -> serde_json::Value {
    serde_json::to_value(InstrumentId::new(symbol, Exchange::new(exchange)))
        .expect("an instrument id serializes")
}

fn nanos(n: u64) -> serde_json::Value {
    serde_json::to_value(UnixNanos::from_u64(n)).expect("a timestamp serializes")
}

/// Parses the text of a ticks file into a reader. Errors carry the 1-based line number.
pub(crate) fn parse(text: &str) -> Result<InMemoryTickReader> {
    let mut lines = text
        .lines()
        .enumerate()
        .map(|(i, l)| (i + 1, l))
        .filter(|(_, l)| !l.trim().is_empty());
    let Some((n, head)) = lines.next() else {
        bail!("ticks file is empty; expected the header {{\"honba_ticks\":{VERSION}}}");
    };
    let header: Header = serde_json::from_str(head)
        .with_context(|| format!("line {n}: expected the header {{\"honba_ticks\":{VERSION}}}"))?;
    if header.honba_ticks != VERSION {
        bail!(
            "line {n}: unsupported ticks file version {}; this build reads version {VERSION}",
            header.honba_ticks
        );
    }
    let (mut quotes, mut trades) = (Vec::new(), Vec::new());
    for (n, raw) in lines {
        let line: Line = serde_json::from_str(raw).with_context(|| format!("line {n}"))?;
        match line {
            Line::Quote {
                symbol,
                exchange,
                ts_event,
                ts_init,
                bid_price,
                ask_price,
                bid_size,
                ask_size,
            } => {
                // Through the wire type, so its invariants (finite, sizes >= 0, not crossed)
                // are checked once, in honba-messages, and `QuoteTick::new` never panics.
                let tick: QuoteTick = serde_json::from_value(serde_json::json!({
                    "instrument_id": instrument(symbol, exchange),
                    "bid_price": bid_price,
                    "ask_price": ask_price,
                    "bid_size": bid_size,
                    "ask_size": ask_size,
                    "ts_event": nanos(ts_event),
                    "ts_init": nanos(ts_init.unwrap_or(ts_event)),
                }))
                .with_context(|| format!("line {n}: invalid quote"))?;
                quotes.push(tick);
            }
            Line::Trade {
                symbol,
                exchange,
                ts_event,
                ts_init,
                price,
                size,
                aggressor,
                trade_id,
            } => {
                let tick: TradeTick = serde_json::from_value(serde_json::json!({
                    "instrument_id": instrument(symbol, exchange),
                    "price": price,
                    "size": size,
                    "aggressor_side": aggressor,
                    "trade_id": trade_id,
                    "ts_event": nanos(ts_event),
                    "ts_init": nanos(ts_init.unwrap_or(ts_event)),
                }))
                .with_context(|| format!("line {n}: invalid trade"))?;
                trades.push(tick);
            }
        }
    }
    if quotes.is_empty() && trades.is_empty() {
        bail!("ticks file holds a header but no ticks");
    }
    Ok(InMemoryTickReader::new()
        .with_quotes(quotes)
        .with_trades(trades))
}

/// Reads and validates a `--ticks` file. Errors carry the path and the line.
pub(crate) fn load(path: &Path) -> Result<InMemoryTickReader> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading --ticks {}", path.display()))?;
    parse(&text).with_context(|| format!("invalid --ticks {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use honba_ports::{TickReader, TickRequest};

    const HEAD: &str = "{\"honba_ticks\":1}\n";
    const QUOTE: &str = "{\"kind\":\"quote\",\"symbol\":\"TCS\",\"exchange\":\"NSE\",\"ts_event\":200,\"bid_price\":99.9,\"ask_price\":100.1,\"bid_size\":500,\"ask_size\":400}\n";
    const TRADE: &str = "{\"kind\":\"trade\",\"symbol\":\"TCS\",\"exchange\":\"NSE\",\"ts_event\":100,\"ts_init\":150,\"price\":100.0,\"size\":50,\"aggressor\":\"buyer\",\"trade_id\":\"T-1\"}\n";

    fn tcs() -> InstrumentId {
        InstrumentId::new("TCS", Exchange::new("NSE"))
    }

    fn err(text: &str) -> String {
        format!("{:#}", parse(text).expect_err("expected an error"))
    }

    #[tokio::test]
    async fn parses_quotes_and_trades_and_defaults_ts_init() {
        let reader = parse(&format!("{HEAD}\n{QUOTE}{TRADE}")).unwrap();
        let req = TickRequest::new(tcs(), None, None).unwrap();
        let quotes = reader.read_quotes(&req).await.unwrap();
        let trades = reader.read_trades(&req).await.unwrap();
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].bid_price(), 99.9);
        assert_eq!(quotes[0].ts_init(), UnixNanos::from_u64(200));
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].ts_init(), UnixNanos::from_u64(150));
        assert_eq!(trades[0].aggressor_side(), AggressorSide::Buyer);
    }

    #[test]
    fn rejects_a_missing_or_wrong_header_with_the_line() {
        assert!(err("").contains("empty"));
        assert!(err(QUOTE).contains("line 1"));
        let e = err("{\"honba_ticks\":2}\n");
        assert!(e.contains("line 1") && e.contains("version 2"), "{e}");
        assert!(err(HEAD).contains("no ticks"));
    }

    #[test]
    fn a_bad_line_names_its_line_number() {
        let e = err(&format!("{HEAD}{QUOTE}not json\n"));
        assert!(e.contains("line 3"), "{e}");
        let e = err(&format!("{HEAD}\n{QUOTE}{{\"kind\":\"depth\"}}\n"));
        assert!(e.contains("line 4"), "{e}");
    }

    #[test]
    fn unknown_fields_and_invariant_violations_are_refused() {
        let extra = QUOTE.replace("\"ts_event\":200", "\"ts_event\":200,\"bogus\":1");
        assert!(err(&format!("{HEAD}{extra}")).contains("line 2"));
        let crossed = QUOTE.replace("\"bid_price\":99.9", "\"bid_price\":101.0");
        let e = err(&format!("{HEAD}{crossed}"));
        assert!(e.contains("line 2") && e.contains("invalid quote"), "{e}");
        let neg = TRADE.replace("\"size\":50", "\"size\":-1");
        assert!(err(&format!("{HEAD}{neg}")).contains("invalid trade"));
    }
}
