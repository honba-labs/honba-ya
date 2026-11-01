//! Versioned NDJSON tick parser/importer (ADR 0030) and Parquet bar summary helpers.
//!
//! Line 1 of a ticks file is the header `{"honba_ticks":1}`. Every later non-blank line is one
//! tick:
//!
//! ```text
//! {"kind":"quote","symbol":"TCS","exchange":"NSE","ts_event":1704067200000000000,"bid_price":99.9,"ask_price":100.1,"bid_size":500,"ask_size":400}
//! {"kind":"trade","symbol":"TCS","exchange":"NSE","ts_event":1704067200000000000,"price":100.0,"size":50,"aggressor":"buyer","trade_id":"T-1"}
//! ```
//!
//! `ts_event` is unix nanoseconds as a JSON integer; `ts_init` is optional and defaults to
//! `ts_event`; `aggressor` is `buyer`, `seller` or `no_aggressor`. Unknown fields are refused.
//! Ticks may come in any order (the reader sorts, stable for ties). Every error names its line.

use std::path::Path;

use anyhow::{Context, Result, bail};
use honba_analytics::RoundTrip;
use honba_core::{Exchange, FillReport, InstrumentId};
use honba_feed::wire::{AggressorSide, QuoteTick, TradeTick, UnixNanos};
use honba_feed::{
    InMemoryTickReader, ParquetBarSource, ParquetTickStore, TICK_DIR, write_tick_file,
};
use honba_market::india::exchange::resolve_instrument;
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;

/// The only NDJSON ticks format version supported by this build (ADR 0030).
pub const VERSION: u64 = 1;

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

fn instrument(symbol: String, exchange: String) -> Result<serde_json::Value> {
    let exchange: Exchange = exchange
        .parse()
        .map_err(|_| anyhow::anyhow!("unknown exchange {exchange:?}; expected NSE, BSE or MCX"))?;
    Ok(serde_json::to_value(InstrumentId::new(exchange, symbol))
        .expect("an instrument id serializes"))
}

fn nanos(n: u64) -> serde_json::Value {
    serde_json::to_value(UnixNanos::from_u64(n)).expect("a timestamp serializes")
}

/// Parses the text of an ADR 0030 NDJSON ticks file into an [`InMemoryTickReader`].
///
/// Errors carry the 1-based line number where parsing or invariant validation failed.
pub fn parse_ticks_ndjson(text: &str) -> Result<InMemoryTickReader> {
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
                // `honba_feed::wire::QuoteTick`'s custom `Deserialize` refuses crossed quotes
                // (bid > ask), while non-negative finite sizes are enforced here.
                if !bid_size.is_finite()
                    || bid_size < 0.0
                    || !ask_size.is_finite()
                    || ask_size < 0.0
                {
                    bail!("line {n}: invalid quote: sizes must be finite and >= 0");
                }
                let tick: QuoteTick = serde_json::from_value(serde_json::json!({
                    "instrument_id": instrument(symbol, exchange).with_context(|| format!("line {n}"))?,
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
                if !size.is_finite() || size < 0.0 {
                    bail!("line {n}: invalid trade: size must be finite and >= 0");
                }
                let tick: TradeTick = serde_json::from_value(serde_json::json!({
                    "instrument_id": instrument(symbol, exchange).with_context(|| format!("line {n}"))?,
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

/// Reads and validates a `--ticks` NDJSON file from disk. Errors carry both the path and line.
pub fn load_ticks_ndjson(path: &Path) -> Result<InMemoryTickReader> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading --ticks {}", path.display()))?;
    parse_ticks_ndjson(&text).with_context(|| format!("invalid --ticks {}", path.display()))
}

/// Imports an ADR 0030 NDJSON ticks file into `{out}/ticks/ticks.parquet` and reads it back via
/// [`ParquetTickStore::open`] before returning `(quote_count, trade_count)`.
pub fn import_ticks_ndjson(ticks: &Path, out: &Path) -> Result<(usize, usize)> {
    let text =
        std::fs::read_to_string(ticks).with_context(|| format!("reading {}", ticks.display()))?;
    let reader = parse_ticks_ndjson(&text)
        .with_context(|| format!("invalid ticks file {}", ticks.display()))?;
    let path = out.join(TICK_DIR).join("ticks.parquet");
    write_tick_file(&path, reader.quotes(), reader.trades())
        .with_context(|| format!("writing {}", path.display()))?;
    let store = ParquetTickStore::open(out)
        .with_context(|| format!("reading back the tick store {}", out.display()))?;
    Ok(store.len())
}

/// Summary of a single Parquet bar file loaded via [`load_parquet_bars_summary`].
#[derive(Debug, Clone, PartialEq)]
pub struct ParquetBarsSummary {
    /// Canonical `EXCHANGE:SYMBOL` string.
    pub qualified_instrument: String,
    /// Number of bars loaded.
    pub bar_count: usize,
    /// First bar `(ts_event, close)` when non-empty.
    pub first: Option<(UnixNanos, f64)>,
    /// Last bar `(ts_event, close)` when non-empty.
    pub last: Option<(UnixNanos, f64)>,
}

/// Resolves `symbol` / `exchange`, verifies `.parquet` extension, and loads a summary of bars.
pub fn load_parquet_bars_summary(
    source: &str,
    symbol: &str,
    exchange: Option<&str>,
) -> Result<ParquetBarsSummary> {
    let instrument = resolve_instrument(symbol, exchange)?;
    let qualified_instrument = format!("{}:{}", instrument.exchange, instrument.symbol);

    let bars = if source.to_ascii_lowercase().ends_with(".parquet") {
        ParquetBarSource::new(source, instrument)
            .bars()
            .with_context(|| format!("reading {source}"))?
    } else {
        bail!("unsupported source extension: {source} (expected .parquet)");
    };

    let first = bars.first().map(|b| (b.ts_event(), b.close()));
    let last = bars.last().map(|b| (b.ts_event(), b.close()));
    Ok(ParquetBarsSummary {
        qualified_instrument,
        bar_count: bars.len(),
        first,
        last,
    })
}

/// Pairs consecutive opposite-side [`FillReport`]s into [`RoundTrip`] records.
pub fn pair_fills(fills: &[FillReport]) -> Vec<RoundTrip> {
    let mut trips = Vec::new();
    let mut i = 0;
    while i + 1 < fills.len() {
        if let Ok(rt) = RoundTrip::from_fills(&fills[i], &fills[i + 1]) {
            trips.push(rt);
        }
        i += 2;
    }
    trips
}

/// Builds a cumulative equity curve starting from `starting` across `trips`.
pub fn equity_curve(starting: f64, trips: &[RoundTrip]) -> Vec<f64> {
    let mut equity = vec![starting];
    let mut cur = starting;
    for t in trips {
        cur += t.net_pnl.to_f64().unwrap_or(0.0);
        equity.push(cur);
    }
    equity
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int64Array};
    use arrow::record_batch::RecordBatch;
    use chrono::{DateTime, Utc};
    use honba_core::{OrderSide, Price, Quantity};
    use honba_feed::import::parquet_source::bar_schema;
    use honba_feed::ports::{TickReader, TickRequest};
    use parquet::arrow::ArrowWriter;
    use rust_decimal::Decimal;
    use std::fs;
    use std::sync::Arc;
    use uuid::Uuid;

    const HEAD: &str = "{\"honba_ticks\":1}\n";
    const QUOTE: &str = "{\"kind\":\"quote\",\"symbol\":\"TCS\",\"exchange\":\"NSE\",\"ts_event\":200,\"bid_price\":99.9,\"ask_price\":100.1,\"bid_size\":500,\"ask_size\":400}\n";
    const TRADE: &str = "{\"kind\":\"trade\",\"symbol\":\"TCS\",\"exchange\":\"NSE\",\"ts_event\":100,\"ts_init\":150,\"price\":100.0,\"size\":50,\"aggressor\":\"buyer\",\"trade_id\":\"T-1\"}\n";

    fn tcs() -> InstrumentId {
        InstrumentId::new(Exchange::Nse, "TCS")
    }

    fn err(text: &str) -> String {
        format!(
            "{:#}",
            parse_ticks_ndjson(text).expect_err("expected an error")
        )
    }

    #[tokio::test]
    async fn parses_quotes_and_trades_and_defaults_ts_init() {
        let reader = parse_ticks_ndjson(&format!("{HEAD}\n{QUOTE}{TRADE}")).unwrap();
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

    #[test]
    fn load_rejects_non_parquet_sources_before_touching_the_file_system() {
        let err = load_parquet_bars_summary("does/not/exist.csv", "TCS", None).unwrap_err();
        assert!(
            err.to_string().contains("unsupported source extension"),
            "{err}"
        );
    }

    #[test]
    fn load_names_the_file_when_it_cannot_be_read() {
        let err = load_parquet_bars_summary("does/not/exist.parquet", "TCS", None).unwrap_err();
        assert!(err.to_string().contains("does/not/exist.parquet"), "{err}");
    }

    #[test]
    fn load_accepts_an_upper_case_parquet_extension() {
        let err = load_parquet_bars_summary("does/not/exist.PARQUET", "TCS", None).unwrap_err();
        assert!(
            !err.to_string().contains("unsupported source extension"),
            "{err}"
        );
    }

    #[test]
    fn load_rejects_a_conflicting_qualifier_before_touching_the_file_system() {
        let err = load_parquet_bars_summary("does/not/exist.parquet", "NSE:TCS", Some("BSE"))
            .unwrap_err();
        assert!(err.to_string().contains("conflicts"), "{err}");
    }

    #[test]
    fn load_rejects_an_unknown_exchange() {
        let err =
            load_parquet_bars_summary("does/not/exist.parquet", "TCS", Some("NYSE")).unwrap_err();
        assert!(err.to_string().contains("NSE, BSE"), "{err}");
    }

    fn write_bars_parquet(path: &Path) {
        let schema = Arc::new(bar_schema());
        let col = |v: f64| -> ArrayRef { Arc::new(Float64Array::from(vec![v; 2])) };
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![60_000_000_000_i64, 120_000_000_000])),
            col(10.0),
            col(12.0),
            col(9.0),
            col(11.0),
            col(100.0),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer =
            ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn load_summarises_parquet_file_and_honours_exchange_qualifier() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("TCS.NSE.parquet");
        write_bars_parquet(&file);

        let summary = load_parquet_bars_summary(file.to_str().unwrap(), "TCS", None).unwrap();
        assert_eq!(summary.qualified_instrument, "NSE:TCS");
        assert_eq!(summary.bar_count, 2);
        assert_eq!(summary.first.unwrap().1, 11.0);

        let bse = load_parquet_bars_summary(file.to_str().unwrap(), "TCS", Some("bse")).unwrap();
        assert_eq!(bse.qualified_instrument, "BSE:TCS");

        let qualified = load_parquet_bars_summary(file.to_str().unwrap(), "BSE:TCS", None).unwrap();
        assert_eq!(qualified.qualified_instrument, "BSE:TCS");
    }

    fn any_instrument() -> InstrumentId {
        InstrumentId::new(Exchange::Nse, "X")
    }

    fn ts(n: u64) -> DateTime<Utc> {
        DateTime::from_timestamp(0, n as u32).expect("fixture timestamp in range")
    }

    fn fill(side: OrderSide, price: f64, n: u64) -> FillReport {
        FillReport::new(
            format!("F-{n}"),
            format!("O-{n}"),
            Uuid::new_v4(),
            any_instrument(),
            side,
            Price::new(Decimal::from_f64_retain(price).expect("fixture price is finite")),
            Quantity::from_u64(1),
            "test-algo",
            ts(n),
        )
    }

    #[test]
    fn pair_fills_pairs_consecutive_entry_exit_fills() {
        let fills = [
            fill(OrderSide::Buy, 100.0, 1),
            fill(OrderSide::Sell, 105.0, 2),
            fill(OrderSide::Sell, 110.0, 3),
            fill(OrderSide::Buy, 104.0, 4),
        ];
        let trips = pair_fills(&fills);
        let pnl: Vec<f64> = trips.iter().map(|t| t.net_pnl.to_f64().unwrap()).collect();
        assert_eq!(pnl, [5.0, 6.0]);
    }

    #[test]
    fn pair_fills_skips_same_side_pairs_and_a_trailing_fill() {
        let fills = [
            fill(OrderSide::Buy, 100.0, 1),
            fill(OrderSide::Buy, 101.0, 2),
            fill(OrderSide::Buy, 102.0, 3),
        ];
        assert!(pair_fills(&fills).is_empty());
        assert!(pair_fills(&[]).is_empty());
    }

    #[test]
    fn equity_curve_accumulates_net_pnl_from_the_start() {
        let trips = pair_fills(&[
            fill(OrderSide::Buy, 100.0, 1),
            fill(OrderSide::Sell, 90.0, 2),
            fill(OrderSide::Buy, 100.0, 3),
            fill(OrderSide::Sell, 130.0, 4),
        ]);
        assert_eq!(equity_curve(1_000.0, &trips), [1_000.0, 990.0, 1_020.0]);
        assert_eq!(equity_curve(1_000.0, &[]), [1_000.0]);
    }
}
