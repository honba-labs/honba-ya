use anyhow::{bail, Context, Result};

use std::path::Path;

use honba_feed::{write_tick_file, ParquetBarSource, ParquetTickStore, TICK_DIR};
use honba_market::india::exchange::resolve_instrument;

/// Reads an NDJSON ticks file (ADR 0030) and writes it as `{out}/ticks/ticks.parquet`, so a
/// run can match against the same ticks without re-parsing the file at every start-up.
///
/// The store is read back before reporting success, so a silently unreadable file is an error
/// rather than a later `l2_requires_ticks`.
pub fn ticks_import(ticks: &Path, out: &Path) -> Result<()> {
    let text =
        std::fs::read_to_string(ticks).with_context(|| format!("reading {}", ticks.display()))?;
    let reader = crate::ticks_file::parse(&text)
        .with_context(|| format!("invalid ticks file {}", ticks.display()))?;
    let path = out.join(TICK_DIR).join("ticks.parquet");
    write_tick_file(&path, reader.quotes(), reader.trades())
        .with_context(|| format!("writing {}", path.display()))?;
    let store = ParquetTickStore::open(out)
        .with_context(|| format!("reading back the tick store {}", out.display()))?;
    let (quotes, trades) = store.len();
    println!(
        "{} quote and {} trade ticks imported into {}",
        quotes,
        trades,
        path.display()
    );
    Ok(())
}

pub fn load(source: &str, symbol: &str, exchange: Option<&str>) -> Result<()> {
    let instrument = resolve_instrument(symbol, exchange)?;
    let qualified = format!("{}:{}", instrument.exchange(), instrument.symbol());

    let bars = if source.to_ascii_lowercase().ends_with(".parquet") {
        ParquetBarSource::new(source, instrument)
            .bars()
            .with_context(|| format!("reading {source}"))?
    } else {
        bail!("unsupported source extension: {source} (expected .parquet)");
    };

    println!("{} bars loaded from {source} ({qualified})", bars.len());
    if let (Some(first), Some(last)) = (bars.first(), bars.last()) {
        println!(
            "first: ts_event={} close={}",
            first.ts_event(),
            first.close()
        );
        println!("last:  ts_event={} close={}", last.ts_event(), last.close());
    }
    Ok(())
}
