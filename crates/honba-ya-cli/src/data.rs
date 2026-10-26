use anyhow::{Context, Result, bail};

use std::path::Path;

use honba_feed::{ParquetBarSource, ParquetTickStore, TICK_DIR, write_tick_file};
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
    let qualified = format!("{}:{}", instrument.exchange, instrument.symbol);

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

/// Inspects the catalog under `data_dir` and prints a summary of timeframes, instruments, bar counts, and dates.
pub fn catalog_inspect(data_dir: &Path, timeframe: Option<&str>) -> Result<()> {
    let loader = honba_backtest::catalog::CatalogLoader::new(data_dir);
    let timeframes = loader
        .list_timeframes()
        .with_context(|| format!("listing timeframes under {}", data_dir.display()))?;

    if timeframes.is_empty() {
        println!("No timeframes found in catalog at {}", data_dir.display());
        return Ok(());
    }

    println!("Catalog at: {}", data_dir.display());
    println!("Available timeframes: {}", timeframes.join(", "));

    let target_tfs: Vec<&str> = if let Some(tf) = timeframe {
        vec![tf]
    } else {
        timeframes.iter().map(|s| s.as_str()).collect()
    };

    for tf in target_tfs {
        println!("\n=== Timeframe: {tf} ===");
        let instruments = loader
            .list_instruments(tf)
            .with_context(|| format!("listing instruments for timeframe {tf}"))?;

        if instruments.is_empty() {
            println!("  No instruments found.");
            continue;
        }

        println!("  Instruments ({}):", instruments.len());
        for inst in &instruments {
            match loader.load_bars(inst, tf, None, None) {
                Ok(bars) => {
                    if let (Some(first), Some(last)) = (bars.first(), bars.last()) {
                        println!(
                            "    - {}: {} bars [{} to {}] first_close={} last_close={}",
                            inst,
                            bars.len(),
                            first.timestamp.format("%Y-%m-%d %H:%M:%S"),
                            last.timestamp.format("%Y-%m-%d %H:%M:%S"),
                            first.close.0,
                            last.close.0,
                        );
                    } else {
                        println!("    - {}: 0 bars", inst);
                    }
                }
                Err(e) => {
                    println!("    - {}: error loading bars ({e})", inst);
                }
            }
        }
    }
    Ok(())
}

/// Verifies catalog data integrity: schema conformance, timestamp monotonicity, no duplicate timestamps,
/// and OHLC relationships (low <= open <= high, low <= close <= high, volume >= 0).
pub fn catalog_verify(data_dir: &Path, timeframe: Option<&str>) -> Result<()> {
    let loader = honba_backtest::catalog::CatalogLoader::new(data_dir);
    let timeframes = loader
        .list_timeframes()
        .with_context(|| format!("listing timeframes under {}", data_dir.display()))?;

    if timeframes.is_empty() {
        bail!(
            "No timeframes found to verify in catalog at {}",
            data_dir.display()
        );
    }

    let target_tfs: Vec<&str> = if let Some(tf) = timeframe {
        vec![tf]
    } else {
        timeframes.iter().map(|s| s.as_str()).collect()
    };

    let mut total_bars = 0usize;
    let mut total_instruments = 0usize;
    let mut anomalies = Vec::new();

    for tf in &target_tfs {
        let instruments = loader.list_instruments(tf)?;
        for inst in &instruments {
            total_instruments += 1;
            let bars = loader
                .load_bars(inst, tf, None, None)
                .with_context(|| format!("loading bars for {inst} at {tf}"))?;

            if bars.is_empty() {
                anomalies.push(format!("{inst} ({tf}): empty partition"));
                continue;
            }

            total_bars += bars.len();
            let mut prev_ts = None;

            for (idx, b) in bars.iter().enumerate() {
                // Timestamp monotonicity
                if let Some(prev) = prev_ts {
                    if b.timestamp <= prev {
                        anomalies.push(format!(
                            "{inst} ({tf}) row {idx}: retrograde timestamp {} <= prev {}",
                            b.timestamp, prev
                        ));
                    }
                }
                prev_ts = Some(b.timestamp);

                // OHLC relationship check
                if b.low.0 > b.high.0 {
                    anomalies.push(format!(
                        "{inst} ({tf}) row {idx}: low {} > high {}",
                        b.low.0, b.high.0
                    ));
                }
                if b.open.0 < b.low.0 || b.open.0 > b.high.0 {
                    anomalies.push(format!(
                        "{inst} ({tf}) row {idx}: open {} out of bounds [{}, {}]",
                        b.open.0, b.low.0, b.high.0
                    ));
                }
                if b.close.0 < b.low.0 || b.close.0 > b.high.0 {
                    anomalies.push(format!(
                        "{inst} ({tf}) row {idx}: close {} out of bounds [{}, {}]",
                        b.close.0, b.low.0, b.high.0
                    ));
                }
                if b.volume.0 < rust_decimal::Decimal::ZERO {
                    anomalies.push(format!(
                        "{inst} ({tf}) row {idx}: negative volume {}",
                        b.volume.0
                    ));
                }
            }
        }
    }

    if anomalies.is_empty() {
        println!(
            "Catalog verification PASSED: {} instruments, {} total bars verified across {} timeframe(s). Zero anomalies detected.",
            total_instruments,
            total_bars,
            target_tfs.len()
        );
        Ok(())
    } else {
        eprintln!(
            "Catalog verification FAILED with {} anomalies:",
            anomalies.len()
        );
        for a in anomalies.iter().take(20) {
            eprintln!("  - {a}");
        }
        if anomalies.len() > 20 {
            eprintln!("  ... and {} more", anomalies.len() - 20);
        }
        bail!("Catalog verification found {} anomalies", anomalies.len());
    }
}
