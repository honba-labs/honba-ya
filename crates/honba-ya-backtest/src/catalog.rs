//! Zero-copy Arrow/Parquet Lakehouse catalog loader for `honba-ya-backtest`.
//!
//! Reads historical bars directly from `honba-data/data/catalog`
//! (`{root}/catalog/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet` or
//! `{catalog_dir}/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet`) as well as Lakehouse v2
//! Hive-partitioned directories (`{root}/bars/timeframe={tf}/exchange={EXCHANGE}/...`),
//! streaming Arrow [`RecordBatch`]es directly into canonical [`honba_core::Bar`]
//! and [`MarketEvent`] sequences with exact `rust_decimal::Decimal` conversion.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use arrow::array::{
    Array, Decimal128Array, Float64Array, Int64Array, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
use thiserror::Error;

use honba_core::{
    Bar, BarReader, BarRequest, Exchange, InstrumentId, PortError, PortResult, Price, Quantity,
    Timeframe,
};

use crate::events::MarketEvent;

/// Errors raised while loading historical bars from the Parquet catalog.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Filesystem I/O failure.
    #[error("catalog io error at {path}: {source}")]
    Io {
        /// Path being accessed.
        path: PathBuf,
        /// Underlying OS error.
        source: std::io::Error,
    },
    /// Parquet decoding failure.
    #[error("parquet error in {path}: {source}")]
    Parquet {
        /// Parquet file path.
        path: PathBuf,
        /// Underlying Parquet error.
        source: Box<parquet::errors::ParquetError>,
    },
    /// Arrow record-batch decoding failure.
    #[error("arrow error in {path}: {source}")]
    Arrow {
        /// Parquet file path.
        path: PathBuf,
        /// Underlying Arrow error.
        source: Box<arrow::error::ArrowError>,
    },
    /// Missing required column in Parquet file.
    #[error("missing required column `{column}` in {path}")]
    MissingColumn {
        /// Parquet file path.
        path: PathBuf,
        /// Column name.
        column: &'static str,
    },
    /// Unsupported column data type in Parquet file.
    #[error("unsupported data type {data_type:?} for column `{column}` in {path}")]
    UnsupportedColumnType {
        /// Parquet file path.
        path: PathBuf,
        /// Column name.
        column: &'static str,
        /// Actual Arrow data type.
        data_type: Box<DataType>,
    },
    /// Invalid or non-finite numeric value in row.
    #[error("invalid value in {path} row {row} column `{column}`: {reason}")]
    InvalidRow {
        /// Parquet file path.
        path: Box<Path>,
        /// Row index within batch.
        row: usize,
        /// Column name.
        column: &'static str,
        /// Explanation.
        reason: Box<str>,
    },
    /// Requested timeframe or instrument partition does not exist.
    #[error(
        "partition not found for instrument {instrument} at timeframe {timeframe} under {root}"
    )]
    NotFound {
        /// Catalog root searched.
        root: PathBuf,
        /// Instrument requested.
        instrument: Box<InstrumentId>,
        /// Timeframe requested.
        timeframe: Box<str>,
    },
}

impl From<CatalogError> for PortError {
    fn from(err: CatalogError) -> Self {
        match err {
            CatalogError::NotFound { .. } => PortError::NotFound(err.to_string()),
            other => PortError::ConnectionFailed(other.to_string()),
        }
    }
}

/// Zero-copy Arrow/Parquet reader over `honba-data/data/catalog`.
#[derive(Debug, Clone)]
pub struct CatalogLoader {
    root: PathBuf,
}

impl CatalogLoader {
    /// Create a catalog loader rooted at `root`.
    ///
    /// `root` may point either to the data directory containing `catalog/`
    /// (e.g. `.../honba-data/data`) or directly to the `catalog` directory itself
    /// (e.g. `.../honba-data/data/catalog`).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Resolve the effective `catalog` directory path.
    pub fn catalog_dir(&self) -> PathBuf {
        let candidate = self.root.join("catalog");
        if candidate.is_dir() {
            candidate
        } else {
            self.root.clone()
        }
    }

    /// List all timeframe directory names present in the catalog, sorted alphabetically.
    pub fn list_timeframes(&self) -> Result<Vec<String>, CatalogError> {
        let cat = self.catalog_dir();
        let mut out = Vec::new();
        for dir in sorted_subdirs(&cat)? {
            if let Some(name) = dir.file_name().map(|n| n.to_string_lossy().into_owned()) {
                out.push(name);
            }
        }
        Ok(out)
    }

    /// List all instruments available under a given timeframe (case-insensitive, e.g. `"1D"` or `"5m"`).
    pub fn list_instruments(&self, timeframe: &str) -> Result<Vec<InstrumentId>, CatalogError> {
        let Some(tf_dir) = self.find_timeframe_dir(timeframe)? else {
            return Ok(Vec::new());
        };
        let mut instruments = Vec::new();
        for exch_dir in sorted_subdirs(&tf_dir)? {
            let exch_name = file_name(&exch_dir);
            let Ok(exchange) = exch_name.parse::<Exchange>() else {
                continue;
            };
            for sym_dir in sorted_subdirs(&exch_dir)? {
                let symbol = file_name(&sym_dir);
                if !symbol.is_empty() {
                    instruments.push(InstrumentId::new(exchange, symbol));
                }
            }
        }
        Ok(instruments)
    }

    /// Load chronological [`Bar`]s for `instrument` at `timeframe` (e.g. `"1D"`, `"5M"`),
    /// optionally bounded by inclusive `from` and `to` UTC timestamps.
    ///
    /// Partition files (`2014.parquet`, ..., `2026.parquet`) are read in sorted order
    /// via Arrow record-batch readers, deduplicating any overlapping boundary timestamps.
    pub fn load_bars(
        &self,
        instrument: &InstrumentId,
        timeframe: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<Bar>, CatalogError> {
        let files = self.locate_partition_files(instrument, timeframe)?;
        if files.is_empty() {
            return Err(CatalogError::NotFound {
                root: self.catalog_dir(),
                instrument: Box::new(instrument.clone()),
                timeframe: timeframe.into(),
            });
        }

        let mut bars = Vec::new();
        let mut last_ts: Option<DateTime<Utc>> = None;

        for path in files {
            read_parquet_bars_into(&path, instrument, from, to, &mut last_ts, &mut bars)?;
        }

        Ok(bars)
    }

    /// Load and chronologically interleave [`MarketEvent::Bar`] events across multiple
    /// instruments for direct feeding into [`crate::BacktestEngine::run_events`].
    pub fn load_market_events(
        &self,
        instruments: &[InstrumentId],
        timeframe: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<MarketEvent>, CatalogError> {
        let mut all_bars = Vec::new();
        for inst in instruments {
            let inst_bars = self.load_bars(inst, timeframe, from, to)?;
            all_bars.extend(inst_bars);
        }
        // Deterministic sort by (timestamp, exchange, symbol)
        all_bars.sort_by(|a, b| {
            a.timestamp
                .cmp(&b.timestamp)
                .then_with(|| {
                    a.instrument
                        .exchange
                        .as_str()
                        .cmp(b.instrument.exchange.as_str())
                })
                .then_with(|| a.instrument.symbol.cmp(&b.instrument.symbol))
        });
        Ok(all_bars.into_iter().map(MarketEvent::Bar).collect())
    }

    fn find_timeframe_dir(&self, timeframe: &str) -> Result<Option<PathBuf>, CatalogError> {
        let cat = self.catalog_dir();
        if !cat.is_dir() {
            return Ok(None);
        }
        for dir in sorted_subdirs(&cat)? {
            let name = file_name(&dir);
            if name.eq_ignore_ascii_case(timeframe) {
                return Ok(Some(dir));
            }
        }
        Ok(None)
    }

    fn locate_partition_files(
        &self,
        instrument: &InstrumentId,
        timeframe: &str,
    ) -> Result<Vec<PathBuf>, CatalogError> {
        // 1. Check standard honba-data/data/catalog/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet
        if let Some(tf_dir) = self.find_timeframe_dir(timeframe)? {
            for exch_dir in sorted_subdirs(&tf_dir)? {
                if exch_dir.file_name().is_some_and(|n| {
                    n.to_string_lossy()
                        .eq_ignore_ascii_case(instrument.exchange.as_str())
                }) {
                    for sym_dir in sorted_subdirs(&exch_dir)? {
                        if sym_dir.file_name().is_some_and(|n| {
                            n.to_string_lossy().eq_ignore_ascii_case(&instrument.symbol)
                        }) {
                            return sorted_parquet_files(&sym_dir);
                        }
                    }
                }
            }
        }

        // 2. Fallback: Check Lakehouse v2 layout:
        //    {root}/bars/timeframe={tf}/exchange={EXCHANGE}/year=*/month=*/{SYMBOL}*.parquet
        let lh_tf_dir = self
            .root
            .join("bars")
            .join(format!("timeframe={}", timeframe.to_ascii_lowercase()))
            .join(format!("exchange={}", instrument.exchange.as_str()));
        if lh_tf_dir.is_dir() {
            let mut files = Vec::new();
            for year_dir in sorted_subdirs(&lh_tf_dir)? {
                for month_dir in sorted_subdirs(&year_dir)? {
                    for file in sorted_parquet_files(&month_dir)? {
                        let fname = file_name(&file);
                        let base = format!("{}.parquet", instrument.symbol);
                        let prefix = format!("{}.part-", instrument.symbol);
                        if fname == base || fname.starts_with(&prefix) {
                            files.push(file);
                        }
                    }
                }
            }
            return Ok(files);
        }

        Ok(Vec::new())
    }
}

impl BarReader for CatalogLoader {
    fn read_bars(&self, req: &BarRequest) -> PortResult<Vec<Bar>> {
        let tf_str = timeframe_to_catalog_str(req.timeframe);
        let bars = self.load_bars(&req.instrument, tf_str, Some(req.from), Some(req.to))?;
        Ok(bars)
    }
}

fn timeframe_to_catalog_str(tf: Timeframe) -> &'static str {
    match tf {
        Timeframe::M1 => "1M",
        Timeframe::M5 => "5M",
        Timeframe::M15 => "15M",
        Timeframe::M30 => "30M",
        Timeframe::H1 => "1H",
        Timeframe::D1 => "1D",
        Timeframe::W1 => "1W",
    }
}

fn read_parquet_bars_into(
    path: &Path,
    instrument: &InstrumentId,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    last_ts: &mut Option<DateTime<Utc>>,
    out: &mut Vec<Bar>,
) -> Result<(), CatalogError> {
    let file = File::open(path).map_err(|source| CatalogError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|source| CatalogError::Parquet {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?
        .build()
        .map_err(|source| CatalogError::Parquet {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;

    for batch_res in reader {
        let batch = batch_res.map_err(|source| CatalogError::Arrow {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;
        append_batch_bars(path, &batch, instrument, from, to, last_ts, out)?;
    }
    Ok(())
}

fn append_batch_bars(
    path: &Path,
    batch: &RecordBatch,
    instrument: &InstrumentId,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    last_ts: &mut Option<DateTime<Utc>>,
    out: &mut Vec<Bar>,
) -> Result<(), CatalogError> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(());
    }

    let schema = batch.schema();
    let ts_idx = schema
        .index_of("ts")
        .or_else(|_| schema.index_of("timestamp"))
        .map_err(|_| CatalogError::MissingColumn {
            path: path.to_path_buf(),
            column: "ts",
        })?;

    let col_idx = |name: &'static str| -> Result<usize, CatalogError> {
        schema
            .index_of(name)
            .map_err(|_| CatalogError::MissingColumn {
                path: path.to_path_buf(),
                column: name,
            })
    };

    let open_col = batch.column(col_idx("open")?).as_ref();
    let high_col = batch.column(col_idx("high")?).as_ref();
    let low_col = batch.column(col_idx("low")?).as_ref();
    let close_col = batch.column(col_idx("close")?).as_ref();
    let vol_col = batch.column(col_idx("volume")?).as_ref();
    let ts_col = batch.column(ts_idx).as_ref();

    for row in 0..num_rows {
        let Some(timestamp) = extract_timestamp(path, ts_col, row)? else {
            continue;
        };

        // Deduplicate boundary rows or non-increasing timestamps across partitions
        if let Some(prev) = *last_ts
            && timestamp <= prev
        {
            continue;
        }
        *last_ts = Some(timestamp);

        if let Some(f) = from
            && timestamp < f
        {
            continue;
        }
        if let Some(t) = to
            && timestamp > t
        {
            continue;
        }

        let open = Price(extract_decimal(path, open_col, "open", row)?);
        let high = Price(extract_decimal(path, high_col, "high", row)?);
        let low = Price(extract_decimal(path, low_col, "low", row)?);
        let close = Price(extract_decimal(path, close_col, "close", row)?);
        let volume = Quantity(extract_decimal(path, vol_col, "volume", row)?.max(Decimal::ZERO));

        out.push(Bar {
            instrument: instrument.clone(),
            open,
            high,
            low,
            close,
            volume,
            timestamp,
        });
    }

    Ok(())
}

fn extract_timestamp(
    path: &Path,
    col: &dyn Array,
    row: usize,
) -> Result<Option<DateTime<Utc>>, CatalogError> {
    if col.is_null(row) {
        return Ok(None);
    }
    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Some(DateTime::<Utc>::from_timestamp_nanos(arr.value(row))))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(Some(DateTime::<Utc>::from_timestamp_nanos(arr.value(row))))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(DateTime::<Utc>::from_timestamp_micros(arr.value(row)))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(DateTime::<Utc>::from_timestamp_millis(arr.value(row)))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = col.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            Ok(DateTime::<Utc>::from_timestamp(arr.value(row), 0))
        }
        other => Err(CatalogError::UnsupportedColumnType {
            path: path.to_path_buf(),
            column: "ts",
            data_type: Box::new(other.clone()),
        }),
    }
}

fn extract_decimal(
    path: &Path,
    col: &dyn Array,
    column: &'static str,
    row: usize,
) -> Result<Decimal, CatalogError> {
    if col.is_null(row) {
        return Err(CatalogError::InvalidRow {
            path: path.into(),
            row,
            column,
            reason: "null numeric value".into(),
        });
    }
    match col.data_type() {
        DataType::Decimal128(_, scale) => {
            let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(Decimal::from_i128_with_scale(arr.value(row), *scale as u32))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let val = arr.value(row);
            decimal_from_f64_lossless(val).ok_or_else(|| CatalogError::InvalidRow {
                path: path.into(),
                row,
                column,
                reason: format!("non-finite float value {val}").into(),
            })
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Decimal::from(arr.value(row)))
        }
        other => Err(CatalogError::UnsupportedColumnType {
            path: path.to_path_buf(),
            column,
            data_type: Box::new(other.clone()),
        }),
    }
}

/// Convert a finite `f64` from legacy Parquet columns into an exact `Decimal`
/// rounded to 4 decimal places (sub-paise tick precision), avoiding binary
/// float artifacts such as `1001.7000122070312` -> `1001.7000`.
pub fn decimal_from_f64_lossless(v: f64) -> Option<Decimal> {
    if !v.is_finite() {
        return None;
    }
    let s = format!("{v:.4}");
    Decimal::from_str(&s)
        .or_else(|_| Decimal::from_f64_retain(v).ok_or(()))
        .ok()
        .map(|d| d.round_dp(4).normalize())
}

fn sorted_subdirs(dir: &Path) -> Result<Vec<PathBuf>, CatalogError> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut dirs = Vec::new();
    for entry in fs::read_dir(dir).map_err(|source| CatalogError::Io {
        path: dir.to_path_buf(),
        source,
    })? {
        let path = entry
            .map_err(|source| CatalogError::Io {
                path: dir.to_path_buf(),
                source,
            })?
            .path();
        if path.is_dir() && !file_name(&path).starts_with('.') {
            dirs.push(path);
        }
    }
    dirs.sort();
    Ok(dirs)
}

fn sorted_parquet_files(dir: &Path) -> Result<Vec<PathBuf>, CatalogError> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).map_err(|source| CatalogError::Io {
        path: dir.to_path_buf(),
        source,
    })? {
        let path = entry
            .map_err(|source| CatalogError::Io {
                path: dir.to_path_buf(),
                source,
            })?
            .path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}
