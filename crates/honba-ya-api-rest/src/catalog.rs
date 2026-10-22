//! A bar reader over the `honba-data` Parquet catalog.
//!
//! `honba-data` writes bars as `{root}/catalog/{TF}/{EXCHANGE}/{SYMBOL}/{PART}.parquet`, e.g.
//! `catalog/1D/NSE/HDFCBANK/2024.parquet` or `catalog/5M/NSE/RELIANCE/2026.parquet`: one
//! directory per timeframe, the partition files of a symbol sorted by name (years) and holding
//! the usual `ts, open, high, low, close, volume` bar columns. The timeframe is the directory
//! name (`1D`, `5M`, `1H`, `1W`, `1MO`, case-insensitive), so a symbol can be served at several
//! timeframes, which a single [`DatasetReader`] cannot (it holds one slice per instrument).
//!
//! [`CatalogBars`] therefore holds one [`Dataset`] per timeframe and answers [`BarReader`] from
//! the one whose spec the request names. Instruments, quotes and depth come from a
//! [`DatasetReader`] over each instrument's most recent slice (see [`load_catalog`]).

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use honba_api::parse_timeframe;
use honba_api::wire::BarSpecification;
use honba_core::{Bar, Exchange, InstrumentId};
use honba_feed::{
    ColumnarSlice, ColumnarSliceBuilder, Dataset, DatasetBuildError, DatasetReader,
    ParquetBarSource, ParquetError,
};
use honba_ports::{BarReader, BarRequest, PortError, PortResult};
use thiserror::Error;

/// The directory under a data root that holds the bar catalog.
pub const CATALOG_DIR: &str = "catalog";

/// Why the catalog could not be loaded.
#[derive(Debug, Error)]
pub enum CatalogLoadError {
    /// A directory or entry could not be read.
    #[error("cannot read catalog {path}: {source}")]
    Io {
        /// The path being read.
        path: PathBuf,
        /// The underlying error.
        source: std::io::Error,
    },
    /// A timeframe directory name is not a timeframe such as `1D` or `5M`.
    #[error("catalog directory {0:?} is not a timeframe such as 1D or 5M")]
    Timeframe(String),
    /// A partition file could not be read.
    #[error("reading {path}: {source}")]
    Parquet {
        /// The partition file.
        path: PathBuf,
        /// The underlying error.
        source: ParquetError,
    },
    /// The rows broke a dataset invariant.
    #[error(transparent)]
    Dataset(#[from] DatasetBuildError),
}

/// Stored bars of the catalog, one [`Dataset`] per timeframe.
#[derive(Clone, Debug)]
pub struct CatalogBars {
    timeframes: Vec<(BarSpecification, DatasetReader, Dataset)>,
}

impl CatalogBars {
    /// The timeframes held, in catalog directory order.
    pub fn timeframes(&self) -> impl Iterator<Item = &BarSpecification> + '_ {
        self.timeframes.iter().map(|(spec, _, _)| spec)
    }
}

#[async_trait]
impl BarReader for CatalogBars {
    async fn read_bars(&self, request: &BarRequest) -> PortResult<Vec<Bar>> {
        let found = self.timeframes.iter().find(|(spec, _, dataset)| {
            *spec == request.spec() && dataset.slice(request.instrument()).is_some()
        });
        match found {
            Some((_, reader, _)) => reader.read_bars(request).await,
            None => Err(PortError::Unsupported(format!(
                "{} has no bars at the requested timeframe",
                request.instrument()
            ))),
        }
    }
}

/// Loads `{root}/catalog`: the per-timeframe bars and a reader listing every instrument found.
///
/// The listing reader holds, per instrument, the slice whose last bar is the most recent across
/// timeframes, so derived quotes are as fresh as the catalog allows. Blocks on I/O; call once at
/// start-up. Rows whose timestamp is not after the previous partition's last bar are dropped,
/// so overlapping partitions do not fail the load.
pub fn load_catalog(root: &Path) -> Result<(CatalogBars, DatasetReader), CatalogLoadError> {
    let catalog = root.join(CATALOG_DIR);
    let mut timeframes = Vec::new();
    let mut latest: std::collections::BTreeMap<InstrumentId, ColumnarSlice> = Default::default();
    for tf_dir in sorted_dirs(&catalog)? {
        let name = file_name(&tf_dir);
        let spec = parse_timeframe(&name.to_ascii_lowercase())
            .map_err(|_| CatalogLoadError::Timeframe(name.clone()))?;
        let mut slices = Vec::new();
        for exchange_dir in sorted_dirs(&tf_dir)? {
            let exchange_name = file_name(&exchange_dir);
            let exchange = exchange_name
                .parse::<Exchange>()
                .map_err(|_| CatalogLoadError::Timeframe(exchange_name.clone()))?;
            for symbol_dir in sorted_dirs(&exchange_dir)? {
                let id = InstrumentId::new(exchange, file_name(&symbol_dir));
                let slice = read_symbol(&symbol_dir, id, spec)?;
                if !slice.is_empty() {
                    slices.push(slice);
                }
            }
        }
        for slice in &slices {
            let newer = latest
                .get(slice.instrument())
                .is_none_or(|held| slice.timestamps().last() > held.timestamps().last());
            if newer {
                latest.insert(slice.instrument().clone(), slice.clone());
            }
        }
        let dataset = Dataset::from_slices(slices)?;
        timeframes.push((spec, DatasetReader::from_dataset(dataset.clone()), dataset));
    }
    let listing =
        DatasetReader::from_dataset(Dataset::from_slices(latest.into_values().collect())?);
    Ok((CatalogBars { timeframes }, listing))
}

/// Concatenates a symbol's partition files, in file-name order, into one slice.
fn read_symbol(
    dir: &Path,
    id: InstrumentId,
    spec: BarSpecification,
) -> Result<ColumnarSlice, CatalogLoadError> {
    let mut parts = entries(dir)?
        .into_iter()
        .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "parquet"))
        .collect::<Vec<_>>();
    parts.sort();
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    let mut last = None;
    for path in parts {
        let part = ParquetBarSource::with_spec(&path, id.clone(), spec)
            .columnar()
            .map_err(|source| CatalogLoadError::Parquet {
                path: path.clone(),
                source,
            })?;
        let ts = part.timestamps();
        let skip = last.map_or(0, |at| ts.partition_point(|t| *t <= at));
        let end = ts.len();
        builder.push_rows(
            &ts[skip..end],
            &part.opens()[skip..end],
            &part.highs()[skip..end],
            &part.lows()[skip..end],
            &part.closes()[skip..end],
            &part.volumes()[skip..end],
        )?;
        last = last.max(ts.last().copied());
    }
    Ok(builder.finish()?)
}

fn entries(dir: &Path) -> Result<Vec<PathBuf>, CatalogLoadError> {
    let io = |source| CatalogLoadError::Io {
        path: dir.to_path_buf(),
        source,
    };
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(io)? {
        paths.push(entry.map_err(io)?.path());
    }
    Ok(paths)
}

/// Sub-directories of `dir`, sorted, skipping hidden ones (e.g. `.gitkeep` holders).
fn sorted_dirs(dir: &Path) -> Result<Vec<PathBuf>, CatalogLoadError> {
    let mut dirs = entries(dir)?
        .into_iter()
        .filter(|path| path.is_dir() && !file_name(path).starts_with('.'))
        .collect::<Vec<_>>();
    dirs.sort();
    Ok(dirs)
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}
