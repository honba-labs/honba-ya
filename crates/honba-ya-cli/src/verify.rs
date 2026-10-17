//! `honba verify` – compile a strategy manifest into its IR (docs/archive/plan.md E0-S8).
//!
//! The CLI reads a JSON file, deserializes it as a `StrategyManifest` (unknown
//! fields rejected: a manifest is an input) and compiles it with
//! `StrategyIr::compile`. On success the IR is printed to stdout as JSON, so an
//! agent or script can pipe it straight into a run; on failure stdout stays
//! empty, stderr carries the stable error code, and the exit code is non-zero.
//! This is the gate that keeps a bad manifest from producing an empty journal
//! that looks like a strategy that did nothing.

use anyhow::{Context, Result};
use std::path::Path;

use honba_strategy::{StrategyIr, StrategyManifest};

pub fn run(path: &Path) -> Result<()> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let manifest: StrategyManifest = serde_json::from_str(&text)
        .with_context(|| format!("parsing {} as StrategyManifest", path.display()))?;
    let ir = StrategyIr::compile(manifest).map_err(|e| anyhow::anyhow!("{}: {e}", e.code()))?;
    println!("{}", serde_json::to_string_pretty(&ir)?);
    Ok(())
}
