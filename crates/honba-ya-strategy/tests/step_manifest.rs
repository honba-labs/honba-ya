//! Integration tests verifying alignment of `step.toml` with [`StrategyManifest`].

use std::fs;
use std::path::{Path, PathBuf};

use honba_strategy::{
    HistoryBars, StepKind, StepManifest, StrategyManifest, sweep_space_from_manifest,
};

fn catalog_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../honba-strategies")
}

fn find_step_tomls(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                find_step_tomls(&path, out);
            } else if path.file_name().and_then(|n| n.to_str()) == Some("step.toml") {
                out.push(path);
            }
        }
    }
}

#[test]
fn test_parse_sma_crossover_step_toml() {
    let path = catalog_root().join("momentum/trend/sma_crossover/step.toml");
    let content = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));

    let step = StepManifest::from_toml_str(&content).expect("failed to parse StepManifest");
    assert_eq!(step.id, "signal.sma_crossover");
    assert_eq!(step.kind, StepKind::Signal);
    assert_eq!(step.title, "SMA crossover");
    assert_eq!(
        step.requires.as_ref().unwrap().history_bars,
        Some(HistoryBars::Param("$slow".to_string()))
    );

    let manifest = step
        .to_strategy_manifest()
        .expect("failed to convert to StrategyManifest");
    assert_eq!(manifest.name, "sma_crossover");
    // Resolved from $slow (default = 50)
    assert_eq!(manifest.warmup_bars, 50);
    assert!(manifest.parameters.contains_key("fast"));
    assert!(manifest.parameters.contains_key("slow"));
    assert!(manifest.parameters.contains_key("capital"));
    assert!(manifest.parameters.contains_key("allocation"));

    let fast_param = &manifest.parameters["fast"];
    assert_eq!(fast_param.default, serde_json::json!(20));
    assert_eq!(fast_param.min, Some(2.0));
    assert_eq!(fast_param.max, Some(50.0));
    assert_eq!(fast_param.step, Some(1.0));
    assert_eq!(fast_param.range, Some("2:50:1".to_string()));

    // Verify sweep space extraction
    let sweep = sweep_space_from_manifest(&manifest).expect("sweep space extraction failed");
    assert!(sweep.contains_key("fast"));
    assert_eq!(sweep["fast"].len(), 49); // 2..=50 step 1

    let direct_manifest =
        StrategyManifest::from_step_toml_str(&content).expect("from_step_toml_str failed");
    assert_eq!(direct_manifest.name, "sma_crossover");
    assert_eq!(direct_manifest.warmup_bars, 50);
    assert_eq!(direct_manifest.parameters.len(), 4);
}

#[test]
fn test_parse_alpha_quality_step_toml() {
    let path = catalog_root().join("universe/alpha/quality/step.toml");
    let content = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));

    let step = StepManifest::from_toml_str(&content).expect("failed to parse StepManifest");
    assert_eq!(step.id, "execute.alpha30_quality");
    assert_eq!(step.kind, StepKind::Execute);
    assert_eq!(step.title, "Alpha 30 quality");
    assert_eq!(
        step.requires.as_ref().unwrap().history_bars,
        Some(HistoryBars::Count(64))
    );

    let manifest =
        StrategyManifest::from_step_toml_str(&content).expect("from_step_toml_str failed");
    assert_eq!(manifest.name, "alpha30_quality");
    assert_eq!(manifest.warmup_bars, 64);
    assert!(manifest.parameters.contains_key("universe"));
    assert!(manifest.parameters.contains_key("schedule"));
}

#[test]
fn test_parse_equal_weight_step_toml() {
    let path = catalog_root().join("portfolio/rebalancing/equal_weight/step.toml");
    let content = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));

    let manifest =
        StrategyManifest::from_step_toml_str(&content).expect("from_step_toml_str failed");
    assert_eq!(manifest.name, "equal_weight");
    assert_eq!(manifest.warmup_bars, 64);
    assert!(manifest.parameters.contains_key("universe"));
    assert!(manifest.parameters.contains_key("allocation"));
}

#[test]
fn test_all_catalog_step_tomls_parse_and_convert() {
    let catalog_dir = catalog_root();
    assert!(
        catalog_dir.exists(),
        "catalog dir not found at {}",
        catalog_dir.display()
    );

    let mut step_files = Vec::new();
    find_step_tomls(&catalog_dir, &mut step_files);

    assert_eq!(
        step_files.len(),
        17,
        "expected 17 step.toml files in the catalog"
    );

    for file_path in step_files {
        let content = fs::read_to_string(&file_path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", file_path.display()));
        let manifest = StrategyManifest::from_step_toml_str(&content)
            .unwrap_or_else(|e| panic!("parsing {}: {e}", file_path.display()));
        assert!(
            !manifest.name.is_empty(),
            "empty name in {}",
            file_path.display()
        );
        assert!(
            !manifest.source_hash.is_empty(),
            "empty hash in {}",
            file_path.display()
        );
    }
}
