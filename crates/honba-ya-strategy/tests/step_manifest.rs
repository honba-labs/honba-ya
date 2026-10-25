//! Integration tests verifying alignment of `step.toml` with [`StrategyManifest`].

use std::fs;
use std::path::{Path, PathBuf};

use honba_strategy::{
    StepKind, StepManifest, StrategyManifest, sweep_space_from_manifest,
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
fn test_parse_kama_crossover_step_toml() {
    let path = catalog_root().join("catalog/trend/kama_crossover/step.toml");
    let content = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));

    let step = StepManifest::from_toml_str(&content).expect("failed to parse StepManifest");
    assert_eq!(step.id, "signal.kama_crossover");
    assert_eq!(step.kind, StepKind::Signal);
    assert_eq!(step.title, "KAMA Crossover");

    let manifest = step
        .to_strategy_manifest()
        .expect("failed to convert to StrategyManifest");
    assert_eq!(manifest.name, "kama_crossover");
    assert!(manifest.parameters.contains_key("fast_period"));
    assert!(manifest.parameters.contains_key("slow_period"));

    let sweep = sweep_space_from_manifest(&manifest).expect("sweep space extraction failed");
    assert!(sweep.contains_key("fast_period"));

    let direct_manifest =
        StrategyManifest::from_step_toml_str(&content).expect("from_step_toml_str failed");
    assert_eq!(direct_manifest.name, "kama_crossover");
}

#[test]
fn test_parse_turtle_20_step_toml() {
    let path = catalog_root().join("catalog/trend/turtle_20/step.toml");
    let content = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));

    let step = StepManifest::from_toml_str(&content).expect("failed to parse StepManifest");
    assert_eq!(step.id, "signal.turtle_20");
    assert_eq!(step.kind, StepKind::Signal);
    assert_eq!(step.title, "Turtle Trading System");

    let manifest =
        StrategyManifest::from_step_toml_str(&content).expect("from_step_toml_str failed");
    assert_eq!(manifest.name, "turtle_20");
}

#[test]
fn test_parse_equal_weight_step_toml() {
    let path = catalog_root().join("catalog/portfolio/equal_weight/step.toml");
    let content = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));

    let manifest =
        StrategyManifest::from_step_toml_str(&content).expect("from_step_toml_str failed");
    assert_eq!(manifest.name, "equal_weight");
    assert!(manifest.parameters.contains_key("drift_threshold"));
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

    assert!(
        !step_files.is_empty(),
        "expected step.toml files in the catalog"
    );

    for file_path in step_files {
        let content = fs::read_to_string(&file_path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", file_path.display()));
        let manifest = StrategyManifest::from_step_toml_str(&content)
            .unwrap_or_else(|e| panic!("from_step_toml_str on {}: {e}", file_path.display()));
        assert!(!manifest.name.is_empty());
    }
}
