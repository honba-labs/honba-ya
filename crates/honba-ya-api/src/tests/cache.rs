//! Unit tests for `crate::cache` (ADR 0020, E4-S1).

use serde_json::json;

use crate::cache::{CacheKey, CachedResult, ResultCache};
use crate::responses::BacktestMetrics;

fn sample_metrics() -> BacktestMetrics {
    BacktestMetrics {
        total_return: 0.15,
        sharpe: 1.85,
        max_drawdown: 0.08,
        trades: 24,
        net_pnl: 150_000.0,
    }
}

#[test]
fn cache_key_changes_with_costs() {
    let manifest_id = "sha256:1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff";
    let params = json!({"fast": 10, "slow": 30});
    let dataset_id = "NSE:TCS:1d:2024-01-01:2024-12-31";

    let key_a = CacheKey::compute(manifest_id, &params, dataset_id, "india.equity.delivery:v1");
    let key_b = CacheKey::compute(manifest_id, &params, dataset_id, "india.equity.delivery:v2");
    let key_zero = CacheKey::compute(manifest_id, &params, dataset_id, "none");

    // Differing costs produce different keys
    assert_ne!(key_a, key_b);
    assert_ne!(key_a, key_zero);
    assert_ne!(key_b, key_zero);

    // Identical inputs produce identical keys
    let key_a_copy =
        CacheKey::compute(manifest_id, &params, dataset_id, "india.equity.delivery:v1");
    assert_eq!(key_a, key_a_copy);

    // Key format is sha256:<64 hex chars>
    assert!(key_a.as_str().starts_with("sha256:"));
    assert_eq!(key_a.as_str().len(), 7 + 64);

    // Reordered dictionary keys produce identical cache key (canonical json)
    let params_reordered = json!({"slow": 30, "fast": 10});
    let key_reordered = CacheKey::compute(
        manifest_id,
        &params_reordered,
        dataset_id,
        "india.equity.delivery:v1",
    );
    assert_eq!(key_a, key_reordered);
}

#[test]
fn cache_key_changes_with_manifest_dataset_and_params() {
    let manifest_a = "sha256:aaaa";
    let manifest_b = "sha256:bbbb";
    let params_a = json!({"fast": 5});
    let params_b = json!({"fast": 10});
    let dataset_a = "NSE:RELIANCE:1d";
    let dataset_b = "NSE:INFY:1d";
    let costs = "india.equity.delivery:v1";

    assert_ne!(
        CacheKey::compute(manifest_a, &params_a, dataset_a, costs),
        CacheKey::compute(manifest_b, &params_a, dataset_a, costs)
    );
    assert_ne!(
        CacheKey::compute(manifest_a, &params_a, dataset_a, costs),
        CacheKey::compute(manifest_a, &params_b, dataset_a, costs)
    );
    assert_ne!(
        CacheKey::compute(manifest_a, &params_a, dataset_a, costs),
        CacheKey::compute(manifest_a, &params_a, dataset_b, costs)
    );
}

#[test]
fn in_memory_result_cache_stores_and_retrieves() {
    let cache = ResultCache::in_memory();
    assert!(cache.is_empty());
    assert_eq!(cache.len(), 0);

    let key = CacheKey::compute("manifest_1", &json!({}), "dataset_1", "costs_1");
    assert_eq!(cache.get(&key), None);

    let cached = CachedResult {
        metrics: sample_metrics(),
        assumptions: json!({"not_modelled": []}),
        journal_path: Some("data/journals/01HXX/events.ndjson".to_string()),
    };

    cache.insert(key.clone(), cached.clone());
    assert!(!cache.is_empty());
    assert_eq!(cache.len(), 1);

    let retrieved = cache.get(&key).expect("cached item must exist");
    assert_eq!(retrieved, cached);

    cache.clear();
    assert!(cache.is_empty());
    assert_eq!(cache.get(&key), None);
}

#[test]
fn disk_backed_result_cache_persists_across_instances() {
    let temp_dir = std::env::temp_dir().join(format!("honba_test_cache_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);

    let cache1 = ResultCache::with_disk_dir(&temp_dir);
    let key = CacheKey::compute(
        "manifest_disk",
        &json!({"x": 1}),
        "dataset_disk",
        "costs_v1",
    );

    let cached = CachedResult {
        metrics: sample_metrics(),
        assumptions: json!({"timing": "next_open"}),
        journal_path: None,
    };

    cache1.insert(key.clone(), cached.clone());

    // Create a new instance pointing to the same disk directory
    let cache2 = ResultCache::with_disk_dir(&temp_dir);
    let retrieved = cache2.get(&key).expect("disk cache must load entry");
    assert_eq!(retrieved, cached);

    let _ = std::fs::remove_dir_all(&temp_dir);
}
