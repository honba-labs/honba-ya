//! Backtest result cache and deterministic cache key (ADR 0020, E4-S1).

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::responses::BacktestMetrics;

/// Canonical JSON serialization: recursively sorts all object keys in `BTreeMap`s
/// so equivalent JSON values yield bit-identical serialized bytes regardless of
/// input map insertion order.
pub fn canonical_json_bytes(val: &Value) -> Vec<u8> {
    fn canonicalize(v: &Value) -> Value {
        match v {
            Value::Object(map) => {
                let sorted: BTreeMap<String, Value> = map
                    .iter()
                    .map(|(k, v)| (k.clone(), canonicalize(v)))
                    .collect();
                serde_json::to_value(sorted).unwrap_or(Value::Null)
            }
            Value::Array(arr) => Value::Array(arr.iter().map(canonicalize).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_vec(&canonicalize(val)).unwrap_or_default()
}

/// A deterministic cache key identifying the exact inputs to a backtest run:
/// `sha256:` plus 64 lowercase hex digits.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheKey(String);

impl CacheKey {
    /// Computes the key from manifest id/JSON, strategy params, dataset id, and cost model version.
    pub fn compute(
        manifest_id: &str,
        params: &Value,
        dataset_id: &str,
        cost_model_version: &str,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(manifest_id.as_bytes());
        hasher.update(b"\n");
        let param_bytes = canonical_json_bytes(params);
        hasher.update(&param_bytes);
        hasher.update(b"\n");
        hasher.update(dataset_id.as_bytes());
        hasher.update(b"\n");
        hasher.update(cost_model_version.as_bytes());

        let digest = hasher.finalize();
        let mut s = String::from("sha256:");
        for b in digest {
            s.push_str(&format!("{b:02x}"));
        }
        Self(s)
    }

    /// Access the underlying key string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for CacheKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A cached backtest execution outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CachedResult {
    /// Backtest performance metrics.
    pub metrics: BacktestMetrics,
    /// Unmodelled assumptions and run metadata.
    pub assumptions: Value,
    /// Optional relative path to the journal file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_path: Option<String>,
}

/// Thread-safe result cache for backtests.
#[derive(Clone, Default)]
pub struct ResultCache {
    memory: Arc<Mutex<HashMap<CacheKey, CachedResult>>>,
    disk_dir: Option<PathBuf>,
}

impl ResultCache {
    /// Creates a purely in-memory cache.
    pub fn in_memory() -> Self {
        Self {
            memory: Arc::new(Mutex::new(HashMap::new())),
            disk_dir: None,
        }
    }

    /// Creates a cache backed by an on-disk directory, loading any previously cached entries.
    pub fn with_disk_dir(dir: impl Into<PathBuf>) -> Self {
        let path = dir.into();
        let _ = fs::create_dir_all(&path);
        let mut map = HashMap::new();
        if let Ok(entries) = fs::read_dir(&path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_file() && p.extension().is_some_and(|ext| ext == "json") {
                    if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                        let key_str = stem.replacen('_', ":", 1);
                        if let Ok(bytes) = fs::read(&p) {
                            if let Ok(cached) = serde_json::from_slice::<CachedResult>(&bytes) {
                                map.insert(CacheKey(key_str), cached);
                            }
                        }
                    }
                }
            }
        }
        Self {
            memory: Arc::new(Mutex::new(map)),
            disk_dir: Some(path),
        }
    }

    /// Retrieves a cached result by key if present.
    pub fn get(&self, key: &CacheKey) -> Option<CachedResult> {
        let mut lock = self.memory.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(res) = lock.get(key) {
            return Some(res.clone());
        }
        if let Some(ref dir) = self.disk_dir {
            let file_name = format!("{}.json", key.as_str().replace(':', "_"));
            let file_path = dir.join(file_name);
            if file_path.is_file() {
                if let Ok(bytes) = fs::read(&file_path) {
                    if let Ok(cached) = serde_json::from_slice::<CachedResult>(&bytes) {
                        lock.insert(key.clone(), cached.clone());
                        return Some(cached);
                    }
                }
            }
        }
        None
    }

    /// Inserts a result into the cache.
    pub fn insert(&self, key: CacheKey, result: CachedResult) {
        let mut lock = self.memory.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(ref dir) = self.disk_dir {
            let file_name = format!("{}.json", key.as_str().replace(':', "_"));
            let file_path = dir.join(file_name);
            if let Ok(bytes) = serde_json::to_vec_pretty(&result) {
                let _ = fs::write(file_path, bytes);
            }
        }
        lock.insert(key, result);
    }

    /// Returns the number of cached items in memory.
    pub fn len(&self) -> usize {
        self.memory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Returns `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears all entries from the in-memory cache.
    pub fn clear(&self) {
        self.memory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}
