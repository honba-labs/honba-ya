//! Wall-clock-free domain check (E2-S3 rest, ADR 0018-era R1 close-out).
//!
//! The sync kernel is event-time only: domain crates must read time from a
//! [`honba_ports::Clock`] they are handed, never from the wall clock. This
//! test walks the production sources of the sync-kernel crates and refuses
//! wall-clock reads (`SystemTime`, `Instant::now`, chrono `::now`), so a
//! stray call fails the suite instead of landing silently.
//!
//! Deliberately narrow: durations and constants (`Duration`, `UNIX_EPOCH`)
//! are inert values; a use needs a clock read to become non-deterministic.
//! Test code (`src/tests/`) may stamp fixtures; doctests are not scanned.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Sync-kernel crates that must read time only from an injected clock
/// (`scripts/dependency_graph.py:SYNC_KERNEL_CRATES`).
const SYNC_KERNEL_CRATES: &[&str] = &[
    "honba-messages",
    "honba-entities",
    "honba-risk",
    "honba-engine",
    "honba-indicators",
    "honba-sim",
    "honba-strategy",
    "honba-market",
    "honba-analytics",
];

/// Wall-clock reads. `UNIX_EPOCH` stays: it is a constant, not a read.
const FORBIDDEN: &[&str] = &["SystemTime", "Instant::now", "Utc::now", "Local::now"];

fn sources_of(crate_name: &str) -> BTreeMap<PathBuf, String> {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(crate_name)
        .join("src");
    let mut out = BTreeMap::new();
    let mut dirs = vec![src];
    while let Some(dir) = dirs.pop() {
        let entries =
            std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().unwrap() != "tests" {
                    dirs.push(path);
                }
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
                out.insert(path, text);
            }
        }
    }
    out
}

/// A comment-only line may name a token while saying never to call it.
fn is_comment_only(line: &str) -> bool {
    line.trim_start().starts_with("//")
}

#[test]
fn no_system_time_in_domain_crates() {
    let mut violations = Vec::new();
    for crate_name in SYNC_KERNEL_CRATES {
        for (path, text) in sources_of(crate_name) {
            for (n, line) in text.lines().enumerate() {
                if is_comment_only(line) {
                    continue;
                }
                for token in FORBIDDEN {
                    if line.contains(token) {
                        violations.push(format!("{}:{}: {token}", path.display(), n + 1));
                    }
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "wall-clock reads in sync-kernel crates (use an injected Clock):\n{}",
        violations.join("\n")
    );
}
