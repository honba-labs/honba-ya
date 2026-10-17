//! The run registry and its on-disk layout (ADR 0017 decisions 2, 3 and 5).
//!
//! `<journals_dir>/<run_id>/manifest.json` (rewritten atomically) and `events.ndjson`
//! (append-only). Every method is blocking file I/O: handlers call them through
//! `spawn_blocking`, workers call them directly. Ids are validated with [`RunId::parse`]
//! before the registry or the filesystem is touched, so a path segment from a request can
//! never leave the journals root.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, SecondsFormat};
use honba_api::{
    ResolvedRequest, RunId, RunIdGenerator, RunKind, RunManifest, RunStatus, MANIFEST_VERSION,
};
use honba_messages::{ErrorCode, ErrorDetail, Message, SCHEMA_VERSION};
use honba_strategy::StrategyIr;
use serde_json::json;

use crate::journal::NdjsonJournal;
use crate::retention::{evictions, RetentionPolicy};

const MANIFEST: &str = "manifest.json";
const MANIFEST_TMP: &str = "manifest.json.tmp";
const EVENTS: &str = "events.ndjson";

/// Wall-clock source, injected so tests are deterministic.
pub trait RunClock: Send + Sync {
    /// Unix milliseconds now.
    fn now_unix_ms(&self) -> u64;
}

/// Entropy source for the 80 random id bits, injected so tests are deterministic.
pub trait RunEntropy: Send + Sync {
    /// The next 80 bits.
    fn next_bits(&self) -> Result<[u8; 10], ErrorDetail>;
}

/// The system clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl RunClock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }
}

/// OS randomness through `getrandom`.
#[derive(Clone, Copy, Debug, Default)]
pub struct OsEntropy;

impl RunEntropy for OsEntropy {
    fn next_bits(&self) -> Result<[u8; 10], ErrorDetail> {
        let mut bits = [0u8; 10];
        getrandom::fill(&mut bits).map_err(|e| {
            ErrorDetail::new(
                ErrorCode::InternalError,
                format!("OS entropy unavailable: {e}"),
            )
            .with_context(json!({"reason": "entropy"}))
        })?;
        Ok(bits)
    }
}

/// What start-up recovery did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Manifests loaded unchanged (terminal).
    pub loaded: usize,
    /// Non-terminal manifests closed as `failed` / `interrupted`.
    pub interrupted: Vec<RunId>,
    /// Entries skipped: bad name, unreadable, or unknown `manifest_version`.
    pub skipped: usize,
}

struct Inner {
    runs: HashMap<RunId, RunManifest>,
    ids: RunIdGenerator,
    accepting: bool,
}

/// The registry of runs plus the journals root.
///
/// The in-memory map is a cache of the manifests: every change writes the manifest first,
/// then updates the map, under one lock.
pub struct RunStore {
    root: PathBuf,
    clock: Box<dyn RunClock>,
    entropy: Box<dyn RunEntropy>,
    inner: Mutex<Inner>,
}

fn not_found() -> ErrorDetail {
    ErrorDetail::new(ErrorCode::NotFound, "run not found")
}

/// An I/O failure as an `internal_error`. Names the operation and I/O kind, never a path.
fn io_error(reason: &str, e: &std::io::Error) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::InternalError, format!("{reason}: {}", e.kind()))
        .with_context(json!({"reason": reason}))
}

fn rfc3339(unix_ms: u64) -> String {
    i64::try_from(unix_ms)
        .ok()
        .and_then(DateTime::from_timestamp_millis)
        .map_or_else(
            || "1970-01-01T00:00:00.000Z".to_owned(),
            |t| t.to_rfc3339_opts(SecondsFormat::Millis, true),
        )
}

fn parse_ms(text: Option<&str>) -> u64 {
    text.and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .and_then(|t| u64::try_from(t.timestamp_millis()).ok())
        .unwrap_or(0)
}

/// Writes `manifest.json` through a sibling temp file and a rename, so a reader never sees a
/// half-written manifest.
fn write_manifest(dir: &Path, manifest: &RunManifest) -> Result<(), ErrorDetail> {
    let bytes = serde_json::to_vec_pretty(manifest).map_err(|e| {
        ErrorDetail::new(
            ErrorCode::InternalError,
            format!("manifest encode failed: {e}"),
        )
        .with_context(json!({"reason": "manifest_write"}))
    })?;
    let write = || -> std::io::Result<()> {
        let tmp = dir.join(MANIFEST_TMP);
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp, dir.join(MANIFEST))
    };
    write().map_err(|e| io_error("manifest_write", &e))
}

impl RunStore {
    /// A store over `root`. Touches nothing on disk; call [`RunStore::recover`] at start-up.
    pub fn new(root: PathBuf, clock: Box<dyn RunClock>, entropy: Box<dyn RunEntropy>) -> Self {
        Self {
            root,
            clock,
            entropy,
            inner: Mutex::new(Inner {
                runs: HashMap::new(),
                ids: RunIdGenerator::default(),
                accepting: true,
            }),
        }
    }

    /// A store using the system clock and OS entropy (production).
    pub fn system(root: PathBuf) -> Self {
        Self::new(root, Box::new(SystemClock), Box::new(OsEntropy))
    }

    /// The journals root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn now(&self) -> (u64, String) {
        let ms = self.clock.now_unix_ms();
        (ms, rfc3339(ms))
    }

    /// Returns the run directory path for `id`.
    pub fn dir(&self, id: &RunId) -> PathBuf {
        self.root.join(id.as_str())
    }

    /// Admits a run: mints an id, writes `manifest.json` (`pending`) and an empty
    /// `events.ndjson`, registers it. Refused with 429 `rate_limited` / `shutting_down` once
    /// [`RunStore::shutdown`] ran. A refusal leaves nothing on disk.
    pub fn submit(
        &self,
        strategy_id: String,
        strategy: StrategyIr,
        request: ResolvedRequest,
    ) -> Result<RunManifest, ErrorDetail> {
        let mut inner = self.lock();
        if !inner.accepting {
            return Err(
                ErrorDetail::new(ErrorCode::RateLimited, "server is shutting down")
                    .with_context(json!({"reason": "shutting_down"})),
            );
        }
        let (ms, at) = self.now();
        let id = inner.ids.next(ms, self.entropy.next_bits()?)?;
        let manifest = RunManifest::new_pending(id.clone(), strategy_id, strategy, request, at);
        let dir = self.dir(&id);
        let create = || -> Result<(), ErrorDetail> {
            std::fs::create_dir_all(&self.root).map_err(|e| io_error("manifest_write", &e))?;
            std::fs::create_dir(&dir).map_err(|e| io_error("manifest_write", &e))?;
            std::fs::File::create(dir.join(EVENTS)).map_err(|e| io_error("journal_write", &e))?;
            write_manifest(&dir, &manifest)
        };
        if let Err(e) = create() {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(e);
        }
        inner.runs.insert(id, manifest.clone());
        Ok(manifest)
    }

    fn lookup(&self, id: &str, kind: Option<RunKind>) -> Result<RunManifest, ErrorDetail> {
        // Validation first: nothing below runs for an ill-formed id.
        let id = RunId::parse(id).map_err(|_| not_found())?;
        self.lock()
            .runs
            .get(&id)
            .filter(|m| kind.map_or(true, |k| m.kind == k))
            .cloned()
            .ok_or_else(not_found)
    }

    /// The run `id` of `kind`. The id is validated before the registry or the filesystem is
    /// touched; ill-formed, unknown, evicted and wrong-kind ids are all `not_found`.
    pub fn load(&self, id: &str, kind: RunKind) -> Result<RunManifest, ErrorDetail> {
        self.lookup(id, Some(kind))
    }

    /// Like [`RunStore::load`] for either kind (`GET /journals/{id}`).
    pub fn load_any(&self, id: &str) -> Result<RunManifest, ErrorDetail> {
        self.lookup(id, None)
    }

    /// Every registered run, ordered by id.
    pub fn list(&self) -> Vec<RunManifest> {
        let mut all: Vec<RunManifest> = self.lock().runs.values().cloned().collect();
        all.sort_by(|a, b| a.run_id.cmp(&b.run_id));
        all
    }

    /// Applies `change` to the run under the registry lock, writes the manifest, then
    /// updates the registry. `at` is the current time as RFC 3339. A refused transition or a
    /// failed write leaves the registry and the manifest unchanged, so a terminal run (for
    /// example one cancelled by shutdown) cannot be overwritten by a late worker.
    pub fn transition(
        &self,
        id: &RunId,
        change: impl FnOnce(&mut RunManifest, &str) -> Result<(), ErrorDetail>,
    ) -> Result<RunManifest, ErrorDetail> {
        let mut inner = self.lock();
        let mut next = inner.runs.get(id).cloned().ok_or_else(not_found)?;
        change(&mut next, &self.now().1)?;
        write_manifest(&self.dir(id), &next)?;
        inner.runs.insert(id.clone(), next.clone());
        Ok(next)
    }

    /// Opens the run's `events.ndjson` for appending.
    pub fn open_journal(&self, id: &RunId) -> Result<NdjsonJournal, ErrorDetail> {
        if !self.lock().runs.contains_key(id) {
            return Err(not_found());
        }
        NdjsonJournal::open(&self.dir(id).join(EVENTS))
    }

    /// The messages in the complete (newline-terminated) records currently on disk; a
    /// trailing partial line is ignored, so the result is a prefix of the final journal.
    pub fn read_journal(&self, id: &str, kind: RunKind) -> Result<Vec<Message>, ErrorDetail> {
        let manifest = self.load(id, kind)?;
        let bytes = std::fs::read(self.dir(&manifest.run_id).join(EVENTS))
            .map_err(|e| io_error("journal_read", &e))?;
        let complete = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
        bytes[..complete]
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(decode_record)
            .collect()
    }

    /// Graceful shutdown in one step: stop admitting and write `cancelled` (with
    /// `finished_at`) for every non-terminal run. Returns how many were cancelled. A service
    /// that grants running runs a grace period uses [`RunStore::begin_shutdown`] and
    /// [`RunStore::cancel_running`] instead.
    pub fn shutdown(&self) -> usize {
        let mut inner = self.lock();
        inner.accepting = false;
        self.cancel_matching(&mut inner, |_| true)
    }

    /// Cancels, under the held lock, every non-terminal run `select` accepts.
    fn cancel_matching(&self, inner: &mut Inner, select: impl Fn(RunStatus) -> bool) -> usize {
        let at = self.now().1;
        let mut open: Vec<RunId> = inner
            .runs
            .values()
            .filter(|m| !m.status.is_terminal() && select(m.status))
            .map(|m| m.run_id.clone())
            .collect();
        open.sort();
        let mut cancelled = 0;
        for id in open {
            let mut next = inner.runs[&id].clone();
            let result = next
                .cancel(&at)
                .and_then(|()| write_manifest(&self.dir(&id), &next));
            match result {
                Ok(()) => {
                    inner.runs.insert(id, next);
                    cancelled += 1;
                }
                Err(e) => tracing::error!(run_id = %id, error = %e, "shutdown cancel failed"),
            }
        }
        cancelled
    }

    /// Phase one of a graceful shutdown: stop admitting and write `cancelled` for every
    /// `pending` run. `running` runs are left to finish on their own. Returns how many were
    /// cancelled.
    pub fn begin_shutdown(&self) -> usize {
        let mut inner = self.lock();
        inner.accepting = false;
        self.cancel_matching(&mut inner, |s| s == RunStatus::Pending)
    }

    /// Phase two: write `cancelled` for every run still `running` (after the grace period).
    /// Returns how many were cancelled.
    pub fn cancel_running(&self) -> usize {
        let mut inner = self.lock();
        self.cancel_matching(&mut inner, |s| s == RunStatus::Running)
    }

    /// Start-up scan: loads manifests, closes non-terminal ones as `failed` /
    /// `interrupted`. Never re-executes anything, never creates the root, and leaves every
    /// entry it does not understand untouched on disk.
    pub fn recover(&self) -> RecoveryReport {
        let mut report = RecoveryReport::default();
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return report;
        };
        let mut names: Vec<String> = entries
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        names.sort();
        let mut inner = self.lock();
        let (_, now) = self.now();
        for name in names {
            let Ok(id) = RunId::parse(&name) else {
                report.skipped += 1;
                continue;
            };
            let Some(mut manifest) = self.read_manifest(&id) else {
                report.skipped += 1;
                continue;
            };
            if !manifest.status.is_terminal() {
                let closed = (|| {
                    if manifest.status == RunStatus::Pending {
                        manifest.start(&now)?;
                    }
                    manifest.fail(
                        ErrorDetail::new(ErrorCode::InternalError, "run interrupted by a restart")
                            .with_context(json!({"reason": "interrupted"})),
                        &now,
                    )?;
                    write_manifest(&self.dir(&id), &manifest)
                })();
                if let Err(e) = closed {
                    tracing::error!(run_id = %id, error = %e, "could not close interrupted run");
                    report.skipped += 1;
                    continue;
                }
                report.interrupted.push(id.clone());
            } else {
                report.loaded += 1;
            }
            inner.runs.insert(id, manifest);
        }
        report
    }

    fn read_manifest(&self, id: &RunId) -> Option<RunManifest> {
        let bytes = std::fs::read(self.dir(id).join(MANIFEST)).ok()?;
        let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
        let version = value
            .get("manifest_version")
            .and_then(serde_json::Value::as_u64);
        if version != Some(u64::from(MANIFEST_VERSION)) {
            tracing::warn!(run_id = %id, ?version, "unknown manifest_version; run not loaded");
            return None;
        }
        let manifest: RunManifest = serde_json::from_value(value).ok()?;
        (manifest.run_id == *id).then_some(manifest)
    }

    /// Start-up retention (after [`RunStore::recover`]): deletes the run directories
    /// [`evictions`] selects and forgets them. Non-terminal runs are never evicted.
    pub fn apply_retention(&self, policy: &RetentionPolicy) -> Vec<RunId> {
        let mut inner = self.lock();
        let terminal: Vec<(RunId, u64)> = inner
            .runs
            .values()
            .filter(|m| m.status.is_terminal())
            .map(|m| (m.run_id.clone(), parse_ms(m.finished_at.as_deref())))
            .collect();
        let mut evicted = Vec::new();
        for id in evictions(&terminal, policy, self.clock.now_unix_ms()) {
            match std::fs::remove_dir_all(self.dir(&id)) {
                Ok(()) => {
                    inner.runs.remove(&id);
                    evicted.push(id);
                }
                Err(e) => tracing::error!(run_id = %id, error = %e, "retention delete failed"),
            }
        }
        evicted
    }
}

fn decode_record(line: &[u8]) -> Result<Message, ErrorDetail> {
    serde_json::from_slice::<Message>(line).map_err(|e| {
        let found = serde_json::from_slice::<serde_json::Value>(line)
            .ok()
            .and_then(|v| v.get("schema_version").and_then(serde_json::Value::as_u64));
        match found {
            Some(found) if found != u64::from(SCHEMA_VERSION) => ErrorDetail::new(
                ErrorCode::Unsupported,
                "journal written at another schema_version",
            )
            .with_context(json!({"found": found, "expected": SCHEMA_VERSION})),
            _ => ErrorDetail::new(
                ErrorCode::InternalError,
                format!("journal record invalid: {e}"),
            )
            .with_context(json!({"reason": "journal_corrupt"})),
        }
    })
}
