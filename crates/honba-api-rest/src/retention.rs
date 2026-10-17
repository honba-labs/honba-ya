//! Retention policy for finished runs (ADR 0017 decision 5), pure.

use honba_api::RunId;

/// How many finished runs, and for how long, start-up keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Newest terminal runs that always survive (default 1,000).
    pub keep_runs: usize,
    /// Terminal runs finished less than this many days ago survive (default 30).
    pub keep_days: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            keep_runs: 1_000,
            keep_days: 30,
        }
    }
}

/// Which of the terminal runs `(id, finished_at_unix_ms)` to evict at `now_unix_ms`.
///
/// A run is evicted iff it is outside the `keep_runs` newest (ordered by `finished_at`, then
/// `run_id`) *and* strictly older than `keep_days`. Returned in eviction order, oldest first.
pub fn evictions(
    terminal: &[(RunId, u64)],
    policy: &RetentionPolicy,
    now_unix_ms: u64,
) -> Vec<RunId> {
    const DAY_MS: u64 = 86_400_000;
    let cutoff = now_unix_ms.saturating_sub(policy.keep_days.saturating_mul(DAY_MS));
    let mut ordered: Vec<&(RunId, u64)> = terminal.iter().collect();
    // Oldest first: (finished_at, run_id) ascending.
    ordered.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));
    let beyond_count = ordered.len().saturating_sub(policy.keep_runs);
    ordered
        .into_iter()
        .take(beyond_count)
        .filter(|(_, finished)| *finished < cutoff)
        .map(|(id, _)| id.clone())
        .collect()
}
