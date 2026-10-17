//! ADR 0017 decision 5: a terminal run is evicted iff it is BOTH outside the `keep_runs`
//! newest AND older than `keep_days`; equivalently it survives if newest-N OR young.

use honba_api::RunId;

use crate::{evictions, RetentionPolicy};

const DAY: u64 = 86_400_000;
const NOW: u64 = 100 * DAY;

fn id(n: u32) -> RunId {
    RunId::parse(&format!("{:0>26}", n)).unwrap()
}

fn evicted(runs: &[(u32, u64)], keep_runs: usize, keep_days: u64) -> Vec<u32> {
    let runs: Vec<(RunId, u64)> = runs.iter().map(|&(n, t)| (id(n), t)).collect();
    let policy = RetentionPolicy {
        keep_runs,
        keep_days,
    };
    evictions(&runs, &policy, NOW)
        .into_iter()
        .map(|r| r.as_str().trim_start_matches('0').parse().unwrap())
        .collect()
}

#[test]
fn defaults_are_1000_runs_and_30_days() {
    let p = RetentionPolicy::default();
    assert_eq!((p.keep_runs, p.keep_days), (1_000, 30));
}

#[test]
fn evicts_only_when_both_old_and_beyond_count() {
    let old = NOW - 40 * DAY;
    let young = NOW - 5 * DAY;
    // keep_runs = 2: ranks by finished_at: 5(young) 4(young) | 3(young) 2(old) 1(old).
    let runs = [
        (1, old),
        (2, old + 1),
        (3, young - 1),
        (4, young),
        (5, young + 1),
    ];
    // Beyond the newest 2 AND older than 30 days -> only 1 and 2.
    assert_eq!(evicted(&runs, 2, 30), vec![1, 2]);
    // Everything young survives regardless of count; old ones inside the newest N survive.
    assert_eq!(evicted(&runs, 4, 30), vec![1]);
    assert_eq!(evicted(&runs, 5, 30), Vec::<u32>::new());
    // Beyond the count but young: survives.
    assert_eq!(
        evicted(&[(1, young), (2, young + 1)], 1, 30),
        Vec::<u32>::new()
    );
    // Old but inside the newest N: survives.
    assert_eq!(evicted(&[(1, old), (2, old + 1)], 5, 30), Vec::<u32>::new());
}

#[test]
fn the_age_boundary_is_strict() {
    let exactly = NOW - 30 * DAY;
    assert_eq!(
        evicted(&[(1, exactly), (2, exactly + 1)], 1, 30),
        Vec::<u32>::new()
    );
    assert_eq!(
        evicted(&[(1, exactly - 1), (2, exactly + 1)], 1, 30),
        vec![1]
    );
}

#[test]
fn ties_on_finished_at_break_by_run_id() {
    let old = NOW - 90 * DAY;
    // Same finished_at: the larger id counts as newer, so keep_runs = 1 keeps id 3.
    assert_eq!(evicted(&[(1, old), (3, old), (2, old)], 1, 30), vec![1, 2]);
}

#[test]
fn keep_runs_zero_leaves_age_as_the_only_rule() {
    let old = NOW - 90 * DAY;
    let young = NOW - DAY;
    assert_eq!(evicted(&[(1, old), (2, young)], 0, 30), vec![1]);
}
