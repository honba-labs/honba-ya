//! Unit tests for `crate::latency` (E3-S2).

use rand::rngs::StdRng;
use rand::SeedableRng;

use crate::latency::{depth_survival_discount, NoLatency, SeededLatency};
use crate::LatencyModel;

fn seeded(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

#[test]
fn no_latency_is_zero() {
    let mut rng = seeded(0);
    assert_eq!(NoLatency.ack_delay_ms(&mut rng), 0);
    assert_eq!(NoLatency.queue_delay_ms(&mut rng), 0);
    assert_eq!(NoLatency.total_delay_ms(&mut rng), 0);
}

#[test]
fn seeded_latency_is_reproducible_and_bounded() {
    let model = SeededLatency::new(5, 20);
    let draw = || {
        let mut rng = seeded(3);
        (model.ack_delay_ms(&mut rng), model.queue_delay_ms(&mut rng))
    };
    assert_eq!(draw(), draw(), "same seed, same delays");
    let (ack, queue) = draw();
    assert!(ack <= 5, "ack {ack} exceeds the bound");
    assert!(queue <= 20, "queue {queue} exceeds the bound");
    assert_eq!((model.ack_max_ms(), model.queue_max_ms()), (5, 20));
}

#[test]
fn depth_survival_discount_is_a_stub_returning_one() {
    // No depth source exists yet (`/depth` returns none): all depth survives.
    assert_eq!(depth_survival_discount(0.0, 0), 1.0);
    assert_eq!(depth_survival_discount(12.5, 1_000), 1.0);
    assert_eq!(depth_survival_discount(1_000_000.0, 60_000), 1.0);
}
