//! Unit tests for `crate::plan`.

use std::sync::Arc;

use honba_entities::{Currency, Money};
use honba_strategy::{BuyAndHold, Strategy};

use super::{any_instrument, buy_and_hold_params, report};
use crate::{
    default_initial_cash, Fitness, SharpeFitness, StrategyFactory, StrategySpec, SweepError,
    SweepPlan, TrialParams, DEFAULT_MAX_CONCURRENCY, DEFAULT_PERIODS_PER_YEAR,
};

struct Fixed;

impl Strategy for Fixed {
    fn name(&self) -> &str {
        "fixed"
    }
}

impl StrategyFactory for Fixed {
    fn build(&self, _seed: u64) -> Box<dyn Strategy> {
        Box::new(Fixed)
    }
}

struct Named(String);

impl Strategy for Named {
    fn name(&self) -> &str {
        &self.0
    }
}

struct Seeded;

impl StrategyFactory for Seeded {
    fn build(&self, seed: u64) -> Box<dyn Strategy> {
        Box::new(Named(format!("seed-{seed}")))
    }
}

fn plan_with(trials: usize) -> SweepPlan {
    SweepPlan::new(
        (0..trials)
            .map(|seed| buy_and_hold_params(seed as u64))
            .collect(),
        Arc::new(SharpeFitness),
    )
}

#[test]
fn max_concurrency_defaults_to_the_trial_count_capped_at_the_documented_limit() {
    assert_eq!(
        plan_with(0).max_concurrency(),
        1,
        "an empty plan is runnable"
    );
    assert_eq!(plan_with(1).max_concurrency(), 1);
    assert_eq!(plan_with(3).max_concurrency(), 3);
    assert_eq!(
        plan_with(DEFAULT_MAX_CONCURRENCY).max_concurrency(),
        DEFAULT_MAX_CONCURRENCY
    );
    assert_eq!(
        plan_with(DEFAULT_MAX_CONCURRENCY + 5).max_concurrency(),
        DEFAULT_MAX_CONCURRENCY
    );
}

#[test]
fn cash_and_periods_default_to_the_documented_values() {
    let plan = plan_with(2);
    assert_eq!(
        plan.initial_cash(),
        default_initial_cash(Currency::Inr).minor()
    );
    assert_eq!(plan.currency(), Currency::Inr);
    assert_eq!(plan.periods_per_year(), DEFAULT_PERIODS_PER_YEAR);
    assert!(plan.validate().is_ok());
    const _: () = assert!(DEFAULT_PERIODS_PER_YEAR > 0.0);
}

#[test]
fn default_cash_is_a_million_major_units_in_every_currency() {
    for &currency in Currency::ALL {
        let cash = default_initial_cash(currency);
        assert_eq!(cash.currency(), currency);
        assert_eq!(
            cash.minor(),
            1_000_000 * 10_i64.pow(u32::from(currency.minor_exponent())),
            "{currency}"
        );
        assert_eq!(cash.to_major_f64(), 1_000_000.0, "{currency}");
    }
}

#[test]
fn builders_replace_only_the_setting_they_name() {
    let trials = vec![buy_and_hold_params(1), buy_and_hold_params(2)];
    let plan = SweepPlan::new(trials.clone(), Arc::new(SharpeFitness))
        .with_max_concurrency(7)
        .with_initial_cash(Money::from_major_f64(500.0, Currency::Inr).unwrap())
        .with_periods_per_year(12.0);
    assert_eq!(plan.trials(), trials.as_slice());
    assert_eq!(plan.trials().len(), 2);
    assert_eq!(plan.max_concurrency(), 7);
    assert_eq!(plan.initial_cash(), 50000);
    assert_eq!(plan.periods_per_year(), 12.0);
}

#[test]
fn repeated_accessors_return_the_same_values_so_a_built_plan_is_immutable() {
    let plan = plan_with(2).with_initial_cash(Money::from_major_f64(250.0, Currency::Inr).unwrap());
    assert_eq!(plan.trials(), plan.trials());
    assert_eq!(plan.max_concurrency(), plan.max_concurrency());
    assert_eq!(plan.initial_cash(), 25000);
    assert_eq!(plan.initial_cash(), plan.initial_cash());
    assert_eq!(plan.periods_per_year(), plan.periods_per_year());
}

#[test]
fn the_fitness_is_the_one_the_plan_was_given() {
    let plan = SweepPlan::new(Vec::new(), Arc::new(SharpeFitness));
    assert_eq!(plan.fitness().score(&report(0, Some(1.5))), 1.5);
}

#[test]
fn zero_concurrency_is_built_and_then_refused_when_the_plan_is_validated() {
    let plan = plan_with(3).with_max_concurrency(0);
    assert_eq!(
        plan.max_concurrency(),
        0,
        "the builder stores what it is given"
    );
    assert_eq!(
        plan.validate().unwrap_err(),
        SweepError::InvalidPlan("max_concurrency must be at least 1, got 0".to_string())
    );
}

#[test]
fn invalid_cash_and_periods_are_refused_with_the_value_that_was_given() {
    for cash in [0.0, -1.0] {
        let minor = Money::from_major_f64(cash, Currency::Inr).unwrap().minor();
        assert_eq!(
            plan_with(1)
                .with_initial_cash(Money::from_major_f64(cash, Currency::Inr).unwrap())
                .validate()
                .unwrap_err(),
            SweepError::InvalidPlan(format!(
                "initial_cash must be finite and positive, got {minor}"
            ))
        );
    }
    // NaN and infinity are rejected at construction time by Money::from_major_f64
    for cash in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(Money::from_major_f64(cash, Currency::Inr).is_err());
    }
    for periods in [0.0, -252.0, f64::NAN, f64::INFINITY] {
        assert_eq!(
            plan_with(1)
                .with_periods_per_year(periods)
                .validate()
                .unwrap_err(),
            SweepError::InvalidPlan(format!(
                "periods_per_year must be finite and positive, got {periods}"
            ))
        );
    }
}

#[test]
fn an_empty_plan_is_legal() {
    let plan = plan_with(0);
    assert!(plan.trials().is_empty());
    assert!(plan.validate().is_ok());
}

#[test]
fn a_plan_and_its_trials_can_be_shared_across_worker_threads() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<SweepPlan>();
    assert_send_sync::<TrialParams>();
    assert_send_sync::<StrategySpec>();
    assert_send_sync::<&'static dyn StrategyFactory>();
    assert_send_sync::<&'static dyn Fitness>();
}

#[test]
fn build_erases_every_spec_into_the_same_strategy_type() {
    let specs = [
        StrategySpec::SmaCrossover {
            instrument: any_instrument(),
            fast: 2,
            slow: 3,
            quantity: 1.0,
        },
        StrategySpec::RsiReversal {
            instrument: any_instrument(),
            period: 2,
            oversold: 30.0,
            overbought: 70.0,
            quantity: 1.0,
        },
        StrategySpec::BuyAndHold {
            instrument: any_instrument(),
            quantity: 1.0,
        },
        StrategySpec::Custom {
            inner: Arc::new(Fixed),
        },
    ];
    let names: Vec<String> = specs
        .iter()
        .map(|spec| spec.build(0).name().to_string())
        .collect();
    assert_eq!(
        names,
        vec!["sma_crossover", "rsi_reversal", "buy_and_hold", "fixed"]
    );
}

#[test]
fn a_custom_factory_is_given_the_trial_seed() {
    let spec = StrategySpec::Custom {
        inner: Arc::new(Seeded),
    };
    assert_eq!(spec.build(7).name(), "seed-7");
    assert_eq!(spec.build(11).name(), "seed-11");
}

#[test]
fn built_in_specs_compare_by_value_and_custom_specs_by_factory_identity() {
    let shared: Arc<dyn StrategyFactory> = Arc::new(Fixed);
    let buy_and_hold = StrategySpec::BuyAndHold {
        instrument: any_instrument(),
        quantity: 1.0,
    };
    assert_eq!(buy_and_hold, buy_and_hold);
    assert_ne!(
        buy_and_hold,
        StrategySpec::BuyAndHold {
            instrument: any_instrument(),
            quantity: 2.0,
        }
    );
    assert_eq!(
        StrategySpec::Custom {
            inner: Arc::clone(&shared)
        },
        StrategySpec::Custom {
            inner: Arc::clone(&shared)
        }
    );
    assert_ne!(
        StrategySpec::Custom {
            inner: Arc::clone(&shared)
        },
        StrategySpec::Custom {
            inner: Arc::new(Fixed)
        }
    );
}

#[test]
fn a_spec_renders_readably_including_the_erased_case() {
    let buy_and_hold = StrategySpec::BuyAndHold {
        instrument: any_instrument(),
        quantity: 1.0,
    };
    assert_eq!(
        format!("{buy_and_hold:?}"),
        "BuyAndHold { instrument: \"X.NSE\", quantity: 1.0 }"
    );
    assert_eq!(
        format!(
            "{:?}",
            StrategySpec::Custom {
                inner: Arc::new(Fixed)
            }
        ),
        "Custom(<factory>)"
    );
}

#[test]
fn a_built_in_spec_builds_the_reference_strategy_it_names() {
    let spec = StrategySpec::BuyAndHold {
        instrument: any_instrument(),
        quantity: 4.0,
    };
    let name = spec.build(0).name().to_string();
    assert_eq!(name, BuyAndHold::new(any_instrument(), 4.0).name());
}
