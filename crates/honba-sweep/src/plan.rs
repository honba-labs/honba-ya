//! What a sweep runs: the trials, the scoring function, and the limits.
//!
//! A [`SweepPlan`] is built once and never mutated. Every `with_*` method
//! consumes the plan and returns the changed one, so a plan can be shared with
//! [`run`](crate::run()) as `&SweepPlan` and no worker can alter what another
//! worker is doing. Nothing in a plan is a handle to anything mutable; the two
//! trait objects it holds ([`Fitness`], [`StrategyFactory`]) are only ever
//! called through `&`.
//!
//! ```
//! use std::sync::Arc;
//! use honba_messages::{Exchange, InstrumentId};
//! use honba_sweep::{SharpeFitness, StrategySpec, SweepPlan, TrialParams};
//!
//! let instrument = InstrumentId::new("NIFTY50", Exchange::new("NSE"));
//! let trials = (5..10)
//!     .map(|slow| TrialParams {
//!         seed: slow as u64,
//!         spec: StrategySpec::SmaCrossover {
//!             instrument: instrument.clone(),
//!             fast: 2,
//!             slow,
//!             quantity: 75.0,
//!         },
//!     })
//!     .collect();
//! let plan = SweepPlan::new(trials, Arc::new(SharpeFitness)).with_max_concurrency(4);
//! assert_eq!(plan.max_concurrency(), 4);
//! assert_eq!(plan.trials().len(), 5);
//! ```

use std::fmt;
use std::sync::Arc;

use honba_entities::{Currency, Money};
use honba_messages::InstrumentId;
use honba_strategy::{BuyAndHold, RsiReversal, SmaCrossover, Strategy};

use crate::error::{Result, SweepError};
use crate::fitness::Fitness;

/// How many trials a sweep runs at once when the caller sets no limit.
///
/// This is a default, not a ceiling: the runtime's blocking pool decides how
/// many trials are actually in flight, and a trial is CPU-bound, so a limit
/// above the number of cores only adds context switching. [`SweepPlan::new`]
/// starts from the number of trials, capped here.
pub const DEFAULT_MAX_CONCURRENCY: usize = 8;

/// Default starting cash, in whole major units of the settlement currency.
const DEFAULT_INITIAL_CASH_MAJOR: i64 = 1_000_000;

/// Default starting cash in `currency`: 1,000,000 major units, scaled by the
/// currency's minor exponent (ADR 0011), so it is the same amount of money in
/// every currency rather than a fixed minor count.
pub fn default_initial_cash(currency: Currency) -> Money {
    Money::new(
        DEFAULT_INITIAL_CASH_MAJOR * 10_i64.pow(u32::from(currency.minor_exponent())),
        currency,
    )
}

/// The `periods_per_year` a trial's return series is annualized with: 252
/// daily bars. It must match the bar interval of the dataset the sweep runs
/// over, which is why it is a setting and not a constant of the platform.
pub const DEFAULT_PERIODS_PER_YEAR: f64 = 252.0;

/// Builds the strategy for one trial.
///
/// A sweep holds a heterogeneous list of strategies before it runs any of them,
/// so each trial's strategy is described by data ([`StrategySpec`]) or built on
/// demand through this trait. `build` is called once per trial, on that trial's
/// own thread, with that trial's seed: a factory is shared by every trial that
/// names it, so it must not keep per-trial state.
pub trait StrategyFactory: Send + Sync {
    /// Returns the strategy for the trial with the given seed.
    fn build(&self, seed: u64) -> Box<dyn Strategy>;
}

/// How to build the strategy for one trial.
///
/// The three named variants cover the reference strategies in
/// [`honba_strategy`]; [`StrategySpec::Custom`] covers everything else, which
/// is how a strategy that does not exist yet still takes part in a sweep. All
/// four erase to the same [`Strategy`] through
/// [`StrategySpec::build`], because a [`StrategyRunner`](honba_strategy::StrategyRunner)
/// is generic over its strategy and a trial list is not.
///
/// Built-in variants compare by value and [`StrategySpec::Custom`] compares by
/// factory identity, which is what lets a [`TrialReport`](crate::TrialReport)
/// stay `PartialEq`: two trials are the same trial when they would build the
/// same strategy the same way.
#[derive(Clone)]
pub enum StrategySpec {
    /// A crossover of two simple moving averages.
    SmaCrossover {
        /// The instrument to trade.
        instrument: InstrumentId,
        /// The fast SMA period.
        fast: usize,
        /// The slow SMA period; must be greater than `fast`.
        slow: usize,
        /// The order quantity emitted on each cross.
        quantity: f64,
    },
    /// A mean-reversion strategy keyed off the relative strength index.
    RsiReversal {
        /// The instrument to trade.
        instrument: InstrumentId,
        /// The RSI period.
        period: usize,
        /// RSI below this is oversold: buy.
        oversold: f64,
        /// RSI above this is overbought: sell.
        overbought: f64,
        /// The order quantity emitted on each signal.
        quantity: f64,
    },
    /// Buy once on the first bar.
    BuyAndHold {
        /// The instrument to buy.
        instrument: InstrumentId,
        /// The order quantity.
        quantity: f64,
    },
    /// Anything else, for strategies that do not exist yet.
    Custom {
        /// The strategy factory.
        inner: Arc<dyn StrategyFactory>,
    },
}

impl fmt::Debug for StrategySpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SmaCrossover {
                instrument,
                fast,
                slow,
                quantity,
            } => f
                .debug_struct("SmaCrossover")
                .field("instrument", &instrument.to_string())
                .field("fast", fast)
                .field("slow", slow)
                .field("quantity", quantity)
                .finish(),
            Self::RsiReversal {
                instrument,
                period,
                oversold,
                overbought,
                quantity,
            } => f
                .debug_struct("RsiReversal")
                .field("instrument", &instrument.to_string())
                .field("period", period)
                .field("oversold", oversold)
                .field("overbought", overbought)
                .field("quantity", quantity)
                .finish(),
            Self::BuyAndHold {
                instrument,
                quantity,
            } => f
                .debug_struct("BuyAndHold")
                .field("instrument", &instrument.to_string())
                .field("quantity", quantity)
                .finish(),
            Self::Custom { inner: _ } => f.write_str("Custom(<factory>)"),
        }
    }
}

impl PartialEq for StrategySpec {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::SmaCrossover {
                    instrument: left_instrument,
                    fast: left_fast,
                    slow: left_slow,
                    quantity: left_quantity,
                },
                Self::SmaCrossover {
                    instrument: right_instrument,
                    fast: right_fast,
                    slow: right_slow,
                    quantity: right_quantity,
                },
            ) => {
                left_instrument == right_instrument
                    && left_fast == right_fast
                    && left_slow == right_slow
                    && left_quantity == right_quantity
            }
            (
                Self::RsiReversal {
                    instrument: left_instrument,
                    period: left_period,
                    oversold: left_oversold,
                    overbought: left_overbought,
                    quantity: left_quantity,
                },
                Self::RsiReversal {
                    instrument: right_instrument,
                    period: right_period,
                    oversold: right_oversold,
                    overbought: right_overbought,
                    quantity: right_quantity,
                },
            ) => {
                left_instrument == right_instrument
                    && left_period == right_period
                    && left_oversold == right_oversold
                    && left_overbought == right_overbought
                    && left_quantity == right_quantity
            }
            (
                Self::BuyAndHold {
                    instrument: left_instrument,
                    quantity: left_quantity,
                },
                Self::BuyAndHold {
                    instrument: right_instrument,
                    quantity: right_quantity,
                },
            ) => left_instrument == right_instrument && left_quantity == right_quantity,
            (Self::Custom { inner: left }, Self::Custom { inner: right }) => {
                Arc::ptr_eq(left, right)
            }
            _ => false,
        }
    }
}

impl StrategySpec {
    /// Builds the strategy for a trial with the given seed.
    ///
    /// The named variants ignore the seed: they have nothing random to seed.
    /// [`StrategySpec::Custom`] passes it to the factory, which is where a
    /// stochastic strategy gets its determinism from.
    ///
    /// # Panics
    ///
    /// Panics if a named variant is given parameters its own strategy refuses
    /// ([`SmaCrossover`] with `fast >= slow` or a zero period,
    /// [`RsiReversal`] with a zero period or inverted thresholds). A sweep
    /// turns that panic into a failed trial rather than losing the run.
    pub fn build(&self, seed: u64) -> Box<dyn Strategy> {
        match self {
            Self::SmaCrossover {
                instrument,
                fast,
                slow,
                quantity,
            } => Box::new(SmaCrossover::new(
                instrument.clone(),
                *fast,
                *slow,
                *quantity,
            )),
            Self::RsiReversal {
                instrument,
                period,
                oversold,
                overbought,
                quantity,
            } => Box::new(RsiReversal::new(
                instrument.clone(),
                *period,
                *oversold,
                *overbought,
                *quantity,
            )),
            Self::BuyAndHold {
                instrument,
                quantity,
            } => Box::new(BuyAndHold::new(instrument.clone(), *quantity)),
            Self::Custom { inner: factory } => factory.build(seed),
        }
    }
}

/// One trial of a sweep: a seed and the strategy to build from it.
///
/// The seed is the trial's whole identity beyond its position in the plan: two
/// trials with equal params must produce equal reports, which is the
/// reproducibility contract [`run`](crate::run()) is built on.
#[derive(Clone, Debug, PartialEq)]
pub struct TrialParams {
    /// The seed handed to the strategy factory.
    pub seed: u64,
    /// How to build the trial's strategy.
    pub spec: StrategySpec,
}

/// What a sweep runs.
///
/// Built with [`SweepPlan::new`] and adjusted with the `with_*` methods, which
/// consume and return the plan, so a plan is fixed once it exists and can be
/// shared across worker threads as `&SweepPlan`.
///
/// An empty trial list is legal and produces an empty report: a sweep with
/// nothing to run is not a mistake the platform has to refuse.
pub struct SweepPlan {
    trials: Vec<TrialParams>,
    fitness: Arc<dyn Fitness>,
    max_concurrency: usize,
    initial_cash: i64,
    currency: Currency,
    periods_per_year: f64,
}

impl SweepPlan {
    /// Creates a plan that runs `trials`, scored by `fitness`.
    ///
    /// `max_concurrency` starts at the number of trials capped at
    /// [`DEFAULT_MAX_CONCURRENCY`], and never below one, so an empty plan is
    /// still a plan that can be run. Cash and periods start at
    /// [`default_initial_cash`] and [`DEFAULT_PERIODS_PER_YEAR`].
    pub fn new(trials: Vec<TrialParams>, fitness: Arc<dyn Fitness>) -> Self {
        let max_concurrency = trials.len().clamp(1, DEFAULT_MAX_CONCURRENCY);
        let cash = default_initial_cash(Currency::Inr);
        Self {
            trials,
            fitness,
            max_concurrency,
            initial_cash: cash.minor(),
            currency: cash.currency(),
            periods_per_year: DEFAULT_PERIODS_PER_YEAR,
        }
    }

    /// Sets how many trials run at once.
    ///
    /// Zero is stored as given and refused by [`run`](crate::run()) with
    /// [`SweepError::InvalidPlan`]: a limit is a plan-level decision, so it is
    /// checked where the plan is used rather than silently repaired here.
    pub fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n;
        self
    }

    /// Sets the cash each trial's ledger starts with.
    pub fn with_initial_cash(mut self, cash: Money) -> Self {
        self.initial_cash = cash.minor();
        self.currency = cash.currency();
        self
    }

    /// Sets the `periods_per_year` a trial's return series is annualized with.
    pub fn with_periods_per_year(mut self, n: f64) -> Self {
        self.periods_per_year = n;
        self
    }

    /// Returns the trials, in the order they will be reported.
    pub fn trials(&self) -> &[TrialParams] {
        &self.trials
    }

    /// Returns how many trials run at once.
    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    /// Returns the cash each trial's ledger starts with.
    pub fn initial_cash(&self) -> i64 {
        self.initial_cash
    }
    /// The settlement currency.
    pub fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the `periods_per_year` a trial's returns are annualized with.
    pub fn periods_per_year(&self) -> f64 {
        self.periods_per_year
    }

    /// Returns the function that scores a finished trial.
    pub fn fitness(&self) -> &dyn Fitness {
        self.fitness.as_ref()
    }

    /// Returns `trial_id`'s params, or `None` when the plan is shorter.
    pub fn trial(&self, trial_id: usize) -> Option<&TrialParams> {
        self.trials.get(trial_id)
    }

    /// Checks that the plan can be run at all.
    ///
    /// Every entry point calls this — [`run`](crate::run()),
    /// [`run_one`](crate::run_one) and [`run_trial`](crate::trial::run_trial) —
    /// so a plan that could not produce a report cannot produce a trial either.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_concurrency == 0 {
            return Err(SweepError::InvalidPlan(format!(
                "max_concurrency must be at least 1, got {}",
                self.max_concurrency
            )));
        }
        let cash = self.initial_cash;
        if !(cash > 0) {
            return Err(SweepError::InvalidPlan(format!(
                "initial_cash must be finite and positive, got {cash}"
            )));
        }
        let periods = self.periods_per_year;
        if !(periods.is_finite() && periods > 0.0) {
            return Err(SweepError::InvalidPlan(format!(
                "periods_per_year must be finite and positive, got {periods}"
            )));
        }
        Ok(())
    }
}
