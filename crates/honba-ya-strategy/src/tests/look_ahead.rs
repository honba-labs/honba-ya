//! Look-ahead guarantee (ROADMAP E4-S4): a strategy's decisions depend only on the bars it
//! has already seen, so truncating the future never changes an earlier decision.
//!
//! The property is asserted for every reference strategy: run it over the full series, then
//! over each prefix, and require the prefix run's decisions to be exactly the full run's
//! decisions up to the prefix's last bar. A strategy that peeked at a future bar (for example
//! an indicator warmed over the whole series) would fail on the shortest prefix that hides the
//! bar it peeked at.

use honba_engine::{Handler, Result};
use honba_messages::{Bar, InstrumentId, UnixNanos};
use honba_sim::BarFillEngine;
use honba_testing::{VecFeed, fixtures::any_instrument};

use crate::{
    BuyAndHold, OrderIntent, RsiReversal, SmaCrossover, Strategy, StrategyContext, StrategyRunner,
};

/// A synthetic close series with enough turns for the reference strategies to trade.
const CLOSES: [f64; 16] = [
    100.0, 101.0, 103.0, 106.0, 104.0, 101.0, 98.0, 96.0, 99.0, 102.0, 105.0, 107.0, 104.0, 100.0,
    97.0, 95.0,
];

fn series() -> Vec<(f64, u64)> {
    CLOSES
        .iter()
        .enumerate()
        .map(|(i, &close)| (close, (i as u64 + 1) * 1_000))
        .collect()
}

/// Runs `strategy` over `bars` through a [`BarFillEngine`] and returns every submitted intent
/// as `(ts_init, intent)`, in submission order.
///
/// The fill engine is fed each bar before the runner, mirroring the handler order the real
/// `Engine` uses (execution handler first), so a bar's own close is the fill price.
fn decisions<S: Strategy>(strategy: S, bars: &[(f64, u64)]) -> Vec<(u64, OrderIntent)> {
    let mut execution = BarFillEngine::new();
    let mut runner = StrategyRunner::new(strategy, execution.clone());
    runner.on_start().unwrap();
    for (close, ts) in bars {
        let message = VecFeed::bar("X", *close, *ts);
        let event = message.event().clone();
        let at = UnixNanos::from_u64(*ts);
        execution.on_event(&event, at).unwrap();
        runner.on_event(&event, at).unwrap();
    }
    runner
        .submitted()
        .iter()
        .map(|submitted| (submitted.ts_init.as_u64(), submitted.intent.clone()))
        .collect()
}

/// For one strategy: for every prefix of `bars`, the prefix run's decisions are exactly the
/// full run's decisions whose `ts_init` is not past the prefix's last bar.
///
/// `build` receives the data the run is over, so a strategy built from its own data (the
/// vectorized-look-ahead shape the property must catch) sees only the bars it is run on.
fn assert_truncation_property<S: Strategy>(
    name: &str,
    build: impl Fn(&[(f64, u64)]) -> S,
    bars: &[(f64, u64)],
) {
    let full = decisions(build(bars), bars);
    for prefix_len in 1..=bars.len() {
        let prefix = decisions(build(&bars[..prefix_len]), &bars[..prefix_len]);
        let last_ts = bars[prefix_len - 1].1;
        let expected: Vec<(u64, OrderIntent)> = full
            .iter()
            .filter(|(ts, _)| *ts <= last_ts)
            .cloned()
            .collect();
        assert_eq!(
            prefix, expected,
            "{name}: decisions up to bar {prefix_len} changed when the future was truncated"
        );
    }
}

#[test]
fn truncation_property() {
    let bars = series();

    assert_truncation_property(
        "buy_and_hold",
        |_| BuyAndHold::new(any_instrument(), 1.0),
        &bars,
    );
    assert_truncation_property(
        "sma_crossover",
        |_| SmaCrossover::new(any_instrument(), 2, 4, 1.0),
        &bars,
    );
    assert_truncation_property(
        "rsi_reversal",
        |_| RsiReversal::new(any_instrument(), 3, 30.0, 70.0, 1.0),
        &bars,
    );
}

/// A deliberately non-causal strategy: it decides on each bar by looking at the *next*
/// bar's close, so its decision for the last visible bar changes when the future is cut.
struct Peeking {
    instrument: InstrumentId,
    closes: Vec<f64>,
    seen: usize,
}

impl Strategy for Peeking {
    fn name(&self) -> &str {
        "peeking"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> Result<()> {
        let current = self.closes[self.seen];
        self.seen += 1;
        if let Some(&next) = self.closes.get(self.seen) {
            if next > current {
                ctx.submit(OrderIntent::market_buy(self.instrument.clone(), 1.0));
            } else {
                ctx.submit(OrderIntent::market_sell(self.instrument.clone(), 1.0));
            }
        }
        Ok(())
    }
}

/// Negative control: the property must actually reject a strategy that peeks at the future,
/// otherwise a green `truncation_property` proves nothing.
#[test]
#[should_panic(expected = "changed when the future was truncated")]
fn truncation_property_detects_a_peeking_strategy() {
    let bars = series();
    assert_truncation_property(
        "peeking",
        |data| Peeking {
            instrument: any_instrument(),
            closes: data.iter().map(|(close, _)| *close).collect(),
            seen: 0,
        },
        &bars,
    );
}
