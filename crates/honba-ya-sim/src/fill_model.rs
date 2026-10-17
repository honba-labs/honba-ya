//! Seeded fill models (E3-S2).
//!
//! A [`FillModel`] turns the price the simulator *wanted* into the price it
//! books, adding slippage. A stochastic model draws from a [`StdRng`] that the
//! simulator owns and seeds once, so the same seed reproduces the same fills:
//! the acceptance contract is byte-identical fill ledgers for a fixed seed.

use honba_messages::OrderSide;
use rand::rngs::StdRng;
use rand::Rng;

/// Applies slippage to the desired price of a fill.
///
/// [`NoSlippage`] is the default identity model. A model may be deterministic
/// (ignore `rng`) or seeded (draw from it).
pub trait FillModel: Send + Sync {
    /// The price to book for `side` when the engine wanted `desired_price`.
    ///
    /// `rng` is the simulator's seeded stream; a deterministic model may ignore it.
    fn price(&self, side: OrderSide, desired_price: f64, rng: &mut StdRng) -> f64;
}

/// The identity fill model: every fill is at the desired price. The default.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSlippage;

impl FillModel for NoSlippage {
    fn price(&self, _side: OrderSide, desired_price: f64, _rng: &mut StdRng) -> f64 {
        desired_price
    }
}

/// The direction a fixed slippage moves a fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlippageKind {
    /// Worse than desired: a buy pays more, a sell receives less.
    Adverse,
    /// Better than desired: a buy pays less, a sell receives more.
    Favorable,
}

/// A slippage model: a fixed basis-point move, or a seeded stochastic one.
///
/// `Static` is deterministic and never touches the RNG. `Seed` draws a
/// `Normal(0, bps)` move from the simulator's stream; its seed pins that stream
/// (see [`SlippageModel::seed`]), so two runs with the same seed book
/// byte-identical fills. The move is signed against the side: a positive draw
/// makes a buy pay more and a sell receive less, a negative draw the reverse.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SlippageModel {
    /// A fixed `bps` move in the direction `kind`.
    Static {
        /// The size of the move, in basis points of the desired price.
        bps: f64,
        /// Whether the move is against or with the side.
        kind: SlippageKind,
    },
    /// A seeded `Normal(0, bps)` move.
    Seed {
        /// The standard deviation, in basis points of the desired price.
        bps: f64,
        /// The seed that pins the simulator's random stream.
        seed: u64,
    },
}

impl SlippageModel {
    /// The seed a simulator installs for this model: `Some` for a seeded model,
    /// `None` for a deterministic one.
    pub fn seed(&self) -> Option<u64> {
        match self {
            Self::Seed { seed, .. } => Some(*seed),
            Self::Static { .. } => None,
        }
    }
}

impl FillModel for SlippageModel {
    fn price(&self, side: OrderSide, desired_price: f64, rng: &mut StdRng) -> f64 {
        let bps = match self {
            Self::Static { bps, kind } => match kind {
                SlippageKind::Adverse => *bps,
                SlippageKind::Favorable => -*bps,
            },
            Self::Seed { bps, .. } => standard_normal(rng) * bps,
        };
        apply(side, desired_price, bps)
    }
}

/// Moves `price` by `bps` against `side`: a positive `bps` makes a buy pay more
/// and a sell receive less.
fn apply(side: OrderSide, price: f64, bps: f64) -> f64 {
    let signed = match side {
        OrderSide::Sell => -bps,
        _ => bps,
    };
    price * (1.0 + signed / 10_000.0)
}

/// A standard normal draw from the seeded stream (Box-Muller).
fn standard_normal(rng: &mut StdRng) -> f64 {
    // `u1` in `(0, 1]` so the logarithm is finite.
    let u1 = 1.0 - rng.gen::<f64>();
    let u2 = rng.gen::<f64>();
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}
