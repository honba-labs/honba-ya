//! Order intents emitted by strategies.

use std::fmt;

use honba_core::{InstrumentId, OrderRequest, OrderSide, OrderType, Price, Quantity, TimeInForce};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A strategy's desire to trade, before it becomes a concrete
/// [`OrderRequest`].
///
/// Strategies emit intents; the runner converts them into order requests
/// (stamping the `algo_id` and client correlation id). This indirection keeps
/// strategies free of bookkeeping they shouldn't care about.
///
/// Unlike the pre-migration shape, the limit/trigger price lives directly in
/// [`OrderType`] (`honba_core`'s `OrderType::Limit(Price)` /
/// `OrderType::StopLoss(Price)` / `OrderType::StopLimit { trigger, limit }`),
/// so an intent cannot be built with a price that doesn't match its order
/// type — the invalid states this type used to reject at runtime
/// (`MissingPrice`, `UnexpectedPrice`, ...) are no longer representable.
///
/// `honba_core::OrderType` also has no trailing-stop variant, so trailing
/// stops are not supported by this port; use [`OrderIntent::stop_buy`] /
/// [`OrderIntent::stop_sell`] with a price recomputed by the strategy on each
/// event instead.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "OrderIntentRepr")]
pub struct OrderIntent {
    /// The instrument to trade.
    pub instrument_id: InstrumentId,
    /// Buy or sell.
    pub side: OrderSide,
    /// Quantity to trade.
    pub quantity: Quantity,
    /// Execution instruction, with any limit/trigger price embedded.
    pub order_type: OrderType,
    /// How long the order remains active.
    pub time_in_force: TimeInForce,
}

/// Why an [`OrderIntent`] violates its invariants.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub enum IntentError {
    /// Quantity is zero or negative.
    NonPositiveQuantity(Quantity),
}

impl fmt::Display for IntentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IntentError::NonPositiveQuantity(q) => write!(f, "quantity must be positive, got {q}"),
        }
    }
}

impl std::error::Error for IntentError {}

/// The raw wire form, validated into an [`OrderIntent`].
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderIntentRepr {
    instrument_id: InstrumentId,
    side: OrderSide,
    quantity: Quantity,
    order_type: OrderType,
    #[serde(default)]
    time_in_force: TimeInForce,
}

impl TryFrom<OrderIntentRepr> for OrderIntent {
    type Error = IntentError;

    fn try_from(r: OrderIntentRepr) -> Result<Self, Self::Error> {
        let intent = OrderIntent {
            instrument_id: r.instrument_id,
            side: r.side,
            quantity: r.quantity,
            order_type: r.order_type,
            time_in_force: r.time_in_force,
        };
        intent.validate()?;
        Ok(intent)
    }
}

impl OrderIntent {
    fn new(
        instrument_id: InstrumentId,
        side: OrderSide,
        quantity: Quantity,
        order_type: OrderType,
    ) -> Self {
        Self {
            instrument_id,
            side,
            quantity,
            order_type,
            time_in_force: TimeInForce::Day,
        }
    }

    /// A market buy.
    pub fn market_buy(instrument_id: InstrumentId, quantity: Quantity) -> Self {
        Self::new(instrument_id, OrderSide::Buy, quantity, OrderType::Market)
    }

    /// A market sell.
    pub fn market_sell(instrument_id: InstrumentId, quantity: Quantity) -> Self {
        Self::new(instrument_id, OrderSide::Sell, quantity, OrderType::Market)
    }

    /// A limit buy.
    pub fn limit_buy(instrument_id: InstrumentId, quantity: Quantity, price: Price) -> Self {
        Self::new(
            instrument_id,
            OrderSide::Buy,
            quantity,
            OrderType::Limit(price),
        )
    }

    /// A limit sell.
    pub fn limit_sell(instrument_id: InstrumentId, quantity: Quantity, price: Price) -> Self {
        Self::new(
            instrument_id,
            OrderSide::Sell,
            quantity,
            OrderType::Limit(price),
        )
    }

    /// A stop-loss buy that triggers at `trigger_price`.
    pub fn stop_buy(instrument_id: InstrumentId, quantity: Quantity, trigger_price: Price) -> Self {
        Self::new(
            instrument_id,
            OrderSide::Buy,
            quantity,
            OrderType::StopLoss(trigger_price),
        )
    }

    /// A stop-loss sell that triggers at `trigger_price`.
    pub fn stop_sell(
        instrument_id: InstrumentId,
        quantity: Quantity,
        trigger_price: Price,
    ) -> Self {
        Self::new(
            instrument_id,
            OrderSide::Sell,
            quantity,
            OrderType::StopLoss(trigger_price),
        )
    }

    /// A stop-limit buy: at `trigger_price` it becomes a limit buy at `limit_price`.
    pub fn stop_limit_buy(
        instrument_id: InstrumentId,
        quantity: Quantity,
        trigger_price: Price,
        limit_price: Price,
    ) -> Self {
        Self::new(
            instrument_id,
            OrderSide::Buy,
            quantity,
            OrderType::StopLimit {
                trigger: trigger_price,
                limit: limit_price,
            },
        )
    }

    /// A stop-limit sell: at `trigger_price` it becomes a limit sell at `limit_price`.
    pub fn stop_limit_sell(
        instrument_id: InstrumentId,
        quantity: Quantity,
        trigger_price: Price,
        limit_price: Price,
    ) -> Self {
        Self::new(
            instrument_id,
            OrderSide::Sell,
            quantity,
            OrderType::StopLimit {
                trigger: trigger_price,
                limit: limit_price,
            },
        )
    }

    /// Checks the intent's invariants: strictly positive quantity. Every
    /// other invariant the pre-migration type enforced at runtime (a price
    /// matching the order type, a recognized side) is now enforced by the
    /// type system through [`OrderType`] and [`OrderSide`].
    pub fn validate(&self) -> Result<(), IntentError> {
        if !self.quantity.is_positive() {
            return Err(IntentError::NonPositiveQuantity(self.quantity));
        }
        Ok(())
    }

    /// Validates the intent and converts it into a concrete
    /// [`OrderRequest`], stamped with the given `algo_id` (SEBI-mandated).
    ///
    /// This is the only way from an intent to an [`OrderRequest`], so an
    /// intent that breaks its invariants (for example `market_buy(id,
    /// Quantity::ZERO)`) never becomes one.
    pub fn into_order(self, algo_id: impl Into<String>) -> Result<OrderRequest, IntentError> {
        self.validate()?;
        let mut request = OrderRequest::new(
            self.instrument_id,
            self.side,
            self.order_type,
            self.quantity,
            algo_id,
        );
        request.tif = self.time_in_force;
        Ok(request)
    }
}
