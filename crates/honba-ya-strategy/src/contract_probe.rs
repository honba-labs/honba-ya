//! The conformance probe: a reference strategy that exercises the whole contract.

use honba_core::chrono::{DateTime, Utc};
use honba_core::{Bar, FillReport, InstrumentId, Money, OrderSide, Price, Quantity, Tick};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::context::StrategyContext;
use crate::error::Result;
use crate::intent::OrderIntent;
use crate::strategy::Strategy;

/// What [`ContractProbe`] read from its context when a hook ran.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Observation {
    /// The hook (`"on_start"`, `"on_bar"`, ...).
    pub hook: &'static str,
    /// [`StrategyContext::now`].
    pub now: DateTime<Utc>,
    /// [`StrategyContext::position`] of the probe's instrument.
    pub position: Decimal,
    /// [`StrategyContext::cash`], in minor units of the ledger currency.
    pub cash: Money,
    /// [`StrategyContext::busy`] for the probe's instrument.
    pub busy: bool,
    /// The number of [`StrategyContext::positions`].
    pub open_positions: usize,
}

/// Exercises every hook, every context capability and all four order types.
///
/// It is the main strategy of the shared conformance fixture
/// (`schema/conformance/strategy_contract.json`, ADR 008) and mirrors the
/// Python `honba.strategies.reference.ContractProbe` exactly. Each hook first
/// records an [`Observation`]. Quantities use the instrument's lot size and
/// prices its tick size (1.0 and 0.01 when the instrument is unknown).
///
/// - `on_start`: market buy one lot (processed with the first event).
/// - `on_bar`: if not busy, market buy one lot when flat, or sell the
///   position when the close is below the previous close.
/// - `on_quote`: if long and not busy, limit sell one lot at the ask.
/// - `on_trade`: if not busy, stop buy one lot at `price + tick` when flat,
///   or stop-limit sell one lot (trigger `price - tick`, limit
///   `price - 2 * tick`) when long.
/// - `on_fill`: after the first buy fill, stop sell its quantity at
///   `price - 10 * tick` (processed with the next event).
/// - `on_stop`: sell the position (never executed: the run is over).
///
/// ```
/// use honba_strategy::{ContractProbe, Strategy};
/// use honba_core::{Exchange, InstrumentId};
///
/// let probe = ContractProbe::new(InstrumentId::new(Exchange::Nse, "NIFTY50"));
/// assert_eq!(probe.name(), "contract_probe");
/// assert!(probe.observations().is_empty());
/// ```
pub struct ContractProbe {
    instrument_id: InstrumentId,
    observations: Vec<Observation>,
    last_close: Option<Price>,
    protected: bool,
}

impl ContractProbe {
    /// Creates a probe trading `instrument_id`.
    pub fn new(instrument_id: InstrumentId) -> Self {
        Self {
            instrument_id,
            observations: Vec::new(),
            last_close: None,
            protected: false,
        }
    }

    /// Everything the probe observed, one entry per hook call.
    pub fn observations(&self) -> &[Observation] {
        &self.observations
    }

    fn lot(&self, ctx: &dyn StrategyContext) -> Quantity {
        ctx.instrument(&self.instrument_id)
            .map_or(Quantity::from_u64(1), |i| i.lot_size)
    }

    fn tick(&self, ctx: &dyn StrategyContext) -> Price {
        ctx.instrument(&self.instrument_id)
            .map_or(Price::new(Decimal::new(1, 2)), |i| i.tick_size)
    }

    fn observe(&mut self, ctx: &dyn StrategyContext, hook: &'static str) {
        self.observations.push(Observation {
            hook,
            now: ctx.now(),
            position: ctx.position(&self.instrument_id),
            cash: ctx.cash(),
            busy: ctx.busy(&self.instrument_id),
            open_positions: ctx.positions().len(),
        });
    }
}

impl Strategy for ContractProbe {
    fn name(&self) -> &str {
        "contract_probe"
    }

    fn on_start(&mut self, ctx: &mut dyn StrategyContext) -> Result<()> {
        self.observe(ctx, "on_start");
        let lot = self.lot(ctx);
        ctx.submit(OrderIntent::market_buy(self.instrument_id.clone(), lot));
        Ok(())
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, bar: &Bar) -> Result<()> {
        self.observe(ctx, "on_bar");
        let prev = self.last_close.replace(bar.close);
        if ctx.busy(&self.instrument_id) {
            return Ok(());
        }
        let pos = ctx.position(&self.instrument_id);
        if pos.is_zero() {
            let lot = self.lot(ctx);
            ctx.submit(OrderIntent::market_buy(self.instrument_id.clone(), lot));
        } else if pos > Decimal::ZERO && prev.is_some_and(|p| bar.close < p) {
            ctx.submit(OrderIntent::market_sell(
                self.instrument_id.clone(),
                Quantity::new(pos),
            ));
        }
        Ok(())
    }

    fn on_quote(&mut self, ctx: &mut dyn StrategyContext, quote: &Tick) -> Result<()> {
        self.observe(ctx, "on_quote");
        let Tick::Quote { ask, .. } = quote else {
            return Ok(());
        };
        if !ctx.busy(&self.instrument_id) && ctx.position(&self.instrument_id) > Decimal::ZERO {
            let lot = self.lot(ctx);
            ctx.submit(OrderIntent::limit_sell(
                self.instrument_id.clone(),
                lot,
                *ask,
            ));
        }
        Ok(())
    }

    fn on_trade(&mut self, ctx: &mut dyn StrategyContext, trade: &Tick) -> Result<()> {
        self.observe(ctx, "on_trade");
        if ctx.busy(&self.instrument_id) {
            return Ok(());
        }
        let Some(price) = trade.last_price() else {
            return Ok(());
        };
        let (pos, lot, tick) = (
            ctx.position(&self.instrument_id),
            self.lot(ctx),
            self.tick(ctx),
        );
        let id = self.instrument_id.clone();
        if pos.is_zero() {
            ctx.submit(OrderIntent::stop_buy(
                id,
                lot,
                Price::new(price.0 + tick.0),
            ));
        } else if pos > Decimal::ZERO {
            ctx.submit(OrderIntent::stop_limit_sell(
                id,
                Quantity::new(pos),
                Price::new(price.0 - tick.0),
                Price::new(price.0 - tick.0 * Decimal::from(2)),
            ));
        }
        Ok(())
    }

    fn on_fill(&mut self, ctx: &mut dyn StrategyContext, fill: &FillReport) -> Result<()> {
        self.observe(ctx, "on_fill");
        if !self.protected && fill.side == OrderSide::Buy {
            self.protected = true;
            let tick = self.tick(ctx);
            ctx.submit(OrderIntent::stop_sell(
                self.instrument_id.clone(),
                fill.quantity,
                Price::new(fill.price.0 - tick.0 * Decimal::from(10)),
            ));
        }
        Ok(())
    }

    fn on_stop(&mut self, ctx: &mut dyn StrategyContext) -> Result<()> {
        self.observe(ctx, "on_stop");
        let pos = ctx.position(&self.instrument_id);
        if pos > Decimal::ZERO {
            ctx.submit(OrderIntent::market_sell(
                self.instrument_id.clone(),
                Quantity::new(pos),
            ));
        }
        Ok(())
    }
}
