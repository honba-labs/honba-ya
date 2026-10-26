//! Pure event-driven backtesting engine enforcing research-live parity.
//!
//! Drives the real, sync, context-driven [`honba_strategy::Strategy`] trait
//! through [`honba_strategy::StrategyRunner`] — the same runner used by live
//! and paper trading — so a backtest gets the identical pre-trade risk
//! gating, warm-up-bar suppression, and intent/order rejection bookkeeping
//! as every other execution mode, instead of reimplementing a subset of it
//! inline.
//!
//! # Research-live parity cost & settlement (Phase 4.2: kokoro canonicalization)
//! Statutory fee computation (`honba_kokoro_ledger::TaxCalculator`),
//! double-entry journal posting (`honba_kokoro_ledger::{Transaction, post_transaction}`),
//! position/PnL tracking (`honba_portfolio::{AccountState, apply_fill_to_account}`),
//! and ICCL T+1 Demat settlement timing & margin holdbacks
//! (`honba_portfolio::{ClearingCalendar, DematHoldings, SettlementEngine}`) are all
//! delegated to the kokoro crates the live-trading actor path also uses.
//!
//! # L2 Order Book Simulation (Phase 4.1)
//! The engine supports optional L2 order book depth simulation with market
//! order execution, limit order matching, spread-crossing penalties, volume
//! participation caps, and adverse fill slippage modeling. Enable via
//! `BacktestConfig::with_l2_simulation()`.
//!
//! # MIS 15:20 Square-off (Phase 4.1)
//! The engine enforces mandatory intraday square-off at 15:20 IST for all
//! open MIS (Margin Intraday Square-off) positions, as per SEBI regulations.

use chrono::{DateTime, NaiveDate, Utc};
use honba_core::{
    CountryCode, Currency, FeeBreakdown, FillReport, InstrumentId, Money, OrderRequest, OrderSide,
    OrderType, Price, Quantity, Tick, TradingCalendar,
};
use honba_kokoro_ledger::{
    Exchange as TaxExchange, ProductType, TaxBreakdown, TaxCalculator, TradeSide, Transaction,
    post_transaction,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

use honba_mon::RiskEngine;
use honba_portfolio::{
    AccountState, ClearingCalendar, DematHoldings, Fill, PositionSide, Product, SettlementEngine,
    SettlementInstruction, apply_fill_to_account,
};
use honba_strategy::{LedgerContext, Strategy, StrategyRunner};

use crate::catalog::{CatalogError, CatalogLoader};
use crate::events::MarketEvent;
use crate::sim::{
    l2_book::OrderBook,
    latency::NetworkLatency,
    mis::{MisSquareOffManager, SquareOffResult},
    slippage::{AdverseFillModel, SlippageModel},
};

/// Convert a `Decimal` share quantity into the `u64` that
/// `honba_portfolio::apply_fill`/`apply_fill_to_account` require.
///
/// Indian equities trade in whole shares, so in practice this conversion is
/// always exact, but the boundary is explicit about what happens otherwise:
/// - The fractional part is TRUNCATED (not rounded), via `Decimal::trunc()`.
/// - A non-positive or otherwise unrepresentable quantity saturates to `0`
///   rather than panicking or silently wrapping.
pub fn decimal_qty_to_u64(qty: Decimal) -> u64 {
    if qty <= Decimal::ZERO {
        return 0;
    }
    qty.trunc().to_u64().unwrap_or(0)
}

/// Map this crate's `ProductType` (used for statutory tax rates) onto
/// `honba_portfolio::Product` (used for T+1 demat-settlement eligibility).
/// Futures/Options never settle via physical demat delivery either way, so
/// both map to `Product::Intraday` (`requires_settlement() == false`),
/// same as a plain intraday equity trade.
fn to_kokoro_product(product: ProductType) -> Product {
    match product {
        ProductType::Delivery => Product::Delivery,
        ProductType::Intraday | ProductType::Futures | ProductType::Options => Product::Intraday,
    }
}

/// Resolve the tax exchange for statutory fee computation from an
/// instrument's country jurisdiction, rather than hardcoding NSE.
///
/// This implements Phase 4.4 item 3 (Instrument Profile Microstructure
/// Resolution): the venue and country are read from the target
/// `Instrument`/`InstrumentProfile` instead of assuming NSE for every
/// order.
fn tax_exchange_for_country(country: CountryCode) -> TaxExchange {
    match country {
        CountryCode::In => TaxExchange::Nse,
        // Non-Indian venues have no BSE equivalent in the kokoro ledger;
        // default to NSE as the canonical cash-segment exchange.
        CountryCode::Us | CountryCode::Uk | CountryCode::Jp => TaxExchange::Nse,
    }
}

/// Apply a fill to the trading account, additionally handling the one case
/// `honba_portfolio::apply_fill` does not cover: a fill in the SAME
/// direction as an already-open position (pyramiding/adding), which
/// `apply_fill` would otherwise misinterpret as a closing/reducing fill.
/// Opening a brand-new position, or reducing/closing/reversing an existing
/// one, both delegate straight to `apply_fill_to_account`.
fn apply_fill_with_add_support(
    account: &mut AccountState,
    instrument: InstrumentId,
    order_side: OrderSide,
    filled_qty: u64,
    fill_price: Price,
) -> Money {
    let new_side = if order_side == OrderSide::Buy {
        PositionSide::Long
    } else {
        PositionSide::Short
    };

    if let Some(idx) = account
        .positions
        .iter()
        .position(|p| p.instrument == instrument)
        && account.positions[idx].side == new_side
    {
        let pos = &mut account.positions[idx];
        let old_qty = pos.quantity.0;
        let add_qty = Decimal::from(filled_qty);
        let new_qty = old_qty + add_qty;
        if new_qty > Decimal::ZERO {
            pos.entry_price =
                Price((pos.entry_price.0 * old_qty + fill_price.0 * add_qty) / new_qty);
        }
        pos.quantity = Quantity::new(new_qty);
        return Money::inr(Decimal::ZERO);
    }

    apply_fill_to_account(account, instrument, new_side, filled_qty, fill_price)
}

/// Configuration for backtest execution with L2 simulation options.
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    /// Initial starting capital in INR
    pub starting_cash: Decimal,
    /// Product type (Delivery vs Intraday)
    pub product_type: ProductType,
    /// Enable L2 order book depth simulation
    pub l2_simulation: bool,
    /// Latency model configuration
    pub latency: Option<NetworkLatency>,
    /// Slippage model configuration
    pub slippage: Option<SlippageModel>,
    /// Adverse fill slippage model across depth tiers
    pub adverse_fill: Option<AdverseFillModel>,
    /// MIS square-off manager
    pub mis_square_off: Option<MisSquareOffManager>,
    /// Number of warm-up bars the strategy must see before any intent it
    /// submits becomes a real order (see [`StrategyRunner::with_warmup_bars`]).
    pub warmup_bars: u32,
    /// ICCL T+1 settlement margin holdback ratio on unsettled delivery sales
    /// (default 20% = `0.20` per SEBI early pay-in / VaR+ELM rules).
    pub iccl_margin_holdback_pct: Decimal,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            starting_cash: dec!(1_000_000.00), // ₹10 Lakhs
            product_type: ProductType::Delivery,
            l2_simulation: false,
            latency: None,
            slippage: None,
            adverse_fill: None,
            mis_square_off: None,
            warmup_bars: 0,
            iccl_margin_holdback_pct: dec!(0.20),
        }
    }
}

impl BacktestConfig {
    /// Create config with L2 simulation enabled.
    pub fn with_l2_simulation(
        starting_cash: Decimal,
        product_type: ProductType,
        latency: NetworkLatency,
        slippage: SlippageModel,
        mis_square_off: MisSquareOffManager,
    ) -> Self {
        Self {
            starting_cash,
            product_type,
            l2_simulation: true,
            latency: Some(latency),
            slippage: Some(slippage),
            adverse_fill: None,
            mis_square_off: Some(mis_square_off),
            warmup_bars: 0,
            iccl_margin_holdback_pct: dec!(0.20),
        }
    }

    /// Attach an [`AdverseFillModel`] for multi-level adverse fill slippage.
    pub fn with_adverse_fill(mut self, model: AdverseFillModel) -> Self {
        self.adverse_fill = Some(model);
        self
    }
}

/// Comprehensive performance and regulatory audit results of a backtest run.
#[derive(Debug, Clone, PartialEq)]
pub struct BacktestResult {
    /// Initial capital in INR
    pub initial_cash: Decimal,
    /// Final equity (settled cash + mark-to-market of live positions)
    pub final_equity: Decimal,
    /// Net PnL after all Indian statutory taxes and transaction fees
    pub net_pnl: Decimal,
    /// Gross trading PnL before fees (FIFO realized PnL from
    /// `honba_portfolio::apply_fill`, summed across every fill)
    pub gross_pnl: Decimal,
    /// Detailed itemized Indian statutory fee breakdown
    pub total_fees: TaxBreakdown,
    /// Total executed trades / fills
    pub total_trades: usize,
    /// Total candidate orders generated by strategy (submitted, suppressed
    /// by warm-up, or rejected by the intent/risk gates)
    pub total_signals: usize,
    /// Number of orders refused by the pre-trade `RiskEngine`
    pub refused_orders: usize,
    /// Maximum portfolio drawdown percentage (e.g. 5.25 for 5.25%)
    pub max_drawdown_pct: Decimal,
    /// Cumulative simulated L2 fill price impact & slippage in INR
    pub price_impact: Decimal,
    /// Every fill booked during the run, in execution order.
    pub fills: Vec<FillReport>,
    /// Mark-to-market equity sampled after every processed event, in
    /// chronological order.
    pub equity_curve: Vec<(DateTime<Utc>, Decimal)>,
    /// Double-entry journal transactions posted to `honba-kokoro-ledger`.
    pub ledger_transactions: Vec<Transaction>,
}

/// An approved, risk-checked order waiting for the L2 book's configured
/// network/broker latency to elapse before it executes.
struct PendingOrder {
    /// The event timestamp at or after which this order may execute.
    ready_ts: DateTime<Utc>,
    /// The order itself.
    order: OrderRequest,
}

/// Event-driven backtester core engine.
pub struct BacktestEngine<S: Strategy> {
    runner: StrategyRunner<S>,
    clearing_calendar: ClearingCalendar,
    settlement_engine: SettlementEngine,
    /// Live (T+0) trading position & FIFO-realized-PnL state, per
    /// `honba_portfolio::apply_fill_to_account`.
    account: AccountState,
    /// ICCL T+1 Demat holdings state (free vs blocked pending settlement).
    demat_holdings: DematHoldings,
    /// Pending T+1 Demat settlement instructions awaiting settlement date.
    pending_instructions: Vec<SettlementInstruction>,

    // L2 order book simulation state
    order_book: Option<OrderBook>,
    // Orders approved and risk-checked but still waiting out network/broker
    // latency before they can execute (see `OrderBook::ready_ts`).
    pending_orders: Vec<PendingOrder>,

    // Slippage and adverse fill models
    slippage_model: Option<SlippageModel>,
    adverse_fill_model: Option<AdverseFillModel>,

    // MIS square-off manager
    mis_square_off: Option<MisSquareOffManager>,

    // Performance & accounting tracking
    initial_cash: Decimal,
    gross_pnl: Decimal,
    total_fees: TaxBreakdown,
    total_price_impact: Decimal,
    /// Cash that has actually settled (T+1-aware, via `ClearingCalendar` +
    /// `SettlementEngine::create_instruction`). Notional and statutory fees
    /// for a fill settle together, on the same settlement date.
    settled_cash: Decimal,
    /// Fills whose cash impact hasn't settled yet: (settle_date, net cash delta).
    pending_settlements: Vec<(NaiveDate, Decimal)>,
    /// Margin holdbacks blocked by ICCL pending T+1 settlement: (settle_date, holdback_amount).
    pending_margin_holdbacks: Vec<(NaiveDate, Decimal)>,
    /// Currently blocked ICCL settlement/delivery margin in INR.
    margin_blocked: Decimal,
    iccl_margin_holdback_pct: Decimal,
    /// Balanced double-entry ledger transactions posted via `honba-kokoro-ledger`.
    ledger_transactions: Vec<Transaction>,
    peak_equity: Decimal,
    max_drawdown: Decimal,
    equity_curve: Vec<(DateTime<Utc>, Decimal)>,

    // Event-loop state
    current_date: Option<NaiveDate>,
    latest_prices: HashMap<InstrumentId, Price>,
    scrips_sold_today: HashSet<InstrumentId>,
    product_type: ProductType,
    /// Net signed quantity held per instrument (positive long, negative
    /// short), updated on every fill. Used by `check_mis_square_off` to
    /// find every instrument with an open position to force-close, rather
    /// than guessing at a single arbitrary instrument.
    open_positions: HashMap<InstrumentId, Decimal>,
    position_sizer: Option<Box<dyn honba_core::ports::PositionSizer>>,
    sizing_method: Option<honba_core::sizing::SizingMethod>,
    sizing_risk_pct: Decimal,
}

impl<S: Strategy> BacktestEngine<S> {
    /// Create a new backtest engine with injected strategy and risk engine.
    ///
    /// Internally wraps `strategy` in a [`StrategyRunner`] seeded with
    /// `config.starting_cash`, `risk_engine` as its pre-trade gate, and
    /// `config.warmup_bars` as its warm-up-bar suppression window.
    ///
    /// `calendar`'s real NSE holiday data (seeded, including Muhurat
    /// sessions) is injected into a `honba_portfolio::ClearingCalendar`
    /// (which otherwise has no hardcoded holiday table of its own) via
    /// `ClearingCalendar::with_holidays`, so T+1 settlement dates respect
    /// the real exchange calendar.
    pub fn new(
        strategy: S,
        risk_engine: RiskEngine,
        config: BacktestConfig,
        calendar: Arc<dyn TradingCalendar>,
    ) -> Self {
        let initial_cash = config.starting_cash;
        let ctx = LedgerContext::with_cash(Money::inr(initial_cash));
        let runner = StrategyRunner::with_context(strategy, ctx)
            .with_risk(risk_engine)
            .with_warmup_bars(config.warmup_bars);

        let clearing_calendar = ClearingCalendar::with_holidays(&calendar.holidays());
        let settlement_engine = SettlementEngine::new(clearing_calendar.clone());

        Self {
            runner,
            clearing_calendar,
            settlement_engine,
            account: AccountState::new(Currency::Inr, Vec::new()),
            demat_holdings: DematHoldings::new(),
            pending_instructions: Vec::new(),
            order_book: if config.l2_simulation {
                Some(OrderBook::new(
                    config
                        .latency
                        .as_ref()
                        .map(|l| l.current_latency())
                        .unwrap_or(0),
                    config
                        .slippage
                        .as_ref()
                        .map(|s| s.daily_range_pct())
                        .unwrap_or_default(),
                ))
            } else {
                None
            },
            pending_orders: Vec::new(),
            slippage_model: config.slippage,
            adverse_fill_model: config.adverse_fill,
            mis_square_off: config.mis_square_off,
            initial_cash,
            gross_pnl: Decimal::ZERO,
            total_fees: TaxBreakdown::default(),
            total_price_impact: Decimal::ZERO,
            settled_cash: initial_cash,
            pending_settlements: Vec::new(),
            pending_margin_holdbacks: Vec::new(),
            margin_blocked: Decimal::ZERO,
            iccl_margin_holdback_pct: config.iccl_margin_holdback_pct,
            ledger_transactions: Vec::new(),
            peak_equity: initial_cash,
            max_drawdown: Decimal::ZERO,
            equity_curve: Vec::new(),
            current_date: None,
            latest_prices: HashMap::new(),
            scrips_sold_today: HashSet::new(),
            product_type: config.product_type,
            open_positions: HashMap::new(),
            position_sizer: None,
            sizing_method: None,
            sizing_risk_pct: dec!(0.01),
        }
    }

    /// Attach dynamic position sizer implementing institutional `PositionSizer` port.
    pub fn with_position_sizer(
        mut self,
        sizer: Box<dyn honba_core::ports::PositionSizer>,
        method: honba_core::sizing::SizingMethod,
        risk_pct: Decimal,
    ) -> Self {
        self.position_sizer = Some(sizer);
        self.sizing_method = Some(method);
        self.sizing_risk_pct = risk_pct;
        self
    }

    /// The attached MIS square-off manager, if any.
    pub fn mis_square_off(&self) -> Option<&MisSquareOffManager> {
        self.mis_square_off.as_ref()
    }

    /// Current T+1 Demat holdings state (free vs blocked pending settlement).
    pub fn demat_holdings(&self) -> &DematHoldings {
        &self.demat_holdings
    }

    /// Currently blocked ICCL settlement/delivery margin in INR.
    pub fn margin_blocked(&self) -> Decimal {
        self.margin_blocked
    }

    /// Current settled cash balance in INR.
    pub fn settled_cash(&self) -> Decimal {
        self.settled_cash
    }

    /// Available trading cash after subtracting ICCL margin holdbacks.
    pub fn available_trading_cash(&self) -> Decimal {
        (self.settled_cash - self.margin_blocked).max(Decimal::ZERO)
    }

    /// Double-entry journal transactions recorded so far.
    pub fn ledger_transactions(&self) -> &[Transaction] {
        &self.ledger_transactions
    }

    /// Load historical bars for `instruments` at `timeframe` from `loader` and run
    /// the deterministic event loop over them.
    pub fn run_catalog(
        &mut self,
        loader: &CatalogLoader,
        instruments: &[InstrumentId],
        timeframe: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<BacktestResult, CatalogError> {
        let events = loader.load_market_events(instruments, timeframe, from, to)?;
        Ok(self.run_events(events))
    }

    /// Process a stream of chronological market events through the complete event loop:
    /// $$\text{MarketEvent} \longrightarrow \text{SignalEvent} \longrightarrow \text{OrderEvent} \longrightarrow \text{FillEvent}$$
    pub fn run_events<I>(&mut self, events: I) -> BacktestResult
    where
        I: IntoIterator<Item = MarketEvent>,
    {
        if let Err(e) = self.runner.on_start() {
            tracing::error!("strategy on_start failed: {e}");
        }

        for event in events {
            self.process_market_event(event);
        }

        if let Err(e) = self.runner.on_stop() {
            tracing::error!("strategy on_stop failed: {e}");
        }

        // Final settlement flush to current date
        if let Some(date) = self.current_date {
            let next_day = self.clearing_calendar.next_trading_day(date);
            self.flush_settlements(next_day);
        }

        let final_equity = self.current_equity();
        let net_pnl = final_equity - self.initial_cash;

        let max_drawdown_pct = if self.peak_equity > Decimal::ZERO {
            (self.max_drawdown / self.peak_equity * dec!(100.00)).round_dp(2)
        } else {
            Decimal::ZERO
        };

        let fills = self.runner.fills().to_vec();
        let total_trades = fills.len();
        let total_signals = self.runner.submitted().len()
            + self.runner.order_rejections().len()
            + self.runner.rejections().len()
            + self.runner.suppressed().len();
        let refused_orders = self.runner.order_rejections().len();

        BacktestResult {
            initial_cash: self.initial_cash,
            final_equity,
            net_pnl,
            gross_pnl: self.gross_pnl,
            total_fees: self.total_fees,
            total_trades,
            total_signals,
            refused_orders,
            max_drawdown_pct,
            price_impact: self.total_price_impact,
            fills,
            equity_curve: self.equity_curve.clone(),
            ledger_transactions: self.ledger_transactions.clone(),
        }
    }

    fn process_market_event(&mut self, event: MarketEvent) {
        let ts = event.timestamp();
        let event_date = ts.date_naive();

        // 1. Check Date Advance for T+1 Settlement & DP charge reset
        if self.current_date != Some(event_date) {
            self.current_date = Some(event_date);
            self.scrips_sold_today.clear();
            self.flush_settlements(event_date);
        }

        // 2. Update price cache
        match &event {
            MarketEvent::Tick(t) => {
                if let Some(p) = t.last_price() {
                    self.latest_prices.insert(t.instrument().clone(), p);
                } else if let Tick::Quote { bid, ask, .. } = t
                    && bid.0 > Decimal::ZERO
                    && ask.0 > Decimal::ZERO
                {
                    let mid = Price(((bid.0 + ask.0) / dec!(2)).round_dp(4));
                    self.latest_prices.insert(t.instrument().clone(), mid);
                }
            }
            MarketEvent::Bar(b) => {
                self.latest_prices.insert(b.instrument.clone(), b.close);
            }
        }

        // 2.5. Execute any previously-approved order whose network/broker
        //    latency has now elapsed, using the price just cached above.
        let mut i = 0;
        while i < self.pending_orders.len() {
            if self.pending_orders[i].ready_ts <= ts {
                let pending = self.pending_orders.remove(i);
                self.execute_order(pending.order, ts);
            } else {
                i += 1;
            }
        }

        // 3. L2 order book simulation: update order book with incoming tick/bar
        if let Some(ob) = self.order_book.as_mut() {
            match &event {
                MarketEvent::Tick(t) => match t {
                    Tick::Ltp { price, volume, .. } => {
                        ob.add_order(*price, volume.0.to_i64().unwrap_or(0));
                    }
                    Tick::Quote {
                        bid,
                        ask,
                        bid_qty,
                        ask_qty,
                        last_price,
                        ..
                    } => {
                        if bid.0 > Decimal::ZERO && ask.0 > Decimal::ZERO {
                            ob.add_bid(*bid, bid_qty.0.to_i64().unwrap_or(0));
                            ob.add_ask(*ask, ask_qty.0.to_i64().unwrap_or(0));
                        } else if let Some(lp) = last_price {
                            let qty = bid_qty.0.max(ask_qty.0).to_i64().unwrap_or(0);
                            ob.add_order(*lp, qty);
                        }
                    }
                    Tick::Depth {
                        bids,
                        asks,
                        last_price,
                        ..
                    } => {
                        if !bids.is_empty() || !asks.is_empty() {
                            ob.set_depth(bids, asks);
                        } else if let Some(lp) = last_price {
                            ob.add_order(*lp, 1);
                        }
                    }
                },
                MarketEvent::Bar(b) => {
                    // Bar approximates close price; add synthetic depth.
                    // Its traded volume is also the simulation's only
                    // observable proxy for average daily volume, which the
                    // participation cap clamps order size against.
                    let volume = b.volume.0.to_i64().unwrap_or(0);
                    ob.set_avg_daily_volume(volume);
                    ob.add_order(b.close, volume);
                }
            }
        }

        // 4. Dispatch the event to the strategy through the runner: sets the
        //    context clock, calls the matching typed hook, then validates,
        //    warm-up-gates and risk-checks whatever intents it submitted.
        let core_event = event.to_core_event();
        if let Err(e) = self.runner.on_event(&core_event, ts) {
            tracing::error!("strategy runner on_event failed: {e}");
        }

        // 5. Drain the risk-checked orders the runner queued. An order
        //    executes immediately unless the L2 book has nonzero latency
        //    configured, in which case it waits in `pending_orders` until a
        //    later event reaches its `ready_ts` (step 2.5 above).
        let orders = self.runner.drain_orders();
        for mut order in orders {
            if let (Some(sizer), Some(method)) = (self.position_sizer.as_ref(), self.sizing_method.clone()) {
                let entry_price = match self.latest_prices.get(&order.instrument) {
                    Some(p) => *p,
                    None => order.order_type.limit_price().unwrap_or(Price(dec!(100.00))),
                };
                let side = match order.side {
                    OrderSide::Buy => honba_core::PositionSide::Long,
                    OrderSide::Sell => honba_core::PositionSide::Short,
                };
                let equity = Money::inr(self.account.cash.amount.max(Decimal::ZERO));
                let budget = honba_core::sizing::RiskBudget::new(
                    equity,
                    self.sizing_risk_pct,
                    Quantity::from_u64(1),
                );
                let req = honba_core::sizing::PositionSizingRequest::new(
                    order.instrument.clone(),
                    side,
                    entry_price,
                    equity,
                    budget,
                    method,
                );
                if let Ok(res) = sizer.calculate_size(&req) {
                    if res.quantity.value() > Decimal::ZERO {
                        order.quantity = res.quantity;
                    }
                }
            }

            let ready_ts = self
                .order_book
                .as_ref()
                .map(|ob| ob.ready_ts(ts))
                .unwrap_or(ts);
            if ready_ts > ts {
                self.pending_orders.push(PendingOrder { ready_ts, order });
            } else {
                self.execute_order(order, ts);
            }
        }

        // 6. MIS square-off check after each execution
        self.check_mis_square_off(ts);

        self.update_drawdown(ts);
    }

    /// Execute an order (already risk-checked and latency-delayed, if
    /// applicable) with the canonical kokoro cost model and optional L2
    /// slippage.
    fn execute_order(&mut self, order: OrderRequest, timestamp: DateTime<Utc>) {
        let instrument = &order.instrument;
        let current_price = match self.latest_prices.get(instrument) {
            Some(p) => *p,
            None => return, // No market price available yet to match
        };

        // Determine order book side and execution mode. `filled_qty` and
        // `remaining_qty` are kept as `Decimal` across both branches (the L2
        // branch's `i64` counts are converted up) so this `if`/`else` type-checks
        // and downstream arithmetic against `Decimal` prices works directly.
        let (filled_qty, mut fill_price, _remaining_qty, mut order_impact) = if let Some(ob) =
            self.order_book.as_mut()
        {
            let qty = order.quantity.0.to_i64().unwrap_or(0);
            let (bid_before, ask_before) = ob.get_spread();
            let pre_depth = ob.best_contra_depth(order.side).max(1);
            let contra_levels_snapshot = ob.contra_levels(order.side).clone();

            let (filled, matched_price, remaining, worst) = match order.order_type {
                OrderType::Limit(limit_price) => {
                    let (f, p, r) = ob.execute_limit_order(order.side, qty, limit_price);
                    (f, p, r, if f > 0 { Some(p) } else { None })
                }
                _ => ob.execute_market_order(order.side, qty),
            };

            if filled <= 0 {
                return;
            }

            // Depth-walk price impact vs current reference price
            let mut impact = (matched_price.0 - current_price.0).abs() * Decimal::from(filled);

            // Spread-crossing & volume-participation slippage calculation
            if let Some(model) = self.slippage_model.as_ref() {
                let spread_penalty =
                    model.compute_spread_crossing_penalty(bid_before, ask_before, matched_price);
                let slip_pct =
                    model.compute_slippage(order.side, filled, pre_depth, matched_price.0);
                let slip_rupees =
                    (matched_price.0 * slip_pct.abs() + spread_penalty) * Decimal::from(filled);
                impact += slip_rupees.round_dp(4);
            }

            // Adverse fill slippage across depth levels, if configured
            let mut final_price = matched_price;
            if let Some(adverse) = self.adverse_fill_model.as_ref() {
                let (adv_pct, adv_worst) = adverse.compute_fill_slippage(
                    order.side,
                    filled,
                    &contra_levels_snapshot,
                    matched_price.0,
                );
                if let Some(wp) = adv_worst {
                    final_price = wp;
                }
                impact +=
                    (matched_price.0 * (adv_pct / dec!(100)) * Decimal::from(filled)).round_dp(4);
            }

            let _ = worst;
            (
                Decimal::from(filled),
                final_price,
                Decimal::from(remaining),
                impact,
            )
        } else {
            // Direct execution at current market price (no L2 simulation)
            (
                order.quantity.0,
                current_price,
                Decimal::ZERO,
                Decimal::ZERO,
            )
        };

        if filled_qty <= Decimal::ZERO {
            return;
        }

        // If slippage model is configured without L2 book, still track slippage impact
        if self.order_book.is_none()
            && let Some(model) = self.slippage_model.as_ref()
        {
            let slip_pct = model.compute_slippage(
                order.side,
                filled_qty.to_i64().unwrap_or(0),
                1_000,
                fill_price.0,
            );
            let adj = (fill_price.0 * slip_pct).round_dp(4);
            match order.side {
                OrderSide::Buy => fill_price = Price(fill_price.0 + adj.abs()),
                OrderSide::Sell => fill_price = Price((fill_price.0 - adj.abs()).max(dec!(0.01))),
            }
            order_impact += (adj.abs() * filled_qty).round_dp(4);
        }

        self.total_price_impact += order_impact;

        let notional = fill_price.0 * filled_qty;
        let is_first_sell = if order.side == OrderSide::Sell {
            self.scrips_sold_today.insert(instrument.clone())
        } else {
            false
        };

        // Compute exact statutory taxes & brokerage via the canonical
        // kokoro TaxCalculator. The exchange is resolved from the
        // instrument's country jurisdiction (Phase 4.4 item 3) rather
        // than hardcoding NSE for every order.
        let tax_side = match order.side {
            OrderSide::Buy => TradeSide::Buy,
            OrderSide::Sell => TradeSide::Sell,
        };
        let country = match order.instrument.exchange {
            honba_core::Exchange::Nse | honba_core::Exchange::Bse | honba_core::Exchange::Mcx => {
                CountryCode::In
            }
        };
        let tax_exchange = tax_exchange_for_country(country);
        let fees: TaxBreakdown = TaxCalculator::compute_charges(
            tax_side,
            self.product_type,
            tax_exchange,
            fill_price.0,
            filled_qty,
            is_first_sell,
        );
        self.total_fees.add_assign(&fees);

        let fill_id = Uuid::new_v4().to_string();

        // Post a balanced double-entry journal transaction to `honba-kokoro-ledger`.
        self.record_ledger_transaction(&fill_id, timestamp, order.side, notional, &fees);

        // `honba_core::FeeBreakdown` has no dedicated slot for the DP
        // (depository participant) charge, so it's folded into
        // `exchange_charges` — the closest "other venue/infra charge"
        // bucket — to keep `fee_breakdown.total()` equal to `fees.total()`.
        let fee_breakdown = FeeBreakdown {
            brokerage: Money::inr(fees.brokerage),
            stt: Money::inr(fees.stt),
            exchange_charges: Money::inr(fees.exchange_turnover + fees.dp_charge),
            gst: Money::inr(fees.gst),
            stamp_duty: Money::inr(fees.stamp_duty),
            sebi_charges: Money::inr(fees.sebi_turnover),
        };
        let fee = fee_breakdown.total();

        let fill_report = FillReport {
            fill_id: fill_id.clone(),
            order_id: order.tag.clone().unwrap_or_default(),
            client_order_id: order.client_order_id,
            instrument: order.instrument.clone(),
            side: order.side,
            price: fill_price,
            quantity: Quantity(filled_qty),
            algo_id: order.algo_id.clone(),
            timestamp,
            correlation_id: order.correlation_id,
            client_id: order.client_id.clone(),
            fee,
            fee_breakdown: Some(fee_breakdown),
            trace_context: None,
        };

        // Research-live parity: feed the fill back through the strategy
        // runner, which books it in the strategy's `LedgerContext`, records
        // it with the attached `RiskEngine`, clears the order's "busy"
        // bookkeeping, and calls `Strategy::on_fill`.
        if let Err(e) = self.runner.on_fill(&fill_report) {
            tracing::error!("strategy runner on_fill failed: {e}");
        }

        // Live (T+0) position & FIFO-realized-PnL tracking, via the
        // canonical kokoro apply_fill (fixed: short PnL and long/short
        // reversal both now compute correctly).
        let filled_qty_u64 = decimal_qty_to_u64(filled_qty);
        let realized = apply_fill_with_add_support(
            &mut self.account,
            order.instrument.clone(),
            order.side,
            filled_qty_u64,
            fill_price,
        );
        self.gross_pnl += realized.amount;

        // Track the net position per instrument so `check_mis_square_off`
        // can find every instrument with an open position, and feed the
        // fill to the MIS square-off manager so its per-instrument
        // eligibility tracking stays live.
        let signed_qty = if order.side == OrderSide::Buy {
            filled_qty
        } else {
            -filled_qty
        };
        *self
            .open_positions
            .entry(order.instrument.clone())
            .or_insert(Decimal::ZERO) += signed_qty;
        if let Some(manager) = self.mis_square_off.as_mut()
            && let Err(e) = manager.process_trade(
                order.instrument.clone(),
                order.side,
                filled_qty.to_i64().unwrap_or(0),
                timestamp,
            )
        {
            tracing::error!("MIS square-off process_trade refused: {e:?}");
        }

        // ICCL T+1 Demat rolling settlement & margin holdback tracking:
        // ClearingCalendar supplies the settlement date (T+1 for Delivery,
        // respecting weekends/holidays; same-day otherwise), and
        // SettlementEngine::create_instruction supplies the signed cash flow.
        let kfill = Fill {
            trade_id: fill_id.clone(),
            instrument: order.instrument.clone(),
            side: if order.side == OrderSide::Buy {
                PositionSide::Long
            } else {
                PositionSide::Short
            },
            quantity: Quantity(filled_qty),
            price: fill_price,
            product: to_kokoro_product(self.product_type),
            timestamp,
            account_id: "default".to_string(),
        };
        let instruction = self
            .settlement_engine
            .create_instruction(&kfill, "default".to_string());

        if instruction.product.requires_settlement() {
            match order.side {
                OrderSide::Buy => {
                    // Block bought Demat shares as unsettled receivable until T+1
                    self.demat_holdings.block_for_settlement(
                        order.instrument.clone(),
                        Quantity(filled_qty),
                        instruction.settle_date,
                        fill_id,
                    );
                    // Upfront purchase obligation + fees blocked until T+1 cash settlement
                    let holdback = notional + fees.total();
                    self.margin_blocked += holdback;
                    self.pending_margin_holdbacks
                        .push((instruction.settle_date, holdback));
                    self.pending_instructions.push(instruction.clone());
                }
                OrderSide::Sell => {
                    // If free Demat shares are already settled, debit them via SettlementEngine
                    // on T+1; if selling unsettled (BTST) or without early pay-in, ICCL holds
                    // back VaR+ELM margin (`iccl_margin_holdback_pct`) until T+1 settlement.
                    let holdback = (notional * self.iccl_margin_holdback_pct).round_dp(2);
                    if holdback > Decimal::ZERO {
                        self.margin_blocked += holdback;
                        self.pending_margin_holdbacks
                            .push((instruction.settle_date, holdback));
                    }
                    if self.demat_holdings.free_holdings(&order.instrument).0 >= filled_qty {
                        self.pending_instructions.push(instruction.clone());
                    }
                }
            }
        }

        self.pending_settlements.push((
            instruction.settle_date,
            instruction.cash_flow.amount - fees.total(),
        ));
    }

    /// Record a balanced double-entry journal transaction in `honba-kokoro-ledger`.
    fn record_ledger_transaction(
        &mut self,
        fill_id: &str,
        timestamp: DateTime<Utc>,
        side: OrderSide,
        notional: Decimal,
        fees: &TaxBreakdown,
    ) {
        let mut debits = BTreeMap::new();
        let mut credits = BTreeMap::new();
        let total_fees = fees.total();

        match side {
            OrderSide::Buy => {
                debits.insert("HoldingsEquityDelivery".to_string(), notional);
                if fees.brokerage > Decimal::ZERO {
                    debits.insert("ExpensesBrokerage".to_string(), fees.brokerage);
                }
                if fees.stt > Decimal::ZERO {
                    debits.insert("ExpensesTaxesSTT".to_string(), fees.stt);
                }
                if fees.stamp_duty > Decimal::ZERO {
                    debits.insert("ExpensesTaxesStampDuty".to_string(), fees.stamp_duty);
                }
                let exch_and_sebi = fees.exchange_turnover + fees.sebi_turnover + fees.dp_charge;
                if exch_and_sebi > Decimal::ZERO {
                    debits.insert("ExpensesExchangeFees".to_string(), exch_and_sebi);
                }
                if fees.gst > Decimal::ZERO {
                    debits.insert("ExpensesTaxesGST".to_string(), fees.gst);
                }
                credits.insert("CashAtBroker".to_string(), notional + total_fees);
            }
            OrderSide::Sell => {
                // Gross sale proceeds + fee expense debits balanced against
                // holdings credit + fee outflow credit
                debits.insert("CashAtBroker".to_string(), notional);
                if fees.brokerage > Decimal::ZERO {
                    debits.insert("ExpensesBrokerage".to_string(), fees.brokerage);
                }
                if fees.stt > Decimal::ZERO {
                    debits.insert("ExpensesTaxesSTT".to_string(), fees.stt);
                }
                if fees.stamp_duty > Decimal::ZERO {
                    debits.insert("ExpensesTaxesStampDuty".to_string(), fees.stamp_duty);
                }
                let exch_and_sebi = fees.exchange_turnover + fees.sebi_turnover + fees.dp_charge;
                if exch_and_sebi > Decimal::ZERO {
                    debits.insert("ExpensesExchangeFees".to_string(), exch_and_sebi);
                }
                if fees.gst > Decimal::ZERO {
                    debits.insert("ExpensesTaxesGST".to_string(), fees.gst);
                }
                credits.insert("HoldingsEquityDelivery".to_string(), notional);
                if total_fees > Decimal::ZERO {
                    credits.insert("LiabilitiesMarginPayable".to_string(), total_fees);
                }
            }
        }

        let tx = Transaction {
            id: fill_id.to_string(),
            timestamp,
            debits,
            credits,
            description: format!("Backtest {:?} fill {}", side, fill_id),
        };
        if post_transaction(&tx).is_ok() {
            self.ledger_transactions.push(tx);
        }
    }

    /// Force-close every instrument with an open MIS (intraday) position
    /// once the 15:20 IST square-off window is reached, per SEBI
    /// regulations. A no-op for delivery (non-MIS) runs: squaring off a
    /// product the broker never auto-closes would be wrong.
    fn check_mis_square_off(&mut self, timestamp: DateTime<Utc>) {
        if self.product_type != ProductType::Intraday {
            return;
        }
        // Cloned so the loop below can call `self.execute_order` (which
        // needs `&mut self`) without holding an immutable borrow of
        // `self.mis_square_off` across it.
        let Some(manager) = self.mis_square_off.clone() else {
            return;
        };

        // Collect first: `execute_order` below mutates `open_positions`, so
        // this can't borrow it while iterating.
        let open: Vec<(InstrumentId, Decimal)> = self
            .open_positions
            .iter()
            .filter(|(_, qty)| **qty != Decimal::ZERO)
            .map(|(id, qty)| (id.clone(), *qty))
            .collect();

        for (instrument, quantity) in open {
            let Some(SquareOffResult::ForcedSquareOff { .. }) =
                manager.check_square_off_time(&instrument, timestamp)
            else {
                continue;
            };
            let side = if quantity > Decimal::ZERO {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };
            let closing_order = OrderRequest::new(
                instrument,
                side,
                OrderType::Market,
                Quantity(quantity.abs()),
                "mis_square_off",
            );
            // Bypasses the strategy's own RiskEngine gate: this is the
            // broker forcing the close, not the strategy choosing to trade.
            self.execute_order(closing_order, timestamp);
        }
    }

    /// Apply every pending settlement whose settle_date has arrived (<= `as_of`).
    fn flush_settlements(&mut self, as_of: NaiveDate) {
        let mut i = 0;
        while i < self.pending_settlements.len() {
            if self.pending_settlements[i].0 <= as_of {
                let (_, amount) = self.pending_settlements.remove(i);
                self.settled_cash += amount;
            } else {
                i += 1;
            }
        }

        // Release ICCL margin holdbacks whose settlement date has arrived
        let mut j = 0;
        while j < self.pending_margin_holdbacks.len() {
            if self.pending_margin_holdbacks[j].0 <= as_of {
                let (_, holdback) = self.pending_margin_holdbacks.remove(j);
                self.margin_blocked = (self.margin_blocked - holdback).max(Decimal::ZERO);
            } else {
                j += 1;
            }
        }

        // Settle due Demat instructions (moves blocked shares into free Demat holdings)
        let mut due_instructions = Vec::new();
        let mut k = 0;
        while k < self.pending_instructions.len() {
            if self.pending_instructions[k].settle_date <= as_of {
                due_instructions.push(self.pending_instructions.remove(k));
            } else {
                k += 1;
            }
        }
        if !due_instructions.is_empty() {
            let _ = self.settlement_engine.process_settlements(
                &mut self.demat_holdings,
                &due_instructions,
                as_of,
            );
        }
    }

    /// Current total equity: settled cash (T+1-aware) plus the live
    /// mark-to-market value of every currently-held trading position,
    /// priced at the most recent tick/bar seen for that instrument.
    fn current_equity(&self) -> Decimal {
        let mut equity = self.settled_cash;
        for pos in &self.account.positions {
            if pos.quantity.0 == Decimal::ZERO {
                continue;
            }
            let mark = self
                .latest_prices
                .get(&pos.instrument)
                .map(|p| p.0)
                .unwrap_or(Decimal::ZERO);
            let signed_qty = match pos.side {
                PositionSide::Long => pos.quantity.0,
                PositionSide::Short => -pos.quantity.0,
            };
            equity += mark * signed_qty;
        }
        equity
    }

    /// Recompute equity, update the running peak/drawdown tracking, and
    /// append the mark-to-market equity curve sample for this event.
    ///
    /// `peak_equity` only ever rises; `max_drawdown` is the largest observed
    /// `peak_equity - current_equity`, which `run_events` later expresses as
    /// a percentage of the peak.
    fn update_drawdown(&mut self, ts: DateTime<Utc>) {
        let equity = self.current_equity();
        if equity > self.peak_equity {
            self.peak_equity = equity;
        }
        let drawdown = self.peak_equity - equity;
        if drawdown > self.max_drawdown {
            self.max_drawdown = drawdown;
        }
        self.equity_curve.push((ts, equity));
    }
}
