//! The `POST /orders` risk gate (ADR 0018 decision 7): approval queue (E5-S4) ->
//! risk stage -> embedded simulator ([`crate::exec`]).
//!
//! The handler parses the request body, confirms the instrument exists, then runs the risk
//! gate: the operator's [`AppState::trading_state`] (default `Active`; seeded via
//! `AppState::with_trading_state` and changed at runtime by `PUT /trading-state`, read afresh
//! for every request) is checked first (`Halted` refuses every placement, `Reducing` only those
//! that reduce the ledger's existing opposite position), then `honba-mon`'s [`RiskEngine`] runs
//! its own structural/SEBI/limit checks ([`check_risk`]). Approved orders are recorded in the
//! write ledger ([`OrderLedger`]) and submitted to the embedded simulator ([`crate::exec`]);
//! fills arrive when the market advances ([`AppState::advance_market`]) and move the ledger row
//! to `Filled`. Known limits: the simulator handles market orders only (a limit order is
//! `Rejected`) and is long-only (a sell with no simulated holding is `Rejected`). Rows report
//! `filled_qty` / `avg_fill_price` from the simulator's fills and `reject_reason` (a stable
//! `ErrorCode` plus message) from its refusals (ADR 0030). A risk refusal is still the 422
//! below; when the request carries a `client_order_id` it is also recorded as a `Rejected` row
//! (`reject_reason` = the refusal's code and message) and a repeat of that id replays the same
//! 422. A keyless refusal is not recorded (the shared route key would poison later orders).
//!
//! When the approval queue is enabled (E5-S4), placements are held in the queue until approved
//! by an operator. Cancel and modify bypass the queue. A refusal from the risk stage is the 422
//! of ADR 0018 decision 5 and never reaches the simulator. Approved placements land in the
//! [`OrderLedger`] keyed by the ADR 0019 decision 3 idempotency key; the decision 6 audit record
//! is not written yet.
//!
//! `DELETE /orders/{id}` cannot add exposure, so it is never risk-checked and is allowed in
//! every state. `POST /positions/close` takes no body: for every instrument with a non-zero
//! ledger position it generates one market order on the opposite side for the full net quantity
//! (it can only reduce the position, so it passes the reduce-only state). Those orders take the
//! same path as `POST /orders` (approval queue when enabled, else the risk stage, then the
//! ledger), so a halted engine still refuses them.
//!
//! # Rebuilt on this migration, not a rename
//!
//! This file used to depend on a rich, stateful `honba_messages::Order` ledger row (with
//! `.order_id()`/`.with_status()`/etc.), a `RiskStage`/`RiskRequest`/`RiskDecision` facade that
//! sourced per-instrument rules from a market profile (`NullMarketProfile`/`ProfileRulesSource`),
//! and `UnixNanos` timestamps. None of those four have a successor: `honba_core` has no rich
//! stateful order type (only the flat `OrderRequest`/`OrderAck`/`FillReport` wire DTOs), and
//! `honba-mon`'s real surface (confirmed by reading `honba-mon/src/engine.rs`) is
//! `RiskEngine::{check_order, check_order_fast, check_decision}` operating directly on
//! `OrderRequest` with no market-profile rule-sourcing hook at all.
//!
//! So the ledger row ([`Order`], [`OrderStatus`], local [`OrderType`], [`RejectReason`]) is
//! redefined locally in this file (AGENTS.md's zero-floats policy means it carries
//! `rust_decimal::Decimal`, not `f64`, for quantity/price), and the risk gate calls
//! `RiskEngine::check_order` directly (see [`check_risk`]'s doc for the resulting reduced scope:
//! no per-instrument lot/tick/circuit rules sourced from a market profile, and every submission
//! runs under one fixed, pre-registered SEBI algo id, [`REST_ALGO_ID`], since the REST request
//! body has no algo-id concept of its own).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, Utc};
use honba_api::{ErrorCode, ErrorDetail, OrdersQuery, OrdersRequest};
use honba_core::rust_decimal::Decimal;
use honba_core::{InstrumentId, OrderSide, Price, Quantity, TimeInForce};
use honba_mon::RiskRefusal;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::market::{ApiQuery, failure, success};
use crate::state::AppState;

/// Order idempotency key — a plain `String` since `honba_messages::OrderId` was removed in
/// Phase 1.3. Carries the caller-provided `client_order_id` when present, else a
/// route-generated placeholder.
type OrderId = String;

/// The fixed SEBI algo id every `/orders` submission runs under, since the REST request body
/// has no algo-id concept of its own. Registered once, at [`AppState::new`].
pub(crate) const REST_ALGO_ID: &str = "rest-api";

/// Local order lifecycle status for this crate's write ledger.
///
/// Distinct from [`honba_core::OrderStatus`] (a leaner post-ack broker state machine with no
/// `Initialized`/`Submitted`/`Accepted` distinction): this ledger tracks a row from before it
/// ever reaches a venue, which the wire status doesn't model. `honba_messages::OrderStatus` had
/// this exact shape; it has no successor, so this is a genuine local redefinition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// Row created, not yet submitted to the simulator.
    Initialized,
    /// Accepted by the risk gate and handed to the simulator.
    Submitted,
    /// Acknowledged by the simulator, not yet filled.
    Accepted,
    /// Partially filled.
    PartiallyFilled,
    /// Completely filled.
    Filled,
    /// Cancelled.
    Cancelled,
    /// Refused (by the risk gate or the simulator) with no fill.
    Rejected,
    /// Expired unfilled.
    Expired,
}

impl OrderStatus {
    /// A row in this status never changes again.
    fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Cancelled | Self::Rejected | Self::Expired
        )
    }
}

/// Local order-type tag. No embedded limit price (unlike `honba_core::OrderType::Limit`): the
/// ledger row tracks price separately, as `Option<Decimal>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    /// Fills at the simulator's next bar open.
    Market,
    /// Not supported by the embedded simulator (`Rejected`, `unsupported_order_type`).
    Limit,
}

/// A ledger-row rejection: a stable [`ErrorCode`] plus the refusing party's own text.
///
/// `honba_core::order::RejectReason` is a closed 3-variant enum
/// (`ExchangeRejected`/`Expired`/`InsufficientMargin`) with no `::new(code, message)`
/// constructor and doesn't fit; this is the free-form shape ADR 0030 wants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectReason {
    /// The stable wire code.
    pub code: ErrorCode,
    /// Human-readable detail.
    pub message: String,
}

impl RejectReason {
    /// A rejection carrying `code` and `message`.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// One row of the write ledger (ADR 0019 decision 3): the order as submitted, plus whatever the
/// risk gate and the simulator have reported about it since.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    order_id: String,
    instrument_id: InstrumentId,
    side: OrderSide,
    order_type: OrderType,
    quantity: Decimal,
    price: Option<Decimal>,
    #[serde(skip)]
    tif: TimeInForce,
    created_at: DateTime<Utc>,
    status: OrderStatus,
    filled_qty: Decimal,
    avg_fill_price: Option<Decimal>,
    filled_at: Option<DateTime<Utc>>,
    reject_reason: Option<RejectReason>,
}

impl Order {
    /// A new, `Initialized` row. `created_at` is the REST edge's own wall clock (this is the
    /// I/O boundary, not a zero-I/O domain crate — AGENTS.md §2.5's no-clock rule binds
    /// `honba-core`/`honba-kokoro`/`honba-mon`, not this crate).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        order_id: impl Into<String>,
        instrument_id: InstrumentId,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Option<Decimal>,
        tif: TimeInForce,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            order_id: order_id.into(),
            instrument_id,
            side,
            order_type,
            quantity,
            price,
            tif,
            created_at,
            status: OrderStatus::Initialized,
            filled_qty: Decimal::ZERO,
            avg_fill_price: None,
            filled_at: None,
            reject_reason: None,
        }
    }

    /// The idempotency key this row is stored under.
    pub fn order_id(&self) -> &str {
        &self.order_id
    }

    /// The traded instrument.
    pub fn instrument_id(&self) -> &InstrumentId {
        &self.instrument_id
    }

    /// Buy or sell.
    pub fn side(&self) -> OrderSide {
        self.side
    }

    /// Market or limit.
    pub fn order_type(&self) -> OrderType {
        self.order_type
    }

    /// The requested quantity.
    pub fn quantity(&self) -> Decimal {
        self.quantity
    }

    /// The limit price, when this is a limit order.
    pub fn price(&self) -> Option<Decimal> {
        self.price
    }

    /// Time in force.
    pub fn time_in_force(&self) -> TimeInForce {
        self.tif
    }

    /// The row's current lifecycle status.
    pub fn status(&self) -> OrderStatus {
        self.status
    }

    /// Cumulative filled quantity.
    pub fn filled_qty(&self) -> Decimal {
        self.filled_qty
    }

    /// Quantity-weighted average fill price, once any quantity has filled.
    pub fn avg_fill_price(&self) -> Option<Decimal> {
        self.avg_fill_price
    }

    /// Event time of the latest fill that moved [`Order::filled_qty`].
    pub fn filled_at(&self) -> Option<DateTime<Utc>> {
        self.filled_at
    }

    /// The recorded refusal, if this row (or its remainder) was refused.
    pub fn reject_reason(&self) -> Option<&RejectReason> {
        self.reject_reason.as_ref()
    }

    /// Returns `self` with its status changed to `status`.
    pub fn with_status(mut self, status: OrderStatus) -> Self {
        self.status = status;
        self
    }

    /// Returns `self` with `filled_qty`/`avg_fill_price` set.
    pub fn with_fill(mut self, filled_qty: Decimal, avg_price: Decimal) -> Self {
        self.filled_qty = filled_qty;
        self.avg_fill_price = Some(avg_price);
        self
    }

    /// Returns `self` with `filled_at` set.
    pub fn with_filled_at(mut self, ts: DateTime<Utc>) -> Self {
        self.filled_at = Some(ts);
        self
    }

    /// Returns `self` carrying `reason` as its recorded refusal.
    pub fn with_reject_reason(mut self, reason: RejectReason) -> Self {
        self.reject_reason = Some(reason);
        self
    }

    /// Returns `self` with no recorded refusal.
    pub fn without_reject_reason(mut self) -> Self {
        self.reject_reason = None;
        self
    }
}

/// The unsigned quantity row `order` can contribute to the position: the part already filled
/// plus the part that may still fill (see [`OrderLedger::position`]).
fn exposure(order: &Order) -> Decimal {
    let filled = order.filled_qty();
    let remaining = (order.quantity() - filled).max(Decimal::ZERO);
    match order.status() {
        OrderStatus::Filled => order.quantity().max(filled),
        OrderStatus::Submitted | OrderStatus::Accepted => filled + remaining,
        OrderStatus::PartiallyFilled if order.reject_reason().is_none() => filled + remaining,
        // A refused remainder, a cancelled/expired/rejected row: only what filled.
        OrderStatus::PartiallyFilled
        | OrderStatus::Cancelled
        | OrderStatus::Expired
        | OrderStatus::Rejected => filled,
        OrderStatus::Initialized => Decimal::ZERO,
    }
}

/// The in-memory order store behind the write ledger (ADR 0019 decision 3).
///
/// One row per client order id, in insertion order. The store starts empty and only records
/// orders the risk stage approved. Rows are advanced by the embedded simulator (`Submitted` ->
/// `Filled` / `Rejected`); no venue gateway is wired.
#[derive(Clone, Debug, Default)]
pub struct OrderLedger {
    inner: Arc<Mutex<BTreeMap<String, Order>>>,
    /// The full error envelope of every risk refusal recorded as a `Rejected` row, by order id,
    /// so a repeat of the id answers with the same 422 (context included).
    refusals: Arc<Mutex<BTreeMap<String, ErrorDetail>>>,
}

impl OrderLedger {
    /// An empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the status of row `order_id`, returning `false` for an unknown id.
    ///
    /// Terminal rows (`Filled`, `Cancelled`, `Rejected`, `Expired`) are never changed.
    pub fn set_status(&self, order_id: &str, status: OrderStatus) -> bool {
        let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(row) = guard.get_mut(order_id) else {
            return false;
        };
        if row.status().is_terminal() {
            return true;
        }
        *row = row.clone().with_status(status);
        true
    }

    /// Books one fill on row `order_id`: `filled_qty` becomes the simulator's cumulative
    /// `cum_qty` (capped at the order quantity, never decreasing), `avg_fill_price` is the
    /// running quantity-weighted average where each `cum_qty` increase is priced at the fill
    /// that reported it, `filled_at` becomes that fill's event time `ts`, and the status
    /// becomes `status`. A fill that completes the row (`Filled`) clears any `reject_reason`
    /// recorded for a refused remainder.
    ///
    /// A terminal row is left untouched (as in [`OrderLedger::set_status`]); an unknown id
    /// returns `false`.
    pub fn apply_fill(
        &self,
        order_id: &str,
        fill_qty: Decimal,
        fill_price: Decimal,
        cum_qty: Decimal,
        ts: DateTime<Utc>,
        status: OrderStatus,
    ) -> bool {
        let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(row) = guard.get_mut(order_id) else {
            return false;
        };
        if row.status().is_terminal() {
            return true;
        }
        // A venue sending a non-positive price/quantity must not poison the row: drop it.
        if fill_price <= Decimal::ZERO || fill_qty <= Decimal::ZERO || cum_qty < Decimal::ZERO {
            return true;
        }
        // The venue's cumulative quantity is the truth for how much has filled; fill sizes only
        // price it. The quantity newly filled is the growth of `cum_qty` (capped at the order
        // quantity), booked at this fill's price, so a venue whose `cum_qty` disagrees with the
        // sum of fill sizes cannot drift the average. A replayed or stale report (no growth)
        // changes neither quantity nor average.
        let prev_qty = row.filled_qty();
        let filled = cum_qty.min(row.quantity()).max(prev_qty);
        let delta = filled - prev_qty;
        let mut next = row.clone().with_status(status);
        if delta > Decimal::ZERO {
            let prev_avg = row.avg_fill_price().unwrap_or(fill_price);
            let avg = (prev_qty * prev_avg + delta * fill_price) / filled;
            next = next.with_fill(filled, avg).with_filled_at(ts);
        }
        // A completed order was not refused: drop the reason recorded for a remainder that the
        // venue then filled after all.
        if status == OrderStatus::Filled {
            next = next.without_reject_reason();
        }
        *row = next;
        true
    }

    /// Records why the venue refused row `order_id` (or, after a partial fill, its remainder).
    ///
    /// The reason is stored on every non-terminal row. A row with no fill becomes `Rejected`; a
    /// `PartiallyFilled` row keeps its status (the filled part stays exposure). A terminal row
    /// is left untouched; an unknown id returns `false`.
    pub fn reject(&self, order_id: &str, reason: RejectReason) -> bool {
        let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(row) = guard.get_mut(order_id) else {
            return false;
        };
        if row.status().is_terminal() {
            return true;
        }
        let status = if row.status() == OrderStatus::PartiallyFilled {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::Rejected
        };
        *row = row.clone().with_status(status).with_reject_reason(reason);
        true
    }

    /// Records `order` under its id, replacing any earlier row with the same id.
    pub fn record(&self, order: Order) {
        let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        // A replaced row is no longer a recorded refusal.
        self.refusals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(order.order_id());
        guard.insert(order.order_id().to_owned(), order);
    }

    /// Records a risk-refused `order` as a `Rejected` row carrying `detail` as its
    /// `reject_reason`, and remembers `detail` so [`OrderLedger::refusal`] can replay it.
    ///
    /// An id that already has a row is left untouched (the first answer for a key stands).
    pub fn record_refusal(&self, order: Order, detail: ErrorDetail) {
        let id = order.order_id().to_owned();
        let row = order
            .with_status(OrderStatus::Rejected)
            .with_reject_reason(RejectReason::new(detail.code, detail.message.clone()));
        // Both maps change under both locks (always `inner` then `refusals`), so a concurrent
        // repeat never sees the row without its refusal.
        let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if guard.contains_key(&id) {
            return;
        }
        let mut refusals = self.refusals.lock().unwrap_or_else(PoisonError::into_inner);
        refusals.insert(id.clone(), detail);
        guard.insert(id, row);
    }

    /// The error envelope of the risk refusal recorded under `order_id`, if that row is one.
    pub fn refusal(&self, order_id: &str) -> Option<ErrorDetail> {
        self.refusals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(order_id)
            .cloned()
    }

    /// Looks up one row by client order id.
    pub fn get(&self, order_id: &str) -> Option<Order> {
        let guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        guard.get(order_id).cloned()
    }

    /// Signed position of `instrument_id` implied by the ledger: buys positive, sells negative,
    /// summed over each row's exposure ([`exposure`]).
    pub fn position(&self, instrument_id: &InstrumentId) -> Decimal {
        let guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        guard
            .values()
            .filter(|o| o.instrument_id() == instrument_id)
            .map(|o| {
                let qty = exposure(o);
                match o.side() {
                    OrderSide::Buy => qty,
                    OrderSide::Sell => -qty,
                }
            })
            .sum()
    }

    /// The non-zero signed position of every instrument in the ledger, in instrument order (see
    /// [`OrderLedger::position`] for the netting rules).
    pub fn positions(&self) -> Vec<(InstrumentId, Decimal)> {
        let ids: std::collections::BTreeSet<InstrumentId> = {
            let guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
            guard.values().map(|o| o.instrument_id().clone()).collect()
        };
        ids.into_iter()
            .map(|id| {
                let qty = self.position(&id);
                (id, qty)
            })
            .filter(|(_, qty)| !qty.is_zero())
            .collect()
    }

    /// Every row, oldest insertion first.
    pub fn list(&self) -> Vec<Order> {
        let guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        guard.values().cloned().collect()
    }

    /// Rows passing `filter`, oldest insertion first.
    pub fn filter<F>(&self, filter: F) -> Vec<Order>
    where
        F: Fn(&Order) -> bool,
    {
        self.list().into_iter().filter(|order| filter(order)).collect()
    }

    /// Marks the row `order_id` cancelled when it is still working.
    ///
    /// An `Initialized` row never left the submitter, so there is nothing to cancel and it is
    /// left untouched (`false`); a row that is filled, already cancelled, rejected or expired is
    /// terminal and is left untouched (`false`); an unknown id is `None`.
    pub fn cancel(&self, order_id: &str) -> Option<bool> {
        let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let row = guard.get_mut(order_id)?;
        if matches!(
            row.status(),
            OrderStatus::Submitted | OrderStatus::Accepted | OrderStatus::PartiallyFilled
        ) {
            *row = row.clone().with_status(OrderStatus::Cancelled);
            Some(true)
        } else {
            Some(false)
        }
    }
}

/// Whether an order for `side`/`quantity` only reduces `position` (ADR 0018's reduce-only rule
/// for `TradingState::Reducing`): a flat or same-direction order never reduces; an
/// opposite-direction order reduces only up to the held size.
fn reduces_position(position: Decimal, side: OrderSide, quantity: Decimal) -> bool {
    match side {
        OrderSide::Buy => position < Decimal::ZERO && quantity <= -position,
        OrderSide::Sell => position > Decimal::ZERO && quantity <= position,
    }
}

/// The operator trading-state refusal (ADR 0018 rules 1-2), checked before the instrument and
/// `RiskEngine` rules, or `None` when the order may proceed to them.
fn state_refusal(
    state: honba_api::TradingState,
    side: OrderSide,
    quantity: Decimal,
    position: Decimal,
) -> Option<RiskRefusal> {
    match state {
        honba_api::TradingState::Active => None,
        honba_api::TradingState::Halted => Some(RiskRefusal::GlobalHalted {
            reason: "operator halted trading".to_string(),
        }),
        honba_api::TradingState::Reducing if reduces_position(position, side, quantity) => None,
        honba_api::TradingState::Reducing => Some(RiskRefusal::ReduceOnlyViolation {
            details: "platform is reduce-only; order must reduce an existing opposite position"
                .to_string(),
        }),
    }
}

/// Runs the order-gate risk checks (ADR 0018 decision 7): the operator trading-state rule
/// ([`state_refusal`]), then `honba-mon`'s pre-trade [`honba_mon::RiskEngine`] (structural
/// validation, SEBI algo-id/OTR, price bands, quantity/notional/loss/rate limits). `now` for the
/// engine is the instrument's latest quote event time (no wall clock inside the engine call), or
/// the Unix epoch when there is no quote yet.
///
/// # Reduced scope vs. the pre-migration design
///
/// The original design additionally sourced per-instrument rules (lot size, tick size, circuit
/// bands) from a market profile (`NullMarketProfile`/`ProfileRulesSource`) through a
/// `RiskStage`/`RiskRequest`/`RiskDecision` facade. None of those three types exist in the
/// current `honba-mon`; its real surface is `RiskEngine::{check_order, check_order_fast,
/// check_decision}` with no market-profile rule-sourcing hook. Porting that layer back is a
/// design addition to `honba-mon` itself (a different repo, out of this crate's migration
/// scope), so this gate runs only `RiskEngine`'s own built-in checks.
async fn check_risk(
    state: &AppState,
    instrument_id: &InstrumentId,
    side: OrderSide,
    quantity: Decimal,
    price: Option<Decimal>,
) -> Result<(), RiskRefusal> {
    let position = state.orders.position(instrument_id);
    if let Some(refusal) = state_refusal(state.trading_state(), side, quantity, position) {
        return Err(refusal);
    }
    let quote = state
        .quotes
        .read_quote(instrument_id, None)
        .await
        .ok()
        .flatten();
    let now = quote
        .as_ref()
        .map(|q| q.ts_event().to_datetime())
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    let order_type = match price {
        Some(px) => honba_core::OrderType::Limit(Price::new(px)),
        None => honba_core::OrderType::Market,
    };
    let request = honba_core::OrderRequest::new(
        instrument_id.clone(),
        side,
        order_type,
        Quantity::new(quantity),
        REST_ALGO_ID,
    );
    let mut engine = state.risk_engine.lock().unwrap_or_else(PoisonError::into_inner);
    engine.check_order(&request, now)
}

fn invalid(message: impl Into<String>) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::ValidationInvalidRequest, message)
}

fn parse_order(
    req: &OrdersRequest,
) -> Result<(InstrumentId, OrderSide, Decimal, Option<Decimal>), ErrorDetail> {
    let instrument_id = req.instrument_id.clone().ok_or_else(|| {
        ErrorDetail::new(
            ErrorCode::ValidationInvalidRequest,
            "orders need instrument_id",
        )
    })?;
    let side = match req.side.as_deref() {
        Some("buy") => OrderSide::Buy,
        Some("sell") => OrderSide::Sell,
        other => {
            return Err(invalid(format!(
                "orders need side buy or sell, got {other:?}"
            )));
        }
    };
    let quantity = match req.qty {
        Some(qty) if qty > Decimal::ZERO => qty,
        other => {
            return Err(invalid(format!(
                "orders need a positive qty, got {other:?}"
            )));
        }
    };
    let order_type = match req.order_type.as_deref() {
        None | Some("market") => OrderType::Market,
        Some("limit") => OrderType::Limit,
        Some(other) => {
            return Err(invalid(format!("unsupported order_type {other:?}")));
        }
    };
    let price = if order_type == OrderType::Limit {
        match req.price {
            Some(px) => Some(px),
            None => return Err(invalid("limit orders need price")),
        }
    } else {
        None
    };
    Ok((instrument_id, side, quantity, price))
}

/// The `ErrorCode` a `RiskRefusal` is reported under. Only `MaxNotionalExceeded` and
/// `InvalidOrder` have a dedicated code in the current, deliberately small `ErrorCode` taxonomy
/// (see `honba-ya-api/src/wire.rs`); every other refusal is a `Conflict` (the request conflicts
/// with the platform's current risk/trading state).
fn refusal_detail(refusal: &RiskRefusal) -> ErrorDetail {
    let code = match refusal {
        RiskRefusal::MaxNotionalExceeded { .. } => ErrorCode::RiskMaxNotionalExceeded,
        RiskRefusal::InvalidOrder { .. } => ErrorCode::ValidationInvalidRequest,
        _ => ErrorCode::Conflict,
    };
    ErrorDetail::new(code, format!("order refused by risk: {}", refusal.rule()))
        .with_context(serde_json::to_value(refusal).unwrap_or(serde_json::Value::Null))
}

/// The 422 for a risk refusal. With `record` (the caller supplied the idempotency key) the
/// refusal also becomes a `Rejected` ledger row, so `GET /orders` shows it and a repeat of the
/// key replays this answer; the response itself is unchanged. A keyless refusal has no stable
/// identity (the route placeholder key is shared) and is never recorded.
fn refuse(state: &AppState, record: Option<Order>, refusal: &RiskRefusal) -> Response {
    let detail = refusal_detail(refusal);
    if let Some(order) = record {
        state.orders.record_refusal(order, detail.clone());
    }
    failure(StatusCode::UNPROCESSABLE_ENTITY, detail)
}

/// Instrument-master lookup through the [`AppState`] handle.
async fn known_instrument(
    state: &AppState,
    instrument_id: &InstrumentId,
) -> Result<Option<honba_feed::wire::Instrument>, ErrorDetail> {
    crate::market::master_get(state, instrument_id).await
}

/// `Access::Write` (`POST /orders`): approval queue (E5-S4) -> risk stage -> simulator. An
/// approved order is acknowledged with 200 and recorded in the write ledger (E11-S7, ADR 0019
/// decision 3).
///
/// The ledger keys every order by the caller's idempotency key: `client_order_id` when the body
/// carries one, else the route-provided `order_id`. A repeat of a seen key is a duplicate no-op
/// that answers with the first row.
pub(crate) async fn post_orders(
    State(state): State<Arc<AppState>>,
    crate::ApiJson(req): crate::ApiJson<OrdersRequest>,
    order_id: OrderId,
) -> Response {
    let (instrument_id, side, quantity, price) = match parse_order(&req) {
        Ok(v) => v,
        Err(detail) => {
            return failure(StatusCode::UNPROCESSABLE_ENTITY, detail);
        }
    };
    // An unknown instrument is refused here, directly (ADR 0018 decision 4 rule 3), after the
    // state rules 1-2 that `check_risk` runs first.
    match known_instrument(&state, &instrument_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return failure(
                StatusCode::NOT_FOUND,
                ErrorDetail::new(
                    ErrorCode::NotFound,
                    format!("unknown instrument {instrument_id}"),
                ),
            );
        }
        Err(detail) => {
            return failure(StatusCode::INTERNAL_SERVER_ERROR, detail);
        }
    }

    // The caller's key when present, else the route's placeholder; a seen key short-circuits
    // with the first row before the risk stage runs again.
    let ledger_id = req
        .client_order_id
        .as_deref()
        .map(|s| s.to_string())
        .unwrap_or_else(|| order_id.clone());
    if let Some(existing) = state.orders.get(ledger_id.as_str()) {
        if let Some(detail) = state.orders.refusal(ledger_id.as_str()) {
            return failure(StatusCode::UNPROCESSABLE_ENTITY, detail);
        }
        return success(order_json(&existing));
    }

    // E5-S4: If approval queue is enabled, enqueue the order and return 202
    if let Some(queue) = &state.approval_queue {
        let position = match queue.enqueue(ledger_id.clone(), req.clone()) {
            Ok(pos) => pos,
            Err(detail) => return failure(StatusCode::TOO_MANY_REQUESTS, detail),
        };
        #[derive(serde::Serialize)]
        struct QueuedResponse {
            status: &'static str,
            queue_position: usize,
            message: &'static str,
        }
        return success(QueuedResponse {
            status: "queued",
            queue_position: position,
            message: "Order held in approval queue; awaiting operator approval",
        });
    }

    // No approval queue: proceed directly to the risk stage.
    match check_risk(&state, &instrument_id, side, quantity, price).await {
        Ok(()) => {
            let order = Order::new(
                ledger_id,
                instrument_id,
                side,
                order_type_of(&req),
                quantity,
                price,
                TimeInForce::Day,
                Utc::now(),
            )
            .with_status(OrderStatus::Submitted);
            state.orders.record(order.clone());
            state.exec.submit(&state.orders, order.clone());
            success(order_json(
                &state.orders.get(order.order_id()).unwrap_or(order),
            ))
        }
        Err(refusal) => {
            let record = req.client_order_id.as_ref().map(|_| {
                Order::new(
                    ledger_id,
                    instrument_id,
                    side,
                    order_type_of(&req),
                    quantity,
                    price,
                    TimeInForce::Day,
                    Utc::now(),
                )
            });
            refuse(&state, record, &refusal)
        }
    }
}

fn order_type_of(req: &OrdersRequest) -> OrderType {
    match req.order_type.as_deref() {
        Some("limit") => OrderType::Limit,
        _ => OrderType::Market,
    }
}

fn order_json(order: &Order) -> serde_json::Value {
    serde_json::to_value(order).unwrap_or(serde_json::Value::Null)
}

/// The ledger's rows, serialized under `"orders"`.
#[derive(Serialize)]
struct OrdersListResponse {
    orders: Vec<Order>,
}

/// `GET /orders`: the orders the risk stage approved, oldest first, filtered by `symbol` and
/// `exchange` when given. Unknown query keys are a 422, never silently ignored; the ledger
/// starts empty.
pub(crate) async fn get_orders(
    State(state): State<Arc<AppState>>,
    ApiQuery(query): ApiQuery<OrdersQuery>,
) -> Response {
    let orders = state.orders.filter(|order| {
        let symbol_ok = match query.symbol.as_deref() {
            None => true,
            Some(symbol) => order.instrument_id().symbol == symbol,
        };
        let exchange_ok = match query.exchange.as_deref() {
            None => true,
            Some(exchange) => order.instrument_id().exchange.as_str() == exchange,
        };
        symbol_ok && exchange_ok
    });
    success(OrdersListResponse { orders })
}

/// `DELETE /orders/{id}` is never risk-checked and is allowed in every state, including halted:
/// cancelling cannot add exposure. A working ledger row moves to `cancelled`; a terminal row and
/// an unknown id keep their 200 answer so a repeated cancel stays a no-op, not an error.
pub(crate) async fn delete_order(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    if state.orders.cancel(id.as_str()) == Some(true) {
        state.exec.cancel(&state.orders, id.as_str());
    }
    success(serde_json::json!({"status": "cancelled"}))
}

/// `POST /positions/close`: for each instrument with a non-zero ledger position, a market order
/// on the opposite side for the full net quantity, which can only reduce the position (so it
/// passes the reduce-only state). Each order takes the `POST /orders` path: held in the approval
/// queue when one is enabled, else risk checked and recorded; a refusal (for example a halted
/// engine) is returned as is. A flat ledger generates nothing. The body is not read, so there is
/// no filter.
pub(crate) async fn post_close_positions(State(state): State<Arc<AppState>>) -> Response {
    for (instrument_id, net) in state.orders.positions() {
        let side = if net > Decimal::ZERO { "sell" } else { "buy" };
        let key = format!(
            "close-{}.{}-{}",
            instrument_id.symbol,
            instrument_id.exchange,
            state.orders.list().len()
        );
        let req = OrdersRequest {
            instrument_id: Some(instrument_id),
            side: Some(side.to_string()),
            order_type: Some("market".to_string()),
            qty: Some(net.abs()),
            price: None,
            tif: None,
            client_order_id: Some(key.clone()),
        };
        if let Some(queue) = &state.approval_queue {
            if let Err(detail) = queue.enqueue(key.clone(), req) {
                return failure(StatusCode::TOO_MANY_REQUESTS, detail);
            }
        } else if let Err(response) = gate_and_record(&state, &req, key.clone(), false).await {
            return response;
        }
    }
    success(serde_json::json!({"status": "closing"}))
}

/// GET /approval/queue: list pending orders in the approval queue (E5-S4).
pub(crate) async fn get_approval_queue(State(state): State<Arc<AppState>>) -> Response {
    if let Some(queue) = &state.approval_queue {
        let pending = queue.pending();
        success(json!({"queue": pending}))
    } else {
        success(json!({"queue": []}))
    }
}

/// POST /approval/approve: approve the oldest pending order (E5-S4).
pub(crate) async fn post_approve_order(State(state): State<Arc<AppState>>) -> Response {
    if let Some(queue) = &state.approval_queue {
        if let Some(req) = queue.approve_oldest() {
            // Re-process the approved order through the risk stage BYPASSING the queue. The
            // queued row keeps the caller's idempotency key (ADR 0019 decision 3), so approval
            // records under the same key the client first sent.
            let order_id = req
                .client_order_id
                .as_deref()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "approved".to_string());
            post_orders_direct(state.clone(), crate::ApiJson(req), order_id).await
        } else {
            failure(
                StatusCode::NOT_FOUND,
                ErrorDetail::new(ErrorCode::NotFound, "no pending orders in approval queue"),
            )
        }
    } else {
        failure(
            StatusCode::BAD_REQUEST,
            ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                "approval queue is not enabled",
            ),
        )
    }
}

// Internal version of post_orders that bypasses the approval queue (for approved orders)
async fn post_orders_direct(
    state: Arc<AppState>,
    crate::ApiJson(req): crate::ApiJson<OrdersRequest>,
    order_id: OrderId,
) -> Response {
    let keyed = req.client_order_id.is_some();
    match gate_and_record(&state, &req, order_id, keyed).await {
        Ok(order) => success(order_json(&order)),
        Err(response) => response,
    }
}

/// Runs the risk stage for `req` and, when approved, records the order in the ledger. With
/// `record_refusal` a refusal is also recorded as a `Rejected` row (see [`refuse`]).
async fn gate_and_record(
    state: &AppState,
    req: &OrdersRequest,
    order_id: OrderId,
    record_refusal: bool,
) -> Result<Order, Response> {
    let (instrument_id, side, quantity, price) =
        parse_order(req).map_err(|detail| failure(StatusCode::UNPROCESSABLE_ENTITY, detail))?;
    match known_instrument(state, &instrument_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return Err(failure(
                StatusCode::NOT_FOUND,
                ErrorDetail::new(
                    ErrorCode::NotFound,
                    format!("unknown instrument {instrument_id}"),
                ),
            ));
        }
        Err(detail) => return Err(failure(StatusCode::INTERNAL_SERVER_ERROR, detail)),
    }
    match check_risk(state, &instrument_id, side, quantity, price).await {
        Ok(()) => {
            let order = Order::new(
                order_id,
                instrument_id,
                side,
                order_type_of(req),
                quantity,
                price,
                TimeInForce::Day,
                Utc::now(),
            )
            .with_status(OrderStatus::Submitted);
            state.orders.record(order.clone());
            state.exec.submit(&state.orders, order.clone());
            Ok(state.orders.get(order.order_id()).unwrap_or(order))
        }
        Err(refusal) => {
            let record = record_refusal.then(|| {
                Order::new(
                    order_id,
                    instrument_id,
                    side,
                    order_type_of(req),
                    quantity,
                    price,
                    TimeInForce::Day,
                    Utc::now(),
                )
            });
            Err(refuse(state, record, &refusal))
        }
    }
}

/// POST /approval/reject: reject the oldest pending order (E5-S4).
pub(crate) async fn post_reject_order(State(state): State<Arc<AppState>>) -> Response {
    if let Some(queue) = &state.approval_queue {
        if let Some(_req) = queue.reject_oldest() {
            // Order rejected; return the rejection to the client
            success(json!({
                "status": "rejected",
                "message": "Order rejected by operator"
            }))
        } else {
            failure(
                StatusCode::NOT_FOUND,
                ErrorDetail::new(ErrorCode::NotFound, "no pending orders in approval queue"),
            )
        }
    } else {
        failure(
            StatusCode::BAD_REQUEST,
            ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                "approval queue is not enabled",
            ),
        )
    }
}
