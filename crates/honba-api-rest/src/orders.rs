//! The `POST /orders` risk gate (ADR 0018 decision 7): approval queue (E5-S4) ->
//! risk stage -> execution gateway (an [`crate::ExecutionVenue`]: the embedded `NextOpenSim` by default; no real venue wired yet, E3-S7).
//!
//! The handler builds a [`RiskRequest`] from the request body plus the ports the
//! composition root already wires: rules come from the `null` profile over the
//! `InstrumentMaster` set, `position` is the signed position the write ledger
//! implies for the instrument ([`OrderLedger::position`]), `trading_state` is the
//! app's operator-set [`AppState::trading_state`] (default `Active`; seeded via
//! `AppState::with_trading_state` and changed at runtime by `PUT /trading-state`, read afresh
//! for every request),
//! `reference_price` comes from `QuoteReader` (the close of the latest bar, via
//! mid when a book exists; the limit price when no quote exists), and `ts` /
//! `last_feed_ts` are the latest quote's `ts_event` (no wall clock; both 0 / `None`
//! when no quote exists). Approved orders are also submitted to the embedded simulator ([`crate::exec`]);
//! fills arrive when the market advances ([`AppState::advance_market`]) and move the
//! ledger row to `Filled`. Known limits: market orders only (a limit order is
//! `Rejected` by the simulator) and order rows still carry a zero timestamp. Rows report
//! `filled_qty` / `avg_fill_price` from the venue's fills and `reject_reason` (stable
//! `ErrorCode` plus message) from its refusals (ADR 0030). A risk refusal is still the 422
//! below; when the request carries a `client_order_id` it is also recorded as a `Rejected` row
//! (`reject_reason` = the refusal's code and message) and a repeat of that id replays the
//! same 422. A keyless refusal is not recorded (the shared route key would poison later orders).
//!
//! When the approval queue is enabled (E5-S4), placements are held in the queue
//! until approved by an operator. Cancel and modify bypass the queue.
//! A refusal from the risk stage is the 422 of ADR 0018 decision 5 and never
//! reaches the gateway. Approved placements land in the [`OrderLedger`] keyed by
//! the ADR 0019 decision 3 idempotency key; the decision 6 audit record is not
//! written yet.
//! `DELETE /orders/{id}` cannot add exposure, so it is never risk-checked and is
//! allowed in every state. `POST /positions/close` takes no body: for every instrument
//! with a non-zero ledger position it generates one market order on the opposite side
//! for the full net quantity (it can only reduce the position, so it passes the
//! reduce-only state). Those orders take the same path as `POST /orders` (approval
//! queue when enabled, else the risk stage, then the ledger), so a halted engine still
//! refuses them. `Order` has no reduce-only flag; the reduction is by construction.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use honba_api::{OrdersQuery, OrdersRequest, OrdersResponse};
use honba_market::NullMarketProfile;
use honba_messages::{
    ErrorCode, ErrorDetail, InstrumentId, Order, OrderId, OrderSide, OrderStatus, OrderType,
    RejectReason, TimeInForce, UnixNanos,
};
use honba_risk::{ProfileRulesSource, RiskCheck, RiskDecision, RiskRequest, RiskStage};
use serde_json::json;

use crate::market::{failure, success, ApiQuery};
use crate::state::AppState;

fn is_terminal(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Expired
    )
}

/// The unsigned quantity row `order` can contribute to the position: the part already filled
/// plus the part that may still fill (see [`OrderLedger::position`]).
fn exposure(order: &Order) -> f64 {
    let filled = order.filled_qty();
    let remaining = (order.quantity() - filled).max(0.0);
    match order.status() {
        OrderStatus::Filled => order.quantity().max(filled),
        OrderStatus::Submitted | OrderStatus::Accepted => filled + remaining,
        OrderStatus::PartiallyFilled if order.reject_reason().is_none() => filled + remaining,
        // A refused remainder, a cancelled/expired/rejected row: only what filled.
        OrderStatus::PartiallyFilled
        | OrderStatus::Cancelled
        | OrderStatus::Expired
        | OrderStatus::Rejected => filled,
        _ => 0.0,
    }
}

/// The in-memory order store behind the write ledger (ADR 0019 decision 3).
///
/// One row per client order id, in insertion order. The store starts empty and
/// only records orders the risk stage approved. Rows are advanced by the embedded
/// simulator (`Submitted` -> `Filled` / `Rejected`); no venue gateway is wired.
#[derive(Clone, Debug, Default)]
pub struct OrderLedger {
    inner: Arc<Mutex<BTreeMap<String, Order>>>,
    /// The full error envelope of every risk refusal recorded as a `Rejected` row, by order
    /// id, so a repeat of the id answers with the same 422 (context included).
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
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(row) = guard.get_mut(order_id) else {
            return false;
        };
        if is_terminal(row.status()) {
            return true;
        }
        *row = row.clone().with_status(status);
        true
    }

    /// Books one fill on row `order_id`: `filled_qty` becomes the venue's cumulative `cum_qty`
    /// (capped at the order quantity, never decreasing), `avg_fill_price` is the running
    /// quantity-weighted average where each `cum_qty` increase is priced at the fill that
    /// reported it, `filled_at` becomes that fill's event time `ts`, and the status becomes
    /// `status`. A fill that completes the row
    /// (`Filled`) clears any `reject_reason` recorded for a refused remainder.
    ///
    /// A terminal row is left untouched (as in [`OrderLedger::set_status`]); an unknown id
    /// returns `false`.
    pub fn apply_fill(
        &self,
        order_id: &str,
        fill_qty: f64,
        fill_price: f64,
        cum_qty: f64,
        ts: UnixNanos,
        status: OrderStatus,
    ) -> bool {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(row) = guard.get_mut(order_id) else {
            return false;
        };
        if is_terminal(row.status()) {
            return true;
        }
        // A venue sending a non-finite or non-positive price/quantity must not poison the row
        // (it would fail validation or serialization of `GET /orders`): drop such a fill.
        if !(fill_price.is_finite() && fill_price > 0.0 && fill_qty.is_finite() && fill_qty > 0.0)
            || !cum_qty.is_finite()
        {
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
        if delta > 0.0 {
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
    /// The reason is stored on every non-terminal row. A row with no fill becomes `Rejected`;
    /// a `PartiallyFilled` row keeps its status (the filled part stays exposure). A terminal
    /// row is left untouched; an unknown id returns `false`.
    pub fn reject(&self, order_id: &str, reason: RejectReason) -> bool {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(row) = guard.get_mut(order_id) else {
            return false;
        };
        if is_terminal(row.status()) {
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
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A replaced row is no longer a recorded refusal.
        self.refusals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(order.order_id().as_str());
        guard.insert(order.order_id().as_str().to_owned(), order);
    }

    /// Records a risk-refused `order` as a `Rejected` row carrying `detail` as its
    /// `reject_reason`, and remembers `detail` so [`OrderLedger::refusal`] can replay it.
    ///
    /// An id that already has a row is left untouched (the first answer for a key stands).
    pub fn record_refusal(&self, order: Order, detail: ErrorDetail) {
        let id = order.order_id().as_str().to_owned();
        let row = order
            .with_status(OrderStatus::Rejected)
            .with_reject_reason(RejectReason::new(detail.code, detail.message.clone()));
        // Both maps change under both locks (always `inner` then `refusals`), so a concurrent
        // repeat never sees the row without its refusal.
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.contains_key(&id) {
            return;
        }
        let mut refusals = self
            .refusals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        refusals.insert(id.clone(), detail);
        guard.insert(id, row);
    }

    /// The error envelope of the risk refusal recorded under `order_id`, if that row is one.
    pub fn refusal(&self, order_id: &str) -> Option<ErrorDetail> {
        self.refusals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(order_id)
            .cloned()
    }

    /// Looks up one row by client order id.
    pub fn get(&self, order_id: &str) -> Option<Order> {
        let guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.get(order_id).cloned()
    }

    /// Signed position of `instrument_id` implied by the ledger: buys positive, sells
    /// negative, summed over each row's exposure ([`exposure`]).
    ///
    /// A row's exposure is what it has filled plus what may still fill: a working row
    /// (`Submitted`, `Accepted`, `PartiallyFilled`) counts at its full quantity, the filled
    /// part plus the remainder, a conservative upper bound which is the safe side for risk
    /// rules. A `PartiallyFilled` row whose remainder the venue refused (it carries a
    /// `reject_reason`) counts only what filled, as does a row that stopped working after a
    /// partial fill (`Cancelled`, `Expired`, `Rejected`). A `Filled` row counts at its
    /// quantity; a row that never filled and no longer works, and an `Initialized` row, add
    /// nothing.
    pub fn position(&self, instrument_id: &InstrumentId) -> f64 {
        let guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .values()
            .filter(|o| o.instrument_id() == instrument_id)
            .map(|o| {
                let qty = exposure(o);
                match o.side() {
                    OrderSide::Buy => qty,
                    OrderSide::Sell => -qty,
                    _ => 0.0,
                }
            })
            .sum()
    }

    /// The non-zero signed position of every instrument in the ledger, in
    /// instrument order (see [`OrderLedger::position`] for the netting rules).
    pub fn positions(&self) -> Vec<(InstrumentId, f64)> {
        let ids: std::collections::BTreeSet<InstrumentId> = {
            let guard = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.values().map(|o| o.instrument_id().clone()).collect()
        };
        ids.into_iter()
            .map(|id| {
                let qty = self.position(&id);
                (id, qty)
            })
            .filter(|(_, qty)| qty.abs() > 1e-9)
            .collect()
    }

    /// Every row, oldest insertion first.
    pub fn list(&self) -> Vec<Order> {
        let guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.values().cloned().collect()
    }

    /// Rows passing `filter`, oldest insertion first.
    pub fn filter<F>(&self, filter: F) -> Vec<Order>
    where
        F: Fn(&Order) -> bool,
    {
        self.list()
            .into_iter()
            .filter(|order| filter(order))
            .collect()
    }

    /// Marks the row `order_id` cancelled when it is still working (ADR 0019:
    /// `CancelRequested` is illegal from `Initialized` and from terminal states).
    ///
    /// An `Initialized` row never left the submitter, so there is nothing to
    /// cancel and it is left untouched (`false`); a row that is filled, already
    /// cancelled, rejected or expired is terminal and is left untouched
    /// (`false`); an unknown id is `None`.
    pub fn cancel(&self, order_id: &str) -> Option<bool> {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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

/// Builds the order-gate [`RiskRequest`].
///
/// * `position`: the signed ledger position of the instrument ([`OrderLedger::position`]).
/// * `ts` / `last_feed_ts`: this crate has no clock (and domain code must not read the
///   wall clock), so event time is the `ts_event` of the instrument's latest quote from
///   the `QuoteReader`. With a quote both are that value (feed age 0); with none, `ts`
///   is 0 and `last_feed_ts` is `None` (a staleness rule, if configured, then refuses).
async fn risk_request(
    state: &AppState,
    order_id: OrderId,
    instrument_id: &InstrumentId,
    side: OrderSide,
    quantity: f64,
    price: Option<f64>,
    reference_price: Option<f64>,
) -> RiskRequest {
    let quote = match state.quotes.read_quote(instrument_id, None).await {
        Ok(Some(quote)) => Some(quote),
        _ => None,
    };
    let feed_ts = quote.as_ref().map(|q| q.ts_event());
    // The touch a market order would cross: ask for a buy, bid for a sell (price collar).
    let expected_price = quote.as_ref().map(|q| match side {
        OrderSide::Buy => q.ask_price(),
        _ => q.bid_price(),
    });
    RiskRequest {
        order_id,
        instrument_id: instrument_id.clone(),
        side,
        quantity,
        price,
        trigger_price: None,
        reference_price,
        expected_price,
        adv: None,
        position: state.orders.position(instrument_id),
        trading_state: state.trading_state(),
        ts: feed_ts.unwrap_or_else(|| UnixNanos::new(0)),
        last_feed_ts: feed_ts,
    }
}

fn invalid(message: impl Into<String>) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::ValidationInvalidRequest, message)
}

fn parse_order(
    req: &OrdersRequest,
) -> Result<(InstrumentId, OrderSide, f64, Option<f64>), ErrorDetail> {
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
        Some(qty) if qty.is_finite() && qty > 0.0 => qty,
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

fn refusal_detail(refusal: &honba_risk::RiskRefusal) -> ErrorDetail {
    ErrorDetail::new(
        refusal.error_code(),
        format!("order refused by risk: {}", refusal.rule()),
    )
    .with_context(refusal.context())
}

/// The 422 for a risk refusal. With `record` (the caller supplied the idempotency key) the
/// refusal also becomes a `Rejected` ledger row, so `GET /orders` shows it and a repeat of the
/// key replays this answer; the response itself is unchanged. A keyless refusal has no stable
/// identity (the route placeholder key is shared) and is never recorded.
fn refuse(state: &AppState, record: Option<Order>, refusal: &honba_risk::RiskRefusal) -> Response {
    let detail = refusal_detail(refusal);
    if let Some(order) = record {
        state.orders.record_refusal(order, detail.clone());
    }
    failure(StatusCode::UNPROCESSABLE_ENTITY, detail)
}

/// Method call on the instrument master through the [`AppState`] handle.
async fn known_instrument(
    state: &AppState,
    instrument_id: &InstrumentId,
) -> Result<Option<honba_entities::Instrument>, ErrorDetail> {
    crate::market::master_get(state, instrument_id).await
}

/// Last-price lookup through the [`AppState`] quote handle.
async fn last_price(state: &AppState, instrument_id: &InstrumentId) -> Option<f64> {
    crate::market::quote_last(state, instrument_id).await
}

/// `Access::Write` (`POST /orders`): approval queue (E5-S4) -> risk stage -> gateway.
/// An approved order is acknowledged with 200 and recorded in the write ledger
/// (E11-S7, ADR 0019 decision 3).
///
/// The ledger keys every order by the caller's idempotency key: `client_order_id`
/// when the body carries one, else the route-provided `order_id`. A repeat of a
/// seen key is a duplicate no-op that answers with the first row.
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
    let instrument = match known_instrument(&state, &instrument_id).await {
        Ok(instrument) => instrument,
        Err(detail) => {
            return failure(StatusCode::INTERNAL_SERVER_ERROR, detail);
        }
    };
    let reference_price = match last_price(&state, &instrument_id).await {
        Some(px) => Some(px),
        None => price,
    };
    let profile = Arc::new(NullMarketProfile::default());
    // An unknown instrument contributes no rules, so the stage itself raises
    // `InstrumentUnknown` at rule 3, after the state rules 1-2 (ADR 0018
    // decision 4); resolving it here first would hoist rule 3 above them.
    let mut stage = match RiskStage::new(
        state.risk_limits.clone(),
        state.account_currency,
        Arc::new(ProfileRulesSource::new(profile, instrument)),
    ) {
        Ok(stage) => stage,
        Err(e) => {
            return failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorDetail::new(ErrorCode::InternalError, e.to_string()),
            );
        }
    };

    // The caller's key when present, else the route's placeholder; a seen key
    // short-circuits with the first row before the risk stage runs again.
    let ledger_id = req
        .client_order_id
        .as_deref()
        .map(OrderId::new)
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

    // No approval queue: proceed directly to risk stage
    let request = risk_request(
        &state,
        ledger_id.clone(),
        &instrument_id,
        side,
        quantity,
        price,
        reference_price,
    )
    .await;
    match stage.check(&request) {
        RiskDecision::Approved => {
            let order = Order::new(
                ledger_id,
                instrument_id,
                side,
                order_type_of(&req),
                quantity,
                price,
                TimeInForce::Day,
                UnixNanos::new(0),
                UnixNanos::new(0),
            )
            .with_status(OrderStatus::Submitted);
            state.orders.record(order.clone());
            state.exec.submit(&state.orders, order.clone());
            success(order_json(
                &state.orders.get(order.order_id().as_str()).unwrap_or(order),
            ))
        }
        RiskDecision::Refused(refusal) => {
            let record = req.client_order_id.as_ref().map(|_| {
                Order::new(
                    ledger_id,
                    instrument_id,
                    side,
                    order_type_of(&req),
                    quantity,
                    price,
                    TimeInForce::Day,
                    UnixNanos::new(0),
                    UnixNanos::new(0),
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

/// `GET /orders`: the orders the risk stage approved, oldest first, filtered by
/// `symbol` and `exchange` when given. Unknown query keys are a 422, never
/// silently ignored; the ledger starts empty.
pub(crate) async fn get_orders(
    State(state): State<Arc<AppState>>,
    ApiQuery(query): ApiQuery<OrdersQuery>,
) -> Response {
    let orders = state.orders.filter(|order| {
        let symbol_ok = match query.symbol.as_deref() {
            None => true,
            Some(symbol) => order.instrument_id().symbol() == symbol,
        };
        let exchange_ok = match query.exchange.as_deref() {
            None => true,
            Some(exchange) => order.instrument_id().exchange().as_str() == exchange,
        };
        symbol_ok && exchange_ok
    });
    success(OrdersResponse { orders })
}

/// `DELETE /orders/{id}` is never risk-checked and is allowed in every state,
/// including halted: cancelling cannot add exposure. A working ledger row moves
/// to `cancelled`; a terminal row and an unknown id keep their 200 answer so a
/// repeated cancel stays a no-op, not an error.
pub(crate) async fn delete_order(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    if state.orders.cancel(id.as_str()) == Some(true) {
        state
            .exec
            .cancel(&state.orders, id.as_str(), UnixNanos::new(0));
    }
    success(serde_json::json!({"status": "cancelled"}))
}

/// `POST /positions/close`: for each instrument with a non-zero ledger position,
/// a market order on the opposite side for the full net quantity, which can only
/// reduce the position (so it passes the reduce-only state). Each order takes the
/// `POST /orders` path: held in the approval queue when one is enabled, else risk
/// checked and recorded; a refusal (for example a halted engine) is returned as is.
/// A flat ledger generates nothing. The body is not read, so there is no filter.
pub(crate) async fn post_close_positions(State(state): State<Arc<AppState>>) -> Response {
    for (instrument_id, net) in state.orders.positions() {
        let side = if net > 0.0 { "sell" } else { "buy" };
        let key = format!(
            "close-{}.{}-{}",
            instrument_id.symbol(),
            instrument_id.exchange(),
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
            if let Err(detail) = queue.enqueue(OrderId::new(key), req) {
                return failure(StatusCode::TOO_MANY_REQUESTS, detail);
            }
        } else if let Err(response) = gate_and_record(&state, &req, OrderId::new(key), false).await
        {
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
            // Re-process the approved order through the risk stage BYPASSING the queue.
            // The queued row keeps the caller's idempotency key (ADR 0019 decision 3),
            // so approval records under the same key the client first sent.
            let order_id = req
                .client_order_id
                .as_deref()
                .map(honba_messages::OrderId::new)
                .unwrap_or_else(|| honba_messages::OrderId::new("approved"));
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

/// Runs the risk stage for `req` and, when approved, records the order in the ledger.
/// With `record_refusal` a refusal is also recorded as a `Rejected` row (see [`refuse`]).
async fn gate_and_record(
    state: &AppState,
    req: &OrdersRequest,
    order_id: OrderId,
    record_refusal: bool,
) -> Result<Order, Response> {
    let (instrument_id, side, quantity, price) =
        parse_order(req).map_err(|detail| failure(StatusCode::UNPROCESSABLE_ENTITY, detail))?;
    let instrument = known_instrument(state, &instrument_id)
        .await
        .map_err(|detail| failure(StatusCode::INTERNAL_SERVER_ERROR, detail))?;
    let reference_price = match last_price(state, &instrument_id).await {
        Some(px) => Some(px),
        None => price,
    };
    let profile = Arc::new(NullMarketProfile::default());
    let mut stage = RiskStage::new(
        state.risk_limits.clone(),
        state.account_currency,
        Arc::new(ProfileRulesSource::new(profile, instrument)),
    )
    .map_err(|e| {
        failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorDetail::new(ErrorCode::InternalError, e.to_string()),
        )
    })?;
    let request = risk_request(
        state,
        order_id.clone(),
        &instrument_id,
        side,
        quantity,
        price,
        reference_price,
    )
    .await;
    match stage.check(&request) {
        RiskDecision::Approved => {
            let order = Order::new(
                order_id,
                instrument_id,
                side,
                order_type_of(req),
                quantity,
                price,
                TimeInForce::Day,
                UnixNanos::new(0),
                UnixNanos::new(0),
            )
            .with_status(OrderStatus::Submitted);
            state.orders.record(order.clone());
            state.exec.submit(&state.orders, order.clone());
            Ok(state.orders.get(order.order_id().as_str()).unwrap_or(order))
        }
        RiskDecision::Refused(refusal) => {
            let record = record_refusal.then(|| {
                Order::new(
                    order_id,
                    instrument_id,
                    side,
                    order_type_of(req),
                    quantity,
                    price,
                    TimeInForce::Day,
                    UnixNanos::new(0),
                    UnixNanos::new(0),
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
