//! Structured tracing spans instrumenting the complete order lifecycle.
//!
//! Spans conform to the lifecycle:
//! `OrderGenerated` -> `RiskEvaluated` -> `BrokerSubmitted` -> `BrokerAcked` -> `Filled` / `Cancelled`
//!
//! Every span incorporates:
//! - `algo_id`
//! - `client_id`
//! - `correlation_id`
//! - Latency markers and execution metadata.

use chrono::Utc;
use honba_core::{FillReport, OrderAck, OrderRequest};
use tracing::{Span, info_span};
use uuid::Uuid;

/// Structured span emitted when a strategy generates an order candidate.
pub fn span_order_generated(req: &OrderRequest) -> Span {
    info_span!(
        "OrderGenerated",
        algo_id = %req.algo_id,
        client_id = %req.client_id_str(),
        correlation_id = %req.correlation_id(),
        client_order_id = %req.client_order_id,
        symbol = %req.instrument.symbol,
        exchange = %req.instrument.exchange,
        side = ?req.side,
        quantity = %req.quantity,
        limit_price = ?req.order_type.limit_price(),
        generated_ts_ms = %Utc::now().timestamp_millis(),
    )
}

/// Structured span emitted when pre-trade risk evaluation completes.
pub fn span_risk_evaluated(
    req: &OrderRequest,
    eval_duration_us: u64,
    approved: bool,
    refusal: Option<&str>,
) -> Span {
    info_span!(
        "RiskEvaluated",
        algo_id = %req.algo_id,
        client_id = %req.client_id_str(),
        correlation_id = %req.correlation_id(),
        client_order_id = %req.client_order_id,
        symbol = %req.instrument.symbol,
        eval_duration_us = eval_duration_us,
        approved = approved,
        refusal_reason = refusal.unwrap_or("none"),
        timestamp_ms = %Utc::now().timestamp_millis(),
    )
}

/// Structured span emitted when an approved order is dispatched to the broker adapter.
pub fn span_broker_submitted(req: &OrderRequest, broker: &str) -> Span {
    info_span!(
        "BrokerSubmitted",
        algo_id = %req.algo_id,
        client_id = %req.client_id_str(),
        correlation_id = %req.correlation_id(),
        client_order_id = %req.client_order_id,
        symbol = %req.instrument.symbol,
        broker = %broker,
        submit_ts_ms = %Utc::now().timestamp_millis(),
    )
}

/// Structured span emitted when the broker acknowledges order placement.
pub fn span_broker_acked(req: &OrderRequest, ack: &OrderAck, dispatch_duration_us: u64) -> Span {
    info_span!(
        "BrokerAcked",
        algo_id = %req.algo_id,
        client_id = %req.client_id_str(),
        correlation_id = %ack.correlation_id(),
        client_order_id = %ack.client_order_id,
        order_id = %ack.order_id,
        status = ?ack.status,
        dispatch_duration_us = dispatch_duration_us,
        ack_ts_ms = %ack.timestamp.timestamp_millis(),
    )
}

/// Structured span emitted when an execution fill report is received.
pub fn span_filled(fill: &FillReport) -> Span {
    info_span!(
        "Filled",
        algo_id = %fill.algo_id,
        client_id = %fill.client_id_str(),
        correlation_id = %fill.correlation_id(),
        client_order_id = %fill.client_order_id,
        order_id = %fill.order_id,
        fill_id = %fill.fill_id,
        symbol = %fill.instrument.symbol,
        side = ?fill.side,
        price = %fill.price,
        quantity = %fill.quantity,
        fill_ts_ms = %fill.timestamp.timestamp_millis(),
    )
}

/// Structured span emitted when an order is cancelled.
pub fn span_cancelled(
    algo_id: &str,
    client_id: &str,
    correlation_id: Uuid,
    order_id: &str,
    instrument: &str,
    latency_us: u64,
) -> Span {
    info_span!(
        "Cancelled",
        algo_id = %algo_id,
        client_id = %client_id,
        correlation_id = %correlation_id,
        order_id = %order_id,
        instrument = %instrument,
        latency_us = latency_us,
        cancelled_ts_ms = %Utc::now().timestamp_millis(),
    )
}
