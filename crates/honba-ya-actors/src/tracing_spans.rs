//! Structured tracing spans instrumenting the complete order lifecycle.
//!
//! Spans conform to the lifecycle:
//! `OrderGenerated` -> `RiskEvaluated` -> `BrokerSubmitted` -> `BrokerAcked` -> `Filled` / `Cancelled`
//!
//! Also provides OpenTelemetry mailbox and actor stage spans:
//! - `actor.process_message`
//! - `market_data.coalesce`
//! - `risk.evaluate_order`
//! - `execution.dispatch_wire`
//!
//! Every span incorporates:
//! - `algo_id`
//! - `client_id`
//! - `correlation_id`
//! - W3C `TraceContext` distributed propagation headers.

use chrono::Utc;
use honba_core::{FillReport, OrderAck, OrderRequest, TraceContext};
use tracing::{Span, info_span};
use uuid::Uuid;

/// Structured span emitted when an actor starts processing an inbound message.
pub fn span_actor_process_message(
    actor: &str,
    msg_type: &str,
    trace_ctx: Option<&TraceContext>,
) -> Span {
    let traceparent = trace_ctx.map(|tc| tc.to_traceparent()).unwrap_or_default();
    info_span!(
        "actor.process_message",
        actor = %actor,
        msg_type = %msg_type,
        traceparent = %traceparent,
    )
}

/// Structured span emitted during market data tick coalescing.
pub fn span_market_data_coalesce(count: usize) -> Span {
    info_span!("market_data.coalesce", coalesced_count = count,)
}

/// Structured span emitted during risk pre-trade order evaluation.
pub fn span_risk_evaluate_order(req: &OrderRequest, trace_ctx: Option<&TraceContext>) -> Span {
    let traceparent = trace_ctx.map(|tc| tc.to_traceparent()).unwrap_or_default();
    info_span!(
        "risk.evaluate_order",
        algo_id = %req.algo_id,
        client_order_id = %req.client_order_id,
        symbol = %req.instrument.symbol,
        exchange = %req.instrument.exchange,
        side = ?req.side,
        quantity = %req.quantity,
        traceparent = %traceparent,
    )
}

/// Structured span emitted when execution layer dispatches an order over the wire.
pub fn span_execution_dispatch_wire(
    req: &OrderRequest,
    broker: &str,
    trace_ctx: Option<&TraceContext>,
) -> Span {
    let traceparent = trace_ctx.map(|tc| tc.to_traceparent()).unwrap_or_default();
    info_span!(
        "execution.dispatch_wire",
        broker = %broker,
        algo_id = %req.algo_id,
        client_order_id = %req.client_order_id,
        symbol = %req.instrument.symbol,
        quantity = %req.quantity,
        traceparent = %traceparent,
    )
}

/// Inject a W3C `TraceContext` into an outbound `OrderRequest`.
pub fn inject_trace_context(req: &mut OrderRequest, trace_ctx: &TraceContext) {
    req.trace_context = Some(*trace_ctx);
}

/// Extract an optional W3C `TraceContext` from an `OrderRequest`.
pub fn extract_trace_context(req: &OrderRequest) -> Option<TraceContext> {
    req.trace_context
}

/// Derive a child span `TraceContext` from a parent or initialize a new root trace context.
pub fn derive_or_create_trace_context(parent: Option<&TraceContext>) -> TraceContext {
    match parent {
        Some(p) => {
            let mut span_bytes = [0u8; 8];
            let rand_val = uuid::Uuid::new_v4().as_u128() as u64;
            span_bytes.copy_from_slice(&rand_val.to_be_bytes());
            p.child_span(span_bytes)
        }
        None => {
            let u = uuid::Uuid::new_v4();
            let mut trace_id = [0u8; 16];
            trace_id.copy_from_slice(u.as_bytes());
            let mut span_id = [0u8; 8];
            let rand_val = uuid::Uuid::new_v4().as_u128() as u64;
            span_id.copy_from_slice(&rand_val.to_be_bytes());
            TraceContext::new_root(trace_id, span_id)
        }
    }
}

/// Structured span emitted when a strategy generates an order candidate.
pub fn span_order_generated(req: &OrderRequest) -> Span {
    let traceparent = req
        .trace_context
        .map(|tc| tc.to_traceparent())
        .unwrap_or_default();
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
        traceparent = %traceparent,
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
    let traceparent = req
        .trace_context
        .map(|tc| tc.to_traceparent())
        .unwrap_or_default();
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
        traceparent = %traceparent,
        timestamp_ms = %Utc::now().timestamp_millis(),
    )
}

/// Structured span emitted when an approved order is dispatched to the broker adapter.
pub fn span_broker_submitted(req: &OrderRequest, broker: &str) -> Span {
    let traceparent = req
        .trace_context
        .map(|tc| tc.to_traceparent())
        .unwrap_or_default();
    info_span!(
        "BrokerSubmitted",
        algo_id = %req.algo_id,
        client_id = %req.client_id_str(),
        correlation_id = %req.correlation_id(),
        client_order_id = %req.client_order_id,
        symbol = %req.instrument.symbol,
        broker = %broker,
        traceparent = %traceparent,
        submit_ts_ms = %Utc::now().timestamp_millis(),
    )
}

/// Structured span emitted when the broker acknowledges order placement.
pub fn span_broker_acked(req: &OrderRequest, ack: &OrderAck, dispatch_duration_us: u64) -> Span {
    let traceparent = ack
        .trace_context
        .map(|tc| tc.to_traceparent())
        .unwrap_or_default();
    info_span!(
        "BrokerAcked",
        algo_id = %req.algo_id,
        client_id = %req.client_id_str(),
        correlation_id = %ack.correlation_id(),
        client_order_id = %ack.client_order_id,
        order_id = %ack.order_id,
        status = ?ack.status,
        dispatch_duration_us = dispatch_duration_us,
        traceparent = %traceparent,
        ack_ts_ms = %ack.timestamp.timestamp_millis(),
    )
}

/// Structured span emitted when an execution fill report is received.
pub fn span_filled(fill: &FillReport) -> Span {
    let traceparent = fill
        .trace_context
        .map(|tc| tc.to_traceparent())
        .unwrap_or_default();
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
        traceparent = %traceparent,
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
