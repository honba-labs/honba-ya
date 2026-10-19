//! Prometheus metrics export and real-time regulatory tracking for Honba.
//!
//! Under SEBI regulatory framework (Retail Algo Circular) and modern SRE standards,
//! trading systems must export real-time gauges and counters for:
//! - Order-to-Trade Ratio (OTR) per `algo_id` and per scrip (`InstrumentId`).
//! - End-to-end order dispatch latency and risk gate evaluation latency.
//! - Cumulative daily PnL, notional turnover, and risk gate refusal counts.

use honba_core::{FillReport, OrderRequest};
use prometheus::{
    CounterVec, Encoder, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::time::Duration;

/// Prometheus metrics collector for SEBI regulatory and execution telemetry.
#[derive(Clone)]
pub struct MetricsCollector {
    registry: Registry,
    otr_gauge: GaugeVec,
    risk_eval_histogram: HistogramVec,
    dispatch_latency_histogram: HistogramVec,
    turnover_counter: CounterVec,
    daily_pnl_gauge: GaugeVec,
    refusal_counter: CounterVec,
    order_ops_counter: CounterVec,
}

impl MetricsCollector {
    /// Create and register all Honba Prometheus metrics.
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();

        // 1. Real-time OTR gauge per algo_id and per scrip symbol
        let otr_opts = Opts::new(
            "honba_otr_ratio",
            "Real-time Order-to-Trade Ratio per algo_id and per scrip",
        );
        let otr_gauge = GaugeVec::new(otr_opts, &["algo_id", "symbol"])?;
        registry.register(Box::new(otr_gauge.clone()))?;

        // 2. Risk gate evaluation latency histogram (microseconds to milliseconds)
        let eval_opts = HistogramOpts::new(
            "honba_risk_evaluation_duration_seconds",
            "Synchronous risk gate evaluation latency in seconds",
        )
        .buckets(vec![
            0.000001, 0.000005, 0.000010, 0.000050, 0.000100, 0.000500, 0.001000, 0.005000,
            0.010000,
        ]);
        let risk_eval_histogram = HistogramVec::new(eval_opts, &["algo_id", "status"])?;
        registry.register(Box::new(risk_eval_histogram.clone()))?;

        // 3. End-to-end broker dispatch latency histogram
        let dispatch_opts = HistogramOpts::new(
            "honba_order_dispatch_duration_seconds",
            "End-to-end broker order placement dispatch latency in seconds",
        )
        .buckets(vec![
            0.0001, 0.0005, 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 5.0,
        ]);
        let dispatch_latency_histogram =
            HistogramVec::new(dispatch_opts, &["algo_id", "broker", "status"])?;
        registry.register(Box::new(dispatch_latency_histogram.clone()))?;

        // 4. Cumulative notional turnover in INR
        let turnover_opts = Opts::new(
            "honba_notional_turnover_inr_total",
            "Cumulative notional trading turnover in INR",
        );
        let turnover_counter = CounterVec::new(turnover_opts, &["algo_id", "symbol"])?;
        registry.register(Box::new(turnover_counter.clone()))?;

        // 5. Cumulative daily realized PnL in INR
        let pnl_opts = Opts::new(
            "honba_daily_pnl_inr",
            "Cumulative daily realized profit and loss in INR",
        );
        let daily_pnl_gauge = GaugeVec::new(pnl_opts, &["algo_id"])?;
        registry.register(Box::new(daily_pnl_gauge.clone()))?;

        // 6. Risk refusal counts by algo and rule
        let refusal_opts = Opts::new(
            "honba_risk_refusals_total",
            "Total pre-trade risk gate refusals categorized by algo_id and regulatory rule",
        );
        let refusal_counter = CounterVec::new(refusal_opts, &["algo_id", "rule"])?;
        registry.register(Box::new(refusal_counter.clone()))?;

        // 7. Order operations counter
        let ops_opts = Opts::new(
            "honba_order_operations_total",
            "Total order operations by algo, symbol, and operation type (generation, placement, cancellation)",
        );
        let order_ops_counter = CounterVec::new(ops_opts, &["algo_id", "symbol", "operation"])?;
        registry.register(Box::new(order_ops_counter.clone()))?;

        Ok(Self {
            registry,
            otr_gauge,
            risk_eval_histogram,
            dispatch_latency_histogram,
            turnover_counter,
            daily_pnl_gauge,
            refusal_counter,
            order_ops_counter,
        })
    }

    /// Access the underlying Prometheus registry.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Record a newly generated candidate order from strategy.
    pub fn record_order_generation(&self, req: &OrderRequest) {
        self.order_ops_counter
            .with_label_values(&[&req.algo_id, &req.instrument.symbol, "generation"])
            .inc();
    }

    /// Record a risk evaluation event and its evaluation duration.
    pub fn record_risk_evaluation(
        &self,
        req: &OrderRequest,
        duration: Duration,
        approved: bool,
        refusal_rule: Option<&str>,
    ) {
        let status = if approved { "approved" } else { "refused" };
        self.risk_eval_histogram
            .with_label_values(&[&req.algo_id, status])
            .observe(duration.as_secs_f64());

        if let Some(rule) = refusal_rule {
            self.refusal_counter
                .with_label_values(&[&req.algo_id, rule])
                .inc();
        }
    }

    /// Record a broker order dispatch event and roundtrip duration.
    pub fn record_broker_dispatch(
        &self,
        req: &OrderRequest,
        broker: &str,
        duration: Duration,
        success: bool,
    ) {
        let status = if success { "success" } else { "failure" };
        self.dispatch_latency_histogram
            .with_label_values(&[&req.algo_id, broker, status])
            .observe(duration.as_secs_f64());

        if success {
            self.order_ops_counter
                .with_label_values(&[&req.algo_id, &req.instrument.symbol, "placement"])
                .inc();
        }
    }

    /// Record an order cancellation.
    pub fn record_cancellation(&self, algo_id: &str, symbol: &str) {
        self.order_ops_counter
            .with_label_values(&[algo_id, symbol, "cancellation"])
            .inc();
    }

    /// Record an execution fill report: updates notional turnover.
    pub fn record_fill(&self, fill: &FillReport) {
        let notional = fill.price.0 * fill.quantity.0;
        if let Some(val) = notional.to_f64() {
            self.turnover_counter
                .with_label_values(&[&fill.algo_id, &fill.instrument.symbol])
                .inc_by(val);
        }
    }

    /// Record cumulative PnL for an algorithm.
    pub fn record_pnl(&self, algo_id: &str, pnl: Decimal) {
        if let Some(val) = pnl.to_f64() {
            self.daily_pnl_gauge.with_label_values(&[algo_id]).set(val);
        }
    }

    /// Update real-time OTR gauge for an algo and instrument.
    pub fn update_otr(&self, algo_id: &str, symbol: &str, otr: f64) {
        self.otr_gauge
            .with_label_values(&[algo_id, symbol])
            .set(otr);
    }

    /// Export all collected metrics in Prometheus plain text format.

    /// Create an Axum router serving `GET /metrics` in Prometheus format.
    pub fn router(self: std::sync::Arc<Self>) -> axum::Router {
        use axum::response::IntoResponse;
        let collector = std::sync::Arc::clone(&self);
        axum::Router::new().route(
            "/metrics",
            axum::routing::get(move || {
                let collector = std::sync::Arc::clone(&collector);
                async move {
                    match collector.export_metrics() {
                        Ok(text) => (
                            axum::http::StatusCode::OK,
                            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
                            text,
                        )
                            .into_response(),
                        Err(e) => (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Metrics export failed: {e}"),
                        )
                            .into_response(),
                    }
                }
            }),
        )
    }

    pub fn export_metrics(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        String::from_utf8(buffer)
            .map_err(|e| prometheus::Error::Msg(format!("Metrics utf8 conversion failed: {e}")))
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new().expect("default MetricsCollector initialization failed")
    }
}
