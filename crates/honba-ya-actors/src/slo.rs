//! Formal Actor Pipeline Service Level Objectives (SLOs) (ADR 0032, Phase 8.1).
//!
//! Under institutional electronic trading requirements and SEBI regulatory standards,
//! the `honba-ya-actors` pipeline enforces strict latency and throughput invariants:
//!
//! | Pipeline Stage | Target Metric / Invariant | Threshold |
//! | :--- | :--- | :--- |
//! | **Market Data Broadcast** | Inbound feed tick to WebSocket client broadcast | $p99 < 2\,\text{ms}$ |
//! | **Strategy Signal Generation**| Tick/Bar arrival to Candidate Order Request | $p99 < 1\,\text{ms}$ |
//! | **End-to-End Execution** | Tick arrival to broker wire socket dispatch | $p99 < 5\,\text{ms}$ |
//! | **Pre-Trade Risk Gate** | Synchronous OTR and price-band evaluation | $p99 < 15\,\mu\text{s}$ |

use std::time::Duration;

/// Maximum permissible latency for Tick-to-Quote WebSocket broadcast ($p99 < 2\,\text{ms}$).
pub const SLO_TICK_TO_QUOTE_WS_BROADCAST_MAX: Duration = Duration::from_millis(2);

/// Maximum permissible latency for Strategy signal-to-order generation ($p99 < 1\,\text{ms}$).
pub const SLO_STRATEGY_SIGNAL_TO_ORDER_MAX: Duration = Duration::from_millis(1);

/// Maximum permissible latency for End-to-end tick-to-wire broker dispatch ($p99 < 5\,\text{ms}$).
pub const SLO_END_TO_END_TICK_TO_WIRE_MAX: Duration = Duration::from_millis(5);

/// Maximum permissible synchronous pre-trade risk evaluation latency ($p99 < 15\,\mu\text{s}$).
pub const SLO_PRE_TRADE_RISK_GATE_MAX: Duration = Duration::from_micros(15);

/// Pipeline SLO violation error.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum SloViolation {
    #[error("WebSocket broadcast latency {actual:?} exceeded SLO threshold {threshold:?}")]
    WebSocketBroadcastLag {
        actual: Duration,
        threshold: Duration,
    },
    #[error("Strategy signal latency {actual:?} exceeded SLO threshold {threshold:?}")]
    StrategySignalLag {
        actual: Duration,
        threshold: Duration,
    },
    #[error("End-to-end wire dispatch latency {actual:?} exceeded SLO threshold {threshold:?}")]
    WireDispatchLag {
        actual: Duration,
        threshold: Duration,
    },
    #[error("Risk gate evaluation latency {actual:?} exceeded SLO threshold {threshold:?}")]
    RiskGateLag {
        actual: Duration,
        threshold: Duration,
    },
}

/// Checks whether an elapsed duration complies with the Tick-to-Quote WebSocket broadcast SLO.
pub fn verify_ws_broadcast_slo(duration: Duration) -> Result<(), SloViolation> {
    if duration <= SLO_TICK_TO_QUOTE_WS_BROADCAST_MAX {
        Ok(())
    } else {
        Err(SloViolation::WebSocketBroadcastLag {
            actual: duration,
            threshold: SLO_TICK_TO_QUOTE_WS_BROADCAST_MAX,
        })
    }
}

/// Checks whether an elapsed duration complies with the Strategy signal-to-order generation SLO.
pub fn verify_strategy_signal_slo(duration: Duration) -> Result<(), SloViolation> {
    if duration <= SLO_STRATEGY_SIGNAL_TO_ORDER_MAX {
        Ok(())
    } else {
        Err(SloViolation::StrategySignalLag {
            actual: duration,
            threshold: SLO_STRATEGY_SIGNAL_TO_ORDER_MAX,
        })
    }
}

/// Checks whether an elapsed duration complies with the End-to-end tick-to-wire broker dispatch SLO.
pub fn verify_wire_dispatch_slo(duration: Duration) -> Result<(), SloViolation> {
    if duration <= SLO_END_TO_END_TICK_TO_WIRE_MAX {
        Ok(())
    } else {
        Err(SloViolation::WireDispatchLag {
            actual: duration,
            threshold: SLO_END_TO_END_TICK_TO_WIRE_MAX,
        })
    }
}

/// Checks whether an elapsed duration complies with the Pre-trade risk gate evaluation SLO.
pub fn verify_risk_gate_slo(duration: Duration) -> Result<(), SloViolation> {
    if duration <= SLO_PRE_TRADE_RISK_GATE_MAX {
        Ok(())
    } else {
        Err(SloViolation::RiskGateLag {
            actual: duration,
            threshold: SLO_PRE_TRADE_RISK_GATE_MAX,
        })
    }
}
