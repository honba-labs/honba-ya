//! Typed LLM decisions (veto and size-down only) for AI-assisted execution (E5-S7).
//!
//! Enforces safety bounds on AI interventions: an LLM can veto or reduce the quantity
//! of an existing order or intent, but can never increase exposure or create unsolicited
//! positions.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The kind of decision an LLM can apply to an order or order intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmDecisionKind {
    /// Pass through the order/intent without modification.
    Approve,
    /// Cancel or reject the proposed order/intent completely (decided quantity = 0).
    Veto,
    /// Scale down the quantity strictly below the proposed quantity.
    SizeDown,
}

/// Errors raised when an LLM decision violates safety invariants.
#[derive(Debug, Error, PartialEq, Clone)]
pub enum LlmDecisionError {
    /// The original quantity must be finite and strictly positive.
    #[error("original quantity must be positive, got {0}")]
    NonPositiveOriginalQuantity(f64),
    /// An LLM attempted to increase quantity (forbidden by `schema_rejects_increase`).
    #[error("LLM decision cannot increase quantity: original {original}, decided {decided} (schema_rejects_increase)")]
    QuantityIncreased {
        /// The proposed original quantity.
        original: f64,
        /// The forbidden increased quantity.
        decided: f64,
    },
    /// A veto decision must have a decided quantity of 0.0.
    #[error("veto decision must have decided quantity 0.0, got {0}")]
    InvalidVetoQuantity(f64),
    /// An approve decision must keep the original quantity.
    #[error("approve decision must keep original quantity {original}, got {decided}")]
    InvalidApproveQuantity {
        /// Original quantity.
        original: f64,
        /// Decided quantity.
        decided: f64,
    },
    /// A size-down decision must strictly reduce the quantity (0 < decided < original).
    #[error("size-down decision must have 0 < decided < original: original {original}, decided {decided}")]
    InvalidSizeDownQuantity {
        /// Original quantity.
        original: f64,
        /// Decided quantity.
        decided: f64,
    },
}

/// A validated, typed decision produced by an AI/LLM model.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LlmDecision {
    /// The client order identifier or intent identifier being acted upon.
    pub order_id: String,
    /// The action taken by the LLM (`approve`, `veto`, `size_down`).
    pub decision: LlmDecisionKind,
    /// The original quantity proposed by the deterministic strategy.
    pub original_quantity: f64,
    /// The quantity after applying the decision (0.0 for veto, <= original for size-down).
    pub decided_quantity: f64,
    /// Natural-language rationale explaining the AI's decision.
    pub rationale: String,
    /// Optional confidence score in range [0.0, 1.0].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
}

impl LlmDecision {
    /// Constructs and validates a new typed LLM decision.
    pub fn new(
        order_id: impl Into<String>,
        decision: LlmDecisionKind,
        original_quantity: f64,
        decided_quantity: f64,
        rationale: impl Into<String>,
    ) -> Result<Self, LlmDecisionError> {
        let dec = Self {
            order_id: order_id.into(),
            decision,
            original_quantity,
            decided_quantity,
            rationale: rationale.into(),
            confidence: None,
        };
        dec.validate()?;
        Ok(dec)
    }

    /// Builder to attach a confidence score.
    pub fn with_confidence(mut self, confidence: f64) -> Self {
        self.confidence = Some(confidence.clamp(0.0, 1.0));
        self
    }

    /// Validates that this decision obeys all safety constraints.
    pub fn validate(&self) -> Result<(), LlmDecisionError> {
        if !self.original_quantity.is_finite() || self.original_quantity <= 0.0 {
            return Err(LlmDecisionError::NonPositiveOriginalQuantity(
                self.original_quantity,
            ));
        }

        // Hard invariant: decided quantity must never exceed original quantity
        if self.decided_quantity > self.original_quantity + 1e-9 {
            return Err(LlmDecisionError::QuantityIncreased {
                original: self.original_quantity,
                decided: self.decided_quantity,
            });
        }

        match self.decision {
            LlmDecisionKind::Veto => {
                if self.decided_quantity.abs() > 1e-9 {
                    return Err(LlmDecisionError::InvalidVetoQuantity(self.decided_quantity));
                }
            }
            LlmDecisionKind::Approve => {
                if (self.decided_quantity - self.original_quantity).abs() > 1e-9 {
                    return Err(LlmDecisionError::InvalidApproveQuantity {
                        original: self.original_quantity,
                        decided: self.decided_quantity,
                    });
                }
            }
            LlmDecisionKind::SizeDown => {
                if self.decided_quantity <= 1e-9
                    || self.decided_quantity >= self.original_quantity - 1e-9
                {
                    return Err(LlmDecisionError::InvalidSizeDownQuantity {
                        original: self.original_quantity,
                        decided: self.decided_quantity,
                    });
                }
            }
        }

        Ok(())
    }
}
