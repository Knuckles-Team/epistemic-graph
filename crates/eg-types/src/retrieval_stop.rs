//! Typed state for the bounded iterative-retrieval stop rule (R082.1).
//!
//! This module is deliberately a data-model seam.  It records the calibration
//! rows and the already validated limits that a later planner operation will
//! consume; it does not decide whether a query should continue.  Keeping the
//! closed reason and refusal vocabularies here means the planner cannot smuggle
//! an unrecognised stop outcome through the wire contract.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::contract::BoundedVec;

/// The maximum number of round-specific calibration rows carried by one table.
pub const MAX_CALIBRATION_ROWS: usize = 256;

/// The reasons a retrieval round may stop or continue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum StopReason {
    /// The configured round count has been exhausted.
    RoundLimit,
    /// A non-round retrieval budget has been exhausted.
    BudgetExhausted,
    /// The completed round added no evidence.
    NoNewEvidence,
    /// The declared quality signal has saturated.
    QualitySaturated,
    /// Repeated answers exceeded a calibrated confidence margin.
    AnswerRepeatedMarginMet,
    /// The round may continue, but there is no calibration for confidence.
    ContinueUncalibrated,
    /// The answer changed, so another bounded round may run.
    ContinueAnswerChanged,
    /// The answer repeated, but the calibrated margin is not met.
    ContinueMarginLow,
}

impl StopReason {
    /// Whether this reason is terminal for the current retrieval operation.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::RoundLimit
                | Self::BudgetExhausted
                | Self::NoNewEvidence
                | Self::QualitySaturated
                | Self::AnswerRepeatedMarginMet
        )
    }
}

/// The typed refusal retained when a round has no calibration row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CalibrationRefusal {
    /// Confidence cannot be used to stop this round without a calibration row.
    RoundNotCalibrated,
}

/// Which bounded budget was exhausted by a terminal verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ExhaustedDimension {
    Round,
    Tokens,
    InferenceCalls,
    Cost,
}

/// One confidence calibration row for one completed retrieval round.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RoundCalibration {
    /// The zero- or one-based round identity chosen by the serving policy.
    pub round: u32,
    /// Confidence at the beginning of this round, in the inclusive [0, 1] range.
    pub baseline_confidence: f64,
    /// The strictly-greater margin required for a confidence stop.
    pub margin_threshold: f64,
}

impl RoundCalibration {
    /// Check the finite confidence values carried by this wire row.
    pub fn validate(&self) -> Result<(), String> {
        if !(self.baseline_confidence.is_finite()
            && (0.0..=1.0).contains(&self.baseline_confidence))
        {
            return Err("calibration baseline confidence must be finite and within [0, 1]".into());
        }
        if !(self.margin_threshold.is_finite() && (0.0..=1.0).contains(&self.margin_threshold)) {
            return Err("calibration margin threshold must be finite and within [0, 1]".into());
        }
        Ok(())
    }
}

/// The immutable calibration table consulted by a stop decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CalibrationTable {
    #[serde(default)]
    pub rows: BoundedVec<RoundCalibration, MAX_CALIBRATION_ROWS>,
}

impl CalibrationTable {
    /// Validate every row and reject ambiguous duplicate round identities.
    pub fn validate(&self) -> Result<(), String> {
        let mut rounds = BTreeSet::new();
        for row in &self.rows {
            row.validate()?;
            if !rounds.insert(row.round) {
                return Err(format!("calibration table repeats round {}", row.round));
            }
        }
        Ok(())
    }

    /// Find the calibration row for a completed round, if one was supplied.
    pub fn for_round(&self, round: u32) -> Option<&RoundCalibration> {
        self.rows.iter().find(|row| row.round == round)
    }
}

/// Limits and measured progress already validated by the retrieval planner.
///
/// The model records both the declared ceilings and current consumption so a
/// later decision operation can report the exhausted dimension without
/// reconstructing or trusting caller-provided budget state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StopContext {
    pub round: u32,
    pub round_limit: u32,
    pub token_limit: u64,
    pub tokens_used: u64,
    pub inference_limit: u32,
    pub inference_calls_used: u32,
    pub cost_limit: u64,
    pub cost_used: u64,
    /// True only when the completed round supplied measured progress evidence.
    pub completed_round_progress: bool,
    /// The policy revision that authorized these limits.
    pub policy_revision: String,
}

impl StopContext {
    /// Validate limits and usage before the context reaches a stop decision.
    pub fn validate(&self) -> Result<(), String> {
        if self.round_limit == 0 || self.round > self.round_limit {
            return Err("stop context round must be within a non-zero round limit".into());
        }
        if self.token_limit == 0 || self.tokens_used > self.token_limit {
            return Err("stop context tokens must be within a non-zero token limit".into());
        }
        if self.inference_limit == 0 || self.inference_calls_used > self.inference_limit {
            return Err(
                "stop context inference calls must be within a non-zero inference limit".into(),
            );
        }
        if self.cost_limit == 0 || self.cost_used > self.cost_limit {
            return Err("stop context cost must be within a non-zero cost limit".into());
        }
        if self.policy_revision.trim().is_empty() {
            return Err("stop context requires a policy revision".into());
        }
        Ok(())
    }
}

/// The typed stop/continue outcome returned to the caller and recorded in the
/// retrieval decision premise.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StopVerdict {
    pub stop: bool,
    pub reason: StopReason,
    pub round: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub margin: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    /// Missing calibration remains visible even when another terminal reason
    /// wins precedence (for example, an exhausted budget).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calibration_refusal: Option<CalibrationRefusal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exhausted_dimension: Option<ExhaustedDimension>,
}

impl StopVerdict {
    /// Validate the structural relationship between reason, stop bit and
    /// optional confidence evidence. This does not perform the stop decision.
    pub fn validate(&self) -> Result<(), String> {
        if self.stop != self.reason.is_terminal() {
            return Err("stop flag does not match the closed stop reason".into());
        }
        for value in [self.margin, self.threshold].into_iter().flatten() {
            if !(value.is_finite() && (0.0..=1.0).contains(&value)) {
                return Err("stop confidence values must be finite and within [0, 1]".into());
            }
        }
        if self.calibration_refusal.is_some() && (self.margin.is_some() || self.threshold.is_some())
        {
            return Err("an uncalibrated verdict cannot carry confidence margin values".into());
        }
        if self.margin.is_some() != self.threshold.is_some() {
            return Err("stop margin and threshold must be supplied together".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn calibration_table() -> CalibrationTable {
        CalibrationTable {
            rows: BoundedVec::new(vec![
                RoundCalibration {
                    round: 0,
                    baseline_confidence: 0.40,
                    margin_threshold: 0.10,
                },
                RoundCalibration {
                    round: 1,
                    baseline_confidence: 0.55,
                    margin_threshold: 0.15,
                },
            ])
            .expect("calibration rows fit the bound"),
        }
    }

    fn context() -> StopContext {
        StopContext {
            round: 1,
            round_limit: 4,
            token_limit: 8_000,
            tokens_used: 1_024,
            inference_limit: 8,
            inference_calls_used: 2,
            cost_limit: 100,
            cost_used: 20,
            completed_round_progress: true,
            policy_revision: "stop-policy-v1".to_string(),
        }
    }

    // spec: EG-FEDERATED-QUERY-R082.1
    #[test]
    fn table_and_verdict_round_trip() {
        let table = calibration_table();
        let stop_context = context();
        assert!(table.validate().is_ok());
        assert!(stop_context.validate().is_ok());
        assert_eq!(table.for_round(1).unwrap().margin_threshold, 0.15);

        let reasons = [
            StopReason::RoundLimit,
            StopReason::BudgetExhausted,
            StopReason::NoNewEvidence,
            StopReason::QualitySaturated,
            StopReason::AnswerRepeatedMarginMet,
            StopReason::ContinueUncalibrated,
            StopReason::ContinueAnswerChanged,
            StopReason::ContinueMarginLow,
        ];
        for reason in reasons {
            let verdict = StopVerdict {
                stop: reason.is_terminal(),
                reason,
                round: 1,
                margin: None,
                threshold: None,
                calibration_refusal: Some(CalibrationRefusal::RoundNotCalibrated),
                exhausted_dimension: (reason == StopReason::BudgetExhausted)
                    .then_some(ExhaustedDimension::Tokens),
            };
            assert!(verdict.validate().is_ok());
            let encoded = serde_json::to_vec(&verdict).expect("verdict serializes");
            let decoded: StopVerdict = serde_json::from_slice(&encoded).expect("verdict decodes");
            assert_eq!(decoded, verdict);
        }

        let encoded = serde_json::to_vec(&(table, stop_context)).expect("model serializes");
        let decoded: (CalibrationTable, StopContext) =
            serde_json::from_slice(&encoded).expect("model decodes");
        assert_eq!(decoded.0, calibration_table());
        assert_eq!(decoded.1, context());
    }

    #[test]
    fn unknown_reason_refuses_and_terminal_calibration_refusal_is_retained() {
        let verdict = StopVerdict {
            stop: true,
            reason: StopReason::BudgetExhausted,
            round: 3,
            margin: None,
            threshold: None,
            calibration_refusal: Some(CalibrationRefusal::RoundNotCalibrated),
            exhausted_dimension: Some(ExhaustedDimension::InferenceCalls),
        };
        let mut value = serde_json::to_value(verdict).expect("verdict serializes");
        assert_eq!(value["calibration_refusal"], "ROUND_NOT_CALIBRATED");
        assert_eq!(value["reason"], "budget_exhausted");
        value["reason"] = serde_json::Value::String("future_reason".into());
        assert!(serde_json::from_value::<StopVerdict>(value).is_err());
    }
}
