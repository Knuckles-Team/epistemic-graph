//! What a statistical decision concluded, and what guarantee it carries.
//!
//! An engine may ACT only when it can state a risk level and the calibration
//! sample behind it. Otherwise it answers advisory scores marked uncalibrated,
//! or it abstains with the same typed reasons the assembly layer uses.

use serde::{Deserialize, Serialize};

use super::super::numeric::{QuantisedValue, UnitRationalWire};
use super::super::record::AbstainReason;
use crate::contract::BoundedVec;

/// Which controlled procedure produced the risk statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RiskMethod {
    LearnThenTest,
    ConformalRiskControl,
}

/// The risk guarantee an acted-on decision carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RiskStatement {
    pub method: RiskMethod,
    /// Risk level the procedure controls.
    pub epsilon: UnitRationalWire,
    /// Failure probability of that control.
    pub delta: UnitRationalWire,
    pub n_calibration: u64,
}

/// How the scores were calibrated, if at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CalibrationMethod {
    Temperature,
    Vector,
    Dirichlet,
    Isotonic,
    None,
}

/// The calibration behind a decision's probabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CalibrationStatement {
    pub method: CalibrationMethod,
    #[serde(default)]
    pub alpha: Option<UnitRationalWire>,
    #[serde(default)]
    pub coverage_lower: Option<UnitRationalWire>,
    #[serde(default)]
    pub coverage_upper: Option<UnitRationalWire>,
    pub n_calibration: u64,
    /// True when the calibration set was synthetic.
    pub synthetic: bool,
}

/// One scored option of an advisory answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ScoredOption {
    pub option_id: String,
    pub score: QuantisedValue,
    /// Present only when the score is calibrated into a probability.
    #[serde(default)]
    pub probability: Option<UnitRationalWire>,
}

/// The conclusion of one statistical decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum StatisticalOutcome {
    /// An option was chosen, with the propensity it was chosen under and the
    /// risk level that choice controls.
    Acted {
        option_id: String,
        propensity: UnitRationalWire,
        prediction_set: BoundedVec<String, 64>,
        risk: RiskStatement,
    },
    /// Scores only. `calibrated` says whether they mean anything numerically.
    Advisory {
        scores: BoundedVec<ScoredOption, 64>,
        calibrated: bool,
    },
    /// Below the policy's floor; the same typed reasons as an assembly.
    Abstained {
        reasons: BoundedVec<AbstainReason, 64>,
    },
}
