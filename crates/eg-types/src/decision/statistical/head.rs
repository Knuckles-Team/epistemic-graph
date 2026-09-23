//! The body of a fitted decision head (EH-027).
//!
//! Two shapes, one body: `WeightedFeatures` is an advisory linear score that
//! never acts, and `ListwiseLogistic` is a linear logit per option turned into
//! a distribution over the candidate set by a softmax. Both are linear in the
//! standardised features, so a recorded explanation (weight x value per
//! feature) is exact for the logit -- and only for the logit.
//!
//! Every number is fixed-point on the `Pico` scale: a head replays to the same
//! bits on every release target because nothing in it is a float.

use serde::{Deserialize, Serialize};

use super::super::numeric::{QuantisedValue, UnitRationalWire};
use super::outcome::RiskStatement;
use crate::contract::BoundedVec;

/// Which head shape a fit produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum HeadKind {
    /// One weight per feature; an advisory score that never acts.
    WeightedFeatures,
    /// A listwise logistic model over the candidate set.
    ListwiseLogistic,
}

/// Format identity of a decision head body.
pub const DECISION_HEAD_SCHEMA_VERSION: u16 = 1;

/// How one raw feature is standardised before the head reads it, and the
/// range it was seen in at fit time. A value outside that range is
/// out-of-distribution and the decision abstains (drift, §6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FeatureStandardisation {
    pub center: QuantisedValue,
    /// Strictly positive.
    pub scale: QuantisedValue,
    pub lower: QuantisedValue,
    pub upper: QuantisedValue,
}

/// Which labels the head was fitted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FittedRegime {
    /// Human or constructed acceptability sets: coverage and risk claims hold
    /// on that distribution.
    FullLabel,
    /// Executed-option outcomes only: no coverage or "best option" claim.
    BanditLabel,
}

/// The calibration a full-label head carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HeadCalibration {
    /// Multiplies the logits before the softmax (temperature scaling).
    pub inverse_temperature: QuantisedValue,
    /// Miscoverage level the prediction set is calibrated at.
    pub alpha: UnitRationalWire,
    /// Largest nonconformity `1 - p` admitted into the prediction set.
    pub set_threshold: QuantisedValue,
    /// Realised coverage interval on the calibration items.
    pub coverage_lower: UnitRationalWire,
    pub coverage_upper: UnitRationalWire,
    /// The certified act threshold on the top probability, when the selective
    /// risk procedure certified one. `None` means the head never acts.
    #[serde(default)]
    pub act_threshold: Option<QuantisedValue>,
    #[serde(default)]
    pub risk: Option<RiskStatement>,
    pub n_calibration: u64,
    pub synthetic: bool,
}

/// The body of a `DecisionHead` component, and of a fit job's draft.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionHeadBody {
    pub schema_version: u16,
    pub kind: HeadKind,
    pub regime: FittedRegime,
    /// `sha256:<hex>` content digest of the feature schema body it reads.
    pub feature_schema_digest: String,
    pub standardisation: BoundedVec<FeatureStandardisation, 32>,
    pub weights: BoundedVec<QuantisedValue, 32>,
    #[serde(default)]
    pub calibration: Option<HeadCalibration>,
    /// Digest of the admitted training items, in order.
    pub training_records_digest: String,
    pub n_training: u64,
    pub synthetic: bool,
}

impl DecisionHeadBody {
    /// The validating constructor.
    pub fn checked(self) -> Result<Self, String> {
        if self.schema_version != DECISION_HEAD_SCHEMA_VERSION {
            return Err(format!(
                "decision head version {} is not served",
                self.schema_version
            ));
        }
        if self.weights.len() != self.standardisation.len() || self.weights.is_empty() {
            return Err("a head declares one weight per standardised feature".to_string());
        }
        if self.standardisation.iter().any(|s| s.scale.value <= 0) {
            return Err("a feature scale must be strictly positive".to_string());
        }
        if self.kind == HeadKind::WeightedFeatures && self.calibration.is_some() {
            return Err(
                "a WeightedFeatures head is advisory and carries no calibration".to_string(),
            );
        }
        if self.regime == FittedRegime::BanditLabel && self.calibration.is_some() {
            return Err("a bandit-label head makes no coverage claim".to_string());
        }
        Ok(self)
    }
}
