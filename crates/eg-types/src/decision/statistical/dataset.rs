//! Labelled decision data: what fitting and evaluation read (EH-022, EH-016).
//!
//! Two label regimes, never mixed in one dataset. A FULL-LABEL item carries an
//! acceptability set -- every option a human (or a construction with ground
//! truth) accepts -- so coverage and act-risk claims hold on that
//! distribution. A BANDIT-LABEL item carries only the executed option's
//! outcome, the exact propensity it was executed under, and who evaluated it;
//! it supports off-policy value inside the logging support and nothing more.
//!
//! Admission is decided by the engine, not the submitter: every field the
//! training invariant turns on (who evaluated, at what fidelity, whether the
//! propensity is the executed policy's) is carried here so the filter can
//! refuse an item and say why.

use serde::{Deserialize, Serialize};

use super::super::numeric::{QuantScaleTag, UnitRationalWire};
use super::super::record::EvidenceClass;
use crate::contract::BoundedVec;

/// Format identity of a labelled dataset.
pub const LABELLED_DATASET_SCHEMA_VERSION: u16 = 1;
/// Most items one dataset may carry.
pub const MAX_DATASET_ITEMS: usize = 4_096;

/// Who produced a full-label acceptability set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum LabelSource {
    Human,
    /// Ground truth by construction (the synthetic suite).
    SyntheticConstruction,
    /// An LLM resolved an abstention. Never a label: a candidate for human
    /// labelling only.
    LlmResolved,
}

/// How completely the evaluated run was traced, plus the censored states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum OutcomeFidelity {
    FullStep,
    ToolCalls,
    FinalOutput,
    TraceIncomplete,
    OutcomeUncertain,
    Cancelled,
}

/// Where a logged propensity came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PropensitySource {
    /// The executed policy's exact probability.
    ExecutedPolicy,
    /// A head's score copied in as if it were a propensity. Never admissible.
    HeadMass,
}

/// The independent evaluation behind a bandit label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OutcomeEvaluation {
    pub evaluation_id: String,
    pub class: EvidenceClass,
    pub producer: String,
    /// The agent the decision selected; a label it produced itself is a
    /// self-report.
    pub selected_agent: String,
    /// The lease holder that ran it; likewise excluded as a producer.
    pub lease_holder: String,
    pub fidelity: OutcomeFidelity,
    /// `None` is a censored run: neither a success nor a failure.
    #[serde(default)]
    pub success: Option<bool>,
}

/// One logged, executed decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LoggedOutcome {
    pub executed: String,
    /// Aligned with the item's candidates; must sum to exactly one.
    pub logging_propensities: BoundedVec<UnitRationalWire, 64>,
    pub propensity_source: PropensitySource,
    /// A pinned decision had propensity one by fiat and is excluded from
    /// off-policy estimates.
    pub pinned: bool,
    pub commit_principal: String,
    pub evaluation: OutcomeEvaluation,
}

/// The label of one item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "label", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ItemLabel {
    Gold {
        acceptable: BoundedVec<String, 64>,
        source: LabelSource,
    },
    Logged(Box<LoggedOutcome>),
}

/// One labelled decision: its candidates, their feature rows and the label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LabelledItem {
    pub item_id: String,
    pub recorded_at_ms: u64,
    /// Calibration class (task class); Mondrian guarantees are per class.
    pub class_key: String,
    pub candidate_ids: BoundedVec<String, 64>,
    /// Row-major, `candidate_ids.len() x feature_names.len()`, on the
    /// dataset's scale.
    pub features: BoundedVec<i64, 2048>,
    pub label: ItemLabel,
    /// Known inclusion probability of an audit-sampled item; its weight is the
    /// inverse (EH-026).
    #[serde(default)]
    pub audit_inclusion: Option<UnitRationalWire>,
}

/// A labelled dataset, pinned by digest in every job that reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LabelledDataset {
    pub schema_version: u16,
    /// `sha256:<hex>` of the feature schema body the rows were computed under.
    pub feature_schema_digest: String,
    pub feature_names: BoundedVec<String, 32>,
    pub scale: QuantScaleTag,
    pub items: BoundedVec<LabelledItem, 4096>,
    /// Synthetic evidence is labelled as such, always.
    pub synthetic: bool,
}

impl LabelledDataset {
    /// Structural validation: version, row shapes and label alignment.
    pub fn checked(self) -> Result<Self, String> {
        if self.schema_version != LABELLED_DATASET_SCHEMA_VERSION {
            return Err(format!(
                "dataset version {} is not served",
                self.schema_version
            ));
        }
        let width = self.feature_names.len();
        if width == 0 {
            return Err("a dataset names at least one feature".to_string());
        }
        for item in &self.items {
            item.check_shape(width)?;
        }
        Ok(self)
    }
}

impl LabelledItem {
    fn check_shape(&self, width: usize) -> Result<(), String> {
        let options = self.candidate_ids.len();
        if options == 0 || self.features.len() != options * width {
            return Err(format!(
                "item {} rows do not match its candidates",
                self.item_id
            ));
        }
        match &self.label {
            ItemLabel::Gold { acceptable, .. } => {
                let known = acceptable
                    .iter()
                    .all(|id| self.candidate_ids.iter().any(|c| c == id));
                if !known {
                    return Err(format!("item {} accepts an unknown option", self.item_id));
                }
            }
            ItemLabel::Logged(logged) => {
                if logged.logging_propensities.len() != options {
                    return Err(format!("item {} propensities are misaligned", self.item_id));
                }
            }
        }
        Ok(())
    }

    /// Index of `option_id` among the candidates.
    pub fn index_of(&self, option_id: &str) -> Option<usize> {
        self.candidate_ids.iter().position(|id| id == option_id)
    }
}
