//! The two admin jobs behind a calibrated decision head: fitting one, and
//! evaluating a candidate before it may be published.
//!
//! Both follow the `AnalyticsJob` shape -- a `submit`/`status` op pair over a
//! durable job row -- because that is how every other long-running admin task
//! on this engine is already driven, and a second shape would mean a second
//! way to lose a job.

use serde::{Deserialize, Serialize};

use super::numeric::{QuantisedValue, UnitRationalWire};
use super::request::DecisionPolicyRef;
use super::statistical::dataset::LabelledDataset;
use super::statistical::head::DecisionHeadBody;
use super::statistical::CalibrationStatement;

pub use super::statistical::head::HeadKind;
use crate::agent_component::ComponentDependency;
use crate::contract::BoundedVec;

/// What kind of labels the training records carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "regime", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum LabelRegime {
    /// Every option's outcome is known, pinned by the gold set's digest.
    FullLabel { gold_set_digest: String },
    /// Only the acted-on option's outcome is known.
    BanditLabel,
}

/// The closed record window a job reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RecordWindow {
    pub from_ms: u64,
    pub to_ms: u64,
}

/// Deterministic optimiser limits. The seed is part of the request, so a fit
/// is reproducible from its record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OptimiserSpec {
    pub max_iterations: u32,
    pub tolerance: QuantisedValue,
    pub seed: u64,
}

/// Fit a decision head into a draft artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionFitRequest {
    pub tenant_id: String,
    pub idempotency_key: String,
    pub head_kind: HeadKind,
    pub feature_schema: ComponentDependency,
    pub policy: DecisionPolicyRef,
    pub label_regime: LabelRegime,
    pub window: RecordWindow,
    pub optimiser: OptimiserSpec,
    /// The labelled items, pinned: a full-label regime's `gold_set_digest`
    /// must equal this dataset's digest.
    pub dataset: LabelledDataset,
    /// Commit principals whose bandit records may train. Empty admits none.
    #[serde(default)]
    pub approved_commit_principals: BoundedVec<String, 64>,
}

/// Which off-policy estimator an evaluation runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum OpeEstimatorKind {
    Ips,
    ClippedIps,
    Snips,
    Switch,
    DoublyRobust,
}

/// What is being evaluated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "candidate", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EvalCandidate {
    /// A fit job's draft artifact, pinned by content digest.
    DraftArtifact { sha256: String, length: u64 },
    /// An already-published head.
    PublishedHead { head: ComponentDependency },
}

/// Evaluate a candidate head against recorded decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionEvalRequest {
    pub tenant_id: String,
    pub idempotency_key: String,
    pub candidate: EvalCandidate,
    pub policy: DecisionPolicyRef,
    pub estimators: BoundedVec<OpeEstimatorKind, 8>,
    #[serde(default)]
    pub gold_set_digest: Option<String>,
    pub window: RecordWindow,
    /// The labelled items the candidate is evaluated on.
    pub dataset: LabelledDataset,
    /// Commit principals whose bandit records may be evaluated on.
    #[serde(default)]
    pub approved_commit_principals: BoundedVec<String, 64>,
}

/// Ask after one submitted job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionJobStatusRequest {
    pub tenant_id: String,
    pub job_id: String,
}

/// One estimator's interval on the candidate head's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OpeEstimateView {
    pub estimator: OpeEstimatorKind,
    pub value: QuantisedValue,
    pub lower: QuantisedValue,
    pub upper: QuantisedValue,
    pub effective_sample_size: QuantisedValue,
    /// Share of logged probability mass the candidate policy does not cover.
    pub unsupported_mass: UnitRationalWire,
}

/// Full-label metrics of a candidate head, each rate an exact fraction of the
/// admitted items and each interval a Clopper-Pearson interval at `delta`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FullLabelMetrics {
    pub n_items: u64,
    /// Top-1 lands in the acceptability set.
    pub top1_hits: u64,
    pub log_loss: QuantisedValue,
    pub brier: QuantisedValue,
    pub expected_calibration_error: QuantisedValue,
    /// Prediction set meets the acceptability set.
    pub covered: u64,
    pub coverage_lower: UnitRationalWire,
    pub coverage_upper: UnitRationalWire,
    /// Items the act rule acted on, and how many of those were wrong.
    pub acted: u64,
    pub acted_wrong: u64,
    pub act_risk_upper: UnitRationalWire,
    /// Sum of prediction-set sizes; mean = this / n_items.
    pub set_size_total: u64,
}

/// Why items were refused as labels, by reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LabelExclusions {
    pub outside_window: u64,
    pub wrong_regime: u64,
    pub llm_resolved: u64,
    pub self_reported: u64,
    pub not_observation: u64,
    pub censored: u64,
    pub below_fidelity_floor: u64,
    pub propensity_not_executed_policy: u64,
    pub pinned: u64,
    pub unapproved_principal: u64,
    pub zero_executed_propensity: u64,
}

/// One option's pooled success rate (Beta-Binomial, class -> option), reported
/// only when its own trials reach the policy's `min_support`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PooledRate {
    pub class_key: String,
    pub option_id: String,
    pub trials: u64,
    pub posterior_mean: QuantisedValue,
}

/// The receipt a head must carry before it may be published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionEvalReceipt {
    pub receipt_digest: String,
    pub head_digest: String,
    pub policy_digest: String,
    pub n_records: u64,
    pub estimates: BoundedVec<OpeEstimateView, 8>,
    #[serde(default)]
    pub calibration: Option<CalibrationStatement>,
    #[serde(default)]
    pub metrics: Option<FullLabelMetrics>,
    pub exclusions: LabelExclusions,
    /// Bandit regime only: pooled per-option success rates at min support.
    #[serde(default)]
    pub pooled: BoundedVec<PooledRate, 64>,
    /// Every promotion gate that failed, by name. Empty exactly when `passed`.
    #[serde(default)]
    pub failed_gates: BoundedVec<String, 16>,
    pub passed: bool,
    pub synthetic: bool,
}

/// What a finished job produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "output", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DecisionJobOutput {
    Fit {
        draft_sha256: String,
        draft_length: u64,
        head_digest: String,
        training_records_digest: String,
        synthetic: bool,
        /// The draft body itself; publishing it as a `DecisionHead` needs a
        /// passed evaluation receipt naming `draft_sha256`.
        draft: Box<DecisionHeadBody>,
        exclusions: LabelExclusions,
    },
    /// Boxed: a receipt is several times the size of every other arm.
    Eval { receipt: Box<DecisionEvalReceipt> },
}

/// Where a job is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DecisionJobState {
    Queued,
    Running,
    /// Boxed: the output carries a whole head or receipt.
    Succeeded {
        output: Box<DecisionJobOutput>,
    },
    Failed {
        code: String,
    },
    Cancelled,
}

/// Which of the two jobs a row is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DecisionJobKind {
    Fit,
    Eval,
}

/// One durable decision-job row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionJobRecord {
    pub schema_version: u16,
    pub job_id: String,
    pub kind: DecisionJobKind,
    pub tenant_id: String,
    pub request_digest: String,
    pub submitted_at_ms: u64,
    pub state: DecisionJobState,
}

/// Declare one `{ submit, status }` decision-job op enum.
///
/// The two jobs differ only in their submission body and their authz action.
/// Writing the enum, the read/write classifier and the tenant accessor twice
/// would be two copies of the same three-line answer, so the shape is declared
/// once here and instantiated twice below.
macro_rules! decision_job_op {
    (
        $(#[$op_meta:meta])*
        $name:ident submits $request:ident under $action:literal
    ) => {
        $(#[$op_meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
        #[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
        pub enum $name {
            /// Enqueue the job. The only mutating op. Boxed: a submission
            /// carries its whole labelled dataset.
            Submit { request: Box<$request> },
            /// Read one submitted job's row.
            Status { request: DecisionJobStatusRequest },
        }

        impl $name {
            /// Whether this operation commits durable state. The ONE
            /// classifier: `server::access::requires_write` and the capability
            /// policy both delegate here, so they cannot disagree.
            pub fn is_mutation(&self) -> bool {
                matches!(self, Self::Submit { .. })
            }

            /// Both operations are administrative; reading another tenant's
            /// fit history is as privileged as starting one.
            pub fn authz_action(&self) -> &'static str {
                $action
            }

            /// The tenant this operation names, compared against the verified
            /// request tenant in the handler.
            pub fn tenant_id(&self) -> &str {
                match self {
                    Self::Submit { request } => &request.tenant_id,
                    Self::Status { request } => &request.tenant_id,
                }
            }
        }
    };
}

decision_job_op! {
    /// Fit a decision head.
    DecisionFitOp submits DecisionFitRequest under "admin:decision-fit"
}

decision_job_op! {
    /// Evaluate a candidate decision head.
    DecisionEvalOp submits DecisionEvalRequest under "admin:decision-eval"
}
