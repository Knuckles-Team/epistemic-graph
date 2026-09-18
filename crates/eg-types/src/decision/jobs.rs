//! The two admin jobs behind a calibrated decision head: fitting one, and
//! evaluating a candidate before it may be published.
//!
//! Both follow the `AnalyticsJob` shape -- a `submit`/`status` op pair over a
//! durable job row -- because that is how every other long-running admin task
//! on this engine is already driven, and a second shape would mean a second
//! way to lose a job.

use serde::{Deserialize, Serialize};

use super::numeric::QuantisedValue;
use super::request::DecisionPolicyRef;
use super::statistical::CalibrationStatement;
use crate::agent_component::ComponentDependency;
use crate::contract::BoundedVec;

/// Which head shape a fit produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum HeadKind {
    /// One weight per feature.
    WeightedFeatures,
    /// A listwise logistic model over the candidate set.
    ListwiseLogistic,
}

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
    pub unsupported_mass: super::numeric::UnitRationalWire,
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
    },
    Eval {
        receipt: DecisionEvalReceipt,
    },
}

/// Where a job is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DecisionJobState {
    Queued,
    Running,
    Succeeded { output: DecisionJobOutput },
    Failed { code: String },
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
            /// Enqueue the job. The only mutating op.
            Submit { request: $request },
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
