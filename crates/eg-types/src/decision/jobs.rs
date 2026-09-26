//! The two admin jobs behind a calibrated decision head: fitting one, and
//! evaluating a candidate before it may be published.
//!
//! Both follow the `AnalyticsJob` shape -- a `submit`/`status` op pair over a
//! durable job row -- because that is how every other long-running admin task
//! on this engine is already driven, and a second shape would mean a second
//! way to lose a job.

use serde::{Deserialize, Serialize};

use super::numeric::{QuantisedValue, UnitRationalWire};
use super::replay::{EvalMode, EvaluationRun};
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
    /// Where the labelled items come from. A full-label regime's
    /// `gold_set_digest` must equal the inline dataset's digest.
    pub source: DatasetSource,
}

/// Where a job's labelled items come from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DatasetSource {
    /// A submitted dataset, pinned by digest (gold sets, exported suites).
    Inline { dataset: Box<LabelledDataset> },
    /// The engine's own decision log: every committed, executed record of
    /// `question_id` the caller may read, with its independent evaluations.
    /// Bandit labels only.
    Logged { question_id: String },
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
    /// Where the labelled items the candidate is evaluated on come from.
    pub source: DatasetSource,
    /// Off-policy (the default) or a walk-forward replay (EH-528).
    #[serde(default)]
    pub mode: EvalMode,
}

/// Ask after one submitted job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionJobStatusRequest {
    pub tenant_id: String,
    pub job_id: String,
}

/// Admin-only lookup of one persisted evaluation receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionReceiptGetRequest {
    pub tenant_id: String,
    pub receipt_digest: String,
}

/// Admin-only, key-ordered discovery. The cursor is the last receipt digest
/// returned by the preceding page; a page may contain at most 50 receipts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionReceiptListRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub after: Option<String>,
    pub limit: u16,
}

/// Time-ordered discovery of independently labelled evaluation receipts.
/// The cursor is an opaque `<20-digit-ms>:<receipt-digest>` key suffix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionReceiptTimelineRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub after: Option<String>,
    pub limit: u16,
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

/// One calibration class's share of a full-label evaluation (per domain).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClassCoverage {
    pub class_key: String,
    pub n_items: u64,
    pub covered: u64,
    /// Historical class support floor. `None` on receipts produced before
    /// per-class interval reporting was added.
    #[serde(default)]
    pub n_min: Option<u64>,
    /// Bounds are withheld until this class meets its own `n_min`.
    #[serde(default)]
    pub metrics: Option<ClassLabelMetrics>,
}

/// Full-label evidence for one class, with marginal intervals at the pinned
/// policy's `delta`. These do not assert a simultaneous guarantee across
/// classes. Act risk is unavailable when no item in the class was acted on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClassLabelMetrics {
    pub top1_hits: u64,
    /// The pinned policy's failure probability for these intervals.
    pub delta: UnitRationalWire,
    pub coverage_lower: UnitRationalWire,
    pub coverage_upper: UnitRationalWire,
    pub acted: u64,
    pub acted_wrong: u64,
    pub act_risk_upper: Option<UnitRationalWire>,
}

/// The promotion protocol's measurements beyond [`FullLabelMetrics`]
/// (EH-295): soft accuracy and score error, abstention and accuracy on what
/// was answered, cost, stability, drift over recorded time, and per-class
/// coverage. Every rate is an exact count; `soft_accuracy` and `score_mae`
/// are means on `Q32`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PromotionMetrics {
    /// Mean probability mass on the acceptability set.
    pub soft_accuracy: QuantisedValue,
    /// Mean `|p_top - [top is acceptable]|`.
    pub score_mae: QuantisedValue,
    /// Items the act rule answered, how many of those were right, and how
    /// many it abstained on (out of distribution or below its threshold).
    pub answered: u64,
    pub answered_hits: u64,
    pub abstained: u64,
    /// Multiply-accumulates the head spent over every item (the scorer
    /// counts them; a linear head costs `options x features`).
    pub macs_total: u64,
    /// Items whose two independent reads were byte-identical.
    pub stable_items: u64,
    /// Coverage of the earlier and the later half by `recorded_at_ms`;
    /// `None` when every item carries one time and drift is not measurable.
    #[serde(default)]
    pub early_coverage: Option<UnitRationalWire>,
    #[serde(default)]
    pub late_coverage: Option<UnitRationalWire>,
    pub per_class: BoundedVec<ClassCoverage, 64>,
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
    /// The promotion protocol's further measurements (full-label only).
    /// Omitted when absent, so bandit receipts keep their bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion: Option<Box<PromotionMetrics>>,
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

/// A bounded page of persisted receipts. Listing alone does not establish
/// real-world calibration: each receipt carries its own synthetic/metrics flags.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionReceiptPage {
    pub receipts: BoundedVec<DecisionEvalReceipt, 50>,
    #[serde(default)]
    pub next_after: Option<String>,
}

/// A full-label, non-synthetic receipt with the authoritative submission time
/// of the job that committed it. Failed promotion gates remain visible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionReceiptTimelineEntry {
    pub submitted_at_ms: u64,
    pub receipt: DecisionEvalReceipt,
    /// Assessment against the policy pinned by this evaluation. Older receipts
    /// have no stored assessment and never inherit the tenant's current policy.
    #[serde(default)]
    pub threshold_alert: Option<DecisionThresholdAssessment>,
}

/// Historical policy thresholds applied to one independently labelled receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionThresholdAssessment {
    pub policy_digest: String,
    pub alpha: UnitRationalWire,
    pub epsilon: UnitRationalWire,
    pub delta: UnitRationalWire,
    pub n_min: u64,
    pub insufficient_support: bool,
    pub coverage_below_policy: Option<bool>,
    pub act_risk_above_policy: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionReceiptTimelinePage {
    pub entries: BoundedVec<DecisionReceiptTimelineEntry, 50>,
    #[serde(default)]
    pub next_after: Option<String>,
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
    /// A replay evaluation's sealed run (EH-528).
    Replay { run: Box<EvaluationRun> },
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

/// Evaluate a head or read its receipts. Every variant requires the same
/// administrative scope and tenant match; only submission mutates state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DecisionEvalOp {
    Submit {
        request: Box<DecisionEvalRequest>,
    },
    Status {
        request: DecisionJobStatusRequest,
    },
    Receipt {
        request: DecisionReceiptGetRequest,
    },
    Receipts {
        request: DecisionReceiptListRequest,
    },
    Timeline {
        request: DecisionReceiptTimelineRequest,
    },
}

impl DecisionEvalOp {
    pub fn is_mutation(&self) -> bool {
        matches!(self, Self::Submit { .. })
    }

    pub fn authz_action(&self) -> &'static str {
        "admin:decision-eval"
    }

    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Submit { request } => &request.tenant_id,
            Self::Status { request } => &request.tenant_id,
            Self::Receipt { request } => &request.tenant_id,
            Self::Receipts { request } => &request.tenant_id,
            Self::Timeline { request } => &request.tenant_id,
        }
    }
}
