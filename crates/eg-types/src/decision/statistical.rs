//! `Method::Decide`: a statistical decision over library or graph candidates,
//! and the record version 2 it produces.
//!
//! This surface is EVALUATE-ONLY in 2.27.x. It reads candidates, scores them
//! under a pinned feature schema and head, and returns a batch of records; it
//! commits nothing. Whether a record is durable, and where, is a separate
//! decision with its own method.

pub mod outcome;

use serde::{Deserialize, Serialize};

use super::errors::DecisionErrorCode;
use super::numeric::{QuantScaleTag, UnitRationalWire};
use super::record::{
    CandidateSourceRecord, EvidenceClass, PremiseRef, ResolutionKind, TraceFidelity,
};
use super::request::{DecisionPolicyRef, LibraryCandidateScope};
use crate::agent_component::ComponentDependency;
use crate::contract::BoundedVec;

pub use outcome::{
    CalibrationMethod, CalibrationStatement, RiskMethod, RiskStatement, ScoredOption,
    StatisticalOutcome,
};

/// The largest record batch one `Decide` may return.
pub const MAX_DECIDE_RECORDS: u16 = 256;

/// What class of question this is. The kind selects the executor; it never
/// selects the guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum QuestionKind {
    Route,
    Rank,
    Classify,
    ResolveEntity,
    SchemaMapping,
    TemplateChoice,
    RetrievalPlan,
    IngestionLane,
    EnrichmentSchedule,
}

/// What is at stake. Exploration is permitted only for
/// [`QuestionSafety::Ordinary`]; everything else must be decided on evidence
/// already in hand or abstained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum QuestionSafety {
    Ordinary,
    Security,
    Policy,
    WriteBack,
    Irreversible,
}

/// One named, classified question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StatisticalQuestion {
    pub question_id: String,
    pub kind: QuestionKind,
    pub safety: QuestionSafety,
}

/// Where the options come from.
/// Not `Eq`: the cross-modal plan carries floats, so equality on a graph-sourced
/// candidate scope is partial, exactly as it is on `Method` itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CandidateSource {
    /// A tenant-bound agent library scope.
    AgentLibrary { scope: LibraryCandidateScope },
    /// An RLS-filtered cross-modal plan -- the same public plan type
    /// `Method::UnifiedQuery` accepts, so there is one plan vocabulary.
    #[cfg(feature = "query")]
    Graph { plan: Box<crate::wire::Plan> },
}

/// A typed question parameter. Values are typed, never substituted into text:
/// a parameter cannot become part of a prompt or a query string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TypedValue {
    Bool(bool),
    Int(i64),
    Text(String),
    Iri(String),
    IriList(BoundedVec<String, 64>),
    Rational(UnitRationalWire),
}

/// One named parameter of a question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TypedParam {
    pub name: String,
    pub value: TypedValue,
}

/// One statistical decision request.
/// Not `Eq`, for the same reason [`CandidateSource`] is not.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecideRequest {
    pub tenant_id: String,
    pub question: StatisticalQuestion,
    pub candidates: CandidateSource,
    pub feature_schema: ComponentDependency,
    /// Absent asks for the deterministic ladder only.
    #[serde(default)]
    pub head: Option<ComponentDependency>,
    pub policy: DecisionPolicyRef,
    /// Sorted by name and unique.
    #[serde(default)]
    pub params: BoundedVec<TypedParam, 64>,
    /// At most [`MAX_DECIDE_RECORDS`].
    #[serde(default)]
    pub max_records: Option<u16>,
}

/// How the feature matrix the decision read is pinned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "matrix", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FeatureMatrixRef {
    /// Small matrices travel inline, exactly, on a named fixed-point scale.
    Inline {
        candidate_ids: BoundedVec<String, 64>,
        feature_names: BoundedVec<String, 32>,
        scale: QuantScaleTag,
        values: BoundedVec<i64, 2048>,
    },
    /// Larger ones are pinned by content digest.
    Blob {
        sha256: String,
        length: u64,
        rows: u32,
        columns: u32,
    },
}

/// How the shortlist the decision scored was produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ShortlistProvenance {
    /// The approximate-nearest-neighbour recall mode, when one was used.
    #[serde(default)]
    pub ann_recall_mode: Option<String>,
    /// The clock the shortlist's recency filters read, recorded so the
    /// shortlist can be reproduced.
    pub now_ms: u64,
    #[serde(default)]
    pub embedder_model_digest: Option<String>,
}

/// A committed exploration draw: the seed is committed before the draw and
/// revealed after, so the draw cannot be chosen after the fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExplorationRecord {
    pub seed_commitment: String,
    #[serde(default)]
    pub revealed_seed: Option<String>,
    pub budget_digest: String,
}

/// The input set of one statistical decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StatisticalInputs {
    pub feature_schema: ComponentDependency,
    #[serde(default)]
    pub head: Option<ComponentDependency>,
    pub policy_digest: String,
    pub feature_matrix: FeatureMatrixRef,
    pub shortlist: ShortlistProvenance,
    #[serde(default)]
    pub exploration: Option<ExplorationRecord>,
}

/// One statistical decision, recorded. Record version 2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StatisticalDecisionRecord {
    pub schema_version: u16,
    pub record_id: String,
    pub tenant_id: String,
    pub caller_principal: String,
    pub created_at_ms: u64,
    pub question: StatisticalQuestion,
    pub candidate_source: CandidateSourceRecord,
    pub inputs: StatisticalInputs,
    pub inputs_digest: String,
    pub resolution_kind: ResolutionKind,
    pub evidence_class: EvidenceClass,
    pub trace_fidelity: TraceFidelity,
    pub premises: BoundedVec<PremiseRef, 1024>,
    pub outcome: StatisticalOutcome,
    #[serde(default)]
    pub calibration: Option<CalibrationStatement>,
    /// Synthetic evidence is labelled as such, always.
    pub synthetic_evidence: bool,
    pub record_digest: String,
}

impl StatisticalDecisionRecord {
    /// The validating constructor; see [`super::DecisionRecord::checked`].
    pub fn checked(self) -> Result<Self, DecisionErrorCode> {
        if self.schema_version != super::STATISTICAL_DECISION_RECORD_SCHEMA_VERSION {
            return Err(DecisionErrorCode::DecisionRecordVersionUnsupported);
        }
        Ok(self)
    }
}

/// One evaluate-only answer: the records the decision produced, and the digest
/// of the inputs they share.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionBatch {
    pub schema_version: u16,
    pub inputs_digest: String,
    pub records: BoundedVec<StatisticalDecisionRecord, 256>,
}
