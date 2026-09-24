//! Retrieval learning over the decision log (EH-394, EH-395, EH-396).
//!
//! A retrieval-plan decision (`QuestionKind::RetrievalPlan`, EH-029) is
//! committed to the log like any other. What the run then RETURNED and which
//! of those units the answer CITED is attested by the committing principal as
//! a [`RetrievalOutcome`] joined to that record. Nothing learned from it is a
//! label until an independent evaluation of the same record exists (EH-016):
//! an observation or better, produced by neither the selected agent, its lease
//! holder nor the principal that attested the outcome, traced at or above the
//! fidelity floor, not censored.
//!
//! From the joined log the engine answers, without any model:
//! * durable hard negatives -- returned, uncited units that outranked a cited
//!   one in an independently judged successful run (EH-395);
//! * per content-class usage -- how often each class was returned and cited,
//!   k-anonymised by the policy's `min_support` (read by AU's admission
//!   feedback, EH-398);
//! * proven retrieval paths -- typed, data-independent plan templates keyed by
//!   task class and composed schema identity, with their judged successes and
//!   failures (EH-394). A template is a PLAN, never cached rows: row-level
//!   security is re-applied every time one runs.
//!
//! Every read filters by the joined record's own visibility first, so a
//! learned artefact never reveals a unit, a count or a path the reader could
//! not already see.

use serde::{Deserialize, Serialize};

use super::super::jobs::RecordWindow;
use super::retrieval_adapter::{AdapterFitRequest, AdapterFitted};
use super::retrieval_generation::{GenerationEvalRequest, GenerationEvaluated};
use super::retrieval_pointer::PointerState;
use crate::contract::BoundedVec;

/// Format identity of every retrieval-learning body.
pub const RETRIEVAL_LEARNING_SCHEMA_VERSION: u16 = 1;
/// Most evidence units one outcome may report.
pub const MAX_RETURNED_EVIDENCE: usize = 256;
/// Widest query vector an outcome may carry.
pub const MAX_QUERY_DIMENSIONS: usize = 4_096;
/// Most hops one path template may declare.
pub const MAX_PATH_EDGES: usize = 8;
/// Most hard negatives one read answers.
pub const MAX_HARD_NEGATIVES: usize = 4_096;
/// Most proven paths one read answers.
pub const MAX_PROVEN_PATHS: usize = 64;
/// Most content classes one usage read answers.
pub const MAX_USAGE_CLASSES: usize = 1_024;

/// One evidence unit a retrieval returned, in rank order (index 0 = rank 1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ReturnedEvidence {
    pub evidence_id: String,
    /// The ingestion content class of the unit (AU's admission table), when
    /// known; read by the per-class usage aggregate.
    #[serde(default)]
    pub content_class: Option<String>,
}

/// A fixed-point query vector: component `i` is `q16[i] / 2^16`, in the
/// embedding space whose `EmbeddingSpaceRef::digest` is `space_digest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct QueryVector {
    pub space_digest: String,
    pub q16: BoundedVec<i32, MAX_QUERY_DIMENSIONS>,
}

/// One hop sequence of a path template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PathEdge {
    pub relationship: String,
    pub min_hops: u8,
    pub max_hops: u8,
}

/// How a path template orders what it reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PathRank {
    /// Topology order only.
    Unranked,
    /// Vector similarity to the bound query.
    Vector,
    /// BM25 over the bound query text.
    Text,
    /// Reciprocal-rank fusion of the vector and text legs.
    FuseRrf,
}

/// The type of one slot a template binds at run time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SlotType {
    QueryText,
    QueryVector,
    AnchorId,
    ClassIri,
}

/// A typed, parameterised, data-independent retrieval plan (EH-394). It names
/// schema terms and slot types only -- never a row -- so it stays valid for as
/// long as the composed schema it was proven under (`composed_digest`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RetrievalPathTemplate {
    /// The task class the run served (an IRI).
    pub task_class: String,
    /// The composed GraphSchema identity the plan's terms resolve under.
    pub composed_digest: String,
    /// The retrieval policy version the run used.
    pub policy_version: String,
    pub anchor_class: String,
    #[serde(default)]
    pub edges: BoundedVec<PathEdge, MAX_PATH_EDGES>,
    pub rank: PathRank,
    #[serde(default)]
    pub slots: BoundedVec<SlotType, 8>,
    /// The skill component the run executed with (topology -> skill, read by
    /// AU's `graph.assemble()`).
    #[serde(default)]
    pub skill_ref: Option<String>,
}

/// What one retrieval run returned and cited, attested by the principal that
/// committed its plan decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RetrievalOutcome {
    /// The committed retrieval-plan record this run executed.
    pub record_id: String,
    #[serde(default)]
    pub returned: BoundedVec<ReturnedEvidence, MAX_RETURNED_EVIDENCE>,
    /// The returned units the answer used; a subset of `returned`.
    #[serde(default)]
    pub cited: BoundedVec<String, MAX_RETURNED_EVIDENCE>,
    /// The query vector the run ranked with, for adapter fitting (EH-396).
    #[serde(default)]
    pub query: Option<QueryVector>,
    /// The plan template the run executed, when it was a typed path.
    #[serde(default)]
    pub path: Option<RetrievalPathTemplate>,
}

fn unique<'a>(ids: impl Iterator<Item = &'a str>) -> bool {
    let mut seen = std::collections::BTreeSet::new();
    ids.into_iter().all(|id| !id.is_empty() && seen.insert(id))
}

fn check_path(path: &RetrievalPathTemplate) -> Result<(), String> {
    let named = [
        &path.task_class,
        &path.composed_digest,
        &path.policy_version,
        &path.anchor_class,
    ];
    if named.iter().any(|value| value.is_empty()) {
        return Err("a path template names its task, schema, policy and anchor".to_string());
    }
    let bounded = path.edges.iter().all(|edge| {
        !edge.relationship.is_empty() && edge.min_hops <= edge.max_hops && edge.max_hops <= 8
    });
    if !bounded {
        return Err("a path edge is a named relationship with 0 <= min <= max <= 8".to_string());
    }
    Ok(())
}

impl RetrievalOutcome {
    /// The validating check: unique returned ids, cited within returned, a
    /// non-empty query in a named space, a well-formed template.
    pub fn check(&self) -> Result<(), String> {
        if self.record_id.is_empty() {
            return Err("a retrieval outcome names its record".to_string());
        }
        if !unique(self.returned.iter().map(|e| e.evidence_id.as_str())) {
            return Err("returned evidence ids must be non-empty and unique".to_string());
        }
        let returned: std::collections::BTreeSet<&str> = self
            .returned
            .iter()
            .map(|e| e.evidence_id.as_str())
            .collect();
        let cited_ok = unique(self.cited.iter().map(String::as_str))
            && self.cited.iter().all(|id| returned.contains(id.as_str()));
        if !cited_ok {
            return Err("cited ids must be unique and among the returned ids".to_string());
        }
        if let Some(query) = &self.query {
            if query.space_digest.is_empty() || query.q16.is_empty() {
                return Err("a query vector names its space and is non-empty".to_string());
            }
        }
        self.path.as_ref().map_or(Ok(()), check_path)
    }

    /// The 1-based rank of `evidence_id` among the returned units.
    pub fn rank_of(&self, evidence_id: &str) -> Option<u32> {
        let index = self
            .returned
            .iter()
            .position(|e| e.evidence_id == evidence_id)?;
        u32::try_from(index + 1).ok()
    }
}

/// A stored outcome and who attested it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StoredRetrievalOutcome {
    pub outcome: RetrievalOutcome,
    /// The verified principal that attested it: the record's committer.
    pub producer: String,
    pub recorded_at_ms: u64,
}

/// Ask for durable hard negatives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HardNegativeRequest {
    /// `None`: every retrieval question.
    #[serde(default)]
    pub question_id: Option<String>,
    pub window: RecordWindow,
    /// At most this many rows (bounded by [`MAX_HARD_NEGATIVES`]).
    pub limit: u32,
}

/// One returned, uncited unit that outranked a cited one in an independently
/// judged successful run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HardNegative {
    pub record_id: String,
    pub evidence_id: String,
    /// Its 1-based rank in the run.
    pub rank: u32,
    /// How many cited units it outranked.
    pub outranked_cited: u32,
}

/// The durable hard-negative set the caller may read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HardNegativeSet {
    pub schema_version: u16,
    /// Outcomes with an admitted independent success verdict.
    pub judged: u64,
    /// Outcomes with no admissible verdict (read, not learned from).
    pub unjudged: u64,
    /// True when `limit` cut the set.
    pub truncated: bool,
    pub rows: BoundedVec<HardNegative, MAX_HARD_NEGATIVES>,
}

/// How often one content class was returned and cited.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClassUsage {
    pub content_class: String,
    pub returned: u64,
    pub cited: u64,
}

/// Per content-class usage over the caller's visible outcomes. A class
/// returned fewer than `min_support` times is reported as zero on both counts
/// (k-anonymity), exactly like the outcome aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RetrievalUsage {
    pub schema_version: u16,
    pub min_support: u64,
    /// Visible outcomes inside the window.
    pub outcomes: u64,
    pub rows: BoundedVec<ClassUsage, MAX_USAGE_CLASSES>,
}

/// Ask for the proven paths of one task class under one schema identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PathRequest {
    pub task_class: String,
    pub composed_digest: String,
    /// `None`: every policy version.
    #[serde(default)]
    pub policy_version: Option<String>,
    pub window: RecordWindow,
}

/// One template with its judged record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProvenPath {
    /// `sha256:<hex>` of the template (domain `eg/retrieval-path/v1`).
    pub template_digest: String,
    pub template: RetrievalPathTemplate,
    /// Independently judged successful runs; at least one, or it is absent.
    pub successes: u64,
    pub failures: u64,
}

/// The proven paths the caller may read, most successes first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProvenPaths {
    pub schema_version: u16,
    pub rows: BoundedVec<ProvenPath, MAX_PROVEN_PATHS>,
}

/// One retrieval-learning operation (`DecisionLog.retrieval`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RetrievalOp {
    /// Join what a committed retrieval run returned and cited.
    RecordOutcome { outcome: Box<RetrievalOutcome> },
    /// Read durable hard negatives.
    HardNegatives { request: HardNegativeRequest },
    /// Read per content-class usage.
    Usage { window: RecordWindow },
    /// Read the proven paths of one task class.
    Paths { request: PathRequest },
    /// Fit a query-side adapter and evaluate it on a held-out split (EH-396).
    FitAdapter { request: Box<AdapterFitRequest> },
    /// Make an evaluated adapter the active one of its space.
    ActivateAdapter {
        space_digest: String,
        adapter_digest: String,
        receipt_digest: String,
    },
    /// Return a space to the adapter active before the current one.
    RollbackAdapter { space_digest: String },
    /// Read a space's adapter pointer and history.
    AdapterStatus { space_digest: String },
    /// Dual-serve judged runs against a shadow generation (EH-397).
    EvaluateGeneration { request: Box<GenerationEvalRequest> },
    /// Resolve a logical graph to an evaluated shadow generation.
    ActivateGeneration {
        logical: String,
        shadow_graph: String,
        receipt_digest: String,
    },
    /// Return a logical graph to the generation before the current one.
    RollbackGeneration { logical: String },
    /// Read a logical graph's generation pointer and history.
    GenerationStatus { logical: String },
}

/// Who an operation is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpClass {
    /// The committer attesting its own run.
    Attest,
    /// Changes what the engine serves: the head administrator's.
    Govern,
    Read,
}

impl RetrievalOp {
    fn class(&self) -> OpClass {
        match self {
            Self::RecordOutcome { .. } => OpClass::Attest,
            Self::FitAdapter { .. }
            | Self::ActivateAdapter { .. }
            | Self::RollbackAdapter { .. }
            | Self::EvaluateGeneration { .. }
            | Self::ActivateGeneration { .. }
            | Self::RollbackGeneration { .. } => OpClass::Govern,
            Self::HardNegatives { .. }
            | Self::Usage { .. }
            | Self::Paths { .. }
            | Self::AdapterStatus { .. }
            | Self::GenerationStatus { .. } => OpClass::Read,
        }
    }

    /// Whether this operation commits durable state.
    pub fn is_mutation(&self) -> bool {
        self.class() != OpClass::Read
    }

    /// The authorization action this operation needs.
    pub fn authz_action(&self) -> &'static str {
        match self.class() {
            OpClass::Attest => "agent:decision-write",
            OpClass::Govern => "admin:decision-head",
            OpClass::Read => "agent:decision-read",
        }
    }
}

/// What one retrieval-learning operation answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RetrievalResult {
    Recorded(Box<StoredRetrievalOutcome>),
    HardNegatives(HardNegativeSet),
    Usage(RetrievalUsage),
    Paths(ProvenPaths),
    Fitted(Box<AdapterFitted>),
    Generation(Box<GenerationEvaluated>),
    Pointer(Box<PointerState>),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(returned: &[&str], cited: &[&str]) -> RetrievalOutcome {
        RetrievalOutcome {
            record_id: "r1".to_string(),
            returned: BoundedVec::new(
                returned
                    .iter()
                    .map(|id| ReturnedEvidence {
                        evidence_id: id.to_string(),
                        content_class: None,
                    })
                    .collect(),
            )
            .unwrap(),
            cited: BoundedVec::new(cited.iter().map(|id| id.to_string()).collect()).unwrap(),
            query: None,
            path: None,
        }
    }

    #[test]
    fn cited_units_are_unique_and_among_the_returned_ones() {
        assert!(outcome(&["a", "b"], &["b"]).check().is_ok());
        assert!(outcome(&["a", "a"], &[]).check().is_err());
        assert!(outcome(&["a"], &["z"]).check().is_err());
        assert!(outcome(&["a", "b"], &["b", "b"]).check().is_err());
        assert_eq!(outcome(&["a", "b"], &[]).rank_of("b"), Some(2));
    }

    #[test]
    fn a_path_template_bounds_its_hops() {
        let mut o = outcome(&["a"], &["a"]);
        o.path = Some(RetrievalPathTemplate {
            task_class: "urn:task:q".to_string(),
            composed_digest: "sha256:x".to_string(),
            policy_version: "1".to_string(),
            anchor_class: "Doc".to_string(),
            edges: BoundedVec::new(vec![PathEdge {
                relationship: "CITES".to_string(),
                min_hops: 2,
                max_hops: 1,
            }])
            .unwrap(),
            rank: PathRank::Vector,
            slots: BoundedVec::default(),
            skill_ref: None,
        });
        assert!(o.check().is_err());
    }
}
