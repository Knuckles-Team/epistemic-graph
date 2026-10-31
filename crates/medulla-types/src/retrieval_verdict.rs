//! Retrieval decisions Medulla owns: the sufficiency verdict with its repair
//! step and fallback policy (MEDULLA-FEDERATED-QUERY-R081.1), the dual-score chunk
//! selection contract (R081.7.1), retrieval feedback and critique (R081.8.1)
//! and the bounded typed-hierarchy descent contract (R081.9.1).
//!
//! These are pure data with validating constructors. The harness applies a
//! verdict; Medulla computes it (`medulla_plan::retrieval_verdict`).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A verdict call carried no `round_limit` (MEDULLA-FEDERATED-QUERY-R081.4).
pub const RETRIEVAL_ROUND_LIMIT_REQUIRED: &str = "RETRIEVAL_ROUND_LIMIT_REQUIRED";
/// A chunk lacks one of its two scores, or a score is non-finite or stale (R081.7.4).
pub const RETRIEVAL_SCORE_PAIR_REQUIRED: &str = "RETRIEVAL_SCORE_PAIR_REQUIRED";
/// The policy's normalization cannot be applied to the scores (R081.7.4).
pub const RETRIEVAL_NORMALIZATION_INVALID: &str = "RETRIEVAL_NORMALIZATION_INVALID";
/// Activation lacks the matching benchmark receipt (R081.7.4).
pub const RETRIEVAL_POLICY_UNVERIFIED: &str = "RETRIEVAL_POLICY_UNVERIFIED";
/// Feedback names forged documents or incompatible revisions (R081.8.4).
pub const RETRIEVAL_FEEDBACK_INVALID: &str = "RETRIEVAL_FEEDBACK_INVALID";
/// A hierarchy has a cycle, a stale summary or a dangling link (R081.9.4).
pub const RETRIEVAL_HIERARCHY_INVALID: &str = "RETRIEVAL_HIERARCHY_INVALID";
/// A breadth, depth, scoring or cost bound ran out (R081.9.4).
pub const RETRIEVAL_BUDGET_EXCEEDED: &str = "RETRIEVAL_BUDGET_EXCEEDED";
/// A contract field is missing, out of bounds or inconsistent.
pub const RETRIEVAL_CONTRACT_INVALID: &str = "RETRIEVAL_CONTRACT_INVALID";

/// A typed retrieval refusal: a stable code and the field or condition it names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
#[error("{code}: {detail}")]
pub struct RetrievalRefusal {
    pub code: String,
    pub detail: String,
}

impl RetrievalRefusal {
    pub fn new(code: &str, detail: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            detail: detail.into(),
        }
    }
}

fn invalid(detail: impl Into<String>) -> RetrievalRefusal {
    RetrievalRefusal::new(RETRIEVAL_CONTRACT_INVALID, detail)
}

fn require_text(field: &str, value: &str) -> Result<(), RetrievalRefusal> {
    if value.trim().is_empty() {
        return Err(invalid(format!("{field} is required")));
    }
    Ok(())
}

fn require_positive(field: &str, value: u64) -> Result<(), RetrievalRefusal> {
    if value == 0 {
        return Err(invalid(format!("{field} must be positive")));
    }
    Ok(())
}

fn require_finite(field: &str, value: f64) -> Result<(), RetrievalRefusal> {
    if !value.is_finite() {
        return Err(RetrievalRefusal::new(
            RETRIEVAL_SCORE_PAIR_REQUIRED,
            format!("{field} is not finite"),
        ));
    }
    Ok(())
}

fn require_unit(field: &str, value: f64) -> Result<(), RetrievalRefusal> {
    if !(0.0..=1.0).contains(&value) {
        return Err(invalid(format!("{field} must lie in [0, 1]")));
    }
    Ok(())
}

fn require_unique<'a>(
    field: &str,
    ids: impl IntoIterator<Item = &'a str>,
) -> Result<(), RetrievalRefusal> {
    let mut seen = BTreeSet::new();
    for id in ids {
        require_text(field, id)?;
        if !seen.insert(id) {
            return Err(invalid(format!("duplicate {field} `{id}`")));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// R081.1 — the sufficiency verdict
// ---------------------------------------------------------------------------

/// Whether the retrieved evidence suffices to answer. Closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Sufficient,
    Insufficient,
}

/// The repair step the harness applies before the next round.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RepairStep {
    /// Re-run with a larger candidate count.
    BoostK { k: u32 },
    /// Re-run under another retrieval mode.
    SwitchMode { mode: String },
    /// Run no further round.
    Stop,
}

/// What the harness may answer when evidence is insufficient.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum FallbackPolicy {
    /// An extractive answer from the partial evidence is allowed.
    Lenient,
    /// Insufficient evidence abstains; no extractive answer.
    Strict,
}

/// Medulla's judgement of one retrieval round (MEDULLA-FEDERATED-QUERY-R081.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SufficiencyVerdict {
    pub verdict: Verdict,
    pub evidence_count: u64,
    pub reason: String,
    pub repair: RepairStep,
    pub fallback: FallbackPolicy,
    pub round: u32,
    pub round_limit: u32,
}

// ---------------------------------------------------------------------------
// R081.7.1 — dual-score chunk selection
// ---------------------------------------------------------------------------

/// The two already computed scores of one candidate chunk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ChunkScorePair {
    pub chunk_id: String,
    pub corpus_revision: String,
    pub embedding_score: f64,
    pub cross_encoder_score: f64,
    pub embedding_model_revision: String,
    pub cross_encoder_model_revision: String,
}

/// How raw scores are mapped onto `[0, 1]` before thresholds apply.
///
/// The degenerate cases are declared, not implied: an empty candidate set
/// selects nothing, and a constant score column (max equals min) normalizes
/// to `constant_value` under `MinMax`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScoreNormalization {
    /// Scores are already in `[0, 1]`; any score outside refuses.
    Identity,
    /// Min-max rescaling per score column; a constant column maps to `constant_value`.
    MinMax { constant_value: f64 },
}

/// The declared selection policy (MEDULLA-FEDERATED-QUERY-R081.7.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SelectionPolicy {
    pub policy_revision: String,
    pub corpus_revision: String,
    pub embedding_model_revision: String,
    pub cross_encoder_model_revision: String,
    pub normalization: ScoreNormalization,
    /// A normalized embedding score at or above this is "high".
    pub embedding_threshold: f64,
    /// A normalized cross-encoder score at or above this is "high".
    pub relevance_threshold: f64,
    /// Embedding weight `w` of the fused score `w*e + (1-w)*r`.
    pub fusion_weight: f64,
    /// The fused score a residual-region candidate must reach.
    pub fusion_threshold: f64,
    pub max_chunks: u32,
    /// Digest of the benchmark receipt that approved activation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_benchmark_digest: Option<String>,
}

impl SelectionPolicy {
    /// Refuse missing provenance and invalid bounds.
    pub fn validate(&self) -> Result<(), RetrievalRefusal> {
        require_text("policy_revision", &self.policy_revision)?;
        require_text("corpus_revision", &self.corpus_revision)?;
        require_text("embedding_model_revision", &self.embedding_model_revision)?;
        require_text(
            "cross_encoder_model_revision",
            &self.cross_encoder_model_revision,
        )?;
        for (field, value) in [
            ("embedding_threshold", self.embedding_threshold),
            ("relevance_threshold", self.relevance_threshold),
            ("fusion_weight", self.fusion_weight),
            ("fusion_threshold", self.fusion_threshold),
        ] {
            require_finite(field, value)?;
            require_unit(field, value)?;
        }
        if let ScoreNormalization::MinMax { constant_value } = self.normalization {
            if !constant_value.is_finite() || !(0.0..=1.0).contains(&constant_value) {
                return Err(RetrievalRefusal::new(
                    RETRIEVAL_NORMALIZATION_INVALID,
                    "constant_value must be a finite value in [0, 1]",
                ));
            }
        }
        require_positive("max_chunks", u64::from(self.max_chunks))
    }

    /// Refuse a chunk whose scores are missing, non-finite or stale against this policy.
    pub fn check_pair(&self, pair: &ChunkScorePair) -> Result<(), RetrievalRefusal> {
        require_text("chunk_id", &pair.chunk_id)?;
        require_finite("embedding_score", pair.embedding_score)?;
        require_finite("cross_encoder_score", pair.cross_encoder_score)?;
        let stale = [
            (
                "corpus_revision",
                &pair.corpus_revision,
                &self.corpus_revision,
            ),
            (
                "embedding_model_revision",
                &pair.embedding_model_revision,
                &self.embedding_model_revision,
            ),
            (
                "cross_encoder_model_revision",
                &pair.cross_encoder_model_revision,
                &self.cross_encoder_model_revision,
            ),
        ]
        .into_iter()
        .find(|(_, got, want)| got != want);
        if let Some((field, got, want)) = stale {
            return Err(RetrievalRefusal::new(
                RETRIEVAL_SCORE_PAIR_REQUIRED,
                format!(
                    "chunk `{}` {field} `{got}` is stale against `{want}`",
                    pair.chunk_id
                ),
            ));
        }
        Ok(())
    }

    /// Validate a candidate set: every pair current, no duplicate chunk ids.
    pub fn check_pairs(&self, pairs: &[ChunkScorePair]) -> Result<(), RetrievalRefusal> {
        for pair in pairs {
            self.check_pair(pair)?;
        }
        require_unique("chunk_id", pairs.iter().map(|p| p.chunk_id.as_str()))
    }
}

/// The score region a candidate falls into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ScoreRegion {
    /// Both scores high: retained.
    DualHigh,
    /// Both scores low: rejected.
    DualLow,
    /// Low embedding, high relevance (vocabulary mismatch): rescued.
    Rescue,
    /// High embedding, low relevance: decided by the fused threshold.
    Residual,
}

/// One candidate's classification inside a [`SelectionReceipt`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ChunkSelection {
    pub chunk_id: String,
    pub embedding_score: f64,
    pub cross_encoder_score: f64,
    pub normalized_embedding: f64,
    pub normalized_relevance: f64,
    pub region: ScoreRegion,
    pub fused_score: f64,
    pub selected: bool,
}

/// What one selection decided (MEDULLA-FEDERATED-QUERY-R081.7.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SelectionReceipt {
    pub policy_revision: String,
    pub corpus_revision: String,
    pub normalization: ScoreNormalization,
    pub candidates: Vec<ChunkSelection>,
    /// Selected chunk ids, best first.
    pub selected: Vec<String>,
    pub max_chunks: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_benchmark_digest: Option<String>,
    /// Selection performs no model inference; always zero.
    pub selection_inference_calls: u32,
}

// ---------------------------------------------------------------------------
// R081.8.1 — retrieval feedback and critique
// ---------------------------------------------------------------------------

/// How feedback treats one term. Closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum FeedbackTier {
    Reinforce,
    Missing,
    Suppress,
}

/// One bounded semantic-perspective expansion of the original query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PerspectiveQuery {
    pub perspective: String,
    pub text: String,
}

/// A term's tier and weight, bound to the documents whose measured relevance produced it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct TermFeedback {
    pub term: String,
    pub tier: FeedbackTier,
    pub weight: f64,
    pub evidence_document_ids: Vec<String>,
}

/// A critique of the round. Untrusted data: it may propose a reformulation,
/// never a score, scope, limit or activation change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RetrievalCritique {
    pub text: String,
    pub model_revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_reformulation: Option<String>,
}

/// The per-continuation budgets feedback must stay within.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FeedbackLimits {
    pub max_rounds: u32,
    pub max_queries: u32,
    pub max_terms: u32,
    pub max_tokens: u64,
    pub max_inference_calls: u32,
}

impl FeedbackLimits {
    fn validate(&self) -> Result<(), RetrievalRefusal> {
        require_positive("max_rounds", u64::from(self.max_rounds))?;
        require_positive("max_queries", u64::from(self.max_queries))?;
        require_positive("max_terms", u64::from(self.max_terms))?;
        require_positive("max_tokens", self.max_tokens)?;
        require_positive("max_inference_calls", u64::from(self.max_inference_calls))
    }
}

/// Measured relevance of one returned document against the original query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DocumentRelevance {
    pub document_id: String,
    pub relevance: f64,
}

/// One round's revision-bound feedback (MEDULLA-FEDERATED-QUERY-R081.8.1).
///
/// Termination is not decided here: the R082 stop rule is the one stop authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RetrievalFeedback {
    pub original_query: String,
    pub round: u32,
    pub corpus_revision: String,
    pub model_revision: String,
    pub perspectives: Vec<PerspectiveQuery>,
    pub documents: Vec<DocumentRelevance>,
    pub terms: Vec<TermFeedback>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub critique: Option<RetrievalCritique>,
    pub limits: FeedbackLimits,
}

impl RetrievalFeedback {
    /// Refuse unbounded requests, missing evidence ids and inconsistent revisions.
    pub fn validate(
        &self,
        corpus_revision: &str,
        model_revision: &str,
    ) -> Result<(), RetrievalRefusal> {
        require_text("original_query", &self.original_query)?;
        self.limits.validate()?;
        if self.round == 0 || self.round > self.limits.max_rounds {
            return Err(invalid(format!(
                "round {} is outside 1..={}",
                self.round, self.limits.max_rounds
            )));
        }
        if self.corpus_revision != corpus_revision || self.model_revision != model_revision {
            return Err(RetrievalRefusal::new(
                RETRIEVAL_FEEDBACK_INVALID,
                "feedback revisions do not match the serving corpus and model",
            ));
        }
        if self.perspectives.len() > self.limits.max_queries as usize {
            return Err(invalid("perspectives exceed max_queries"));
        }
        if self.terms.len() > self.limits.max_terms as usize {
            return Err(invalid("terms exceed max_terms"));
        }
        require_unique(
            "document_id",
            self.documents.iter().map(|d| d.document_id.as_str()),
        )?;
        self.validate_terms()
    }

    fn validate_terms(&self) -> Result<(), RetrievalRefusal> {
        let known: BTreeSet<&str> = self
            .documents
            .iter()
            .map(|d| d.document_id.as_str())
            .collect();
        for term in &self.terms {
            require_text("term", &term.term)?;
            if !term.weight.is_finite() {
                return Err(invalid(format!(
                    "term `{}` weight is not finite",
                    term.term
                )));
            }
            if term.tier != FeedbackTier::Missing && term.evidence_document_ids.is_empty() {
                return Err(invalid(format!("term `{}` has no evidence ids", term.term)));
            }
            if let Some(forged) = term
                .evidence_document_ids
                .iter()
                .find(|id| !known.contains(id.as_str()))
            {
                return Err(RetrievalRefusal::new(
                    RETRIEVAL_FEEDBACK_INVALID,
                    format!("term `{}` cites unreturned document `{forged}`", term.term),
                ));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// R081.9.1 — bounded typed hierarchy retrieval
// ---------------------------------------------------------------------------

/// The descent stage a receipt entry belongs to, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum DescentStage {
    Summary,
    Community,
    Entity,
}

/// The declared descent policy (MEDULLA-FEDERATED-QUERY-R081.9.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DescentPolicy {
    pub hierarchy_revision: String,
    pub policy_revision: String,
    pub beam_width: u32,
    pub max_depth: u32,
    pub max_visited_communities: u32,
    pub max_entity_candidates: u32,
    pub max_reranker_calls: u32,
    pub max_tokens: u64,
    pub max_cost: u64,
}

impl DescentPolicy {
    /// Refuse an absent hierarchy revision and zero required limits.
    pub fn validate(&self) -> Result<(), RetrievalRefusal> {
        require_text("hierarchy_revision", &self.hierarchy_revision)?;
        require_text("policy_revision", &self.policy_revision)?;
        for (field, value) in [
            ("beam_width", u64::from(self.beam_width)),
            ("max_depth", u64::from(self.max_depth)),
            (
                "max_visited_communities",
                u64::from(self.max_visited_communities),
            ),
            (
                "max_entity_candidates",
                u64::from(self.max_entity_candidates),
            ),
            ("max_reranker_calls", u64::from(self.max_reranker_calls)),
            ("max_tokens", self.max_tokens),
            ("max_cost", self.max_cost),
        ] {
            require_positive(field, value)?;
        }
        Ok(())
    }
}

/// One community node of a hierarchy, with its summary and member entities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct HierarchyCommunity {
    pub community_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub summary_id: String,
    /// The hierarchy revision the summary was generated at.
    pub summary_revision: String,
    pub tenant: String,
    pub entity_ids: Vec<String>,
}

/// Validate parent links: every parent exists, ids are unique, no cycle.
pub fn validate_hierarchy(
    communities: &[HierarchyCommunity],
    hierarchy_revision: &str,
) -> Result<(), RetrievalRefusal> {
    require_unique(
        "community_id",
        communities.iter().map(|c| c.community_id.as_str()),
    )?;
    let parents: std::collections::BTreeMap<&str, Option<&str>> = communities
        .iter()
        .map(|c| (c.community_id.as_str(), c.parent_id.as_deref()))
        .collect();
    for community in communities {
        if community.summary_revision != hierarchy_revision {
            return Err(RetrievalRefusal::new(
                RETRIEVAL_HIERARCHY_INVALID,
                format!("community `{}` summary is stale", community.community_id),
            ));
        }
        check_ancestry(&community.community_id, &parents)?;
    }
    Ok(())
}

fn check_ancestry(
    start: &str,
    parents: &std::collections::BTreeMap<&str, Option<&str>>,
) -> Result<(), RetrievalRefusal> {
    let mut seen = BTreeSet::from([start]);
    let mut cursor = parents.get(start).copied().flatten();
    while let Some(parent) = cursor {
        let Some(next) = parents.get(parent) else {
            return Err(RetrievalRefusal::new(
                RETRIEVAL_HIERARCHY_INVALID,
                format!("community `{start}` has dangling parent `{parent}`"),
            ));
        };
        if !seen.insert(parent) {
            return Err(RetrievalRefusal::new(
                RETRIEVAL_HIERARCHY_INVALID,
                format!("community `{start}` is on a parent cycle"),
            ));
        }
        cursor = *next;
    }
    Ok(())
}

/// One scored visit in a descent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct HierarchyVisit {
    pub stage: DescentStage,
    pub id: String,
    pub score: f64,
    /// Which scorer produced `score`.
    pub score_provenance: String,
    pub kept: bool,
    /// Root-to-node community path.
    pub path: Vec<String>,
}

/// What one descent did (MEDULLA-FEDERATED-QUERY-R081.9.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DescentReceipt {
    pub hierarchy_revision: String,
    pub policy_revision: String,
    pub beam_width: u32,
    pub depth: u32,
    pub visits: Vec<HierarchyVisit>,
    pub visited_communities: u32,
    pub entity_candidates: u32,
    pub reranker_calls: u32,
    pub cost: u64,
    /// Final entities, best first, each with its community path.
    pub entities: Vec<HierarchyVisit>,
    /// True when a bound stopped the descent before it finished.
    pub incomplete: bool,
}

// ---- Typed document-section hierarchy (MEDULLA-FEDERATED-QUERY-R081.9.7.1) ----
// The community descent contract above uses the `Descent*` names; the section-aware
// `Hierarchy*` policy and receipt below follow R081.9.1/R081.9.7.1.

/// The two hierarchy families share one policy shape but have different node
/// contracts.  A document-section policy never silently falls back to a
/// community policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum HierarchyRetrievalKind {
    Community,
    DocumentSection,
}

/// The stored granularity of one document-section node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SectionGranularity {
    Document,
    Section,
    SourceSpan,
}

/// The stage whose score and visited evidence a receipt describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum HierarchyStage {
    Community,
    DocumentSection,
}

/// A parent relation carried by a hierarchy policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HierarchyParentLink {
    pub parent_id: String,
    pub child_id: String,
}

/// An inclusive, document-revision-bound source span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SectionSourceSpan {
    pub start: u64,
    pub end: u64,
}

/// One stored node in a document-section hierarchy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HierarchySectionNode {
    pub id: String,
    pub document_id: String,
    pub document_revision: String,
    pub parent_id: Option<String>,
    pub ordered_children: Vec<String>,
    pub granularity: SectionGranularity,
    pub summary: String,
    pub source_span: SectionSourceSpan,
}

/// A score input is named and revision-bound so a runtime can reconstruct
/// why a candidate was considered without treating a score as authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HierarchyScoreProvenance {
    pub scorer: String,
    pub scorer_revision: String,
    pub basis: String,
}

/// The bounded policy shared by community and document-section retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HierarchyRetrievalPolicy {
    pub kind: HierarchyRetrievalKind,
    pub hierarchy_revision: String,
    pub community_ids: Vec<String>,
    pub summary_ids: Vec<String>,
    pub entity_ids: Vec<String>,
    pub parent_links: Vec<HierarchyParentLink>,
    pub stage: HierarchyStage,
    pub score_provenance: Vec<HierarchyScoreProvenance>,
    pub beam_width: u32,
    pub max_depth: u32,
    pub max_visited_communities: u32,
    pub max_entity_candidates: u32,
    pub max_reranker_calls: u32,
    pub max_tokens: u32,
    pub max_total_cost_micros: u64,
    pub section_nodes: Vec<HierarchySectionNode>,
}

/// Evidence emitted by a bounded hierarchy stage.  It is deliberately a
/// record, not an authorization or a claim that serving has completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HierarchyStageReceipt {
    pub hierarchy_revision: String,
    pub kind: HierarchyRetrievalKind,
    pub stage: HierarchyStage,
    pub visited_ids: Vec<String>,
    pub selected_id: Option<String>,
    pub pruned_ids: Vec<String>,
    pub score_provenance: Vec<HierarchyScoreProvenance>,
    pub beam_width: u32,
    pub depth: u32,
}

/// Fail-closed policy and tree-shape errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HierarchyValidationError {
    #[error("{0} must not be empty")]
    EmptyField(&'static str),
    #[error("{0} must be greater than zero")]
    ZeroLimit(&'static str),
    #[error("duplicate identifier '{0}'")]
    DuplicateIdentifier(String),
    #[error("community policy cannot carry document-section nodes")]
    CommunityHasSectionNodes,
    #[error("document-section policy requires at least one section node")]
    MissingSectionNodes,
    #[error("unknown node '{0}'")]
    UnknownNode(String),
    #[error("unknown parent-link endpoint '{0}'")]
    UnknownParentLinkEndpoint(String),
    #[error("the document-section hierarchy must contain exactly one root")]
    InvalidRootCount,
    #[error("node '{0}' is its own parent")]
    SelfParent(String),
    #[error("node '{0}' is listed more than once by its parent")]
    DuplicateChild(String),
    #[error("parent '{parent_id}' and child '{child_id}' disagree about their link")]
    ParentChildMismatch { parent_id: String, child_id: String },
    #[error("policy parent_links do not match section-node parent_id fields")]
    ParentLinkMismatch,
    #[error("cycle detected through node '{0}'")]
    Cycle(String),
    #[error("document-section nodes do not share document identity: '{0}'")]
    DocumentIdentityMismatch(String),
    #[error("source span for node '{0}' has start after end")]
    InvalidSpan(String),
    #[error("child '{child_id}' is outside parent '{parent_id}' source span")]
    SpanNotNested { parent_id: String, child_id: String },
    #[error("children of parent '{parent_id}' are not in source-span order at '{child_id}'")]
    ChildOrderMismatch { parent_id: String, child_id: String },
    #[error("node '{0}' has an invalid granularity for its position")]
    InvalidGranularity(String),
    #[error("receipt selected_id is not among visited_ids")]
    SelectedNodeNotVisited,
    #[error("receipt kind and stage do not agree")]
    ReceiptKindStageMismatch,
}

impl HierarchyRetrievalPolicy {
    /// Validate identity, limits, and the complete section tree before any
    /// traversal or serving code consumes this policy.
    pub fn validate(&self) -> Result<(), HierarchyValidationError> {
        validate_text("hierarchy_revision", &self.hierarchy_revision)?;
        validate_limits(self)?;
        validate_unique_ids(&self.community_ids)?;
        validate_unique_ids(&self.summary_ids)?;
        validate_unique_ids(&self.entity_ids)?;
        validate_score_provenance(&self.score_provenance)?;
        if (self.kind == HierarchyRetrievalKind::Community)
            != (self.stage == HierarchyStage::Community)
        {
            return Err(HierarchyValidationError::ReceiptKindStageMismatch);
        }

        match self.kind {
            HierarchyRetrievalKind::Community => {
                if !self.section_nodes.is_empty() {
                    return Err(HierarchyValidationError::CommunityHasSectionNodes);
                }
                validate_links_against_ids(&self.parent_links, &self.community_ids)
            }
            HierarchyRetrievalKind::DocumentSection => self.validate_section_tree(),
        }
    }

    fn validate_section_tree(&self) -> Result<(), HierarchyValidationError> {
        if self.section_nodes.is_empty() {
            return Err(HierarchyValidationError::MissingSectionNodes);
        }
        let nodes = collect_nodes(&self.section_nodes)?;
        validate_document_identity(&nodes)?;
        validate_roots(&nodes)?;
        validate_node_edges(&nodes)?;
        validate_parent_links_match_nodes(&self.parent_links, &nodes)?;
        validate_parent_cycles(&nodes)
    }
}

impl HierarchyStageReceipt {
    /// Validate the receipt's local bounds and stage identity.
    pub fn validate(&self) -> Result<(), HierarchyValidationError> {
        validate_text("hierarchy_revision", &self.hierarchy_revision)?;
        if self.beam_width == 0 {
            return Err(HierarchyValidationError::ZeroLimit("beam_width"));
        }
        validate_unique_ids(&self.visited_ids)?;
        validate_unique_ids(&self.pruned_ids)?;
        validate_score_provenance(&self.score_provenance)?;
        if self
            .selected_id
            .as_ref()
            .is_some_and(|selected| !self.visited_ids.iter().any(|id| id == selected))
        {
            return Err(HierarchyValidationError::SelectedNodeNotVisited);
        }
        if (self.kind == HierarchyRetrievalKind::Community)
            != (self.stage == HierarchyStage::Community)
        {
            return Err(HierarchyValidationError::ReceiptKindStageMismatch);
        }
        Ok(())
    }
}

fn validate_text(name: &'static str, value: &str) -> Result<(), HierarchyValidationError> {
    if value.trim().is_empty() {
        Err(HierarchyValidationError::EmptyField(name))
    } else {
        Ok(())
    }
}

fn validate_limits(policy: &HierarchyRetrievalPolicy) -> Result<(), HierarchyValidationError> {
    let limits = [
        ("beam_width", policy.beam_width as u64),
        ("max_depth", policy.max_depth as u64),
        (
            "max_visited_communities",
            policy.max_visited_communities as u64,
        ),
        ("max_entity_candidates", policy.max_entity_candidates as u64),
        ("max_reranker_calls", policy.max_reranker_calls as u64),
        ("max_tokens", policy.max_tokens as u64),
        ("max_total_cost_micros", policy.max_total_cost_micros),
    ];
    limits
        .into_iter()
        .find(|(_, value)| *value == 0)
        .map_or(Ok(()), |(name, _)| {
            Err(HierarchyValidationError::ZeroLimit(name))
        })
}

fn validate_unique_ids(ids: &[String]) -> Result<(), HierarchyValidationError> {
    let mut seen = BTreeSet::new();
    for id in ids {
        validate_text("identifier", id)?;
        if !seen.insert(id) {
            return Err(HierarchyValidationError::DuplicateIdentifier(id.clone()));
        }
    }
    Ok(())
}

fn validate_score_provenance(
    provenance: &[HierarchyScoreProvenance],
) -> Result<(), HierarchyValidationError> {
    for entry in provenance {
        validate_text("score_provenance.scorer", &entry.scorer)?;
        validate_text("score_provenance.scorer_revision", &entry.scorer_revision)?;
        validate_text("score_provenance.basis", &entry.basis)?;
    }
    Ok(())
}

fn validate_links_against_ids(
    links: &[HierarchyParentLink],
    allowed_ids: &[String],
) -> Result<(), HierarchyValidationError> {
    let allowed = allowed_ids.iter().collect::<BTreeSet<_>>();
    let mut children = BTreeSet::new();
    for link in links {
        if !allowed.contains(&link.parent_id) {
            return Err(HierarchyValidationError::UnknownParentLinkEndpoint(
                link.parent_id.clone(),
            ));
        }
        if !allowed.contains(&link.child_id) {
            return Err(HierarchyValidationError::UnknownParentLinkEndpoint(
                link.child_id.clone(),
            ));
        }
        if link.parent_id == link.child_id {
            return Err(HierarchyValidationError::SelfParent(link.child_id.clone()));
        }
        if !children.insert(&link.child_id) {
            return Err(HierarchyValidationError::DuplicateChild(
                link.child_id.clone(),
            ));
        }
    }
    Ok(())
}

fn collect_nodes(
    section_nodes: &[HierarchySectionNode],
) -> Result<BTreeMap<String, &HierarchySectionNode>, HierarchyValidationError> {
    let mut nodes = BTreeMap::new();
    for node in section_nodes {
        validate_text("section.id", &node.id)?;
        validate_text("section.document_id", &node.document_id)?;
        validate_text("section.document_revision", &node.document_revision)?;
        validate_text("section.summary", &node.summary)?;
        if node.source_span.start > node.source_span.end {
            return Err(HierarchyValidationError::InvalidSpan(node.id.clone()));
        }
        if nodes.insert(node.id.clone(), node).is_some() {
            return Err(HierarchyValidationError::DuplicateIdentifier(
                node.id.clone(),
            ));
        }
    }
    Ok(nodes)
}

fn validate_document_identity(
    nodes: &BTreeMap<String, &HierarchySectionNode>,
) -> Result<(), HierarchyValidationError> {
    let first = nodes.values().next().expect("section_nodes is non-empty");
    for node in nodes.values() {
        if node.document_id != first.document_id
            || node.document_revision != first.document_revision
        {
            return Err(HierarchyValidationError::DocumentIdentityMismatch(
                node.id.clone(),
            ));
        }
    }
    Ok(())
}

fn validate_roots(
    nodes: &BTreeMap<String, &HierarchySectionNode>,
) -> Result<(), HierarchyValidationError> {
    let roots = nodes
        .values()
        .filter(|node| node.parent_id.is_none())
        .count();
    if roots == 1 {
        Ok(())
    } else {
        Err(HierarchyValidationError::InvalidRootCount)
    }
}

fn validate_node_edges(
    nodes: &BTreeMap<String, &HierarchySectionNode>,
) -> Result<(), HierarchyValidationError> {
    for node in nodes.values() {
        validate_node_granularity(node)?;
        let mut seen_children = BTreeSet::new();
        let mut previous_span = None;
        for child_id in &node.ordered_children {
            if !seen_children.insert(child_id) {
                return Err(HierarchyValidationError::DuplicateChild(child_id.clone()));
            }
            let child = nodes
                .get(child_id)
                .ok_or_else(|| HierarchyValidationError::UnknownNode(child_id.clone()))?;
            if child.parent_id.as_deref() != Some(node.id.as_str()) {
                return Err(HierarchyValidationError::ParentChildMismatch {
                    parent_id: node.id.clone(),
                    child_id: child.id.clone(),
                });
            }
            if let Some(previous) = previous_span {
                if (child.source_span.start, child.source_span.end) < previous {
                    return Err(HierarchyValidationError::ChildOrderMismatch {
                        parent_id: node.id.clone(),
                        child_id: child.id.clone(),
                    });
                }
            }
            previous_span = Some((child.source_span.start, child.source_span.end));
            if child.source_span.start < node.source_span.start
                || child.source_span.end > node.source_span.end
            {
                return Err(HierarchyValidationError::SpanNotNested {
                    parent_id: node.id.clone(),
                    child_id: child.id.clone(),
                });
            }
        }
        if let Some(parent_id) = &node.parent_id {
            let parent = nodes
                .get(parent_id)
                .ok_or_else(|| HierarchyValidationError::UnknownNode(parent_id.clone()))?;
            if !parent.ordered_children.iter().any(|id| id == &node.id) {
                return Err(HierarchyValidationError::ParentChildMismatch {
                    parent_id: parent.id.clone(),
                    child_id: node.id.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_node_granularity(node: &HierarchySectionNode) -> Result<(), HierarchyValidationError> {
    if node.parent_id.is_none() && node.granularity != SectionGranularity::Document {
        return Err(HierarchyValidationError::InvalidGranularity(
            node.id.clone(),
        ));
    }
    if node.parent_id.is_some() && node.granularity == SectionGranularity::Document {
        return Err(HierarchyValidationError::InvalidGranularity(
            node.id.clone(),
        ));
    }
    if node.granularity == SectionGranularity::SourceSpan && !node.ordered_children.is_empty() {
        return Err(HierarchyValidationError::InvalidGranularity(
            node.id.clone(),
        ));
    }
    Ok(())
}

fn validate_parent_links_match_nodes(
    links: &[HierarchyParentLink],
    nodes: &BTreeMap<String, &HierarchySectionNode>,
) -> Result<(), HierarchyValidationError> {
    let expected = nodes
        .values()
        .filter_map(|node| {
            node.parent_id
                .as_ref()
                .map(|parent_id| (parent_id.clone(), node.id.clone()))
        })
        .collect::<BTreeSet<_>>();
    let actual = links
        .iter()
        .map(|link| (link.parent_id.clone(), link.child_id.clone()))
        .collect::<BTreeSet<_>>();
    if expected == actual {
        Ok(())
    } else {
        Err(HierarchyValidationError::ParentLinkMismatch)
    }
}

fn validate_parent_cycles(
    nodes: &BTreeMap<String, &HierarchySectionNode>,
) -> Result<(), HierarchyValidationError> {
    for node in nodes.values() {
        let mut visited = BTreeSet::new();
        let mut current = Some(node.id.as_str());
        while let Some(id) = current {
            if !visited.insert(id) {
                return Err(HierarchyValidationError::Cycle(id.to_string()));
            }
            current = nodes
                .get(id)
                .and_then(|candidate| candidate.parent_id.as_deref());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> SelectionPolicy {
        SelectionPolicy {
            policy_revision: "p1".into(),
            corpus_revision: "c1".into(),
            embedding_model_revision: "emb@1".into(),
            cross_encoder_model_revision: "ce@1".into(),
            normalization: ScoreNormalization::Identity,
            embedding_threshold: 0.7,
            relevance_threshold: 0.6,
            fusion_weight: 0.3,
            fusion_threshold: 0.4,
            max_chunks: 4,
            activation_benchmark_digest: Some("sha256:bench".into()),
        }
    }

    fn pair(id: &str, e: f64, r: f64) -> ChunkScorePair {
        ChunkScorePair {
            chunk_id: id.into(),
            corpus_revision: "c1".into(),
            embedding_score: e,
            cross_encoder_score: r,
            embedding_model_revision: "emb@1".into(),
            cross_encoder_model_revision: "ce@1".into(),
        }
    }

    // spec: MEDULLA-FEDERATED-QUERY-R081.1
    #[test]
    fn verdict_round_trips_and_rejects_unknown() {
        for (verdict, repair, fallback) in [
            (
                Verdict::Insufficient,
                RepairStep::BoostK { k: 16 },
                FallbackPolicy::Lenient,
            ),
            (
                Verdict::Insufficient,
                RepairStep::SwitchMode {
                    mode: "text".into(),
                },
                FallbackPolicy::Strict,
            ),
            (
                Verdict::Sufficient,
                RepairStep::Stop,
                FallbackPolicy::Strict,
            ),
        ] {
            let v = SufficiencyVerdict {
                verdict,
                evidence_count: 2,
                reason: "min_evidence_count".into(),
                repair,
                fallback,
                round: 1,
                round_limit: 3,
            };
            let json = serde_json::to_string(&v).unwrap();
            assert_eq!(
                serde_json::from_str::<SufficiencyVerdict>(&json).unwrap(),
                v
            );
        }
        let bad = r#"{"verdict":"maybe","evidence_count":0,"reason":"","repair":{"kind":"stop"},"fallback":"strict","round":1,"round_limit":1}"#;
        assert!(serde_json::from_str::<SufficiencyVerdict>(bad).is_err());
        let extra = r#"{"verdict":"sufficient","evidence_count":0,"reason":"","repair":{"kind":"stop"},"fallback":"strict","round":1,"round_limit":1,"x":1}"#;
        assert!(serde_json::from_str::<SufficiencyVerdict>(extra).is_err());
        let bad_repair = r#"{"verdict":"sufficient","evidence_count":0,"reason":"","repair":{"kind":"retry"},"fallback":"strict","round":1,"round_limit":1}"#;
        assert!(serde_json::from_str::<SufficiencyVerdict>(bad_repair).is_err());
    }

    // spec: MEDULLA-FEDERATED-QUERY-R081.7.1
    #[test]
    fn selection_contract_round_trips_and_refuses_invalid_inputs() {
        let p = policy();
        p.validate().unwrap();
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(serde_json::from_str::<SelectionPolicy>(&json).unwrap(), p);
        let receipt = SelectionReceipt {
            policy_revision: "p1".into(),
            corpus_revision: "c1".into(),
            normalization: ScoreNormalization::MinMax {
                constant_value: 0.5,
            },
            candidates: vec![ChunkSelection {
                chunk_id: "a".into(),
                embedding_score: 0.9,
                cross_encoder_score: 0.9,
                normalized_embedding: 0.9,
                normalized_relevance: 0.9,
                region: ScoreRegion::DualHigh,
                fused_score: 0.9,
                selected: true,
            }],
            selected: vec!["a".into()],
            max_chunks: 4,
            activation_benchmark_digest: Some("sha256:bench".into()),
            selection_inference_calls: 0,
        };
        let json = serde_json::to_string(&receipt).unwrap();
        assert_eq!(
            serde_json::from_str::<SelectionReceipt>(&json).unwrap(),
            receipt
        );

        let nan = p.check_pairs(&[pair("a", f64::NAN, 0.5)]).unwrap_err();
        assert_eq!(nan.code, RETRIEVAL_SCORE_PAIR_REQUIRED);
        let mut stale = pair("a", 0.5, 0.5);
        stale.cross_encoder_model_revision = String::new();
        assert_eq!(
            p.check_pairs(&[stale]).unwrap_err().code,
            RETRIEVAL_SCORE_PAIR_REQUIRED
        );
        let dup = p
            .check_pairs(&[pair("a", 0.5, 0.5), pair("a", 0.4, 0.4)])
            .unwrap_err();
        assert_eq!(dup.code, RETRIEVAL_CONTRACT_INVALID);
        let mut no_model = policy();
        no_model.embedding_model_revision = String::new();
        assert!(no_model.validate().is_err());
        let mut zero = policy();
        zero.max_chunks = 0;
        assert!(zero.validate().is_err());
        let mut over = policy();
        over.fusion_weight = 1.5;
        assert!(over.validate().is_err());
        let mut bad_norm = policy();
        bad_norm.normalization = ScoreNormalization::MinMax {
            constant_value: f64::INFINITY,
        };
        assert_eq!(
            bad_norm.validate().unwrap_err().code,
            RETRIEVAL_NORMALIZATION_INVALID
        );
    }

    fn feedback() -> RetrievalFeedback {
        RetrievalFeedback {
            original_query: "solar storage".into(),
            round: 1,
            corpus_revision: "c1".into(),
            model_revision: "m1".into(),
            perspectives: vec![PerspectiveQuery {
                perspective: "economic".into(),
                text: "solar storage cost".into(),
            }],
            documents: vec![DocumentRelevance {
                document_id: "d1".into(),
                relevance: 0.8,
            }],
            terms: vec![
                TermFeedback {
                    term: "battery".into(),
                    tier: FeedbackTier::Reinforce,
                    weight: 1.2,
                    evidence_document_ids: vec!["d1".into()],
                },
                TermFeedback {
                    term: "grid".into(),
                    tier: FeedbackTier::Missing,
                    weight: 0.0,
                    evidence_document_ids: vec![],
                },
                TermFeedback {
                    term: "panel".into(),
                    tier: FeedbackTier::Suppress,
                    weight: 0.5,
                    evidence_document_ids: vec!["d1".into()],
                },
            ],
            critique: Some(RetrievalCritique {
                text: "consider grid storage".into(),
                model_revision: "m1".into(),
                proposed_reformulation: Some("grid-scale solar storage".into()),
            }),
            limits: FeedbackLimits {
                max_rounds: 3,
                max_queries: 4,
                max_terms: 8,
                max_tokens: 4096,
                max_inference_calls: 6,
            },
        }
    }

    // R081.8.1's termination fields come from the R082 stop model; this test
    // covers the feedback half only.
    #[test]
    fn feedback_round_trips_tiers_and_refuses_unbounded_or_forged() {
        let f = feedback();
        f.validate("c1", "m1").unwrap();
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(serde_json::from_str::<RetrievalFeedback>(&json).unwrap(), f);
        assert!(serde_json::from_str::<FeedbackTier>("\"boost\"").is_err());

        assert_eq!(
            f.validate("c2", "m1").unwrap_err().code,
            RETRIEVAL_FEEDBACK_INVALID
        );
        let mut forged = feedback();
        forged.terms[0].evidence_document_ids = vec!["d9".into()];
        assert_eq!(
            forged.validate("c1", "m1").unwrap_err().code,
            RETRIEVAL_FEEDBACK_INVALID
        );
        let mut missing = feedback();
        missing.terms[0].evidence_document_ids.clear();
        assert_eq!(
            missing.validate("c1", "m1").unwrap_err().code,
            RETRIEVAL_CONTRACT_INVALID
        );
        let mut unbounded = feedback();
        unbounded.limits.max_tokens = 0;
        assert_eq!(
            unbounded.validate("c1", "m1").unwrap_err().code,
            RETRIEVAL_CONTRACT_INVALID
        );
        let mut late = feedback();
        late.round = 4;
        assert!(late.validate("c1", "m1").is_err());
    }

    fn community(id: &str, parent: Option<&str>) -> HierarchyCommunity {
        HierarchyCommunity {
            community_id: id.into(),
            parent_id: parent.map(str::to_string),
            summary_id: format!("s-{id}"),
            summary_revision: "h1".into(),
            tenant: "t1".into(),
            entity_ids: vec![],
        }
    }

    // spec: MEDULLA-FEDERATED-QUERY-R081.9.1
    #[test]
    fn hierarchy_policy_refuses_absent_revision_bad_links_and_zero_limits() {
        let policy = DescentPolicy {
            hierarchy_revision: "h1".into(),
            policy_revision: "p1".into(),
            beam_width: 2,
            max_depth: 3,
            max_visited_communities: 10,
            max_entity_candidates: 20,
            max_reranker_calls: 5,
            max_tokens: 1000,
            max_cost: 100,
        };
        policy.validate().unwrap();
        let json = serde_json::to_string(&policy).unwrap();
        assert_eq!(
            serde_json::from_str::<DescentPolicy>(&json).unwrap(),
            policy
        );
        let mut absent = policy.clone();
        absent.hierarchy_revision = " ".into();
        assert!(absent.validate().is_err());
        let mut zero = policy.clone();
        zero.max_reranker_calls = 0;
        assert!(zero.validate().is_err());
        assert!(serde_json::from_str::<DescentPolicy>(
            r#"{"hierarchy_revision":"h1","policy_revision":"p1","beam_width":2}"#
        )
        .is_err());

        validate_hierarchy(
            &[community("root", None), community("a", Some("root"))],
            "h1",
        )
        .unwrap();
        let dangling = validate_hierarchy(&[community("a", Some("ghost"))], "h1").unwrap_err();
        assert_eq!(dangling.code, RETRIEVAL_HIERARCHY_INVALID);
        let cycle = validate_hierarchy(
            &[community("a", Some("b")), community("b", Some("a"))],
            "h1",
        )
        .unwrap_err();
        assert_eq!(cycle.code, RETRIEVAL_HIERARCHY_INVALID);
        let stale = validate_hierarchy(&[community("a", None)], "h2").unwrap_err();
        assert_eq!(stale.code, RETRIEVAL_HIERARCHY_INVALID);
    }
}

#[cfg(test)]
mod section_hierarchy_tests {
    use super::*;

    fn policy(kind: HierarchyRetrievalKind) -> HierarchyRetrievalPolicy {
        HierarchyRetrievalPolicy {
            kind,
            hierarchy_revision: "hierarchy-r1".to_string(),
            community_ids: vec!["community-1".to_string()],
            summary_ids: vec!["summary-1".to_string()],
            entity_ids: vec!["entity-1".to_string()],
            parent_links: Vec::new(),
            stage: match kind {
                HierarchyRetrievalKind::Community => HierarchyStage::Community,
                HierarchyRetrievalKind::DocumentSection => HierarchyStage::DocumentSection,
            },
            score_provenance: vec![HierarchyScoreProvenance {
                scorer: "declared-score".to_string(),
                scorer_revision: "score-r1".to_string(),
                basis: "stored-summary".to_string(),
            }],
            beam_width: 4,
            max_depth: 8,
            max_visited_communities: 16,
            max_entity_candidates: 32,
            max_reranker_calls: 2,
            max_tokens: 512,
            max_total_cost_micros: 10_000,
            section_nodes: Vec::new(),
        }
    }

    fn node(
        id: &str,
        parent_id: Option<&str>,
        children: &[&str],
        granularity: SectionGranularity,
        start: u64,
        end: u64,
    ) -> HierarchySectionNode {
        HierarchySectionNode {
            id: id.to_string(),
            document_id: "document-1".to_string(),
            document_revision: "document-r1".to_string(),
            parent_id: parent_id.map(str::to_string),
            ordered_children: children.iter().map(|child| (*child).to_string()).collect(),
            granularity,
            summary: format!("summary for {id}"),
            source_span: SectionSourceSpan { start, end },
        }
    }

    #[test]
    fn document_section_policy_validates_nested_tree_and_round_trips() {
        let mut document = policy(HierarchyRetrievalKind::DocumentSection);
        document.section_nodes = vec![
            node(
                "document-1",
                None,
                &["section-1"],
                SectionGranularity::Document,
                0,
                100,
            ),
            node(
                "section-1",
                Some("document-1"),
                &["span-1"],
                SectionGranularity::Section,
                10,
                80,
            ),
            node(
                "span-1",
                Some("section-1"),
                &[],
                SectionGranularity::SourceSpan,
                20,
                40,
            ),
        ];
        document.parent_links = vec![
            HierarchyParentLink {
                parent_id: "document-1".to_string(),
                child_id: "section-1".to_string(),
            },
            HierarchyParentLink {
                parent_id: "section-1".to_string(),
                child_id: "span-1".to_string(),
            },
        ];

        document.validate().expect("nested section tree is valid");
        let encoded = serde_json::to_string(&document).expect("policy serializes");
        let decoded: HierarchyRetrievalPolicy =
            serde_json::from_str(&encoded).expect("policy deserializes");
        assert_eq!(decoded, document);
    }

    #[test]
    fn community_policy_preserves_community_fields_without_section_nodes() {
        let community = policy(HierarchyRetrievalKind::Community);
        community.validate().expect("community policy is valid");
        assert_eq!(community.community_ids, vec!["community-1".to_string()]);
        assert_eq!(community.summary_ids, vec!["summary-1".to_string()]);
        assert_eq!(community.entity_ids, vec!["entity-1".to_string()]);
    }

    #[test]
    fn malformed_tree_refuses_multiple_roots_and_outside_spans() {
        let mut multiple_roots = policy(HierarchyRetrievalKind::DocumentSection);
        multiple_roots.section_nodes = vec![
            node("document-1", None, &[], SectionGranularity::Document, 0, 10),
            node(
                "document-2",
                None,
                &[],
                SectionGranularity::Document,
                20,
                30,
            ),
        ];
        assert_eq!(
            multiple_roots.validate(),
            Err(HierarchyValidationError::InvalidRootCount)
        );

        let mut outside_span = policy(HierarchyRetrievalKind::DocumentSection);
        outside_span.section_nodes = vec![
            node(
                "document-1",
                None,
                &["section-1"],
                SectionGranularity::Document,
                0,
                10,
            ),
            node(
                "section-1",
                Some("document-1"),
                &[],
                SectionGranularity::Section,
                5,
                20,
            ),
        ];
        outside_span.parent_links = vec![HierarchyParentLink {
            parent_id: "document-1".to_string(),
            child_id: "section-1".to_string(),
        }];
        assert!(matches!(
            outside_span.validate(),
            Err(HierarchyValidationError::SpanNotNested { .. })
        ));

        let mut cyclic = policy(HierarchyRetrievalKind::DocumentSection);
        cyclic.section_nodes = vec![
            node(
                "document-1",
                None,
                &[],
                SectionGranularity::Document,
                0,
                100,
            ),
            node(
                "cycle-1",
                Some("cycle-2"),
                &["cycle-2"],
                SectionGranularity::Section,
                20,
                50,
            ),
            node(
                "cycle-2",
                Some("cycle-1"),
                &["cycle-1"],
                SectionGranularity::Section,
                20,
                50,
            ),
        ];
        cyclic.parent_links = vec![
            HierarchyParentLink {
                parent_id: "cycle-2".to_string(),
                child_id: "cycle-1".to_string(),
            },
            HierarchyParentLink {
                parent_id: "cycle-1".to_string(),
                child_id: "cycle-2".to_string(),
            },
        ];
        assert!(matches!(
            cyclic.validate(),
            Err(HierarchyValidationError::Cycle(_))
        ));
    }

    #[test]
    fn receipt_rejects_unvisited_selection_and_kind_stage_mismatch() {
        let receipt = HierarchyStageReceipt {
            hierarchy_revision: "hierarchy-r1".to_string(),
            kind: HierarchyRetrievalKind::DocumentSection,
            stage: HierarchyStage::Community,
            visited_ids: vec!["section-1".to_string()],
            selected_id: Some("section-2".to_string()),
            pruned_ids: Vec::new(),
            score_provenance: Vec::new(),
            beam_width: 1,
            depth: 1,
        };
        assert_eq!(
            receipt.validate(),
            Err(HierarchyValidationError::SelectedNodeNotVisited)
        );

        let mut matching = receipt;
        matching.selected_id = None;
        assert_eq!(
            matching.validate(),
            Err(HierarchyValidationError::ReceiptKindStageMismatch)
        );
    }
}
