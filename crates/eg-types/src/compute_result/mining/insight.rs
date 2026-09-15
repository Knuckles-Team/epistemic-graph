//! Results of the graph-insight miners: entity resolution, causal impact, process
//! mining, root cause, risk propagation, ontology gaps, retrieval quality and
//! communities.

use serde::{Deserialize, Serialize};

/// One matched record pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EntityMatchRow {
    pub left: String,
    pub right: String,
    pub similarity: f64,
    pub block_key: String,
}

/// `MineEntityResolve`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EntityResolutionMiningResult {
    pub matches: Vec<EntityMatchRow>,
    pub n_records: usize,
    pub n_matches: usize,
    pub written_back: usize,
}

/// `MineCausalImpact`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CausalImpactMiningResult {
    pub pre_mean: f64,
    pub post_mean: f64,
    pub effect_size: f64,
    pub relative_effect: f64,
    pub std_error: f64,
    pub confidence: f64,
    /// `its` (interrupted time series) or `did` (difference in differences).
    pub method: String,
    pub written_back: usize,
}

/// A directly-follows edge between two activities.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DirectlyFollowsRow {
    pub from: String,
    pub to: String,
    pub count: usize,
}

/// A causal (`from` → `to`) activity relation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CausalRelationRow {
    pub from: String,
    pub to: String,
}

/// A parallel activity relation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ParallelRelationRow {
    pub a: String,
    pub b: String,
}

/// `MineProcess`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProcessMiningResult {
    pub dfg: Vec<DirectlyFollowsRow>,
    pub causal: Vec<CausalRelationRow>,
    pub parallel: Vec<ParallelRelationRow>,
    pub start_activities: Vec<String>,
    pub end_activities: Vec<String>,
    pub n_traces: usize,
    pub n_activities: usize,
    pub written_back: usize,
}

/// One root-cause candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RootCauseCandidateRow {
    pub node: String,
    pub score: f64,
    pub hops: usize,
}

/// `MineRootCause`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RootCauseMiningResult {
    pub symptom: String,
    pub candidates: Vec<RootCauseCandidateRow>,
    /// The top candidate; `null` when there is none.
    pub best: Option<String>,
    pub written_back: usize,
}

/// One node's propagated risk.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RiskScoreRow {
    pub node: String,
    pub score: f64,
}

/// `MineRiskPropagation`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RiskPropagationMiningResult {
    pub scores: Vec<RiskScoreRow>,
    pub iterations: usize,
    pub converged: bool,
    pub written_back: usize,
}

/// One ontology gap.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OntologyGapRow {
    pub class: String,
    pub kind: String,
    pub severity: f64,
}

/// `MineOntologyGap`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OntologyGapMiningResult {
    pub gaps: Vec<OntologyGapRow>,
    pub n_classes: usize,
    pub n_gaps: usize,
    pub written_back: usize,
}

/// `MineRetrievalQuality`: aggregate retrieval metrics over the traces.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RetrievalQualityMiningResult {
    pub precision_at_k: f64,
    pub recall_at_k: f64,
    pub mrr: f64,
    pub f1: f64,
    pub ndcg_at_k: f64,
    pub n_queries: usize,
    pub k: usize,
    pub written_back: usize,
}

/// One detected community.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CommunityRow {
    pub members: Vec<String>,
    pub density: f64,
}

/// `MineCommunity`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CommunityMiningResult {
    pub communities: Vec<CommunityRow>,
    /// `null` for an algorithm that does not optimize modularity.
    pub modularity: Option<f64>,
    pub n_nodes: usize,
    pub written_back: usize,
    /// `true` when the budget expired before convergence: the partition is the
    /// best found so far, and a writeback has already persisted it.
    pub deadline_hit: bool,
}
