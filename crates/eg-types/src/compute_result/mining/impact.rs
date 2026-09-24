//! `MineRiskPropagation`: the mass-share model and the probabilistic impact
//! models (EH-526, `eg_compute::graph_algos::impact`).

use serde::{Deserialize, Serialize};

/// Which propagation model `MineRiskPropagation` runs.
///
/// `share` (the default) is personalised PageRank: each node's SHARE of the
/// propagated risk mass. The shares sum to one, so they are not probabilities:
/// ten independent paths into a node divide mass rather than raising the
/// node's chance of being hit. `noisy_or` and `independent_cascade` return the
/// probability that each node is hit, with the edge weights read as
/// transmission probabilities and the seed values as seed-hit probabilities.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RiskModel {
    #[default]
    Share,
    /// Noisy-OR over the seeds' downstream cone: exact on a polytree cone, an
    /// upper bound on other DAGs, unrolled for `hops` rounds on a cycle.
    NoisyOr(ImpactOptions),
    /// Seeded Monte Carlo of the independent-cascade (live-edge) model, with
    /// 95% Wilson intervals per node.
    IndependentCascade(ImpactOptions),
}

/// Options of the probabilistic impact models.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ImpactOptions {
    /// Hop bound (capped at 64).
    pub hops: u32,
    /// Monte Carlo worlds (`independent_cascade` only).
    pub samples: u32,
    /// Seed of the Monte Carlo stream; the run replays bit-identically.
    pub rng_seed: u64,
    /// How many of the strongest seed-to-node paths to report.
    pub top_paths: u32,
    /// Report per-seed Shapley attribution for the reported path targets.
    pub attribute_seeds: bool,
    /// With writeback: assess only these node ids (the recomputed cone of an
    /// event-driven run). Empty assesses every hit node.
    pub assess: Vec<String>,
    /// With writeback: the assessment scope recorded on, and keying, each
    /// `:ImpactAssessment` node (a standing impact watch's id). Empty = `adhoc`.
    pub scope: String,
    /// With writeback: the as-of time (unix ms) recorded on each assessment.
    /// Part of the request, so a WAL replay writes the same facts.
    pub as_of_ms: u64,
}

impl Default for ImpactOptions {
    fn default() -> Self {
        Self {
            hops: 8,
            samples: 2_000,
            rng_seed: 0,
            top_paths: 10,
            attribute_seeds: false,
            assess: Vec::new(),
            scope: String::new(),
            as_of_ms: 0,
        }
    }
}

/// What an impact probability means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ImpactSemantics {
    /// Noisy-OR on a polytree cone: the exact live-edge probability.
    Exact,
    /// Noisy-OR on a DAG with shared ancestry: an upper bound.
    UpperBound,
    /// Noisy-OR on a cyclic cone unrolled to the hop bound: an upper bound.
    CyclicUnroll,
    /// Independent-cascade Monte Carlo: an unbiased estimate with intervals.
    MonteCarlo,
}

/// One node's propagated risk.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RiskScoreRow {
    pub node: String,
    /// `share`: the node's mass share. Impact models: `P(node is hit)`.
    pub score: f64,
    /// Impact models: hops from the nearest seed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hops: Option<u32>,
    /// `independent_cascade`: the 95% Wilson interval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lower: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper: Option<f64>,
}

/// The most probable seed-to-node path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ImpactPathRow {
    pub node: String,
    /// Node ids, seed first.
    pub path: Vec<String>,
    pub probability: f64,
}

/// One seed's Shapley share of one node's impact probability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SeedAttributionRow {
    pub node: String,
    pub seed: String,
    pub shapley: f64,
}

/// The impact-model half of a `MineRiskPropagation` result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ImpactReport {
    pub semantics: ImpactSemantics,
    /// `sha256:` provenance digest of the model, its options and every input.
    pub digest: String,
    pub hops: u32,
    /// Expected number of hit nodes.
    pub expected_spread: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_lower: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_upper: Option<f64>,
    pub paths: Vec<ImpactPathRow>,
    pub attribution: Vec<SeedAttributionRow>,
    /// Why attribution was asked for but not produced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribution_note: Option<String>,
}

/// `MineRiskPropagation`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RiskPropagationMiningResult {
    pub scores: Vec<RiskScoreRow>,
    /// `share`: power iterations. Impact models: recurrence rounds or worlds.
    pub iterations: usize,
    pub converged: bool,
    pub written_back: usize,
    /// Present exactly for the impact models.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impact: Option<ImpactReport>,
}
