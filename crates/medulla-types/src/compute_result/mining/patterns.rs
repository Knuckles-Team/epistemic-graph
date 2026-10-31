//! Results of the pattern miners: association rules, sequences, forecasts, text
//! topics and frequent subgraphs.

use serde::{Deserialize, Serialize};

/// One association rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AssociationRuleRow {
    pub antecedent: Vec<String>,
    pub consequent: Vec<String>,
    pub support: f64,
    pub confidence: f64,
    pub lift: f64,
}

/// `MineAssociate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AssociationMiningResult {
    pub rules: Vec<AssociationRuleRow>,
    pub n_transactions: usize,
    pub n_rules: usize,
    pub written_back: usize,
}

/// One frequent sequential pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SequentialPatternRow {
    pub items: Vec<String>,
    pub support: f64,
    pub count: usize,
}

/// `MineSequence`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SequenceMiningResult {
    pub patterns: Vec<SequentialPatternRow>,
    pub n_sequences: usize,
    pub n_patterns: usize,
    pub written_back: usize,
}

/// `MineForecast`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForecastMiningResult {
    pub forecast: Vec<f64>,
    pub lower: Vec<f64>,
    pub upper: Vec<f64>,
    pub algorithm: String,
    pub horizon: usize,
    pub n_obs: usize,
    pub written_back: usize,
    /// STL decomposition components; present only for the `stl` algorithm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trend: Option<Vec<f64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seasonal: Option<Vec<f64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residual: Option<Vec<f64>>,
}

/// A weighted term.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TermWeight {
    pub term: String,
    pub weight: f64,
}

/// A document's weighted terms.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DocTerms {
    pub doc_id: String,
    pub terms: Vec<TermWeight>,
}

/// A topic's weighted terms.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TopicTerms {
    pub topic_id: usize,
    pub terms: Vec<TermWeight>,
}

/// `MineText`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TextMiningResult {
    pub doc_terms: Vec<DocTerms>,
    pub topics: Vec<TopicTerms>,
    pub doc_topics: Vec<Vec<f64>>,
    /// Absent when there were no documents to mine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    pub n_docs: usize,
    pub written_back: usize,
}

/// A labeled edge of a frequent subgraph pattern, by pattern-local node index.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PatternEdge {
    pub from: usize,
    pub to: usize,
    pub label: String,
}

/// One frequent subgraph pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SubgraphPatternRow {
    /// Node labels, indexed by pattern-local node index.
    pub nodes: Vec<String>,
    pub edges: Vec<PatternEdge>,
    pub support: f64,
    pub count: usize,
}

/// gSpan frequent-subgraph patterns.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GspanMiningResult {
    pub patterns: Vec<SubgraphPatternRow>,
    pub algorithm: String,
    pub n_host_nodes: usize,
    pub n_host_edges: usize,
    pub written_back: usize,
}

/// Three-node motif counts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MotifCountsRow {
    pub wedge: usize,
    pub triangle: usize,
    pub directed_cycle3: usize,
}

/// Motif counting over the host graph; never writes back.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MotifMiningResult {
    pub motifs: MotifCountsRow,
    pub algorithm: String,
    pub n_host_nodes: usize,
    pub n_host_edges: usize,
    pub written_back: usize,
}

/// `MineSubgraph`: the body is selected by the request's algorithm.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum SubgraphMiningResult {
    Gspan(GspanMiningResult),
    Motif(MotifMiningResult),
}
