use super::*;

// ── Supporting Types ────────────────────────────────────────────────────

/// Which frequent-itemset engine `MineAssociate` runs (CONCEPT:EG-KG.mining.frequent-itemset-mining).
/// All three are exact and agree on the frequent-itemset set for a given support;
/// they differ only in traversal strategy. FP-Growth is the default (no candidate
/// generation → fastest on dense baskets).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MineAlgorithm {
    #[default]
    Fpgrowth,
    Apriori,
    Eclat,
}

/// Which sequential-pattern engine `MineSequence` runs (CONCEPT:EG-KG.mining.prefixspan
/// — Phase 4). Both are exact and agree on the frequent-pattern set for a given
/// support. PrefixSpan is the default (projection-based, no candidate generation).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MineSeqAlgorithm {
    #[default]
    Prefixspan,
    Gsp,
}

/// Which forecasting engine `MineForecast` runs (CONCEPT:EG-KG.mining.arima —
/// Phase 4). ARIMA is the default (Hannan-Rissanen AR(p)/MA(q) after `d`-order
/// differencing); `holtwinters` degrades to Holt's linear-trend method when
/// `period` is 0; `stl` is a classical decomposition + extrapolation.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ForecastAlgorithm {
    #[default]
    Arima,
    Holtwinters,
    Stl,
}

/// Which text-mining engine `MineText` runs (CONCEPT:EG-KG.mining.tfidf — Phase
/// 4). TF-IDF is the default (descriptive per-document term weights); `lda`/
/// `nmf` fit a `k`-topic model.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TextAlgorithm {
    #[default]
    Tfidf,
    Lda,
    Nmf,
}

/// Which algorithm `MineSubgraph` runs (CONCEPT:EG-KG.mining.gspan-frequent-subgraph
/// — Phase 4). `gspan` is the default (labeled frequent-subgraph patterns);
/// `motif` is a label-agnostic topological census.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SubgraphAlgorithm {
    #[default]
    Gspan,
    Motif,
}

/// serde default for [`Method::MineSubgraph::max_edges`].
#[cfg(feature = "mining")]
pub(super) fn default_max_subgraph_edges() -> usize {
    3
}

// ── Residual insight/mining families — supporting types + defaults ─────────

/// One stored retrieval trace for `MineRetrievalQuality` (CONCEPT:EG-KG.mining.retrieval-quality):
/// what a query actually retrieved (ranked) vs. what was actually relevant.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RetrievalTraceSpec {
    /// Ranked ids the retrieval actually returned.
    pub retrieved: Vec<String>,
    /// Ground-truth relevant ids for this query.
    pub relevant: Vec<String>,
}

/// Which existing GDS kernel `MineCommunity` wraps (CONCEPT:EG-KG.mining.community-writeback).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CommunityAlgorithm {
    #[default]
    Louvain,
    #[serde(rename = "labelprop")]
    LabelPropagation,
}

/// serde default for [`Method::MineEntityResolve::bucket_precision`].
#[cfg(feature = "mining")]
pub(super) fn default_bucket_precision() -> i32 {
    1
}

/// serde default for [`Method::MineEntityResolve::threshold`].
#[cfg(feature = "mining")]
pub(super) fn default_match_threshold() -> f64 {
    0.5
}

/// serde default for [`Method::MineRootCause::max_hops`].
#[cfg(feature = "mining")]
pub(super) fn default_max_hops() -> usize {
    5
}

/// serde default for [`Method::MineRootCause::decay`].
#[cfg(feature = "mining")]
pub(super) fn default_decay() -> f64 {
    0.85
}

/// serde default for [`Method::MineRiskPropagation::damping`].
#[cfg(feature = "mining")]
pub(super) fn default_damping() -> f64 {
    0.85
}

/// serde default for [`Method::MineRiskPropagation::tolerance`].
#[cfg(feature = "mining")]
pub(super) fn default_risk_tolerance() -> f64 {
    1e-9
}

/// serde default for [`Method::MineCommunity::resolution`].
#[cfg(feature = "mining")]
pub(super) fn default_resolution() -> f64 {
    1.0
}

/// serde default for [`Method::MineCommunity::weighted`].
#[cfg(any(feature = "mining", feature = "ml-pipeline"))]
pub(super) fn default_true() -> bool {
    true
}

/// A graph-derived text source for `MineText` (CONCEPT:EG-KG.mining.tfidf —
/// Phase 4). Each node carrying `node_label` contributes one document: its
/// `field` string property, tokenized (lowercase, alnum-run split).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TextSource {
    /// The node label whose instances each contribute one document.
    pub node_label: String,
    /// The string property to tokenize into the document.
    pub field: String,
    /// Cap the number of nodes scanned (0 = uncapped).
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub limit: usize,
}

/// serde default topic count for `lda`/`nmf`.
#[cfg(feature = "mining")]
pub(super) fn default_topic_k() -> usize {
    3
}

/// serde default LDA symmetric doc-topic prior.
#[cfg(feature = "mining")]
pub(super) fn default_lda_alpha() -> f64 {
    0.1
}

/// serde default LDA symmetric topic-term prior.
#[cfg(feature = "mining")]
pub(super) fn default_lda_beta() -> f64 {
    0.01
}

/// serde default Gibbs sweeps / NMF iterations.
#[cfg(feature = "mining")]
pub(super) fn default_text_iterations() -> usize {
    200
}

/// serde default terms kept per document/topic row.
#[cfg(feature = "mining")]
pub(super) fn default_top_n() -> usize {
    10
}

/// serde default for [`Method::MineForecast::horizon`].
#[cfg(feature = "mining")]
pub(super) fn default_horizon() -> usize {
    10
}

/// serde default ARIMA autoregressive order.
#[cfg(feature = "mining")]
pub(super) fn default_arima_p() -> usize {
    1
}

/// serde default ARIMA differencing order.
#[cfg(feature = "mining")]
pub(super) fn default_arima_d() -> usize {
    1
}

/// serde default Holt-Winters level smoothing.
#[cfg(feature = "mining")]
pub(super) fn default_hw_alpha() -> f64 {
    0.3
}

/// serde default Holt-Winters trend smoothing.
#[cfg(feature = "mining")]
pub(super) fn default_hw_beta() -> f64 {
    0.1
}

/// serde default Holt-Winters seasonal smoothing.
#[cfg(feature = "mining")]
pub(super) fn default_hw_gamma() -> f64 {
    0.1
}

/// serde default two-sided forecast confidence level.
#[cfg(feature = "mining")]
pub(super) fn default_confidence() -> f64 {
    0.95
}

/// A graph-derived transaction source for `MineAssociate` (CONCEPT:EG-KG.mining.graph-derived-transactions).
///
/// Each node carrying `node_label` becomes one "basket owner"; the basket is the set
/// of `item_field` values gathered from its neighbors (following edges in
/// `direction`), optionally filtered to a `relation`. This turns node neighborhoods
/// into transactions so mining runs directly over resident graph data — the
/// cross-modal hook (e.g. "for each :Capability, the set of concepts it touches" =
/// one transaction ⇒ concept-co-occurrence rules).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TransactionSource {
    /// The node label whose instances each become one transaction/basket owner.
    pub node_label: String,
    /// Edge direction to gather neighbors: `out` (successors, default), `in`
    /// (predecessors), or `any` (both).
    #[serde(default = "default_mine_direction")]
    pub direction: String,
    /// Which value of each neighbor becomes an item: `label` (the neighbor's
    /// type/label, default) or `prop:<key>` (a neighbor property value). When
    /// `None`, the neighbor's node id is used verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_field: Option<String>,
    /// Optional edge-relation filter: only follow edges whose `relationship`
    /// property equals this. `None` ⇒ all edges.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relation: Option<String>,
    /// Cap the number of basket owners scanned (0 = uncapped).
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub limit: usize,
}

/// serde default for [`TransactionSource::direction`].
#[cfg(feature = "mining")]
fn default_mine_direction() -> String {
    "out".to_string()
}

/// A graph-derived sequence source for `MineSequence` (CONCEPT:EG-KG.mining.prefixspan
/// — Phase 4). Each node carrying `node_label` becomes one ordered sequence: the
/// list of `item_field` values gathered from its neighbors in `direction`,
/// preserving the RESIDENT EDGE INSERTION ORDER (the natural "ordered edge
/// sequence per node" — edges accumulate in the order they were added, so this
/// is compute-near-data over the bitemporal write history without a separate
/// tsdb/event-log dependency), optionally filtered to a `relation`.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SequenceSource {
    /// The node label whose instances each become one ordered sequence.
    pub node_label: String,
    /// Edge direction to gather neighbors: `out` (successors, default — the
    /// natural "what happened after" order), `in` (predecessors), or `any`.
    #[serde(default = "default_mine_direction")]
    pub direction: String,
    /// Which value of each neighbor becomes an item: `label` (the neighbor's
    /// type/label, default) or `prop:<key>` (a neighbor property value). When
    /// `None`, the neighbor's node id is used verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_field: Option<String>,
    /// Optional edge-relation filter: only follow edges whose `relationship`
    /// property equals this. `None` ⇒ all edges.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relation: Option<String>,
    /// Cap the number of sequence owners scanned (0 = uncapped).
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub limit: usize,
}

/// A graph-derived VECTOR source for `MineCluster` / `MineAnomaly`
/// (CONCEPT:EG-KG.mining.node-embedding-source). Each node carrying `node_label`
/// contributes ONE feature row = its stored embedding vector, and its node id is
/// carried alongside so write-back can link the mined `:Cluster` / `:Anomaly` node
/// back to it. This is the cross-modal hook — "cluster / anomaly-detect the
/// embeddings of these nodes" runs compute-near-data over resident vectors.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct VectorSource {
    /// The node label whose instances each contribute one embedding row.
    pub node_label: String,
    /// Cap the number of nodes scanned (0 = uncapped).
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub limit: usize,
}

/// Which clustering engine `MineCluster` runs (CONCEPT:EG-KG.mining.dbscan-density).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ClusterAlgorithm {
    #[default]
    Dbscan,
    Hierarchical,
    Gmm,
    Kmedoids,
}

/// Hierarchical agglomerative linkage criterion (CONCEPT:EG-KG.mining.hierarchical-linkage).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum Linkage {
    Single,
    Complete,
    #[default]
    Average,
}

/// Which detector `MineAnomaly` runs (CONCEPT:EG-KG.mining.isolation-forest).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AnomalyAlgorithm {
    #[default]
    Zscore,
    Isoforest,
    Lof,
    Ocsvm,
}

/// One-Class SVM kernel (CONCEPT:EG-KG.mining.oneclass-svm).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SvmKernel {
    #[default]
    Rbf,
    Linear,
}

/// serde default for DBSCAN `eps`.
#[cfg(feature = "mining")]
pub(super) fn default_eps() -> f64 {
    0.5
}

/// serde default for DBSCAN `min_pts`.
#[cfg(feature = "mining")]
pub(super) fn default_min_pts() -> usize {
    5
}

/// serde default cluster count `k`.
#[cfg(feature = "mining")]
pub(super) fn default_k() -> usize {
    3
}

/// serde default EM / PAM iteration cap.
#[cfg(feature = "mining")]
pub(super) fn default_max_iter() -> usize {
    100
}

/// serde default LOF neighbor count.
#[cfg(feature = "mining")]
pub(super) fn default_lof_k() -> usize {
    20
}

/// serde default Isolation Forest tree count.
#[cfg(feature = "mining")]
pub(super) fn default_n_trees() -> usize {
    100
}

/// serde default Isolation Forest subsample size.
#[cfg(feature = "mining")]
pub(super) fn default_sample_size() -> usize {
    256
}

/// serde default One-Class SVM ν.
#[cfg(feature = "mining")]
pub(super) fn default_nu() -> f64 {
    0.1
}
