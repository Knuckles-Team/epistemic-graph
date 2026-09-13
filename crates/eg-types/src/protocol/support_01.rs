use super::*;

/// Which classifier `MineClassifyFit` fits (CONCEPT:EG-KG.mining.naive-bayes).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ClassifyAlgorithm {
    /// Gaussian Naive Bayes (default) — continuous features.
    #[default]
    Gaussiannb,
    /// Multinomial Naive Bayes — count features.
    Multinomialnb,
    /// k-nearest-neighbor majority vote.
    Knn,
    /// One-vs-rest logistic regression.
    Logistic,
    /// One-vs-rest linear SVM (SVC).
    Svc,
}

/// Which reduction engine `MineReduce` runs (CONCEPT:EG-KG.mining.truncated-svd).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ReduceAlgorithm {
    /// Truncated SVD (default) — unsupervised linear projection.
    #[default]
    Svd,
    /// Fisher LDA — supervised (needs labels).
    Lda,
    /// UMAP layout.
    Umap,
    /// t-SNE embedding.
    Tsne,
}

/// serde default k-NN neighbor count.
#[cfg(feature = "mining")]
fn default_knn_k() -> usize {
    5
}

/// serde default Multinomial NB Laplace smoothing.
#[cfg(feature = "mining")]
fn default_nb_alpha() -> f64 {
    1.0
}

/// serde default logistic / SVC learning rate.
#[cfg(feature = "mining")]
fn default_class_lr() -> f64 {
    0.1
}

/// serde default logistic / SVC epochs.
#[cfg(feature = "mining")]
fn default_class_epochs() -> usize {
    300
}

/// serde default linear-SVC inverse-regularization C.
#[cfg(feature = "mining")]
fn default_svc_c() -> f64 {
    1.0
}

/// serde default reduced dimensionality.
#[cfg(feature = "mining")]
fn default_n_components() -> usize {
    2
}

/// serde default UMAP neighbor count.
#[cfg(feature = "mining")]
fn default_umap_neighbors() -> usize {
    15
}

/// serde default UMAP minimum embedded distance.
#[cfg(feature = "mining")]
fn default_umap_min_dist() -> f64 {
    0.1
}

/// serde default t-SNE perplexity.
#[cfg(feature = "mining")]
fn default_tsne_perplexity() -> f64 {
    30.0
}

/// serde default UMAP / t-SNE epochs.
#[cfg(feature = "mining")]
fn default_reduce_epochs() -> usize {
    300
}

/// serde default t-SNE learning rate.
#[cfg(feature = "mining")]
fn default_tsne_lr() -> f64 {
    100.0
}

// ── Graph-learning wire types (CONCEPT:EG-KG.graphlearn.link-predictor) ──

/// A graph-derived subgraph source for `GraphLearn*` (CONCEPT:EG-KG.graphlearn.link-predictor).
///
/// Every node carrying `node_label` becomes a vertex; edges among them (following
/// `direction`, optionally filtered to `relation`) are the observed positive links
/// the KAN learns from. Isolated label instances are kept as candidate endpoints.
#[cfg(feature = "graphlearn")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphSource {
    /// The node label whose instances form the learning subgraph's vertices.
    pub node_label: String,
    /// Edge direction to gather links: `any` (both, default — links are undirected
    /// for prediction), `out` (successors), or `in` (predecessors).
    #[serde(default = "default_gl_direction")]
    pub direction: String,
    /// Optional edge-relation filter: only use edges whose `relationship` equals
    /// this. `None` ⇒ all edges among the label's nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relation: Option<String>,
    /// Cap the number of label nodes scanned (0 = uncapped).
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub limit: usize,
}

/// serde default for [`GraphSource::direction`].
#[cfg(feature = "graphlearn")]
fn default_gl_direction() -> String {
    "any".to_string()
}

/// Training + architecture knobs for `GraphLearnFit` (CONCEPT:EG-KG.graphlearn.link-predictor).
#[cfg(feature = "graphlearn")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphLearnParams {
    /// Polynomial basis for the edge functions: `chebyshev` (default) or `jacobi`.
    #[serde(default = "default_gl_basis")]
    pub basis: String,
    /// Polynomial degree per edge function.
    #[serde(default = "default_gl_degree")]
    pub degree: usize,
    /// Hidden width; `0` ⇒ a single interpretable layer (one edge fn per feature).
    #[serde(default)]
    pub hidden: usize,
    /// Adam training epochs.
    #[serde(default = "default_gl_epochs")]
    pub epochs: usize,
    /// Adam learning rate.
    #[serde(default = "default_gl_lr")]
    pub lr: f64,
    /// Negatives (sampled non-edges) per positive edge.
    #[serde(default = "default_gl_neg_ratio")]
    pub neg_ratio: f64,
    /// Seed for negative sampling + parameter init (deterministic).
    #[serde(default = "default_gl_seed")]
    pub seed: u64,
    /// 1-hop neighbour-aggregation self-retention for the node-feature channel.
    #[serde(default = "default_gl_alpha")]
    pub alpha: f64,
}

#[cfg(feature = "graphlearn")]
impl Default for GraphLearnParams {
    fn default() -> Self {
        Self {
            basis: default_gl_basis(),
            degree: default_gl_degree(),
            hidden: 0,
            epochs: default_gl_epochs(),
            lr: default_gl_lr(),
            neg_ratio: default_gl_neg_ratio(),
            seed: default_gl_seed(),
            alpha: default_gl_alpha(),
        }
    }
}

#[cfg(feature = "graphlearn")]
fn default_gl_basis() -> String {
    "chebyshev".to_string()
}
#[cfg(feature = "graphlearn")]
fn default_gl_degree() -> usize {
    4
}
#[cfg(feature = "graphlearn")]
fn default_gl_epochs() -> usize {
    200
}
#[cfg(feature = "graphlearn")]
fn default_gl_lr() -> f64 {
    0.05
}
#[cfg(feature = "graphlearn")]
fn default_gl_neg_ratio() -> f64 {
    1.0
}
#[cfg(feature = "graphlearn")]
fn default_gl_seed() -> u64 {
    42
}
#[cfg(feature = "graphlearn")]
fn default_gl_alpha() -> f64 {
    0.5
}
#[cfg(feature = "graphlearn")]
fn default_gl_top_k() -> usize {
    50
}

/// The distributed graph algorithm a `DistributedCompute` / matview runs across
/// shards (CONCEPT:EG-KG.storage.feature). Each is a vertex-centric (Pregel/GAS) computation the
/// cross-shard superstep coordinator drives; the single-shard fast path stays the
/// always-on `PageRank`/`ConnectedComponents` ops.
#[cfg(feature = "compute-dist")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DistAlgo {
    /// PageRank — `damping`·rank-mass propagation for `iterations` supersteps. The
    /// cross-shard result matches the single-graph result on the UNION graph.
    PageRank { damping: f64, iterations: usize },
    /// Weakly-connected components — every vertex labeled with its component's
    /// representative, via label-propagation supersteps to a fixpoint.
    ConnectedComponents,
    /// BFS levels from `source` — every reachable vertex labeled with its hop distance.
    Bfs { source: String },
}

/// Materialized result of a `Method::Vf2SubgraphMatch` run (CONCEPT:EG-KG.mining.gspan-frequent-subgraph). Returned via
/// `ResultPayload::raw`. VF2 subgraph isomorphism is NP-hard with no bound
/// otherwise, so the backtracking search stops early once it hits `max_results`
/// collected matches or `max_steps` candidate-pair attempts (whichever first);
/// `truncated` is `true` when it stopped for either reason — the caller is seeing
/// a PARTIAL result, not proof no further match exists, and must raise
/// `max_results`/`max_steps` explicitly on the request to see more. The matcher
/// and its budget live in eg-core; this is the wire projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vf2MatchResult {
    pub matches: Vec<std::collections::HashMap<String, String>>,
    pub truncated: bool,
}

/// Materialized result of a `Method::Sql` query (CONCEPT:EG-KG.query.read-only-sql-query). Returned via
/// `ResultPayload::raw` — `rows[i]` is a MessagePack-encoded `Vec<serde_json::Value>`
/// aligned to `columns`, so the Python client double-unpacks the top-level `Raw`
/// blob then unpacks each row blob into a list of cells. Lives in eg-types (the
/// wire-DTO crate) so the protocol can embed it; the query algorithm stays in
/// eg-query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<u8>>,
}

/// Materialized result of a `Method::Sparql` SELECT (CONCEPT:EG-KG.ontology.concept-11). Returned via
/// `ResultPayload::raw`. `vars` is the projected variable order; each row is aligned
/// to `vars` with `None` for an unbound (OPTIONAL) variable. Lives in eg-types (the
/// wire-DTO crate) so the protocol can embed it; the evaluator lives in eg-rdf.
#[cfg(feature = "sparql")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparqlResult {
    pub vars: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

/// Materialized result of a `Method::OwlReason` run (CONCEPT:EG-KG.ontology.incremental-materialization). Returned via
/// `ResultPayload::raw`. The reasoner lives in eg-rdf; this is the wire projection.
#[cfg(feature = "owl")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwlReasonResult {
    /// Derived named-class subsumptions `(sub, sup)` (the reflexive/asserted ones are
    /// included; the closure is the full classification hierarchy).
    pub subclasses: Vec<(String, String)>,
    /// Per-subsumption confidence in `[0,1]` (CONCEPT:EG-KG.ontology.concept-13), ALIGNED index-for-index
    /// with `subclasses`. `1.0` for a hard/asserted subsumption; the propagated
    /// `axiom_conf × ∏ premise_conf` (max over alternative derivations) for an uncertain
    /// one. A fully-hard ontology yields all `1.0`.
    pub subclass_conf: Vec<f64>,
    /// Inferred instance memberships `(instance, class)` — every individual mapped to
    /// every class it (provably) belongs to, INCLUDING classes reached only through
    /// existential restrictions / role chains. When `target_class` was set, restricted
    /// to that class's members. Only memberships with confidence `≥ min_confidence`.
    pub instances: Vec<(String, String)>,
    /// Per-membership confidence in `[0,1]` (CONCEPT:EG-KG.ontology.concept-13), ALIGNED index-for-index
    /// with `instances`: the type fact's confidence (per-node confidence × Ebbinghaus
    /// decay) × the subsumption confidence — so an old/decayed or weakly-asserted fact
    /// yields a lower-confidence membership.
    pub instance_conf: Vec<f64>,
    /// `true` iff the ontology is consistent (no class forced to subsume `owl:Nothing`).
    pub consistent: bool,
    /// Named classes derived to be unsatisfiable (`A ⊑ ⊥`); empty when consistent.
    pub unsatisfiable: Vec<String>,
}

/// One node of a reconstructed OWL proof tree (CONCEPT:EG-KG.ontology.owl-proof-tree-explanation) — the wire
/// projection of `eg_rdf::owl::ProofNode`. `rule == "asserted"` marks a LEAF (a
/// reflexive seed or a base fact with no recorded justification — the proof bottoms
/// out there); any other `rule` is a completion rule name (`"CR-sub"`, `"CR-some⁺"`,
/// `"CR-instance"`, …) that consumed `premises` (each itself a full sub-proof) plus the
/// cited `axioms`. Recursive — `premises` nests to the tree's actual depth (never
/// flattened), so a client walks it exactly like the reasoner derived it.
#[cfg(feature = "owl")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofNodeWire {
    pub sub: String,
    pub sup: String,
    pub rule: String,
    pub axioms: Vec<String>,
    pub confidence: f64,
    pub premises: Vec<ProofNodeWire>,
}

/// Materialized result of a `Method::OwlExplain` run (CONCEPT:EG-KG.ontology.owl-proof-tree-explanation). Returned
/// via `ResultPayload::raw`. `tree` is `None` when `sub ⊑ sup` does not hold (nothing
/// to explain) — `found` mirrors that as a convenience boolean for callers that only
/// json-decode the top level.
#[cfg(feature = "owl")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwlExplainResult {
    /// Whether `sub ⊑ sup` holds under the classification (`tree.is_some()`).
    pub found: bool,
    /// The reconstructed proof tree, or `None` when `sub ⊑ sup` does not hold.
    pub tree: Option<ProofNodeWire>,
    /// `true` iff the classified ontology is consistent.
    pub consistent: bool,
    /// Named classes derived to be unsatisfiable; empty when consistent.
    pub unsatisfiable: Vec<String>,
}

// ── EXPLAIN surface wire results (CONCEPT:EG-KG.query.plan-dag, E5 phase 4) ──────────
// Diagnostics-only projections: `op`/`rule` render the underlying `eg_plan`/`eg_epistemic`
// type via its `Debug` impl (a typed wire mirror of the WHOLE cross-modal `Op` algebra —
// or the epistemic `JustRule` enum — would duplicate it for a read-only surface with no
// other consumer; the facade, which depends on `eg_plan`, builds these strings, so
// `eg_types` itself stays free of an `eg_plan`/`eg_epistemic` dependency, matching Rule R1).

/// One node of an `EXPLAIN PLAN` dump — the wire projection of an `eg_plan::dag::PlanNode`,
/// annotated with the SAME plan-time cost/cardinality estimate
/// (`eg_plan::ModalityCardinality`) the cost optimizer itself reorders on — so `EXPLAIN
/// PLAN` proves *why* a rewrite happened (bounded candidate/rerank/decode work, not just
/// *that* the op order changed). All estimate fields are plan-time abstract-unit numbers
/// over the SAME RLS-filtered snapshot the plan would execute against (CONCEPT:GOC-12,
/// cross-modal query intelligence) — never derived from an unfiltered store, so a denied
/// row can never inflate another agent's estimated candidate/cost numbers either (see
/// `eg-types` Rule R1: cost metadata lives on this wire projection, never on `Op` itself).
#[cfg(feature = "query")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainNodeWire {
    pub id: usize,
    /// `Debug`-rendered `eg_plan::Op`.
    pub op: String,
    pub inputs: Vec<usize>,
    /// Estimated rows flowing INTO this node — `0.0` for a source (no inputs), the prior
    /// node's `estimated_rows_out` for a single-input node, and the SUM of every input's
    /// `estimated_rows_out` for a multi-input (fan-in/join) node — a documented, safe
    /// over-estimate for the rare branch case; execution semantics are unaffected either
    /// way since EXPLAIN never executes an op.
    pub estimated_rows_in: f64,
    /// Estimated rows this node emits (`eg_plan::Cardinality::rows_out`).
    pub estimated_rows_out: f64,
    /// Estimated output selectivity — `rows_out / rows_in`, clamped `[0, 1]`
    /// (`eg_plan::ModalityCardinality::selectivity`); `1.0` for a source.
    pub estimated_selectivity: f64,
    /// Estimated CPU work in abstract units (`eg_plan::CostEstimate::cpu`).
    pub estimated_cost_cpu: f64,
    /// Estimated I/O work in abstract units — index probes / blob reads
    /// (`eg_plan::CostEstimate::io`).
    pub estimated_cost_io: f64,
}

/// Materialized result of a `Method::ExplainPlan` run. Returned via `ResultPayload::raw`.
#[cfg(feature = "query")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainPlanResult {
    /// The plan as a `PlanDag` BEFORE the DAG-aware cost optimizer.
    pub before: Vec<ExplainNodeWire>,
    /// The plan as a `PlanDag` AFTER `eg_plan::optimizer::optimize_dag`.
    pub after: Vec<ExplainNodeWire>,
    /// The active optimizer rule set, in application order (`eg_plan::cost_opt_rule_names()`).
    pub applied_rules: Vec<String>,
}

/// DAG-safe wire mirror of `eg_modality::ResourceId` for an evidence subject.
#[cfg(feature = "query")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum EvidenceResourceWire {
    Artifact(String),
    Occurrence(String),
    Rendition(String),
    Segment(String),
    Feature(String),
    EvidenceLocus(String),
}

/// DAG-safe wire mirror of `eg_modality::EvidenceAddress`.
#[cfg(feature = "query")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EvidenceAddressWire {
    CharacterRange {
        start: u64,
        end: u64,
    },
    TableCellRange {
        row_start: u64,
        row_end: u64,
        col_start: u64,
        col_end: u64,
    },
    ImageRegion {
        x: f64,
        y: f64,
        width: f64,
        height: f64,
    },
    PageRegion {
        page: u32,
        x: f64,
        y: f64,
        width: f64,
        height: f64,
    },
    AudioRange {
        start_ms: u64,
        end_ms: u64,
    },
    VideoTimeRange {
        start_ms: u64,
        end_ms: u64,
    },
    FrameRange {
        start_frame: u64,
        end_frame: u64,
    },
    MetricWindow {
        start_ms: u64,
        end_ms: u64,
    },
    Point {
        x: f64,
        y: f64,
    },
    RowVersion {
        row_ref: String,
        version: u64,
    },
    CodeSymbol {
        revision_ref: String,
        symbol_ref: String,
        start_line: u32,
        end_line: u32,
    },
    TraceSpan {
        trace_ref: String,
        span_ref: String,
    },
}
