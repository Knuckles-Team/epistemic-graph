//! Wire DTOs embedded in feature-gated `protocol::Method` variants. They live in
//! eg-types (the bottom of the DAG) so the protocol enum can name them without
//! depending on eg-compute. The `finance` / `datascience` modules re-export them
//! (`pub use eg_types::wire::Order;` etc.) so their algorithm code is untouched.
//!
//! Each type is gated by the same feature as the Method variant that carries it,
//! so a build that omits a compute domain drops its wire types from the enum too.

#[cfg(any(feature = "finance", feature = "datascience", feature = "query"))]
use serde::{Deserialize, Serialize};

// ── unified cross-modal query plan AST (CONCEPT:KG-2.208) ────────────────────

/// A simple equality / range predicate over a node property, compiled to a SQL
/// `WHERE` fragment and evaluated by the DataFusion FILTER leg in `eg-plan`.
#[cfg(feature = "query")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Pred {
    /// `prop == value` (string compare on the JSON-stringified value).
    Eq { prop: String, value: String },
    /// `prop > n` (numeric).
    GtNum { prop: String, n: f64 },
    /// `prop < n` (numeric).
    LtNum { prop: String, n: f64 },
}

/// One cross-modal operator. A [`Plan`] is an ordered list of these — a pipeline
/// where each op `(RowSet) -> RowSet`. This increment binds SQL + graph + vector
/// (`Scan | Filter | Traverse | Rank | Limit`); reasoning/blob ops are later
/// increments. The algorithm lives in `eg-plan`; this is the wire DTO.
#[cfg(feature = "query")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Op {
    /// SOURCE — seed from all nodes carrying `label` (`type == label`).
    Scan { label: String },
    /// FILTER (relational) — keep rows matching ALL `preds`, via real DataFusion.
    Filter { preds: Vec<Pred> },
    /// TRAVERSE (graph) — follow `rel` edges `min..=max` hops (petgraph BFS).
    Traverse { rel: String, min: usize, max: usize },
    /// RANK (vector) — re-order by cosine similarity to `query` (SemanticStore kNN).
    Rank { query: Vec<f32> },
    /// RANK (lexical, BM25) — re-order the candidate set by BM25 relevance to the
    /// natural-language `query` string over the text index (CONCEPT:KG-2.215). A
    /// sibling of the vector `Rank`: it produces a score-per-id over the SAME RowSet
    /// currency, so the closed algebra is unchanged. Gated by `text` (the Tantivy
    /// index lives in eg-text behind its own gate; this is just the wire variant).
    #[cfg(feature = "text")]
    RankText { query: String },
    /// FUSE (hybrid) — reciprocal-rank-fusion of the results of two SUB-PLANS over the
    /// same seed (typically a vector `Rank` branch and a lexical `RankText` branch)
    /// into ONE ranked RowSet (CONCEPT:KG-2.215). The modern hybrid-retrieval pattern:
    /// fuse the RANKS (not the incomparable BM25/cosine scores) so a doc strong in
    /// BOTH modalities out-ranks one strong in only one. `k` is the RRF damping
    /// constant (use `eg_text::RRF_K` = 60 by convention; `0.0` ⇒ that default).
    #[cfg(feature = "text")]
    FuseRrf {
        left: Vec<Op>,
        right: Vec<Op>,
        k: f32,
    },
    /// SOURCE (semantic, OWL) — seed the RowSet with every individual the native OWL 2
    /// reasoner INFERS to be a member of `target_class` (CONCEPT:KG-2.219/220). The
    /// reasoner classifies the graph's TBox (the OWL axioms loaded as RDF) and returns
    /// the instances of `target_class` — INCLUDING ones reached through existential
    /// restrictions / role chains for which the property-graph stored NO explicit type
    /// edge. `ontology` (Turtle) carries the axioms; an empty string ⇒ use the axioms
    /// already in the graph. The result then flows — like any RowSet — into a graph
    /// `Traverse`, a vector `Rank`, a SQL `Filter`, or a `Limit`. Gated by `owl`.
    #[cfg(feature = "owl-plan")]
    Reason {
        /// The named class whose (inferred) members seed the RowSet (canonical `<iri>`
        /// or a bare IRI string — both are accepted).
        target_class: String,
        /// OWL axioms as a Turtle document. Empty ⇒ classify the axioms already loaded
        /// into the request's graph.
        #[serde(default)]
        ontology: String,
    },
    /// SOURCE (semantic, SPARQL) — seed the RowSet with the node bindings of `var` in
    /// the result of the SPARQL `query` (a basic graph pattern, CONCEPT:KG-2.220),
    /// evaluated over the request's graph. A SPARQL-selected candidate set as a normal
    /// RowSet source: it then flows into the SAME graph/vector/SQL/time ops as any
    /// other op. Gated by `owl` (which implies `sparql`).
    #[cfg(feature = "owl-plan")]
    SparqlBgp {
        /// A SPARQL 1.1 SELECT (the basic-graph-pattern surface eg-rdf evaluates).
        query: String,
        /// The projected variable whose (resource) bindings become the RowSet ids.
        var: String,
    },
    /// LIMIT — top-k, respecting the current order.
    Limit { k: usize },
}

/// A logical plan: an ordered list of [`Op`]s over one `RowSet`. The serializable
/// wire payload of `Method::UnifiedQuery` (CONCEPT:KG-2.208).
#[cfg(feature = "query")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    pub ops: Vec<Op>,
}

#[cfg(feature = "query")]
impl Plan {
    pub fn new(ops: Vec<Op>) -> Self {
        Self { ops }
    }
}

// ── finance ────────────────────────────────────────────────────────────────

/// A single order in the book (matched by `eg-compute::finance::exchange`).
#[cfg(feature = "finance")]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Order {
    pub id: String,
    pub side: String, // "buy" or "sell"
    pub price: f64,
    pub quantity: f64,
    pub timestamp: u64,
}

/// One fiscal year of standardized financial-statement inputs (forensic scores).
#[cfg(feature = "finance")]
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct YearData {
    pub sales: f64,
    pub cogs: f64,
    pub sga: f64,
    pub net_income: f64,
    pub cfo: f64, // operating cash flow
    pub receivables: f64,
    pub current_assets: f64,
    pub current_liabilities: f64,
    pub ppe_net: f64,
    pub depreciation: f64,
    pub total_assets: f64,
    pub total_liabilities: f64,
    pub long_term_debt: f64,
    pub retained_earnings: f64,
    pub ebit: f64,
    pub market_cap: f64,
    pub shares: f64,
}

// ── datascience ──────────────────────────────────────────────────────────────

/// Hyperparameters for the estimators. All optional; per-estimator defaults are
/// applied at fit time to mirror scikit-learn's defaults.
#[cfg(feature = "datascience")]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EstimatorParams {
    pub alpha: Option<f64>,
    pub l1_ratio: Option<f64>,
    pub max_depth: Option<usize>,
    pub min_samples_split: Option<usize>,
    pub min_samples_leaf: Option<usize>,
    pub n_estimators: Option<usize>,
    pub learning_rate: Option<f64>,
    pub max_features: Option<usize>,
    pub subsample: Option<f64>,
    pub random_state: Option<u64>,
    // SVR
    #[serde(rename = "C")]
    pub c: Option<f64>,
    pub epsilon: Option<f64>,
    pub gamma: Option<f64>,
    pub kernel: Option<String>,
    pub max_iter: Option<usize>,
    pub tol: Option<f64>,
}

/// A flat regression tree: node `i` is a leaf when `feature < 0`.
#[cfg(feature = "datascience")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeNode {
    pub feature: i64,
    pub threshold: f64,
    pub left: i64,
    pub right: i64,
    pub value: f64,
}

#[cfg(feature = "datascience")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionTree {
    pub nodes: Vec<TreeNode>,
}

#[cfg(feature = "datascience")]
impl DecisionTree {
    /// Walk the flat tree to the leaf for one feature row. `pub` so the estimator
    /// code in `eg-compute::datascience` can drive prediction across the crate
    /// boundary (the data lives here; the fit/predict logic stays upstream).
    pub fn predict_one(&self, x: &[f64]) -> f64 {
        if self.nodes.is_empty() {
            return 0.0;
        }
        let mut idx = 0usize;
        loop {
            let node = &self.nodes[idx];
            if node.feature < 0 {
                return node.value;
            }
            idx = if x[node.feature as usize] <= node.threshold {
                node.left as usize
            } else {
                node.right as usize
            };
        }
    }
}

/// Serializable fitted model returned by `fit_estimator`.
#[cfg(feature = "datascience")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "model")]
pub enum FittedModel {
    Linear {
        coefficients: Vec<f64>,
        intercept: f64,
    },
    Tree(DecisionTree),
    Forest {
        trees: Vec<DecisionTree>,
    },
    GradientBoosting {
        init: f64,
        learning_rate: f64,
        trees: Vec<DecisionTree>,
    },
    AdaBoost {
        trees: Vec<DecisionTree>,
        weights: Vec<f64>,
    },
    Svr {
        support_vectors: Vec<Vec<f64>>,
        dual_coef: Vec<f64>,
        intercept: f64,
        kernel: String,
        gamma: f64,
    },
}
