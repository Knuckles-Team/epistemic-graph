//! Wire DTOs embedded in feature-gated `protocol::Method` variants. They live in
//! eg-types (the bottom of the DAG) so the protocol enum can name them without
//! depending on eg-compute. The `finance` / `datascience` modules re-export them
//! (`pub use eg_types::wire::Order;` etc.) so their algorithm code is untouched.
//!
//! Each type is gated by the same feature as the Method variant that carries it,
//! so a build that omits a compute domain drops its wire types from the enum too.

#[cfg(any(
    feature = "finance",
    feature = "datascience",
    feature = "query",
    feature = "streaming"
))]
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

/// A FOREIGN (external) RowSet source for the federation `Op::ForeignScan`
/// (CONCEPT:KG-2.232, Lane P). A federated query reads rows from a source OUTSIDE
/// the local engine and composes them with the local graph/vector/SQL ops — so a
/// `ForeignScan` is just another RowSet leaf, like `Scan`/`Reason`/`SparqlBgp`. Two
/// kinds, behind one wire enum; the `ForeignSource` trait in eg-plan turns each into
/// a [`crate::RowSet`].
#[cfg(feature = "federation")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ForeignSourceSpec {
    /// A REMOTE epistemic-graph engine, reached over the SAME length-prefixed
    /// MessagePack + HMAC transport this engine speaks. The federation client
    /// connects to `endpoint` (a `host:port` TCP address), sends a `UnifiedQueryText`
    /// (UQL) — or, when `uql` is empty, a `CypherQuery` — against the remote `graph`,
    /// and projects the result rows into a local RowSet. `secret` is the remote's
    /// HMAC-SHA256 auth secret (empty ⇒ the remote runs insecure). This composes the
    /// engine with ANOTHER engine without a Python round-trip — the cross-engine
    /// federation seam.
    RemoteEngine {
        /// `host:port` of the remote engine's TCP listener.
        endpoint: String,
        /// The remote graph to query (e.g. `__commons__`).
        graph: String,
        /// HMAC-SHA256 secret of the remote (empty ⇒ remote is insecure).
        #[serde(default)]
        secret: String,
        /// A UQL query run on the remote (its rows seed the RowSet). When empty, `cypher`
        /// is used instead.
        #[serde(default)]
        uql: String,
        /// A Cypher query run on the remote when `uql` is empty (must `RETURN` an id
        /// column named by `id_field`).
        #[serde(default)]
        cypher: String,
        /// For the Cypher path: the RETURN column that carries the node id. Ignored on
        /// the UQL path (a UQL plan's rows are already `[id, score?]`).
        #[serde(default)]
        id_field: String,
    },
    /// A GENERIC HTTP/JSON source. The federation client issues an HTTP GET to `url`
    /// (pure-Rust rustls client — NOT openssl, gated OUT of the Pi tier), walks the
    /// JSON response to the array at `json_path` (a dotted path, e.g. `data.items`),
    /// and maps each element into a RowSet row via `field_map`: the element field
    /// named `field_map.id` becomes the row id, and (optionally) `field_map.score`
    /// becomes the row score. Any external REST API thereby becomes a joinable RowSet.
    HttpJson {
        /// The HTTP(S) URL to GET.
        url: String,
        /// Dotted path to the JSON array of rows (empty ⇒ the response is itself the
        /// array).
        #[serde(default)]
        json_path: String,
        /// Maps a JSON element to a RowSet row (which field is the id / the score).
        field_map: HttpFieldMap,
    },
    /// An EXTERNAL relational-SQL database (Postgres/MySQL/…) (CONCEPT:KG-2.239). The
    /// federation client connects to `dsn`, runs `query`, and maps each result row to a
    /// RowSet row: the column named `id_field` becomes the row id (stringified), and
    /// (optionally) `score_field` becomes the row score. This lets ONE unified plan JOIN
    /// an external SQL table with the LOCAL graph/vector — the "engine federates external
    /// SQL" half that sql-mcp alone cannot give: sql-mcp speaks SQL to the engine, this
    /// lets the engine speak SQL OUT to a foreign RDBMS and fuse the rows in-plan.
    ///
    /// This wire variant is PURE serde (it carries only the DSN + query strings), so it
    /// is present whenever `federation` is on. The actual SQL *driver* (a pure-Rust /
    /// rustls client — NEVER openssl) lives in eg-plan behind its own `federation-sql`
    /// gate and is folded OUT of the Pi tier (the Pi contract: no SQL driver / rustls /
    /// openssl). A `federation` build WITHOUT `federation-sql` accepts + registers a
    /// `Sql` spec but errors clearly if a plan actually tries to fetch it.
    Sql {
        /// The external database DSN (e.g. `postgres://user:pw@host:5432/db`).
        dsn: String,
        /// The SQL query to run against the external DB. Its result columns are mapped
        /// to RowSet rows via `id_field` / `score_field`.
        query: String,
        /// The result column whose value is the row id (stringified).
        id_field: String,
        /// The result column whose numeric value is the row score (absent ⇒ unscored).
        #[serde(default)]
        score_field: Option<String>,
    },
}

/// Which JSON element fields become a RowSet row's id / score (for
/// [`ForeignSourceSpec::HttpJson`]).
#[cfg(feature = "federation")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HttpFieldMap {
    /// The element field whose value is the row id (stringified).
    pub id: String,
    /// The element field whose numeric value is the row score (absent ⇒ unscored).
    #[serde(default)]
    pub score: Option<String>,
}

/// One cross-modal operator. A [`Plan`] is an ordered list of these — a pipeline
/// where each op `(RowSet) -> RowSet`. This increment binds SQL + graph + vector
/// (`Scan | Filter | Traverse | Rank | Limit`); reasoning/blob ops are later
/// increments. The algorithm lives in `eg-plan`; this is the wire DTO.
/// Which timeline an [`Op::AsOf`] instant pins (bi-temporal, KG-2.250).
#[cfg(feature = "query")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TimeAxis {
    /// Valid (event) time — "what was TRUE at the instant" (`valid_from`/`valid_until`).
    #[default]
    Valid,
    /// Transaction time — "what we BELIEVED at the instant" (`tx_from`/`tx_to`).
    Transaction,
}

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
    /// UDF (WASM) — transform the current `RowSet` through a registered, SANDBOXED
    /// WebAssembly user function (CONCEPT:KG-2.228). The executor serializes the input
    /// rows (ids + scores) to bytes, runs the wasm module `id` under fuel + memory
    /// limits with NO host capabilities, and deserializes the returned rows back into
    /// the pipeline. A pure `RowSet -> RowSet` op like every other, so a UDF composes
    /// with Scan/Filter/Traverse/Rank/Limit. Gated by `wasm-udf` (the wasmtime runtime
    /// lives in eg-wasm behind its own gate; this is just the wire variant).
    #[cfg(feature = "wasm-udf")]
    Udf { id: String },
    /// SOURCE (federation) — read rows from an EXTERNAL source and seed the RowSet
    /// (CONCEPT:KG-2.232, Lane P). `source` is either a REMOTE epistemic-graph engine
    /// (queried over the same transport) or a generic HTTP/JSON API — see
    /// [`ForeignSourceSpec`]. The resulting RowSet then flows — like any other source
    /// op — into a downstream `Filter`/`Traverse`/`Rank`/`Limit`, so a federated query
    /// JOINS a foreign source with the LOCAL graph in ONE plan. A `ForeignScan` placed
    /// AFTER a local source op replaces the input (it is a source, not a transform),
    /// exactly like `Scan`/`Reason`/`SparqlBgp`. Gated by `federation` (the HTTP client
    /// is rustls/pure-Rust and kept OUT of the Pi tier — the Pi contract).
    ///
    /// `ForeignScan` is the RESOLVED federation EXECUTOR (it carries a fully-specified
    /// [`ForeignSourceSpec`] and actually fetches+joins). The UQL `FOREIGN "<name>"`
    /// clause (CONCEPT:KG-2.235) instead lowers to the lighter [`Op::Foreign`] name
    /// MARKER below — the parser only has a name, and resolving that name to a concrete
    /// `ForeignSourceSpec` requires the server-side `foreign_sources` registry that
    /// eg-plan (below the server) cannot reach. The two are complementary: `Foreign`
    /// is the named-reference surface, `ForeignScan` is the resolved executor a
    /// server/planner constructs.
    #[cfg(feature = "federation")]
    ForeignScan {
        source: ForeignSourceSpec,
        /// When this `ForeignScan` is NOT the first op, intersect its rows with the
        /// current candidate set (a foreign∩local JOIN keyed on id) instead of
        /// replacing the input. The default (false) makes it a pure source. The
        /// preceding rows' ORDER is preserved on an intersect.
        #[serde(default)]
        join: bool,
    },
    /// TIME (`AS OF [TX] @<ts>`, CONCEPT:KG-2.235 / KG-2.250) — pin the RowSet to a
    /// point-in-time `ts` (unix seconds) and DROP rows not live at that instant. A
    /// RowSet-narrowing temporal filter executed in `eg-plan` (dep-free blob scan, no
    /// DataFusion — Pi-safe). `axis` selects the timeline: `Valid` = "what was TRUE at
    /// ts" (`valid_from`/`valid_until`); `Transaction` = "what we BELIEVED at ts"
    /// (`tx_from`/`tx_to`). `#[serde(default)]` keeps older plans (no axis) as valid-time.
    AsOf {
        ts: f64,
        #[serde(default)]
        axis: TimeAxis,
    },
    /// TIME (`WINDOW <dur>`, CONCEPT:KG-2.235) — declare a trailing time window of
    /// `secs` seconds for the windowed time-series aggregate. A RowSet-preserving
    /// CONTEXT op paired with `AsOf`; passes the rows through unchanged today (the
    /// windowed aggregate is the eg-tsdb seam) but lets the `WINDOW <dur>` UQL clause
    /// lower to ONE plan AST. Always available under `query`.
    Window { secs: f64 },
    /// FEDERATION (`FOREIGN "<name>"`, CONCEPT:KG-2.235) — mark the seed as drawn from
    /// the named foreign source `name` (a registered federation peer). A RowSet
    /// CONTEXT op: today it passes the rows through (the cross-source pull is the
    /// federation seam) but gives the `FOREIGN "<name>"` UQL clause a plan AST to lower
    /// to. Always available under `query`. The RESOLVED-source executor is the
    /// `federation`-gated [`Op::ForeignScan`] above; see its note for why the UQL
    /// name marker stays distinct from the resolved spec.
    Foreign { name: String },
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

// ── Streaming / CDC / continuous queries / watch / triggers (CONCEPT:KG-2.229/230) ──
// PURE-data wire DTOs for the reactive surface. They are produced/consumed by the
// `streaming` handler over the existing one-Response-per-Request transport (cursor /
// long-poll, NOT a side-channel), so the enum stays free of any server type.

/// The kind of a captured change. Mirrors the durable-mutation `Method` set the CDC
/// feed records (the ledger's `is_durable_mutation` family), flattened to the unit a
/// consumer reasons about (node add/remove/update, edge add/remove).
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CdcKind {
    AddNode,
    RemoveNode,
    UpdateNode,
    AddEdge,
    RemoveEdge,
}

/// One ordered change in a graph's CDC feed (CONCEPT:KG-2.229). `seq` is the per-graph
/// monotonic cursor: a consumer tails with `CdcRead { from_seq }` and re-reads from a
/// later `seq` to skip what it has already seen. `before`/`after` carry the affected
/// node/edge property blob (MessagePack) pre- and post-mutation; `None` means absent
/// (an add has no `before`, a remove no `after`).
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcEvent {
    pub seq: u64,
    pub graph: String,
    pub kind: CdcKind,
    /// The node id for node changes / the edge source for edge changes.
    pub node_id: String,
    /// The edge target (empty for node changes).
    #[serde(default)]
    pub target_id: String,
    /// The change's `type`/label property after the mutation (for label-filtered
    /// watch + triggers); best-effort decoded from `after` (else `before`).
    #[serde(default)]
    pub label: String,
    #[serde(default, with = "serde_bytes")]
    pub before: Vec<u8>,
    #[serde(default, with = "serde_bytes")]
    pub after: Vec<u8>,
    /// True when `before` carried a value (distinguishes an empty blob from absent).
    #[serde(default)]
    pub had_before: bool,
    /// True when `after` carried a value.
    #[serde(default)]
    pub had_after: bool,
}

/// Spec for a registered continuous query (CONCEPT:KG-2.229), incrementally maintained
/// as CDC changes arrive. One graph, one label filter, one aggregate. Deliberately
/// SIMPLE — a counting/sum/filter view that updates on delta rather than re-running.
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContinuousQuerySpec {
    /// The graph whose CDC feed drives this view.
    pub graph: String,
    /// Only changes whose label equals this maintain the view (empty ⇒ all nodes).
    #[serde(default)]
    pub label: String,
    /// The aggregate maintained: "count" (live node count for the label) or
    /// "sum:<field>" (running sum of a numeric node property).
    pub agg: ContinuousAgg,
}

/// The incremental aggregate a continuous query maintains.
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ContinuousAgg {
    /// Live count of matching nodes (add +1, remove -1).
    Count,
    /// Running sum of a numeric node property (delta = new - old on update).
    Sum { field: String },
}

/// Current result of a continuous query — the incrementally-maintained value plus the
/// CDC `seq` it reflects (so a reader knows how current it is).
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContinuousQueryResult {
    pub name: String,
    /// The aggregate value (count, or sum).
    pub value: f64,
    /// The CDC seq this result has folded in up to (the watermark).
    pub through_seq: u64,
}

/// A batch returned by a `Watch` long-poll (CONCEPT:KG-2.230): the matching changes
/// since the client's cursor plus the next cursor to resume from.
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WatchBatch {
    pub events: Vec<CdcEvent>,
    /// The cursor to pass as `from_seq` next time (one past the last delivered seq).
    pub next_seq: u64,
}

/// One trigger registration (CONCEPT:KG-2.230). When a CDC change in `graph` matches
/// `label` + `op`, the trigger fires its `action` (an opaque MessagePack payload the
/// consumer interprets — e.g. a notification topic / a webhook spec). Fired actions
/// are recorded in a per-graph fired log a client polls with `FiredTriggers`.
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TriggerInfo {
    pub name: String,
    pub graph: String,
    #[serde(default)]
    pub label: String,
    /// "add" | "remove" | "update" | "any" — the change kind that fires it.
    pub op: String,
    /// How many times this trigger has fired.
    pub fire_count: u64,
}

/// A recorded trigger firing (CONCEPT:KG-2.230). Returned by `FiredTriggers` so a
/// reaction consumer pulls the action payload + the change that fired it.
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FiredAction {
    /// Per-graph monotonic fired-id (the cursor for `FiredTriggers`).
    pub fire_seq: u64,
    pub trigger: String,
    pub graph: String,
    /// The CDC seq of the change that fired the trigger.
    pub change_seq: u64,
    pub node_id: String,
    /// The opaque action payload registered with the trigger (MessagePack).
    #[serde(default, with = "serde_bytes")]
    pub action: Vec<u8>,
}
