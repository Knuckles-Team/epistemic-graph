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
    /// DOCUMENT/JSON — keep rows whose node property document satisfies a deep
    /// JSONPath predicate (CONCEPT:EG-084). `path` is a JSONPath (`$.a.b`, `$.a[0]`,
    /// `$.a[*]`, wildcard) evaluated against the row's decoded JSON; `op` is the
    /// existence / equality / `@>`-containment test. This is the lowered form of the
    /// Postgres JSON operators (`->`, `->>`, `@>`, `jsonb_path_query`) and a Mongo-style
    /// `$match` — see `eg_query::sql::classify`. Like the spatial preds it is NOT lowered
    /// to SQL (DataFusion has no JSONPath): `eg-plan`'s FILTER leg splits it out and
    /// applies it per-row against the stored JSON, and the planner can consult eg-core's
    /// inverted path-index for candidate selectivity. PURE serde here (a string, a small
    /// enum, and a `serde_json::Value` literal — exactly as `CepAttrPredSpec` carries),
    /// so it is present whenever `query` is on and adds NO dependency.
    JsonPath { path: String, op: JsonPathOp },
    /// SPATIAL — keep rows whose geometry (in node property `column`, stored as WKT)
    /// is spatially WITHIN the query geometry `wkt` (CONCEPT:EG-083). Evaluated by
    /// eg-geo's planar `within` in eg-plan (behind the `geo` feature) — NOT lowered to
    /// SQL (DataFusion has no spatial), so the FILTER leg splits spatial preds out and
    /// applies them per-row against the stored geometry. Gated by `geo` (implies
    /// `query`); pure serde here (only two strings).
    #[cfg(feature = "geo")]
    SpatialWithin { column: String, wkt: String },
    /// SPATIAL — keep rows whose geometry (node property `column`, WKT) lies within
    /// planar `distance` of the query geometry `wkt` — an `ST_DWithin` (CONCEPT:EG-083).
    /// Evaluated by eg-geo's planar `distance` in eg-plan. Gated by `geo`.
    #[cfg(feature = "geo")]
    SpatialDWithin {
        column: String,
        wkt: String,
        distance: f64,
    },
    /// SPATIAL (DE-9IM, CONCEPT:EG-258) — keep rows whose geometry (node property `column`,
    /// WKT) is topologically related to the query geometry `wkt` per the named DE-9IM
    /// relation. Each mirrors [`Pred::SpatialWithin`] (two strings) and is evaluated per-row
    /// by eg-geo's `predicates` in eg-plan — NOT lowered to SQL. Gated by `geo`.
    ///
    /// `SpatialContains` = the row geometry CONTAINS the query geometry; `SpatialCovers`
    /// its boundary-inclusive superset; `SpatialTouches` boundary-only contact;
    /// `SpatialCrosses` interiors meeting in lower dimension; `SpatialOverlaps` same-dim
    /// partial overlap; `SpatialEquals` geometric equality; `SpatialDisjoint` no shared
    /// point.
    #[cfg(feature = "geo")]
    SpatialContains { column: String, wkt: String },
    #[cfg(feature = "geo")]
    SpatialCovers { column: String, wkt: String },
    #[cfg(feature = "geo")]
    SpatialTouches { column: String, wkt: String },
    #[cfg(feature = "geo")]
    SpatialCrosses { column: String, wkt: String },
    #[cfg(feature = "geo")]
    SpatialOverlaps { column: String, wkt: String },
    #[cfg(feature = "geo")]
    SpatialEquals { column: String, wkt: String },
    #[cfg(feature = "geo")]
    SpatialDisjoint { column: String, wkt: String },
}

/// DOCUMENT/JSON — the test applied by [`Pred::JsonPath`] against the value(s) a
/// JSONPath resolves to (CONCEPT:EG-084). PURE serde (a small tag + an optional
/// `serde_json::Value` literal); the actual walk/containment lives in
/// `eg_core::jsonpath` behind eg-plan's FILTER leg — this is the wire variant.
#[cfg(feature = "query")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum JsonPathOp {
    /// The path resolves to at least one value (`jsonb_path_query` existence / `@?`).
    Exists,
    /// Some value at the path equals `value` (Postgres `->` / `->>` equality). A `->>`
    /// text compare coerces a scalar to text, so a JSON-string `value` also matches a
    /// numeric leaf whose canonical text is equal.
    Eq { value: serde_json::Value },
    /// The value at the path CONTAINS `value` per Postgres `@>` JSON containment
    /// (object ⊇ object, array ⊇ array/scalar, scalar equality).
    Contains { value: serde_json::Value },
}

/// SPATIAL — the constructive geometry op applied by `Op::SpatialOp` (CONCEPT:EG-259).
/// Pure serde here (Pi-safe, no eg-geo dep); the executor maps it to eg-geo's `algebra`
/// behind eg-plan's `geo` gate. Unary ops (`Buffer`/`ConvexHull`/`Simplify`/`Centroid`)
/// derive from the row geometry alone; binary ops (`Union`/`Intersection`/`Difference`)
/// take the second operand as a WKT literal.
#[cfg(feature = "geo")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SpatialOpKind {
    /// Grow the geometry outward by `distance` (a convex buffer polygon).
    Buffer { distance: f64 },
    /// The convex hull of the geometry's vertices.
    ConvexHull,
    /// Douglas–Peucker vertex reduction at `tolerance`.
    Simplify { tolerance: f64 },
    /// The centroid (a `Point`).
    Centroid,
    /// The (convex) union of the row geometry with `wkt`.
    Union { wkt: String },
    /// The intersection of the row geometry with `wkt` (Sutherland–Hodgman; convex clip).
    Intersection { wkt: String },
    /// The difference of the row geometry minus `wkt` (documented subset).
    Difference { wkt: String },
}

/// TENSOR — how `TensorOpKind::Reduce` collapses one axis (CONCEPT:EG-085). Mirrors
/// eg-tensor's `ReduceKind`, but defined HERE (pure serde, no eg-tensor dep) so the
/// wire stays Pi-safe; the executor maps it to eg-tensor's enum behind eg-plan's
/// `tensor` gate.
#[cfg(feature = "tensor")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TensorReduceKind {
    Sum,
    Mean,
    Max,
    Min,
}

/// TENSOR — the scalar op `TensorOpKind::Elementwise` applies to every element
/// (CONCEPT:EG-085). Mirrors eg-tensor's `ElementwiseOp`; pure serde here.
#[cfg(feature = "tensor")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TensorElementwiseOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// TENSOR — the transform carried by `Op::TensorOp` (CONCEPT:EG-085): one of the
/// eg-tensor array ops, applied per-row to each row's tensor. `Slice` gathers a
/// hyper-rectangle (`ranges[d] = (start, end)`, one per axis); `Reduce` collapses one
/// `axis` with `kind`; `Elementwise` applies `op` with `scalar` to every element. PURE
/// serde (only `usize`/`f64`/the two plain enums) — the actual N-D array math lives in
/// eg-tensor behind eg-plan's `tensor` gate; this is the wire variant.
#[cfg(feature = "tensor")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TensorOpKind {
    Slice { ranges: Vec<(usize, usize)> },
    Reduce { axis: usize, kind: TensorReduceKind },
    Elementwise { op: TensorElementwiseOp, scalar: f64 },
}

/// STREAM — a per-event attribute predicate for a CEP matcher (CONCEPT:EG-088). Mirrors
/// eg-stream's `AttrPredicate`, but defined HERE (pure serde, no eg-stream dep) so the
/// wire stays Pi-safe; the executor maps it to eg-stream's enum behind eg-plan's
/// `stream` gate.
#[cfg(feature = "stream")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CepAttrPredSpec {
    Eq { field: String, value: serde_json::Value },
    Gt { field: String, value: f64 },
    Lt { field: String, value: f64 },
    Exists { field: String },
}

/// STREAM — one event matcher: an optional event `key` + attribute predicates that ALL
/// must hold (CONCEPT:EG-088). Mirrors eg-stream's `EventMatcher`; pure serde here.
#[cfg(feature = "stream")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CepMatcherSpec {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub preds: Vec<CepAttrPredSpec>,
}

/// STREAM — the sliding / tumbling window a CEP pattern runs over (CONCEPT:EG-088).
/// Mirrors eg-stream's `Window`; pure serde here.
#[cfg(feature = "stream")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CepWindowSpec {
    Sliding { size: u64 },
    Tumbling { size: u64 },
}

/// STREAM — the CEP pattern tree (CONCEPT:EG-088): `Sequence` (matchers in order within
/// the window), `Within` (a duration constraint wrapping an inner pattern), `Absence`
/// (`a` NOT-followed-by `b` within `within`). Mirrors eg-stream's `CepPattern`; pure
/// serde — the actual NFA lives in eg-stream behind eg-plan's `stream` gate.
#[cfg(feature = "stream")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CepNodeSpec {
    Sequence(Vec<CepMatcherSpec>),
    Within {
        within: u64,
        pattern: Box<CepNodeSpec>,
    },
    Absence {
        a: CepMatcherSpec,
        b: CepMatcherSpec,
        within: u64,
    },
}

/// STREAM — the full CEP spec carried by `Op::Cep` (CONCEPT:EG-088): the `pattern` tree
/// + the `window` it is evaluated over. PURE serde — the executor turns it into an
/// eg-stream `run(pattern, events, window)` call behind eg-plan's `stream` gate; this is
/// the wire variant.
#[cfg(feature = "stream")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CepPatternSpec {
    pub pattern: CepNodeSpec,
    pub window: CepWindowSpec,
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
    /// A NAMED reference to a foreign source registered in the executor's
    /// `ForeignSourceRegistry` (CONCEPT:EG-073). Unlike the self-describing variants
    /// above (which carry the full connection spec inline), this carries ONLY a `name`
    /// — the executor resolves it to a concrete, pre-registered [`ForeignSource`] at
    /// plan time. This is the resolution seam the UQL `FOREIGN "<name>"` clause needs:
    /// the parser only ever sees a name, and binding that name to a live source lives
    /// with the server/facade (which owns the registry), not in the wire DTO. A `Named`
    /// spec handed to a source builder WITHOUT a registry is a clean typed error, never
    /// a silent empty set.
    Named {
        /// The registry key naming a pre-registered foreign source.
        name: String,
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
    /// RANK (graph distance, CONCEPT:KG-2.254) — re-order the candidate set by inverse
    /// shortest-path hop distance from `center` over the graph topology, score
    /// `1/(1+hops)` (unreachable → 0). A graph-NATIVE reranker (Graphiti's
    /// `node_distance`): proximity to a focal node, fused alongside vector/BM25. Reuses
    /// the same BFS the `Traverse` leg uses; dep-free, so it ships under base `query`.
    RankNodeDistance { center: String },
    /// RANK (provenance salience, CONCEPT:KG-2.254) — re-order the candidate set by how
    /// many edges MENTION each node (incoming-edge count), score normalized to the max
    /// in the set. Graphiti's `episode_mentions` salience: a node many episodes point at
    /// ranks higher. Topology-only, dep-free, base `query`.
    RankMentions {},
    /// RANK (MMR diversity, CONCEPT:KG-2.255) — re-order the candidate set by Maximal
    /// Marginal Relevance: greedily pick the next item maximizing
    /// `lambda*rel - (1-lambda)*max_sim_to_already_picked`, where rel is the item's
    /// incoming relevance score (from a prior `Rank`) and sim is cosine over stored
    /// embeddings. Reduces near-duplicate redundancy in the top-k — the diversity
    /// reranker Graphiti does NOT have. `k` caps how many to re-rank (0 ⇒ all). Reads
    /// `ctx.semantic` (always present), so base `query`.
    RankMmr { lambda: f32, k: usize },
    /// RANK (lexical, BM25) — re-order the candidate set by BM25 relevance to the
    /// natural-language `query` string over the text index (CONCEPT:KG-2.215). A
    /// sibling of the vector `Rank`: it produces a score-per-id over the SAME RowSet
    /// currency, so the closed algebra is unchanged. Gated by `text` (the Tantivy
    /// index lives in eg-text behind its own gate; this is just the wire variant).
    #[cfg(feature = "text")]
    RankText { query: String },
    /// FUSE (hybrid) — reciprocal-rank-fusion of N SUB-PLAN `branches` over the SAME
    /// seed into ONE ranked RowSet (CONCEPT:KG-2.215 / KG-2.253). The modern hybrid-
    /// retrieval pattern, generalized past two legs: fuse the RANKS (not the
    /// incomparable BM25/cosine/distance scores) so a doc strong across MORE branches
    /// out-ranks one strong in only one. The canonical tri-modal hybrid is
    /// `branches = [[Rank{vec}], [RankText{q}], [RankNodeDistance{c}]]`. `k` is the RRF
    /// damping constant (use `eg_text::RRF_K` = 60 by convention; `0.0` ⇒ that default).
    #[cfg(feature = "text")]
    FuseRrf { branches: Vec<Vec<Op>>, k: f32 },
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
    /// SOURCE (spatial, CONCEPT:EG-083) — seed the RowSet with every node in the spatial
    /// `layer` (a node label / `type`) whose geometry's bounding box intersects `bbox`
    /// (`[minx, miny, maxx, maxy]`). The executor builds eg-geo's packed Hilbert R-tree
    /// over the layer's geometries and runs `query_bbox`, so the returned candidate set
    /// then flows — like any source op — into a downstream spatial `Filter`
    /// (`SpatialWithin`/`SpatialDWithin`) / `Traverse` / `Rank` / `Limit`, composing a
    /// spatial filter with graph + vector in ONE plan. Gated by `geo` (the geometry model
    /// + R-tree live in eg-geo behind eg-plan's own `geo` gate; this is the wire variant).
    #[cfg(feature = "geo")]
    SpatialScan { layer: String, bbox: [f64; 4] },
    /// TRANSFORM (spatial CRS, CONCEPT:EG-255) — reproject each row's stored geometry into
    /// the target CRS `to_epsg`. The SOURCE CRS is the row geometry's EWKT `SRID=…;` tag
    /// when present, else the explicit `from_epsg` override. Rows with no/invalid geometry,
    /// no resolvable source CRS, or an unsupported EPSG code are DROPPED (order-preserving,
    /// exactly as the tensor/spatial legs narrow their input) — the derived geometry
    /// validates the transform per row. The pure-Rust reprojection math (WGS84 / Web-Mercator
    /// / UTM, NO PROJ C dep) lives in eg-geo behind eg-plan's `geo` gate; this is the
    /// CRS-carrying wire variant. Gated by `geo`.
    #[cfg(feature = "geo")]
    Reproject {
        to_epsg: u32,
        #[serde(default)]
        from_epsg: Option<u32>,
    },
    /// TRANSFORM (constructive geometry, CONCEPT:EG-259) — apply the constructive op `kind`
    /// (buffer / convex-hull / simplify / centroid / union / intersection / difference) to
    /// each row's stored geometry, producing a DERIVED geometry per row. Rows whose geometry
    /// is missing/invalid or where the op yields nothing (e.g. an empty intersection) are
    /// DROPPED — order- and score-preserving, exactly as the tensor `TensorOp` leg. The
    /// pure-Rust algebra lives in eg-geo behind eg-plan's `geo` gate; this is the wire
    /// variant. Gated by `geo`.
    #[cfg(feature = "geo")]
    SpatialOp { kind: SpatialOpKind },
    /// SOURCE (tensor, CONCEPT:EG-085) — seed the RowSet with every node in the `layer`
    /// (a node label / `type`) that carries a stored tensor (a dense N-D array in the
    /// conventional `tensor` node property). The returned candidate set then flows —
    /// like any source op — into a downstream `TensorOp` (slice/reduce/elementwise) /
    /// `Traverse` / `Rank` / `Limit`, composing an array modality with graph + vector in
    /// ONE plan. Gated by `tensor` (the N-D array model + ops live in eg-tensor behind
    /// eg-plan's own `tensor` gate; this is the wire variant). The persisted tensors are
    /// content-addressed in the blob CAS per the concept row (`ChunkStore` + EG-071).
    #[cfg(feature = "tensor")]
    TensorScan { layer: String },
    /// TRANSFORM (tensor, CONCEPT:EG-085) — apply the eg-tensor op `kind`
    /// (slice/reduce/elementwise) to each row's stored tensor. Rows whose tensor is
    /// missing/invalid or where the op fails are dropped (order-preserving), exactly as
    /// the spatial `Filter` leg drops rows with no geometry. Gated by `tensor`; the
    /// N-D array math lives in eg-tensor behind eg-plan's `tensor` gate.
    #[cfg(feature = "tensor")]
    TensorOp { kind: TensorOpKind },
    /// TRANSFORM (stream, CONCEPT:EG-088) — run the bounded NFA CEP engine (eg-stream)
    /// over the input RowSet interpreted as a time-ordered event stream: each row's node
    /// blob carries `ts`/`key`/`attrs`, and the op keeps the rows that participate in a
    /// detected match (order-preserving, exactly as the spatial/tensor legs narrow their
    /// input). `pattern` carries the pattern tree (sequence/within/absence) + the
    /// sliding/tumbling `window`. Gated by `stream` (the NFA lives in eg-stream behind
    /// eg-plan's own `stream` gate; this is the wire variant). EG-067 `Op::Window` is the
    /// windowing primitive; a live standing CEP query fed by the EG-064 CDC
    /// `ChangeNotifier` bus is a documented follow-up — the batch `Op::Cep` over a RowSet
    /// is what lands.
    #[cfg(feature = "stream")]
    Cep { pattern: CepPatternSpec },
    /// FUSE (multimodal sensor fusion, CONCEPT:EG-098) — time-align N heterogeneous sensor
    /// `streams` to ONE common clock and emit fused multi-channel rows. Each named stream is
    /// a node layer (label / `type`) whose nodes carry a `valid_from` event time and either a
    /// scalar `value` OR an opaque tensor-blob reference (an EG-085 camera/LiDAR frame). The
    /// executor resolves the streams off the snapshot, calls eg-tsdb's `sensor_fuse` — which
    /// reuses the eg-tsdb ASOF backward-join to carry each stream's latest sample at-or-before
    /// each reference instant, within `tolerance_ns` — and emits one fused row per instant
    /// (id = the aligned ts, score = the count of present, non-gap channels) so a downstream
    /// `Limit`/`Rank` composes over the fused series. `tolerance_ns` is the max staleness (ns)
    /// a channel may carry forward; `0` ⇒ exact-instant matches only. Gated behind
    /// `timeseries`; the variant only exists when eg-types/timeseries is on (pulled by
    /// eg-plan/timeseries), so a non-timeseries build has neither the variant nor its executor
    /// arm (the tensor/stream gating precedent). Composes EG-085 + EG-088 + eg-tsdb ASOF
    /// (EG-067); the alignment math lives in eg-tsdb behind eg-plan's `timeseries` gate — this
    /// is the pure-serde wire variant.
    #[cfg(feature = "timeseries")]
    SensorFuse {
        streams: Vec<String>,
        tolerance_ns: u64,
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

// ── spatial wire-variant round-trip (CONCEPT:EG-083) ─────────────────────────
#[cfg(all(test, feature = "geo"))]
mod geo_tests {
    use super::*;

    /// The spatial Op/Pred variants are pure-serde and round-trip through MessagePack
    /// (the wire format) unchanged — the proof `Method::UnifiedQuery { plan }` can carry
    /// a spatial plan.
    #[test]
    fn spatial_variants_round_trip() {
        let plan = Plan::new(vec![
            Op::SpatialScan {
                layer: "City".into(),
                bbox: [0.0, 0.0, 10.0, 10.0],
            },
            Op::Filter {
                preds: vec![
                    Pred::SpatialWithin {
                        column: "geom".into(),
                        wkt: "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))".into(),
                    },
                    Pred::SpatialDWithin {
                        column: "geom".into(),
                        wkt: "POINT (5 5)".into(),
                        distance: 2.5,
                    },
                ],
            },
            Op::Limit { k: 5 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }

    /// The GIS batch wire variants — CRS reprojection (EG-255), the DE-9IM relation preds
    /// (EG-258), and the constructive `SpatialOp` (EG-259) — all round-trip through
    /// MessagePack unchanged.
    #[test]
    fn gis_batch_variants_round_trip() {
        let plan = Plan::new(vec![
            Op::SpatialScan {
                layer: "Parcel".into(),
                bbox: [0.0, 0.0, 100.0, 100.0],
            },
            Op::Reproject {
                to_epsg: 3857,
                from_epsg: Some(4326),
            },
            Op::Filter {
                preds: vec![
                    Pred::SpatialContains {
                        column: "geom".into(),
                        wkt: "POINT (5 5)".into(),
                    },
                    Pred::SpatialTouches {
                        column: "geom".into(),
                        wkt: "LINESTRING (0 0, 1 1)".into(),
                    },
                    Pred::SpatialOverlaps {
                        column: "geom".into(),
                        wkt: "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))".into(),
                    },
                    Pred::SpatialDisjoint {
                        column: "geom".into(),
                        wkt: "POINT (99 99)".into(),
                    },
                ],
            },
            Op::SpatialOp {
                kind: SpatialOpKind::Buffer { distance: 2.0 },
            },
            Op::SpatialOp {
                kind: SpatialOpKind::ConvexHull,
            },
            Op::SpatialOp {
                kind: SpatialOpKind::Intersection {
                    wkt: "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))".into(),
                },
            },
            Op::Limit { k: 10 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }
}

// ── tensor wire-variant round-trip (CONCEPT:EG-085) ──────────────────────────
#[cfg(all(test, feature = "tensor"))]
mod tensor_tests {
    use super::*;

    /// The tensor Op variants + `TensorOpKind` are pure-serde and round-trip through
    /// MessagePack (the wire format) unchanged — the proof `Method::UnifiedQuery { plan }`
    /// can carry a tensor plan.
    #[test]
    fn tensor_variants_round_trip() {
        let plan = Plan::new(vec![
            Op::TensorScan {
                layer: "Frame".into(),
            },
            Op::TensorOp {
                kind: TensorOpKind::Slice {
                    ranges: vec![(0, 2), (1, 3)],
                },
            },
            Op::TensorOp {
                kind: TensorOpKind::Reduce {
                    axis: 1,
                    kind: TensorReduceKind::Mean,
                },
            },
            Op::TensorOp {
                kind: TensorOpKind::Elementwise {
                    op: TensorElementwiseOp::Mul,
                    scalar: 2.0,
                },
            },
            Op::Limit { k: 5 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }
}

// ── stream / CEP wire-variant round-trip (CONCEPT:EG-088) ─────────────────────
#[cfg(all(test, feature = "stream"))]
mod stream_tests {
    use super::*;

    /// The `Op::Cep` variant + its `CepPatternSpec` tree (sequence/within/absence,
    /// matchers, attribute predicates, window) are pure-serde and round-trip through
    /// MessagePack (the wire format) unchanged — the proof `Method::UnifiedQuery { plan }`
    /// can carry a CEP plan.
    #[test]
    fn cep_variant_round_trips() {
        let matcher = CepMatcherSpec {
            key: Some("trade".into()),
            preds: vec![
                CepAttrPredSpec::Gt {
                    field: "qty".into(),
                    value: 100.0,
                },
                CepAttrPredSpec::Eq {
                    field: "sym".into(),
                    value: serde_json::json!("ACME"),
                },
                CepAttrPredSpec::Exists {
                    field: "venue".into(),
                },
            ],
        };
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::Cep {
                pattern: CepPatternSpec {
                    pattern: CepNodeSpec::Within {
                        within: 30,
                        pattern: Box::new(CepNodeSpec::Sequence(vec![
                            matcher.clone(),
                            CepMatcherSpec {
                                key: Some("cancel".into()),
                                preds: vec![],
                            },
                        ])),
                    },
                    window: CepWindowSpec::Sliding { size: 60 },
                },
            },
            Op::Cep {
                pattern: CepPatternSpec {
                    pattern: CepNodeSpec::Absence {
                        a: matcher,
                        b: CepMatcherSpec {
                            key: Some("ack".into()),
                            preds: vec![CepAttrPredSpec::Lt {
                                field: "latency".into(),
                                value: 5.0,
                            }],
                        },
                        within: 10,
                    },
                    window: CepWindowSpec::Tumbling { size: 100 },
                },
            },
            Op::Limit { k: 5 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }
}

// ── sensor-fusion wire-variant round-trip (CONCEPT:EG-098) ───────────────────
#[cfg(all(test, feature = "timeseries"))]
mod timeseries_tests {
    use super::*;

    /// The `Op::SensorFuse` variant is pure-serde and round-trips through MessagePack
    /// (the wire format) unchanged — the proof `Method::UnifiedQuery { plan }` can carry
    /// a sensor-fusion plan.
    #[test]
    fn sensor_fuse_variant_round_trips() {
        let plan = Plan::new(vec![
            Op::SensorFuse {
                streams: vec!["imu".into(), "gps".into(), "lidar".into()],
                tolerance_ns: 50_000_000,
            },
            Op::Limit { k: 10 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }
}

// ── document/JSON wire-variant round-trip (CONCEPT:EG-084) ───────────────────
#[cfg(all(test, feature = "query"))]
mod docjson_tests {
    use super::*;

    /// CONCEPT:EG-084 — the `Pred::JsonPath` variant + its `JsonPathOp` (existence /
    /// equality / `@>` containment) are pure serde and round-trip through MessagePack
    /// (the wire format) unchanged, so `Method::UnifiedQuery { plan }` can carry a deep
    /// JSON filter.
    #[test]
    fn eg084_jsonpath_pred_round_trips() {
        let plan = Plan::new(vec![
            Op::Filter {
                preds: vec![
                    Pred::JsonPath {
                        path: "$.meta.lang".into(),
                        op: JsonPathOp::Eq {
                            value: serde_json::json!("rust"),
                        },
                    },
                    Pred::JsonPath {
                        path: "$.tags[*]".into(),
                        op: JsonPathOp::Exists,
                    },
                    Pred::JsonPath {
                        path: "$".into(),
                        op: JsonPathOp::Contains {
                            value: serde_json::json!({"meta": {"year": 2024}}),
                        },
                    },
                ],
            },
            Op::Limit { k: 5 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }

    /// CONCEPT:EG-084 — JSON round-trip too (the REST/UQL surface serializes the plan as
    /// JSON), proving the enum tags are stable across both wire encodings.
    #[test]
    fn eg084_jsonpath_pred_json_round_trips() {
        let p = Pred::JsonPath {
            path: "$.a.b".into(),
            op: JsonPathOp::Contains {
                value: serde_json::json!([1, 2, 3]),
            },
        };
        let s = serde_json::to_string(&p).unwrap();
        let back: Pred = serde_json::from_str(&s).unwrap();
        assert_eq!(p, back);
    }
}
