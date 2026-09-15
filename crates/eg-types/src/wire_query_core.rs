//! Core unified-query wire AST types.

#[cfg(feature = "query")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "stream")]
use super::CepPatternSpec;
#[cfg(feature = "federation")]
use super::ForeignSourceSpec;
#[cfg(feature = "probabilistic")]
use super::ProbQuery;
#[cfg(feature = "geo")]
use super::SpatialOpKind;
#[cfg(feature = "tensor")]
use super::TensorOpKind;
#[cfg(feature = "timeseries")]
use super::{FuseClock, FuseStream};

/// A simple equality / range predicate over a node property, compiled to a SQL
/// `WHERE` fragment and evaluated by the DataFusion FILTER leg in `eg-plan`.
#[cfg(feature = "query")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum Pred {
    /// `prop == value` (string compare on the JSON-stringified value).
    Eq { prop: String, value: String },
    /// `prop > n` (numeric).
    GtNum { prop: String, n: f64 },
    /// `prop < n` (numeric).
    LtNum { prop: String, n: f64 },
    /// DOCUMENT/JSON — keep rows whose node property document satisfies a deep
    /// JSONPath predicate (CONCEPT:EG-KG.query.json-wire-roundtrip). `path` is a JSONPath (`$.a.b`, `$.a[0]`,
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
    /// is spatially WITHIN the query geometry `wkt` (CONCEPT:EG-KG.ontology.singles-concept). Evaluated by
    /// eg-geo's planar `within` in eg-plan (behind the `geo` feature) — NOT lowered to
    /// SQL (DataFusion has no spatial), so the FILTER leg splits spatial preds out and
    /// applies them per-row against the stored geometry. Gated by `geo` (implies
    /// `query`); pure serde here (only two strings).
    #[cfg(feature = "geo")]
    SpatialWithin { column: String, wkt: String },
    /// SPATIAL — keep rows whose geometry (node property `column`, WKT) lies within
    /// planar `distance` of the query geometry `wkt` — an `ST_DWithin` (CONCEPT:EG-KG.ontology.singles-concept).
    /// Evaluated by eg-geo's planar `distance` in eg-plan. Gated by `geo`.
    #[cfg(feature = "geo")]
    SpatialDWithin {
        column: String,
        wkt: String,
        distance: f64,
    },
    /// SPATIAL (DE-9IM, CONCEPT:EG-KG.ontology.de-9im-relations) — keep rows whose geometry (node property `column`,
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
/// JSONPath resolves to (CONCEPT:EG-KG.query.json-wire-roundtrip). PURE serde (a small tag + an optional
/// `serde_json::Value` literal); the actual walk/containment lives in
/// `eg_core::jsonpath` behind eg-plan's FILTER leg — this is the wire variant.
#[cfg(feature = "query")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
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
/// Which timeline an [`Op::AsOf`] instant pins (bi-temporal, KG-2.250).
#[cfg(feature = "query")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum Op {
    /// SOURCE — seed from all nodes carrying `label` (`type == label`).
    Scan { label: String },
    /// FILTER (relational) — keep rows matching ALL `preds`, via real DataFusion.
    Filter { preds: Vec<Pred> },
    /// TRAVERSE (graph) — follow `rel` edges `min..=max` hops (petgraph BFS).
    Traverse { rel: String, min: usize, max: usize },
    /// RANK (vector) — re-order by cosine similarity to `query` (SemanticStore kNN).
    Rank { query: Vec<f32> },
    /// RANK (vector-from-TEXT, CONCEPT:EG-KG.compute.no-embedder-bound-op) — like `Rank`, but the query vector is
    /// RESOLVED at exec time from the natural-language `text` by the server-side embedder
    /// bound on the `PlanCtx` (`eg_plan::PlanCtx::with_embedder`). This closes the UQL
    /// `RANK BY ~ "text"` NL→vector seam: the front-end no longer needs the caller to
    /// pre-embed and pass a literal vector. With NO embedder bound the op is a clean typed
    /// ERROR (never a panic) — the documented unbound behavior. Base `query` (the
    /// `TextEmbedder` trait is dep-free; the kNN reuses the SAME `SemanticStore` path as
    /// `Rank`, so a build without an embedder is byte-for-byte unchanged).
    RankEmbed { text: String },
    /// RANK (graph distance, CONCEPT:EG-KG.query.uql-parser-ops) — re-order the candidate set by inverse
    /// shortest-path hop distance from `center` over the graph topology, score
    /// `1/(1+hops)` (unreachable → 0). A graph-NATIVE reranker (Graphiti's
    /// `node_distance`): proximity to a focal node, fused alongside vector/BM25. Reuses
    /// the same BFS the `Traverse` leg uses; dep-free, so it ships under base `query`.
    RankNodeDistance { center: String },
    /// RANK (provenance salience, CONCEPT:EG-KG.query.uql-parser-ops) — re-order the candidate set by how
    /// many edges MENTION each node (incoming-edge count), score normalized to the max
    /// in the set. Graphiti's `episode_mentions` salience: a node many episodes point at
    /// ranks higher. Topology-only, dep-free, base `query`.
    RankMentions {},
    /// RANK (MMR diversity, CONCEPT:AU-KG.retrieval.mmr-diversification) — re-order the candidate set by Maximal
    /// Marginal Relevance: greedily pick the next item maximizing
    /// `lambda*rel - (1-lambda)*max_sim_to_already_picked`, where rel is the item's
    /// incoming relevance score (from a prior `Rank`) and sim is cosine over stored
    /// embeddings. Reduces near-duplicate redundancy in the top-k — the diversity
    /// reranker Graphiti does NOT have. `k` caps how many to re-rank (0 ⇒ all). Reads
    /// `ctx.semantic` (always present), so base `query`.
    RankMmr { lambda: f32, k: usize },
    /// RANK (lexical, BM25) — re-order the candidate set by BM25 relevance to the
    /// natural-language `query` string over the text index (CONCEPT:AU-KG.query.text-spatial-time). A
    /// sibling of the vector `Rank`: it produces a score-per-id over the SAME RowSet
    /// currency, so the closed algebra is unchanged. Gated by `text` (the Tantivy
    /// index lives in eg-text behind its own gate; this is just the wire variant).
    #[cfg(feature = "text")]
    RankText { query: String },
    /// FUSE (hybrid) — reciprocal-rank-fusion of N SUB-PLAN `branches` over the SAME
    /// seed into ONE ranked RowSet (CONCEPT:AU-KG.query.text-spatial-time / KG-2.253). The modern hybrid-
    /// retrieval pattern, generalized past two legs: fuse the RANKS (not the
    /// incomparable BM25/cosine/distance scores) so a doc strong across MORE branches
    /// out-ranks one strong in only one. The canonical tri-modal hybrid is
    /// `branches = [[Rank{vec}], [RankText{q}], [RankNodeDistance{c}]]`. `k` is the RRF
    /// damping constant (use `eg_text::RRF_K` = 60 by convention; `0.0` ⇒ that default).
    #[cfg(feature = "text")]
    FuseRrf { branches: Vec<Vec<Op>>, k: f32 },
    /// SOURCE (semantic, OWL) — seed the RowSet with every individual the native OWL 2
    /// reasoner INFERS to be a member of `target_class` (CONCEPT:EG-KG.ontology.incremental-materialization/220). The
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
    /// the result of the SPARQL `query` (a basic graph pattern, CONCEPT:EG-KG.ontology.concept-12),
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
    /// WebAssembly user function (CONCEPT:EG-KG.query.rowset-execution). The executor serializes the input
    /// rows (ids + scores) to bytes, runs the wasm module `id` under fuel + memory
    /// limits with NO host capabilities, and deserializes the returned rows back into
    /// the pipeline. A pure `RowSet -> RowSet` op like every other, so a UDF composes
    /// with Scan/Filter/Traverse/Rank/Limit. Gated by `wasm-udf` (the wasmtime runtime
    /// lives in eg-wasm behind its own gate; this is just the wire variant).
    #[cfg(feature = "wasm-udf")]
    Udf { id: String },
    /// SOURCE (federation) — read rows from an EXTERNAL source and seed the RowSet
    /// (CONCEPT:EG-KG.query.query-federation, Lane P). `source` is either a REMOTE epistemic-graph engine
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
    /// clause (CONCEPT:EG-KG.query.sparql-completeness) instead lowers to the lighter [`Op::Foreign`] name
    /// MARKER below — the parser only has a name, and resolving that name to a concrete
    /// `ForeignSourceSpec` goes through the server-side `foreign_sources` map, which the
    /// served query handler turns into eg-plan's own `ForeignSourceRegistry` and binds
    /// onto the `PlanCtx` (CONCEPT:EG-KG.query.closure-backed-source). The two are
    /// complementary: `Foreign`
    /// is the named-reference surface, `ForeignScan` is the resolved executor a
    /// server/planner constructs.
    #[cfg(feature = "federation")]
    ForeignScan {
        source: Box<ForeignSourceSpec>,
        /// When this `ForeignScan` is NOT the first op, intersect its rows with the
        /// current candidate set (a foreign∩local JOIN keyed on id) instead of
        /// replacing the input. The default (false) makes it a pure source. The
        /// preceding rows' ORDER is preserved on an intersect.
        #[serde(default)]
        join: bool,
    },
    /// TIME (`AS OF [TX] @<ts>`, CONCEPT:EG-KG.query.sparql-completeness / KG-2.250) — pin the RowSet to a
    /// point-in-time `ts` (unix seconds) and DROP rows not live at that instant. A
    /// RowSet-narrowing temporal filter executed in `eg-plan` (dep-free blob scan, no
    /// DataFusion — Pi-safe). `axis` selects the timeline: `Valid` = "what was TRUE at
    /// ts" (`valid_from`/`valid_until`); `Transaction` = "what we BELIEVED at ts"
    /// (`tx_from`/`tx_to`). The axis is explicit in the current plan contract.
    AsOf { ts: f64, axis: TimeAxis },
    /// TIME (`WINDOW <dur>`, CONCEPT:EG-KG.query.sparql-completeness) — declare a trailing time window of
    /// `secs` seconds for the windowed time-series aggregate. A RowSet-preserving
    /// CONTEXT op paired with `AsOf`; passes the rows through unchanged today (the
    /// windowed aggregate is the eg-tsdb seam) but lets the `WINDOW <dur>` UQL clause
    /// lower to ONE plan AST. Always available under `query`.
    ///
    /// As of CONCEPT:EG-KG.compute.tsscan-series-window-60s the executor no longer merely passes the rows through: an
    /// `Op::Window` over a RowSet of `(ts, value)` rows (e.g. from `Op::TsScan`, or any
    /// scored row) now emits a REAL tumbling windowed aggregate (MEAN, via eg-tsdb's
    /// `time_bucket`) — one row per non-empty bucket (`id` = the aligned bucket start,
    /// `score` = the aggregate) — composing downstream into `Rank`/`Limit`. Under a
    /// non-`timeseries` build the op keeps its RowSet-preserving passthrough behavior.
    Window { secs: f64 },
    /// TIME (`WINDOW <dur> <agg>`, CONCEPT:EG-KG.compute.trailing-aggregate-selector-lowers) — the SELECTABLE-aggregate form of
    /// `Op::Window`: a real tumbling windowed aggregate over `(ts, value)` rows whose
    /// aggregate function `agg` (one of `mean`/`avg`, `sum`, `min`, `max`, `count`,
    /// `first`, `last`; unknown ⇒ `mean`) is resolved to an `eg_tsdb::query::Agg`. Emits
    /// one row per non-empty bucket (`id` = aligned bucket start, `score` = the aggregate),
    /// composing downstream exactly like `Window`. Base `query` (the eg-tsdb aggregate is
    /// only wired under `timeseries`; a non-`timeseries` build passes the rows through, as
    /// `Window` does).
    WindowAgg { secs: f64, agg: String },
    /// FEDERATION (`FOREIGN "<name>"`, CONCEPT:EG-KG.query.sparql-completeness) — replace the seed with rows from
    /// the registered foreign source `name`. The plan fails when no registry/source is
    /// bound; foreign intent is never ignored. Always available under `query`, while
    /// execution requires the `federation` feature. The inline-spec counterpart is the
    /// `federation`-gated [`Op::ForeignScan`] above.
    Foreign { name: String },
    /// SOURCE (spatial, CONCEPT:EG-KG.ontology.singles-concept) — seed the RowSet with every node in the spatial
    /// `layer` (a node label / `type`) whose geometry's bounding box intersects `bbox`
    /// (`[minx, miny, maxx, maxy]`). The executor builds eg-geo's packed Hilbert R-tree
    /// over the layer's geometries and runs `query_bbox`, so the returned candidate set
    /// then flows — like any source op — into a downstream spatial `Filter`
    /// (`SpatialWithin`/`SpatialDWithin`) / `Traverse` / `Rank` / `Limit`, composing a
    /// spatial filter with graph + vector in ONE plan. Gated by `geo` (the geometry model
    /// + R-tree live in eg-geo behind eg-plan's own `geo` gate; this is the wire variant).
    #[cfg(feature = "geo")]
    SpatialScan { layer: String, bbox: [f64; 4] },
    /// TRANSFORM (spatial CRS, CONCEPT:EG-KG.domains.coordinate-reference-system) — reproject each row's stored geometry into
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
    /// TRANSFORM (constructive geometry, CONCEPT:EG-KG.ontology.concept-9) — apply the constructive op `kind`
    /// (buffer / convex-hull / simplify / centroid / union / intersection / difference) to
    /// each row's stored geometry, producing a DERIVED geometry per row. Rows whose geometry
    /// is missing/invalid or where the op yields nothing (e.g. an empty intersection) are
    /// DROPPED — order- and score-preserving, exactly as the tensor `TensorOp` leg. The
    /// pure-Rust algebra lives in eg-geo behind eg-plan's `geo` gate; this is the wire
    /// variant. Gated by `geo`.
    #[cfg(feature = "geo")]
    SpatialOp { kind: SpatialOpKind },
    /// SOURCE (tensor, CONCEPT:EG-KG.storage.content-addressed-dedup) — seed the RowSet with every node in the `layer`
    /// (a node label / `type`) that carries a stored tensor (a dense N-D array in the
    /// conventional `tensor` node property). The returned candidate set then flows —
    /// like any source op — into a downstream `TensorOp` (slice/reduce/elementwise) /
    /// `Traverse` / `Rank` / `Limit`, composing an array modality with graph + vector in
    /// ONE plan. Gated by `tensor` (the N-D array model + ops live in eg-tensor behind
    /// eg-plan's own `tensor` gate; this is the wire variant). The persisted tensors are
    /// content-addressed in the blob CAS per the concept row (`ChunkStore` + EG-071).
    #[cfg(feature = "tensor")]
    TensorScan { layer: String },
    /// TRANSFORM (tensor, CONCEPT:EG-KG.storage.content-addressed-dedup) — apply the eg-tensor op `kind`
    /// (slice/reduce/elementwise) to each row's stored tensor. Rows whose tensor is
    /// missing/invalid or where the op fails are dropped (order-preserving), exactly as
    /// the spatial `Filter` leg drops rows with no geometry. Gated by `tensor`; the
    /// N-D array math lives in eg-tensor behind eg-plan's `tensor` gate.
    #[cfg(feature = "tensor")]
    TensorOp { kind: TensorOpKind },
    /// TRANSFORM (stream, CONCEPT:EG-KG.query.pipelined-execution) — run the bounded NFA CEP engine (eg-stream)
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
    /// FUSE (multimodal sensor fusion, CONCEPT:EG-KG.query.multi-rate-sensor-stream) — time-align N heterogeneous sensor
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
    /// FUSE (multimodal sensor fusion on a DECLARED clock, CONCEPT:EG-KG.query.multi-rate-sensor-stream) —
    /// the fixed-grid / tumbling-window sibling of `Op::SensorFuse`. Where `SensorFuse` fuses
    /// onto the UNION clock of the samples (data-driven instants, ASOF/tolerance only), this
    /// op resamples every stream onto the time base the caller DECLARES in `clock`
    /// ([`FuseClock::Uniform`] grid or [`FuseClock::Tumbling`] EG-067 windows), each stream
    /// under its OWN [`FuseInterp`] mode (`Nearest` / `Linear` / `AsofHold`) — so the output
    /// instants, and the values at them, are independent of when the sensors happened to fire.
    /// That is a different semantics, not a different spelling: a `Linear` channel yields a
    /// genuinely INTERPOLATED reading at a grid instant no sample sits on, which ASOF cannot
    /// produce.
    ///
    /// A SOURCE op: it resolves its streams off the snapshot and REPLACES the input, exactly
    /// like `SensorFuse` / `TensorScan` / `SpatialScan`. The executor stacks the aligned
    /// channels into a `[timesteps × channels]` eg-tensor frame + validity mask
    /// (`eg_tensor::fusion`) and projects ONE row per clock instant: `id` = the instant (or
    /// window start), `score` = the PRIMARY channel's (stream 0) fused reading — the
    /// `Op::TsScan` "field 0" projection lifted onto the fused frame — or `None` where that
    /// channel gapped. An instant at which EVERY channel is a gap emits NO row, so the
    /// validity mask is what decides emission.
    ///
    /// `tolerance_ns` bounds per-channel staleness (`None` = unbounded): a `Nearest`/
    /// `AsofHold` match farther away than it, or a `Linear` bracket wider than it, is a GAP.
    ///
    /// Gated behind `timeseries` (the SensorFuse gating precedent); the alignment math lives
    /// in `eg_tsdb::fusion` and the tensor stacking in `eg_tensor::fusion`, both behind
    /// eg-plan's `timeseries` gate — this is the pure-serde wire variant.
    #[cfg(feature = "timeseries")]
    SensorAlign {
        streams: Vec<FuseStream>,
        clock: FuseClock,
        tolerance_ns: Option<u64>,
    },
    /// SOURCE (time-series, CONCEPT:EG-KG.query.native-time-series) — seed the RowSet from native TSDB series.
    /// Scans each series in `series` for points in the `[from, to)` timestamp window and
    /// emits them as rows so a downstream `Rank`/`Limit`/`Filter` composes the tsdb leg
    /// with the graph/vector/relational legs in ONE plan (tsdb-in-plan fusion). Bounds
    /// are `f64` seconds (uniform with the other numeric plan ops); the executor lowers
    /// them to the eg-tsdb `SeriesStore` ns range internally. Gated behind `timeseries`;
    /// the variant only exists when eg-types/timeseries is on (pulled by
    /// eg-plan/timeseries), so a non-timeseries build has neither the variant nor its
    /// executor arm (the `SensorFuse` gating precedent). The scan implementation lives in
    /// eg-plan behind its `timeseries` gate; this is the pure-serde wire variant.
    #[cfg(feature = "timeseries")]
    TsScan {
        series: Vec<String>,
        from: f64,
        to: f64,
    },
    /// TRANSFORM (probabilistic, CONCEPT:EG-KG.compute.uncertainty-values) — run the probabilistic query `query`
    /// against each row's stored `Distribution` VALUE (the conventional `distribution`
    /// node property, the tagged serde form of `eg_types::Distribution`) and SCORE the
    /// row with the closed-form result — expectation / marginal probability / conditional
    /// posterior mean / deterministic seeded sample — re-ordering the RowSet by that score
    /// DESCENDING, exactly as `Rank` produces a scored order a downstream `Limit` respects.
    /// Rows whose `distribution` is missing/invalid, or where the query does not apply
    /// (e.g. a `Conditional` with an unsupported conjugate pair), are DROPPED — mirroring
    /// how the tensor/spatial legs narrow their input. Gated by `probabilistic` (the
    /// distribution + Bayesian math lives in eg-types/eg-compute behind eg-plan's own
    /// `probabilistic` gate; this is the pure-serde wire variant — no RNG-from-clock, so a
    /// seeded `Sample` is reproducible). Mirrors the `tensor`/`stream` gating precedent.
    #[cfg(feature = "probabilistic")]
    Probabilistic { query: ProbQuery },
    /// SOURCE/FILTER (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) — the evidence
    /// FOR `claim_id`: nodes linked to it by an INCOMING `SUPPORTS`/`SUPPORTS_BELIEF`/
    /// `HAS_EVIDENCE`/`CORROBORATES` edge (classified by `eg_epistemic::classify_relationship`).
    /// As a SOURCE (empty input) it seeds the RowSet with the supporting ids; mid-pipeline it
    /// narrows the candidate set to the ones that ALSO support `claim_id` — the same
    /// seed-or-filter shape as `Op::AsOf`. Gated by `epistemic` (the belief-substrate model
    /// lives in eg-epistemic behind eg-plan's own `epistemic` gate; this is the wire variant).
    #[cfg(feature = "epistemic")]
    EvidenceFor { claim_id: String },
    /// FILTER/SOURCE (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) — evidence
    /// AGAINST `node_id`: nodes linked to it by an INCOMING `CONTRADICTS`/`CONTRADICTS_BELIEF`/
    /// `REFUTES` OR `ATTACKS`/`DEFEATS`/`UNDERCUTS` edge (an attack is a stronger contradiction,
    /// so both count). Same seed-or-filter shape as `EvidenceFor`. Gated by `epistemic`.
    #[cfg(feature = "epistemic")]
    Contradicts { node_id: String },
    /// FILTER/SOURCE (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) — the claims
    /// `node_id` itself supports: nodes reached by an OUTGOING `SUPPORTS`/`SUPPORTS_BELIEF`/
    /// `HAS_EVIDENCE`/`CORROBORATES` edge FROM `node_id` — the mirror direction of
    /// `EvidenceFor`. Same seed-or-filter shape. Gated by `epistemic`.
    #[cfg(feature = "epistemic")]
    SupportedBy { node_id: String },
    /// TIME+TRANSFORM (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) — pin the
    /// candidate set to what the engine BELIEVED at transaction-time `ts` (composes the
    /// existing `Op::AsOf { ts, axis: Transaction }` bi-temporal filter) and then RE-SCORE
    /// each surviving row by its propagated belief confidence at that instant
    /// (`eg_epistemic::propagate_confidence`). The UQL sibling `VALID AS OF <ts>` is a pure
    /// ALIAS for `Op::AsOf { axis: Valid }` (no belief propagation) — this op is the one that
    /// actually walks the support/contradiction/attack graph. Gated by `epistemic`.
    #[cfg(feature = "epistemic")]
    BeliefAsOf { ts: f64 },
    /// TRANSFORM (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) — re-weight every
    /// row currently in the RowSet by the propagated reliability (belief confidence) of the
    /// named `source_id` node, via `eg_epistemic::propagate_confidence`. Represents "discount
    /// this candidate set by how much I trust source X" — a uniform scalar multiplier over
    /// existing scores (unscored rows are treated as score `1.0`). Gated by `epistemic`.
    #[cfg(feature = "epistemic")]
    SourceReliability { source_id: String },
    /// TRANSFORM (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) — re-score EACH
    /// row in the current RowSet by ITS OWN propagated belief confidence
    /// (`eg_epistemic::propagate_confidence` walking that row's own support/contradiction/
    /// attack neighbourhood), re-ordering descending — the `CONFIDENCE` UQL keyword with no
    /// argument. Mirrors `Op::Probabilistic`'s "score-then-rank" shape but over the belief
    /// graph instead of a stored `Distribution`. Gated by `epistemic`.
    #[cfg(feature = "epistemic")]
    ConfidenceOp {},
    /// TRANSFORM (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) — the answer to
    /// `EXPLAIN BELIEF <node_id>`: build the recursive justification tree
    /// (`eg_epistemic::explain_belief`) rooted at `node_id`, flatten it (pre-order, deduped) to
    /// `(claim_id, confidence)` pairs, and either SEED the RowSet with them (empty input) or
    /// narrow+re-score the current candidate set to the ones that appear in the tree (mirrors
    /// the `EvidenceFor`/`Contradicts` seed-or-filter shape). The FULL nested proof tree
    /// (rule names, premise structure) is NOT representable in the flat `RowSet` currency —
    /// this plan-Op surface is the queryable (composes-with-`Traverse`/`Rank`/`Limit`)
    /// projection of it; a standalone `Method::ExplainBelief` returning the tree verbatim
    /// (mirroring `Method::OwlExplain`'s `ProofNodeWire`) is a documented follow-up, not
    /// built here (E2 scope is the wire+UQL plan surface). Gated by `epistemic`.
    #[cfg(feature = "epistemic")]
    ExplainBelief { node_id: String },
    /// LIMIT — top-k, respecting the current order.
    Limit { k: usize },
}

/// A logical plan: an ordered list of [`Op`]s over one `RowSet`. The serializable
/// wire payload of `Method::UnifiedQuery` (CONCEPT:AU-KG.compute.vector).
#[cfg(feature = "query")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct Plan {
    pub ops: Vec<Op>,
}

#[cfg(feature = "query")]
impl Plan {
    pub fn new(ops: Vec<Op>) -> Self {
        Self { ops }
    }
}
