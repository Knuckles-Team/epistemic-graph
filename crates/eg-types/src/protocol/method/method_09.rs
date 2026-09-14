macro_rules! __eg_method_chunk_9 {
    () => {
        __eg_method_chunk_10!(@acc [


    /// Sequential-pattern mining (CONCEPT:EG-KG.mining.prefixspan — Phase 4).
    /// Finds frequent ORDERED subsequences (PrefixSpan or GSP; both agree) over
    /// EITHER explicit `sequences` (each a time-ordered list of item labels — an
    /// item may repeat) OR a graph-derived `source` that turns each node's
    /// ordered neighbor list (following resident edge insertion order) into one
    /// sequence — the "what reliably follows what" hook (evolution/commit
    /// timelines, event streams). Returns rows `{items, support, count}`. With
    /// `writeback=true` it materializes each pattern as a typed
    /// `:SequentialPattern` node linked to any item that is a resident node — a
    /// graph MUTATION, WAL-replayed by re-mining deterministically. Gated
    /// `mining`.
    #[cfg(feature = "mining")]
    MineSequence {
        /// Explicit ordered sequences — each a time-ordered list of item labels.
        /// Empty ⇒ use `source`.
        #[serde(default)]
        sequences: Vec<Vec<String>>,
        /// Graph-derived sequence source (compute-near-data). Used when
        /// `sequences` is empty.
        #[serde(default)]
        source: Option<SequenceSource>,
        /// Minimum fractional support (0.0–1.0) a pattern must meet.
        #[serde(default = "default_min_support")]
        min_support: f64,
        /// Which sequential-pattern engine to run (both agree; PrefixSpan default).
        #[serde(default)]
        algorithm: MineSeqAlgorithm,
        /// Materialize each pattern as a typed `:SequentialPattern` node linked to
        /// its resident item nodes.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per pattern (E6) —
        /// see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the
        /// pattern's support. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    /// Classical time-series forecasting (CONCEPT:EG-KG.mining.arima — Phase 4).
    /// Forecasts `horizon` future points (with an approximate confidence band)
    /// from a 1-D `values` series — a tsdb window handed in by the caller,
    /// mirroring `MineAnomaly`'s client-supplied `values` cut (the native
    /// in-handler TsScan source is the same documented follow-up). `algorithm`
    /// selects ARIMA(p,d,q) (Hannan-Rissanen), additive Holt-Winters/ETS
    /// (degrades to Holt linear-trend when `period` is 0), or a classical STL-
    /// style decomposition + trend/seasonal extrapolation. With
    /// `writeback=true` it materializes the forecast as a typed `:Forecast`
    /// node — linked to a resident node named `series_id` when one exists — a
    /// graph MUTATION, WAL-replayed by re-forecasting deterministically. Gated
    /// `mining`.
    #[cfg(feature = "mining")]
    MineForecast {
        /// The 1-D series to forecast (required — a tsdb window handed in by
        /// the caller).
        #[serde(default)]
        values: Vec<f64>,
        /// Which forecasting engine to run.
        #[serde(default)]
        algorithm: ForecastAlgorithm,
        /// Steps to forecast beyond the series.
        #[serde(default = "default_horizon")]
        horizon: usize,
        /// ARIMA autoregressive order.
        #[serde(default = "default_arima_p")]
        p: usize,
        /// ARIMA differencing order.
        #[serde(default = "default_arima_d")]
        d: usize,
        /// ARIMA moving-average order.
        #[serde(default)]
        q: usize,
        /// Seasonal period for Holt-Winters / STL (`0` ⇒ non-seasonal Holt
        /// linear-trend fallback for Holt-Winters; trend-only for STL).
        #[serde(default)]
        period: usize,
        /// Holt-Winters level smoothing.
        #[serde(default = "default_hw_alpha")]
        alpha: f64,
        /// Holt-Winters trend smoothing.
        #[serde(default = "default_hw_beta")]
        beta: f64,
        /// Holt-Winters seasonal smoothing.
        #[serde(default = "default_hw_gamma")]
        gamma: f64,
        /// Two-sided confidence level for the forecast band (e.g. `0.95`).
        #[serde(default = "default_confidence")]
        confidence: f64,
        /// Optional identity for the write-back `:Forecast` node; when it names
        /// a resident node, the forecast is linked `FORECAST_OF` → that node.
        /// Empty ⇒ the node id is derived from the input `values` + `algorithm`.
        #[serde(default)]
        series_id: String,
        /// Materialize the forecast as a typed `:Forecast` node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the forecast (E6)
        /// — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the
        /// forecast's `confidence` band level. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    /// Text mining (CONCEPT:EG-KG.mining.tfidf — Phase 4). `tfidf` returns each
    /// document's term weights (descriptive, read-only); `lda`/`nmf` fit a
    /// `k`-topic model over the corpus. Documents come from EITHER explicit
    /// `docs` (each a pre-tokenized `Vec<String>` — use `tokenize`-equivalent
    /// client-side, or pass raw words) OR a graph-derived `source` that
    /// tokenizes a text property off a node label (compute-near-data — no
    /// Tantivy/eg-text dependency). With `writeback=true` (`lda`/`nmf` only)
    /// each topic is materialized as a typed `:Topic` node, linked to any
    /// source document that is a resident node — a graph MUTATION,
    /// WAL-replayed by re-mining deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineText {
        /// Explicit pre-tokenized documents. Empty ⇒ use `source`.
        #[serde(default)]
        docs: Vec<Vec<String>>,
        /// Graph-derived text source (compute-near-data). Used when `docs` is
        /// empty.
        #[serde(default)]
        source: Option<TextSource>,
        /// Which text-mining engine to run.
        #[serde(default)]
        algorithm: TextAlgorithm,
        /// Topic count for `lda`/`nmf`.
        #[serde(default = "default_topic_k")]
        k: usize,
        /// LDA symmetric doc-topic Dirichlet prior.
        #[serde(default = "default_lda_alpha")]
        alpha: f64,
        /// LDA symmetric topic-term Dirichlet prior.
        #[serde(default = "default_lda_beta")]
        beta: f64,
        /// Gibbs sweeps (`lda`) / multiplicative-update iterations (`nmf`).
        #[serde(default = "default_text_iterations")]
        iterations: usize,
        /// Seed for LDA's Gibbs sampler / NMF's initial factors (deterministic).
        #[serde(default)]
        seed: u64,
        /// How many terms to keep per document/topic row.
        #[serde(default = "default_top_n")]
        top_n: usize,
        /// Materialize each topic as a typed `:Topic` node (`lda`/`nmf` only —
        /// a no-op for `tfidf`, which has no topics to write back).
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per topic (D3, mirroring
        /// E6) — `lda`/`nmf` only, a no-op for `tfidf` (which has no topics, mirroring
        /// `writeback`). Quality = the topic's mean doc-membership strength among the
        /// documents DOMINANTLY assigned to it (`mean(doc_topics[d][t])` over docs `d`
        /// whose argmax topic is `t`) — a topic-coherence proxy: both LDA's Dirichlet
        /// posterior and NMF's row-normalized `W` are already `[0,1]` distributions that
        /// sum to 1 across topics (see `eg_compute::mining::text` module docs), so this
        /// is a principled, already-bounded score requiring no extra normalization.
        /// Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    /// Frequent subgraph mining + motif counting (CONCEPT:EG-KG.mining.gspan-frequent-subgraph
    /// — Phase 4, the graph-native differentiator). UNLIKE every other mining
    /// op in this family, this one mines the RESIDENT GRAPH's own topology
    /// directly — no rows/vectors handed in. `gspan` finds frequent connected
    /// subgraph PATTERNS (level-wise growth up to `max_edges` edges,
    /// canonicalized + exactly re-counted); `motif` censuses small
    /// label-agnostic topological motifs (wedges, triangles, directed
    /// 3-cycles). `label`, when given, restricts the scanned host graph to
    /// nodes of that one type (both edge endpoints must match) — `None` scans
    /// the whole resident graph heterogeneously. With `writeback=true`
    /// (`gspan` only) each frequent pattern is materialized as a typed
    /// `:FrequentSubgraph` node, linked to every host node appearing in any of
    /// its embeddings — a graph MUTATION, WAL-replayed by re-mining
    /// deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineSubgraph {
        /// Optional: restrict the host graph to nodes of this one type.
        /// `None` ⇒ the whole resident graph (heterogeneous).
        #[serde(default)]
        label: Option<String>,
        /// Minimum fractional support (0.0–1.0, of the host's total edge
        /// count) a pattern's embedding count must meet. Ignored by `motif`.
        #[serde(default = "default_min_support")]
        min_support: f64,
        /// Pattern-size growth cap (tractability). Ignored by `motif`.
        #[serde(default = "default_max_subgraph_edges")]
        max_edges: usize,
        /// Which algorithm to run.
        #[serde(default)]
        algorithm: SubgraphAlgorithm,
        /// Materialize each frequent pattern as a typed `:FrequentSubgraph`
        /// node (`gspan` only — a no-op for `motif`, which has no patterns).
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per frequent pattern
        /// (E6, `gspan` only) — see [`Method::MineAssociate::as_claim`]. Confidence
        /// is seeded from the pattern's support. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    // ── Residual insight/mining families (Gap-5) ──────────────────────
    // Rounds out the mining surface begun by `MineAssociate` above with 8 more
    // families, each following the SAME shape (explicit-or-graph-derived input,
    // optional `writeback` of a typed node, optional `as_claim` epistemic
    // writeback gated `all(mining, epistemic)`).
    /// Entity resolution + record linkage (CONCEPT:EG-KG.mining.entity-resolution).
    /// DISTINCT from the existing always-on `ResolveCandidates` op (all-pairs
    /// cosine + union-find dedup-ladder proposals, no epistemic writeback): this
    /// mining family instead supports BOTH Jaccard record linkage over token
    /// attributes (`records`, blocked by an explicit `block_keys`) AND cosine
    /// entity resolution over embeddings (`vectors`/`source`, blocked by a grid
    /// bucket), and materializes each match as a typed `:EntityMatch` node — a
    /// graph MUTATION, WAL-replayed by re-resolving deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineEntityResolve {
        /// Token-attribute records (Jaccard record linkage). Empty ⇒ use
        /// `vectors`/`source`.
        #[serde(default)]
        records: Vec<Vec<String>>,
        /// Blocking key per record, same length as `records`. All-empty-string
        /// (or shorter than `records`) ⇒ one global block (no blocking).
        #[serde(default)]
        block_keys: Vec<String>,
        /// Explicit embedding rows (cosine entity resolution). Used when
        /// `records` is empty; empty ⇒ use `source`.
        #[serde(default)]
        vectors: Vec<Vec<f64>>,
        /// Graph-derived vector source (node embeddings) — used when `records`
        /// and `vectors` are both empty.
        #[serde(default)]
        source: Option<VectorSource>,
        /// Optional external ids parallel to `records`/`vectors` (the explicit
        /// paths only — `source` supplies its own resident node ids). Shorter
        /// than the input ⇒ missing entries fall back to their index.
        #[serde(default)]
        ids: Vec<String>,
        /// Grid-bucket rounding precision for the `vectors`/`source` blocking path.
        #[serde(default = "default_bucket_precision")]
        bucket_precision: i32,
        /// Minimum similarity (Jaccard or Cosine, `[0,1]`) to emit a match.
        #[serde(default = "default_match_threshold")]
        threshold: f64,
        /// Materialize each match as a typed `:EntityMatch` node linked to both
        /// members (when they are resident node ids).
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per match (E6) —
        /// see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the
        /// match's OWN similarity. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    /// Causal impact estimation (CONCEPT:EG-KG.mining.causal-impact): interrupted
    /// time series (a single `series`) or difference-in-differences (`series` +
    /// non-empty `control`), split at `intervention_index`. Mirrors
    /// `MineForecast`'s "caller hands in the tsdb window" convention — no direct
    /// tsdb coupling. With `writeback=true` materializes a typed `:CausalEffect`
    /// node — a graph MUTATION, WAL-replayed by re-estimating deterministically.
    /// Gated `mining`.
    #[cfg(feature = "mining")]
    MineCausalImpact {
        /// The (treatment, for DiD) series to analyze — required.
        #[serde(default)]
        series: Vec<f64>,
        /// The control series for difference-in-differences. Empty ⇒ plain
        /// interrupted-time-series (no control).
        #[serde(default)]
        control: Vec<f64>,
        /// Index of the FIRST post-intervention observation (in BOTH series for DiD).
        #[serde(default)]
        intervention_index: usize,
        /// Optional identity for the write-back `:CausalEffect` node. Empty ⇒
        /// derived from the input series + algorithm.
        #[serde(default)]
        series_id: String,
        /// Materialize the estimate as a typed `:CausalEffect` node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the estimate
        /// (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded
        /// from the estimate's own significance (`1 - two_sided_p`). Requires
        /// `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    /// Process mining (CONCEPT:EG-KG.mining.process-mining): directly-follows
    /// graph + alpha-miner-lite footprint (causal / parallel / choice relations,
    /// start/end activity sets) over ordered event `traces`. With
    /// `writeback=true` materializes the footprint as a typed `:ProcessModel`
    /// node — a graph MUTATION, WAL-replayed by re-mining deterministically.
    /// Gated `mining`.
    #[cfg(feature = "mining")]
    MineProcess {
        /// Ordered activity-label traces — each a time-ordered event sequence
        /// (an activity may repeat within a trace). Required.
        #[serde(default)]
        traces: Vec<Vec<String>>,
        /// Optional identity for the write-back `:ProcessModel` node. Empty ⇒
        /// derived from the mined footprint's own shape.
        #[serde(default)]
        process_id: String,
        /// Materialize the footprint as a typed `:ProcessModel` node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the model (E6)
        /// — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from
        /// the fraction of observed activity pairs classified `causal`/`parallel`
        /// (vs. `choice`) — a log-coverage proxy, already `[0,1]`. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    /// Root-cause propagation (CONCEPT:EG-KG.mining.root-cause): given a directed
    /// weighted dependency graph (`edges`, `cause -> effect`) and a per-node
    /// anomaly `scores` vector (the existing `anomaly` family's own output, or
    /// any other score), find the most-likely upstream root cause of one
    /// `symptom` node. With `writeback=true` materializes the top candidate as a
    /// typed `:RootCause` node linked to the symptom — a graph MUTATION,
    /// WAL-replayed by re-searching deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineRootCause {
        /// Node ids, index-aligned with `scores` and referenced by `edges`.
        #[serde(default)]
        nodes: Vec<String>,
        /// Anomaly score per node, index-aligned with `nodes` (negative clamps to `0.0`).
        #[serde(default)]
        scores: Vec<f64>,
        /// Dependency edges `(cause_id, effect_id, weight)`; `weight` clamped to `[0,1]`.
        #[serde(default)]
        edges: Vec<(String, String, f64)>,
        /// The already-flagged anomalous node whose root cause to find (required).
        #[serde(default)]
        symptom: String,
        /// Search depth cap.
        #[serde(default = "default_max_hops")]
        max_hops: usize,
        /// Per-hop score decay `(0,1]` (mirrors PageRank's damping factor).
        #[serde(default = "default_decay")]
        decay: f64,
        /// Materialize the top candidate as a typed `:RootCause` node linked to the symptom.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the top
        /// candidate (E6) — see [`Method::MineAssociate::as_claim`]. Confidence
        /// mirrors `anomaly`'s `score / (1 + score)` mapping over the candidate's
        /// OWN raw responsibility score (normalizing against the candidate list
        /// would be trivially `1.0` for the top candidate). Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    /// Seeded risk propagation (CONCEPT:EG-KG.mining.risk-propagation): personalized
    /// PageRank over a directed weighted graph (`edges`), restarting to a `seed`
    /// risk distribution instead of teleporting uniformly. With `writeback=true`
    /// materializes each node's propagated score as a typed `:RiskScore` node — a
    /// graph MUTATION, WAL-replayed by re-propagating deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineRiskPropagation {
        /// Node ids, index-aligned with `seed` and referenced by `edges`.
        #[serde(default)]
        nodes: Vec<String>,
        /// Seed risk per node, index-aligned with `nodes` (any non-negative
        /// scale — normalized internally; all-zero ⇒ all-zero result).
        #[serde(default)]
        seed: Vec<f64>,
        /// Weighted directed edges `(from_id, to_id, weight)`; `weight` clamped `>= 0`.
        #[serde(default)]
        edges: Vec<(String, String, f64)>,
        /// Damping factor (probability of following an edge vs. restarting to `seed`).
        #[serde(default = "default_damping")]
        damping: f64,
        /// L1 convergence tolerance.
        #[serde(default = "default_risk_tolerance")]
        tolerance: f64,
        /// Hard iteration cap.
        #[serde(default = "default_max_iter")]
        max_iterations: usize,
        /// Materialize each node's propagated score as a typed `:RiskScore` node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per scored node (E6)
        /// — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the
        /// node's own propagated share (already `[0,1]`, mass-conserving). Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    /// Ontology-gap detection (CONCEPT:EG-KG.mining.ontology-gap): scans the
    /// resident graph's own node-`type`/edge-`relationship` class shape (GRAPH-NATIVE —
    /// no `rdf`/OWL-reasoner dependency) for completeness gaps: no declared
    /// properties, an unresolved `subClassOf` parent (an orphan subclass), or a
    /// fully disconnected class. With `writeback=true` materializes each gap as a
    /// typed `:OntologyGap` node linked to its class — a graph MUTATION,
    /// WAL-replayed by re-scanning deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineOntologyGap {
        /// Optional: restrict the scan to class nodes of this one type
        /// (`None` ⇒ every node whose `type`/`node_type` is `Class` or `OwlClass`).
        #[serde(default)]
        label: Option<String>,
        /// Materialize each gap as a typed `:OntologyGap` node linked to its class.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per gap (E6) — see
        /// [`Method::MineAssociate::as_claim`]. Confidence is seeded from the
        /// gap kind's fixed documented severity (`eg_compute::mining::ontology_gap::GapKind::severity`).
        /// Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },


    /// Retrieval-quality evaluation (CONCEPT:EG-KG.mining.retrieval-quality):
    /// precision@k / recall@k / MRR over stored retrieval `traces`. With
    /// `writeback=true` materializes the aggregate report as a typed
    /// `:RetrievalQuality` node — a graph MUTATION, WAL-replayed by
    /// re-evaluating deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineRetrievalQuality {
        /// Retrieval traces to evaluate — required.
        #[serde(default)]
        traces: Vec<RetrievalTraceSpec>,
        /// Precision/recall/MRR cutoff. `0` ⇒ use each trace's full retrieved list.
        #[serde(default)]
        k: usize,
        /// Optional identity for the write-back `:RetrievalQuality` node. Empty ⇒
        /// derived from the input traces.
        #[serde(default)]
        query_id: String,
        /// Materialize the aggregate report as a typed `:RetrievalQuality` node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the report (E6)
        /// — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from
        /// the report's own F1 (harmonic mean of precision@k/recall@k, already
        /// `[0,1]`). Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },
        ]);
    };
}

pub(crate) use __eg_method_chunk_9;
