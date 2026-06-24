// CONCEPT:KG-2.19 — Epistemic Graph Service Wire Protocol
//
// Length-prefixed MessagePack framing for UDS/TCP communication between
// the Python client and the Tokio service layer. Every request
// is authenticated via HMAC-SHA256.

use serde::{Deserialize, Serialize};

/// serde defaults for `DsTrainTestSplit` so older clients omitting these fields
/// get scikit-learn-compatible behavior (shuffle on, fixed seed).
fn default_shuffle() -> bool {
    true
}
fn default_split_seed() -> u64 {
    42
}

/// serde defaults for the training loss/optimizer kernels (CONCEPT:KG-2.22).
fn default_temperature() -> f64 {
    1.0
}
fn default_dpo_beta() -> f64 {
    0.1
}
fn default_clip_eps() -> f64 {
    0.2
}
fn default_adam_beta1() -> f64 {
    0.9
}
fn default_adam_beta2() -> f64 {
    0.999
}
fn default_adam_eps() -> f64 {
    1e-8
}

/// serde default for the Ebbinghaus decay half-life (CONCEPT:KG-2.16): 7 days in
/// seconds. Older clients omitting it get a one-week memory half-life.
fn default_decay_half_life() -> f64 {
    604_800.0
}

// ── Request ─────────────────────────────────────────────────────────────

/// Top-level request envelope sent by the Python client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    /// Monotonically increasing request ID for correlation.
    pub id: u64,
    /// Target graph name (e.g., "agent:planner", "__commons__", "channel:p2p:a:b").
    pub graph: String,
    /// HMAC-SHA256 hex digest for authentication.
    pub auth_token: String,
    /// Caller identity for ACL enforcement (see `isolation.rs`). Optional and
    /// backward-compatible: older clients simply omit the field. When isolation
    /// rules are registered, graph-targeted operations are checked against this
    /// identity; an absent identity is treated as an anonymous agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// The operation to perform.
    #[serde(flatten)]
    pub method: Method,
}

// ── Method ──────────────────────────────────────────────────────────────

/// All operations supported by the service.
// `IntoStaticStr` (metrics builds) yields the variant name as the bounded
// `op` label for request counters/histograms (CONCEPT:KG-2.51).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "metrics", derive(strum::IntoStaticStr))]
#[serde(tag = "method", content = "params")]
pub enum Method {
    // ── Node CRUD ────────────────────────────────────────────────────
    AddNode {
        node_id: String,
        #[serde(with = "serde_bytes")]
        properties_msgpack: Vec<u8>,
    },
    RemoveNode {
        node_id: String,
    },
    HasNode {
        node_id: String,
    },
    GetNodes,
    /// Labeled + bounded node fetch: return at most `limit` nodes whose
    /// `type`/`label`/`labels` matches `label` (limit 0 ⇒ no cap). Unlike
    /// `GetNodes` (which materializes the WHOLE graph), this bounds the wire
    /// payload to `limit`, so a `MATCH (n:Label) … LIMIT k` no longer pulls every
    /// node's properties off the engine. (CONCEPT:KG-2.51)
    GetNodesByLabel {
        label: String,
        limit: usize,
    },
    GetNodeProperties {
        node_id: String,
    },
    /// Atomic compare-and-set on a node's property blob (CONCEPT:KG-2 backend-
    /// agnostic atomic claim). `conditions_msgpack`/`updates_msgpack` are
    /// MessagePack-encoded JSON objects (field→value maps, same encoding as
    /// `properties_msgpack`). Under the topology write guard: if every condition
    /// matches the node's current value (a MISSING field reads as `null`), the
    /// updates are merged in and `true` is returned; otherwise (node absent, any
    /// condition fails, or decode fails) the node is left untouched and `false`
    /// is returned. One in-engine CAS suffices for all backends (the engine is
    /// the authoritative store; mirrors follow).
    CompareAndSetNodeFields {
        node_id: String,
        #[serde(with = "serde_bytes")]
        conditions_msgpack: Vec<u8>,
        #[serde(with = "serde_bytes")]
        updates_msgpack: Vec<u8>,
    },
    /// Batch property read: fetch properties for many nodes in ONE round-trip
    /// instead of N `GetNodeProperties` calls. Returns a `Raw` list of
    /// `[node_id, properties_msgpack | nil]` in input order (nil ⇒ absent), so the
    /// caller learns which ids were missing. Bounded by `MAX_BATCH_IDS`.
    GetNodePropertiesBatch {
        node_ids: Vec<String>,
    },
    /// Batch existence check: `Raw` list of bools in input order.
    HasNodesBatch {
        node_ids: Vec<String>,
    },
    NodeCount,
    NodeIds,

    // ── Edge CRUD ────────────────────────────────────────────────────
    AddEdge {
        source_id: String,
        target_id: String,
        #[serde(with = "serde_bytes")]
        properties_msgpack: Vec<u8>,
    },
    RemoveEdge {
        source_id: String,
        target_id: String,
    },
    HasEdge {
        source_id: String,
        target_id: String,
    },
    GetEdges,
    /// Bulk-export the graph as RDF triples ``[subject, predicate, object]`` in a
    /// single call — the fast path for local SPARQL materialization (CONCEPT:KG-2.7):
    /// edges → (src, rel_type, tgt), node type → (id, "rdf:type", node_type), and
    /// scalar node properties → (id, prop, literal). Avoids per-node round-trips.
    GetTriples,
    ClearGraph,
    GetEdgeProperties {
        source_id: String,
        target_id: String,
    },
    /// Batch edge property read: `Raw` list of `properties_msgpack | nil` in input
    /// order (nil ⇒ no such edge). Bounded by `MAX_BATCH_IDS`.
    GetEdgePropertiesBatch {
        edges: Vec<(String, String)>,
    },
    EdgeCount,

    // ── Neighbor Queries ─────────────────────────────────────────────
    InDegree {
        node_id: String,
    },
    OutDegree {
        node_id: String,
    },
    GetPredecessors {
        node_id: String,
    },
    GetSuccessors {
        node_id: String,
    },
    GetNeighbors {
        node_id: String,
    },

    // ── Cross-graph union reads (CONCEPT:KG-2.171) ───────────────────
    // Read across a SET of content graphs as if they were one, so writes can be
    // partitioned across per-graph write locks (each lane its own graph/lock)
    // while reads still see the union. Missing graphs in the set are skipped
    // (a lane graph may not exist yet). Routed like the other cross-graph reads
    // (DiffAgainst): the handler re-enters the registry, point-reads/snapshots
    // each core off-lock, and merges — never holding two graph locks at once.
    /// First-found node properties across `graphs` (in order); `Null` if absent
    /// in every graph.
    UnionGetNodeProperties {
        graphs: Vec<String>,
        node_id: String,
    },
    /// Label scan unioned + deduped by node id across `graphs` (limit 0 ⇒ no cap).
    UnionGetNodesByLabel {
        graphs: Vec<String>,
        label: String,
        limit: usize,
    },
    /// Neighbour ids unioned + deduped across every graph that contains the anchor.
    UnionGetNeighbors {
        graphs: Vec<String>,
        node_id: String,
    },

    // ── Graph Algorithms ─────────────────────────────────────────────
    TopologicalSort,
    FindCycle,
    GetShortestPath {
        source_id: String,
        target_id: String,
    },
    GetBlastRadius {
        node_id: String,
        max_depth: usize,
    },
    DegreeCentrality {
        node_id: String,
    },
    DegreeCentralityAll,
    BetweennessCentrality,
    PageRank {
        damping: f64,
        iterations: usize,
    },
    PersonalizedPageRank {
        seed_nodes: Vec<(String, f64)>,
        damping: f64,
        iterations: usize,
    },
    ConnectedComponents,
    StronglyConnectedComponents,
    MinimumSpanningTree,
    CommunityDetection {
        resolution: f64,
    },
    /// Stateless community detection over a call graph passed inline — NO tenant
    /// load, NO persistence. The ingest path previously bulk-loaded ~160k edges
    /// into a throwaway tenant just to run this, then deleted the tenant; passing
    /// the edges directly removes that whole round-trip + the tenant sprawl.
    CommunityDetectEphemeral {
        node_ids: Vec<String>,
        edges: Vec<(String, String)>,
        resolution: f64,
    },
    GraphColoring,
    ComputeSimilarityEdges {
        threshold: f64,
    },

    // ── Lifecycle ────────────────────────────────────────────────────
    PruneByLifecycle {
        max_age_secs: u64,
        min_score: f64,
    },
    GetContextView {
        agent_id: String,
        max_tokens: u32,
    },
    BatchUpdate {
        #[serde(with = "serde_bytes")]
        operations_msgpack: Vec<u8>,
    },
    Metrics,
    EvictLRU {
        max_nodes: usize,
    },

    // ── Temporal Decay (CONCEPT:KG-2.16 — Ebbinghaus forgetting curve) ──
    DecaySweep {
        #[serde(default = "default_decay_half_life")]
        half_life_secs: f64,
        #[serde(default)]
        floor: f64,
        #[serde(default)]
        prune: bool,
    },
    TouchNodes {
        node_ids: Vec<String>,
    },

    // ── Serialization ────────────────────────────────────────────────
    ToMsgpack,
    FromMsgpack {
        #[serde(with = "serde_bytes")]
        msgpack: Vec<u8>,
    },

    // ── Ledger ───────────────────────────────────────────────────────
    GetLedger,
    ClearLedger,
    ApplyLedger {
        transactions: Vec<String>,
    },

    // ── Subgraph & Matching ──────────────────────────────────────────
    GetSubgraph {
        node_ids: Vec<String>,
    },
    Fork,
    DiffAgainst {
        other_graph: String,
    },
    CompactNodesByType {
        node_type: String,
        threshold: usize,
    },

    // ── Reasoning ────────────────────────────────────────────────────
    // CONCEPT:KG-2.17 - Compiled Semantic Reasoner. A single round of
    // forward-chaining OWL/RDFS inference (Datalog) plus optional
    // domain/range and property-chain inference. All rule sets default to
    // empty so clients may run any subset without sending every field.
    RunDatalogReasoning {
        #[serde(default)]
        subclass_relations: Vec<(String, String)>,
        #[serde(default)]
        subproperty_relations: Vec<(String, String)>,
        #[serde(default)]
        symmetric_properties: Vec<String>,
        #[serde(default)]
        transitive_properties: Vec<String>,
        #[serde(default)]
        inverse_properties: Vec<(String, String)>,
        /// (property, domain_type) — subjects of `property` are inferred to be `domain_type`.
        #[serde(default)]
        domain_rules: Vec<(String, String)>,
        /// (property, range_type) — objects of `property` are inferred to be `range_type`.
        #[serde(default)]
        range_rules: Vec<(String, String)>,
        /// (predicate_a, predicate_b, inferred_predicate) — chain composition.
        #[serde(default)]
        property_chains: Vec<(String, String, String)>,
    },

    // ── Multi-Tenant Graph Management ────────────────────────────────
    CreateGraph {
        graph_name: String,
        graph_type: GraphType,
    },
    DeleteGraph {
        graph_name: String,
    },
    ListGraphs,

    // ── Dynamic Communication Channels ───────────────────────────────
    CreateChannel {
        channel_id: String,
        channel_type: ChannelType,
        creator: String,
        initial_members: Vec<String>,
    },
    JoinChannel {
        channel_id: String,
        agent_id: String,
    },
    LeaveChannel {
        channel_id: String,
        agent_id: String,
    },
    CloseChannel {
        channel_id: String,
        /// Optional embedding of the conversation summary.
        summary_embedding: Option<Vec<f32>>,
        /// Optional topic/metadata for the KG imprint.
        topic_metadata: Option<String>,
    },
    SendMessage {
        channel_id: String,
        sender: String,
        payload: String,
    },
    GetChannelMessages {
        channel_id: String,
        limit: Option<usize>,
    },
    ListChannels,
    GetChannelMembers {
        channel_id: String,
    },

    // ── Service-Level ────────────────────────────────────────────────
    Ping,
    Health,
    Shutdown,
    Checkpoint,
    Reconcile {
        graph_name: String,
        #[serde(with = "serde_bytes")]
        msgpack: Vec<u8>,
    },
    ApplyMutation {
        event_type: String,
        query: String,
    },
    ParseRepository {
        root_path: String,
    },
    Vf2SubgraphMatch {
        pattern_graph_name: String,
    },

    // ── AST Parsing ──────────────────────────────────────────────────
    ParseFile {
        file_path: String,
        #[serde(with = "serde_bytes")]
        source: Vec<u8>,
    },
    /// Batched parse: one round-trip for N files (CONCEPT:KG-2.16). The blob is
    /// a MessagePack-encoded `Vec<(file_path, source_bytes)>`; the response is an
    /// ordered `Vec<ParseResult>`, one per input file. Mirrors `BatchUpdate`.
    ParseFiles {
        #[serde(with = "serde_bytes")]
        files_msgpack: Vec<u8>,
    },
    /// Parse a batch AND resolve cross-file call/import edges in one round-trip
    /// (CONCEPT:KG-2.8r). The blob is the same MessagePack `Vec<(file_path,
    /// source_bytes)>` as `ParseFiles`, but the batch is treated as one
    /// resolution scope (a repository, or a delta set): the response is a SINGLE
    /// resolved `IndexResult` whose `calls`/`depends_on` edges point at real node
    /// ids, not bare names. Use this (not `ParseFiles`) to ingest a repo's symbol
    /// graph; use `ParseFiles` only when per-file raw results are wanted.
    IndexRepository {
        #[serde(with = "serde_bytes")]
        files_msgpack: Vec<u8>,
    },

    // ── Screen Observation (computer-use) ─────────────────────────────
    /// Turn a captured desktop frame into durable session/frame/UIElement graph
    /// entities in one round-trip (CONCEPT:KG-2.185). The blob is a MessagePack map
    /// `{session_id, frame_seq, prev_frame_id, prev_hash, png: bin, elements: [..]}`;
    /// the response is a SINGLE `ScreenObservationResult` (nodes + edges), mirroring
    /// `IndexRepository`. The screenshot bytes never persist — only its dimensions +
    /// content hash do, for frame-diff.
    ObserveScreen {
        #[serde(with = "serde_bytes")]
        obs_msgpack: Vec<u8>,
    },

    // ── Semantic Compute ─────────────────────────────────────────────
    AddEmbedding {
        node_id: String,
        embedding: Vec<f32>,
    },
    SemanticSearch {
        query_embedding: Vec<f32>,
        n_results: usize,
    },
    /// CONCEPT:EG-010 — embedding-free lexical classification gate: which
    /// capability-node terms (Tool/Skill/MCPServer names+synonyms) appear in the
    /// query. The "free" tier between structural routing and `SemanticSearch`.
    MatchOntologyTerms {
        query: String,
    },
    SpectralCluster {
        vectors: Vec<Vec<f64>>,
        max_k: usize,
        domain: String,
    },
    HypergraphEncodeInteraction {
        pos_a: usize,
        pos_b: usize,
        pos_dim: usize,
        hidden_dim: usize,
        out_dim: usize,
        seed: u64,
    },
    BatchCosineSimilarity {
        query: Vec<f32>,
        targets: Vec<Vec<f32>>,
    },
    FindSimilarPairs {
        embeddings: Vec<Vec<f32>>,
        ids: Vec<String>,
        threshold: f32,
        use_lsh: bool,
        lsh_num_tables: usize,
        lsh_hash_size: usize,
        seed: u64,
    },

    // ── Quantitative Finance ──────────────────────────────────────────
    FinanceOptimizePortfolio {
        expected_returns: Vec<f64>,
        cov_matrix: Vec<Vec<f64>>,
        risk_free_rate: f64,
        min_weight: Option<f64>,
        max_weight: Option<f64>,
    },
    FinanceRiskParity {
        cov_matrix: Vec<Vec<f64>>,
    },
    FinanceBlackLitterman {
        market_weights: Vec<f64>,
        cov_matrix: Vec<Vec<f64>>,
        views: Vec<f64>,
        pick_matrix: Vec<Vec<f64>>,
        tau: f64,
        risk_aversion: f64,
    },
    FinanceEfficientFrontier {
        expected_returns: Vec<f64>,
        cov_matrix: Vec<Vec<f64>>,
        target_return: f64,
    },

    // ── Data Science Primitives (CONCEPT:KG-2.22) ─────────────────────
    DsLinearRegression {
        x: Vec<Vec<f64>>,
        y: Vec<f64>,
    },
    DsKMeans {
        data: Vec<Vec<f64>>,
        k: usize,
        max_iter: usize,
    },
    DsPca {
        data: Vec<Vec<f64>>,
        n_components: usize,
    },
    DsComputeStats {
        data: Vec<Vec<f64>>,
    },
    DsTrainTestSplit {
        data: Vec<Vec<f64>>,
        labels: Vec<f64>,
        test_ratio: f64,
        #[serde(default = "default_shuffle")]
        shuffle: bool,
        #[serde(default = "default_split_seed")]
        seed: u64,
    },
    // These two variants embed `datascience` domain types, so they are gated with
    // the feature — a slim server without `datascience` simply doesn't know them.
    #[cfg(feature = "datascience")]
    DsFitEstimator {
        estimator: String,
        x: Vec<Vec<f64>>,
        y: Vec<f64>,
        #[serde(default)]
        params: crate::wire::EstimatorParams,
    },
    #[cfg(feature = "datascience")]
    DsPredictEstimator {
        model: crate::wire::FittedModel,
        x: Vec<Vec<f64>>,
    },

    // ── Training loss / optimizer kernels (CONCEPT:KG-2.22) ────────────
    DsSoftmax {
        logits: Vec<f64>,
        #[serde(default = "default_temperature")]
        temperature: f64,
    },
    DsLogSoftmax {
        logits: Vec<f64>,
    },
    DsCrossEntropy {
        logits: Vec<Vec<f64>>,
        labels: Vec<usize>,
    },
    DsDpoLoss {
        policy_chosen: Vec<f64>,
        policy_rejected: Vec<f64>,
        ref_chosen: Vec<f64>,
        ref_rejected: Vec<f64>,
        #[serde(default = "default_dpo_beta")]
        beta: f64,
    },
    DsGrpoSurrogate {
        logprob: Vec<f64>,
        old_logprob: Vec<f64>,
        advantage: Vec<f64>,
        #[serde(default = "default_clip_eps")]
        clip_eps: f64,
    },
    DsKlDivergence {
        logprob: Vec<f64>,
        ref_logprob: Vec<f64>,
    },
    DsAdamStep {
        params: Vec<f64>,
        grads: Vec<f64>,
        #[serde(default)]
        m: Vec<f64>,
        #[serde(default)]
        v: Vec<f64>,
        lr: f64,
        #[serde(default = "default_adam_beta1")]
        beta1: f64,
        #[serde(default = "default_adam_beta2")]
        beta2: f64,
        #[serde(default = "default_adam_eps")]
        eps: f64,
        t: u64,
    },
    DsSgdStep {
        params: Vec<f64>,
        grads: Vec<f64>,
        lr: f64,
    },

    // ── Extended Finance: Risk (CONCEPT:KG-2.20) ──────────────────────
    FinanceVar {
        returns: Vec<f64>,
        confidence: f64,
    },
    FinanceCvar {
        returns: Vec<f64>,
        confidence: f64,
    },
    FinanceMaxDrawdown {
        returns: Vec<f64>,
    },
    FinanceDrawdownSeries {
        returns: Vec<f64>,
    },
    FinanceDownsideDeviation {
        returns: Vec<f64>,
        target: f64,
    },
    FinanceRiskMetrics {
        returns: Vec<f64>,
        risk_free_rate: f64,
    },
    FinanceMonteCarloVar {
        mean: f64,
        std_dev: f64,
        n_simulations: usize,
        confidence: f64,
    },
    FinanceStressTest {
        weights: Vec<f64>,
        expected_returns: Vec<f64>,
        cov_matrix: Vec<Vec<f64>>,
        shock_factors: Vec<f64>,
    },

    // ── Extended Finance: Regime detection (HMM) ──────────────────────
    FinanceDetectRegimes {
        observations: Vec<f64>,
        n_states: usize,
        max_iter: usize,
        tol: f64,
    },

    // ── Extended Finance: Signals / alpha ─────────────────────────────
    FinanceRollingZscore {
        values: Vec<f64>,
        window: usize,
    },
    FinanceEwma {
        values: Vec<f64>,
        span: usize,
    },
    FinanceSignalDecay {
        signal: Vec<f64>,
        half_life: f64,
    },
    FinanceCombineAlphas {
        signals: Vec<Vec<f64>>,
        weights: Vec<f64>,
    },
    FinanceCrossSectionalRank {
        cross_section: Vec<Vec<f64>>,
    },
    FinanceMomentum {
        prices: Vec<f64>,
        lookback: usize,
    },
    FinanceMeanReversion {
        values: Vec<f64>,
        window: usize,
    },
    FinanceInformationCoefficient {
        signal: Vec<f64>,
        forward_returns: Vec<f64>,
    },

    // ── Extended Finance: Execution / microstructure ──────────────────
    FinanceTwap {
        total_quantity: f64,
        n_slices: usize,
        start_time: u64,
        interval_secs: u64,
    },
    FinanceVwap {
        total_quantity: f64,
        volume_profile: Vec<f64>,
        start_time: u64,
        interval_secs: u64,
    },
    FinanceMarketImpact {
        daily_volatility: f64,
        order_quantity: f64,
        average_daily_volume: f64,
        impact_coefficient: f64,
    },
    FinancePairsTrading {
        prices_a: Vec<f64>,
        prices_b: Vec<f64>,
        lookback: usize,
    },
    // Embeds a `finance` domain type → gated with the feature.
    #[cfg(feature = "finance")]
    FinanceMatchOrders {
        orders: Vec<crate::wire::Order>,
    },

    // ── Market Making / Microstructure (CONCEPT:KG-2.20f) ─────────────
    FinanceAvellanedaStoikov {
        mid: f64,
        inventory: f64,
        sigma: f64,
        gamma: f64,
        kappa: f64,
        tau: f64,
    },
    FinanceGltQuotes {
        mid: f64,
        inventory: f64,
        sigma: f64,
        gamma: f64,
        kappa: f64,
        a: f64,
    },
    FinanceLogitQuotes {
        p_mid: f64,
        inventory: f64,
        sigma: f64,
        gamma: f64,
        kappa: f64,
        tau: f64,
        boundary_m: f64,
    },
    FinanceGlostenMilgromSpread {
        alpha: f64,
        p: f64,
    },
    FinanceExpectedPnlRate {
        delta: f64,
        a: f64,
        kappa: f64,
        alpha: f64,
        p: f64,
        v_h: f64,
        v_l: f64,
    },
    FinanceBreakevenAlpha {
        delta: f64,
        p: f64,
        v_h: f64,
        v_l: f64,
    },
    FinanceOfiSeries {
        ts: Vec<f64>,
        bid_px: Vec<f64>,
        bid_sz: Vec<f64>,
        ask_px: Vec<f64>,
        ask_sz: Vec<f64>,
        window_secs: f64,
    },
    FinanceMicropriceSeries {
        bid_px: Vec<f64>,
        bid_sz: Vec<f64>,
        ask_px: Vec<f64>,
        ask_sz: Vec<f64>,
    },
    FinanceVpinPm {
        buy_vol: Vec<f64>,
        sell_vol: Vec<f64>,
        p_mean: Vec<f64>,
    },
    FinanceHawkesMle {
        times: Vec<f64>,
        t_horizon: f64,
        max_iter: usize,
    },
    FinanceHardimanBouchaud {
        times: Vec<f64>,
        t_horizon: f64,
        n_windows: usize,
    },

    // ── Kyle insider/stealth surveillance (CONCEPT:KG-2.20k) ──────────
    FinanceKyleLambda {
        price_changes: Vec<f64>,
        signed_order_flow: Vec<f64>,
    },
    FinanceSurveillanceRisk {
        buy_vol: Vec<f64>,
        sell_vol: Vec<f64>,
        p_mean: Vec<f64>,
        signed_flow: Vec<f64>,
        price_changes: Vec<f64>,
        baseline_sigma: f64,
    },

    // ── Position Sizing (CONCEPT:KG-2.20f) ────────────────────────────
    FinanceKellyFraction {
        q: f64,
        c: f64,
        fraction: f64,
    },
    FinanceBayesianKelly {
        alpha: f64,
        beta: f64,
        c: f64,
        n_quadrature: usize,
    },
    FinancePosteriorCredibleInterval {
        alpha: f64,
        beta: f64,
        level: f64,
    },

    // ── Backtest Validation (CONCEPT:KG-2.20f) ────────────────────────
    FinancePurgedCpcv {
        n_samples: usize,
        n_groups: usize,
        n_test_groups: usize,
        purge_window: usize,
        embargo: usize,
    },
    FinanceDeflatedSharpe {
        observed_sr: f64,
        n_trials: usize,
        sr_returns: Vec<f64>,
    },
    FinanceProbabilityBacktestOverfit {
        insample: Vec<Vec<f64>>,
        oos: Vec<Vec<f64>>,
    },
    FinanceDieboldMariano {
        losses_a: Vec<f64>,
        losses_b: Vec<f64>,
        h: usize,
    },

    // ── Forensic Accounting (CONCEPT:KG-2.20g) ────────────────────────
    // Embeds `finance` domain types → gated with the feature.
    #[cfg(feature = "finance")]
    FinanceForensicReport {
        this_year: crate::wire::YearData,
        prior_year: crate::wire::YearData,
    },

    // ── State-Space / Stat-Arb (CONCEPT:KG-2.20h) ─────────────────────
    FinanceKalmanFilter1d {
        observations: Vec<f64>,
        f: f64,
        q: f64,
        h: f64,
        r: f64,
        x0: f64,
        p0: f64,
    },
    FinanceKalmanBeta {
        market_returns: Vec<f64>,
        asset_returns: Vec<f64>,
        q: f64,
        r: f64,
        beta0: f64,
        p0: f64,
    },
    FinanceKalmanVolatility {
        returns: Vec<f64>,
        q: f64,
        r: f64,
        log_var0: Option<f64>,
        p0: f64,
        annualization: f64,
    },
    FinanceAdfTest {
        series: Vec<f64>,
        max_lag: usize,
    },
    FinanceOuCalibrate {
        spread: Vec<f64>,
        dt: f64,
    },
    FinanceOuOptimalThresholds {
        theta: f64,
        mu: f64,
        sigma: f64,
        sigma_eq: f64,
        cost: f64,
    },
    FinanceMarkovTransitionMatrix {
        states: Vec<usize>,
        n_states: usize,
    },

    // ── Signal Combination / Sizing / Calibration (CONCEPT:KG-2.20i) ──
    FinanceOrderBookImbalance {
        v_bid: Vec<f64>,
        v_ask: Vec<f64>,
    },
    FinanceQueueImbalance {
        bid_q: Vec<f64>,
        ask_q: Vec<f64>,
        bid_rate: Vec<f64>,
        ask_rate: Vec<f64>,
    },
    FinanceRealizedVolTick {
        mid: Vec<f64>,
        window: usize,
    },
    FinanceSpreadReversion {
        bid_px: Vec<f64>,
        ask_px: Vec<f64>,
        window: usize,
    },
    FinanceInformationRatio {
        ic: f64,
        n_independent: f64,
    },
    FinanceEffectiveIndependentN {
        returns_matrix: Vec<Vec<f64>>,
    },
    FinanceAlphaCombinationEngine {
        returns_matrix: Vec<Vec<f64>>,
        lookback: usize,
    },
    FinanceBrierScore {
        forecasts: Vec<f64>,
        outcomes: Vec<f64>,
    },
    FinanceConvergenceGate {
        strengths: Vec<f64>,
        strong_threshold: f64,
        min_agree: usize,
    },
    FinanceEmpiricalKelly {
        p: f64,
        b: f64,
        historical_returns: Vec<f64>,
        n_simulations: usize,
        seed: u64,
    },

    // ── Derivatives: SABR volatility surface (CONCEPT:KG-2.20j) ────────
    FinanceSabrImpliedVol {
        f: f64,
        k: f64,
        t: f64,
        alpha: f64,
        beta: f64,
        rho: f64,
        nu: f64,
    },
    FinanceSabrSmile {
        f: f64,
        strikes: Vec<f64>,
        t: f64,
        alpha: f64,
        beta: f64,
        rho: f64,
        nu: f64,
    },
    FinanceSabrCalibrate {
        f: f64,
        t: f64,
        strikes: Vec<f64>,
        market_vols: Vec<f64>,
        beta: f64,
    },

    // ── Zero-Trust Consensus ─────────────────────────────────────────
    RegisterIdentity {
        agent_id: String,
        role: crate::acl::AgentRole,
        teams: Vec<String>,
        signature: String,
    },
    ApplyMultisigMutation {
        signatures: Vec<String>,
        threshold: usize,
        mutation_type: String,
        query: String,
    },

    // ── Query (SQL + Cypher) ──────────────────────────────────────────
    // Read-only relational query surface (CONCEPT:KG-2.178). `SELECT … FROM
    // nodes …` over ONE graph via DataFusion, gated behind the facade `query`
    // feature; in a slim build the variant falls to the not-built catch-all.
    // `params_msgpack` is reserved for future bound parameters.
    Sql {
        query: String,
        #[serde(default, with = "serde_bytes")]
        params_msgpack: Vec<u8>,
    },
    // Read-only Cypher query surface (CONCEPT:KG-2.179). A `MATCH … WHERE … RETURN
    // … LIMIT …` over ONE graph, compiled to the engine's own primitives (the
    // eg-core label index, `vf2_subgraph_match`, and petgraph BFS) — NO DataFusion,
    // so it ships in the lean Pi build behind the facade `cypher` feature. Reuses
    // the same `QueryResult` carrier as `Sql` (returned via `ResultPayload::raw`):
    // a Cypher RETURN is the same columns+row-blobs shape, so no new payload
    // variant. In a build without `cypher` the variant falls to the not-built
    // catch-all.
    CypherQuery {
        query: String,
    },

    // ── Unified cross-modal query (CONCEPT:KG-2.208/209) ──────────────────
    // ONE plan that filters (relational/DataFusion) → traverses (graph/BFS) →
    // ranks (vector/kNN) over the SAME off-lock snapshot, instead of three siloed
    // round-trips. The `plan` is the serializable [`crate::wire::Plan`] AST (an
    // ordered list of `Scan|Filter|Traverse|Rank|Limit` ops over a shared RowSet);
    // the bespoke planner (eg-plan) sequences the existing legs and applies a
    // cost-based filter-vs-vector reorder (CONCEPT:KG-2.209). Read-only this
    // increment. Gated behind the facade `query` feature (the FILTER leg needs
    // DataFusion); in a slim build the variant falls to the not-built catch-all.
    // Result via `ResultPayload::raw` — a list of `[id, score|nil]` rows.
    #[cfg(feature = "query")]
    UnifiedQuery {
        plan: crate::wire::Plan,
        /// Optional cost-based reorder hint: when set, the planner reorders an
        /// adjacent (Filter, Rank) pair by this estimated filter selectivity in
        /// [0,1] (CONCEPT:KG-2.209). Absent ⇒ the plan executes as given.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reorder_filter_selectivity: Option<f64>,
    },

    // ── Transactions (CONCEPT:KG-2.180 — multi-op OCC ACID) ───────────────
    // Server-side STAGED, OPTIMISTIC, snapshot-isolation transactions. `BeginTxn`
    // returns a server-issued `txn_id` (String). The `Txn*` ops STAGE durable
    // mutations into a server-held write-set (nothing touches the graph or
    // persistence until commit) and ack with `Bool(true)`. `Commit` takes the
    // topology write lock ONCE — the serialization point — validates the OCC
    // read-set (no targeted node changed since begin), applies the staged write-set
    // atomically through one `GraphTxn`, bumps the version counter, and persists;
    // it returns `Bool(false)` on conflict (true rollback — nothing applied).
    // `Rollback` discards the staged state and returns `Bool(true)`. The write
    // coalescer is NOT involved: staged ops are applied directly via `GraphTxn` at
    // commit, so there is no interaction/deadlock with the per-graph write worker
    // (which only handles NON-transactional single-op writes). A long-open txn
    // never holds `topo.write()`. (A single redb WriteTransaction per commit — a
    // true durability barrier — is a future enhancement; M6 persists per staged op
    // at commit and relies on the single GraphTxn for in-memory atomicity.)
    BeginTxn {
        /// Optional explicit target graph; defaults to the request envelope's
        /// `graph` when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        graph: Option<String>,
        /// Reserved isolation hint; only snapshot isolation is implemented.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        isolation: Option<String>,
    },
    TxnAddNode {
        txn_id: String,
        node_id: String,
        #[serde(with = "serde_bytes")]
        properties_msgpack: Vec<u8>,
    },
    TxnRemoveNode {
        txn_id: String,
        node_id: String,
    },
    TxnAddEdge {
        txn_id: String,
        source_id: String,
        target_id: String,
        #[serde(with = "serde_bytes")]
        properties_msgpack: Vec<u8>,
    },
    TxnRemoveEdge {
        txn_id: String,
        source_id: String,
        target_id: String,
    },
    TxnCas {
        txn_id: String,
        node_id: String,
        #[serde(with = "serde_bytes")]
        conditions_msgpack: Vec<u8>,
        #[serde(with = "serde_bytes")]
        updates_msgpack: Vec<u8>,
    },
    Commit {
        txn_id: String,
    },
    Rollback {
        txn_id: String,
    },

    // ── Time-series (CONCEPT:KG-2.210/211 — native TSDB) ──────────────────
    // Native time-series store + query primitives (the eg-tsdb crate), gated
    // behind the facade `tsdb` feature; in a slim build each variant falls to the
    // graph_ops not-built catch-all. Series are keyed by `series_id` in their OWN
    // redb file (`series.redb`) beside `graph.redb`. Points cross the wire as a
    // MessagePack blob (`Vec<(i64 ts, Vec<f64> values)>`) so the protocol enum (at
    // the bottom of the DAG) stays free of any eg-tsdb type. Query results return
    // via `ResultPayload::raw` (the client double-unpacks), matching `Sql`/`Cypher`.
    //
    // `TsAppend` is the ONE durable write here (handled out-of-band of the graph
    // write-coalescer — it targets the series store, not the graph core); the rest
    // are read-only.
    TsAppend {
        series_id: String,
        /// Field count per point (1 for a scalar series, N for OHLCV…). Used only
        /// when the series is NEW; an existing series' stored schema wins.
        n_fields: usize,
        /// Bucket/time-partition width in nanoseconds (series-creation parameter).
        bucket_ns: u64,
        /// Optional field names (series-creation metadata).
        #[serde(default)]
        field_names: Vec<String>,
        /// MessagePack `Vec<(i64, Vec<f64>)>` — the batch of points (one round-trip).
        #[serde(with = "serde_bytes")]
        points_msgpack: Vec<u8>,
    },
    TsRange {
        series_id: String,
        /// Inclusive lower / exclusive upper ts bound (ns).
        from: i64,
        to: i64,
    },
    TsAsofJoin {
        /// The "right" series each left event is joined to by nearest-prior ts.
        series_id: String,
        /// MessagePack `Vec<i64>` — the left event timestamps (ns).
        #[serde(with = "serde_bytes")]
        left_ts_msgpack: Vec<u8>,
        /// Optional tolerance (ns); a match older than this is dropped (`None` =
        /// unbounded). `-1` encodes `None` over the wire.
        #[serde(default)]
        tolerance: i64,
    },
    TsWindow {
        series_id: String,
        from: i64,
        to: i64,
        /// Window width (ns) for the bucketed aggregate.
        width: i64,
        /// Aggregate function: one of first/last/min/max/mean/sum/count.
        agg: String,
    },
    TsGapFill {
        series_id: String,
        from: i64,
        to: i64,
        /// Grid step (ns) for the LOCF densification.
        step: i64,
    },
}

// ── Supporting Types ────────────────────────────────────────────────────

/// Materialized result of a `Method::Sql` query (CONCEPT:KG-2.178). Returned via
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

/// Graph type for multi-tenant registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphType {
    Agent,
    Team,
    Global,
    Commons,
}

/// Channel type for dynamic communication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelType {
    /// 1:1 direct messaging between two agents.
    PeerToPeer,
    /// Many-to-many group channel.
    Group,
}

// ── Response ────────────────────────────────────────────────────────────

/// Untagged result payload for efficient serialization without JSON overhead.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResultPayload {
    Bool(bool),
    Count(u64),
    Float(f64),
    String(String),
    Ids(Vec<String>),
    NodeList(Vec<(String, serde_json::Value)>),
    EdgeList(Vec<(String, String, Vec<u8>)>),
    PropertiesMsgpack(#[serde(with = "serde_bytes")] Vec<u8>),
    Rows(Vec<Vec<u8>>),
    /// A typed result serialized STRAIGHT to MessagePack (Phase C-D — compact
    /// result encoding). Skips building a `serde_json::Value` tree on the server —
    /// the dominant allocator for large algorithm results (PageRank/centrality/
    /// communities over the whole graph). On the wire it is a MessagePack `bin`
    /// (identical shape to `PropertiesMsgpack`); the Python client decodes any
    /// top-level `bytes` result with a second `unpackb`, recovering the exact same
    /// structure the `Json` path produced. Lives after `PropertiesMsgpack` so the
    /// untagged decoder is unaffected.
    Raw(#[serde(with = "serde_bytes")] Vec<u8>),
    Json(serde_json::Value),
}

impl ResultPayload {
    /// Encode a typed value straight to MessagePack as a [`ResultPayload::Raw`],
    /// bypassing the `serde_json::Value` tree (the dominant allocator for large
    /// algorithm results). The compact encoding is the ONE wire contract — clients
    /// decode a top-level `bytes` result with a second `unpackb`. No fallback, no
    /// flag: a greenfield codebase carries no dual-path legacy baggage. (Phase C-D)
    pub fn raw<T: Serialize>(value: &T) -> Self {
        ResultPayload::Raw(rmp_serde::to_vec_named(value).unwrap_or_default())
    }
}

/// Response envelope sent back to the Python client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    /// Correlation ID matching the request.
    pub id: u64,
    /// Result payload on success.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<ResultPayload>,
    /// Error message on failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    /// Create a successful response.
    pub fn ok(id: u64, result: ResultPayload) -> Self {
        Response {
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Create an error response.
    pub fn err(id: u64, error: impl Into<String>) -> Self {
        Response {
            id,
            result: None,
            error: Some(error.into()),
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_roundtrip_add_node() {
        let req = Request {
            id: 1,
            graph: "agent:planner".to_string(),
            auth_token: "abc123".to_string(),
            agent_id: None,
            method: Method::AddNode {
                node_id: "n1".to_string(),
                properties_msgpack: vec![
                    0x81, 0xa4, 0x74, 0x79, 0x70, 0x65, 0xa5, 0x41, 0x67, 0x65, 0x6e, 0x74,
                ],
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, 1);
        assert_eq!(parsed.graph, "agent:planner");
    }

    #[test]
    fn test_request_roundtrip_create_channel() {
        let req = Request {
            id: 42,
            graph: "__commons__".to_string(),
            auth_token: "tok".to_string(),
            agent_id: Some("agent:a".to_string()),
            method: Method::CreateChannel {
                channel_id: "channel:p2p:a:b".to_string(),
                channel_type: ChannelType::PeerToPeer,
                creator: "agent:a".to_string(),
                initial_members: vec!["agent:a".to_string(), "agent:b".to_string()],
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, 42);
        if let Method::CreateChannel { channel_type, .. } = parsed.method {
            assert_eq!(channel_type, ChannelType::PeerToPeer);
        } else {
            panic!("Wrong method variant");
        }
    }

    #[test]
    fn test_response_ok() {
        let resp = Response::ok(1, ResultPayload::Json(serde_json::json!({"count": 42})));
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"count\":42"));
        assert!(!json.contains("error"));
    }

    #[test]
    fn test_response_err() {
        let resp = Response::err(2, "node not found");
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("node not found"));
        assert!(!json.contains("result"));
    }

    #[test]
    fn test_all_graph_types_roundtrip() {
        for gt in [
            GraphType::Agent,
            GraphType::Team,
            GraphType::Global,
            GraphType::Commons,
        ] {
            let json = serde_json::to_string(&gt).unwrap();
            let parsed: GraphType = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, gt);
        }
    }

    #[test]
    fn test_method_ping_roundtrip() {
        let method = Method::Ping;
        let json = serde_json::to_string(&method).unwrap();
        let parsed: Method = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, Method::Ping));
    }

    #[test]
    fn test_method_pagerank_roundtrip() {
        let method = Method::PageRank {
            damping: 0.85,
            iterations: 100,
        };
        let json = serde_json::to_string(&method).unwrap();
        let parsed: Method = serde_json::from_str(&json).unwrap();
        if let Method::PageRank {
            damping,
            iterations,
        } = parsed
        {
            assert!((damping - 0.85).abs() < f64::EPSILON);
            assert_eq!(iterations, 100);
        } else {
            panic!("Wrong method");
        }
    }

    #[test]
    fn raw_result_payload_decodes_to_typed_value() {
        // Phase C-D compact encoding: a Raw payload carries the typed result as a
        // MessagePack bin. Over the wire it round-trips as a bin and decodes back
        // to the EXACT typed value the JSON path produced — what the Python client
        // does on any top-level `bytes` result.
        let scores: Vec<(String, f64)> = vec![("a".into(), 0.5), ("b".into(), 0.25)];
        let resp = Response::ok(7, ResultPayload::raw(&scores));
        let wire = rmp_serde::to_vec_named(&resp).unwrap();
        let decoded: Response = rmp_serde::from_slice(&wire).unwrap();
        // Untagged: a bin result decodes as the first bin-shaped variant; the inner
        // bytes are identical regardless of the variant name.
        let inner = match decoded.result {
            Some(ResultPayload::Raw(b)) | Some(ResultPayload::PropertiesMsgpack(b)) => b,
            other => panic!("expected a bin result payload, got {:?}", other),
        };
        let back: Vec<(String, f64)> = rmp_serde::from_slice(&inner).unwrap();
        assert_eq!(back, scores);
    }

    #[test]
    fn test_method_apply_mutation_roundtrip() {
        let method = Method::ApplyMutation {
            event_type: "TRIPLE_INSERT".to_string(),
            query: "INSERT DATA { <A> <B> <C> }".to_string(),
        };
        let json = serde_json::to_string(&method).unwrap();
        let parsed: Method = serde_json::from_str(&json).unwrap();
        if let Method::ApplyMutation { event_type, query } = parsed {
            assert_eq!(event_type, "TRIPLE_INSERT");
            assert_eq!(query, "INSERT DATA { <A> <B> <C> }");
        } else {
            panic!("Wrong method");
        }
    }
}
