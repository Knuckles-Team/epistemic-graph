macro_rules! __eg_method_chunk_3 {
    (@acc [$($variants:tt)*]) => {
        __eg_method_chunk_4!(@acc [
$($variants)*


    // ── Cluster topology discovery (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1) ──────
    // Engine-authoritative client-side cluster discovery, replacing the static
    // hand-maintained `GRAPH_RAFT_GROUP_ENDPOINTS` map (`reports/wave1/ADR-scale-trio.md`
    // §ADR-1). Each node self-reports its own `{node_id, raft_addr,
    // advertised_client_addr, tls_server_name}` via an internal typed Raft command; every node's
    // durable copy converges because the SAME committed log entry applies
    // deterministically on every replica (see
    // `server::persistence::node_info_store` docs) -- the SAME replication story
    // `CatalogAssign` uses for the M3 tenant catalog, NOT graph nodes (the
    // placement catalog's O(N) full-scan lesson). `ClusterMembers` cross-references
    // that durable store against every live `MultiRaft` group's membership/leader
    // to answer a complete topology snapshot from ANY reachable node -- not just
    // the leader, unlike `PlacementRoute` -- so a client's bounded seed-retry (ADR-1
    // decision 3/4) can re-resolve via any healthy contact.
    /// Read the current cluster topology (CONCEPT:EG-KG.sharding.cluster-topology): every known Raft
    /// group's members, each with its role (`leader`/`follower`/`learner`),
    /// health and client-reachable endpoint. The response is an authenticated,
    /// bounded discovery snapshot carrying a stable `cluster_id`, monotonic
    /// `membership_epoch`/`placement_epoch`, immutable member identities and
    /// certificate-rotation metadata. Gated `cluster:topology-read` -- NOT
    /// `admin:cluster-read` -- so an ordinary service role (not just a cluster
    /// operator) can discover where to reconnect after a failover. Always declared;
    /// a non-raft build or a raft build with no live `MultiRaft` answers a
    /// well-formed empty topology rather than an error. The Python client rejects
    /// unsigned, wrong-cluster, stale, or differently bound snapshots.
    ClusterMembers,

    // ── Fleet server registry (CONCEPT:EG-KG.sharding.server-registry, W2.5) ──────
    // Push-registration + lease-TTL heartbeat for fleet MCP/agent servers, writing
    // REAL, queryable knowledge-graph `:Server` nodes -- unlike the internal topology rows
    // above (deliberately NOT graph nodes, the placement O(N)-scan lesson), a
    // `:Server` node IS a first-class KG entity the fleet queries (`MATCH
    // (s:Server)-[:PROVIDES]->(r:CallableResource)`), the SAME shape
    // `agent_utilities.knowledge_graph.core.engine_ingestion.ingest_mcp_server`
    // writes today via `MERGE (s:Server {id: $id}) SET s.name=…, s.url=…,
    // s.timestamp=…` (Cypher, `node_type: "Server"` is the canonical label field
    // eg-query's CREATE/MERGE path sets — see `crates/eg-query/src/cypher/exec.rs`
    // `relationship_fixture`). `RegisterServer` self-translates into
    // `Method::AddNode` against `__commons__` (see `dispatch.rs`), reusing the
    // existing durable-commit + CDC + audit machinery byte-for-byte — the SAME
    // "translate then delegate" shape `Method::ApplyMultisigMutation` uses for
    // `Method::ApplyMutation`. Raft/cluster native-consensus wiring is a tracked
    // follow-up (`reports/issue-register.md`); single-node/`full` (the shipped
    // build) is fully wired.
    /// Push-register (or renew, idempotently) this server's fleet identity
    /// (CONCEPT:EG-KG.sharding.server-registry) as a `:Server` node in
    /// `__commons__`. `name` becomes the node id `srv:<name>` (must match
    /// `^[A-Za-z0-9_.-]{1,128}$`, the same bound au's config-sync ingestion
    /// enforces); `url` is a bounded opaque endpoint reference (never a raw
    /// credentialed URL — callers pass the same kind of privacy-safe reference
    /// au's `persistence_reference` produces); `resources_json` is an optional,
    /// size-bounded opaque JSON object (non-sensitive metadata, mirrors au's
    /// `_mcp_persistence_resources`); `ttl_secs` is the caller's requested lease
    /// duration (bounded server-side). The SERVER computes the absolute
    /// `lease_expires_at_ms`/`last_heartbeat_ms` from its own clock — it never
    /// trusts a caller-supplied timestamp. Re-calling with the SAME `name`
    /// renews the lease (a heartbeat is just a repeat `RegisterServer` call) and
    /// refreshes every other field, exactly like the internal topology self-report
    /// semantics; `registered_at_ms` is preserved from the prior row if one
    /// exists. A periodic engine sweep expires (removes) a `:Server` node whose
    /// lease has lapsed, emitting a CDC `RemoveNode` event. Returns `Bool` on
    /// success.
    RegisterServer {
        name: String,
        url: String,
        #[serde(default)]
        resources_json: String,
        ttl_secs: u64,
    },

    /// List the live fleet server registry from the engine-owned `__commons__`
    /// graph. The request graph is never an authority input: the server forces
    /// `__commons__`, applies the verified caller's graph ACL and row-level
    /// projection, and returns a keyset page sorted by UTF-8 server name. A
    /// continuation cursor is fenced by both the graph revision and a canonical
    /// digest of the complete live typed snapshot; a changed/expired row makes
    /// the cursor stale and requires a restart. In the single-node/full runtime
    /// this is an authoritative local committed read. A clustered deployment
    /// must not describe it as cluster-linearizable until a cluster read barrier
    /// is added to this route.
    ListRegisteredServers {
        request: crate::result_contract::cluster::RegisteredServerListRequest,
    },


    // ── Placement-catalog wire consumption (CONCEPT:EG-KG.sharding.placement-route-rpc, DIST-P2-4) ──
    // Exposes the engine's sole placement authority over the wire. The response is
    // complete even for an unplaced/single-node partition, so callers never hash or
    // guess. A configured Raft node without MultiRaft is an invalid cluster.
    /// Resolve `(tenant, sub_key)`'s current placement (CONCEPT:EG-KG.sharding.placement-route-rpc). `client_epoch`
    /// is the caller's last-known routing epoch for this partition (`0` if never
    /// resolved). Returns the schema-generated `PlacementRoute`. A placed route
    /// always has a non-zero epoch; an authoritative unplaced route uses epoch zero.
    PlacementRoute {
        request: crate::epistemic_operations::PlacementRouteRequest,
    },


    // ── Placement-catalog ADMIN mutations (CONCEPT:EG-KG.sharding.placement-catalog-admin-rpc,
    // DIST-P2-5) ─────────────────────────────────────────────────────────────
    // Before this existed, `PlacementCatalog`'s assign/split/merge/online-move machinery
    // (`src/raft/placement.rs`, `src/raft/reshard.rs::TenantManager`) was reachable ONLY
    // from in-process Rust (tests/harnesses) -- there was no wire method to actually
    // TRIGGER a placement decision or an online move from outside the engine process,
    // even on a real multi-group Raft cluster. This closes that gap: a thin RPC entry
    // point over the ALREADY-PROVEN `MultiRaft`/`TenantManager` admin API
    // (`src/server/handlers/placement.rs`), raft/cluster-only, admin-scoped
    // (`"admin:cluster"`, the same tier as `Reshard`/`CatalogAssign`). ONE `Method`
    // variant carrying a nested op enum (mirrors `ServedModality { op }` above) rather
    // than three flat variants, keeping the top-level `Method` enum's growth to +1.
    /// Drive the placement-catalog admin API (CONCEPT:EG-KG.sharding.placement-catalog-admin-rpc):
    /// [`PlacementAdminOp::Assign`] (the placement DECISION leg), [`PlacementAdminOp::Move`]
    /// (the full PLAN → EXECUTE → CATALOG-UPDATE leg — snapshot → per-graph
    /// durability-barrier catch-up → fenced cutover, reusing the already-proven
    /// `TenantManager::move_partition` state machine, crash-safe via its durable move
    /// journal), or [`PlacementAdminOp::AbortMove`] (roll back before the cutover
    /// fence). Raft/cluster only; a non-clustered build returns a typed "not available"
    /// error. Returns operation-specific JSON — see [`PlacementAdminOp`].
    PlacementAdmin {
        op: PlacementAdminOp,
    },


    // ── Online backup / restore + PITR (CONCEPT:EG-KG.sharding.reshard-on-restore) ──────────────
    // The wire surface for the DR ops the durable store now supports: an ONLINE
    // consistent backup (per-shard begin_read() MVCC snapshot, EG-027, streamed
    // verbatim to a portable bundle reusing EG-030's raw-row copy) and a restore
    // (verbatim import via the EG-030 engine). Redb-only; in a non-redb build they
    // return a clean "not available" error, exactly like the EG-038 admin surface.
    /// Take an ONLINE consistent backup under the operator-provisioned
    /// `EPISTEMIC_GRAPH_BACKUP_ROOT`. `destination` is a bounded logical bundle name,
    /// never a host path. The RPC is disabled when no private root is configured.
    Backup {
        destination: String,
        label: Option<String>,
    },

    /// Restore the logical bundle name `source` from the operator-provisioned backup
    /// root (CONCEPT:EG-KG.sharding.reshard-on-restore). The engine holds an
    /// exclusive lock on its live store, so this stages the rebuilt copy in an
    /// engine-owned sibling directory and returns only an opaque stage reference for
    /// the operator to correlate after stopping the engine;
    /// an in-place restore uses the offline `restore` CLI. Returns a `RestoreReport` JSON.
    Restore {
        source: String,
        /// Required current target layout. Setting this to a different value from the
        /// bundle proves restore-time migration rather than silently preserving K.
        target_shards: usize,
    },


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

    /// Cooperatively cancel an IN-FLIGHT request by its `target_req_id` (CONCEPT:EG-KG.query.streaming-spillable-collect,
    /// L36) — trips the `CancellationToken` the request-scoped registry (`server::request_cancel`)
    /// registered for it, if one is still live. A REAL `Method::Sql` read currently threads a
    /// registered token down to `collect_streaming`, which observes it at the next batch
    /// boundary and stops the stream short (chunk-granular, never mid-batch). Returns
    /// `ResultPayload::Bool(true)` iff a live cancellable request was found and cancelled,
    /// `false` when the request already finished, was never cancellable, or never existed —
    /// never an error (cancelling a request that already completed is a harmless no-op).
    CancelRequest {
        target_req_id: u64,
    },


    // ── Cost / Efficiency (CONCEPT:EG-KG.compute.lane-v, Lane V) ──────────────────
    /// Return one bounded, ACL-filtered ResourceStats page. `cursor` is an opaque
    /// exclusive graph-name key from the previous response's `next_cursor`; `limit`
    /// must be between one and the server's finite maximum; `summary` suppresses all
    /// per-graph/per-tenant arrays while retaining aggregate counters.  The body is
    /// the only current resource-statistics request shape.
    #[cfg(feature = "cost")]
    ResourceStatsPage {
        #[serde(default)]
        cursor: Option<String>,
        #[serde(default = "default_resource_stats_limit")]
        limit: usize,
        #[serde(default)]
        summary: bool,
    },

    Reconcile {
        graph_name: String,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        msgpack: Vec<u8>,
    },

    ApplyMutation {
        event_type: String,
        query: String,
    },

    /// VF2 subgraph isomorphism match of `pattern_graph_name` against this graph
    /// (CONCEPT:EG-KG.mining.gspan-frequent-subgraph). The backtracking search is NP-hard with no bound
    /// otherwise, so it stops early once it collects `max_results` matches or spends
    /// `max_steps` candidate-pair attempts (whichever first). `0` for either ⇒ the
    /// engine's conservative built-in default (`eg_core::graph::DEFAULT_VF2_MAX_RESULTS`/
    /// `DEFAULT_VF2_MAX_STEPS`) — a caller wanting more must ask for it explicitly.
    /// Returns a `Raw`-encoded [`Vf2MatchResult`].
    Vf2SubgraphMatch {
        pattern_graph_name: String,
        #[serde(default)]
        max_results: usize,
        #[serde(default)]
        max_steps: usize,
    },


    // ── AST Parsing ──────────────────────────────────────────────────
    ParseFile {
        file_path: String,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        source: Vec<u8>,
    },

    /// Batched parse: one round-trip for N files (CONCEPT:EG-KG.memory.forgetting-curve-decay). The blob is
    /// a MessagePack-encoded `Vec<(file_path, source_bytes)>`; the response is an
    /// ordered `Vec<ParseResult>`, one per input file. Mirrors `BatchUpdate`.
    ParseFiles {
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        files_msgpack: Vec<u8>,
    },

    /// Parse a batch AND resolve cross-file call/import edges in one round-trip
    /// (CONCEPT:EG-KG.compute.turn-each-project). The blob is the same MessagePack `Vec<(file_path,
    /// source_bytes)>` as `ParseFiles`, but the batch is treated as one
    /// resolution scope (a repository, or a delta set): the response is a SINGLE
    /// resolved `IndexResult` whose `calls`/`depends_on` edges point at real node
    /// ids, not bare names. Use this (not `ParseFiles`) to ingest a repo's symbol
    /// graph; use `ParseFiles` only when per-file raw results are wanted.
    IndexRepository {
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        files_msgpack: Vec<u8>,
    },


    // ── Screen Observation (computer-use) ─────────────────────────────
    /// Turn a captured desktop frame into durable session/frame/UIElement graph
    /// entities in one round-trip (CONCEPT:AU-KG.ontology.owl-screen-bridge). The blob is a MessagePack map
    /// `{session_id, frame_seq, prev_frame_id, prev_hash, png: bin, elements: [..]}`;
    /// the response is a SINGLE `ScreenObservationResult` (nodes + edges), mirroring
    /// `IndexRepository`. The screenshot bytes never persist — only its dimensions +
    /// content hash do, for frame-diff.
    ObserveScreen {
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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

    /// CONCEPT:EG-KG.retrieval.one-round-trip-discovery — one-round-trip hybrid discovery. Given the caller's
    /// de-duplicated `keywords` plus a `query_embedding`, dense-retrieve candidate
    /// nodes via the HNSW index (the same batch primitive as `SemanticSearch`),
    /// then re-rank each by BOTH its semantic similarity AND lexical keyword
    /// overlap over its `name`/`description`/`type`, returning the top-`k` with
    /// their human-readable text as `[{id,name,description,type,score}, …]`.
    /// Complements `SemanticSearch` (which returns bare `(id, score)`): Discover
    /// folds the keyword signal in and hydrates the result text in one call, so a
    /// router/orchestrator gets a ready-to-read shortlist without an N+1 metadata
    /// fetch. An empty `query_embedding` (embedder/vLLM unavailable) degrades to a
    /// bounded keyword-only scan.
    Discover {
        keywords: Vec<String>,
        query_embedding: Vec<f32>,
        k: usize,
    },

    /// CONCEPT:EG-ORCH.routing.lexical-capability-escalation — embedding-free lexical classification gate: which
    /// capability-node terms (Tool/Skill/MCPServer names+synonyms) appear in the
    /// query. The "free" tier between structural routing and `SemanticSearch`.
    MatchOntologyTerms {
        query: String,
    },

    /// CONCEPT:EG-KG.compute.l2-normalize-batch-vectors — L2-normalize a batch of vectors IN-ENGINE via the `eg-numeric`
    /// kernel (compute-near-data over a resident vector set): returns each row's unit
    /// vector `v/‖v‖` (feature `numeric`).
    BatchL2Normalize {
        vectors: Vec<Vec<f64>>,
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


    // ── Data Science Primitives (CONCEPT:EG-KG.compute.rust-native-training-loss) ─────────────────────
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
        shuffle: bool,
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


    // ── Training loss / optimizer kernels (CONCEPT:EG-KG.compute.rust-native-training-loss) ────────────
    DsSoftmax {
        logits: Vec<f64>,
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
        beta: f64,
    },

    DsGrpoSurrogate {
        logprob: Vec<f64>,
        old_logprob: Vec<f64>,
        advantage: Vec<f64>,
        clip_eps: f64,
    },

    DsKlDivergence {
        logprob: Vec<f64>,
        ref_logprob: Vec<f64>,
    },

    DsAdamStep {
        params: Vec<f64>,
        grads: Vec<f64>,
        m: Vec<f64>,
        v: Vec<f64>,
        lr: f64,
        beta1: f64,
        beta2: f64,
        eps: f64,
        t: u64,
    },

    DsSgdStep {
        params: Vec<f64>,
        grads: Vec<f64>,
        lr: f64,
    },


    // ── Extended Finance: Risk (CONCEPT:AU-KG.memory.mementified-context) ──────────────────────
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
        ]);
    };
}

pub(crate) use __eg_method_chunk_3;
