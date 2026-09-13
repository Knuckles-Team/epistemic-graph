macro_rules! __eg_method_chunk_2 {
    () => {
        __eg_method_chunk_3!(@acc [


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

    /// Batch neighbor read: fetch neighbor ids for many nodes in ONE round-trip
    /// instead of N `GetNeighbors` calls (D-DPF-1 — the N+1 this closes). Returns
    /// a `Raw` list of `[node_id, Vec<String>]` in input order; a missing/absent
    /// node yields an empty neighbor list rather than failing the whole batch, so
    /// one bad id in a large discover-then-hydrate batch cannot sink the rest.
    /// Bounded by `MAX_BATCH_IDS`.
    GetNeighborsBatch {
        node_ids: Vec<String>,
    },


    // ── Cross-graph union reads (CONCEPT:EG-KG.query.cross-graph-union) ───────────────────
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

    /// Native entity-resolution candidate generator (CONCEPT:AU-KG.compute.when-exposes-native) — composes
    /// embedding similarity + clustering into one server-side READ op that returns
    /// merge proposals (same_as / extends). Never mutates; the client applies via
    /// `BatchUpdate`. The escalation tier for the agent-utilities dedup ladder.
    ResolveCandidates {
        sim_threshold: f64,
        merge_threshold: f64,
        #[serde(default)]
        node_type: Option<String>,
    },


    // ── Hierarchical cluster visualization (VIZ-1, CONCEPT:EG-KG.compute.leiden-hierarchy) ──
    // Server-side hierarchical (Leiden) clustering, exposed for million-node
    // graph visualization: a client renders a few thousand top-level CLUSTER
    // nodes and drills in on demand instead of laying out every node/edge
    // client-side. Deliberately NOT `mutation::GATEWAY_ROUTED` and NOT a KG
    // write — the computed hierarchy is durably cached in its OWN store
    // (`server::persistence::cluster_hierarchy_store`, `PersistenceBackend::
    // save_cluster_hierarchy`/`load_cluster_hierarchy`), never as graph nodes
    // or edges: the engine holds at most one edge per ordered node pair and
    // `upsert_edge` REPLACES the relationship type, so representing cluster
    // membership as edges between existing nodes would silently destroy
    // asserted relationships (measured defect on this program). Sits in the
    // SAME "Graph Algorithms" read-only bucket as `CommunityDetection` above
    // (`compute:graph-algo`, `mutates: false` in `eg_capabilities::policy` —
    // the cache is a non-authoritative, always-recomputable-from-the-graph
    // derived artifact, exactly like the plan-backed matview result cache).
    /// (Re)compute the hierarchy for the request's graph and persist it,
    /// replacing any previously cached hierarchy for the same graph.
    /// `ClusterHierarchyClusters`/`ClusterHierarchyExpand` serve the cached
    /// result until this is called again — "refreshable", not
    /// recomputed-per-request.
    ClusterHierarchyRefresh {
        /// Optional: restrict the clustered projection to nodes of this one
        /// type — mirrors `MineCommunity::label`.
        #[serde(default)]
        label: Option<String>,
        /// Leiden resolution γ (higher ⇒ more, smaller clusters).
        #[serde(default = "default_cluster_hierarchy_resolution")]
        resolution: f64,
        #[serde(default)]
        seed: u64,
    },

    /// `GET clusters(graph, level, parent_cluster_id?)` — one level of the
    /// cached hierarchy, optionally restricted to one parent's children.
    /// `level` 1 is the finest computed level; `parent_cluster_id` unset
    /// returns every cluster at `level`. Errors if no hierarchy is cached yet
    /// (call `ClusterHierarchyRefresh` first).
    ClusterHierarchyClusters {
        level: usize,
        #[serde(default)]
        parent_cluster_id: Option<String>,
    },

    /// `GET expand(graph, cluster_id)` — a level-1 cluster's individual member
    /// nodes/edges (read live off the current graph, keyed by the cached
    /// membership) plus its child clusters (empty for a level-1 cluster, since
    /// level 1 is the finest computed level).
    ClusterHierarchyExpand {
        cluster_id: String,
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        operations_msgpack: Vec<u8>,
    },

    /// Batched CROSS-GRAPH write (CONCEPT:EG-KG.storage.multi-graph-batch-write). One
    /// round-trip carries a `BatchUpdate`-shaped op list for MANY named graphs; the
    /// server applies each graph's sub-batch through the normal per-graph write
    /// path CONCURRENTLY, so N distinct graphs commit across N of the K redb shard
    /// writers in parallel instead of the client serializing N round-trips that
    /// each re-acquire one lock. `batches_msgpack` decodes to
    /// `Vec<(graph_name, operations_msgpack)>` — each inner blob is exactly a
    /// `BatchUpdate.operations_msgpack`, so it REUSES the existing batch primitive
    /// (no new per-op op). Carries its graphs in the METHOD (like `NlQuery`), so it
    /// is routed BEFORE the single-`graph` graph-op path in dispatch. The reply is
    /// `{ "results": { graph: <batch_result> }, "errors": { graph: msg } }`.
    MultiGraphBatchUpdate {
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        batches_msgpack: Vec<u8>,
    },

    Metrics,

    EvictLRU {
        max_nodes: usize,
    },


    // ── Temporal Decay (CONCEPT:EG-KG.memory.forgetting-curve-decay — Ebbinghaus forgetting curve) ──
    DecaySweep {
        half_life_secs: f64,
        floor: f64,
        prune: bool,
    },

    TouchNodes {
        node_ids: Vec<String>,
    },


    // ── Serialization ────────────────────────────────────────────────
    ToMsgpack,

    FromMsgpack {
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        msgpack: Vec<u8>,
    },


    // ── Ledger ───────────────────────────────────────────────────────
    GetLedger,

    ClearLedger,

    ApplyLedger {
        transactions: Vec<String>,
    },

    // Tamper-evident audit log verification (CONCEPT:EG-KG.sharding.row-level-security, feature `security`):
    // walk the target graph's hash-chained audit log and report OK or the first break.
    #[cfg(feature = "security")]
    AuditVerify,

    /// Produce + server-side-verify a Merkle inclusion proof for one node against
    /// a prior provenance anchor (CONCEPT:EG-KG.sharding.row-level-security, feature `security`) — the
    /// extension that lets [`AuditVerify`](Method::AuditVerify)'s tamper-evidence
    /// reach the ANCHORED NODES' CONTENT, not just the ordering of mutations. A
    /// periodic engine job Merkle-hashes the target graph's `:ToolCall`/
    /// `:RunTrace` provenance-node window and folds the root into this SAME
    /// hash chain as one more entry (`audit::provenance_anchor_line`); this
    /// method re-hashes `node_id`'s CURRENT durable content and walks the
    /// anchor-time sibling path up to that chain-protected root — a mismatch
    /// (`MerkleInclusionReport.verified == false`) proves the node's durable
    /// bytes changed after anchoring, whether by raw tampering or an ordinary
    /// later overwrite. `anchor_seq` selects a specific anchor by its
    /// audit-chain seq; `None` uses the target graph's most recent anchor.
    /// Errors when the graph has no anchor yet or `anchor_seq` names an entry
    /// that is not one; `included == false` (not an error) means `node_id`
    /// simply was not part of that anchor's window. Returns
    /// `Raw(MerkleInclusionReport)`.
    #[cfg(feature = "security")]
    AuditProveInclusion {
        node_id: String,
        anchor_seq: Option<u64>,
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
    // CONCEPT:EG-KG.compute.compiled-semantic-reasoner - Compiled Semantic Reasoner. A single round of
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


    // ── Governed change ingestion ────────────────────────────────────
    /// Atomically materialize one externally sourced object and all of its
    /// governance/provenance state. The embedded MutationBatch is the graph-row
    /// mutation authority; blob/feature/evidence/policy/lineage, content version,
    /// typed cursor, durable status, and outbox share its commit point.
    ApplyChangeEnvelope {
        envelope: Box<crate::change_envelope::ChangeEnvelope>,
    },

    /// Atomically materialize a BATCH of externally sourced objects. Envelopes are
    /// grouped by their `mutation.graph` and each graph's envelopes land in ONE
    /// coalesced redb transaction (the atomic graph-batch); envelopes spanning
    /// graphs split into independent per-graph sub-batches. Within a graph-batch the
    /// commit is all-or-nothing — a single failing envelope aborts that graph's
    /// transaction and every envelope in the graph reports the batch outcome. Across
    /// graphs the sub-batches are independent (partial success). Per-envelope results
    /// (applied / idempotent-skip / conflict) are returned in request order. Same
    /// policy class as `ApplyChangeEnvelope`. Bounded by `MAX_ENVELOPES_PER_BATCH`.
    ApplyChangeEnvelopes {
        envelopes: Vec<crate::change_envelope::ChangeEnvelope>,
    },

    /// Read a committed envelope by stable identity for retry reconciliation.
    GetChangeEnvelope {
        envelope_id: String,
        tenant: String,
    },

    /// Read the current typed content version for an object in this graph/tenant.
    GetContentVersion {
        object_id: String,
        tenant: String,
    },

    /// Read the current typed source cursor. Cursors are partition scoped and are
    /// never compared as strings.
    GetChangeCursor {
        source: String,
        #[serde(default)]
        partition: String,
        tenant: String,
    },


    // ── Governed document/image/audio/video serving ─────────────────────
    // Graph-scoped and available in the one main build. Authority comes only from
    // the verified request context; no caller-supplied tenant or policy scope is
    // accepted by the operation DTO.
    #[cfg(feature = "modality-serving")]
    ServedModality {
        op: crate::modality::ServedModalityOp,
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


    // ── M3 catalog-driven resharding admin (CONCEPT:EG-KG.backend.m3-admin-dispatch) ───────────
    // The wire surface that DRIVES the M3 ops the engine already has the building
    // blocks for: online single-node resharding (EG-032), the tenant catalog
    // (EG-031), and the rebalancing planner (EG-035) + its execution (EG-039). All
    // are redb-only; in a non-redb build they return a clean "not available" error.
    /// Online-move `graph`'s durable rows to shard `to_shard` while the engine RUNS,
    /// then flip the catalog route (CONCEPT:EG-KG.backend.catalog-shard-resolve). Returns a `ReshardReport` JSON.
    Reshard {
        graph: String,
        to_shard: u32,
    },

    /// Populate / assign an explicit catalog placement for `graph` (CONCEPT:EG-KG.sharding.empty-catalog-routing).
    /// Flips the ROUTE only — to MOVE the rows too use `Reshard`. Returns `Bool`.
    CatalogAssign {
        graph: String,
        shard: u32,
        node: Option<u32>,
    },

    /// Re-place `graph` onto `shard`, preserving its node placement (CONCEPT:EG-KG.sharding.empty-catalog-routing).
    CatalogReassign {
        graph: String,
        shard: u32,
    },

    /// Drop `graph`'s explicit placement — it reverts to EG-026 FNV-1a routing.
    CatalogRemove {
        graph: String,
    },

    /// List every explicit catalog placement `{graph, shard, node}` (JSON).
    CatalogList,

    /// Compute (do NOT execute) a rebalance plan over live per-shard/per-graph load
    /// (CONCEPT:EG-KG.sharding.even-load-rebalance). Returns the ordered `{graph, from_shard, to_shard}` moves +
    /// the per-shard load it planned against, as JSON.
    RebalancePlan {
        tolerance: Option<f64>,
        max_moves: Option<usize>,
    },

    /// Compute a rebalance plan AND execute it move-by-move via online resharding
    /// (CONCEPT:EG-KG.backend.r3-plan-execution, R3 plan execution). Each move is one online `Reshard` — online,
    /// one graph at a time, other graphs unaffected. Returns the executed moves' reports.
    RebalanceExecute {
        tolerance: Option<f64>,
        max_moves: Option<usize>,
    },


    // ── Raft cluster membership admin (CONCEPT:EG-KG.storage.kg-kg-2 — cluster_deployment.md §5 item 2) ──
    // The wire surface that lets an operator attach a fresh node to a LIVE Raft
    // group without a bespoke binary. `MultiRaft::add_group_learner` /
    // `change_group_voters` (src/raft/multi.rs) already implement the openraft
    // add-learner / change-membership lifecycle — they simply had NO external
    // caller, so §2c of the cluster deployment runbook could not actually be
    // driven outside the in-process test harness. Both ops are leader-only; a
    // follower answers `OPERATION_REDIRECTED` naming the current leader
    // (mirroring `PlacementRoute`'s stale-route redirect), and an engine with
    // no live `MultiRaft` returns a clean typed error rather than a silent
    // no-op. Always declared (like the M3 admin block above); the real answer
    // is `raft`-gated, and a non-raft build returns "not available".
    /// Attach `node_id` (reachable at `addr`) to `group` as a NON-VOTING
    /// LEARNER (CONCEPT:EG-KG.storage.kg-kg-2). Starts replication immediately and BLOCKS
    /// until the learner's log is caught up, but does NOT change the voter
    /// set — quorum size and fault tolerance are unaffected. The safe,
    /// always-available first step before optionally promoting the node with
    /// `RaftChangeMembership`. `group` defaults to the single-group
    /// deployment's `raft::DEFAULT_GROUP` (0) when omitted. Returns `Bool` on
    /// success.
    RaftAddLearner {
        group: Option<u64>,
        node_id: u64,
        addr: String,
    },

    /// Set `group`'s VOTER set to exactly `voters` (CONCEPT:EG-KG.storage.kg-kg-2) — openraft
    /// `change_membership`. The usual way to PROMOTE one or more learners
    /// added via `RaftAddLearner`: pass the full desired voter set (existing
    /// voters plus the learner(s) being promoted). Refuses to produce an
    /// empty voter set. `group` defaults to `raft::DEFAULT_GROUP` (0) when
    /// omitted. Returns `Bool` on success.
    RaftChangeMembership {
        group: Option<u64>,
        voters: Vec<u64>,
    },
        ]);
    };
}

pub(crate) use __eg_method_chunk_2;
