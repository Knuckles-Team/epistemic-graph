macro_rules! __eg_method_chunk_10 {
    (@acc [$($variants:tt)*]) => {
        __eg_method_chunk_11!(@acc [
$($variants)*


    /// Community detection as a mining family (CONCEPT:EG-KG.mining.community-writeback):
    /// wraps the EXISTING GDS Louvain / label-propagation kernels
    /// (`eg_compute::graph_algos`, already exposed on the Cypher `CALL gds.*`
    /// surface) — adds NO new algorithm, only the epistemic writeback. Runs over
    /// the resident graph (optionally restricted to one node `label`, like
    /// `MineSubgraph`). With `writeback=true` materializes each community as a
    /// typed `:Community` node linked to its members — a graph MUTATION,
    /// WAL-replayed by re-detecting deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineCommunity {
        /// Optional: restrict the projected graph to nodes of this one type.
        #[serde(default)]
        label: Option<String>,
        /// Which existing GDS kernel to run.
        #[serde(default)]
        algorithm: CommunityAlgorithm,
        /// Louvain modularity resolution (ignored by label-propagation).
        #[serde(default = "default_resolution")]
        resolution: f64,
        /// Iteration/sweep cap.
        #[serde(default = "default_max_iter")]
        max_iterations: usize,
        /// Seed for Louvain's deterministic shuffle (ignored by label-propagation).
        #[serde(default)]
        seed: u64,
        /// Weight neighbor votes by edge weight (label-propagation only; ignored by Louvain).
        #[serde(default = "default_true")]
        weighted: bool,
        /// Materialize each community as a typed `:Community` node linked to its members.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per community (E6)
        /// — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from
        /// the community's own internal-edge density (already `[0,1]`). Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    // ── Decide layer (RF-ADR-010) ──────────────────────────────────────────
    /// Typed task/capability requirements -> a validated `AgentGraph` draft
    /// with coverage derivations and a verifiable certificate, or a typed
    /// abstention. Reads one tenant-bound agent-library snapshot; commits
    /// nothing. Boxed like `SemanticIndex`/`KgDelegate` so one request cannot
    /// set the size of every `Method`.
    AgentAssemble {
        request: Box<crate::decision::AssemblyRequest>,
    },
    /// Read-only capability coverage (EG-DECISION-ENGINE-R126.2.1): for each
    /// capability in the closure of native task/capability IRIs, the visible
    /// Agent Library components that cover it (with derivation premises and
    /// evidence class) and the registered A2A agent cards that self-declare it
    /// (always claims). Reads one tenant-bound agent-library snapshot; ranks
    /// nothing, commits nothing.
    CapabilityCoverage {
        request: Box<crate::decision::coverage::CapabilityCoverageRequest>,
    },
    /// Snapshot-checked commit of one assembly `DecisionRecord` as a
    /// `DecisionRecord` component. Re-derives the record and compares the
    /// catalog digest before writing, so a stale decision is refused rather
    /// than stored.
    DecisionCommit {
        request: Box<crate::decision::DecisionCommitRequest>,
    },
    /// Statistical decision over library or RLS-filtered graph candidates.
    /// EVALUATE-ONLY: it answers records and commits none of them.
    Decide {
        request: Box<crate::decision::DecideRequest>,
    },
    /// Admin job: fit a decision head into a Blob CAS draft artifact.
    DecisionFit {
        op: Box<crate::decision::DecisionFitOp>,
    },
    /// Admin job: off-policy and calibration evaluation producing the
    /// promotion receipt a head must carry before it may be published.
    DecisionEval {
        op: Box<crate::decision::DecisionEvalOp>,
    },
    /// The statistical decision log: commit an acted-on record after
    /// verify-replay, join independent outcome evaluations, and read the
    /// outcome aggregate the log answers.
    DecisionLog {
        op: Box<crate::decision::statistical::log::DecisionLogOp>,
    },
    /// General bounded 0-1 integer programme with an independently verifiable
    /// certificate. Pure compute: no store, no clock, no float.
    Solve {
        request: Box<crate::solve::SolveRequest>,
    },

    // ── Connector MCP pack import (RF-ADR-009) ─────────────────────────────
    /// Atomic connector MCP pack import and its admin surface.
    ConnectorPack {
        op: Box<crate::connector_pack::ConnectorPackOp>,
    },

    /// RF-ADR-009 native ingestion authority. Resolves the exact Connector
    /// Manifest mapping under the verified tenant, admits raw records to CAS,
    /// maps them and atomically commits graph material, provenance and cursor.
    SourceIngest {
        request: Box<crate::source_ingestion::SourceIngestionRequest>,
    },

    /// Latest authoritative SourceIngest checkpoint for restart/failover CAS
    /// recovery. Tenant and graph are taken only from verified request context.
    SourceIngestStatus {
        connector: crate::contract::ResourceId,
        stream: crate::contract::ResourceId,
    },
    /// EG-owned D18 source change sets and append-only write-back receipts.
    /// The engine records governed observations; it never calls a vendor API.
    WriteBack {
        op: Box<crate::write_back::WriteBackOp>,
    },

    // ── Graph schema sources and the mutation outbox (X9, X10) ─────────────
    /// Attach, replace or detach one keyed schema source on the request graph.
    GraphSchema {
        op: Box<crate::graph_schema::GraphSchemaOp>,
    },
    /// List the request graph's schema sources and its composed digest.
    GraphSchemaList,
    /// One page of the classes and properties the request graph's composed
    /// schema declares, ordered by `(iri, kind)` (EH-389). `kind` keeps one
    /// term kind; `cursor` is the previous page's `next_cursor`; `limit` is
    /// 1..=`MAX_GRAPH_SCHEMA_TERMS_PAGE`.
    GraphSchemaClasses {
        #[serde(default)]
        kind: Option<crate::graph_schema::GraphSchemaTermKind>,
        #[serde(default)]
        cursor: Option<String>,
        limit: u32,
    },
    /// Operator view and in-order re-delivery of one owner's mutation outbox.
    MutationOutbox {
        op: Box<crate::mutation_outbox::MutationOutboxOp>,
    },

    // ── User-managed edge-native indexes (EH-351 / EH-352) ─────────────────
    /// Create, refresh, drop or list the edge indexes of the request graph.
    EdgeIndex {
        op: Box<crate::managed_index::EdgeIndexOp>,
    },
    /// Search one edge index of the request graph; edges come back as edges.
    EdgeSearch {
        request: Box<crate::managed_index::EdgeSearchRequest>,
    },
        ]);
    };
}

pub(crate) use __eg_method_chunk_10;
