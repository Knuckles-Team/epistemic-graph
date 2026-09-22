//! The read-method RLS coverage classification.
//!
//! Split out of `server::access` because it is one self-contained ratchet: a
//! partition of every non-mutating method into three documented buckets, plus
//! the reason each non-row-scoped entry cites. `access.rs` keeps the
//! authority types and the per-request checks; this file keeps the inventory
//! they are audited against. Both architecture gates read the module TREE, so
//! moving the inventory here changes nothing they can see.

// ── L-RLS-1 (next-level-analysis report §9 item #10): read-method RLS coverage ──

/// Every non-mutating (`mutates == false`) protocol method falls into EXACTLY
/// one documented bucket below — there is no silent fourth category. Mirrors
/// `server::mutation::tests::gateway_routed_set_matches_mutating_policy_surface`'s
/// exhaustive-partition idiom (mutation side) on the READ side.
///
/// **[`RLS_ROUTED`]** — the method's serving handler filters its `GraphView`/
/// `GraphCore` through [`GraphReadAuthority::filter_view`]/
/// [`GraphReadAuthority::project_core`] (which call
/// `crate::isolation::IsolationLayer::can_see_row`,
/// `crates/eg-core/src/isolation.rs`) before any row reaches the caller. Every
/// entry was confirmed by reading its owning handler's actual `project_core`/
/// `filter_view` call site, not assumed from the method's name — see
/// `every_read_method_routes_through_rls_or_is_a_documented_exception`'s own
/// test module for the file:line evidence recorded per group.
///
/// **[`NON_ROW_SCOPED`]** — the method never constructs a `GraphView`/
/// `GraphCore` at all: either it is pure compute over caller-supplied data
/// with no server-held row (finance/datascience math, a sandboxed WASM UDF),
/// or it reads a DIFFERENT store whose authority is a verified tenant/actor/
/// owner/membership scope rather than per-node `_owner`/`_visibility`/
/// `_grants` — each entry names the real mechanism and why RLS's public/
/// grants sharing model does not apply to that store's data shape.
/// `TsRange`/`TsAsofJoin`/`TsWindow`/`TsGapFill`
/// (`src/server/handlers/timeseries.rs`) are the exact gap the next-level-
/// analysis report §9 item #10 named; see that file's module doc for the full
/// reasoning [`REASON_TENANT_KEY_SCOPED_SERIES`] cites.
///
/// **[`NOT_YET_AUDITED`]** — this pass did not trace the method's handler to
/// a `project_core`/`filter_view` call with enough confidence to assert
/// either of the above buckets. Deliberately distinct from both: a flagged,
/// visible TODO for a human/future pass, never a silent assumption of safety
/// in either direction.
///
/// Ratchet, mirroring the mutation-side gate: `UNDOCUMENTED` (a method whose
/// name matches none of the three lists) is the only HARD test failure below.
/// Shrinking [`NOT_YET_AUDITED`] to zero — by moving an entry into
/// `RLS_ROUTED` or `NON_ROW_SCOPED` backed by a real, cited call site — is the
/// intended burn-down, exactly like `OPEN_NOT_JUSTIFIED` on the mutation side.
///
/// **Blind spot this method-level ratchet cannot see, and where its coverage
/// continues (W1b / L-RLS-2):** `AnalyticsJob` never appears in `RLS_ROUTED`/
/// `NON_ROW_SCOPED`/`NOT_YET_AUDITED` below at all, because
/// `eg_capabilities`'s static policy table classifies the WHOLE method
/// `mutates: true` (a conservative upper bound — one wire method carries
/// `Submit`/`Status`/`Cancel`/`Resume` `op`s of different shapes, and `Status`
/// is genuinely a read), so it never reaches this file's `!p.mutates` read
/// partition. The actual per-request row-read question for `AnalyticsJob`
/// isn't "does this method read a row" (bundled with three writes, it always
/// says yes) but "does THIS SUBMISSION's `JobKind` read a graph row
/// server-side" — a question this method-granularity table cannot express.
/// That finer axis is ratcheted separately, at job-KIND granularity, by
/// `handlers::jobs::reads_graph_rows_server_side` (an exhaustive, no-wildcard
/// match a new `JobKind` variant cannot silently skip) plus the architecture
/// test in `handlers::jobs_read_rls_architecture` (declared from
/// `handlers::jobs`'s own test module), which independently cross-checks that
/// classification and textually pins `handle_submit`'s fail-closed ordering.
/// Both ratchets currently agree: zero shipped read surfaces of any kind
/// bypass RLS without a documented, checked reason.
pub(super) const RLS_ROUTED: &[&str] = &[
    "BatchL2Normalize",
    "BestTrajectory",
    "BetweennessCentrality",
    "CdcRead",
    "CommunityDetectEphemeral",
    "CommunityDetection",
    "ComputeSimilarityEdges",
    "ConnectedComponents",
    "DegreeCentrality",
    "DegreeCentralityAll",
    "DiffAgainst",
    "DiscountedReturn",
    "Discover",
    "DistributedCompute",
    "EdgeCount",
    "EpistemicStatus",
    "ExplainBelief",
    "ExplainEvidence",
    "ExplainPlan",
    "ExplainPolicy",
    "ExplainProvenance",
    "ExplainProvenanceByIds",
    "FindCycle",
    "FiredTriggers",
    "Fork",
    "GetBlastRadius",
    "GetContextView",
    "GetEdgeProperties",
    "GetEdgePropertiesBatch",
    "GetEdges",
    "GetEdgesPage",
    "GetMatView",
    "GetNeighbors",
    "GetNeighborsBatch",
    "GetNodeProperties",
    "GetNodePropertiesBatch",
    "GetNodes",
    "GetNodesByLabel",
    "GetPredecessors",
    "GetRdf",
    "GetShortestPath",
    "GetSubgraph",
    "GetSuccessors",
    "GraphColoring",
    "HasEdge",
    "HasNode",
    "HasNodesBatch",
    "InDegree",
    "IndexRepository",
    "KnowledgeStream",
    "ListGraphs",
    "ListRegisteredServers",
    "ListTriggers",
    "MatchOntologyTerms",
    "MaterializationStatus",
    "MineClassifyFit",
    // t1-grounding-0802 follow-up audit: `handlers::pipeline::try_handle` (module doc,
    // `src/server/handlers/pipeline.rs`) receives the SAME `core.clone()` the mining/
    // graphlearn handlers ahead of it in dispatch.rs's `'dispatch` block do -- the
    // already-`project_core`-projected core built once at the top of
    // `graph_ops::try_handle`. `Evaluate`/`Compare`'s feature-extraction steps read
    // the live subgraph through that core; `Train`/`Serve`/`Predict` are separately
    // GATEWAY_ROUTED writes.
    "MiningPipelineCompare",
    "MiningPipelineEvaluate",
    "MinimumSpanningTree",
    "NlQuery",
    "NodeCount",
    "NodeIds",
    "ObserveScreen",
    "OutDegree",
    "OwlExplain",
    "OwlReason",
    "OwlReasonDistributed",
    "PageRank",
    "ParseFile",
    "ParseFiles",
    "PersonalizedPageRank",
    "PlanMatViewGet",
    "ReadContinuousQuery",
    "ResolveCandidates",
    "ResolveConflict",
    "RunRules",
    "SceneChildren",
    "SemanticSearch",
    "ShaclValidate",
    "ShexValidate",
    "Sparql",
    "SparqlVirtual",
    "StaleMaterializations",
    "StreamCommittedOffset",
    "StreamRead",
    "StronglyConnectedComponents",
    "SummariesAtLevel",
    "SummaryChildren",
    "ToMsgpack",
    "TopologicalSort",
    "TxnUnifiedQuery",
    "TxnUnifiedQueryText",
    "UnifiedQuery",
    "UnifiedQueryText",
    "UnionGetNeighbors",
    "UnionGetNodeProperties",
    "UnionGetNodesByLabel",
    "Vf2SubgraphMatch",
    "Watch",
    "WhatChanged",
    "WorldTransform",
];

pub(super) const REASON_SERVER_LIFECYCLE: &str =
    "server-lifecycle / liveness methods touch no tenant-owned row";
pub(super) const REASON_AUDIT_CHAIN_ADMIN_GATED: &str =
    "AuditVerify/AuditProveInclusion walk the hash-chained audit log (incl. its provenance-anchor entries) under the kg:admin capability gate -- not a graph row read";
// BUG A1 (2026-08-12): GetLedger was previously (wrongly) RLS_ROUTED. Its handler
// (`handlers::graph_ops::try_handle`, `Method::GetLedger` arm) reads the mutation
// ledger off `raw_core` (captured before `read_authority.project_core` shadows
// `core`), the SAME raw-core pattern `raw_ledger_len` uses just above in that file
// for `Metrics.total_mutations` -- the ledger is process-observability (an
// audit/debug trail of every committed mutation string), never row-visible node/edge
// data, so RLS's per-row `_owner`/`_visibility`/`_grants` model does not apply to it
// at all. It is instead authorized by its own dedicated `ledger:read` RBAC action
// (`eg_capabilities::policy`), enforced in `dispatch.rs` (`verified_context.
// allows_method`) BEFORE any handler runs -- routing it through row-level
// `project_core` as well was a redundant SECOND gate that, instead of narrowing
// visibility, silently destroyed the data: `project_core`'s detached copy is built
// via `add_node_no_ledger`/`add_edge_no_ledger` and therefore never carries a ledger
// at all (see `build_projection`'s own doc), so GetLedger returned `[]` on every
// request in the default (`security`-compiled) build regardless of how many
// mutations had committed.
pub(super) const REASON_LEDGER_ADMIN_OBSERVABILITY: &str =
    "src/server/handlers/graph_ops.rs's GetLedger arm reads raw_core.get_ledger() (captured before project_core shadows core, same pattern as raw_ledger_len for Metrics.total_mutations) -- the mutation ledger is process-observability, not row-visible data, and is gated by its own ledger:read RBAC authz_action (eg_capabilities::policy) enforced in dispatch.rs before any handler runs, not by per-row RLS";
pub(super) const REASON_VERIFIED_TENANT_CLAIM: &str =
    "dispatch.rs compares the request's tenant against verified_context.claims().tenant before serving it (GetChangeEnvelope/GetContentVersion/GetChangeCursor) -- an explicit verified-tenant-claim check, not a graph row";
pub(super) const REASON_NATIVE_CAPACITY_TENANT_GATED: &str =
    "CapacityStatus/ReconcileCapacity page native capacity CELLS and LEASES out of redb (redb_store::capacity_lease::read) -- control-plane rows keyed by (graph, cell/lease id), never a GraphView node/edge row, so there is no row for RLS to filter. dispatch.rs compares the request's tenant against verified_context.tenant() and requires kg:admin or capacity:read:aggregate to cross it, on top of the capacity:read method gate in allows_method";
pub(super) const REASON_CLUSTER_ADMIN_GATED: &str =
    "cluster-wide / control-plane methods gated by the kg:admin capability (IsolationLayer::require_admin_capability) -- span the whole registry/cluster, not one resolved graph's rows, mirroring mutation.rs's own NON_GATEWAY_COORDINATED classification of the equivalent admin writes";
pub(super) const REASON_CHANNEL_MEMBERSHIP: &str =
    "dispatch.rs checks ServerState::channels.authorize_member(channel_id, tenant_scope, agent_id) before returning channel messages/roster -- channel membership is the authority, not graph row visibility";
pub(super) const REASON_PURE_COMPUTE: &str =
    "pure compute over caller-supplied arrays/parameters (finance/datascience primitives) -- no server-held graph row is read";
pub(super) const REASON_ASR_PURE_COMPUTE: &str =
    "src/server/handlers/asr.rs::handle/transcribe_file only verifies a caller-resolved model artifact and transcribes caller-supplied audio -- it never reads GraphCore or a tenant-owned row, so cross-tenant row RLS is not applicable";
pub(super) const REASON_QUANTUM_PURE_COMPUTE: &str =
    "src/server/handlers/quantum.rs::handle builds and executes a bounded circuit from caller-supplied candidates/programs -- it never reads GraphCore or a tenant-owned row, so ranking is not a graph-row read and cannot bypass RLS";
pub(super) const REASON_VIZ_CARRIER_SCOPED: &str =
    "src/server/handlers/viz.rs never reads GraphCore: its persistent ColumnStore/provenance side-store is an owner-scoped non-row surface, and verified CarrierAuthority namespaces dataset handles plus result references before lookup, cache, ingestion, or shaping";
pub(super) const REASON_SANDBOXED_UDF: &str =
    "wasm_udf.rs's RunUdf executes a sandboxed WASM module over an opaque caller-supplied byte payload only -- no graph state is read";
pub(super) const REASON_TENANT_KEY_SCOPED_SERIES: &str =
    "src/server/handlers/timeseries.rs::scoped_key derives the SeriesKey from CarrierAuthority (verified tenant+actor) -- a series is namespaced under the CALLING actor's own scope, so cross-actor addressing is structurally impossible (never constructs a GraphView). Stronger than default-deny in one respect (no address collision possible at all), but does not support _visibility:public/_grants sharing -- the exact gap the next-level-analysis report section 9 item #10 named; see that file's module doc for the full reasoning";
pub(super) const REASON_BLOB_OWNER_SCOPED: &str =
    "src/server/handlers/blob.rs checks authority.owner_scope() against the stored manifest/cursor owner_scope on every fetch (ensure_blob_owner/authorize_fetch); content is addressed by digest, not by a graph row";
pub(super) const REASON_TENANT_KEY_SCOPED_KV: &str =
    "src/server/kv.rs namespaces every key via CarrierAuthority::namespace(\"kv-namespace\", ...) -- the identical tenant+actor-scoped-key pattern as timeseries; never constructs a GraphView";
pub(super) const REASON_ADMIN_ONLY_CEP: &str =
    "src/server/cep.rs gates CEP subscription management, including polling one, behind CarrierAuthority::require_admin -- stricter than row RLS, not row-scoped at all";
// L-RLS-1 follow-up audit (CONCEPT:EPI-P3-3/P3-6): the 5 formerly-`NOT_YET_AUDITED` epistemic
// read methods were traced to their handlers in `src/server/handlers/query.rs`. Two
// (`ResolveConflict`/`ExplainEvidence`) DO build a `BeliefGraph` off `core.analysis_snapshot()`
// and are now `RLS_ROUTED` above (their handler arms were fixed to `rls.filter_view` the
// snapshot first -- see those arms' own doc comments). The other three below never touch
// `core`/`state`/`GraphView` at all.
pub(super) const REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE: &str =
    "src/server/handlers/query.rs's causal_estimate_wire/causal_counterfactual_wire/rank_by_provenance_wire (Method::CausalEstimate/CausalCounterfactual/RankByProvenance) build an ephemeral eg_epistemic::CausalGraph, or rank eg_epistemic::RetrievalCandidates, purely from the REQUEST's own variables/do_values/actual/candidates/weights fields -- these three handler arms never call core.analysis_snapshot() or reference state/core/GraphView at all (confirmed by reading each arm and its wire fn body), so there is no tenant-owned graph row in play to RLS-scope; the same posture as REASON_PURE_COMPUTE's finance/datascience primitives, just epistemic-causal's own request-scoped SCM/ranking inputs instead";
// t1-grounding-0802 follow-up audit: `ClusterMembers` was added (ADR-1/W1.1
// engine-authoritative cluster-topology discovery) without a classification here.
// Auth-unification wave 1 (Workstream C): `GetIdentity` is the identity READ-BACK that
// closes `RegisterIdentity`'s blind-upsert gap -- `RegisterIdentity` REPLACES a principal's
// whole role set, so a merge-before-register caller must be able to read the current set
// first (the production graph-os scheduler logged `existing_roles is unknown (None)` at every
// boot precisely because no such RPC existed).
pub(super) const REASON_IDENTITY_STORE_ADMIN_GATED: &str =
    "src/server/dispatch.rs's GetIdentity arm answers from IsolationLayer::get_identity -- a point read of the in-memory RBAC identity map (`agents: HashMap<String, AgentIdentity>`, crates/eg-core/src/isolation.rs), the SAME store RegisterIdentity writes and RbacAdmin governs. It is control-plane principal metadata (agent_id/role/teams/roles), not one resolved graph's rows: it never constructs a GraphView/GraphCore and never calls project_core/filter_view, so there is no per-node `_owner`/`_visibility`/`_grants` for RLS to filter on. Authority is the same `security:admin` authz_action the rest of the Zero-Trust Consensus family carries (crates/eg-capabilities/src/lib.rs), so it grants no visibility to any caller who could not already call RegisterIdentity/RbacAdmin against that identical store";
pub(super) const REASON_CLUSTER_TOPOLOGY_READ: &str =
    "src/server/handlers/topology.rs::handle_cluster_members answers from the durable NodeInfoStore + live MultiRaft membership (self-reported node/raft-group topology) -- not one resolved graph's rows, never touches core/GraphView/project_core. Gated by its own authz_action `cluster:topology-read` (crates/eg-capabilities/src/lib.rs), deliberately NOT kg:admin (REASON_CLUSTER_ADMIN_GATED) so ordinary service roles can re-resolve after a failover -- a distinct, weaker gate than the admin-only cluster methods above, so it gets its own reason rather than being folded into theirs";
// BUG-044-class facade audit (push/eg-merge-artifacts): `QueryWorkItemReservation`/
// `ResourceReservationStatus` route in `dispatch.rs` (the `is_resource_reservation_query_method`
// guard, just above the WorkItem claim-capability block) straight to
// `PersistenceBackend::read_resource_reservation`/`read_resource_reservation_status` against the
// dedicated native reservation ledger -- never a `GraphView`/`core.analysis_snapshot()`. Under
// placement it is served only by the current group leader (the SAME `routed_raft`/linearizable
// read-barrier gate `REASON_CLUSTER_TOPOLOGY_READ`'s neighbor uses); `ResourceReservationStatus`
// additionally redacts aggregate host totals unless the caller holds `resource:read:aggregate`/
// `kg:admin` (see `redact_resource_status_result`) -- native reservation-scoped authority, not
// per-row RLS.
pub(super) const REASON_NATIVE_RESERVATION_READ: &str =
    "dispatch.rs's is_resource_reservation_query_method guard routes QueryWorkItemReservation/ResourceReservationStatus directly to PersistenceBackend::read_resource_reservation/read_resource_reservation_status against the dedicated native reservation ledger (redb_store.rs), gated by the current placement leader + linearizable read barrier under raft -- never a GraphView/core.analysis_snapshot() row read; ResourceReservationStatus additionally redacts aggregate totals unless the caller holds resource:read:aggregate/kg:admin";
// `VerifyWorkItemClaimCapability` shares `MintWorkItemClaimCapability`'s dedicated block in
// dispatch.rs ("Native WorkItem claim capabilities use a dedicated private ledger and never
// enter MutationBatch/result/outbox/CDC projections"): it reads
// `redb_store::work_item_capability::verify_work_item_claim_capability` keyed by an
// `AuthenticatedAuthority` built from the verified request context (tenant/principal/session),
// never a `GraphView`.
pub(super) const REASON_NATIVE_CAPABILITY_LEDGER: &str =
    "dispatch.rs's dedicated WorkItem-claim-capability block routes VerifyWorkItemClaimCapability to redb_store::work_item_capability::verify_work_item_claim_capability against its own private ledger, keyed by an AuthenticatedAuthority derived from the verified request context -- never enters MutationBatch/result/outbox/CDC projections or a GraphView, same posture as MintWorkItemClaimCapability's write-side native ledger";
// RMDD-28 dispatch wiring (fix/eg-devlane-dispatch): `DevelopmentLaneStatus`/
// `QueryDevelopmentLane` now route through `handlers::development_lane::try_handle` to
// `PersistenceBackend::read_development_lane[_status]`, which call
// `redb_store::development_lane::read_development_lane[_status]` directly against the native
// `development_lane_*` redb tables -- never a `GraphView`/`core.analysis_snapshot()`. Gated by
// the current placement leader under raft, same posture as `REASON_NATIVE_RESERVATION_READ`.
// The kernel's own `public_hold` projection (not a dispatch-layer redaction step) already
// redacts `worktree_locator`/`host_ref`/`host_target_alias` on every row returned, so no
// additional per-caller redaction is needed here the way `ResourceReservationStatus` needs one.
pub(super) const REASON_NATIVE_DEVELOPMENT_LANE_READ: &str =
    "handlers::development_lane::try_handle routes DevelopmentLaneStatus/QueryDevelopmentLane directly to PersistenceBackend::read_development_lane/read_development_lane_status, which call redb_store::development_lane::read_development_lane/read_development_lane_status against the native development_lane_* redb tables (redb_store/development_lane.rs), gated by the current placement leader under raft -- never a GraphView/core.analysis_snapshot() row read; the kernel's own public_hold projection already redacts worktree_locator/host_ref/host_target_alias on every row, so no dispatch-layer redaction is needed";

// RF-ADR-010 A1: an assembly reads ONE tenant-bound agent_library.redb snapshot
// through the same tenant-scoped accessors the four agent layers use. The
// candidate set is a component catalog, not graph rows, so there is no
// `GraphView` to filter -- the tenant binding IS the scope, exactly as it is
// for `AgentComponent.Search`, whose result set this read is drawn from.
pub(super) const REASON_AGENT_LIBRARY_TENANT_SNAPSHOT: &str =
    "AgentAssemble reads one tenant-bound agent_library.redb snapshot through the same tenant-scoped component accessors AgentComponent.Search uses -- a component catalog, never a GraphView/core.analysis_snapshot() row read, so the tenant binding is the whole scope";
// The solver is a pure function of its request: it opens no store, reads no
// clock and returns a certificate a caller can re-verify without the engine.
// There is no row for RLS to scope.
pub(super) const REASON_SOLVE_PURE_COMPUTE: &str =
    "Solve is a pure function of the model in its request: no store is opened, no clock is read, and the certificate it returns is independently verifiable -- there is no row to scope";
// X9: the schema-source list is graph CONTROL state (which shapes and ontology
// documents are attached, and their digests), not graph data. It is gated by
// `security:admin`, which additionally requires the RBAC admin capability.
pub(super) const REASON_GRAPH_SCHEMA_CONTROL_STATE: &str =
    "GraphSchemaList reads the request graph's attached schema-source keys, origins and digests -- graph control state behind security:admin and the RBAC admin capability, never a GraphView/core.analysis_snapshot() row read";
// RF-ADR-009: status reads one source-partition marker keyed by the verified
// tenant, graph, connector and stream. The marker is ingestion control state,
// not a caller-visible graph row; its `source:ingest` capability and verified
// tenant binding are the complete read scope.
pub(super) const REASON_SOURCE_INGESTION_MARKER: &str =
    "SourceIngestStatus reads one durable source-partition marker keyed by verified tenant, request graph, connector and stream behind source:ingest -- ingestion control state, never a GraphView/core.analysis_snapshot() row read";

// RF-ADR-010 DL-4: `Decide` reads its candidates from ONE tenant-bound
// agent_library.redb snapshot through `AgentComponent.Search`'s accessor, and
// its pinned feature schema, head and policy bodies from the same owner. The
// graph arm of `CandidateSource` is REFUSED before any graph is opened
// (`CANDIDATE_PLAN_REFUSED`) until graph-sourced records land; that package
// moves this entry to `RLS_ROUTED` with a structural pin.
pub(super) const REASON_DECIDE_LIBRARY_SNAPSHOT: &str =
    "Decide reads its candidates and pinned catalog bodies from one tenant-bound agent_library.redb snapshot through AgentComponent.Search's accessor and refuses graph-sourced candidates before opening any graph -- never a GraphView/core.analysis_snapshot() row read";

// EH-219: `GetWorkItem`/`ListWorkItems` route through `native_routes::route_work_item_reads`
// (placement leader + read barrier under raft) to `handlers::work_item_read`, which refuses any
// request tenant other than the VERIFIED carrier tenant and then reads the redb authority's
// node rows through `redb_store::work_item::{read_work_item, list_work_items}` -- never a
// `GraphView`/`core.analysis_snapshot()`. A WorkItem row is control-plane state whose scope is
// its `tenant` field, not per-node `_owner`/`_visibility`/`_grants`; the projection returns only
// the caller view (no lease owner/epoch/fencing token).
pub(super) const REASON_NATIVE_WORK_ITEM_TENANT_READ: &str =
    "native_routes::route_work_item_reads routes GetWorkItem/ListWorkItems/GetControlLease (placement leader + read barrier under raft) to handlers::work_item_read, which refuses any request tenant other than the verified carrier tenant, then reads redb node rows via redb_store::work_item::{read_work_item, list_work_items, read_control_lease} and projects only rows whose `tenant` equals it -- never a GraphView/core.analysis_snapshot() row read; lease owner/epoch/fencing token are never projected";

pub(super) const NON_ROW_SCOPED: &[(&str, &str)] = &[
    // REASON_DECIDE_LIBRARY_SNAPSHOT
    ("Decide", REASON_DECIDE_LIBRARY_SNAPSHOT),
    // REASON_AGENT_LIBRARY_TENANT_SNAPSHOT
    ("AgentAssemble", REASON_AGENT_LIBRARY_TENANT_SNAPSHOT),
    // REASON_SOLVE_PURE_COMPUTE
    ("Solve", REASON_SOLVE_PURE_COMPUTE),
    // REASON_GRAPH_SCHEMA_CONTROL_STATE
    ("GraphSchemaList", REASON_GRAPH_SCHEMA_CONTROL_STATE),
    // REASON_SOURCE_INGESTION_MARKER
    ("SourceIngestStatus", REASON_SOURCE_INGESTION_MARKER),
    // REASON_SERVER_LIFECYCLE
    ("CancelRequest", REASON_SERVER_LIFECYCLE),
    ("Health", REASON_SERVER_LIFECYCLE),
    ("Metrics", REASON_SERVER_LIFECYCLE),
    ("Ping", REASON_SERVER_LIFECYCLE),
    ("ResourceStatsPage", REASON_SERVER_LIFECYCLE),
    // REASON_AUDIT_CHAIN_ADMIN_GATED
    ("AuditVerify", REASON_AUDIT_CHAIN_ADMIN_GATED),
    ("AuditProveInclusion", REASON_AUDIT_CHAIN_ADMIN_GATED),
    // REASON_LEDGER_ADMIN_OBSERVABILITY
    ("GetLedger", REASON_LEDGER_ADMIN_OBSERVABILITY),
    // REASON_VERIFIED_TENANT_CLAIM
    ("GetChangeCursor", REASON_VERIFIED_TENANT_CLAIM),
    ("GetChangeEnvelope", REASON_VERIFIED_TENANT_CLAIM),
    ("GetContentVersion", REASON_VERIFIED_TENANT_CLAIM),
    // REASON_CLUSTER_ADMIN_GATED
    ("Backup", REASON_CLUSTER_ADMIN_GATED),
    ("CatalogList", REASON_CLUSTER_ADMIN_GATED),
    ("ExportSqliteFile", REASON_CLUSTER_ADMIN_GATED),
    ("PlacementRoute", REASON_CLUSTER_ADMIN_GATED),
    ("RebalancePlan", REASON_CLUSTER_ADMIN_GATED),
    // REASON_NATIVE_CAPACITY_TENANT_GATED
    ("CapacityStatus", REASON_NATIVE_CAPACITY_TENANT_GATED),
    ("ReconcileCapacity", REASON_NATIVE_CAPACITY_TENANT_GATED),
    // REASON_CLUSTER_TOPOLOGY_READ
    ("ClusterMembers", REASON_CLUSTER_TOPOLOGY_READ),
    ("GetIdentity", REASON_IDENTITY_STORE_ADMIN_GATED),
    // REASON_CHANNEL_MEMBERSHIP
    ("GetChannelMembers", REASON_CHANNEL_MEMBERSHIP),
    ("GetChannelMessages", REASON_CHANNEL_MEMBERSHIP),
    ("ListChannels", REASON_CHANNEL_MEMBERSHIP),
    // REASON_PURE_COMPUTE
    ("Asr", REASON_ASR_PURE_COMPUTE),
    ("Quantum", REASON_QUANTUM_PURE_COMPUTE),
    ("DsAdamStep", REASON_PURE_COMPUTE),
    ("DsComputeStats", REASON_PURE_COMPUTE),
    ("DsCrossEntropy", REASON_PURE_COMPUTE),
    ("DsDpoLoss", REASON_PURE_COMPUTE),
    ("DsFitEstimator", REASON_PURE_COMPUTE),
    ("DsGrpoSurrogate", REASON_PURE_COMPUTE),
    ("DsKMeans", REASON_PURE_COMPUTE),
    ("DsKlDivergence", REASON_PURE_COMPUTE),
    ("DsLinearRegression", REASON_PURE_COMPUTE),
    ("DsLogSoftmax", REASON_PURE_COMPUTE),
    ("DsPca", REASON_PURE_COMPUTE),
    ("DsPredictEstimator", REASON_PURE_COMPUTE),
    ("DsSgdStep", REASON_PURE_COMPUTE),
    ("DsSoftmax", REASON_PURE_COMPUTE),
    ("DsTrainTestSplit", REASON_PURE_COMPUTE),
    ("FinanceAdfTest", REASON_PURE_COMPUTE),
    ("FinanceAlphaCombinationEngine", REASON_PURE_COMPUTE),
    ("FinanceAvellanedaStoikov", REASON_PURE_COMPUTE),
    ("FinanceBayesianKelly", REASON_PURE_COMPUTE),
    ("FinanceBlackLitterman", REASON_PURE_COMPUTE),
    ("FinanceBreakevenAlpha", REASON_PURE_COMPUTE),
    ("FinanceBrierScore", REASON_PURE_COMPUTE),
    ("FinanceCombineAlphas", REASON_PURE_COMPUTE),
    ("FinanceConvergenceGate", REASON_PURE_COMPUTE),
    ("FinanceCrossSectionalRank", REASON_PURE_COMPUTE),
    ("FinanceCvar", REASON_PURE_COMPUTE),
    ("FinanceDeflatedSharpe", REASON_PURE_COMPUTE),
    ("FinanceDetectRegimes", REASON_PURE_COMPUTE),
    ("FinanceDieboldMariano", REASON_PURE_COMPUTE),
    ("FinanceDownsideDeviation", REASON_PURE_COMPUTE),
    ("FinanceDrawdownSeries", REASON_PURE_COMPUTE),
    ("FinanceEffectiveIndependentN", REASON_PURE_COMPUTE),
    ("FinanceEfficientFrontier", REASON_PURE_COMPUTE),
    ("FinanceEmpiricalKelly", REASON_PURE_COMPUTE),
    ("FinanceEwma", REASON_PURE_COMPUTE),
    ("FinanceExpectedPnlRate", REASON_PURE_COMPUTE),
    ("FinanceForensicReport", REASON_PURE_COMPUTE),
    ("FinanceGlostenMilgromSpread", REASON_PURE_COMPUTE),
    ("FinanceGltQuotes", REASON_PURE_COMPUTE),
    ("FinanceHardimanBouchaud", REASON_PURE_COMPUTE),
    ("FinanceHawkesMle", REASON_PURE_COMPUTE),
    ("FinanceInformationCoefficient", REASON_PURE_COMPUTE),
    ("FinanceInformationRatio", REASON_PURE_COMPUTE),
    ("FinanceKalmanBeta", REASON_PURE_COMPUTE),
    ("FinanceKalmanFilter1d", REASON_PURE_COMPUTE),
    ("FinanceKalmanVolatility", REASON_PURE_COMPUTE),
    ("FinanceKellyFraction", REASON_PURE_COMPUTE),
    ("FinanceKyleLambda", REASON_PURE_COMPUTE),
    ("FinanceLogitQuotes", REASON_PURE_COMPUTE),
    ("FinanceMarketImpact", REASON_PURE_COMPUTE),
    ("FinanceMarkovTransitionMatrix", REASON_PURE_COMPUTE),
    ("FinanceMatchOrders", REASON_PURE_COMPUTE),
    ("FinanceMaxDrawdown", REASON_PURE_COMPUTE),
    ("FinanceMeanReversion", REASON_PURE_COMPUTE),
    ("FinanceMicropriceSeries", REASON_PURE_COMPUTE),
    ("FinanceMomentum", REASON_PURE_COMPUTE),
    ("FinanceMonteCarloVar", REASON_PURE_COMPUTE),
    ("FinanceOfiSeries", REASON_PURE_COMPUTE),
    ("FinanceOptimizePortfolio", REASON_PURE_COMPUTE),
    ("FinanceOrderBookImbalance", REASON_PURE_COMPUTE),
    ("FinanceOuCalibrate", REASON_PURE_COMPUTE),
    ("FinanceOuOptimalThresholds", REASON_PURE_COMPUTE),
    ("FinancePairsTrading", REASON_PURE_COMPUTE),
    ("FinancePosteriorCredibleInterval", REASON_PURE_COMPUTE),
    ("FinanceProbabilityBacktestOverfit", REASON_PURE_COMPUTE),
    ("FinancePurgedCpcv", REASON_PURE_COMPUTE),
    ("FinanceQueueImbalance", REASON_PURE_COMPUTE),
    ("FinanceRealizedVolTick", REASON_PURE_COMPUTE),
    ("FinanceRiskMetrics", REASON_PURE_COMPUTE),
    ("FinanceRiskParity", REASON_PURE_COMPUTE),
    ("FinanceRollingZscore", REASON_PURE_COMPUTE),
    ("FinanceSabrCalibrate", REASON_PURE_COMPUTE),
    ("FinanceSabrImpliedVol", REASON_PURE_COMPUTE),
    ("FinanceSabrSmile", REASON_PURE_COMPUTE),
    ("FinanceSignalDecay", REASON_PURE_COMPUTE),
    ("FinanceSpreadReversion", REASON_PURE_COMPUTE),
    ("FinanceStressTest", REASON_PURE_COMPUTE),
    ("FinanceSurveillanceRisk", REASON_PURE_COMPUTE),
    ("FinanceTwap", REASON_PURE_COMPUTE),
    ("FinanceVar", REASON_PURE_COMPUTE),
    ("FinanceVpinPm", REASON_PURE_COMPUTE),
    ("FinanceVwap", REASON_PURE_COMPUTE),
    // REASON_SANDBOXED_UDF
    ("RunUdf", REASON_SANDBOXED_UDF),
    // REASON_VIZ_CARRIER_SCOPED
    ("Viz", REASON_VIZ_CARRIER_SCOPED),
    // REASON_TENANT_KEY_SCOPED_SERIES
    ("TsAsofJoin", REASON_TENANT_KEY_SCOPED_SERIES),
    ("TsGapFill", REASON_TENANT_KEY_SCOPED_SERIES),
    ("TsListSeries", REASON_TENANT_KEY_SCOPED_SERIES),
    ("TsRange", REASON_TENANT_KEY_SCOPED_SERIES),
    ("TsWindow", REASON_TENANT_KEY_SCOPED_SERIES),
    // REASON_BLOB_OWNER_SCOPED
    ("BlobChunkGet", REASON_BLOB_OWNER_SCOPED),
    ("BlobFetchBegin", REASON_BLOB_OWNER_SCOPED),
    ("BlobFetchEnd", REASON_BLOB_OWNER_SCOPED),
    // REASON_TENANT_KEY_SCOPED_KV
    ("KvGet", REASON_TENANT_KEY_SCOPED_KV),
    ("KvScan", REASON_TENANT_KEY_SCOPED_KV),
    // REASON_ADMIN_ONLY_CEP
    ("CepPoll", REASON_ADMIN_ONLY_CEP),
    // REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE
    ("CausalCounterfactual", REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE),
    ("CausalEstimate", REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE),
    ("RankByProvenance", REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE),
    // REASON_NATIVE_RESERVATION_READ
    ("QueryWorkItemReservation", REASON_NATIVE_RESERVATION_READ),
    ("ResourceReservationStatus", REASON_NATIVE_RESERVATION_READ),
    // REASON_NATIVE_CAPABILITY_LEDGER
    (
        "VerifyWorkItemClaimCapability",
        REASON_NATIVE_CAPABILITY_LEDGER,
    ),
    // REASON_NATIVE_DEVELOPMENT_LANE_READ
    ("DevelopmentLaneStatus", REASON_NATIVE_DEVELOPMENT_LANE_READ),
    ("QueryDevelopmentLane", REASON_NATIVE_DEVELOPMENT_LANE_READ),
    // REASON_NATIVE_WORK_ITEM_TENANT_READ
    ("GetWorkItem", REASON_NATIVE_WORK_ITEM_TENANT_READ),
    ("ListWorkItems", REASON_NATIVE_WORK_ITEM_TENANT_READ),
    ("GetControlLease", REASON_NATIVE_WORK_ITEM_TENANT_READ),
];

// L-RLS-1 burn-down (CONCEPT:EPI-P3-3/P3-6): the 5 methods this pass covered (see the
// `REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE` doc above) were the entire prior contents of this
// list. Empty is the intended end state, not an initial one -- a future audit pass adds a
// new entry here ONLY as a flagged, visible TODO, never silently.
//
// fix/eg-devlane-dispatch: `DevelopmentLaneStatus`/`QueryDevelopmentLane` were parked here as
// NOT_YET_AUDITED by the push/eg-merge-artifacts facade audit (commit 174c381) because they had
// no dispatch.rs routing arm at all. They are now genuinely dispatch-routed (see
// `REASON_NATIVE_DEVELOPMENT_LANE_READ` in `NON_ROW_SCOPED` above), so this list returns to its
// intended empty end state.
pub(super) const NOT_YET_AUDITED: &[(&str, &str)] = &[
    // VIZ-1 hierarchical clustering (merged in 6fc8c552). These read the
    // non-authoritative `cluster_hierarchy_store`, not graph rows, so they do not
    // construct a GraphView -- which argues for NON_ROW_SCOPED. But cluster
    // MEMBERSHIP is derived from graph structure, so a cluster listing can reveal
    // that two nodes are connected without the caller being able to read either
    // node. Whether that is a disclosure worth gating has NOT been traced end to
    // end, so these are parked here deliberately rather than asserted safe.
    // Burn down by tracing the handler to a project_core/filter_view call and
    // moving them to RLS_ROUTED, or by justifying NON_ROW_SCOPED with a cited
    // call site.
    // Declared mutates:false because it writes only the derived cache, never
    // graph rows -- but it READS the entire graph to compute the clustering,
    // which is the widest read in this group. Same untraced question, higher
    // stakes: park it, do not assume it safe.
    ("ClusterHierarchyRefresh", "VIZ-1: recomputes the hierarchy by reading the whole graph; writes only the derived cluster_hierarchy store. Widest read of the three; disclosure question untraced"),
    ("ClusterHierarchyClusters", "VIZ-1: reads the derived cluster_hierarchy store, not graph rows; cluster membership still reflects graph structure, and that disclosure question is untraced"),
    ("ClusterHierarchyExpand", "VIZ-1: same store and same untraced disclosure question as ClusterHierarchyClusters"),
];
