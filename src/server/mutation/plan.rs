use crate::isolation::IsolationLayer;
use crate::protocol::{GraphType, Method};
use eg_capabilities::DurabilityDomain;

/// The pre-authz identity/placement context a gateway call needs, captured by the
/// caller (`dispatch_graph_op`) from the graph registry BEFORE its read lock is
/// dropped — `(isolation, graph_type, owner)`, mirroring exactly what
/// `access::check_graph_access` already requires for every other graph-scoped
/// method. An owned tuple (not borrowed from the registry) because it must outlive
/// the registry lock guard.
pub type GatewayAuthzCtx = (IsolationLayer, GraphType, Option<String>);

/// The declared capability profile of one mutation, POPULATED FROM
/// `eg_capabilities::policy` -- never re-hardcoded here (CONCEPT:EG-P0-2's central
/// requirement: this crate is a consumer of that table, not a second source of
/// truth). See `eg-capabilities`' own docs for the meaning of each field.
#[derive(Debug, Clone)]
pub struct MutationPlan {
    /// The `Method` variant's name (for logging/tests only — never used to
    /// re-derive policy; see [`method_variant_name`]).
    pub method_name: &'static str,
    pub mutates: bool,
    pub durability_domain: DurabilityDomain,
    pub authz_action: &'static str,
    pub idempotent: bool,
    pub audited: bool,
    pub emits_cdc: bool,
    pub txn_participation: eg_capabilities::TxnParticipation,
}

impl MutationPlan {
    /// Build the plan for `method` straight from `eg_capabilities::policy` — the
    /// ONE source of truth. Never inspects `method`'s payload beyond what
    /// `policy()` itself does (i.e. this never hardcodes a per-variant judgment
    /// call that duplicates/diverges from the ledger).
    pub fn for_method(method: &Method) -> Self {
        let p = eg_capabilities::policy(method);
        MutationPlan {
            method_name: method_variant_name(method),
            mutates: p.mutates,
            durability_domain: p.durability_domain,
            authz_action: p.authz_action,
            idempotent: p.idempotent,
            audited: p.audited,
            emits_cdc: p.emits_cdc,
            txn_participation: p.txn_participation,
        }
    }
}

/// The methods currently routed through [`commit_mutation`] (CONCEPT:EG-P0-2). Kept
/// as a public, testable allowlist so the migration surface is machine-visible: see
/// `tests::gateway_routed_set_matches_mutating_policy_surface` for the computed
/// complement (every other mutating method, per `eg_capabilities::method_policy_entries()`),
/// which is owned by an explicit engine-native consensus coordinator.
pub const GATEWAY_ROUTED: &[&str] = &[
    "AddNode",
    "CreateNodeIfAbsent",
    "RemoveNode",
    "AddEdge",
    "RemoveEdge",
    "CreateSummaryNode",
    "Consolidate",
    "Reinforce",
    // ── L11 rollout batch 2 (EG-P0-2 continued): graph-core family — same shape
    // as the original 7 (single durable/derivable `GraphCore` mutation), all
    // handled in `graph_ops.rs` already. ──
    "CompareAndSetNodeFields",
    "ClaimNext",
    "DecayNode",
    "DecayMemories",
    "EvictBelow",
    "Maintain",
    "AddSceneObject",
    "SetPose",
    "Reparent",
    "StartTrajectory",
    "AppendStep",
    "AddEmbedding",
    "InvalidateEdge",
    "SupersedeEdge",
    "ClearGraph",
    "EvictLRU",
    "DecaySweep",
    "TouchNodes",
    "FromMsgpack",
    "Reconcile",
    "ApplyMutation",
    "RunDatalogReasoning",
    // `IcvConfigure` is UNCONDITIONAL in the `Method` enum but its handler (and
    // hence its gateway arm) is behind `feature = "shacl"` — same shape as
    // `RunDatalogReasoning`/`reasoning` above. Its validated policy is graph
    // control state and therefore uses the same staged/durable gateway shape.
    "IcvConfigure",
    // X9. Same shape and same reason as `IcvConfigure` directly above: a keyed
    // schema source is graph control state, staged and committed through the
    // one gateway so it is ordered, audited and CDC-emitted with every other
    // write to that graph.
    "GraphSchema",
    "PruneByLifecycle",
    "BatchUpdate",
    "ClearLedger",
    "ApplyLedger",
    "CompactNodesByType",
    // ── L11 rollout batch 2: message-broker / stream family (Outbox durability
    // domain) — also handled in `graph_ops.rs`, behind `feature = "broker"`. ──
    "DeclareExchange",
    "DeleteExchange",
    "BindQueue",
    "UnbindQueue",
    "Publish",
    "DeclareQueue",
    "PublishEx",
    "BrokerConsume",
    "BrokerAck",
    "BrokerReject",
    "SweepExpired",
    "StreamDeclare",
    "StreamPublish",
    "StreamTrim",
    "StreamCommitOffset",
    "PublishConfirmed",
    "PublishIdempotent",
    "BrokerAckTag",
    "BrokerNackTag",
    "BrokerRenewTag",
    // ── L11 rollout batch 3: RUNTIME-CONDITIONAL graph-learning family (only
    // mutates when the request's own `writeback` field is true) — routed via
    // `commit_conditional_mutation`, behind `feature = "graphlearn"`. ──
    "GraphLearnFit",
    "GraphLearnPredict",
    // ── L11 rollout batch 3: RUNTIME-CONDITIONAL data-mining family (same
    // `writeback`-gated shape as GraphLearn* above), behind `feature = "mining"`.
    // `MineClassifyFit` is deliberately absent (policy explicit-false: it never
    // writes back) and keeps its plain read-only arm in `mining.rs`. ──
    "MineAssociate",
    "MineCluster",
    "MineAnomaly",
    "MineClassifyPredict",
    "MineReduce",
    "MineSequence",
    "MineForecast",
    "MineText",
    "MineSubgraph",
    "MineEntityResolve",
    "MineCausalImpact",
    "MineProcess",
    "MineRootCause",
    "MineRiskPropagation",
    "MineOntologyGap",
    "MineRetrievalQuality",
    "MineCommunity",
    // ── ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline): RUNTIME-CONDITIONAL writes —
    // Train/Predict mutate only when their `writeback` is true; Serve ALWAYS writes the
    // `:ServedModel` pointer. Same `commit_conditional_mutation` shape, behind
    // `feature = "ml-pipeline"`. Evaluate/Compare are read-only (not routed). ──
    "MiningPipelineTrain",
    "MiningPipelineServe",
    "MiningPipelinePredict",
    // ── L11 rollout batch 4: RUNTIME-CONDITIONAL query surface — the parsed
    // statement decides whether THIS call mutates. Routed via
    // `commit_conditional_mutation_async` at the query dispatch site (they need
    // `state`/`rls`), NOT the graph-ops `try_handle_gateway` entry point (which
    // hands them back). `CypherQuery`/`Sql` are unconditional in the `Method`
    // enum; `GraphQl` is behind `feature = "graphql"`. ──
    "Sql",
    "CypherQuery",
    "GraphQl",
    // ── L11 rollout batch 4: native RDF write surface (GraphRedb-durable,
    // audited) — routed via `commit_conditional_mutation_async` at the rdf
    // dispatch site because parsing and graph projection require `state`.
    // Behind `feature = "rdf"`. ──
    "AddTriples",
    "RemoveTriples",
    "DropNamedGraph",
    // Verified, runtime-conditional governed document/image/audio/video service.
    // Its dedicated dispatch arm invokes `commit_conditional_mutation` before the
    // generic handler chain so ephemeral source bytes never enter durable receipts.
    #[cfg(feature = "modality-serving")]
    "ServedModality",
];

/// Resolve the node and edge CRUD methods owned by the graph core.
fn node_edge_method_name(m: &Method) -> Option<&'static str> {
    match m {
        Method::AddNode { .. } => Some("AddNode"),
        Method::CreateNodeIfAbsent { .. } => Some("CreateNodeIfAbsent"),
        Method::RemoveNode { .. } => Some("RemoveNode"),
        Method::AddEdge { .. } => Some("AddEdge"),
        Method::RemoveEdge { .. } => Some("RemoveEdge"),
        _ => None,
    }
}

/// Resolve graph-memory lifecycle methods (summaries, reinforcement, claims,
/// and decay) owned by the graph core.
fn memory_method_name(m: &Method) -> Option<&'static str> {
    match m {
        Method::CreateSummaryNode { .. } => Some("CreateSummaryNode"),
        Method::Consolidate { .. } => Some("Consolidate"),
        Method::Reinforce { .. } => Some("Reinforce"),
        Method::CompareAndSetNodeFields { .. } => Some("CompareAndSetNodeFields"),
        Method::ClaimNext { .. } => Some("ClaimNext"),
        Method::DecayNode { .. } => Some("DecayNode"),
        Method::DecayMemories { .. } => Some("DecayMemories"),
        Method::EvictBelow { .. } => Some("EvictBelow"),
        Method::Maintain { .. } => Some("Maintain"),
        _ => None,
    }
}

/// Resolve scene and trajectory methods owned by the graph core.
fn scene_trajectory_method_name(m: &Method) -> Option<&'static str> {
    match m {
        Method::AddSceneObject { .. } => Some("AddSceneObject"),
        Method::SetPose { .. } => Some("SetPose"),
        Method::Reparent { .. } => Some("Reparent"),
        Method::StartTrajectory { .. } => Some("StartTrajectory"),
        Method::AppendStep { .. } => Some("AppendStep"),
        _ => None,
    }
}

/// Resolve embedding and temporal-edge methods owned by the graph core.
fn embedding_edge_method_name(m: &Method) -> Option<&'static str> {
    match m {
        Method::AddEmbedding { .. } => Some("AddEmbedding"),
        Method::InvalidateEdge { .. } => Some("InvalidateEdge"),
        Method::SupersedeEdge { .. } => Some("SupersedeEdge"),
        _ => None,
    }
}

/// Resolve graph-wide maintenance methods owned by the graph core.
fn graph_maintenance_method_name(m: &Method) -> Option<&'static str> {
    match m {
        Method::ClearGraph => Some("ClearGraph"),
        Method::EvictLRU { .. } => Some("EvictLRU"),
        Method::DecaySweep { .. } => Some("DecaySweep"),
        Method::TouchNodes { .. } => Some("TouchNodes"),
        Method::FromMsgpack { .. } => Some("FromMsgpack"),
        Method::Reconcile { .. } => Some("Reconcile"),
        _ => None,
    }
}

/// Resolve graph mutation and reasoning-control methods. The feature guards
/// mirror the protocol's ownership of the reasoning and SHACL variants.
fn graph_control_method_name(m: &Method) -> Option<&'static str> {
    match m {
        Method::ApplyMutation { .. } => Some("ApplyMutation"),
        #[cfg(feature = "reasoning")]
        Method::RunDatalogReasoning { .. } => Some("RunDatalogReasoning"),
        #[cfg(feature = "shacl")]
        Method::IcvConfigure { .. } => Some("IcvConfigure"),
        _ => None,
    }
}

/// Resolve the X9 schema-source method. Its own resolver rather than an arm on
/// [`graph_control_method_name`], because it is the only gateway-routed method
/// that is ALSO local-only: the two lists it belongs to are different, and a
/// reader looking for why should find it named.
fn graph_schema_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "shacl")]
        Method::GraphSchema { .. } => Some("GraphSchema"),
        _ => None,
    }
}

/// Resolve lifecycle and ledger methods owned by the graph core.
fn lifecycle_ledger_method_name(m: &Method) -> Option<&'static str> {
    match m {
        Method::PruneByLifecycle { .. } => Some("PruneByLifecycle"),
        Method::BatchUpdate { .. } => Some("BatchUpdate"),
        Method::ClearLedger => Some("ClearLedger"),
        Method::ApplyLedger { .. } => Some("ApplyLedger"),
        Method::CompactNodesByType { .. } => Some("CompactNodesByType"),
        _ => None,
    }
}

/// Resolve broker topology/control methods. The broker feature guard stays on
/// each protocol arm so a lean build keeps the same unavailable-method behavior.
fn broker_control_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "broker")]
        Method::DeclareExchange { .. } => Some("DeclareExchange"),
        #[cfg(feature = "broker")]
        Method::DeleteExchange { .. } => Some("DeleteExchange"),
        #[cfg(feature = "broker")]
        Method::BindQueue { .. } => Some("BindQueue"),
        #[cfg(feature = "broker")]
        Method::UnbindQueue { .. } => Some("UnbindQueue"),
        _ => None,
    }
}

/// Resolve broker queue-policy declaration methods.
fn broker_queue_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "broker")]
        Method::DeclareQueue { .. } => Some("DeclareQueue"),
        _ => None,
    }
}

/// Resolve broker publish and delivery methods.
fn broker_delivery_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "broker")]
        Method::Publish { .. } => Some("Publish"),
        #[cfg(feature = "broker")]
        Method::PublishEx { .. } => Some("PublishEx"),
        #[cfg(feature = "broker")]
        Method::BrokerConsume { .. } => Some("BrokerConsume"),
        #[cfg(feature = "broker")]
        Method::BrokerAck { .. } => Some("BrokerAck"),
        #[cfg(feature = "broker")]
        Method::BrokerReject { .. } => Some("BrokerReject"),
        #[cfg(feature = "broker")]
        Method::SweepExpired { .. } => Some("SweepExpired"),
        _ => None,
    }
}

/// Resolve broker stream methods.
fn broker_stream_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "broker")]
        Method::StreamDeclare { .. } => Some("StreamDeclare"),
        #[cfg(feature = "broker")]
        Method::StreamPublish { .. } => Some("StreamPublish"),
        #[cfg(feature = "broker")]
        Method::StreamTrim { .. } => Some("StreamTrim"),
        #[cfg(feature = "broker")]
        Method::StreamCommitOffset { .. } => Some("StreamCommitOffset"),
        _ => None,
    }
}

/// Resolve broker confirmation and tag-lifecycle methods.
fn broker_confirmation_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "broker")]
        Method::PublishConfirmed { .. } => Some("PublishConfirmed"),
        #[cfg(feature = "broker")]
        Method::PublishIdempotent { .. } => Some("PublishIdempotent"),
        #[cfg(feature = "broker")]
        Method::BrokerAckTag { .. } => Some("BrokerAckTag"),
        #[cfg(feature = "broker")]
        Method::BrokerNackTag { .. } => Some("BrokerNackTag"),
        #[cfg(feature = "broker")]
        Method::BrokerRenewTag { .. } => Some("BrokerRenewTag"),
        _ => None,
    }
}

/// Resolve graph-learning's fit and prediction methods.
fn graph_learning_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "graphlearn")]
        Method::GraphLearnFit { .. } => Some("GraphLearnFit"),
        #[cfg(feature = "graphlearn")]
        Method::GraphLearnPredict { .. } => Some("GraphLearnPredict"),
        _ => None,
    }
}

/// Resolve ML-pipeline training, serving, and prediction methods.
fn ml_pipeline_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelineTrain { .. } => Some("MiningPipelineTrain"),
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelineServe { .. } => Some("MiningPipelineServe"),
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelinePredict { .. } => Some("MiningPipelinePredict"),
        _ => None,
    }
}

/// Resolve mining's discovery and classification methods.
fn mining_discovery_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "mining")]
        Method::MineAssociate { .. } => Some("MineAssociate"),
        #[cfg(feature = "mining")]
        Method::MineCluster { .. } => Some("MineCluster"),
        #[cfg(feature = "mining")]
        Method::MineAnomaly { .. } => Some("MineAnomaly"),
        #[cfg(feature = "mining")]
        Method::MineClassifyPredict { .. } => Some("MineClassifyPredict"),
        _ => None,
    }
}

/// Resolve mining's reduction, sequence, and forecasting methods.
fn mining_sequence_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "mining")]
        Method::MineReduce { .. } => Some("MineReduce"),
        #[cfg(feature = "mining")]
        Method::MineSequence { .. } => Some("MineSequence"),
        #[cfg(feature = "mining")]
        Method::MineForecast { .. } => Some("MineForecast"),
        _ => None,
    }
}

/// Resolve mining's text, subgraph, and entity-resolution methods.
fn mining_content_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "mining")]
        Method::MineText { .. } => Some("MineText"),
        #[cfg(feature = "mining")]
        Method::MineSubgraph { .. } => Some("MineSubgraph"),
        #[cfg(feature = "mining")]
        Method::MineEntityResolve { .. } => Some("MineEntityResolve"),
        _ => None,
    }
}

/// Resolve mining's causal, process, and root-cause methods.
fn mining_reasoning_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "mining")]
        Method::MineCausalImpact { .. } => Some("MineCausalImpact"),
        #[cfg(feature = "mining")]
        Method::MineProcess { .. } => Some("MineProcess"),
        #[cfg(feature = "mining")]
        Method::MineRootCause { .. } => Some("MineRootCause"),
        _ => None,
    }
}

/// Resolve mining's risk, ontology, retrieval, and community methods.
fn mining_graph_quality_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "mining")]
        Method::MineRiskPropagation { .. } => Some("MineRiskPropagation"),
        #[cfg(feature = "mining")]
        Method::MineOntologyGap { .. } => Some("MineOntologyGap"),
        #[cfg(feature = "mining")]
        Method::MineRetrievalQuality { .. } => Some("MineRetrievalQuality"),
        #[cfg(feature = "mining")]
        Method::MineCommunity { .. } => Some("MineCommunity"),
        _ => None,
    }
}

/// Resolve the runtime-conditional SQL/Cypher query surface.
fn query_method_name(m: &Method) -> Option<&'static str> {
    match m {
        Method::Sql { .. } => Some("Sql"),
        Method::CypherQuery { .. } => Some("CypherQuery"),
        #[cfg(feature = "graphql")]
        Method::GraphQl { .. } => Some("GraphQl"),
        _ => None,
    }
}

/// Resolve the typed SQL source append surface.
fn sql_source_method_name(m: &Method) -> Option<&'static str> {
    #[cfg(feature = "query")]
    if matches!(m, Method::SqlSourceBatch { .. }) {
        return Some("SqlSourceBatch");
    }
    #[cfg(not(feature = "query"))]
    let _ = m;
    None
}

/// Resolve the native RDF write surface.
fn rdf_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "rdf")]
        Method::AddTriples { .. } => Some("AddTriples"),
        #[cfg(feature = "rdf")]
        Method::RemoveTriples { .. } => Some("RemoveTriples"),
        #[cfg(feature = "rdf")]
        Method::DropNamedGraph => Some("DropNamedGraph"),
        _ => None,
    }
}

/// Resolve the governed document/image/audio/video service.
fn modality_method_name(m: &Method) -> Option<&'static str> {
    match m {
        #[cfg(feature = "modality-serving")]
        Method::ServedModality { .. } => Some("ServedModality"),
        _ => None,
    }
}

/// Resolve self-routed cluster administration methods.
fn cluster_admin_method_name(m: &Method) -> Option<&'static str> {
    match m {
        // Self-routed cluster-admin (see `SELF_ROUTED_ADMIN_METHODS`): named here
        // too so `cluster_mutation_route`'s drift self-check is meaningful rather
        // than comparing against the `_ => "other"` catch-all.
        Method::RaftAddLearner { .. } => Some("RaftAddLearner"),
        Method::RaftChangeMembership { .. } => Some("RaftChangeMembership"),
        _ => None,
    }
}

// These resolvers follow the protocol's semantic sections rather than slicing
// the old match by size. Keeping the list explicit makes ownership and lookup
// order auditable while leaving unknown variants to the public fallback.
const METHOD_NAME_RESOLVERS: &[fn(&Method) -> Option<&'static str>] = &[
    node_edge_method_name,
    memory_method_name,
    scene_trajectory_method_name,
    embedding_edge_method_name,
    graph_maintenance_method_name,
    graph_control_method_name,
    graph_schema_method_name,
    lifecycle_ledger_method_name,
    broker_control_method_name,
    broker_queue_method_name,
    broker_delivery_method_name,
    broker_stream_method_name,
    broker_confirmation_method_name,
    graph_learning_method_name,
    ml_pipeline_method_name,
    mining_discovery_method_name,
    mining_sequence_method_name,
    mining_content_method_name,
    mining_reasoning_method_name,
    mining_graph_quality_method_name,
    query_method_name,
    sql_source_method_name,
    rdf_method_name,
    modality_method_name,
    cluster_admin_method_name,
    native_local_only_method_name,
    write_back_method_name,
];

/// Extract a `Method` variant's name as a `&'static str`, covering exactly
/// [`GATEWAY_ROUTED`] (the only names this module's logic branches on) plus
/// the self-routed cluster-admin names and a catch-all `"other"` for every
/// non-routed variant. NOT a general-purpose reflection helper — deliberately
/// narrow to this workstream's routed set.
pub fn method_variant_name(m: &Method) -> &'static str {
    METHOD_NAME_RESOLVERS
        .iter()
        .find_map(|resolve| resolve(m))
        .unwrap_or("other")
}

/// Is `method` one of [`GATEWAY_ROUTED`]? `dispatch_graph_op` routes this set to
/// `handlers::graph_ops::try_handle_gateway`, where one kernel owns apply,
/// durability, audit, and CDC.
pub fn is_gateway_routed(m: &Method) -> bool {
    GATEWAY_ROUTED.contains(&method_variant_name(m))
}

/// Cluster execution class for a public request mutation.
///
/// Only commands with a deterministic Raft state-machine representation may be
/// acknowledged in clustered mode. The complete mutating capability ledger is
/// covered by graph commands, typed native commands, explicit service control,
/// a self-routed admin handler, or an explicit local-only refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMutationRoute {
    ReadOnly,
    VolatileControl,
    /// A mutation whose durable authority has process-local ordering only (see
    /// [`LOCAL_ONLY_METHODS`]). A single-node server serves it through its own
    /// owner transaction; clustered admission refuses it with
    /// `LOCAL_ONLY_CLUSTER_REFUSAL` before placement, proposal, saga or store.
    LocalOnly,
    ConsensusGraph,
    ConsensusNative,
    ConsensusFanout,
    /// Cluster-wide, non-graph-scoped admin mutation with its OWN dedicated
    /// handler that resolves `MultiRaft` and performs the leader check /
    /// `OPERATION_REDIRECTED` routing itself (`handlers::raft_admin::try_handle`,
    /// the same shape `handlers::placement::try_handle` uses for reads) — it
    /// does NOT go through [`propose_native_mutation`](crate::server::dispatch)'s
    /// generic `NativeMutationCommand` proposal/replication path, so it must
    /// never be classified `ConsensusNative` (see [`SELF_ROUTED_ADMIN_METHODS`]).
    SelfRoutedAdmin,
}

/// Public coordinator commands that decompose into independently placed,
/// typed consensus graph commands before any local mutation executes.
pub const CONSENSUS_FANOUT_METHODS: &[&str] = &["MultiGraphBatchUpdate", "ApplyChangeEnvelopes"];

/// Mutating, cluster-wide (non-graph-scoped) admin methods that are NOT routed
/// through the generic `NativeMutationCommand` proposal path
/// ([`ClusterMutationRoute::ConsensusNative`]) even though nothing else claims
/// them (not [`GATEWAY_ROUTED`], not `is_durable_mutation`, not
/// [`CONSENSUS_FANOUT_METHODS`]).
///
/// This is the deliberate complement of `crate::raft::NATIVE_CONSENSUS_METHODS`:
/// that list is every mutating method with a *bounded* `NativeMutationCommand`
/// in the command catalog; THIS list is every mutating method that instead owns
/// a dedicated, self-routing handler (resolves `MultiRaft`,
/// performs the leader check, and answers `OPERATION_REDIRECTED` itself — exactly
/// like `handlers::placement::try_handle` does for the read-only `PlacementRoute`,
/// just for a method that also mutates). Routing a `SelfRoutedAdmin` method
/// through `propose_native_mutation` instead would hit
/// `NativeMutationCommand::from_public_method`'s `Err` arm (no bounded native
/// command exists) before ever reaching its real handler — the exact bug this
/// list exists to prevent structurally, not by adding this file to the "please
/// remember" list.
///
/// `crate::server::mutation::tests::clustered_mutation_inventory_is_complete`
/// keeps this list, `GATEWAY_ROUTED`, `crate::raft::NATIVE_CONSENSUS_METHODS`,
/// and `CONSENSUS_FANOUT_METHODS` an exhaustive, non-overlapping partition of
/// every mutating method in `eg_capabilities::method_policy_entries()` — a Method added to
/// the protocol without being placed in exactly one of these buckets fails that
/// test, not a silent `CLUSTER_MUTATION_UNAVAILABLE` at request time.
pub const SELF_ROUTED_ADMIN_METHODS: &[&str] = &["RaftAddLearner", "RaftChangeMembership"];

/// Mutating methods whose authority has no replicated ordering yet.
///
/// The SQL source owner serializes the source cursor, grant snapshot and
/// receipt under a process-local authority lock, so replicas cannot apply it
/// identically. The agent-library family (X7) and the decision, pack and
/// outbox surfaces built on it commit through the SAME process-local owner, so
/// they share that limitation exactly. Before this list named them, a clustered
/// agent publish fell through to a generic refusal that said nothing about why.
///
/// These are refused in clustered mode with [`LOCAL_ONLY_CLUSTER_REFUSAL`]
/// rather than proposed as a native command that does not exist, or applied on
/// one node only.
pub const LOCAL_ONLY_METHODS: &[&str] = &[
    "AgentComponent",
    "AgentGraph",
    "AgentLibrary",
    "AgentTemplate",
    "ConnectorPack",
    "WriteBack",
    "DecisionCommit",
    "DecisionEval",
    "DecisionFit",
    "GraphSchema",
    "MutationOutbox",
    "SqlSourceBatch",
];

/// The local-only names that are NOT gateway-routed.
///
/// `GraphSchema` is deliberately absent: it resolves through
/// [`graph_control_method_name`] because it IS gateway-routed, and naming it
/// twice would make `method_variant_name` depend on resolver order.
fn native_local_only_method_name(m: &Method) -> Option<&'static str> {
    match m {
        Method::AgentComponent { .. } => Some("AgentComponent"),
        Method::AgentGraph { .. } => Some("AgentGraph"),
        Method::AgentLibrary { .. } => Some("AgentLibrary"),
        Method::AgentTemplate { .. } => Some("AgentTemplate"),
        Method::ConnectorPack { .. } => Some("ConnectorPack"),
        Method::DecisionCommit { .. } => Some("DecisionCommit"),
        Method::DecisionEval { .. } => Some("DecisionEval"),
        Method::DecisionFit { .. } => Some("DecisionFit"),
        Method::MutationOutbox { .. } => Some("MutationOutbox"),
        _ => None,
    }
}

/// Governed connector write-back is local-only until its control owner has a
/// replicated ordering protocol. It remains a separate semantic resolver from
/// the agent/decision catalog group above.
fn write_back_method_name(m: &Method) -> Option<&'static str> {
    match m {
        Method::WriteBack { .. } => Some("WriteBack"),
        _ => None,
    }
}

/// The typed clustered-mode refusal for [`LOCAL_ONLY_METHODS`].
#[cfg(feature = "raft")]
pub const LOCAL_ONLY_CLUSTER_REFUSAL: &str =
    "CLUSTER_MUTATION_UNAVAILABLE: this mutation authority has no replicated ordering";

/// Closed current-schema `ApplyMutation.event_type` values that are consumed by
/// a served native coordinator before the ordinary single-graph gateway. Their
/// payloads remain exact signed protocol data; no open-ended event dispatch is
/// admitted here.
pub const COORDINATED_APPLY_MUTATION_EVENTS: &[&str] = &[
    #[cfg(feature = "sparql-http")]
    crate::server::sparql_http::SPARQL_HTTP_UPDATE_EVENT,
];

#[cfg(feature = "sparql-http")]
pub(crate) fn is_sparql_http_update(method: &Method) -> bool {
    matches!(method, Method::ApplyMutation { event_type, .. }
        if event_type == crate::server::sparql_http::SPARQL_HTTP_UPDATE_EVENT)
}

pub(super) fn consensus_apply_is_authorized() -> bool {
    #[cfg(feature = "raft")]
    {
        crate::server::dispatch::is_replicated_apply()
    }
    #[cfg(not(feature = "raft"))]
    {
        false
    }
}

pub fn cluster_mutation_route(method: &Method) -> ClusterMutationRoute {
    if !eg_capabilities::policy(method).mutates {
        return ClusterMutationRoute::ReadOnly;
    }
    if let Some(route) = local_only_route(method).or_else(|| cluster_mutation_route_admin(method)) {
        return route;
    }
    if let Some(route) = cluster_mutation_route_consensus(method) {
        return route;
    }
    #[cfg(feature = "modality-serving")]
    if let Method::ServedModality { op } = method {
        return if op.mutates() {
            ClusterMutationRoute::ConsensusNative
        } else {
            ClusterMutationRoute::ReadOnly
        };
    }
    if crate::mutation_apply::is_durable_mutation(method)
        && !crate::server::mutation_batch::is_work_item_mutation_method(method)
        && !crate::server::mutation_batch::is_capacity_method(method)
    {
        ClusterMutationRoute::ConsensusGraph
    } else {
        // The native constructor is the fail-closed typed admission boundary.
        // The inventory test below proves every CURRENT mutating method reaching
        // this branch has a bounded constructor; a future method cannot mutate
        // locally because dispatch proposes this route unconditionally.
        ClusterMutationRoute::ConsensusNative
    }
}

/// [`LOCAL_ONLY_METHODS`] are refused in clustered mode, never proposed.
fn local_only_route(method: &Method) -> Option<ClusterMutationRoute> {
    LOCAL_ONLY_METHODS
        .contains(&method_variant_name(method))
        .then_some(ClusterMutationRoute::LocalOnly)
}

/// The admin/control-plane arms of [`cluster_mutation_route`]: `Shutdown`,
/// `KgDelegate`, `RaftAddLearner`/`RaftChangeMembership`, `PlacementAdmin`,
/// `RegisterServer`.
/// `None` when `method` is none of these (the caller falls through to
/// [`cluster_mutation_route_consensus`]).
fn cluster_mutation_route_admin(method: &Method) -> Option<ClusterMutationRoute> {
    // RF-ADR-009 performs tenant-bound mapping/raw admission locally, then
    // routes its sole graph effect through the existing ChangeEnvelope
    // consensus authority. Proposing SourceIngest itself as a native command
    // would either require caller records in a second command language or
    // double-propose the lowered envelope.
    // KgDelegate likewise validates explicit Local/Multi/Missing placement
    // authority before lowering to the existing replicated WorkItem command.
    if matches!(
        method,
        Method::Shutdown | Method::KgDelegate { .. } | Method::SourceIngest { .. }
    ) {
        return Some(ClusterMutationRoute::VolatileControl);
    }
    if matches!(
        method,
        Method::RaftAddLearner { .. } | Method::RaftChangeMembership { .. }
    ) {
        // These mutate (`admin:cluster`, `ControlRedb`, `Saga` — the SAME policy
        // shape as `Reshard`/`CatalogAssign`/etc.), so `authz_action ==
        // "admin:cluster"` alone can't discriminate them: those sibling methods
        // legitimately ARE `ConsensusNative` (they have a bounded
        // `NativeMutationDomain::ClusterAdmin` command). What actually sets
        // `RaftAddLearner`/`RaftChangeMembership` apart is that they have NO
        // bounded `NativeMutationCommand` at all — see
        // `SELF_ROUTED_ADMIN_METHODS`'s doc comment — and are handled entirely by
        // `handlers::raft_admin::try_handle`, which resolves `MultiRaft` and does
        // its own leader check/`OPERATION_REDIRECTED` redirect, exactly like
        // `handlers::placement::try_handle` does for `PlacementRoute`.
        debug_assert!(
            SELF_ROUTED_ADMIN_METHODS.contains(&method_variant_name(method)),
            "SELF_ROUTED_ADMIN_METHODS drifted from this match arm"
        );
        return Some(ClusterMutationRoute::SelfRoutedAdmin);
    }
    // `PlacementAdmin` (CONCEPT:EG-KG.sharding.placement-catalog-admin-rpc, DIST-P2-5) is durable, but
    // NOT via this generic layer: `MultiRaft::placement_assign`/`TenantManager::
    // move_partition`/`abort_move` each commit through their OWN raft round-trip
    // (`commit_placement` -> the DEFAULT group's `client_write`) BEFORE this
    // classifier ever runs on the replicated-apply pass. Routing it through
    // `ConsensusNative`'s generic `propose_native_mutation` wrapper (as `Reshard`/
    // `CatalogAssign` correctly do — they have no self-contained commit of their
    // own) would propose a SECOND, redundant raft entry and, worse, re-enter
    // `handlers::placement::try_handle` -> another `commit_placement` proposal from
    // INSIDE that second entry's own apply callback. `VolatileControl` is the
    // existing bucket for "this method's own mechanism handles durability; the
    // generic cluster-routing layer does nothing extra" (see `Shutdown` above) --
    // the name describes this layer's role, not whether the effect persists.
    if matches!(method, Method::PlacementAdmin { .. }) {
        return Some(ClusterMutationRoute::VolatileControl);
    }
    // `RegisterServer` (CONCEPT:EG-KG.sharding.server-registry, W2.5) never itself reaches
    // this classifier at runtime: `dispatch.rs`'s `handle_register_server` intercepts it
    // BEFORE the mutation gateway, validates it, and self-translates into a
    // `Method::AddNode` against `__commons__` dispatched through the ordinary
    // `dispatch_graph_op` path (which IS `GATEWAY_ROUTED`, hence already properly
    // raft-replicated). Same shape as `PlacementAdmin` immediately above: this generic
    // layer does nothing extra for the method NAMED `RegisterServer` because its own
    // mechanism (the translation) already lands the mutation on an already-safe path.
    // `FleetCatalog` (EH-345) writes the same way: its handler self-translates
    // each write into a `CreateNodeIfAbsent`/`CompareAndSetNodeFields` against
    // `__commons__` through `dispatch_graph_op`.
    if matches!(
        method,
        Method::RegisterServer { .. } | Method::FleetCatalog { .. }
    ) {
        return Some(ClusterMutationRoute::VolatileControl);
    }
    None
}

/// The consensus/fanout arms of [`cluster_mutation_route`]: `ApplyChangeEnvelope`
/// (single-graph native consensus), and the batch/fanout-shaped methods
/// (`ApplyChangeEnvelopes`, the SPARQL-HTTP update event, `MultiGraphBatchUpdate`)
/// that decompose into independently-placed per-graph commits. `None` when `method`
/// is none of these.
fn cluster_mutation_route_consensus(method: &Method) -> Option<ClusterMutationRoute> {
    if matches!(method, Method::ApplyChangeEnvelope { .. }) {
        return Some(ClusterMutationRoute::ConsensusNative);
    }
    // The batch coordinator decomposes into per-graph `ApplyChangeEnvelope`-shaped
    // sub-batches (each an independently-placed native consensus commit) before any
    // local mutation — the same shape as `MultiGraphBatchUpdate`.
    if matches!(method, Method::ApplyChangeEnvelopes { .. }) {
        return Some(ClusterMutationRoute::ConsensusFanout);
    }
    #[cfg(feature = "sparql-http")]
    if is_sparql_http_update(method) {
        return Some(ClusterMutationRoute::ConsensusFanout);
    }
    if matches!(method, Method::MultiGraphBatchUpdate { .. }) {
        return Some(ClusterMutationRoute::ConsensusFanout);
    }
    None
}
