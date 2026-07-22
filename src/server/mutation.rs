//! `MutationPlan` + the single commit gateway (CONCEPT:EG-P0-2).
//!
//! ## Why this exists
//!
//! `eg-capabilities` (CONCEPT:EG-P0-1) is a machine-checked, exhaustive `MethodPolicy`
//! table over every `Method` variant — but on its own it is a one-way AUDIT: it
//! cross-checks the existing hand-rolled classifiers (`access.rs::requires_write`,
//! `mutation_apply::is_durable_mutation`, `audit.rs::audit_line`, `cdc.rs::emit_for_method`)
//! against its declared policy, without either side CONSUMING the other. Handlers
//! still mutate `eg-core` directly (`src/server/handlers/graph_ops.rs`'s
//! `g.add_node(...)` etc.), and the dispatch shell (`src/server/dispatch.rs`)
//! separately re-derives whether to durably commit / CDC-emit a given method from its
//! own classifiers.
//!
//! This module is the other direction: [`MutationPlan`] is POPULATED FROM
//! `eg_capabilities::policy` (never re-hardcoded), and [`commit_mutation`] is the
//! ONE place a routed mutation's authz + durable write + CDC emission happen
//! together, driven by that plan. The invariant this buys: for a method in
//! [`GATEWAY_ROUTED`], mutation + durability + audit + CDC happen together, in one
//! call, declared by policy — never scattered back across the dispatch shell.
//!
//! ## Scope (read before assuming more is covered)
//!
//! [`GATEWAY_ROUTED`] methods are wired through this gateway. EG-P0-2 started with
//! 7 (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge` + `CreateSummaryNode`/
//! `Consolidate`/`Reinforce`); the L11 rollout expanded it across the full routed
//! graph surface, adding:
//!   * the rest of the plain graph-core family (CAS/claim/edge-temporal/scene/
//!     trajectory/embedding/ledger/lifecycle ops — all handled in `graph_ops.rs`
//!     already, same shape as the original 7);
//!   * the message-broker/stream family (`Outbox` durability domain, behind
//!     `feature = "broker"` — proves the gateway is domain-agnostic: `commit_mutation`
//!     doesn't branch on `DurabilityDomain`, only on whether it's `None`);
//!   * `IcvConfigure` (validated graph control state carried in the staged
//!     `GraphCore`, so policy and data share the graph-scoped durable commit);
//!   * BOTH runtime-conditional families whose REAL `mutates` answer is a
//!     per-invocation `writeback: bool` field, not `policy()`'s conservative
//!     upper bound: `GraphLearnFit`/`GraphLearnPredict` and every writeback-capable
//!     `Mine*` — routed via [`commit_conditional_mutation`], NOT `commit_mutation`,
//!     so a `writeback: false` call is correctly treated as a plain read (Read
//!     ACL, no durability/audit/CDC) instead of being incorrectly gated behind
//!     Write access or persisted as a phantom mutation.
//!
//! Coverage is machine-partitioned by
//! `tests::gateway_routed_set_matches_mutating_policy_surface`: graph-scoped
//! mutations use this gateway; WorkItem, OCC/2PC, lifecycle, and non-graph stores
//! identify their native atomic coordinator. `OPEN_NOT_JUSTIFIED` is asserted
//! empty, so a new mutating method cannot silently bypass both categories.
//!
//! `src/server/dispatch.rs` wires `handlers::graph_ops::try_handle_gateway` ahead
//! of the generic handler chain. A routed method therefore reaches exactly one
//! mutation kernel. The gateway re-enters the per-graph writer from inside
//! `commit_mutation` (L18, see [`try_coalesce_apply`]), so routed structural writes
//! retain batching without a second durability path.
//!
//! ## What this gateway does NOT reimplement
//!
//! Audit-chain emission (the tamper-evident hash chain, `audit.rs` +
//! `redb_store::append_audit_entry`) is stateful PER GRAPH (each entry chains off
//! the previous one's hash) and already lives inside the durable-commit path
//! (`PersistenceBackend::record_durable` → the redb backend). Reimplementing
//! that chaining here would risk a second, diverging chain. Instead,
//! Compact row commits and staged-state MutationBatch commits both append the audit
//! entry inside their authoritative redb transaction. `plan.audited`
//! (sourced from `eg_capabilities::policy`) is the assertable, tested FACT that this
//! delegation actually happens for the methods policy says should be audited (see
//! `tests::routed_mutation_produces_one_durable_record_one_audit_entry_and_one_cdc_event`,
//! which uses a REAL `RedbBackend` and reads the audit chain back).
//!
//! The per-graph write-coalescer's batching optimization is NO LONGER bypassed for
//! the routed set (L18/EG-P0-6): [`commit_mutation`] step 4 routes a coalescable
//! routed mutation (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`) through the
//! `WriteCoalescerRegistry::writer_for` path (see [`try_coalesce_apply`]), so the
//! hot-path structural writes batch
//! (`stats().ops()` counts them) while durability/audit/CDC ordering is preserved. The
//! non-coalescable routed memory ops (`CreateSummaryNode`/`Consolidate`/`Reinforce`)
//! keep the inline `apply`. With Raft active, dispatch reaches the consensus barrier
//! before this local gateway; each committed ordinary Raft method is then staged and
//! committed through the same state-backed MutationBatch authority on every replica.

use std::sync::Arc;

use eg_capabilities::DurabilityDomain;

use crate::graph::GraphCore;
use crate::isolation::{AccessLevel, IsolationLayer};
use crate::protocol::{GraphType, Method, Response, ResultPayload};
use crate::server::access::check_graph_access;
use crate::server::persistence::PersistenceBackend;

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
/// complement (every other mutating method, per `eg_capabilities::ALL_METHODS`),
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

/// Extract a `Method` variant's name as a `&'static str`, covering exactly
/// [`GATEWAY_ROUTED`] (the only names this module's logic branches on) plus a
/// catch-all `"other"` for every non-routed variant. NOT a general-purpose
/// reflection helper — deliberately narrow to this workstream's routed set.
pub fn method_variant_name(m: &Method) -> &'static str {
    match m {
        Method::AddNode { .. } => "AddNode",
        Method::CreateNodeIfAbsent { .. } => "CreateNodeIfAbsent",
        Method::RemoveNode { .. } => "RemoveNode",
        Method::AddEdge { .. } => "AddEdge",
        Method::RemoveEdge { .. } => "RemoveEdge",
        Method::CreateSummaryNode { .. } => "CreateSummaryNode",
        Method::Consolidate { .. } => "Consolidate",
        Method::Reinforce { .. } => "Reinforce",
        Method::CompareAndSetNodeFields { .. } => "CompareAndSetNodeFields",
        Method::ClaimNext { .. } => "ClaimNext",
        Method::DecayNode { .. } => "DecayNode",
        Method::DecayMemories { .. } => "DecayMemories",
        Method::EvictBelow { .. } => "EvictBelow",
        Method::Maintain { .. } => "Maintain",
        Method::AddSceneObject { .. } => "AddSceneObject",
        Method::SetPose { .. } => "SetPose",
        Method::Reparent { .. } => "Reparent",
        Method::StartTrajectory { .. } => "StartTrajectory",
        Method::AppendStep { .. } => "AppendStep",
        Method::AddEmbedding { .. } => "AddEmbedding",
        Method::InvalidateEdge { .. } => "InvalidateEdge",
        Method::SupersedeEdge { .. } => "SupersedeEdge",
        Method::ClearGraph => "ClearGraph",
        Method::EvictLRU { .. } => "EvictLRU",
        Method::DecaySweep { .. } => "DecaySweep",
        Method::TouchNodes { .. } => "TouchNodes",
        Method::FromMsgpack { .. } => "FromMsgpack",
        Method::Reconcile { .. } => "Reconcile",
        Method::ApplyMutation { .. } => "ApplyMutation",
        #[cfg(feature = "reasoning")]
        Method::RunDatalogReasoning { .. } => "RunDatalogReasoning",
        #[cfg(feature = "shacl")]
        Method::IcvConfigure { .. } => "IcvConfigure",
        Method::PruneByLifecycle { .. } => "PruneByLifecycle",
        Method::BatchUpdate { .. } => "BatchUpdate",
        Method::ClearLedger => "ClearLedger",
        Method::ApplyLedger { .. } => "ApplyLedger",
        Method::CompactNodesByType { .. } => "CompactNodesByType",
        #[cfg(feature = "broker")]
        Method::DeclareExchange { .. } => "DeclareExchange",
        #[cfg(feature = "broker")]
        Method::DeleteExchange { .. } => "DeleteExchange",
        #[cfg(feature = "broker")]
        Method::BindQueue { .. } => "BindQueue",
        #[cfg(feature = "broker")]
        Method::UnbindQueue { .. } => "UnbindQueue",
        #[cfg(feature = "broker")]
        Method::Publish { .. } => "Publish",
        #[cfg(feature = "broker")]
        Method::DeclareQueue { .. } => "DeclareQueue",
        #[cfg(feature = "broker")]
        Method::PublishEx { .. } => "PublishEx",
        #[cfg(feature = "broker")]
        Method::BrokerConsume { .. } => "BrokerConsume",
        #[cfg(feature = "broker")]
        Method::BrokerAck { .. } => "BrokerAck",
        #[cfg(feature = "broker")]
        Method::BrokerReject { .. } => "BrokerReject",
        #[cfg(feature = "broker")]
        Method::SweepExpired { .. } => "SweepExpired",
        #[cfg(feature = "broker")]
        Method::StreamDeclare { .. } => "StreamDeclare",
        #[cfg(feature = "broker")]
        Method::StreamPublish { .. } => "StreamPublish",
        #[cfg(feature = "broker")]
        Method::StreamTrim { .. } => "StreamTrim",
        #[cfg(feature = "broker")]
        Method::StreamCommitOffset { .. } => "StreamCommitOffset",
        #[cfg(feature = "broker")]
        Method::PublishConfirmed { .. } => "PublishConfirmed",
        #[cfg(feature = "broker")]
        Method::PublishIdempotent { .. } => "PublishIdempotent",
        #[cfg(feature = "broker")]
        Method::BrokerAckTag { .. } => "BrokerAckTag",
        #[cfg(feature = "broker")]
        Method::BrokerNackTag { .. } => "BrokerNackTag",
        #[cfg(feature = "broker")]
        Method::BrokerRenewTag { .. } => "BrokerRenewTag",
        #[cfg(feature = "graphlearn")]
        Method::GraphLearnFit { .. } => "GraphLearnFit",
        #[cfg(feature = "graphlearn")]
        Method::GraphLearnPredict { .. } => "GraphLearnPredict",
        #[cfg(feature = "mining")]
        Method::MineAssociate { .. } => "MineAssociate",
        #[cfg(feature = "mining")]
        Method::MineCluster { .. } => "MineCluster",
        #[cfg(feature = "mining")]
        Method::MineAnomaly { .. } => "MineAnomaly",
        #[cfg(feature = "mining")]
        Method::MineClassifyPredict { .. } => "MineClassifyPredict",
        #[cfg(feature = "mining")]
        Method::MineReduce { .. } => "MineReduce",
        #[cfg(feature = "mining")]
        Method::MineSequence { .. } => "MineSequence",
        #[cfg(feature = "mining")]
        Method::MineForecast { .. } => "MineForecast",
        #[cfg(feature = "mining")]
        Method::MineText { .. } => "MineText",
        #[cfg(feature = "mining")]
        Method::MineSubgraph { .. } => "MineSubgraph",
        #[cfg(feature = "mining")]
        Method::MineEntityResolve { .. } => "MineEntityResolve",
        #[cfg(feature = "mining")]
        Method::MineCausalImpact { .. } => "MineCausalImpact",
        #[cfg(feature = "mining")]
        Method::MineProcess { .. } => "MineProcess",
        #[cfg(feature = "mining")]
        Method::MineRootCause { .. } => "MineRootCause",
        #[cfg(feature = "mining")]
        Method::MineRiskPropagation { .. } => "MineRiskPropagation",
        #[cfg(feature = "mining")]
        Method::MineOntologyGap { .. } => "MineOntologyGap",
        #[cfg(feature = "mining")]
        Method::MineRetrievalQuality { .. } => "MineRetrievalQuality",
        #[cfg(feature = "mining")]
        Method::MineCommunity { .. } => "MineCommunity",
        // L11 batch 4: runtime-conditional query surface (`Sql`/`CypherQuery`
        // unconditional in the enum; `GraphQl` behind `graphql`).
        Method::Sql { .. } => "Sql",
        Method::CypherQuery { .. } => "CypherQuery",
        #[cfg(feature = "graphql")]
        Method::GraphQl { .. } => "GraphQl",
        // L11 batch 4: native RDF write surface (behind `rdf`).
        #[cfg(feature = "rdf")]
        Method::AddTriples { .. } => "AddTriples",
        #[cfg(feature = "rdf")]
        Method::RemoveTriples { .. } => "RemoveTriples",
        #[cfg(feature = "rdf")]
        Method::DropNamedGraph => "DropNamedGraph",
        #[cfg(feature = "modality-serving")]
        Method::ServedModality { .. } => "ServedModality",
        _ => "other",
    }
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
/// covered by graph commands, typed native commands, or explicit service control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMutationRoute {
    ReadOnly,
    VolatileControl,
    ConsensusGraph,
    ConsensusNative,
    ConsensusFanout,
}

/// Shared plaintext ceiling for one encrypted native consensus command. Served
/// coordinators preflight against this exact value before allocating/dispatching
/// their encoded method, and Raft enforces it again when sealing/opening.
pub(crate) const MAX_NATIVE_COORDINATOR_PAYLOAD_BYTES: usize = 128 * 1024 * 1024;

/// Public coordinator commands that decompose into independently placed,
/// typed consensus graph commands before any local mutation executes.
pub const CONSENSUS_FANOUT_METHODS: &[&str] = &["MultiGraphBatchUpdate"];

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

fn consensus_apply_is_authorized() -> bool {
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
    if matches!(method, Method::Shutdown) {
        return ClusterMutationRoute::VolatileControl;
    }
    if matches!(method, Method::ApplyChangeEnvelope { .. }) {
        return ClusterMutationRoute::ConsensusNative;
    }
    #[cfg(feature = "sparql-http")]
    if is_sparql_http_update(method) {
        return ClusterMutationRoute::ConsensusFanout;
    }
    if matches!(method, Method::MultiGraphBatchUpdate { .. }) {
        return ClusterMutationRoute::ConsensusFanout;
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
        && !crate::server::mutation_batch::is_work_item_method(method)
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

/// Everything [`commit_mutation`] needs beyond the plan + the apply closure.
/// Borrowed, not owned — built fresh per request from already-resolved
/// `dispatch_graph_op` locals (the registry lock is never re-acquired here).
pub struct MutationCtx<'a> {
    pub req_id: u64,
    pub caller: Option<&'a str>,
    /// Opaque tenant authority derived from the verified request context.
    pub tenant_scope: &'a str,
    pub graph_name: &'a str,
    pub graph_type: GraphType,
    pub owner: Option<&'a str>,
    pub isolation: &'a IsolationLayer,
    pub core: &'a Arc<GraphCore>,
    pub persistence: Option<&'a Arc<dyn PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    pub cdc: Option<&'a Arc<crate::server::cdc::CdcHub>>,
    /// Generation-owned completeness/freshness watermark for this resident core.
    /// The registry replaces or invalidates this handle on lifecycle transitions,
    /// so a stale gateway completion cannot publish into a same-name recreation.
    pub materialization_manifest:
        Option<&'a Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    /// Per-graph write-coalescer registry (CONCEPT:EG-KG.sharding.per-graph-write-coalescer, L18).
    /// When present + enabled, a coalescable routed mutation
    /// (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`) has its in-memory apply
    /// BATCHED onto this graph's single-writer queue instead of taking the topology
    /// lock itself, so routing a hot-path write through the gateway retains
    /// write-batching.
    /// `None` (or a disabled/non-coalescable method) ⇒ the `apply` closure runs
    /// inline, unchanged. See [`commit_mutation`] step 4.
    pub write_coalescer: Option<&'a Arc<crate::write_coalescer::WriteCoalescerRegistry>>,
}

/// Publish the resident freshness watermark only after an authoritative gateway
/// success. The manifest owns monotonic/out-of-order and lifecycle-phase fencing;
/// this gateway owns the exact durable-commit boundary that makes the version
/// authoritative.
fn advance_authoritative_manifest(ctx: &MutationCtx<'_>, plan: &MutationPlan, response: &Response) {
    if response.error.is_some()
        || !plan.mutates
        || matches!(plan.durability_domain, DurabilityDomain::None)
    {
        return;
    }
    let Some(manifest) = ctx.materialization_manifest else {
        return;
    };
    if let Ok(mut manifest) = manifest.write() {
        manifest.advance_committed_version(ctx.core.version());
    }
}

/// Bounded process-global idempotency-replay cache (CONCEPT:EG-P0-2). Keyed by a
/// deterministic `(method, graph, identity-args)` string (see [`idempotency_key`]).
///
/// Bounded, not TTL/LRU, for this increment: past [`MAX_IDEMPOTENCY_ENTRIES`] a new
/// key is simply not cached (fails OPEN — the mutation still applies correctly, it
/// just stops being replay-dedup-eligible until the process restarts or an entry
/// frees up) rather than evicting an arbitrary entry and risking a wrong cache hit
/// for a DIFFERENT request that happens to reuse a key. A real bounded LRU/TTL
/// policy is outside the graph gateway because its native coordinator owns it.
pub struct IdempotencyStore {
    seen: dashmap::DashMap<String, Response>,
}

/// Cap on live idempotency-cache entries (see [`IdempotencyStore`] docs).
const MAX_IDEMPOTENCY_ENTRIES: usize = 10_000;

impl IdempotencyStore {
    pub fn new() -> Self {
        IdempotencyStore {
            seen: dashmap::DashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<Response> {
        self.seen.get(key).map(|r| r.clone())
    }

    pub fn insert(&self, key: String, response: Response) {
        if self.seen.len() < MAX_IDEMPOTENCY_ENTRIES {
            self.seen.insert(key, response);
        }
    }

    #[cfg(test)]
    pub fn clear(&self) {
        self.seen.clear();
    }
}

impl Default for IdempotencyStore {
    fn default() -> Self {
        Self::new()
    }
}

/// The process-global idempotency cache. A `OnceLock` singleton (matching the
/// `max_response_nodes()` precedent in `state.rs`) rather than a new `ServerState`
/// field, so wiring this gateway in touches none of the many `ServerState`
/// construction sites across the codebase.
fn idempotency_store() -> &'static IdempotencyStore {
    static STORE: std::sync::OnceLock<IdempotencyStore> = std::sync::OnceLock::new();
    STORE.get_or_init(IdempotencyStore::new)
}

/// Derive a deterministic replay-dedup key for an idempotent method, scoped to
/// `graph_name` (so the SAME node id in two different graphs never collides). Only
/// meaningful when `MutationPlan::idempotent` is true; the fallback `Debug`-based
/// arm covers a future idempotent addition to [`GATEWAY_ROUTED`] generically
/// (correct but not as precise as a bespoke key).
fn idempotency_key(graph_name: &str, method: &Method) -> String {
    let modality_discriminator = |value: &eg_types::ServedModalityKind| match value {
        eg_types::ServedModalityKind::Document => "document",
        eg_types::ServedModalityKind::Image => "image",
        eg_types::ServedModalityKind::Audio => "audio",
        eg_types::ServedModalityKind::Video => "video",
    };
    match method {
        Method::RemoveNode { node_id } => format!("RemoveNode|{graph_name}|{node_id}"),
        Method::RemoveEdge {
            source_id,
            target_id,
        } => format!("RemoveEdge|{graph_name}|{source_id}|{target_id}"),
        #[cfg(feature = "modality-serving")]
        Method::ServedModality {
            op:
                eg_types::ServedModalityOp::Ingest {
                    modality,
                    idempotency_ref,
                    target_occurrence_id,
                    expected_version,
                    bundle_msgpack,
                    source_bytes,
                },
        } => {
            use sha2::{Digest, Sha256};
            let mut digest = Sha256::new();
            digest.update(b"served-modality-idempotency-v2");
            digest.update(modality_discriminator(modality).as_bytes());
            for value in [idempotency_ref.as_bytes(), target_occurrence_id.as_bytes()] {
                digest.update((value.len() as u64).to_be_bytes());
                digest.update(value);
            }
            digest.update(expected_version.unwrap_or(u64::MAX).to_be_bytes());
            digest.update(Sha256::digest(bundle_msgpack));
            digest.update(Sha256::digest(source_bytes));
            format!("ServedModality|{graph_name}|{}", hex::encode(digest.finalize()))
        }
        Method::ServedModality {
            op:
                eg_types::ServedModalityOp::Delete {
                    modality,
                    idempotency_ref,
                    occurrence_id,
                    expected_version,
                },
        } => format!(
            "ServedModalityDelete|{graph_name}|{}|{idempotency_ref}|{occurrence_id}|{expected_version}",
            modality_discriminator(modality)
        ),
        Method::ServedModality {
            op: eg_types::ServedModalityOp::IngestStream { modality, items },
        } => {
            use sha2::{Digest, Sha256};
            let mut digest = Sha256::new();
            digest.update(b"served-modality-stream-idempotency-v2");
            digest.update(modality_discriminator(modality).as_bytes());
            digest.update((items.len() as u64).to_be_bytes());
            for item in items {
                for value in [
                    item.idempotency_ref.as_bytes(),
                    item.target_occurrence_id.as_bytes(),
                ] {
                    digest.update((value.len() as u64).to_be_bytes());
                    digest.update(value);
                }
                digest.update(item.expected_version.unwrap_or(u64::MAX).to_be_bytes());
                digest.update(Sha256::digest(&item.bundle_msgpack));
                digest.update(Sha256::digest(&item.source_bytes));
            }
            format!("ServedModalityStream|{graph_name}|{}", hex::encode(digest.finalize()))
        }
        other => format!("{other:?}|{graph_name}"),
    }
}

/// Source bodies belong only to the live native decoder. State-backed batches
/// persist a digest of categorical operation data plus already-opaque references;
/// neither source bytes nor their direct content hash enter the receipt. The
/// complete effect is independently authenticated by `MutationStateDescriptor`
/// and the encrypted affected-row payload.
pub(crate) fn durable_receipt_method(method: &Method) -> Method {
    #[cfg(feature = "modality-serving")]
    if let Method::ServedModality { op } = method {
        use eg_types::{ServedModalityKind, ServedModalityOp};
        use sha2::{Digest, Sha256};

        let mut digest = Sha256::new();
        let mut field = |value: &[u8]| {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
        };
        field(b"served-modality-receipt-v1");
        let modality = |value: &ServedModalityKind| match value {
            ServedModalityKind::Document => b"document".as_slice(),
            ServedModalityKind::Image => b"image".as_slice(),
            ServedModalityKind::Audio => b"audio".as_slice(),
            ServedModalityKind::Video => b"video".as_slice(),
        };
        match op {
            ServedModalityOp::Ingest {
                modality: kind,
                idempotency_ref,
                target_occurrence_id,
                expected_version,
                ..
            } => {
                field(b"ingest");
                field(modality(kind));
                field(idempotency_ref.as_bytes());
                field(target_occurrence_id.as_bytes());
                field(&expected_version.unwrap_or(u64::MAX).to_be_bytes());
            }
            ServedModalityOp::IngestStream {
                modality: kind,
                items,
            } => {
                field(b"ingest-stream");
                field(modality(kind));
                field(&(items.len() as u64).to_be_bytes());
                for item in items {
                    field(item.idempotency_ref.as_bytes());
                    field(item.target_occurrence_id.as_bytes());
                    field(&item.expected_version.unwrap_or(u64::MAX).to_be_bytes());
                }
            }
            ServedModalityOp::Delete {
                modality: kind,
                idempotency_ref,
                occurrence_id,
                expected_version,
            } => {
                field(b"delete");
                field(modality(kind));
                field(idempotency_ref.as_bytes());
                field(occurrence_id.as_bytes());
                field(&expected_version.to_be_bytes());
            }
            ServedModalityOp::MoveToCold {
                modality: kind,
                occurrence_id,
            } => {
                field(b"move-to-cold");
                field(modality(kind));
                field(occurrence_id.as_bytes());
            }
            ServedModalityOp::Restore {
                modality: kind,
                occurrence_id,
            } => {
                field(b"restore");
                field(modality(kind));
                field(occurrence_id.as_bytes());
            }
            ServedModalityOp::CollectTombstones {
                modality: kind,
                through_event_sequence,
            } => {
                field(b"collect-tombstones");
                field(modality(kind));
                field(&through_event_sequence.to_be_bytes());
            }
            _ => return method.clone(),
        }
        drop(field);
        return Method::ApplyMutation {
            event_type: "served_modality_v1".to_string(),
            query: format!("sha256:{}", hex::encode(digest.finalize())),
        };
    }
    method.clone()
}

/// Try to apply a coalescable routed mutation THROUGH this graph's write-coalescer
/// (CONCEPT:EG-KG.sharding.per-graph-write-coalescer, L18), returning `Some(Response)` when it was coalesced
/// (the outcome mapped to the SAME `Response` the gateway's inline `apply` closure
/// would produce) or `None` when it must fall back to the inline closure.
///
/// `None` is returned when no coalescer is configured on `ctx`, or `method` is not
/// one of the four coalescable structural writes (`AddNode`/`RemoveNode`/`AddEdge`/
/// `RemoveEdge`) — the other routed methods (`CreateSummaryNode`/`Consolidate`/
/// `Reinforce`) are multi-field memory ops the coalescer does not model, so they
/// keep the inline closure. Mirrors `dispatch::try_coalesce_write` exactly (enqueue
/// with a per-op linger `Instant`, use `apply_one_inline` on a full/closed queue,
/// and await the writer's outcome).
async fn try_coalesce_apply(ctx: &MutationCtx<'_>, method: &Method) -> Option<Response> {
    use crate::write_coalescer::{WriteOp, WriteOutcome};
    use tokio::sync::oneshot;

    let coalescer = ctx.write_coalescer?;

    let (reply, reply_rx) = oneshot::channel::<WriteOutcome>();
    // Only the four coalescable structural writes map to a WriteOp; every other
    // routed method (memory ops) returns None to keep the inline apply.
    let op = match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => WriteOp::AddNode {
            node_id: node_id.clone(),
            properties_msgpack: properties_msgpack.clone(),
            reply,
        },
        Method::RemoveNode { node_id } => WriteOp::RemoveNode {
            node_id: node_id.clone(),
            reply,
        },
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => WriteOp::AddEdge {
            source_id: source_id.clone(),
            target_id: target_id.clone(),
            properties_msgpack: properties_msgpack.clone(),
            reply,
        },
        Method::RemoveEdge {
            source_id,
            target_id,
        } => WriteOp::RemoveEdge {
            source_id: source_id.clone(),
            target_id: target_id.clone(),
            reply,
        },
        _ => return None,
    };

    let writer = coalescer.writer_for(ctx.graph_name, ctx.core);
    // Enqueue; on a full/closed queue apply this single op inline under its own txn
    // (same engine effect, just not batched) so a saturated writer never drops or
    // stalls the write — identical to `dispatch::try_coalesce_write`.
    if let Err(op) = writer.try_enqueue(op) {
        writer.apply_one_inline(ctx.core, ctx.graph_name, op);
    }

    // Await the outcome and rebuild the exact Response the inline closure returns:
    // every coalescable routed op's closure yields `String("ok")` on success (an
    // AddEdge failure surfaces its error string).
    let outcome = reply_rx.await.unwrap_or(WriteOutcome::WriterGone);
    Some(match outcome {
        WriteOutcome::Ok => Response::ok(ctx.req_id, ResultPayload::String("ok".to_string())),
        WriteOutcome::Cas(b) => Response::ok(ctx.req_id, ResultPayload::Bool(b)),
        WriteOutcome::Err(e) => Response::err(ctx.req_id, e),
        WriteOutcome::WriterGone => {
            Response::err(ctx.req_id, "write worker unavailable".to_string())
        }
    })
}

/// Pre-apply state carried from [`commit_prepare`] to [`commit_finalize`], so the
/// SAME steps-1-3 / steps-5-8 logic backs BOTH the sync-apply [`commit_mutation`]
/// (graph-core/broker/memory ops) and the async-apply
/// [`commit_conditional_mutation_async`] (the query + RDF surfaces, whose execution
/// is `async` and needs `state`/`rls`) — one implementation, never two drifting
/// copies of the durability/audit/CDC/idempotency contract.
struct CommitPrep {
    /// `Some` only for a policy-idempotent method — the replay-dedup cache key.
    dedup_key: Option<String>,
    /// CDC pre-image captured before apply (Skip when policy `emits_cdc == false`).
    #[cfg(feature = "streaming")]
    cdc_pre: crate::server::cdc::CdcPre,
}

/// Steps 1-3 of the commit gateway (CONCEPT:EG-P0-2): Write-authz, idempotency-replay
/// short-circuit, and the pre-apply CDC image. Returns `Err(Response)` to short-
/// circuit the whole call (an ACCESS_DENIED, or a cached idempotent replay — no re-
/// apply), else `Ok(CommitPrep)` for the caller to run apply + [`commit_finalize`].
fn commit_prepare(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Result<CommitPrep, Response> {
    // 1. Authz -- the SAME isolation ACL check every other graph-scoped method
    // goes through, driven by the plan's `mutates` flag. (A read-only invocation of
    // a runtime-conditional method never reaches here — see
    // `commit_conditional_mutation_async`'s `mutates_now == false` branch.)
    if !consensus_apply_is_authorized() {
        if let Err(denied) = check_graph_access(
            ctx.isolation,
            ctx.caller,
            ctx.graph_name,
            ctx.graph_type,
            ctx.owner,
            AccessLevel::Write,
        ) {
            return Err(Response::err(ctx.req_id, denied));
        }
    }

    // 2. Idempotency-replay dedup -- ONLY for methods policy marks idempotent. A
    // byte-identical replay short-circuits BEFORE touching storage: no re-apply, no
    // second durable record, no second audit-chain entry, no duplicate CDC event.
    let dedup_key = plan
        .idempotent
        .then(|| idempotency_key(ctx.graph_name, method));
    if let Some(key) = &dedup_key {
        if let Some(cached) = idempotency_store().get(key) {
            return Err(cached);
        }
    }

    // 3. CDC pre-image, captured BEFORE the mutation applies -- gated on the
    // POLICY's `emits_cdc`.
    #[cfg(feature = "streaming")]
    let cdc_pre = if plan.emits_cdc {
        crate::server::cdc::capture_before(ctx.core, method)
    } else {
        crate::server::cdc::CdcPre::Skip
    };

    Ok(CommitPrep {
        dedup_key,
        #[cfg(feature = "streaming")]
        cdc_pre,
    })
}

/// Steps 5-8 of the commit gateway: mark-dirty, durable commit (which for a redb
/// backend also appends the tamper-evident audit-chain entry — see module docs),
/// CDC emit (+ the `epistemic-tms` truth-maintenance hook, step 7.5, riding the same
/// ordering), and idempotency-replay cache insert. `response` is the applied
/// outcome; on an ERROR response NOTHING durable/audited/CDC-emitted/cached happens.
async fn commit_finalize(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    response: Response,
    prep: CommitPrep,
    durability_already_committed: bool,
    preserve_node_indexes: bool,
) -> Response {
    if response.error.is_some() {
        return response;
    }

    // 5. Mark the graph dirty (checkpoint scheduling). Idempotent flag set.
    if plan.mutates {
        if preserve_node_indexes {
            ctx.core.mark_dirty_preserving_indexes();
        } else {
            ctx.core.mark_dirty();
        }
    }

    // 6. Durable commit -- reuses the EXISTING `PersistenceBackend` plumbing.
    if !durability_already_committed && !matches!(plan.durability_domain, DurabilityDomain::None) {
        let Some(persistence) = ctx.persistence else {
            return Response::err(
                ctx.req_id,
                "durable mutation requires a persistence backend",
            );
        };
        let fname = crate::persist::sanitize(ctx.graph_name);
        if let Err(e) = persistence.record_durable(&fname, method).await {
            return Response::err(
                ctx.req_id,
                format!("durable commit failed (write not acknowledged): {e}"),
            );
        }
    }

    // 7. CDC emit -- only AFTER the authoritative durable commit succeeds.
    #[cfg(feature = "streaming")]
    if plan.emits_cdc {
        if let Some(hub) = ctx.cdc {
            crate::server::cdc::emit_for_method(
                hub,
                ctx.core,
                ctx.graph_name,
                method,
                prep.cdc_pre,
            );
        }
    }

    // 8. Cache the response for idempotent-replay dedup.
    if let Some(key) = prep.dedup_key {
        idempotency_store().insert(key, response.clone());
    }

    response
}

/// The single commit gateway (CONCEPT:EG-P0-2): authz, apply, durable commit,
/// audit (delegated — see module docs), CDC, and idempotency-replay dedup, all in
/// ONE call, driven entirely by `plan` (sourced from `eg_capabilities::policy`).
///
/// `apply` performs the actual `eg-core` mutation and returns the success payload;
/// it is only invoked after authz passes and (for an idempotent replay hit) is
/// skipped entirely. On a failed apply, nothing durable/audited/CDC-emitted/cached
/// happens.
pub async fn commit_mutation<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    let response = commit_mutation_inner(ctx, plan, method, apply).await;
    advance_authoritative_manifest(ctx, plan, &response);
    response
}

async fn commit_mutation_inner<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    // Every mutation shares one logical graph lane from validation through RAM
    // publication. Durable domains fail closed without an authoritative batch
    // backend.
    let _mutation_guard = crate::server::mutation_batch::lock_graph(ctx.graph_name).await;
    let prep = match commit_prepare(ctx, plan, method) {
        Ok(p) => p,
        Err(short_circuit) => return short_circuit,
    };

    if matches!(plan.durability_domain, DurabilityDomain::None) {
        let response = match apply(ctx.core) {
            Ok(payload) => Response::ok(ctx.req_id, payload),
            Err(error) => Response::err(ctx.req_id, error),
        };
        return commit_finalize(ctx, plan, method, response, prep, true, false).await;
    }
    let Some(persistence) = ctx.persistence else {
        return Response::err(
            ctx.req_id,
            "authoritative MutationBatch commit requires a persistence backend",
        );
    };

    let fname = crate::persist::sanitize(ctx.graph_name);
    let batch_id = crate::server::mutation_batch::opaque_request_key(
        "rpc",
        ctx.graph_name,
        ctx.req_id,
        method,
    );

    // A retry after `fsync` but before RAM publication repairs the serving
    // projection from authority and returns the exact stored result. No handler is
    // re-executed and no duplicate outbox row is produced.
    match persistence.read_mutation_batch(&fname, &batch_id).await {
        Ok(Some(record)) => {
            match persistence.read_authoritative_graph_snapshot(&fname).await {
                Ok(Some((snapshot, version))) => {
                    if let Err(error) = ctx.core.install_committed_snapshot(snapshot, version) {
                        return Response::err(
                            ctx.req_id,
                            format!("committed projection reconciliation failed: {error}"),
                        );
                    }
                }
                Ok(None) => {
                    return Response::err(
                        ctx.req_id,
                        "committed projection reconciliation found no authoritative graph",
                    );
                }
                Err(error) => {
                    return Response::err(
                        ctx.req_id,
                        format!("committed projection reconciliation failed: {error}"),
                    )
                }
            }
            let response = record
                .result_msgpack
                .as_deref()
                .ok_or_else(|| "committed MutationBatch has no durable result".to_string())
                .and_then(|bytes| {
                    eg_types::msgpack::decode_bounded(
                        bytes,
                        eg_types::msgpack::MsgpackLimits::new(64 * 1024 * 1024, 1_000_000, 64),
                    )
                    .map_err(|_| "committed MutationBatch result is corrupt".to_string())
                })
                .map(|payload| Response::ok(ctx.req_id, payload))
                .unwrap_or_else(|error| Response::err(ctx.req_id, error));
            if let Some(key) = prep.dedup_key {
                idempotency_store().insert(key, response.clone());
            }
            return response;
        }
        Ok(None) => {}
        Err(error) => {
            return Response::err(
                ctx.req_id,
                format!("MutationBatch status lookup failed: {error}"),
            )
        }
    }

    let created_at_ms = crate::server::dispatch::authoritative_now_ms();

    // Row-local operations have a deterministic success result and can commit
    // their compact Method directly before applying the serving projection.
    if let Some(predicted) = prepublish_success(ctx.core, method) {
        let source_version = match crate::server::mutation_batch::authoritative_graph_version(
            persistence,
            &fname,
            ctx.core,
        )
        .await
        {
            Ok(version) => version,
            Err(error) => {
                return Response::err(
                    ctx.req_id,
                    format!("authoritative version read failed: {error}"),
                )
            }
        };
        let batch = match crate::server::mutation_batch::compile_methods(
            crate::server::mutation_batch::CompileBatch {
                batch_id: &batch_id,
                request_id: ctx.req_id,
                principal: ctx.caller,
                tenant: ctx.tenant_scope,
                graph: ctx.graph_name,
                placement_epoch: 0,
                idempotency_key: &batch_id,
                expected_graph_version: Some(source_version),
                fencing_token: None,
                created_at_ms,
                default_surface: crate::mutation_batch::MutationSurface::Graph,
                authoritative_state: None,
            },
            vec![method.clone()],
        ) {
            Ok(batch) => batch,
            Err(error) => {
                return Response::err(ctx.req_id, format!("MutationBatch compile failed: {error}"))
            }
        };
        let result = match rmp_serde::to_vec_named(&predicted) {
            Ok(result) => result,
            Err(error) => {
                return Response::err(
                    ctx.req_id,
                    format!("MutationBatch result encode failed: {error}"),
                )
            }
        };
        let committed = match persistence
            .commit_mutation_batch(&fname, &batch, Some(&result), created_at_ms)
            .await
        {
            Ok(committed) => committed,
            Err(error) => {
                return Response::err(
                    ctx.req_id,
                    format!("MutationBatch durable commit failed: {error}"),
                )
            }
        };
        if committed.replayed {
            return match persistence.read_authoritative_graph_snapshot(&fname).await {
                Ok(Some((snapshot, version))) => {
                    match ctx.core.install_committed_snapshot(snapshot, version) {
                        Ok(()) => Response::ok(ctx.req_id, predicted),
                        Err(error) => Response::err(ctx.req_id, error),
                    }
                }
                Ok(None) => Response::err(ctx.req_id, "committed graph image is missing"),
                Err(error) => Response::err(ctx.req_id, error),
            };
        }
        let (response, indexes_maintained) = match try_coalesce_apply(ctx, method).await {
            Some(response) => (response, true),
            None => (
                match apply(ctx.core) {
                    Ok(payload) => Response::ok(ctx.req_id, payload),
                    Err(error) => Response::err(ctx.req_id, error),
                },
                preserves_node_derived_indexes(method),
            ),
        };
        return commit_finalize(ctx, plan, method, response, prep, true, indexes_maintained).await;
    }

    // Runtime-result operations execute against an isolated authoritative image,
    // never the live projection. Its authenticated affected rows, terminal result,
    // batch, version/fence, and outbox share one redb commit point.
    let (base_snapshot, source_version) =
        match persistence.read_authoritative_graph_snapshot(&fname).await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => match crate::server::mutation_batch::authoritative_graph_version(
                persistence,
                &fname,
                ctx.core,
            )
            .await
            {
                Ok(version) => (ctx.core.snapshot(), version),
                Err(error) => {
                    return Response::err(
                        ctx.req_id,
                        format!("authoritative version read failed: {error}"),
                    )
                }
            },
            Err(error) => {
                return Response::err(
                    ctx.req_id,
                    format!("authoritative graph staging read failed: {error}"),
                )
            }
        };
    let base_snapshot_for_delta = base_snapshot.clone();
    let staged = match GraphCore::from_snapshot(base_snapshot, source_version) {
        Ok(staged) => staged,
        Err(error) => return Response::err(ctx.req_id, format!("graph staging failed: {error}")),
    };
    let payload = match apply(&staged) {
        Ok(payload) => payload,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let staged_snapshot = staged.snapshot();
    let row_delta = match crate::graph_delta::GraphRowDelta::between(
        &base_snapshot_for_delta,
        &staged_snapshot,
    ) {
        Ok(delta) => delta,
        Err(error) => {
            return Response::err(ctx.req_id, format!("staged graph delta failed: {error}"))
        }
    };
    let state_msgpack = match row_delta.to_msgpack() {
        Ok(bytes) => bytes,
        Err(error) => {
            return Response::err(
                ctx.req_id,
                format!("staged graph delta serialization failed: {error}"),
            )
        }
    };
    let max_bytes = mutation_snapshot_max_bytes();
    if max_bytes > 0 && state_msgpack.len() > max_bytes {
        return Response::err(
            ctx.req_id,
            format!(
                "staged mutation delta is {} bytes, above the configured {} byte limit",
                state_msgpack.len(),
                max_bytes
            ),
        );
    }
    use sha2::{Digest, Sha256};
    let target_graph_version = match source_version.checked_add(1) {
        Some(version) => version,
        None => return Response::err(ctx.req_id, "authoritative graph version overflow"),
    };
    let descriptor = crate::mutation_batch::MutationStateDescriptor {
        algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
        digest: hex::encode(Sha256::digest(&state_msgpack)),
        source_graph_version: source_version,
        target_graph_version,
    };
    let batch = match crate::server::mutation_batch::compile_methods(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: ctx.req_id,
            principal: ctx.caller,
            tenant: ctx.tenant_scope,
            graph: ctx.graph_name,
            placement_epoch: 0,
            idempotency_key: &batch_id,
            expected_graph_version: Some(source_version),
            fencing_token: None,
            created_at_ms,
            default_surface: crate::mutation_batch::MutationSurface::Graph,
            authoritative_state: Some(descriptor),
        },
        vec![durable_receipt_method(method)],
    ) {
        Ok(batch) => batch,
        Err(error) => {
            return Response::err(ctx.req_id, format!("MutationBatch compile failed: {error}"))
        }
    };
    let result = match rmp_serde::to_vec_named(&payload) {
        Ok(result) => result,
        Err(error) => {
            return Response::err(
                ctx.req_id,
                format!("MutationBatch result encode failed: {error}"),
            )
        }
    };
    let committed = match persistence
        .commit_mutation_batch_state(
            &fname,
            &batch,
            state_msgpack,
            Some(&result),
            created_at_ms,
            // The ORIGINAL method's policy-audited answer, captured before
            // `compile_methods` erased its identity into an opaque state receipt
            // (see `redb_store::commit_mutation_batch_inner`'s doc comment).
            // `TouchNodes` is the standing durable-but-unaudited example.
            plan.audited,
        )
        .await
    {
        Ok(committed) => committed,
        Err(error) => {
            return Response::err(
                ctx.req_id,
                format!("staged MutationBatch durable commit failed: {error}"),
            )
        }
    };
    if committed.replayed {
        return match persistence.read_authoritative_graph_snapshot(&fname).await {
            Ok(Some((snapshot, version))) => {
                match ctx.core.install_committed_snapshot(snapshot, version) {
                    Ok(()) => Response::ok(ctx.req_id, payload),
                    Err(error) => Response::err(ctx.req_id, error),
                }
            }
            Ok(None) => Response::err(ctx.req_id, "committed graph image is missing"),
            Err(error) => Response::err(ctx.req_id, error),
        };
    } else if let Err(error) =
        publish_committed_row_delta(persistence, &fname, ctx.core, &row_delta, source_version).await
    {
        return Response::err(
            ctx.req_id,
            format!("durable mutation projection publish failed: {error}"),
        );
    }
    commit_finalize(
        ctx,
        plan,
        method,
        Response::ok(ctx.req_id, payload),
        prep,
        true,
        row_delta.preserves_node_derived_indexes(),
    )
    .await
}

fn preserves_node_derived_indexes(method: &Method) -> bool {
    matches!(
        method,
        Method::AddEdge { .. }
            | Method::RemoveEdge { .. }
            | Method::AddEmbedding { .. }
            | Method::InvalidateEdge { .. }
    )
}

pub(crate) async fn publish_committed_row_delta(
    persistence: &Arc<dyn PersistenceBackend>,
    graph_fname: &str,
    core: &Arc<GraphCore>,
    delta: &crate::graph_delta::GraphRowDelta,
    source_version: u64,
) -> Result<(), String> {
    if core.version() == source_version {
        if delta.apply_to(core).is_ok() {
            return Ok(());
        }
    }
    // A bypass writer or an unexpected projection error cannot undo the already
    // committed authority. Repair from redb only on that exceptional path; the
    // normal path installs `D` affected rows and performs no full-image read.
    let (snapshot, version) = persistence
        .read_authoritative_graph_snapshot(graph_fname)
        .await?
        .ok_or_else(|| "committed graph projection is missing".to_string())?;
    let expected_target = source_version
        .checked_add(1)
        .ok_or_else(|| "committed graph version overflow".to_string())?;
    if version != expected_target {
        return Err("committed graph projection has an unexpected version".to_string());
    }
    // The caller still owns the normal finalize/mark-dirty step. Install at the
    // source version so that step advances exactly once and emits one change.
    core.prepare_snapshot_publish(snapshot, source_version)
}

fn mutation_snapshot_max_bytes() -> usize {
    static MAX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *MAX.get_or_init(|| {
        if let Ok(value) = std::env::var("EPISTEMIC_GRAPH_MUTATION_SNAPSHOT_MAX_BYTES") {
            if let Ok(parsed) = value.trim().parse::<usize>() {
                return parsed;
            }
        }
        let ram = crate::autosize::detect_capacity().total_ram_bytes;
        if ram == 0 {
            128 * 1024 * 1024
        } else {
            (ram / 16).clamp(16 * 1024 * 1024, 2 * 1024 * 1024 * 1024) as usize
        }
    })
}

/// Return the exact success payload for operations whose row effect can be
/// validated without publishing it.  This is the safe live rollout set for
/// durable-before-RAM; no optimistic guess is made for complex/runtime-derived
/// mutations.
fn prepublish_success(core: &GraphCore, method: &Method) -> Option<ResultPayload> {
    match method {
        Method::AddNode { .. }
        | Method::RemoveNode { .. }
        | Method::RemoveEdge { .. }
        | Method::ClearGraph
        | Method::AddEmbedding { .. } => Some(ResultPayload::String("ok".to_string())),
        Method::AddEdge {
            source_id,
            target_id,
            ..
        } if core.has_node(source_id) && core.has_node(target_id) => {
            Some(ResultPayload::String("ok".to_string()))
        }
        Method::BatchUpdate { operations_msgpack } => {
            let result = crate::algorithms::batch_update_preview(core, operations_msgpack).ok()?;
            eg_types::msgpack::decode_property_value(&result)
                .ok()
                .map(ResultPayload::Json)
        }
        _ => None,
    }
}

/// The gateway entry point for a RUNTIME-CONDITIONAL method (CONCEPT:EG-P0-2, L11
/// rollout continued) -- one whose `eg_capabilities::policy()` `mutates: true` is
/// a conservative UPPER BOUND (see the `RUNTIME_CONDITIONAL` divergence table in
/// `eg-capabilities/tests/consistency.rs`) because the REAL answer depends on a
/// per-invocation field (a `writeback: bool`, or a parsed query) that a static
/// per-variant table cannot see. `mutates_now` is that resolved, per-call truth,
/// decided by the CALLER (the gateway match arm) from the request's own field --
/// never re-derived here, and never a second classifier: this function still
/// drives everything through the SAME `plan` (sourced from `policy()`) on the
/// `mutates_now == true` branch.
///
/// - `mutates_now == true`: identical to [`commit_mutation`] -- full authz
///   (Write) + durability + audit + CDC + idempotency-replay, driven by `plan`.
/// - `mutates_now == false`: THIS INVOCATION is a plain read (the upper bound
///   didn't materialize), so it is treated as one: a Read-only ACL check (never
///   Write), `apply` runs, and NOTHING durable/audited/CDC-emitted/cached happens
///   -- exactly what a method never in [`GATEWAY_ROUTED`] does for an ordinary
///   read. This is what keeps a `writeback: false` call from being incorrectly
///   gated behind Write access or persisted as a phantom mutation (the bug this
///   whole function exists to prevent -- see the module docs' RUNTIME_CONDITIONAL
///   discussion).
pub async fn commit_conditional_mutation<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    mutates_now: bool,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    if mutates_now {
        return commit_mutation(ctx, plan, method, apply).await;
    }
    // Read-only path: same ACL surface every other read-only graph method goes
    // through (`access::check_graph_access`), just Read instead of Write.
    if !consensus_apply_is_authorized() {
        if let Err(denied) = check_graph_access(
            ctx.isolation,
            ctx.caller,
            ctx.graph_name,
            ctx.graph_type,
            ctx.owner,
            AccessLevel::Read,
        ) {
            return Response::err(ctx.req_id, denied);
        }
    }
    match apply(ctx.core) {
        Ok(payload) => Response::ok(ctx.req_id, payload),
        Err(e) => Response::err(ctx.req_id, e),
    }
}

/// The ASYNC-apply twin of [`commit_conditional_mutation`] (CONCEPT:EG-P0-2, L11):
/// for a routed method whose EXECUTION is itself `async` and needs more than a bare
/// `&GraphCore` — the query surface (`Sql`/`CypherQuery`/`GraphQl`, run on the
/// blocking pool via `handlers::query::try_handle`, needing `state`/`caller`/`rls`)
/// and the native RDF surface (`AddTriples`/`RemoveTriples`/`DropNamedGraph`, run
/// via `handlers::rdf::try_handle`; lossless multi-valued literals live inside the
/// staged graph image). The `apply` closure captures whatever
/// state it needs and returns the payload; the gateway wraps it with the IDENTICAL
/// authz / durability / audit / CDC / idempotency contract as the sync path (via the
/// shared [`commit_prepare`] / [`commit_finalize`]).
///
/// `mutates_now` is the resolved, per-call truth (never `policy()`'s conservative
/// upper bound): the RDF ops pass `true` (they always mutate), while the query ops
/// pass the SAME runtime parse `access::requires_write` already uses
/// (`sql_is_write` / `cypher_is_write` / `graphql_is_mutation`) — so a `SELECT` /
/// read-only Cypher / GraphQL `query` is a Read-authz passthrough with NO
/// durability/audit/CDC, while a SQL write / Cypher `CREATE|SET|DELETE` / GraphQL
/// `mutation` goes through the full Write-authz commit.
pub async fn commit_conditional_mutation_async<F, Fut>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    mutates_now: bool,
    apply: F,
) -> Response
where
    F: FnOnce(Arc<GraphCore>) -> Fut,
    Fut: std::future::Future<Output = Result<ResultPayload, String>>,
{
    let response =
        commit_conditional_mutation_async_inner(ctx, plan, method, mutates_now, apply).await;
    if mutates_now {
        advance_authoritative_manifest(ctx, plan, &response);
    }
    response
}

async fn commit_conditional_mutation_async_inner<F, Fut>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    mutates_now: bool,
    apply: F,
) -> Response
where
    F: FnOnce(Arc<GraphCore>) -> Fut,
    Fut: std::future::Future<Output = Result<ResultPayload, String>>,
{
    if !mutates_now {
        // Read-only path: Read-ACL only, apply, and NOTHING durable/audited/
        // CDC-emitted/cached — exactly what any non-mutating graph read does.
        if !consensus_apply_is_authorized() {
            if let Err(denied) = check_graph_access(
                ctx.isolation,
                ctx.caller,
                ctx.graph_name,
                ctx.graph_type,
                ctx.owner,
                AccessLevel::Read,
            ) {
                return Response::err(ctx.req_id, denied);
            }
        }
        return match apply(Arc::clone(ctx.core)).await {
            Ok(payload) => Response::ok(ctx.req_id, payload),
            Err(e) => Response::err(ctx.req_id, e),
        };
    }

    // Mutating path: stage against a complete authoritative image. Query/RDF
    // handlers never receive the live serving core, so an execution or durable
    // commit failure cannot leak a partial mutation into RAM.
    let _mutation_guard = crate::server::mutation_batch::lock_graph(ctx.graph_name).await;
    let prep = match commit_prepare(ctx, plan, method) {
        Ok(p) => p,
        Err(short_circuit) => return short_circuit,
    };
    if matches!(plan.durability_domain, DurabilityDomain::None) {
        let response = match apply(Arc::clone(ctx.core)).await {
            Ok(payload) => Response::ok(ctx.req_id, payload),
            Err(error) => Response::err(ctx.req_id, error),
        };
        return commit_finalize(ctx, plan, method, response, prep, true, false).await;
    }
    let Some(persistence) = ctx.persistence else {
        return Response::err(
            ctx.req_id,
            "authoritative MutationBatch commit requires a persistence backend",
        );
    };

    let fname = crate::persist::sanitize(ctx.graph_name);
    let batch_id = crate::server::mutation_batch::opaque_request_key(
        "rpc-async",
        ctx.graph_name,
        ctx.req_id,
        method,
    );
    match persistence.read_mutation_batch(&fname, &batch_id).await {
        Ok(Some(record)) => {
            if let Ok(Some((snapshot, version))) =
                persistence.read_authoritative_graph_snapshot(&fname).await
            {
                if let Err(error) = ctx.core.install_committed_snapshot(snapshot, version) {
                    return Response::err(
                        ctx.req_id,
                        format!("committed projection reconciliation failed: {error}"),
                    );
                }
            } else {
                return Response::err(
                    ctx.req_id,
                    "committed projection reconciliation found no authoritative graph",
                );
            }
            let response = record
                .result_msgpack
                .as_deref()
                .ok_or_else(|| "committed MutationBatch has no durable result".to_string())
                .and_then(|bytes| {
                    eg_types::msgpack::decode_bounded(
                        bytes,
                        eg_types::msgpack::MsgpackLimits::new(64 * 1024 * 1024, 1_000_000, 64),
                    )
                    .map_err(|_| "committed MutationBatch result is corrupt".to_string())
                })
                .map(|payload| Response::ok(ctx.req_id, payload))
                .unwrap_or_else(|error| Response::err(ctx.req_id, error));
            if let Some(key) = prep.dedup_key {
                idempotency_store().insert(key, response.clone());
            }
            return response;
        }
        Ok(None) => {}
        Err(error) => {
            return Response::err(
                ctx.req_id,
                format!("MutationBatch status lookup failed: {error}"),
            )
        }
    }

    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let (base_snapshot, source_version) =
        match persistence.read_authoritative_graph_snapshot(&fname).await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => match crate::server::mutation_batch::authoritative_graph_version(
                persistence,
                &fname,
                ctx.core,
            )
            .await
            {
                Ok(version) => (ctx.core.snapshot(), version),
                Err(error) => {
                    return Response::err(
                        ctx.req_id,
                        format!("authoritative version read failed: {error}"),
                    )
                }
            },
            Err(error) => {
                return Response::err(
                    ctx.req_id,
                    format!("authoritative graph staging read failed: {error}"),
                )
            }
        };
    let base_snapshot_for_delta = base_snapshot.clone();
    let staged = match GraphCore::from_snapshot(base_snapshot, source_version) {
        Ok(staged) => Arc::new(staged),
        Err(error) => return Response::err(ctx.req_id, format!("graph staging failed: {error}")),
    };
    let payload = match apply(Arc::clone(&staged)).await {
        Ok(payload) => payload,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let staged_snapshot = staged.snapshot();
    let row_delta = match crate::graph_delta::GraphRowDelta::between(
        &base_snapshot_for_delta,
        &staged_snapshot,
    ) {
        Ok(delta) => delta,
        Err(error) => {
            return Response::err(ctx.req_id, format!("staged graph delta failed: {error}"))
        }
    };
    let state_msgpack = match row_delta.to_msgpack() {
        Ok(bytes) => bytes,
        Err(error) => {
            return Response::err(
                ctx.req_id,
                format!("staged graph delta serialization failed: {error}"),
            )
        }
    };
    let max_bytes = mutation_snapshot_max_bytes();
    if max_bytes > 0 && state_msgpack.len() > max_bytes {
        return Response::err(
            ctx.req_id,
            format!(
                "staged mutation delta is {} bytes, above the configured {} byte limit",
                state_msgpack.len(),
                max_bytes
            ),
        );
    }
    use sha2::{Digest, Sha256};
    let target_graph_version = match source_version.checked_add(1) {
        Some(version) => version,
        None => return Response::err(ctx.req_id, "authoritative graph version overflow"),
    };
    let descriptor = crate::mutation_batch::MutationStateDescriptor {
        algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
        digest: hex::encode(Sha256::digest(&state_msgpack)),
        source_graph_version: source_version,
        target_graph_version,
    };
    let default_surface = if is_rdf_gateway_method(method) {
        crate::mutation_batch::MutationSurface::Rdf
    } else {
        crate::mutation_batch::MutationSurface::Query
    };
    let batch = match crate::server::mutation_batch::compile_methods(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: ctx.req_id,
            principal: ctx.caller,
            tenant: ctx.tenant_scope,
            graph: ctx.graph_name,
            placement_epoch: 0,
            idempotency_key: &batch_id,
            expected_graph_version: Some(source_version),
            fencing_token: None,
            created_at_ms,
            default_surface,
            authoritative_state: Some(descriptor),
        },
        vec![method.clone()],
    ) {
        Ok(batch) => batch,
        Err(error) => {
            return Response::err(ctx.req_id, format!("MutationBatch compile failed: {error}"))
        }
    };
    let result = match rmp_serde::to_vec_named(&payload) {
        Ok(result) => result,
        Err(error) => {
            return Response::err(
                ctx.req_id,
                format!("MutationBatch result encode failed: {error}"),
            )
        }
    };
    let committed = match persistence
        .commit_mutation_batch_state(
            &fname,
            &batch,
            state_msgpack,
            Some(&result),
            created_at_ms,
            // The ORIGINAL method's policy-audited answer, captured before
            // `compile_methods` erased its identity into an opaque state receipt
            // (see `redb_store::commit_mutation_batch_inner`'s doc comment).
            // `TouchNodes` is the standing durable-but-unaudited example.
            plan.audited,
        )
        .await
    {
        Ok(committed) => committed,
        Err(error) => {
            return Response::err(
                ctx.req_id,
                format!("staged MutationBatch durable commit failed: {error}"),
            )
        }
    };
    if committed.replayed {
        return match persistence.read_authoritative_graph_snapshot(&fname).await {
            Ok(Some((snapshot, version))) => {
                match ctx.core.install_committed_snapshot(snapshot, version) {
                    Ok(()) => Response::ok(ctx.req_id, payload),
                    Err(error) => Response::err(ctx.req_id, error),
                }
            }
            Ok(None) => Response::err(ctx.req_id, "committed graph image is missing"),
            Err(error) => Response::err(ctx.req_id, error),
        };
    }
    if let Err(error) =
        publish_committed_row_delta(persistence, &fname, ctx.core, &row_delta, source_version).await
    {
        return Response::err(
            ctx.req_id,
            format!("durable mutation projection publish failed: {error}"),
        );
    }
    commit_finalize(
        ctx,
        plan,
        method,
        Response::ok(ctx.req_id, payload),
        prep,
        true,
        row_delta.preserves_node_derived_indexes(),
    )
    .await
}

/// Is `m` one of the query-surface gateway methods routed via
/// [`commit_conditional_mutation_async`] at the query dispatch site (they need
/// `state`/`rls` the graph-ops `try_handle_gateway` entry point does not carry)?
/// Part of [`GATEWAY_ROUTED`], but handed back by `try_handle_gateway` so it reaches
/// its dedicated wrap in `dispatch.rs`.
pub fn is_query_gateway_method(m: &Method) -> bool {
    matches!(method_variant_name(m), "Sql" | "CypherQuery" | "GraphQl")
}

/// SQL table/catalog writes and GraphQL cross-modal operations own native domain
/// coordinators. Their commit points now include universal MutationBatch status,
/// fences, idempotency and outbox, but they must not also be wrapped in a second
/// graph-snapshot batch. Ordinary graph SQL/GraphQL mutations still return `false`
/// and use the universal staged gateway.
pub fn is_query_native_coordinator(m: &Method) -> bool {
    #[cfg(feature = "query")]
    if let Method::Sql { query, .. } = m {
        use eg_query::StatementKind;
        return match eg_query::classify(query) {
            Ok(
                StatementKind::InsertNodes(_)
                | StatementKind::InsertNodesSelect(_)
                | StatementKind::UpdateNodes(_)
                | StatementKind::UpdateNodesJoin(_)
                | StatementKind::DeleteNodes(_)
                | StatementKind::DeleteNodesJoin(_),
            ) => false,
            // User-table/catalog statements atomically commit their rows and
            // universal batch metadata inside TableStore. Running them against a
            // graph staging closure would introduce a second, unrelated authority.
            Ok(_) => true,
            Err(_) => false,
        };
    }
    #[cfg(feature = "graphql")]
    if let Method::GraphQl { query, .. } = m {
        return !matches!(
            eg_graphql::classify_crossmodal(query),
            eg_graphql::CrossModalRoute::NotCrossModal
        );
    }
    let _ = m;
    false
}

/// Is `m` one of the native-RDF gateway methods routed via
/// [`commit_conditional_mutation_async`] at the rdf dispatch site (their durable
/// write carries the lossless RDF dataset in the staged graph image)? Part of
/// [`GATEWAY_ROUTED`]; handed back by `try_handle_gateway` for the same reason as
/// [`is_query_gateway_method`].
pub fn is_rdf_gateway_method(m: &Method) -> bool {
    matches!(
        method_variant_name(m),
        "AddTriples" | "RemoveTriples" | "DropNamedGraph"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "redb")]
    use crate::durability::DurabilityPolicy;
    use crate::protocol::GraphType;
    #[cfg(feature = "redb")]
    use crate::server::persistence::redb_backend::RedbBackend;

    /// W1c: `check_graph_access_with_policy` (`src/server/access.rs`, the L-RLS-1
    /// hardening) now mandates a provisioned identity for EVERY graph-scoped
    /// access -- an empty, rule-less `IsolationLayer` (no registered agents) is
    /// unconditionally denied, not just under-ruled. Every test below needs a
    /// caller that actually clears that gate, so they provision "system-agent"
    /// with `AgentRole::System` (the one role `IsolationLayer::check_access`
    /// treats as unconditionally allowed, same mechanism
    /// `unauthorized_actor_is_rejected_at_the_gateway` already relies on for its
    /// registered identities) instead of an insufficient no-rules layer.
    fn isolation_with_system_agent() -> IsolationLayer {
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(crate::acl::AgentIdentity {
            agent_id: "system-agent".to_string(),
            role: crate::acl::AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        isolation
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "eg-mutation-gateway-test-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn validated_public_batch_has_a_deterministic_prepublish_result() {
        let core = GraphCore::new();
        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "a", "properties": {"text": "alpha"}},
            {"op": "add_embedding", "id": "a", "embedding": [1.0, 0.0]}
        ]))
        .unwrap();
        let method = Method::BatchUpdate {
            operations_msgpack: operations,
        };
        let Some(ResultPayload::Json(result)) = prepublish_success(&core, &method) else {
            panic!("validated BatchUpdate must use compact authoritative rows");
        };
        assert_eq!(result["added_nodes"], 1);
        assert_eq!(result["added_embeddings"], 1);

        let malformed = Method::BatchUpdate {
            operations_msgpack: vec![0xc1],
        };
        assert!(prepublish_success(&core, &malformed).is_none());
    }

    /// (a) A GATEWAY_ROUTED, audited+CDC-emitting mutation (`AddNode`) produces a
    /// durable record + an audit-chain entry + a CDC event -- all from the one
    /// `commit_mutation` call, against a REAL `RedbBackend` (not a mock), so this
    /// is exercising the actual durable-commit + audit-chain-append path, not just
    /// asserting the gateway's own bookkeeping.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn routed_mutation_produces_one_durable_record_one_audit_entry_and_one_cdc_event() {
        let dir = temp_dir("durable-audit-cdc");
        let dir_s = dir.to_string_lossy().to_string();
        let backend =
            RedbBackend::open(dir_s, DurabilityPolicy::Each, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        let isolation = isolation_with_system_agent();
        let cdc_hub = Arc::new(crate::server::cdc::CdcHub::new());
        let graph_name = "g-eg-p0-2-a";

        let method = Method::AddNode {
            node_id: "n1".to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"v": 1})).unwrap(),
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.mutates, "AddNode must be classified as a mutation");
        assert!(plan.audited, "AddNode is policy-audited");
        assert!(plan.emits_cdc, "AddNode policy-emits CDC");
        assert_eq!(plan.durability_domain, DurabilityDomain::GraphRedb);

        let ctx = MutationCtx {
            req_id: 1,
            caller: Some("system-agent"),
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            cdc: Some(&cdc_hub),
            materialization_manifest: None,
            write_coalescer: None,
        };

        let (node_id, props) = match &method {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } => (node_id.clone(), properties_msgpack.clone()),
            _ => unreachable!(),
        };
        let resp = commit_mutation(&ctx, &plan, &method, move |core| {
            core.add_node(node_id, props);
            Ok(ResultPayload::String("ok".to_string()))
        })
        .await;
        assert!(
            resp.error.is_none(),
            "commit_mutation failed: {:?}",
            resp.error
        );

        // The write actually landed durably.
        let fname = crate::persist::sanitize(graph_name);
        let read_back = persistence
            .as_redb()
            .expect("configured backend is redb")
            .read_node(&fname, "n1")
            .await
            .expect("read back the durably-committed node");
        assert!(
            read_back.is_some(),
            "AddNode did not durably commit via the gateway"
        );

        // Audit: the tamper-evident chain grew by exactly one entry for this graph.
        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok, "audit chain broke: {report:?}");
        assert_eq!(
            report.entries, 1,
            "exactly one audited mutation (AddNode) went through the gateway"
        );

        // CDC: one AddNode event was emitted into the hub's feed for this graph.
        let events = cdc_hub.read(graph_name, 0, 100).expect("cdc read");
        assert_eq!(
            events.len(),
            1,
            "expected exactly one CDC event, got {events:?}"
        );
    }

    /// (a2) A GATEWAY_ROUTED durable + audited BUT NON-CDC mutation (`Reinforce`)
    /// commits durably, appends exactly ONE audit-chain entry, and leaves the CDC
    /// feed UNTOUCHED. As of L3/EG-P0-6, `audit_line` is exhaustive over the full
    /// durable-mutation surface, so `Reinforce` is now policy-`audited: true` (its
    /// agent-memory write chains into the tamper-evident log); it stays
    /// `emits_cdc: false`. This test now proves CDC -- NOT audit -- is the
    /// policy-gated leg of the gateway: audit fires, CDC does not.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn audited_mutation_writes_audit_but_cdc_stays_policy_gated() {
        let dir = temp_dir("reinforce-audit-no-cdc");
        let dir_s = dir.to_string_lossy().to_string();
        let backend =
            RedbBackend::open(dir_s, DurabilityPolicy::Each, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        core.add_node("n1".to_string(), Vec::new());
        let isolation = isolation_with_system_agent();
        let cdc_hub = Arc::new(crate::server::cdc::CdcHub::new());
        let graph_name = "g-eg-p0-2-a2";

        let method = Method::Reinforce {
            node_id: "n1".to_string(),
            now_ms: 1_000,
            weight: 1.0,
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.mutates);
        assert!(plan.audited, "Reinforce is now policy-audited (L3/EG-P0-6)");
        assert!(!plan.emits_cdc, "Reinforce policy-does-NOT-emit-CDC");
        assert_eq!(plan.durability_domain, DurabilityDomain::GraphRedb);

        let ctx = MutationCtx {
            req_id: 2,
            caller: Some("system-agent"),
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            cdc: Some(&cdc_hub),
            materialization_manifest: None,
            write_coalescer: None,
        };
        let resp = commit_mutation(&ctx, &plan, &method, |core| {
            core.reinforce("n1", 1_000, 1.0);
            Ok(ResultPayload::Bool(true))
        })
        .await;
        assert!(resp.error.is_none());

        let fname = crate::persist::sanitize(graph_name);
        let report = persistence
            .as_redb()
            .unwrap()
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok, "audit chain broke: {report:?}");
        assert_eq!(
            report.entries, 1,
            "Reinforce is now audited -- exactly one audit-chain entry"
        );
        assert_eq!(
            cdc_hub.read(graph_name, 0, 100).expect("cdc read").len(),
            0,
            "Reinforce must NOT emit a CDC event (CDC stays the policy-gated leg)"
        );
    }

    /// (a2b) W1c: drive one `method` through the REAL `commit_mutation` gateway
    /// against a fresh `RedbBackend`, asserting the policy is `audited: true` +
    /// `emits_cdc: true` and that exactly one audit-chain entry and one CDC marker
    /// event actually land. Shared by every W1c admin/ledger-method case below (each
    /// gets its own temp dir/backend/graph, so `report.entries`/the CDC feed start
    /// clean per call).
    #[cfg(feature = "redb")]
    async fn assert_w1c_method_audits_and_emits_one_cdc_marker<F>(
        tag: &str,
        method: Method,
        apply: F,
    ) where
        F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
    {
        let dir = temp_dir(&format!("w1c-{tag}"));
        let dir_s = dir.to_string_lossy().to_string();
        let backend =
            RedbBackend::open(dir_s, DurabilityPolicy::Each, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);
        let core = Arc::new(GraphCore::new());
        let isolation = isolation_with_system_agent();
        let cdc_hub = Arc::new(crate::server::cdc::CdcHub::new());
        let graph_name = format!("g-w1c-{tag}");

        let plan = MutationPlan::for_method(&method);
        assert!(plan.mutates, "{tag}: must be classified as a mutation");
        assert!(plan.audited, "{tag}: W1c must now be policy-audited");
        assert!(plan.emits_cdc, "{tag}: W1c must now policy-emit CDC");
        assert_eq!(plan.durability_domain, DurabilityDomain::GraphRedb);

        let ctx = MutationCtx {
            req_id: 1,
            caller: Some("system-agent"),
            tenant_scope: "opaque-test-tenant",
            graph_name: &graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            cdc: Some(&cdc_hub),
            materialization_manifest: None,
            write_coalescer: None,
        };
        let resp = commit_mutation(&ctx, &plan, &method, apply).await;
        assert!(
            resp.error.is_none(),
            "{tag}: commit_mutation failed: {:?}",
            resp.error
        );

        let fname = crate::persist::sanitize(&graph_name);
        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok, "{tag}: audit chain broke: {report:?}");
        assert_eq!(
            report.entries, 1,
            "{tag}: exactly one audit-chain entry must land"
        );

        let events = cdc_hub.read(&graph_name, 0, 100).expect("cdc read");
        assert_eq!(
            events.len(),
            1,
            "{tag}: exactly one CDC marker event, got {events:?}"
        );
    }

    /// (a2c) W1c: the 9 durable admin/ledger methods (`FromMsgpack`/`ClearLedger`/
    /// `ApplyLedger`/`CompactNodesByType`/`RunDatalogReasoning`/`Reconcile`/
    /// `ApplyMutation`/`ApplyMultisigMutation`/`IcvConfigure`) were GATEWAY_ROUTED
    /// and GraphRedb-durable but `audited: false, emits_cdc: false` -- invisible to
    /// both the tamper-evident audit chain and the CDC feed despite committing
    /// durably. Each now audits + emits CDC, consistent with the rest of the
    /// durable mutation surface (`audit::audit_line` / `cdc::emit_for_method`).
    /// 7 of the 9 don't map onto a single node/edge row, so each emits ONE
    /// reserved-marker `UpdateNode` CDC event (the same shape `ApplyChangeEnvelope`/
    /// `ServedModality` already use); those 7 are exercised here.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn w1c_marker_admin_ledger_methods_now_audit_and_emit_cdc() {
        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "clear-ledger",
            Method::ClearLedger,
            |core| {
                core.clear_ledger();
                Ok(ResultPayload::String("ok".to_string()))
            },
        )
        .await;

        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "apply-ledger",
            Method::ApplyLedger {
                transactions: Vec::new(),
            },
            |core| {
                core.apply_ledger(Vec::new())
                    .map(|()| ResultPayload::String("ok".to_string()))
            },
        )
        .await;

        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "compact-nodes-by-type",
            Method::CompactNodesByType {
                node_type: "x".into(),
                threshold: 1,
            },
            |core| {
                let removed = core.compact_nodes_by_type("x", 1);
                Ok(ResultPayload::Json(
                    serde_json::json!({ "removed_nodes": removed }),
                ))
            },
        )
        .await;

        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "apply-mutation",
            Method::ApplyMutation {
                event_type: "w1c_test_event".into(),
                query: "SELECT 1".into(),
            },
            |_core| Ok(ResultPayload::String("ok".to_string())),
        )
        .await;

        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "apply-multisig-mutation",
            Method::ApplyMultisigMutation {
                signatures: vec!["sig1".into(), "sig2".into()],
                threshold: 2,
                mutation_type: "policy_update".into(),
                query: "UPDATE policy SET x = 1".into(),
            },
            |_core| Ok(ResultPayload::Bool(true)),
        )
        .await;

        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "icv-configure",
            Method::IcvConfigure {
                graph: None,
                mode: "enforce".into(),
                shapes: "@prefix sh: <http://www.w3.org/ns/shacl#> .".into(),
            },
            |_core| Ok(ResultPayload::Bool(true)),
        )
        .await;

        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "run-datalog-reasoning",
            Method::RunDatalogReasoning {
                subclass_relations: Vec::new(),
                subproperty_relations: Vec::new(),
                symmetric_properties: Vec::new(),
                transitive_properties: Vec::new(),
                inverse_properties: Vec::new(),
                domain_rules: Vec::new(),
                range_rules: Vec::new(),
                property_chains: Vec::new(),
            },
            |_core| {
                Ok(ResultPayload::Json(serde_json::json!({
                    "inferred_count": 0,
                    "inferred_triples": Vec::<()>::new(),
                })))
            },
        )
        .await;
    }

    /// (a2d) W1c: `FromMsgpack`/`Reconcile` both replace the graph's ENTIRE
    /// node/edge content with an imported/merged authoritative image
    /// (`GraphCore::from_msgpack`) -- the same "whole graph replaced" shape as
    /// `ClearGraph`, so their CDC signal is a feed RESET (`CdcHub::reset_graph`),
    /// not a marker event. Proven by seeding one real CDC event first (a durable
    /// `AddNode`) and observing the feed empty out afterward, while the audit
    /// chain still grows by one entry for the `FromMsgpack`/`Reconcile` call
    /// itself.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn w1c_from_msgpack_and_reconcile_reset_the_cdc_feed_and_audit() {
        for (tag, build_method) in [
            (
                "from-msgpack",
                (|msgpack: Vec<u8>| Method::FromMsgpack { msgpack })
                    as fn(Vec<u8>) -> Method,
            ),
            (
                "reconcile",
                (|msgpack: Vec<u8>| Method::Reconcile {
                    graph_name: "other-graph".to_string(),
                    msgpack,
                }) as fn(Vec<u8>) -> Method,
            ),
        ] {
            let dir = temp_dir(&format!("w1c-{tag}"));
            let dir_s = dir.to_string_lossy().to_string();
            let backend =
                RedbBackend::open(dir_s, DurabilityPolicy::Each, 64).expect("open redb backend");
            let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);
            let core = Arc::new(GraphCore::new());
            let isolation = isolation_with_system_agent();
            let cdc_hub = Arc::new(crate::server::cdc::CdcHub::new());
            let graph_name = format!("g-w1c-{tag}");

            let ctx = MutationCtx {
                req_id: 1,
                caller: Some("system-agent"),
                tenant_scope: "opaque-test-tenant",
                graph_name: &graph_name,
                graph_type: GraphType::Commons,
                owner: None,
                isolation: &isolation,
                core: &core,
                persistence: Some(&persistence),
                cdc: Some(&cdc_hub),
                materialization_manifest: None,
                write_coalescer: None,
            };

            // Seed one durable AddNode -- one audit entry, one CDC event.
            let seed = Method::AddNode {
                node_id: "seed".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"v": 1}))
                    .unwrap(),
            };
            let seed_plan = MutationPlan::for_method(&seed);
            let seed_resp = commit_mutation(&ctx, &seed_plan, &seed, |core| {
                core.add_node("seed".to_string(), Vec::new());
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await;
            assert!(seed_resp.error.is_none(), "{tag}: seed AddNode failed");
            assert_eq!(
                cdc_hub.read(&graph_name, 0, 100).expect("cdc read").len(),
                1,
                "{tag}: seed AddNode must emit exactly one CDC event"
            );

            // FromMsgpack/Reconcile: replace with a (valid, empty) authoritative
            // snapshot of the SAME core -- policy-audited + policy-CDC-emitting.
            let msgpack = core.to_msgpack().expect("to_msgpack");
            let method = build_method(msgpack.clone());
            let plan = MutationPlan::for_method(&method);
            assert!(plan.audited, "{tag}: W1c must now be policy-audited");
            assert!(plan.emits_cdc, "{tag}: W1c must now policy-emit CDC");

            let resp = commit_mutation(&ctx, &plan, &method, move |core| {
                core.from_msgpack(&msgpack)
                    .map(|()| ResultPayload::String("ok".to_string()))
            })
            .await;
            assert!(
                resp.error.is_none(),
                "{tag}: commit_mutation failed: {:?}",
                resp.error
            );

            let fname = crate::persist::sanitize(&graph_name);
            let report = persistence
                .as_redb()
                .expect("configured backend is redb")
                .audit_verify_blocking(&fname)
                .expect("audit_verify_blocking");
            assert!(report.ok, "{tag}: audit chain broke: {report:?}");
            assert_eq!(
                report.entries, 2,
                "{tag}: seed AddNode + {tag} == exactly two audit-chain entries"
            );

            assert_eq!(
                cdc_hub.read(&graph_name, 0, 100).expect("cdc read").len(),
                0,
                "{tag}: the CDC feed must be RESET (empty) after the whole-graph replace"
            );
        }
    }

    /// (a5) L11 rollout batch 2, family representative: a message-broker method
    /// (`DeclareExchange`, `DurabilityDomain::Outbox`) routed through the SAME
    /// `commit_mutation` gateway produces the policy-declared durable/audit effect
    /// (audited, no CDC) against a REAL `RedbBackend` — proving the Outbox
    /// durability domain commits through the identical `record_durable` path as
    /// the GraphRedb-domain methods above (the backend does not branch on
    /// `DurabilityDomain`, only on whether it's `None`).
    #[cfg(all(feature = "redb", feature = "broker"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn broker_family_routed_mutation_is_audited_with_no_cdc() {
        let dir = temp_dir("broker-declare-exchange");
        let dir_s = dir.to_string_lossy().to_string();
        let backend =
            RedbBackend::open(dir_s, DurabilityPolicy::Each, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        let isolation = isolation_with_system_agent();
        let cdc_hub = Arc::new(crate::server::cdc::CdcHub::new());
        let graph_name = "g-eg-p0-2-l11-broker-a";

        let method = Method::DeclareExchange {
            exchange: "orders".to_string(),
            kind: "direct".to_string(),
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.mutates);
        assert_eq!(plan.durability_domain, DurabilityDomain::Outbox);
        assert!(plan.audited, "DeclareExchange is policy-audited");
        assert!(!plan.emits_cdc, "DeclareExchange policy-does-NOT-emit-CDC");

        let ctx = MutationCtx {
            req_id: 10,
            caller: Some("system-agent"),
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            cdc: Some(&cdc_hub),
            materialization_manifest: None,
            write_coalescer: None,
        };

        let resp = commit_mutation(&ctx, &plan, &method, move |core| {
            let Some(k) = crate::broker::ExchangeKind::parse("direct") else {
                return Err("bad kind".to_string());
            };
            crate::broker::declare_exchange(core, "orders", k)
                .map(|()| ResultPayload::String("ok".to_string()))
        })
        .await;
        assert!(
            resp.error.is_none(),
            "commit_mutation failed: {:?}",
            resp.error
        );

        let fname = crate::persist::sanitize(graph_name);
        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok, "audit chain broke: {report:?}");
        assert_eq!(
            report.entries, 1,
            "exactly one audited mutation (DeclareExchange) went through the gateway"
        );
        assert_eq!(
            cdc_hub.read(graph_name, 0, 100).expect("cdc read").len(),
            0,
            "DeclareExchange must NOT emit a CDC event (Outbox methods never do)"
        );
    }

    /// (a6) State-backed maintenance operations use the same authoritative
    /// MutationBatch boundary as ordinary graph writes. `TouchNodes` remains
    /// intentionally unaudited and CDC-silent, but its resulting graph image and
    /// terminal result must survive restart.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn touch_nodes_commits_authoritative_state_without_audit_or_cdc() {
        let dir = temp_dir("touch-nodes-state-backed");
        let dir_s = dir.to_string_lossy().to_string();
        let backend =
            RedbBackend::open(dir_s, DurabilityPolicy::Each, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        core.add_node(
            "n1".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({})).expect("encode seed node"),
        );
        let isolation = isolation_with_system_agent();
        let graph_name = "g-eg-p0-2-l11-none-durability-a";

        let method = Method::TouchNodes {
            node_ids: vec!["n1".to_string()],
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.mutates);
        assert_eq!(plan.durability_domain, DurabilityDomain::GraphRedb);
        assert!(!plan.audited);
        assert!(!plan.emits_cdc);

        let ctx = MutationCtx {
            req_id: 11,
            caller: Some("system-agent"),
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };

        let resp = commit_mutation(&ctx, &plan, &method, move |core| {
            let now = 1_000u64;
            let touched = core.touch_nodes(&["n1".to_string()], now);
            Ok(ResultPayload::Count(touched as u64))
        })
        .await;
        assert!(
            resp.error.is_none(),
            "commit_mutation failed: {:?}",
            resp.error
        );

        let fname = crate::persist::sanitize(graph_name);
        let batch_id =
            crate::server::mutation_batch::opaque_request_key("rpc", graph_name, 11, &method);
        let record = persistence
            .read_mutation_batch(&fname, &batch_id)
            .await
            .expect("read mutation batch")
            .expect("TouchNodes batch is durable");
        assert_eq!(record.batch.tenant, "opaque-test-tenant");
        let (snapshot, version) = persistence
            .read_authoritative_graph_snapshot(&fname)
            .await
            .expect("read authoritative graph")
            .expect("TouchNodes graph image is durable");
        assert!(version > 0);
        let node = snapshot
            .nodes
            .iter()
            .find(|(id, _)| id == "n1")
            .expect("durable node");
        let properties: serde_json::Value =
            eg_types::msgpack::decode_property_value(node.1.as_slice())
                .expect("decode durable node");
        assert_eq!(properties["last_access"], serde_json::json!(1_000));
        assert_eq!(properties["confidence"], serde_json::json!(1.0));

        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok, "audit chain broke: {report:?}");
        assert_eq!(
            report.entries, 0,
            "TouchNodes is state-backed but intentionally unaudited"
        );
    }

    /// (a7) L11 rollout batch 3, RUNTIME-CONDITIONAL family representative:
    /// `MineAssociate` proves `commit_conditional_mutation` resolves the
    /// durability/audit/authz gateway from the request's OWN `writeback` field,
    /// not from `policy()`'s conservative upper bound:
    ///   * `writeback: true`  -> Write access required, one audited durable entry.
    ///   * `writeback: false` -> Read access SUFFICES (an agent with only Read
    ///     succeeds), and NOTHING durable/audited is produced -- proving a
    ///     read-only call is never incorrectly gated behind Write or persisted as
    ///     a phantom mutation (the bug this function exists to prevent).
    #[cfg(all(feature = "redb", feature = "mining"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn mining_family_writeback_gates_durability_and_authz() {
        let dir = temp_dir("mine-associate-conditional");
        let dir_s = dir.to_string_lossy().to_string();
        let backend =
            RedbBackend::open(dir_s, DurabilityPolicy::Each, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        let graph_name = "g-eg-p0-2-l11-mining-a";

        // This test's job is the DURABILITY/AUDIT half of the runtime-conditional
        // gateway (below). The Read-vs-Write ACL distinction is already covered
        // structurally by `commit_conditional_mutation`'s own code path (the
        // `mutates_now == false` branch calls `check_graph_access(.., AccessLevel::
        // Read)`; `true` goes through `commit_mutation`, which uses `AccessLevel::
        // Write`) and by `unauthorized_actor_is_rejected_at_the_gateway` proving the
        // Write gate itself denies an unauthorized caller.
        let isolation = isolation_with_system_agent();

        let transactions = vec![
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "b".to_string()],
        ];

        // ── writeback: false -- a plain read: no durability, no audit ──
        let method_ro = Method::MineAssociate {
            transactions: transactions.clone(),
            source: None,
            min_support: 0.5,
            min_confidence: 0.5,
            algorithm: crate::protocol::MineAlgorithm::default(),
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let plan_ro = MutationPlan::for_method(&method_ro);
        let ctx_ro = MutationCtx {
            req_id: 20,
            caller: Some("system-agent"),
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };
        let resp = commit_conditional_mutation(&ctx_ro, &plan_ro, &method_ro, false, |core| {
            let resp = crate::server::handlers::mining::handle_associate(
                20,
                core,
                transactions.clone(),
                None,
                0.5,
                0.5,
                crate::protocol::MineAlgorithm::default(),
                false,
                #[cfg(feature = "epistemic")]
                false,
            );
            match resp.error {
                Some(e) => Err(e),
                None => Ok(resp.result.unwrap()),
            }
        })
        .await;
        assert!(
            resp.error.is_none(),
            "read-only call failed: {:?}",
            resp.error
        );

        let fname = crate::persist::sanitize(graph_name);
        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok);
        assert_eq!(
            report.entries, 0,
            "writeback:false must produce NO durable/audit record"
        );

        // ── writeback: true -- a real mutation: durable + audited ──
        let method_rw = Method::MineAssociate {
            transactions: transactions.clone(),
            source: None,
            min_support: 0.5,
            min_confidence: 0.5,
            algorithm: crate::protocol::MineAlgorithm::default(),
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let plan_rw = MutationPlan::for_method(&method_rw);
        assert!(plan_rw.audited, "MineAssociate is policy-audited");
        assert!(!plan_rw.emits_cdc, "MineAssociate policy-does-NOT-emit-CDC");
        let ctx_rw = MutationCtx {
            req_id: 21,
            caller: Some("system-agent"),
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };
        let resp = commit_conditional_mutation(&ctx_rw, &plan_rw, &method_rw, true, |core| {
            let resp = crate::server::handlers::mining::handle_associate(
                21,
                core,
                transactions.clone(),
                None,
                0.5,
                0.5,
                crate::protocol::MineAlgorithm::default(),
                true,
                #[cfg(feature = "epistemic")]
                false,
            );
            match resp.error {
                Some(e) => Err(e),
                None => Ok(resp.result.unwrap()),
            }
        })
        .await;
        assert!(
            resp.error.is_none(),
            "writeback call failed: {:?}",
            resp.error
        );

        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok);
        assert_eq!(
            report.entries, 1,
            "writeback:true must produce exactly one audited durable record"
        );
    }

    /// (b) An unauthorized actor is rejected AT THE GATEWAY, before `apply` ever
    /// runs (proven by asserting the graph is left untouched).
    #[tokio::test(flavor = "multi_thread")]
    async fn unauthorized_actor_is_rejected_at_the_gateway() {
        let core = Arc::new(GraphCore::new());
        let mut isolation = IsolationLayer::new();
        // Registering ANY identity flips `has_rules()` on, switching graph-
        // targeted dispatch into enforcing mode.
        isolation.register_agent(crate::acl::AgentIdentity {
            agent_id: "owner".to_string(),
            role: crate::acl::AgentRole::Agent,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        isolation.register_agent(crate::acl::AgentIdentity {
            agent_id: "intruder".to_string(),
            role: crate::acl::AgentRole::Agent,
            teams: Vec::new(),
            roles: Vec::new(),
        });

        let method = Method::AddNode {
            node_id: "n1".to_string(),
            properties_msgpack: Vec::new(),
        };
        let plan = MutationPlan::for_method(&method);

        let ctx = MutationCtx {
            req_id: 3,
            caller: Some("intruder"),
            tenant_scope: "opaque-test-tenant",
            graph_name: "agent:owner",
            graph_type: GraphType::Agent,
            owner: Some("owner"),
            isolation: &isolation,
            core: &core,
            persistence: None,
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };

        let mut apply_ran = false;
        let resp = commit_mutation(&ctx, &plan, &method, |core| {
            apply_ran = true;
            core.add_node("n1".to_string(), Vec::new());
            Ok(ResultPayload::String("ok".to_string()))
        })
        .await;

        assert!(
            resp.error.is_some(),
            "expected ACCESS_DENIED, got {:?}",
            resp
        );
        assert!(resp.error.unwrap().contains("ACCESS_DENIED"));
        assert!(!apply_ran, "apply must never run for a denied caller");
        assert!(!core.has_node("n1"), "the graph must be untouched");
    }

    /// (c) An idempotent method's (`RemoveNode`) replay with the SAME key is
    /// deduped: the second call returns the cached response WITHOUT re-invoking
    /// `apply` (a re-`apply` on an already-removed node would still succeed, so
    /// only an explicit apply-count check proves dedup, not just a repeated OK).
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn idempotent_replay_is_deduped_without_reapplying() {
        let dir = temp_dir("idempotent-replay");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open(
                dir.to_string_lossy().to_string(),
                DurabilityPolicy::Each,
                64,
            )
            .expect("open redb backend"),
        );
        let core = Arc::new(GraphCore::new());
        core.add_node("n1".to_string(), Vec::new());
        let isolation = isolation_with_system_agent();
        let graph_name = "g-eg-p0-2-idem-unique-9f31";

        let method = Method::RemoveNode {
            node_id: "n1".to_string(),
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.idempotent, "RemoveNode is policy-idempotent");

        let ctx = MutationCtx {
            req_id: 4,
            caller: Some("system-agent"),
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };

        let apply_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count1 = apply_count.clone();
        let resp1 = commit_mutation(&ctx, &plan, &method, |core| {
            count1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            core.remove_node("n1".to_string());
            Ok(ResultPayload::String("ok".to_string()))
        })
        .await;
        assert!(resp1.error.is_none());
        assert_eq!(apply_count.load(std::sync::atomic::Ordering::SeqCst), 1);

        // Replay: identical method + graph -- must hit the cache, NOT re-apply.
        let count2 = apply_count.clone();
        let resp2 = commit_mutation(&ctx, &plan, &method, |core| {
            count2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            core.remove_node("n1".to_string());
            Ok(ResultPayload::String("ok".to_string()))
        })
        .await;
        assert!(resp2.error.is_none());
        assert_eq!(
            apply_count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "replay must be deduped, not re-applied"
        );
        assert_eq!(
            format!("{:?}", resp1.result),
            format!("{:?}", resp2.result),
            "replay returns the cached response"
        );
    }

    /// (d) Bypass guard, part 1: `MutationPlan` never diverges from
    /// `eg_capabilities::policy` for any [`GATEWAY_ROUTED`] method -- if someone
    /// edits `MutationPlan::for_method` to hardcode a field instead of reading it
    /// off `policy()`, this catches the divergence.
    #[test]
    fn routed_methods_plan_is_never_hardcoded_relative_to_policy() {
        let samples: Vec<Method> = vec![
            Method::AddNode {
                node_id: "x".into(),
                properties_msgpack: Vec::new(),
            },
            Method::CreateNodeIfAbsent {
                node_id: "x".into(),
                properties_msgpack: Vec::new(),
            },
            Method::RemoveNode {
                node_id: "x".into(),
            },
            Method::AddEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                properties_msgpack: Vec::new(),
            },
            Method::RemoveEdge {
                source_id: "a".into(),
                target_id: "b".into(),
            },
            Method::CreateSummaryNode {
                level: 1,
                child_ids: vec!["a".into()],
                props_msgpack: Vec::new(),
            },
            Method::Consolidate {
                episodic_ids: vec!["a".into()],
                semantic_props_msgpack: Vec::new(),
            },
            Method::Reinforce {
                node_id: "x".into(),
                now_ms: 0,
                weight: 0.0,
            },
            // ── L11 rollout batch 2: graph-core family ──
            Method::CompareAndSetNodeFields {
                node_id: "x".into(),
                conditions_msgpack: Vec::new(),
                updates_msgpack: Vec::new(),
            },
            Method::ClaimNext {
                label: "x".into(),
                updates_msgpack: Vec::new(),
            },
            Method::DecayNode {
                node_id: "x".into(),
                now_ms: 0,
                half_life_ms: 1,
            },
            Method::DecayMemories {
                now_ms: 0,
                half_life_ms: 1,
                ids: vec!["x".into()],
            },
            Method::EvictBelow {
                ids: vec!["x".into()],
                threshold: 0.0,
                delete: false,
            },
            Method::Maintain {
                ids: vec!["x".into()],
                now_ms: 0,
                half_life_ms: 1,
                evict_threshold: 0.0,
                delete: false,
            },
            Method::AddSceneObject {
                pose_msgpack: Vec::new(),
                parent: None,
            },
            Method::SetPose {
                node_id: "x".into(),
                pose_msgpack: Vec::new(),
            },
            Method::Reparent {
                node_id: "x".into(),
                new_parent: None,
            },
            Method::StartTrajectory {
                props_msgpack: Vec::new(),
            },
            Method::AppendStep {
                traj_id: "x".into(),
                action_msgpack: Vec::new(),
                reward: 0.0,
                state_ref: None,
                next_state_ref: None,
                t: 0,
            },
            Method::AddEmbedding {
                node_id: "x".into(),
                embedding: vec![0.0],
            },
            Method::InvalidateEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                relationship: "r".into(),
                invalid_at: 0,
                tx_now: 0,
            },
            Method::SupersedeEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                properties_msgpack: Vec::new(),
                prior_source: "a".into(),
                prior_target: "b".into(),
                prior_relationship: "r".into(),
                valid_at: 0,
                tx_now: 0,
            },
            Method::ClearGraph,
            Method::EvictLRU { max_nodes: 1 },
            Method::DecaySweep {
                half_life_secs: 1.0,
                floor: 0.0,
                prune: false,
            },
            Method::TouchNodes {
                node_ids: vec!["x".into()],
            },
            Method::FromMsgpack {
                msgpack: Vec::new(),
            },
            Method::Reconcile {
                graph_name: "g".into(),
                msgpack: Vec::new(),
            },
            Method::ApplyMutation {
                event_type: "e".into(),
                query: "q".into(),
            },
            #[cfg(feature = "shacl")]
            Method::IcvConfigure {
                graph: None,
                mode: "enforce".into(),
                shapes: "@prefix sh: <http://www.w3.org/ns/shacl#> .".into(),
            },
            #[cfg(feature = "reasoning")]
            Method::RunDatalogReasoning {
                subclass_relations: Vec::new(),
                subproperty_relations: Vec::new(),
                symmetric_properties: Vec::new(),
                transitive_properties: Vec::new(),
                inverse_properties: Vec::new(),
                domain_rules: Vec::new(),
                range_rules: Vec::new(),
                property_chains: Vec::new(),
            },
            Method::PruneByLifecycle {
                max_age_secs: 1,
                min_score: 0.0,
            },
            Method::BatchUpdate {
                operations_msgpack: Vec::new(),
            },
            Method::ClearLedger,
            Method::ApplyLedger {
                transactions: Vec::new(),
            },
            Method::CompactNodesByType {
                node_type: "x".into(),
                threshold: 1,
            },
            // ── L11 rollout batch 2: message-broker / stream family ──
            #[cfg(feature = "broker")]
            Method::DeclareExchange {
                exchange: "x".into(),
                kind: "direct".into(),
            },
            #[cfg(feature = "broker")]
            Method::DeleteExchange {
                exchange: "x".into(),
            },
            #[cfg(feature = "broker")]
            Method::BindQueue {
                exchange: "x".into(),
                queue: "q".into(),
                routing_key: "k".into(),
            },
            #[cfg(feature = "broker")]
            Method::UnbindQueue {
                exchange: "x".into(),
                queue: "q".into(),
                routing_key: "k".into(),
            },
            #[cfg(feature = "broker")]
            Method::Publish {
                exchange: "x".into(),
                routing_key: "k".into(),
                payload: Vec::new(),
            },
            #[cfg(feature = "broker")]
            Method::DeclareQueue {
                queue: "q".into(),
                dl_exchange: None,
                dl_routing_key: None,
                max_delivery_count: None,
                message_ttl_ms: None,
                queue_expiry_ms: None,
                max_priority: None,
            },
            #[cfg(feature = "broker")]
            Method::PublishEx {
                exchange: "x".into(),
                routing_key: "k".into(),
                payload: Vec::new(),
                priority: 0,
                delay_ms: None,
                ttl_ms: None,
                now_ms: None,
            },
            #[cfg(feature = "broker")]
            Method::BrokerConsume {
                queue: "q".into(),
                group: "g".into(),
                consumer: "c".into(),
                now_ms: 0,
                lease_ms: 0,
                prefetch: 0,
            },
            #[cfg(feature = "broker")]
            Method::BrokerAck {
                queue: "q".into(),
                node_id: "x".into(),
            },
            #[cfg(feature = "broker")]
            Method::BrokerReject {
                queue: "q".into(),
                node_id: "x".into(),
                requeue: false,
                now_ms: 0,
            },
            #[cfg(feature = "broker")]
            Method::SweepExpired { now_ms: 0 },
            #[cfg(feature = "broker")]
            Method::StreamDeclare {
                stream: "s".into(),
                max_messages: None,
                max_age_ms: None,
            },
            #[cfg(feature = "broker")]
            Method::StreamPublish {
                stream: "s".into(),
                payload: Vec::new(),
                now_ms: 0,
            },
            #[cfg(feature = "broker")]
            Method::StreamTrim {
                stream: "s".into(),
                now_ms: 0,
            },
            #[cfg(feature = "broker")]
            Method::StreamCommitOffset {
                stream: "s".into(),
                group: "g".into(),
                offset: 0,
            },
            #[cfg(feature = "broker")]
            Method::PublishConfirmed {
                exchange: "x".into(),
                routing_key: "k".into(),
                payload: Vec::new(),
                priority: 0,
                delay_ms: None,
                ttl_ms: None,
                now_ms: None,
            },
            #[cfg(feature = "broker")]
            Method::PublishIdempotent {
                exchange: "x".into(),
                routing_key: "k".into(),
                payload: Vec::new(),
                producer_id: None,
                seq: 0,
                priority: 0,
                delay_ms: None,
                ttl_ms: None,
                now_ms: None,
            },
            #[cfg(feature = "broker")]
            Method::BrokerAckTag {
                delivery_tag: 0,
                consumer: "c".into(),
            },
            #[cfg(feature = "broker")]
            Method::BrokerNackTag {
                delivery_tag: 0,
                consumer: "c".into(),
                requeue: false,
                now_ms: 0,
            },
            #[cfg(feature = "broker")]
            Method::BrokerRenewTag {
                delivery_tag: 0,
                consumer: "c".into(),
                now_ms: 0,
                lease_ms: 1,
            },
            // ── L11 rollout batch 3: runtime-conditional graph-learning family ──
            #[cfg(feature = "graphlearn")]
            Method::GraphLearnFit {
                source: crate::protocol::GraphSource {
                    node_label: "x".into(),
                    direction: "any".into(),
                    relation: None,
                    limit: 0,
                },
                params: crate::protocol::GraphLearnParams {
                    basis: "chebyshev".into(),
                    degree: 3,
                    hidden: 0,
                    epochs: 1,
                    lr: 0.1,
                    neg_ratio: 1.0,
                    seed: 0,
                    alpha: 0.5,
                },
                writeback: false,
            },
            #[cfg(feature = "graphlearn")]
            Method::GraphLearnPredict {
                model: serde_json::Value::Null,
                source: crate::protocol::GraphSource {
                    node_label: "x".into(),
                    direction: "any".into(),
                    relation: None,
                    limit: 0,
                },
                candidate_pairs: Vec::new(),
                top_k: 10,
                writeback: false,
            },
            // ── L11 rollout batch 3: runtime-conditional data-mining family.
            // `MineAssociate` is a SPOT CHECK, not full coverage of all 17: every
            // Mine* variant's `policy()` arm (`crates/eg-capabilities/src/lib.rs`)
            // is one of 4 groups that share the IDENTICAL `MethodPolicy` value, and
            // `MutationPlan::for_method`'s call site has no per-variant branching at
            // all (it is `eg_capabilities::policy(method)` verbatim for every Method
            // in existence) -- so one sample exercises the same code path the other
            // 16 would. Full name-level coverage (every Mine* name really is a
            // mutating, policy-known method) is still asserted for ALL 17 by
            // `gateway_routed_set_matches_mutating_policy_surface` below, which
            // needs no `Method` construction (it reads `eg_capabilities::ALL_METHODS`
            // by name). The runtime-conditional (`writeback`) gateway PATH itself is
            // proven end-to-end (WAL + audit, no CDC, Read-vs-Write ACL both ways)
            // by `mining_family_writeback_gates_durability_and_authz` below.
            #[cfg(feature = "mining")]
            Method::MineAssociate {
                transactions: vec![vec!["a".into(), "b".into()]],
                source: None,
                min_support: 0.1,
                min_confidence: 0.5,
                algorithm: crate::protocol::MineAlgorithm::default(),
                writeback: false,
                #[cfg(feature = "epistemic")]
                as_claim: false,
            },
        ];
        // NOTE: this crate's default/test feature set always includes `broker` +
        // `reasoning` + `graphlearn` + `mining` (`default = ["graph", "algorithms",
        // "metrics", "full"]`), so every sample above is unconditionally constructed
        // in the build this test actually runs under; the `#[cfg]`s only keep the
        // file correct (it still compiles) for a slimmer, non-default feature
        // selection. `samples` is intentionally a SUBSET of `GATEWAY_ROUTED` now
        // (see the Mine* comment above) -- every sample must still be routed +
        // policy-consistent, but not every routed name needs a hand-built sample.
        assert!(samples.len() <= GATEWAY_ROUTED.len());
        for m in &samples {
            assert!(is_gateway_routed(m), "{m:?} must be in GATEWAY_ROUTED");
            let plan = MutationPlan::for_method(m);
            let p = eg_capabilities::policy(m);
            assert_eq!(plan.mutates, p.mutates, "{}", plan.method_name);
            assert_eq!(
                plan.durability_domain, p.durability_domain,
                "{}",
                plan.method_name
            );
            assert_eq!(plan.authz_action, p.authz_action, "{}", plan.method_name);
            assert_eq!(plan.idempotent, p.idempotent, "{}", plan.method_name);
            assert_eq!(plan.audited, p.audited, "{}", plan.method_name);
            assert_eq!(plan.emits_cdc, p.emits_cdc, "{}", plan.method_name);
            assert_eq!(
                plan.txn_participation, p.txn_participation,
                "{}",
                plan.method_name
            );
        }
    }

    /// L11 rollout: every mutating method NOT in [`GATEWAY_ROUTED`] falls into
    /// EXACTLY one of two documented buckets -- there is no silent third category.
    ///
    /// **NON_GATEWAY_COORDINATED** -- does not fit `commit_mutation`'s
    /// `(ctx, plan, method, apply)` shape for a real, load-bearing architectural
    /// reason (a different commit protocol entirely, a non-graph-scoped store
    /// with no `GraphCore`/`graph_name` in scope, a cross-shard/cluster-wide op,
    /// or a registry-lifecycle op that runs before/across a single graph's core
    /// exists). Each entry cites the native MutationBatch, saga, translation, or
    /// explicitly non-authoritative staging mechanism that owns the operation.
    ///
    /// **OPEN_NOT_JUSTIFIED** -- reserved for a graph-scoped, structurally routable
    /// mutation that has no coordinator yet. It is intentionally empty and asserted
    /// empty below; adding such a method is therefore a visible test-gated regression.
    const NON_GATEWAY_COORDINATED: &[(&str, &str)] = &[
        // ── OCC/2PC multi-op transaction protocol (src/server/handlers/txn.rs) ──
        // `Commit` applies a DYNAMIC list of already-staged Methods through one or
        // more child MutationBatches, while a multi-graph commit routes through 2PC
        // (`CrossShardCoordinator`), and a cross-modal commit lands
        // graph+vector+blob+series in ONE redb `WriteTransaction`
        // (`commit_cross_modal_txn`) -- none of that is a single `(ctx, plan,
        // method, apply)` call the gateway models. The Txn* STAGE ops don't even
        // mutate durable state at stage time (only `Commit` does); they self-route
        // in `dispatch.rs` BEFORE `dispatch_graph_op` entirely (resolved from
        // `open_txns`, not `req.graph`).
        ("BeginTxn", "encrypted Raft-native OCC staging authority"),
        ("TxnAddNode", "replicated OCC staging; named Commit receipt and graph MutationBatch own publication authority"),
        ("TxnRemoveNode", "ephemeral OCC staging; named Commit receipt and graph MutationBatch own authority"),
        ("TxnAddEdge", "ephemeral OCC staging; named Commit receipt and graph MutationBatch own authority"),
        ("TxnRemoveEdge", "ephemeral OCC staging; named Commit receipt and graph MutationBatch own authority"),
        ("TxnCas", "ephemeral OCC staging; named Commit receipt and graph MutationBatch own authority"),
        ("TxnAddEmbedding", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("TxnBlobRef", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("TxnAddMeasurement", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("TxnAxiom", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("TxnConstruct", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("TxnPlanWriteback", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("TxnMaterializeBelief", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("Commit", "named control-plane receipt coordinates graph/cross-modal/2PC child MutationBatches"),
        ("Rollback", "replicated removal of encrypted OCC staging authority"),
        ("ClaimWorkItem", "dedicated engine-native MutationBatch lease transition in mutation_batch.rs/redb_store.rs"),
        ("RenewWorkItemLease", "dedicated engine-native MutationBatch lease-fence transition in mutation_batch.rs/redb_store.rs"),
        ("CommitWorkItemResult", "dedicated engine-native MutationBatch result/dependency transition in mutation_batch.rs/redb_store.rs"),
        ("CancelWorkItem", "dedicated engine-native MutationBatch pending-cancellation transition in mutation_batch.rs/redb_store.rs"),
        ("DeferWorkItem", "dedicated engine-native MutationBatch fenced deferral transition in mutation_batch.rs/redb_store.rs"),
        // ── Non-graph-scoped stores: no GraphCore/graph_name in scope at all;
        // each self-routes in dispatch.rs BEFORE the per-graph chain, exactly
        // like Txn*, with its OWN dedicated redb file + commit path. ──
        ("BlobBegin", "native MutationBatch in blob.redb: durable upload cursor + status/fence/idempotency/outbox in one WTX"),
        ("BlobChunkPut", "native MutationBatch in blob.redb: chunk + upload cursor + coordinator metadata in one WTX"),
        ("BlobCommit", "native MutationBatch in blob.redb: manifest/cursor transition + coordinator metadata in one WTX"),
        ("BlobGc", "native MutationBatch in blob.redb: GC rows + exact result/coordinator metadata in one WTX"),
        ("BlobRef", "native MutationBatch in blob.redb: refcount + coordinator metadata in one WTX"),
        ("BlobUnref", "native MutationBatch in blob.redb: refcount + coordinator metadata in one WTX"),
        ("KvPut", "native MutationBatch in kv.redb: KV row + status/fence/idempotency/outbox in one WTX"),
        ("KvDelete", "native MutationBatch in kv.redb: KV row + status/fence/idempotency/outbox in one WTX"),
        ("KvCas", "native MutationBatch in kv.redb: CAS decision/row + exact result/coordinator metadata in one WTX"),
        ("TsAppend", "native MutationBatch in series.redb: series rows/projection + coordinator metadata in one WTX"),
        #[cfg(feature = "jobs")]
        ("AnalyticsJob", "native MutationBatch in jobs.redb; asynchronous claim writeback uses a staged graph MutationBatch"),
        // `Statechart` self-routes in dispatch.rs BEFORE dispatch_graph_op (see the
        // `Method::Statechart` arm there and `handlers::statechart` module docs) --
        // it never reaches this gateway's `try_handle_gateway`/`commit_mutation` at
        // all. `eg-statechart`'s `StatechartStore::instantiate`/`send_event` commit
        // through `eg-mutation-store` (the SAME universal MutationBatch/OCC
        // primitive `eg-jobs` uses for `AnalyticsJob`, per eg-statechart/Cargo.toml)
        // against their OWN `statecharts.redb`, keyed by def_id/instance_id, not a
        // graph -- structurally identical to `AnalyticsJob` above, just gated
        // `statechart` instead of `jobs`. Note: this is `eg-mutation-store` (a
        // generic per-store OCC/durable-commit primitive shared by jobs/kv/blob/
        // series/statecharts), NOT this module's `GATEWAY_ROUTED`/`commit_mutation`
        // -- the two are easily conflated by name but are different mechanisms.
        #[cfg(feature = "statechart")]
        ("Statechart", "native MutationBatch in statecharts.redb; instance define/instantiate/send_event commit status/version/fence/idempotency/outbox in one WTX via eg-mutation-store, exactly like AnalyticsJob in jobs.redb"),
        ("ImportSqliteFile", "native SQL-catalog MutationBatch: all imported tables + exact result/coordinator metadata in one WTX"),
        // ── Process-global registries on ServerState: opaque control-redb sagas,
        // no GraphCore/graph_name; dispatched directly in the top-level match. ──
        ("CreateChannel", "opaque prepared/committed session-control MutationBatch"),
        ("JoinChannel", "opaque prepared/committed session-control MutationBatch"),
        ("LeaveChannel", "opaque prepared/committed session-control MutationBatch"),
        ("CloseChannel", "opaque prepared/committed session-control MutationBatch"),
        ("SendMessage", "request-scoped opaque session-control saga; a committed retry never duplicates delivery"),
        ("RegisterIdentity", "native rbac.redb MutationBatch shares the RBAC snapshot WTX"),
        ("RbacAdmin", "native rbac.redb MutationBatch shares the RBAC snapshot WTX"),
        ("RegisterForeignSource", "opaque prepared/committed session-control MutationBatch"),
        ("RegisterUdf", "opaque prepared/committed session-control MutationBatch"),
        ("RegisterContinuousQuery", "opaque prepared/committed session-control MutationBatch"),
        ("DropContinuousQuery", "opaque prepared/committed session-control MutationBatch"),
        ("RegisterTrigger", "opaque prepared/committed session-control MutationBatch"),
        ("DropTrigger", "opaque prepared/committed session-control MutationBatch"),
        ("CepSubscribe", "opaque prepared/committed session-control MutationBatch"),
        ("CepUnsubscribe", "opaque prepared/committed session-control MutationBatch"),
        // ── Cluster-wide / cross-shard admin ops: operate across the WHOLE
        // registry/cluster under a durable control-plane saga
        // (handlers::admin::try_handle / handlers::dist_compute::try_handle), not
        // a single resolved graph's core. ──
        ("Reshard", "prepared/committed admin MutationBatch saga around cluster-wide resharding"),
        ("CatalogAssign", "prepared/committed admin MutationBatch saga around the durable tenant catalog"),
        ("CatalogReassign", "prepared/committed admin MutationBatch saga around the durable tenant catalog"),
        ("CatalogRemove", "prepared/committed admin MutationBatch saga around the durable tenant catalog"),
        ("RebalanceExecute", "prepared/committed admin MutationBatch saga around cluster-wide rebalance"),
        ("Restore", "prepared/committed admin MutationBatch saga around online restore/PITR"),
        ("PlacementAssign", "raft-replicated placement-catalog admin op; MultiRaft::placement_assign commits through the DEFAULT group's own client_write to the __placement_catalog__ control graph, not this gateway's per-graph MutationBatch"),
        ("PlacementMove", "raft-replicated placement-catalog admin op; TenantManager::move_partition drives its durable move journal through MultiRaft's commit_placement, not this gateway's per-graph MutationBatch"),
        ("PlacementAbortMove", "raft-replicated placement-catalog admin op; TenantManager::abort_move restores the pre-cutover source route through MultiRaft's commit_placement, not this gateway's per-graph MutationBatch"),
        ("CreateMatView", "prepared/committed control-plane MutationBatch saga around the durable cross-shard view row"),
        ("RefreshMatView", "prepared/committed control-plane MutationBatch saga around the durable cross-shard view row"),
        ("PlanMatViewDefine", "prepared/committed control-plane MutationBatch saga around the durable plan definition"),
        ("PlanMatViewRefresh", "prepared/committed control-plane MutationBatch saga around derived-cache refresh"),
        ("PlanMatViewDrop", "prepared/committed control-plane MutationBatch saga around durable definition removal"),
        // ── Registry-lifecycle ops: run BEFORE/ACROSS a single graph's core
        // exists (or spans many), so there is no one EXISTING graph to route
        // "through". ──
        ("CreateGraph", "native lifecycle MutationBatch commits graph identity before registry publication"),
        ("DeleteGraph", "native lifecycle MutationBatch commits purge before registry eviction"),
        ("MultiGraphBatchUpdate", "cluster placement fanout emits one typed graph command per child; standalone mode uses a durable parent saga"),
        ("ApplyChangeEnvelope", "governed envelope coordinator commits typed graph/object/provenance rows, cursor, version, and outbox through one native MutationBatch"),
        ("RecomputeMaterialization", "fenced reasoning-projection coordinator resolves authoritative graph provenance and fsyncs its projection watermark"),
        // ── Server lifecycle: TxnParticipation::None, not a graph mutation. ──
        ("Shutdown", "server-lifecycle control-plane action, not a graph mutation"),
        // ── Translates away before it could ever reach the gateway. ──
        (
            "ApplyMultisigMutation",
            "validates the multisig threshold then TRANSLATES into a Method::ApplyMutation \
             dispatched through the ordinary dispatch_graph_op path (which IS gateway-routed) -- \
             see dispatch.rs; by the time a mutation happens the method value has already become \
             ApplyMutation, so this variant itself never reaches commit_mutation directly",
        ),
    ];

    /// Graph-scoped methods requiring a coordinator outside this gateway. The set is
    /// **EMPTY**: all former entries now have an owning mutation kernel --
    ///   - `Sql`/`CypherQuery`/`GraphQl` are now routed via
    ///     `commit_conditional_mutation_async` at the query dispatch site (the async
    ///     twin built for exactly the `state`/`rls`-needing surfaces);
    ///   - `AddTriples`/`RemoveTriples`/`DropNamedGraph` are routed the same way at
    ///     the RDF dispatch site, with multi-valued literals included in the staged
    ///     authoritative image;
    ///   - `RunRules` was AUDITED to be genuinely read-only, so the fix was the
    ///     policy (`eg_capabilities::policy(RunRules).mutates` : true -> false), not
    ///     a route -- it left the mutating surface entirely.
    ///
    /// Kept as an explicit (empty) const, and asserted empty below, so "nothing is
    /// silently deferred" stays a machine-checked invariant: any future mutating
    /// method that is routable-but-unrouted must be listed here (a hard, visible
    /// admission), never dropped into the untracked void.
    const OPEN_NOT_JUSTIFIED: &[(&str, &str)] = &[];

    /// (d) Bypass guard, part 2: the migration surface is machine-visible. Every
    /// name in [`GATEWAY_ROUTED`] really exists in `eg_capabilities::ALL_METHODS`
    /// and really is `mutates == true` (catches a rename/typo silently un-routing
    /// a method); the COMPLEMENT (every other mutating method) must fall ENTIRELY
    /// into [`NON_GATEWAY_COORDINATED`] or [`OPEN_NOT_JUSTIFIED`] -- an undocumented name in
    /// the complement is a hard test failure (a silent skip), not a warning.
    #[test]
    fn gateway_routed_set_matches_mutating_policy_surface() {
        use std::collections::BTreeSet;

        let all_mutating: BTreeSet<&'static str> = eg_capabilities::ALL_METHODS
            .iter()
            .filter(|(_, p, _)| p.mutates)
            .map(|(name, _, _)| *name)
            .collect();

        for routed in GATEWAY_ROUTED {
            assert!(
                all_mutating.contains(routed),
                "GATEWAY_ROUTED name '{routed}' is not a mutating method in \
                 eg_capabilities::ALL_METHODS (renamed/typo'd?)"
            );
        }

        let routed_set: BTreeSet<&'static str> = GATEWAY_ROUTED.iter().copied().collect();
        let not_yet_migrated: Vec<&'static str> =
            all_mutating.difference(&routed_set).copied().collect();

        let coordinated: std::collections::HashMap<&'static str, &'static str> =
            NON_GATEWAY_COORDINATED.iter().copied().collect();
        let open: std::collections::HashMap<&'static str, &'static str> =
            OPEN_NOT_JUSTIFIED.iter().copied().collect();

        // Every NON_GATEWAY_COORDINATED / OPEN_NOT_JUSTIFIED entry must be a real,
        // currently-non-routed, mutating method name -- catches a stale doc entry
        // (a name that got routed, renamed, or was never mutating) as loudly as an
        // undocumented one.
        for name in coordinated.keys().chain(open.keys()) {
            assert!(
                not_yet_migrated.contains(name),
                "'{name}' is documented in NON_GATEWAY_COORDINATED/OPEN_NOT_JUSTIFIED but is NOT in the \
                 current non-gateway set (already routed, renamed, or never a mutating method?) -- stale entry"
            );
        }

        let undocumented: Vec<&&'static str> = not_yet_migrated
            .iter()
            .filter(|name| !coordinated.contains_key(*name) && !open.contains_key(*name))
            .collect();
        assert!(
            undocumented.is_empty(),
            "UNDOCUMENTED non-gateway entries (silently deferred, not allowed): {undocumented:?} -- \
             add each to NON_GATEWAY_COORDINATED (native coordinator) or OPEN_NOT_JUSTIFIED (honest \
             remainder) in server::mutation::tests"
        );
        assert_eq!(
            not_yet_migrated.len(),
            coordinated.len() + open.len(),
            "NON_GATEWAY_COORDINATED + OPEN_NOT_JUSTIFIED must exactly partition the non-gateway set"
        );
        // L11 close-out invariant: the OPEN (routable-but-unrouted) bucket is EMPTY.
        // Every non-routed mutating method has a concrete native coordinator,
        // translation, or pre-commit staging protocol; none is a durability bypass.
        assert!(
            open.is_empty(),
            "OPEN_NOT_JUSTIFIED must be empty (all routable methods are owned); entries: {:?}",
            open.keys().collect::<Vec<_>>()
        );

        println!(
            "EG-P0-2/L11 gateway migration surface: {} routed / {} mutating total; \
             {} non-gateway = {} natively coordinated + \
             {} open-not-justified (MUST be 0)",
            routed_set.len(),
            all_mutating.len(),
            not_yet_migrated.len(),
            coordinated.len(),
            open.len(),
        );
    }

    /// Clustered admission has no deferred mutation family. The union of the
    /// graph state-machine gateway, bounded native commands, specialized governed
    /// envelope/modality commands, and process shutdown exactly equals the current
    /// mutating capability ledger.
    #[cfg(feature = "raft")]
    #[test]
    fn clustered_mutation_inventory_is_complete() {
        use std::collections::BTreeSet;

        let expected: BTreeSet<&'static str> = eg_capabilities::ALL_METHODS
            .iter()
            .filter(|(_, policy, _)| policy.mutates)
            .map(|(name, _, _)| *name)
            .collect();
        let mut covered: BTreeSet<&'static str> = GATEWAY_ROUTED.iter().copied().collect();
        covered.extend(crate::raft::NATIVE_CONSENSUS_METHODS.iter().copied());
        covered.extend(CONSENSUS_FANOUT_METHODS.iter().copied());
        covered.insert("ApplyChangeEnvelope");
        covered.insert("ServedModality");
        covered.insert("Shutdown");

        let missing: Vec<_> = expected.difference(&covered).copied().collect();
        let stale: Vec<_> = covered.difference(&expected).copied().collect();
        assert!(
            missing.is_empty() && stale.is_empty(),
            "cluster mutation inventory drift: missing={missing:?}, stale={stale:?}"
        );
    }

    #[test]
    fn clustered_mutations_are_consensus_typed() {
        assert_eq!(
            cluster_mutation_route(&Method::AddNode {
                node_id: "opaque-node".to_string(),
                properties_msgpack: Vec::new(),
            }),
            ClusterMutationRoute::ConsensusGraph,
        );
        assert_eq!(
            cluster_mutation_route(&Method::CreateGraph {
                graph_name: "opaque-graph".to_string(),
                graph_type: GraphType::Global,
            }),
            ClusterMutationRoute::ConsensusNative,
        );
        assert_eq!(
            cluster_mutation_route(&Method::ApplyMutation {
                event_type: "opaque-event".to_string(),
                query: "opaque-operation".to_string(),
            }),
            ClusterMutationRoute::ConsensusNative,
        );
        #[cfg(feature = "sparql-http")]
        assert_eq!(
            cluster_mutation_route(&Method::ApplyMutation {
                event_type: crate::server::sparql_http::SPARQL_HTTP_UPDATE_EVENT.to_string(),
                query: "CLEAR DEFAULT".to_string(),
            }),
            ClusterMutationRoute::ConsensusFanout,
        );
        assert_eq!(
            cluster_mutation_route(&Method::MultiGraphBatchUpdate {
                batches_msgpack: Vec::new(),
            }),
            ClusterMutationRoute::ConsensusFanout,
        );
        assert_eq!(
            cluster_mutation_route(&Method::Shutdown),
            ClusterMutationRoute::VolatileControl,
        );
        assert_eq!(
            cluster_mutation_route(&Method::PlacementRoute {
                request: crate::epistemic_operations::PlacementRouteRequest {
                    schema_version:
                        crate::epistemic_operations::PlacementRouteRequestSchemaVersion::V1,
                    tenant_ref: "opaque-tenant".to_string(),
                    partition_ref: "opaque-partition".to_string(),
                    client_epoch: 0,
                },
            }),
            ClusterMutationRoute::ReadOnly,
        );
    }
}
