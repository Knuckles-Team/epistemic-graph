//! `MutationPlan` + the single commit gateway (CONCEPT:EG-P0-2).
//!
//! ## Why this exists
//!
//! `eg-capabilities` (CONCEPT:EG-P0-1) is a machine-checked, exhaustive `MethodPolicy`
//! table over every `Method` variant — but on its own it is a one-way AUDIT: it
//! cross-checks the existing hand-rolled classifiers (`access.rs::requires_write`,
//! `wal.rs::is_durable_mutation`, `audit.rs::audit_line`, `cdc.rs::emit_for_method`)
//! against its declared policy, without either side CONSUMING the other. Handlers
//! still mutate `eg-core` directly (`src/server/handlers/graph_ops.rs`'s
//! `g.add_node(...)` etc.), and the dispatch shell (`src/server/dispatch.rs`)
//! separately re-derives whether to WAL-record / CDC-emit a given method from its
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
//! `Consolidate`/`Reinforce`); the L11 rollout expanded this to 74, adding:
//!   * the rest of the plain graph-core family (CAS/claim/edge-temporal/scene/
//!     trajectory/embedding/ledger/lifecycle ops — all handled in `graph_ops.rs`
//!     already, same shape as the original 7);
//!   * the message-broker/stream family (`Outbox` durability domain, behind
//!     `feature = "broker"` — proves the gateway is domain-agnostic: `commit_mutation`
//!     doesn't branch on `DurabilityDomain`, only on whether it's `None`);
//!   * `IcvConfigure` (never touches `GraphCore` at all — `apply` ignores its
//!     `&GraphCore` parameter — but still rides the graph-scoped authz/durability
//!     shape);
//!   * BOTH runtime-conditional families whose REAL `mutates` answer is a
//!     per-invocation `writeback: bool` field, not `policy()`'s conservative
//!     upper bound: `GraphLearnFit`/`GraphLearnPredict` and every writeback-capable
//!     `Mine*` — routed via [`commit_conditional_mutation`], NOT `commit_mutation`,
//!     so a `writeback: false` call is correctly treated as a plain read (Read
//!     ACL, no durability/audit/CDC) instead of being incorrectly gated behind
//!     Write access or persisted as a phantom mutation.
//!
//! This is still not a claim of full coverage — see
//! `tests::gateway_routed_set_matches_mutating_policy_surface` for the machine-
//! visible, EXHAUSTIVELY-PARTITIONED backlog of mutating methods NOT yet migrated:
//! every name falls into either `JUSTIFIED_NA` (a real architectural reason — a
//! different commit protocol like the OCC/2PC `Txn*`/`Commit` family, a non-graph-
//! scoped store like `Blob*`/`Kv*`/`TsAppend`, a cluster-wide/cross-shard admin op,
//! a registry-lifecycle op, or a process-global registry) or `OPEN_NOT_JUSTIFIED`
//! (graph-scoped and structurally routable, just not wired yet — the RDF triple
//! family and the `Sql`/`CypherQuery`/`GraphQl` runtime-conditional family) — an
//! undocumented name is a hard test failure, never a silent skip.
//!
//! `src/server/dispatch.rs` wires `handlers::graph_ops::try_handle_gateway` in
//! AHEAD of both the dispatch-shell write-coalescer branch (`try_coalesce_write`)
//! and the legacy WAL/CDC tail for exactly the routed set, so there is never a
//! double-apply: a routed method never reaches `dispatch::try_coalesce_write`, the
//! old direct-`eg-core`-call arms in `graph_ops::try_handle`/`mining::try_handle`/
//! `graphlearn::try_handle` (now `unreachable!()` there), or the dispatch shell's
//! own `record_method`/`cdc_pre`/`cdc_method` computation (all three are gated off
//! [`is_gateway_routed`] there). The coalescer BATCHING itself is not lost, though
//! — the gateway re-enters the SAME per-graph writer from INSIDE `commit_mutation`
//! (L18, see [`try_coalesce_apply`]), so a routed structural write still batches.
//!
//! ## What this gateway does NOT reimplement
//!
//! Audit-chain emission (the tamper-evident hash chain, `audit.rs` +
//! `redb_store::append_audit_entry`) is stateful PER GRAPH (each entry chains off
//! the previous one's hash) and already lives inside the durable-commit path
//! (`PersistenceBackend::record`/`record_durable` → the redb backend). Reimplementing
//! that chaining here would risk a second, diverging chain. Instead,
//! `commit_mutation` DELEGATES to the existing `record`/`record_durable` call for
//! durability (which — for a redb backend — appends the audit entry atomically with
//! the data row when `audit::audit_line(method)` resolves), and `plan.audited`
//! (sourced from `eg_capabilities::policy`) is the assertable, tested FACT that this
//! delegation actually happens for the methods policy says should be audited (see
//! `tests::routed_mutation_produces_one_wal_record_one_audit_entry_and_one_cdc_event`,
//! which uses a REAL `RedbBackend` and reads the audit chain back).
//!
//! The per-graph write-coalescer's batching optimization is NO LONGER bypassed for
//! the routed set (L18/EG-P0-6): [`commit_mutation`] step 4 routes a coalescable
//! routed mutation (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`) through the SAME
//! `WriteCoalescerRegistry::writer_for` path the legacy `dispatch::try_coalesce_write`
//! uses (see [`try_coalesce_apply`]), so the hot-path structural writes batch again
//! (`stats().ops()` counts them) while WAL/audit/CDC ordering is preserved. The
//! non-coalescable routed memory ops (`CreateSummaryNode`/`Consolidate`/`Reinforce`)
//! keep the inline `apply`. The Raft consensus barrier / cross-region replica log are
//! still untouched (both already return from `dispatch_graph_op` before this gateway
//! is reached, or are simply not yet wired into it) — see the module docs above and
//! the workstream report for the full backlog.

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
/// complement (every OTHER mutating method, per `eg_capabilities::ALL_METHODS`) —
/// that complement IS the EG-P0-2 rollout backlog.
pub const GATEWAY_ROUTED: &[&str] = &[
    "AddNode",
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
    // `RunDatalogReasoning`/`reasoning` above. It never touches `GraphCore` at
    // all (it writes a process-global ICV guard policy keyed by graph name), but
    // still rides the ordinary graph-scoped dispatch chain, so it fits the
    // gateway's `(ctx, plan, method, apply)` shape with an `apply` that ignores
    // `core` entirely.
    "IcvConfigure",
    "PruneByLifecycle",
    "BatchUpdate",
    "ParseRepository",
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
    // dispatch site because its durable write also touches the optional
    // `rdf-redb` lossless quad store on `state`. Behind `feature = "rdf"`. ──
    "AddTriples",
    "RemoveTriples",
    "DropNamedGraph",
];

/// Extract a `Method` variant's name as a `&'static str`, covering exactly
/// [`GATEWAY_ROUTED`] (the only names this module's logic branches on) plus a
/// catch-all `"other"` for every non-routed variant. NOT a general-purpose
/// reflection helper — deliberately narrow to this workstream's routed set.
pub fn method_variant_name(m: &Method) -> &'static str {
    match m {
        Method::AddNode { .. } => "AddNode",
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
        Method::ParseRepository { .. } => "ParseRepository",
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
        _ => "other",
    }
}

/// Is `method` one of [`GATEWAY_ROUTED`]? `dispatch_graph_op` uses this to (a)
/// route it to `handlers::graph_ops::try_handle_gateway` AHEAD of the write-
/// coalescer, and (b) suppress its own legacy `record_method`/`cdc_pre`/
/// `cdc_method` computation for the SAME method, so durability/audit/CDC happen
/// exactly once, from `commit_mutation`, never from both places.
pub fn is_gateway_routed(m: &Method) -> bool {
    GATEWAY_ROUTED.contains(&method_variant_name(m))
}

/// Everything [`commit_mutation`] needs beyond the plan + the apply closure.
/// Borrowed, not owned — built fresh per request from already-resolved
/// `dispatch_graph_op` locals (the registry lock is never re-acquired here).
pub struct MutationCtx<'a> {
    pub req_id: u64,
    pub caller: Option<&'a str>,
    pub graph_name: &'a str,
    pub graph_type: GraphType,
    pub owner: Option<&'a str>,
    pub isolation: &'a IsolationLayer,
    pub core: &'a Arc<GraphCore>,
    pub persistence: Option<&'a Arc<dyn PersistenceBackend>>,
    pub redb_authoritative: bool,
    #[cfg(feature = "streaming")]
    pub cdc: Option<&'a Arc<crate::server::cdc::CdcHub>>,
    /// Per-graph write-coalescer registry (CONCEPT:EG-KG.sharding.per-graph-write-coalescer, L18).
    /// When present + enabled, a coalescable routed mutation
    /// (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`) has its in-memory apply
    /// BATCHED onto this graph's single-writer queue instead of taking the topology
    /// lock itself, exactly like the legacy `dispatch::try_coalesce_write` path — so
    /// routing a hot-path write through the gateway no longer loses write-batching.
    /// `None` (or a disabled/non-coalescable method) ⇒ the `apply` closure runs
    /// inline, unchanged. See [`commit_mutation`] step 4.
    pub write_coalescer: Option<&'a Arc<crate::write_coalescer::WriteCoalescerRegistry>>,
}

/// Bounded process-global idempotency-replay cache (CONCEPT:EG-P0-2). Keyed by a
/// deterministic `(method, graph, identity-args)` string (see [`idempotency_key`]).
///
/// Bounded, not TTL/LRU, for this increment: past [`MAX_IDEMPOTENCY_ENTRIES`] a new
/// key is simply not cached (fails OPEN — the mutation still applies correctly, it
/// just stops being replay-dedup-eligible until the process restarts or an entry
/// frees up) rather than evicting an arbitrary entry and risking a wrong cache hit
/// for a DIFFERENT request that happens to reuse a key. A real bounded LRU/TTL
/// policy is a documented follow-up, not required to prove the gateway pattern.
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
    match method {
        Method::RemoveNode { node_id } => format!("RemoveNode|{graph_name}|{node_id}"),
        Method::RemoveEdge {
            source_id,
            target_id,
        } => format!("RemoveEdge|{graph_name}|{source_id}|{target_id}"),
        other => format!("{other:?}|{graph_name}"),
    }
}

/// Try to apply a coalescable routed mutation THROUGH this graph's write-coalescer
/// (CONCEPT:EG-KG.sharding.per-graph-write-coalescer, L18), returning `Some(Response)` when it was coalesced
/// (the outcome mapped to the SAME `Response` the gateway's inline `apply` closure
/// would produce) or `None` when it must fall back to the inline closure.
///
/// `None` is returned when ANY of: no coalescer is configured on `ctx`; the
/// coalescer is disabled (`EPISTEMIC_GRAPH_WRITE_COALESCE=0`); or `method` is not
/// one of the four coalescable structural writes (`AddNode`/`RemoveNode`/`AddEdge`/
/// `RemoveEdge`) — the other routed methods (`CreateSummaryNode`/`Consolidate`/
/// `Reinforce`) are multi-field memory ops the coalescer does not model, so they
/// keep the inline closure. Mirrors `dispatch::try_coalesce_write` exactly (enqueue
/// with a per-op linger `Instant`, fall back to `apply_one_inline` on a full/closed
/// queue, await the writer's outcome), so a routed write batches identically to how
/// the same method batched on the legacy path before EG-P0-2 routed it.
async fn try_coalesce_apply(ctx: &MutationCtx<'_>, method: &Method) -> Option<Response> {
    use crate::write_coalescer::{WriteOp, WriteOutcome};
    use tokio::sync::oneshot;

    let coalescer = ctx.write_coalescer?;
    if !coalescer.enabled() {
        return None;
    }

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

    let writer = coalescer.writer_for(ctx.graph_name, ctx.core)?;
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

    // 2. Idempotency-replay dedup -- ONLY for methods policy marks idempotent. A
    // byte-identical replay short-circuits BEFORE touching storage: no re-apply, no
    // second WAL record, no second audit-chain entry, no duplicate CDC event.
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
) -> Response {
    if response.error.is_some() {
        return response;
    }

    // 5. Mark the graph dirty (checkpoint scheduling). Idempotent flag set.
    if plan.mutates {
        ctx.core.mark_dirty();
    }

    // 6. Durable commit -- reuses the EXISTING `PersistenceBackend` plumbing.
    if !matches!(plan.durability_domain, DurabilityDomain::None) {
        if let Some(p) = ctx.persistence {
            let fname = crate::persist::sanitize(ctx.graph_name);
            if ctx.redb_authoritative {
                if let Err(e) = p.record_durable(&fname, method).await {
                    return Response::err(
                        ctx.req_id,
                        format!("durable commit failed (write not acknowledged): {e}"),
                    );
                }
            } else {
                p.record(&fname, method);
            }
        }
    }

    // 7. CDC emit -- AFTER the durable commit succeeded, mirroring the legacy
    // shell's ordering.
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

    // 7.5. Truth-maintenance change-feed hook (CONCEPT:EG-KG.epistemic.truth-maintenance —
    // EPI-P3-2's server-side wiring, `src/server/tms_hook.rs`): same "after the durable
    // commit succeeded" ordering as CDC above. Independent of `streaming` -- this hook
    // has no CDC dependency (see `eg_epistemic::recompute`'s module docs). No-op for any
    // method `tms_hook::change_event_for_method` does not map (the overwhelming
    // majority), so this costs one cheap match per gateway-routed commit.
    #[cfg(feature = "epistemic-tms")]
    {
        crate::server::tms_hook::notify(method);
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
/// happens — exactly like the legacy per-handler + dispatch-shell path.
pub async fn commit_mutation<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    let prep = match commit_prepare(ctx, plan, method) {
        Ok(p) => p,
        Err(short_circuit) => return short_circuit,
    };

    // 4. Apply the actual eg-core mutation.
    //
    // L18: a coalescable routed mutation (`AddNode`/`RemoveNode`/`AddEdge`/
    // `RemoveEdge`) is BATCHED onto this graph's single-writer coalescer queue when
    // one is configured + enabled — the SAME `writer_for` path the legacy
    // `dispatch::try_coalesce_write` uses. Anything else — a non-coalescable routed
    // op, a disabled/absent coalescer — falls back to the inline `apply` closure,
    // unchanged. Steps 5-8 run per-op AFTER, so ordering is intact.
    let response = match try_coalesce_apply(ctx, method).await {
        Some(resp) => resp,
        None => match apply(ctx.core) {
            Ok(payload) => Response::ok(ctx.req_id, payload),
            Err(e) => Response::err(ctx.req_id, e),
        },
    };

    commit_finalize(ctx, plan, method, response, prep).await
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
/// via `handlers::rdf::try_handle`, whose durable write ALSO touches the optional
/// `rdf-redb` lossless quad store on `state`). The `apply` closure captures whatever
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
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<ResultPayload, String>>,
{
    if !mutates_now {
        // Read-only path: Read-ACL only, apply, and NOTHING durable/audited/
        // CDC-emitted/cached — exactly what any non-mutating graph read does.
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
        return match apply().await {
            Ok(payload) => Response::ok(ctx.req_id, payload),
            Err(e) => Response::err(ctx.req_id, e),
        };
    }

    // Mutating path: the SAME steps-1-3 / steps-5-8 contract as `commit_mutation`,
    // with the async apply in between (no coalescer — the query/RDF surfaces are not
    // coalescable single-op structural writes).
    let prep = match commit_prepare(ctx, plan, method) {
        Ok(p) => p,
        Err(short_circuit) => return short_circuit,
    };
    let response = match apply().await {
        Ok(payload) => Response::ok(ctx.req_id, payload),
        Err(e) => Response::err(ctx.req_id, e),
    };
    commit_finalize(ctx, plan, method, response, prep).await
}

/// Is `m` one of the query-surface gateway methods routed via
/// [`commit_conditional_mutation_async`] at the query dispatch site (they need
/// `state`/`rls` the graph-ops `try_handle_gateway` entry point does not carry)?
/// Part of [`GATEWAY_ROUTED`], but handed back by `try_handle_gateway` so it reaches
/// its dedicated wrap in `dispatch.rs`.
pub fn is_query_gateway_method(m: &Method) -> bool {
    matches!(method_variant_name(m), "Sql" | "CypherQuery" | "GraphQl")
}

/// Is `m` one of the native-RDF gateway methods routed via
/// [`commit_conditional_mutation_async`] at the rdf dispatch site (their durable
/// write also touches the optional `rdf-redb` quad store on `state`)? Part of
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
    use crate::protocol::GraphType;
    #[cfg(feature = "redb")]
    use crate::server::persistence::redb_backend::RedbBackend;
    #[cfg(feature = "redb")]
    use crate::wal_service::FsyncPolicy;

    fn isolation_no_rules() -> IsolationLayer {
        IsolationLayer::new()
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

    /// (a) A GATEWAY_ROUTED, audited+CDC-emitting mutation (`AddNode`) produces a
    /// WAL/redb record + an audit-chain entry + a CDC event -- all from the ONE
    /// `commit_mutation` call, against a REAL `RedbBackend` (not a mock), so this
    /// is exercising the actual durable-commit + audit-chain-append path, not just
    /// asserting the gateway's own bookkeeping.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn routed_mutation_produces_one_wal_record_one_audit_entry_and_one_cdc_event() {
        let dir = temp_dir("wal-audit-cdc");
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, FsyncPolicy::Each, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        let isolation = isolation_no_rules();
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
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            // Authoritative (commit-before-ack) so `commit_mutation` AWAITS the
            // durable group-commit before returning -- the write (and its audit-
            // chain entry) is provably on disk when we read it back below, with no
            // race against the off-reactor writer that the fire-and-forget
            // `record()` path would introduce.
            redb_authoritative: true,
            cdc: Some(&cdc_hub),
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

        // WAL/redb: the write actually landed durably.
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
        let backend = RedbBackend::open(dir_s, FsyncPolicy::Each, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        core.add_node("n1".to_string(), Vec::new());
        let isolation = isolation_no_rules();
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
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            // Authoritative so `commit_mutation` AWAITS the durable group-commit
            // (which appends the audit-chain entry) before returning, so the
            // read-back below never races the off-reactor writer.
            redb_authoritative: true,
            cdc: Some(&cdc_hub),
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

    /// (a5) L11 rollout batch 2, family representative: a message-broker method
    /// (`DeclareExchange`, `DurabilityDomain::Outbox`) routed through the SAME
    /// `commit_mutation` gateway produces the policy-declared WAL/audit effect
    /// (audited, no CDC) against a REAL `RedbBackend` — proving the Outbox
    /// durability domain commits through the identical `record_durable` path as
    /// the GraphRedb-domain methods above (the backend does not branch on
    /// `DurabilityDomain`, only on whether it's `None`).
    #[cfg(all(feature = "redb", feature = "broker"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn broker_family_routed_mutation_is_audited_with_no_cdc() {
        let dir = temp_dir("broker-declare-exchange");
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, FsyncPolicy::Each, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        let isolation = isolation_no_rules();
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
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            redb_authoritative: true,
            cdc: Some(&cdc_hub),
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

    /// (a6) L11 rollout batch 2, family representative: a `DurabilityDomain::None`
    /// method (`TouchNodes` — a deliberately non-durable, non-audited maintenance
    /// op per `wal.rs`'s own doc comment) still applies its mutation and passes
    /// authz through the gateway, but produces NO WAL/redb record and NO audit
    /// entry — proving `commit_mutation` step 6 correctly no-ops persistence for
    /// the `None` domain rather than writing a phantom row.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn none_durability_routed_mutation_applies_but_is_not_persisted() {
        let dir = temp_dir("touch-nodes-none-durability");
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, FsyncPolicy::Each, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        core.add_node("n1".to_string(), Vec::new());
        let isolation = isolation_no_rules();
        let graph_name = "g-eg-p0-2-l11-none-durability-a";

        let method = Method::TouchNodes {
            node_ids: vec!["n1".to_string()],
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.mutates);
        assert_eq!(plan.durability_domain, DurabilityDomain::None);
        assert!(!plan.audited);
        assert!(!plan.emits_cdc);

        let ctx = MutationCtx {
            req_id: 11,
            caller: Some("system-agent"),
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            redb_authoritative: true,
            cdc: None,
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
        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok, "audit chain broke: {report:?}");
        assert_eq!(
            report.entries, 0,
            "TouchNodes is durability_domain::None -- no WAL/audit record should exist"
        );
    }

    /// (a7) L11 rollout batch 3, RUNTIME-CONDITIONAL family representative:
    /// `MineAssociate` proves `commit_conditional_mutation` resolves the
    /// durability/audit/authz gateway from the request's OWN `writeback` field,
    /// not from `policy()`'s conservative upper bound:
    ///   * `writeback: true`  -> Write access required, ONE audited WAL/redb entry.
    ///   * `writeback: false` -> Read access SUFFICES (an agent with only Read
    ///     succeeds), and NOTHING durable/audited is produced -- proving a
    ///     read-only call is never incorrectly gated behind Write or persisted as
    ///     a phantom mutation (the bug this function exists to prevent).
    #[cfg(all(feature = "redb", feature = "mining"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn mining_family_writeback_gates_durability_and_authz() {
        let dir = temp_dir("mine-associate-conditional");
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, FsyncPolicy::Each, 64).expect("open redb backend");
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
        let isolation = isolation_no_rules();

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
            caller: Some("reader"),
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            redb_authoritative: true,
            cdc: None,
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
            caller: Some("writer"),
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            redb_authoritative: true,
            cdc: None,
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
            "writeback:true must produce EXACTLY one audited WAL/redb record"
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
            graph_name: "agent:owner",
            graph_type: GraphType::Agent,
            owner: Some("owner"),
            isolation: &isolation,
            core: &core,
            persistence: None,
            redb_authoritative: false,
            cdc: None,
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
    #[tokio::test(flavor = "multi_thread")]
    async fn idempotent_replay_is_deduped_without_reapplying() {
        let core = Arc::new(GraphCore::new());
        core.add_node("n1".to_string(), Vec::new());
        let isolation = isolation_no_rules();
        let graph_name = "g-eg-p0-2-idem-unique-9f31";

        let method = Method::RemoveNode {
            node_id: "n1".to_string(),
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.idempotent, "RemoveNode is policy-idempotent");

        let ctx = MutationCtx {
            req_id: 4,
            caller: None,
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: None,
            redb_authoritative: false,
            cdc: None,
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
                mode: "off".into(),
                shapes: String::new(),
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
            Method::ParseRepository {
                root_path: "x".into(),
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
            Method::BrokerAckTag { delivery_tag: 0 },
            #[cfg(feature = "broker")]
            Method::BrokerNackTag {
                delivery_tag: 0,
                requeue: false,
                now_ms: 0,
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
    /// **JUSTIFIED_NA** -- genuinely does not fit `commit_mutation`'s
    /// `(ctx, plan, method, apply)` shape for a real, load-bearing architectural
    /// reason (a different commit protocol entirely, a non-graph-scoped store
    /// with no `GraphCore`/`graph_name` in scope, a cross-shard/cluster-wide op,
    /// or a registry-lifecycle op that runs before/across a single graph's core
    /// exists). Each entry cites the file/mechanism that already owns its
    /// authz+durability, so this is an audit trail, not an excuse.
    ///
    /// **OPEN_NOT_JUSTIFIED** -- IS graph-scoped and structurally routable, but
    /// genuinely NOT wired yet (no architectural blocker -- just not completed in
    /// this rollout increment). Kept SEPARATE from `JUSTIFIED_NA` on purpose: this
    /// is the honest remainder the task brief's "nothing silently deferred" rule
    /// is about. `RunRules` additionally flags a POSSIBLE pre-existing ledger
    /// inaccuracy (its handler looks read-only) that needs a human audit before
    /// it's safe to route either way.
    const JUSTIFIED_NA: &[(&str, &str)] = &[
        // ── OCC/2PC multi-op transaction protocol (src/server/handlers/txn.rs) ──
        // `Commit` applies a DYNAMIC list of already-staged Methods (each with its
        // own durable record), a multi-graph commit routes through 2PC
        // (`CrossShardCoordinator`), and a cross-modal commit lands
        // graph+vector+blob+series in ONE redb `WriteTransaction`
        // (`commit_cross_modal_txn`) -- none of that is a single `(ctx, plan,
        // method, apply)` call the gateway models. The Txn* STAGE ops don't even
        // mutate durable state at stage time (only `Commit` does); they self-route
        // in `dispatch.rs` BEFORE `dispatch_graph_op` entirely (resolved from
        // `open_txns`, not `req.graph`).
        ("TxnAddNode", "OCC txn stage/commit protocol (txn.rs) -- see JUSTIFIED_NA doc above"),
        ("TxnRemoveNode", "OCC txn stage/commit protocol (txn.rs) -- see JUSTIFIED_NA doc above"),
        ("TxnAddEdge", "OCC txn stage/commit protocol (txn.rs) -- see JUSTIFIED_NA doc above"),
        ("TxnRemoveEdge", "OCC txn stage/commit protocol (txn.rs) -- see JUSTIFIED_NA doc above"),
        ("TxnCas", "OCC txn stage/commit protocol (txn.rs) -- see JUSTIFIED_NA doc above"),
        ("TxnAddEmbedding", "OCC txn stage/commit protocol (txn.rs) -- see JUSTIFIED_NA doc above"),
        ("TxnBlobRef", "OCC txn stage/commit protocol (txn.rs) -- see JUSTIFIED_NA doc above"),
        ("TxnAddMeasurement", "OCC txn stage/commit protocol (txn.rs) -- see JUSTIFIED_NA doc above"),
        ("TxnAxiom", "OCC txn stage/commit protocol (txn.rs) -- see JUSTIFIED_NA doc above"),
        ("TxnConstruct", "OCC txn stage/commit protocol (txn.rs) -- see JUSTIFIED_NA doc above"),
        ("TxnPlanWriteback", "OCC txn stage/commit protocol (txn.rs) -- see JUSTIFIED_NA doc above"),
        ("TxnMaterializeBelief", "OCC txn stage/commit protocol (txn.rs) -- see JUSTIFIED_NA doc above"),
        ("Commit", "OCC txn commit point (applies a DYNAMIC staged write-set, possibly 2PC/cross-modal) -- txn.rs"),
        // ── Non-graph-scoped stores: no GraphCore/graph_name in scope at all;
        // each self-routes in dispatch.rs BEFORE the per-graph chain, exactly
        // like Txn*, with its OWN dedicated redb file + commit path. ──
        ("BlobBegin", "content-addressed CAS store (blob/store.rs, own blob.redb) -- not graph-scoped"),
        ("BlobChunkPut", "content-addressed CAS store (blob/store.rs, own blob.redb) -- not graph-scoped"),
        ("BlobCommit", "content-addressed CAS store (blob/store.rs, own blob.redb) -- not graph-scoped"),
        ("BlobGc", "content-addressed CAS store (blob/store.rs, own blob.redb) -- not graph-scoped"),
        ("BlobRef", "content-addressed CAS store (blob/store.rs, own blob.redb) -- not graph-scoped"),
        ("BlobUnref", "content-addressed CAS store (blob/store.rs, own blob.redb) -- not graph-scoped"),
        ("KvPut", "namespaced KV surface (kv.rs, own kv.redb) -- not graph-scoped"),
        ("KvDelete", "namespaced KV surface (kv.rs, own kv.redb) -- not graph-scoped"),
        ("KvCas", "namespaced KV surface (kv.rs, own kv.redb) -- not graph-scoped"),
        ("TsAppend", "time-series store (handlers/timeseries.rs, own series.redb) -- not graph-scoped"),
        #[cfg(feature = "jobs")]
        ("AnalyticsJob", "durable analytics-job plane (handlers/jobs.rs, own jobs.redb, CONCEPT:INT-P2-1) -- not graph-scoped"),
        ("ImportSqliteFile", "file-scoped: moves rows through a process-global user-table store, like Blob*/Kv*"),
        // ── Process-global registries on ServerState: durability_domain::None,
        // no GraphCore/graph_name; dispatched directly in the top-level match. ──
        ("CreateChannel", "process-global ChannelManager (ServerState::channels) -- no GraphCore/graph_name"),
        ("JoinChannel", "process-global ChannelManager (ServerState::channels) -- no GraphCore/graph_name"),
        ("LeaveChannel", "process-global ChannelManager (ServerState::channels) -- no GraphCore/graph_name"),
        ("CloseChannel", "process-global ChannelManager (ServerState::channels) -- no GraphCore/graph_name"),
        ("SendMessage", "process-global ChannelManager (ServerState::channels) -- no GraphCore/graph_name"),
        ("RegisterIdentity", "process-global isolation/identity registry (ServerState::isolation)"),
        ("RbacAdmin", "process-global RBAC policy registry (ServerState::isolation)"),
        ("RegisterForeignSource", "process-global query-federation registry (handlers/federation.rs)"),
        ("RegisterUdf", "process-global WASM UDF registry (handlers/wasm_udf.rs)"),
        ("RegisterContinuousQuery", "process-global CDC continuous-query registry (ServerState)"),
        ("DropContinuousQuery", "process-global CDC continuous-query registry (ServerState)"),
        ("RegisterTrigger", "process-global CDC trigger registry (ServerState)"),
        ("DropTrigger", "process-global CDC trigger registry (ServerState)"),
        ("CepSubscribe", "process-global CEP subscription registry (ServerState)"),
        ("CepUnsubscribe", "process-global CEP subscription registry (ServerState)"),
        // ── Cluster-wide / cross-shard admin ops: operate across the WHOLE
        // registry/cluster via the concrete redb backend directly
        // (handlers::admin::try_handle / handlers::dist_compute::try_handle), not
        // a single resolved graph's core. ──
        ("Reshard", "cluster-wide resharding admin (handlers/admin.rs, as_redb) -- not single-graph-scoped"),
        ("CatalogAssign", "cluster-wide tenant catalog admin (handlers/admin.rs, as_redb) -- not single-graph-scoped"),
        ("CatalogReassign", "cluster-wide tenant catalog admin (handlers/admin.rs, as_redb) -- not single-graph-scoped"),
        ("CatalogRemove", "cluster-wide tenant catalog admin (handlers/admin.rs, as_redb) -- not single-graph-scoped"),
        ("RebalanceExecute", "cluster-wide rebalance execution (handlers/admin.rs, as_redb) -- not single-graph-scoped"),
        ("Restore", "cluster-wide online restore/PITR (handlers/admin.rs, as_redb) -- not single-graph-scoped"),
        ("CreateMatView", "cross-shard materialized-view lifecycle (handlers/dist_compute.rs) -- not single-graph-scoped"),
        ("RefreshMatView", "cross-shard materialized-view lifecycle (handlers/dist_compute.rs) -- not single-graph-scoped"),
        ("PlanMatViewDefine", "cross-shard materialized-view lifecycle (handlers/dist_compute.rs) -- not single-graph-scoped"),
        ("PlanMatViewRefresh", "cross-shard materialized-view lifecycle (handlers/dist_compute.rs) -- not single-graph-scoped"),
        ("PlanMatViewDrop", "cross-shard materialized-view lifecycle (handlers/dist_compute.rs) -- not single-graph-scoped"),
        ("DistributedCompute", "cross-shard distributed compute (handlers/dist_compute.rs) -- not single-graph-scoped"),
        // ── Registry-lifecycle ops: run BEFORE/ACROSS a single graph's core
        // exists (or spans many), so there is no one EXISTING graph to route
        // "through". ──
        ("CreateGraph", "creates the registry entry itself -- no existing graph core to route through yet"),
        ("DeleteGraph", "evicts the registry entry itself -- no existing graph core to route through anymore"),
        ("MultiGraphBatchUpdate", "fans a batch out across MANY named graphs concurrently; routed before the single-graph path"),
        // ── Server lifecycle: TxnParticipation::None, not a graph mutation. ──
        ("Checkpoint", "server-lifecycle control-plane action, not a graph mutation"),
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

    /// Genuinely graph-scoped + structurally routable, but NOT wired yet -- no
    /// architectural blocker, just not done. As of the L11 batch-4 close-out this is
    /// **EMPTY**: all 7 former entries were resolved --
    ///   - `Sql`/`CypherQuery`/`GraphQl` are now routed via
    ///     `commit_conditional_mutation_async` at the query dispatch site (the async
    ///     twin built for exactly the `state`/`rls`-needing surfaces);
    ///   - `AddTriples`/`RemoveTriples`/`DropNamedGraph` are routed the same way at
    ///     the rdf dispatch site (its durable write threads `state` for the optional
    ///     `rdf-redb` quad store);
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
    /// into [`JUSTIFIED_NA`] or [`OPEN_NOT_JUSTIFIED`] -- an undocumented name in
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

        let justified_na: std::collections::HashMap<&'static str, &'static str> =
            JUSTIFIED_NA.iter().copied().collect();
        let open: std::collections::HashMap<&'static str, &'static str> =
            OPEN_NOT_JUSTIFIED.iter().copied().collect();

        // Every JUSTIFIED_NA / OPEN_NOT_JUSTIFIED entry must actually be a real,
        // currently-non-routed, mutating method name -- catches a stale doc entry
        // (a name that got routed, renamed, or was never mutating) as loudly as an
        // undocumented one.
        for name in justified_na.keys().chain(open.keys()) {
            assert!(
                not_yet_migrated.contains(name),
                "'{name}' is documented in JUSTIFIED_NA/OPEN_NOT_JUSTIFIED but is NOT in the \
                 current backlog (already routed, renamed, or never a mutating method?) -- stale doc entry"
            );
        }

        let undocumented: Vec<&&'static str> = not_yet_migrated
            .iter()
            .filter(|name| !justified_na.contains_key(*name) && !open.contains_key(*name))
            .collect();
        assert!(
            undocumented.is_empty(),
            "UNDOCUMENTED backlog entries (silently deferred, not allowed): {undocumented:?} -- \
             add each to JUSTIFIED_NA (real architectural reason) or OPEN_NOT_JUSTIFIED (honest \
             remainder) in server::mutation::tests"
        );
        assert_eq!(
            not_yet_migrated.len(),
            justified_na.len() + open.len(),
            "JUSTIFIED_NA + OPEN_NOT_JUSTIFIED must exactly partition the backlog (no overlap, no gaps)"
        );
        // L11 close-out invariant: the OPEN (routable-but-unrouted) bucket is EMPTY.
        // The entire backlog is now JUSTIFIED_NA -- every non-routed mutating method
        // has a real architectural reason (a different commit protocol, a non-graph-
        // scoped store, a cluster-wide op, a registry-lifecycle op, ...), none is a
        // "just didn't get to it" deferral.
        assert!(
            open.is_empty(),
            "OPEN_NOT_JUSTIFIED must be empty (L11 closed all routable methods); still open: {:?}",
            open.keys().collect::<Vec<_>>()
        );

        println!(
            "EG-P0-2/L11 gateway migration surface: {} routed / {} mutating total; \
             {} backlog = {} justified-N/A (documented architectural reasons) + \
             {} open-not-justified (MUST be 0)",
            routed_set.len(),
            all_mutating.len(),
            not_yet_migrated.len(),
            justified_na.len(),
            open.len(),
        );
    }
}
