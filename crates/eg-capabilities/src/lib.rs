//! MethodPolicy: the machine-checked capability ledger for every `eg_types::protocol::Method`
//! wire-protocol operation (CONCEPT:EG-P0-1).
//!
//! ## Why this exists
//!
//! The engine's capability truth was previously scattered across a hand-maintained
//! `docs/capabilities.md` plus independent classifiers that each answered a
//! DIFFERENT question about the same `Method` enum:
//!
//!   - `src/server/access.rs::requires_write`  -- does this mutate the target graph?
//!   - `src/mutation_apply.rs::is_durable_mutation` -- does this enter the authoritative
//!     mutation commit path?
//!   - `src/audit.rs::audit_line`               -- is this chained into the tamper-evident
//!     hash-chain audit log?
//!   - `src/server/cdc.rs::emit_for_method`     -- does this emit a Change-Data-Capture event?
//!
//! There is no single, machine-checked source of truth that ties these together, and no
//! guarantee that adding a new `Method` variant updates all four. This crate is that single
//! source of truth: [`MethodPolicy`] describes each variant's mutation/durability/authz/
//! idempotency/audit/CDC/transaction-participation profile, and [`policy`] is an EXHAUSTIVE
//! `match` over `Method` with **no wildcard arm** -- so adding a new variant to
//! `eg_types::protocol::Method` without declaring its policy here is a compile error, not a
//! silent gap.
//!
//! This crate defines the policy table, the exhaustiveness guarantee, the generated
//! Markdown ledger (see [`gen_ledger`]), and consistency tests against the remaining
//! classifiers. Served mutation planning consumes this policy directly; the snapshot
//! cross-checks remain as drift alarms for classifiers that have not yet been deleted.
//!
//! ## Crate shape
//!
//! A leaf crate: the ONLY dependency is `eg-types` (with every one of its optional features
//! turned on -- see `Cargo.toml` for why). It is not a dependency of the main
//! `epistemic-graph` package's default build; see the root `Cargo.toml`'s `members` comment.

use eg_types::protocol::{CypherMode, Method};

/// Where (if anywhere) a mutation's effect survives a process/host crash.
///
/// These are REAL, distinct persistence domains in the engine today, not a
/// theoretical taxonomy:
///   - [`GraphRedb`](DurabilityDomain::GraphRedb): authoritative graph-shard state,
///     committed through the universal MutationBatch kernel (`src/mutation_apply.rs`,
///     `src/redb_store.rs`).
///   - [`SeriesRedb`](DurabilityDomain::SeriesRedb): the timeseries store's own
///     `series.redb`, entirely separate from graph shards (`src/server/handlers/timeseries.rs`).
///   - [`KvRedb`](DurabilityDomain::KvRedb): the namespaced KV surface's own `kv.redb`,
///     committed with `redb::Durability::Immediate` (`src/server/kv.rs`).
///   - [`BlobRedb`](DurabilityDomain::BlobRedb): the content-addressed blob store's own
///     `blob.redb`, group-committed `Immediate` (`src/server/blob/store.rs`).
///   - [`Outbox`](DurabilityDomain::Outbox): the message-broker/stream control-graph state
///     (EG-275..284) -- committed alongside `GraphRedb`, but called out separately
///     because it is a semantically distinct
///     domain (queues/exchanges/streams, not node/edge content).
///   - [`JobsRedb`](DurabilityDomain::JobsRedb): the durable analytics-job plane's own
///     `jobs.redb` (CONCEPT:INT-P2-1), entirely separate from graph shards (`eg-jobs`,
///     wired by the facade's `src/server/handlers/jobs.rs`, feature `jobs`).
///   - [`ReasoningProjection`](DurabilityDomain::ReasoningProjection): the fsync'd,
///     per-graph incremental reasoning authority, advanced from the MutationBatch
///     outbox and fenced by its source graph watermark.
///   - [`ControlRedb`](DurabilityDomain::ControlRedb): native RBAC state or an opaque
///     prepared/committed coordinator receipt in the placement-group-owned redb
///     projection (directly owned by the process only in single-node serving).
///   - [`VolatileControl`](DurabilityDomain::VolatileControl): an explicitly ephemeral
///     process/session transition. It covers server lifecycle and in-memory transaction
///     staging only; it never acknowledges a user-data commit and is not crash-durable.
///   - [`None`](DurabilityDomain::None): no state transition; reserved for reads and
///     pure computation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurabilityDomain {
    GraphRedb,
    SeriesRedb,
    KvRedb,
    JobsRedb,
    ReasoningProjection,
    BlobRedb,
    Outbox,
    ControlRedb,
    VolatileControl,
    None,
}

/// The isolation/consistency contract a method's storage effect (if any) participates in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxnParticipation {
    /// A single, all-or-nothing engine-internal transition (one MutationBatch/redb
    /// commit or one in-memory swap) -- the common case for CRUD-shaped ops.
    Atomic,
    /// Multi-step or multi-party coordination with NO single-commit atomicity across the
    /// whole flow (the OCC `BeginTxn`/`Txn*`/`Commit`/`Rollback` family, multisig,
    /// CRDT reconcile, cross-shard/distributed compute, matview refresh, chunked blob
    /// upload, cluster resharding/rebalancing, online backup/restore).
    Saga,
    /// A read executed against a single consistent point-in-time view.
    Snapshot,
    /// No transactional participation at all -- pure compute with no graph interaction,
    /// or a server-lifecycle/control-plane action (ping, health, shutdown, metrics).
    None,
}

/// The declared capability profile of one `Method` variant.
///
/// Field-by-field:
///   - `mutates`: does invoking this method change persisted or in-memory server state?
///     For the handful of variants where the REAL answer is runtime-conditional (a
///     sub-operation, a `writeback: bool` field, or a parsed SQL/Cypher/GraphQL
///     statement), this is the conservative UPPER BOUND (`true` if it can ever mutate).
///   - `durability_domain`: see [`DurabilityDomain`].
///   - `authz_action`: the canonical `<primitive>:<verb>` scope consumed by the
///     protocol-policy inventory and dispatch authorization. Primitive-specific
///     enforcement can retain a narrower native check, but must not invent a second
///     scope string outside this ledger.
///   - `idempotent`: does invoking this method twice with identical arguments leave the
///     system in the same FINAL state as invoking it once? A judgment call (there is no
///     existing idempotency classifier in the codebase to check against).
///   - `audited`: is this method chained into the tamper-evident hash-chain audit log
///     (`src/audit.rs::audit_line`)? Cross-checked against the real function.
///   - `emits_cdc`: does this method emit a Change-Data-Capture event
///     (`src/server/cdc.rs::emit_for_method`)? Cross-checked against the real function.
///   - `txn_participation`: see [`TxnParticipation`]. A judgment call (there is no
///     existing classifier in the codebase to check against).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MethodPolicy {
    pub mutates: bool,
    pub durability_domain: DurabilityDomain,
    pub authz_action: &'static str,
    pub idempotent: bool,
    pub audited: bool,
    pub emits_cdc: bool,
    pub txn_participation: TxnParticipation,
}

impl MethodPolicy {
    /// True when this method's effect (if it mutates at all) is durable via ANY of the
    /// engine's real persistence domains -- a state-backed MutationBatch, a native
    /// redb store, or a durable control-plane coordinator receipt. `None` means the
    /// operation itself creates no durable state transition. `VolatileControl` is an
    /// explicit non-durable state change and therefore also returns false.
    pub const fn is_durable(&self) -> bool {
        !matches!(
            self.durability_domain,
            DurabilityDomain::None | DurabilityDomain::VolatileControl
        )
    }
}

/// Coarse access decision generated for every protocol method. Primitive-specific
/// authorizers can consume `primitive` + `verb`; graph dispatch can consume the
/// read/write projection without maintaining another hand-written method list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PolicyAccess {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProtocolPolicyInventoryEntry {
    pub method: &'static str,
    pub primitive: &'static str,
    pub verb: &'static str,
    pub access: PolicyAccess,
    pub policy: MethodPolicy,
}

/// Generate the protocol-method -> primitive-policy inventory from the exhaustive
/// `ALL_METHODS` table. There is no second allowlist: adding a Method without policy
/// still fails at `policy()`, and the inventory automatically gains the same row.
pub fn protocol_policy_inventory() -> Vec<ProtocolPolicyInventoryEntry> {
    ALL_METHODS
        .iter()
        .map(|(method, policy, _)| {
            let (primitive, verb) = policy
                .authz_action
                .split_once(':')
                .expect("MethodPolicy.authz_action must be primitive:verb");
            ProtocolPolicyInventoryEntry {
                method: *method,
                primitive,
                verb,
                access: if policy.mutates {
                    PolicyAccess::Write
                } else {
                    PolicyAccess::Read
                },
                policy: *policy,
            }
        })
        .collect()
}

/// The exhaustive, no-wildcard capability policy for one `Method` variant.
///
/// This is the core deliverable of EG-P0-1: the compiler enforces that EVERY variant of
/// `eg_types::protocol::Method` has a declared policy here. Delete a variant from `Method`
/// and this match still compiles (an arm becomes unreachable dead code, caught by a normal
/// `unreachable_patterns` lint on the enum's own definition site, not here). ADD a variant
/// to `Method` without adding it to one of the arms below, and this function fails to
/// compile with "non-exhaustive patterns" -- that failure IS the guarantee.
#[allow(clippy::match_like_matches_macro)]
pub fn policy(m: &Method) -> MethodPolicy {
    let result = match m {
        // AUTO-GENERATED reference: this match was authored by hand (internal workstream
        // EG-P0-1), grouping variants that share an identical MethodPolicy. Grouping is by
        // IDENTICAL policy value among declaration-order-adjacent variants only -- it is a
        // readability aid, not a semantic claim that ungrouped variants elsewhere differ.
        Method::AddNode { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "node:write",
            idempotent: false,
            audited: true,
            emits_cdc: true,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::CreateNodeIfAbsent { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "node:write",
            // The state transition is idempotent, but the boolean result is
            // intentionally state-dependent (winner=true, later callers=false).
            // It must not enter the cross-request response cache.
            idempotent: false,
            audited: true,
            emits_cdc: true,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::RemoveNode { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "node:write",
            idempotent: true,
            audited: true,
            emits_cdc: true,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::HasNode { .. }
        | Method::GetNodes
        | Method::GetNodesByLabel { .. }
        | Method::GetNodeProperties { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "node:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::CompareAndSetNodeFields { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "node:write",
            idempotent: true,
            audited: true,
            emits_cdc: true,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::ClaimNext { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "node:write",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::ClaimWorkItem { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "work:claim",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::RenewWorkItemLease { .. }
        | Method::CommitWorkItemResult { .. }
        | Method::CancelWorkItem { .. }
        | Method::DeferWorkItem { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "work:write",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::DeclareExchange { .. }
        | Method::DeleteExchange { .. }
        | Method::BindQueue { .. }
        | Method::UnbindQueue { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "broker:admin",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::Publish { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "broker:publish",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::DeclareQueue { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "broker:admin",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::PublishEx { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "broker:publish",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::BrokerConsume { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "broker:consume",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::BrokerAck { .. } | Method::BrokerReject { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "broker:ack",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::SweepExpired { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "broker:admin",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::StreamDeclare { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "stream:admin",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::StreamPublish { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "stream:write",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::StreamRead { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "stream:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::StreamTrim { .. } | Method::StreamCommitOffset { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "stream:admin",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::StreamCommittedOffset { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "stream:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::PublishConfirmed { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "broker:publish",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::PublishIdempotent { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "broker:publish",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::BrokerAckTag { .. }
        | Method::BrokerNackTag { .. }
        | Method::BrokerRenewTag { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::Outbox,
            authz_action: "broker:ack",
            // These owner-fenced results describe the CURRENT generation. Caching
            // a prior success across requests would incorrectly bless a stale tag.
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::CreateSummaryNode { .. }
        | Method::Consolidate { .. }
        | Method::Reinforce { .. }
        | Method::DecayNode { .. }
        | Method::DecayMemories { .. }
        | Method::EvictBelow { .. }
        | Method::Maintain { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "memory:write",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::SummaryChildren { .. } | Method::SummariesAtLevel { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "memory:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::AddSceneObject { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "scene:write",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::SetPose { .. } | Method::Reparent { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "scene:write",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::WorldTransform { .. } | Method::SceneChildren { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "scene:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::StartTrajectory { .. } | Method::AppendStep { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "memory:write",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::DiscountedReturn { .. } | Method::BestTrajectory { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "memory:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::GetNodePropertiesBatch { .. }
        | Method::HasNodesBatch { .. }
        | Method::NodeCount
        | Method::NodeIds => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "node:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::AddEdge { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "edge:write",
            idempotent: false,
            audited: true,
            emits_cdc: true,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::RemoveEdge { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "edge:write",
            idempotent: true,
            audited: true,
            emits_cdc: true,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::InvalidateEdge { .. } | Method::SupersedeEdge { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "edge:write",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::HasEdge { .. } | Method::GetEdges => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "edge:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::ClearGraph => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "graph:admin",
            idempotent: true,
            audited: true,
            emits_cdc: true,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::GetEdgeProperties { .. }
        | Method::GetEdgePropertiesBatch { .. }
        | Method::EdgeCount
        | Method::InDegree { .. }
        | Method::OutDegree { .. }
        | Method::GetPredecessors { .. }
        | Method::GetSuccessors { .. }
        | Method::GetNeighbors { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "edge:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::UnionGetNodeProperties { .. }
        | Method::UnionGetNodesByLabel { .. }
        | Method::UnionGetNeighbors { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "node:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::TopologicalSort
        | Method::FindCycle
        | Method::GetShortestPath { .. }
        | Method::GetBlastRadius { .. }
        | Method::DegreeCentrality { .. }
        | Method::DegreeCentralityAll
        | Method::BetweennessCentrality
        | Method::PageRank { .. }
        | Method::PersonalizedPageRank { .. }
        | Method::ConnectedComponents
        | Method::StronglyConnectedComponents
        | Method::MinimumSpanningTree
        | Method::CommunityDetection { .. }
        | Method::CommunityDetectEphemeral { .. }
        | Method::GraphColoring
        | Method::ComputeSimilarityEdges { .. }
        | Method::ResolveCandidates { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:graph-algo",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::PruneByLifecycle { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "node:admin",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::GetContextView { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "node:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::BatchUpdate { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "node:write",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::MultiGraphBatchUpdate { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "node:write",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::Metrics => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "service:control",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::EvictLRU { .. } | Method::DecaySweep { .. } | Method::TouchNodes { .. } => {
            MethodPolicy {
                mutates: true,
                durability_domain: DurabilityDomain::GraphRedb,
                authz_action: "node:admin",
                idempotent: false,
                audited: false,
                emits_cdc: false,
                txn_participation: TxnParticipation::Atomic,
            }
        }
        Method::ToMsgpack => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "graph:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::FromMsgpack { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "graph:admin",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::GetLedger => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "ledger:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::ClearLedger => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "ledger:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::ApplyLedger { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "ledger:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::AuditVerify => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "security:audit",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::GetSubgraph { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "node:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::Fork | Method::DiffAgainst { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "graph:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::CompactNodesByType { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "node:admin",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::RunDatalogReasoning { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "reasoning:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::ApplyChangeEnvelope { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "ingest:write",
            idempotent: true,
            audited: true,
            emits_cdc: true,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::GetChangeEnvelope { .. }
        | Method::GetContentVersion { .. }
        | Method::GetChangeCursor { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "ingest:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        #[cfg(feature = "modality-serving")]
        Method::ServedModality { op } if op.mutates() => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "modality:write",
            idempotent: op.is_idempotent_mutation(),
            audited: true,
            emits_cdc: true,
            txn_participation: TxnParticipation::Atomic,
        },
        #[cfg(feature = "modality-serving")]
        Method::ServedModality { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "modality:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::CreateGraph { .. } | Method::DeleteGraph { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "graph:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::ListGraphs => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "graph:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::Reshard { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "admin:cluster",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::CatalogAssign { .. }
        | Method::CatalogReassign { .. }
        | Method::CatalogRemove { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "admin:cluster",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::CatalogList | Method::RebalancePlan { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "admin:cluster-read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::RebalanceExecute { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "admin:cluster",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::PlacementRoute { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "admin:cluster-read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::Backup { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "admin:backup",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::Restore { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "admin:backup",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::CreateChannel { .. }
        | Method::JoinChannel { .. }
        | Method::LeaveChannel { .. }
        | Method::CloseChannel { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "channel:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::SendMessage { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "channel:write",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::GetChannelMessages { .. }
        | Method::ListChannels
        | Method::GetChannelMembers { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "channel:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::Ping | Method::Health => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "service:control",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::CancelRequest { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "service:control",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::Shutdown => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::VolatileControl,
            authz_action: "service:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::ResourceStats => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "service:control",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::Reconcile { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "graph:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::ApplyMutation { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "graph:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::Vf2SubgraphMatch { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:graph-algo",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::ParseFile { .. } | Method::ParseFiles { .. } | Method::IndexRepository { .. } => {
            MethodPolicy {
                mutates: false,
                durability_domain: DurabilityDomain::None,
                authz_action: "compute:parse",
                idempotent: true,
                audited: false,
                emits_cdc: false,
                txn_participation: TxnParticipation::None,
            }
        }
        Method::ObserveScreen { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:vision",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        // The canonical durable-mutation classifier covers embedding writes; the
        // ledger therefore assigns the authoritative graph state domain.
        Method::AddEmbedding { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "node:write",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::SemanticSearch { .. }
        | Method::Discover { .. }
        | Method::MatchOntologyTerms { .. }
        | Method::BatchL2Normalize { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:semantic",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::FinanceOptimizePortfolio { .. }
        | Method::FinanceRiskParity { .. }
        | Method::FinanceBlackLitterman { .. }
        | Method::FinanceEfficientFrontier { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:finance",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::DsLinearRegression { .. }
        | Method::DsKMeans { .. }
        | Method::DsPca { .. }
        | Method::DsComputeStats { .. }
        | Method::DsTrainTestSplit { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:datascience",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::DsFitEstimator { .. } | Method::DsPredictEstimator { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:datascience",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::DsSoftmax { .. }
        | Method::DsLogSoftmax { .. }
        | Method::DsCrossEntropy { .. }
        | Method::DsDpoLoss { .. }
        | Method::DsGrpoSurrogate { .. }
        | Method::DsKlDivergence { .. }
        | Method::DsAdamStep { .. }
        | Method::DsSgdStep { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:datascience",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::FinanceVar { .. }
        | Method::FinanceCvar { .. }
        | Method::FinanceMaxDrawdown { .. }
        | Method::FinanceDrawdownSeries { .. }
        | Method::FinanceDownsideDeviation { .. }
        | Method::FinanceRiskMetrics { .. }
        | Method::FinanceMonteCarloVar { .. }
        | Method::FinanceStressTest { .. }
        | Method::FinanceDetectRegimes { .. }
        | Method::FinanceRollingZscore { .. }
        | Method::FinanceEwma { .. }
        | Method::FinanceSignalDecay { .. }
        | Method::FinanceCombineAlphas { .. }
        | Method::FinanceCrossSectionalRank { .. }
        | Method::FinanceMomentum { .. }
        | Method::FinanceMeanReversion { .. }
        | Method::FinanceInformationCoefficient { .. }
        | Method::FinanceTwap { .. }
        | Method::FinanceVwap { .. }
        | Method::FinanceMarketImpact { .. }
        | Method::FinancePairsTrading { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:finance",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::FinanceMatchOrders { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:finance",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::FinanceAvellanedaStoikov { .. }
        | Method::FinanceGltQuotes { .. }
        | Method::FinanceLogitQuotes { .. }
        | Method::FinanceGlostenMilgromSpread { .. }
        | Method::FinanceExpectedPnlRate { .. }
        | Method::FinanceBreakevenAlpha { .. }
        | Method::FinanceOfiSeries { .. }
        | Method::FinanceMicropriceSeries { .. }
        | Method::FinanceVpinPm { .. }
        | Method::FinanceHawkesMle { .. }
        | Method::FinanceHardimanBouchaud { .. }
        | Method::FinanceKyleLambda { .. }
        | Method::FinanceSurveillanceRisk { .. }
        | Method::FinanceKellyFraction { .. }
        | Method::FinanceBayesianKelly { .. }
        | Method::FinancePosteriorCredibleInterval { .. }
        | Method::FinancePurgedCpcv { .. }
        | Method::FinanceDeflatedSharpe { .. }
        | Method::FinanceProbabilityBacktestOverfit { .. }
        | Method::FinanceDieboldMariano { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:finance",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::FinanceForensicReport { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:finance",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::FinanceKalmanFilter1d { .. }
        | Method::FinanceKalmanBeta { .. }
        | Method::FinanceKalmanVolatility { .. }
        | Method::FinanceAdfTest { .. }
        | Method::FinanceOuCalibrate { .. }
        | Method::FinanceOuOptimalThresholds { .. }
        | Method::FinanceMarkovTransitionMatrix { .. }
        | Method::FinanceOrderBookImbalance { .. }
        | Method::FinanceQueueImbalance { .. }
        | Method::FinanceRealizedVolTick { .. }
        | Method::FinanceSpreadReversion { .. }
        | Method::FinanceInformationRatio { .. }
        | Method::FinanceEffectiveIndependentN { .. }
        | Method::FinanceAlphaCombinationEngine { .. }
        | Method::FinanceBrierScore { .. }
        | Method::FinanceConvergenceGate { .. }
        | Method::FinanceEmpiricalKelly { .. }
        | Method::FinanceSabrImpliedVol { .. }
        | Method::FinanceSabrSmile { .. }
        | Method::FinanceSabrCalibrate { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "compute:finance",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::RegisterIdentity { .. } | Method::RbacAdmin { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "security:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::ApplyMultisigMutation { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "security:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        // CONCEPT:INT-P2-1 -- the durable analytics-job plane. `mutates: true` is the
        // conservative upper bound (like `RbacAdmin { op }` above): the real
        // per-op answer is runtime-conditional on `JobOp` (`Status` is a pure read;
        // `Submit`/`Cancel`/`Resume` mutate the job store). Own durability domain
        // (`JobsRedb`, its own `jobs.redb`) -- NOT `GraphRedb`, so it is excluded from
        // the graph mutation-applier cross-check the same way `SeriesRedb`/`KvRedb`/
        // `BlobRedb` are (see the consistency test). Not audited/CDC-emitted for the same reason
        // `TsAppend`/`Kv*` aren't: it self-manages its own durability out of band of
        // the graph tamper-evident chain, and self-routes before `dispatch_graph_op`
        // (see `mutation.rs`'s native-coordinator inventory), so it never reaches
        // `mutation_apply::apply`/`audit.rs::audit_line` at all.
        #[cfg(feature = "jobs")]
        Method::AnalyticsJob { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::JobsRedb,
            authz_action: "jobs:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::Sql { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "query:sql",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::CypherQuery { mode, .. } => match mode {
            CypherMode::Read => MethodPolicy {
                mutates: false,
                durability_domain: DurabilityDomain::None,
                authz_action: "query:cypher",
                idempotent: true,
                audited: false,
                emits_cdc: false,
                txn_participation: TxnParticipation::Snapshot,
            },
            CypherMode::Write => MethodPolicy {
                mutates: true,
                durability_domain: DurabilityDomain::GraphRedb,
                authz_action: "query:cypher",
                idempotent: false,
                audited: true,
                emits_cdc: false,
                txn_participation: TxnParticipation::Atomic,
            },
        },
        Method::GraphQl { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "query:graphql",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        #[cfg(feature = "knowledge-batch")]
        Method::KnowledgeStream { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "query:stream",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::UnifiedQuery { .. } | Method::UnifiedQueryText { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "query:unified",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::ExplainPlan { .. }
        | Method::ExplainProvenance { .. }
        | Method::ExplainProvenanceByIds { .. }
        | Method::ExplainPolicy { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "explain:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::ExplainBelief { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "explain:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        // L53 (EPI-P3-5 UQL wiring): the acceptance-capstone + temporal-diff read ops.
        // Both read-only, no durability, no audit/CDC — same profile as `ExplainBelief`.
        Method::EpistemicStatus { .. } | Method::WhatChanged { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "explain:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::RecomputeMaterialization { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ReasoningProjection,
            authz_action: "reasoning:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::MaterializationStatus { .. } | Method::StaleMaterializations => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "explain:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        // EPI-P3-7 (gap-fill): standalone Dung argumentation conflict resolution. Builds a
        // `BeliefGraph` off the caller's read-only `GraphView` snapshot and runs
        // grounded/preferred/stable extension computation -- read-only, no durability, no
        // audit/CDC, same profile as `EpistemicStatus`/`ExplainBelief` above.
        Method::ResolveConflict { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "explain:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        // CONCEPT:EG-X1 + EPI-P3-3/P3-6 (facade wiring): multimodal-citation resolution +
        // calibrated causal reasoning (intervention/observation/counterfactual) +
        // provenance-aware retrieval ranking. All four read-only, no durability, no
        // audit/CDC — same profile as `ExplainBelief` above (`ExplainEvidence` walks a
        // `BeliefGraph`; `CausalEstimate`/`CausalCounterfactual`/`RankByProvenance` are
        // pure functions over request-carried inputs, needing no graph snapshot at all).
        Method::ExplainEvidence { .. }
        | Method::CausalEstimate { .. }
        | Method::CausalCounterfactual { .. }
        | Method::RankByProvenance { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "explain:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::NlQuery { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "query:nl",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::RegisterForeignSource { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "federation:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::RegisterUdf { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "udf:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::RunUdf { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "udf:exec",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::DistributedCompute { .. } => MethodPolicy {
            // Pregel/GAS execution returns a computed result and performs no
            // writeback. Materialization is represented by the distinct
            // Create*/Refresh* methods below.
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "distcompute:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::CreateMatView { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "matview:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::GetMatView { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "matview:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::RefreshMatView { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "matview:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::PlanMatViewDefine { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "matview:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::PlanMatViewGet { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "matview:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::PlanMatViewRefresh { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "matview:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::PlanMatViewDrop { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "matview:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::BeginTxn { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "txn:control",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::TxnAddNode { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "txn:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::TxnRemoveNode { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "txn:write",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::TxnAddEdge { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "txn:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::TxnRemoveEdge { .. } | Method::TxnCas { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "txn:write",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::TxnAddEmbedding { .. }
        | Method::TxnBlobRef { .. }
        | Method::TxnAddMeasurement { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "txn:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::TxnAxiom { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "txn:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::TxnConstruct { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "txn:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::TxnPlanWriteback { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "txn:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::TxnMaterializeBelief { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "txn:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::TxnUnifiedQuery { .. } | Method::TxnUnifiedQueryText { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "txn:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::Commit { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "txn:control",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::Rollback { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "txn:control",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::TsAppend { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::SeriesRedb,
            authz_action: "timeseries:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::TsRange { .. }
        | Method::TsAsofJoin { .. }
        | Method::TsWindow { .. }
        | Method::TsGapFill { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "timeseries:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::BlobBegin { .. } | Method::BlobChunkPut { .. } | Method::BlobCommit { .. } => {
            MethodPolicy {
                mutates: true,
                durability_domain: DurabilityDomain::BlobRedb,
                authz_action: "blob:write",
                idempotent: false,
                audited: false,
                emits_cdc: false,
                txn_participation: TxnParticipation::Saga,
            }
        }
        Method::BlobFetchBegin { .. }
        | Method::BlobChunkGet { .. }
        | Method::BlobFetchEnd { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "blob:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::BlobRef { .. } | Method::BlobUnref { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::BlobRedb,
            authz_action: "blob:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::BlobGc => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::BlobRedb,
            authz_action: "blob:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::KvGet { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "kv:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::KvPut { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::KvRedb,
            authz_action: "kv:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::KvDelete { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::KvRedb,
            authz_action: "kv:write",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::KvScan { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "kv:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::KvCas { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::KvRedb,
            authz_action: "kv:write",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::ImportSqliteFile { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "admin:sqlite-file",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::ExportSqliteFile { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "admin:sqlite-file",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::AddTriples { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "rdf:write",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::GetRdf => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "rdf:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::RemoveTriples { .. } | Method::DropNamedGraph => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "rdf:write",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::Sparql { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "sparql:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::SparqlVirtual { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "sparql:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::OwlReason { .. }
        | Method::OwlReasonDistributed { .. }
        | Method::OwlExplain { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "owl:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        // EG-P0-2/L11: RunRules is READ-ONLY (unlike its sibling RunDatalogReasoning, which
        // materialises inferred edges in-place). `handle_run_rules` (src/server/handlers/rdf.rs)
        // runs `eg_rdf::rules::run_rule_reasoning_on_view(view: &GraphView, ..)` over an OFF-LOCK
        // `analysis_snapshot()` and RETURNS the inferred triples -- it never calls add_node/
        // add_edge/any writeback. The earlier `mutates: true` was a semantic guess that the
        // L11 handler audit disproved; corrected to a read (matches access.rs, which never
        // classified it as a write).
        Method::RunRules { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "reasoning:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::ShaclValidate { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "validation:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::IcvConfigure { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "security:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::ShexValidate { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "validation:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::CdcRead { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "cdc:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::RegisterContinuousQuery { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "cdc:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::ReadContinuousQuery { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "cdc:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::DropContinuousQuery { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "cdc:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::Watch { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "cdc:read",
            idempotent: false,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::None,
        },
        Method::RegisterTrigger { .. } | Method::DropTrigger { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "cdc:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::ListTriggers { .. } | Method::FiredTriggers { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "cdc:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::CepSubscribe { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "cep:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::CepPoll { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "cep:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::CepUnsubscribe { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "cep:admin",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Saga,
        },
        Method::MineAssociate { .. } | Method::MineCluster { .. } | Method::MineAnomaly { .. } => {
            MethodPolicy {
                mutates: true,
                durability_domain: DurabilityDomain::GraphRedb,
                authz_action: "mining:write",
                idempotent: false,
                audited: true,
                emits_cdc: false,
                txn_participation: TxnParticipation::Atomic,
            }
        }
        Method::MineClassifyFit { .. } => MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "mining:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        },
        Method::MineClassifyPredict { .. } | Method::MineReduce { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "mining:write",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::GraphLearnFit { .. } | Method::GraphLearnPredict { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "graphlearn:write",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        // The canonical durable-mutation classifier covers the writeback=true cases
        // for these four methods, so the ledger assigns the graph state domain.
        Method::MineSequence { .. }
        | Method::MineForecast { .. }
        | Method::MineText { .. }
        | Method::MineSubgraph { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "mining:write",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
        Method::MineEntityResolve { .. }
        | Method::MineCausalImpact { .. }
        | Method::MineProcess { .. }
        | Method::MineRootCause { .. }
        | Method::MineRiskPropagation { .. }
        | Method::MineOntologyGap { .. }
        | Method::MineRetrievalQuality { .. }
        | Method::MineCommunity { .. } => MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "mining:write",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        },
    };
    assert!(
        !result.mutates || !matches!(result.durability_domain, DurabilityDomain::None),
        "a mutating Method must name its durable or volatile state domain"
    );
    if matches!(result.durability_domain, DurabilityDomain::VolatileControl) {
        assert!(
            matches!(
                m,
                Method::Shutdown
                    | Method::TxnAddNode { .. }
                    | Method::TxnRemoveNode { .. }
                    | Method::TxnAddEdge { .. }
                    | Method::TxnRemoveEdge { .. }
                    | Method::TxnCas { .. }
                    | Method::TxnAddEmbedding { .. }
                    | Method::TxnBlobRef { .. }
                    | Method::TxnAddMeasurement { .. }
                    | Method::TxnAxiom { .. }
                    | Method::TxnConstruct { .. }
                    | Method::TxnPlanWriteback { .. }
                    | Method::TxnMaterializeBelief { .. }
            ),
            "VolatileControl is restricted to process lifecycle and transaction staging"
        );
    }
    result
}

/// `(variant name, policy, note)` for every `Method` variant, in the SAME declaration
/// order as `eg_types::protocol::Method` (mirrors `crates/eg-types/src/protocol.rs`).
/// `note` is a non-empty, human-readable explanation whenever this variant's policy is a
/// documented judgment call or a known divergence from an existing classifier; empty
/// otherwise. Used by [`gen_ledger`] and by the consistency test (`tests/consistency.rs`).
pub const ALL_METHODS: &[(&str, MethodPolicy, &str)] = &[
        ("AddNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
        ("CreateNodeIfAbsent", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "atomic create returns true only to the inserting writer, so its result is not cross-request cacheable"),
        ("RemoveNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
        ("HasNode", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetNodes", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetNodesByLabel", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetNodeProperties", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("CompareAndSetNodeFields", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
        ("ClaimNext", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("DeclareExchange", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("DeleteExchange", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("BindQueue", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("UnbindQueue", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("Publish", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)"),
        ("DeclareQueue", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("PublishEx", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)"),
        ("BrokerConsume", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:consume", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("BrokerAck", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("BrokerReject", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("ClaimWorkItem", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:claim", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "engine-native tenant/fair WorkItem lease claim"),
        ("RenewWorkItemLease", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "lease epoch and fencing token are validated atomically"),
        ("CommitWorkItemResult", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "terminal result references and outbox commit atomically"),
        ("CancelWorkItem", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "pending cancellation never steals an active lease"),
        ("DeferWorkItem", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "fenced lease release schedules retry without consuming an attempt"),
        ("SweepExpired", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("StreamDeclare", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("StreamPublish", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("StreamRead", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "stream:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("StreamTrim", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("StreamCommitOffset", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("StreamCommittedOffset", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "stream:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("PublishConfirmed", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)"),
        ("PublishIdempotent", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)"),
        ("BrokerAckTag", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "current-generation result must not be replay-cached across requests"),
        ("BrokerNackTag", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "current-generation result must not be replay-cached across requests"),
        ("BrokerRenewTag", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "current-generation result must not be replay-cached across requests"),
        ("CreateSummaryNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("Consolidate", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("Reinforce", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("DecayNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("DecayMemories", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("EvictBelow", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("Maintain", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("SummaryChildren", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "memory:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("SummariesAtLevel", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "memory:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("AddSceneObject", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "scene:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("SetPose", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "scene:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("Reparent", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "scene:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("WorldTransform", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "scene:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("SceneChildren", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "scene:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("StartTrajectory", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("AppendStep", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("DiscountedReturn", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "memory:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("BestTrajectory", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "memory:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetNodePropertiesBatch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("HasNodesBatch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("NodeCount", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("NodeIds", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("AddEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "edge:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
        ("RemoveEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "edge:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
        ("InvalidateEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "edge:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("SupersedeEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "edge:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("HasEdge", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetEdges", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ClearGraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:admin", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
        ("GetEdgeProperties", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetEdgePropertiesBatch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("EdgeCount", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("InDegree", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("OutDegree", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetPredecessors", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetSuccessors", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetNeighbors", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("UnionGetNodeProperties", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("UnionGetNodesByLabel", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("UnionGetNeighbors", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("TopologicalSort", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("FindCycle", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetShortestPath", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetBlastRadius", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("DegreeCentrality", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("DegreeCentralityAll", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("BetweennessCentrality", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("PageRank", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("PersonalizedPageRank", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ConnectedComponents", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("StronglyConnectedComponents", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("MinimumSpanningTree", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("CommunityDetection", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("CommunityDetectEphemeral", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GraphColoring", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ComputeSimilarityEdges", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ResolveCandidates", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("PruneByLifecycle", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch commits the resulting authoritative image"),
        ("GetContextView", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("BatchUpdate", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("MultiGraphBatchUpdate", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "node:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "durable parent coordinator with per-graph MutationBatch children"),
        ("Metrics", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("EvictLRU", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch commits the resulting authoritative image"),
        ("DecaySweep", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch commits the resulting authoritative image"),
        ("TouchNodes", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch commits the resulting authoritative image"),
        ("ToMsgpack", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("FromMsgpack", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch commits the imported authoritative image"),
        ("GetLedger", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "ledger:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ClearLedger", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "ledger:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch"),
        ("ApplyLedger", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "ledger:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch"),
        ("AuditVerify", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "security:audit", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetSubgraph", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("Fork", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "returns the forked snapshot to the caller; never registers/persists it server-side"),
        ("DiffAgainst", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("CompactNodesByType", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch"),
        ("RunDatalogReasoning", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "reasoning:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch commits inferred facts"),
        ("ApplyChangeEnvelope", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "ingest:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "Engine-native object/material/governance/version/cursor/outbox commit; verified context is mandatory"),
        ("GetChangeEnvelope", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "ingest:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "Verified tenant-scoped reconciliation read"),
        ("GetContentVersion", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "ingest:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "Typed content versions are never compared lexically"),
        ("GetChangeCursor", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "ingest:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "Typed source cursors are tenant/graph/partition scoped"),
        #[cfg(feature = "modality-serving")]
        ("ServedModality", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "modality:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "runtime-conditional: authority/query/events/capabilities are verified read snapshots; ingest/delete/cold/restore commit an encrypted state-backed MutationBatch"),
        ("CreateGraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "native lifecycle MutationBatch before registry publication"),
        ("DeleteGraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "native lifecycle MutationBatch before registry eviction"),
        ("ListGraphs", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("Reshard", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed admin MutationBatch saga"),
        ("CatalogAssign", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed admin MutationBatch saga"),
        ("CatalogReassign", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed admin MutationBatch saga"),
        ("CatalogRemove", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed admin MutationBatch saga"),
        ("CatalogList", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster-read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("RebalancePlan", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster-read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("RebalanceExecute", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed admin MutationBatch saga"),
        ("PlacementRoute", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster-read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "engine-authoritative complete route; single-node returns authoritative unplaced group 0/epoch 0, while clustered routing requires a live MultiRaft control leader"),
        ("Backup", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:backup", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "reads a consistent snapshot out to a bundle; does not mutate the live graph"),
        ("Restore", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:backup", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed admin MutationBatch saga"),
        ("CreateChannel", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch; message/member payloads stay out of the ledger"),
        ("JoinChannel", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
        ("LeaveChannel", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
        ("CloseChannel", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
        ("SendMessage", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "channel:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "request-scoped opaque session-control receipt prevents acknowledgement-lost duplicate sends"),
        ("GetChannelMessages", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "channel:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ListChannels", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "channel:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetChannelMembers", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "channel:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("Ping", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("Health", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("Shutdown", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::VolatileControl, authz_action: "service:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, "explicitly ephemeral process control; never acknowledges a user-data commit"),
        ("CancelRequest", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("ResourceStats", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("Reconcile", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "state-backed MutationBatch commits the merged image"),
        ("ApplyMutation", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch"),
        ("Vf2SubgraphMatch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ParseFile", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:parse", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("ParseFiles", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:parse", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("IndexRepository", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:parse", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("ObserveScreen", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:vision", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("AddEmbedding", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("SemanticSearch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("Discover", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("MatchOntologyTerms", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("BatchL2Normalize", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("FinanceOptimizePortfolio", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceRiskParity", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceBlackLitterman", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceEfficientFrontier", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsLinearRegression", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsKMeans", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsPca", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsComputeStats", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsTrainTestSplit", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsFitEstimator", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsPredictEstimator", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsSoftmax", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsLogSoftmax", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsCrossEntropy", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsDpoLoss", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsGrpoSurrogate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsKlDivergence", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsAdamStep", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("DsSgdStep", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceVar", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceCvar", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceMaxDrawdown", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceDrawdownSeries", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceDownsideDeviation", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceRiskMetrics", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceMonteCarloVar", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceStressTest", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceDetectRegimes", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceRollingZscore", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceEwma", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceSignalDecay", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceCombineAlphas", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceCrossSectionalRank", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceMomentum", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceMeanReversion", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceInformationCoefficient", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceTwap", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceVwap", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceMarketImpact", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinancePairsTrading", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceMatchOrders", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceAvellanedaStoikov", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceGltQuotes", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceLogitQuotes", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceGlostenMilgromSpread", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceExpectedPnlRate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceBreakevenAlpha", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceOfiSeries", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceMicropriceSeries", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceVpinPm", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceHawkesMle", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceHardimanBouchaud", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceKyleLambda", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceSurveillanceRisk", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceKellyFraction", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceBayesianKelly", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinancePosteriorCredibleInterval", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinancePurgedCpcv", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceDeflatedSharpe", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceProbabilityBacktestOverfit", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceDieboldMariano", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceForensicReport", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceKalmanFilter1d", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceKalmanBeta", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceKalmanVolatility", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceAdfTest", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceOuCalibrate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceOuOptimalThresholds", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceMarkovTransitionMatrix", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceOrderBookImbalance", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceQueueImbalance", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceRealizedVolTick", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceSpreadReversion", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceInformationRatio", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceEffectiveIndependentN", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceAlphaCombinationEngine", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceBrierScore", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceConvergenceGate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceEmpiricalKelly", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceSabrImpliedVol", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceSabrSmile", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("FinanceSabrCalibrate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("RegisterIdentity", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "security:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "RBAC/identity snapshot and MutationBatch metadata share one rbac.redb WTX"),
        ("RbacAdmin", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "security:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "runtime-conditional: List is a read; role and grant updates share one rbac.redb WTX with MutationBatch metadata"),
        ("ApplyMultisigMutation", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "security:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "threshold validation translates into the graph MutationBatch gateway"),
        #[cfg(feature = "jobs")]
        ("AnalyticsJob", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::JobsRedb, authz_action: "jobs:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "runtime-conditional: Status is a read; Submit/Cancel/Resume commit through the native jobs.redb MutationBatch gateway"),
        ("Sql", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "query:sql", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "runtime-conditional; graph DML uses staged graph state while table/catalog writes atomically commit SQL rows plus MutationBatch status/fence/idempotency/outbox"),
        ("CypherQuery", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "query:cypher", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "runtime-conditional; writes execute against a staged graph and publish only after durable MutationBatch commit"),
        ("GraphQl", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "query:graphql", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "runtime-conditional; ordinary writes stage through MutationBatch and cross-modal commit atomically includes universal status/fence/idempotency/outbox"),
        #[cfg(feature = "knowledge-batch")]
        ("KnowledgeStream", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:stream", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "one RequestContext/RLS/placement-bound stream with the sole native Arrow IPC projection for all seven query families"),
        ("UnifiedQuery", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:unified", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("UnifiedQueryText", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:unified", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ExplainPlan", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ExplainProvenance", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ExplainProvenanceByIds", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "CONCEPT:EG-KB-CURRENCY — ID-seeded sibling of ExplainProvenance, same policy profile"),
        ("ExplainPolicy", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ExplainBelief", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("EpistemicStatus", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "L53 (EPI-P3-5) acceptance capstone; handler additionally gated `epistemic-tms`"),
        ("WhatChanged", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "L53 (EPI-P3-5) bitemporal diff; handler additionally gated `epistemic-tms`"),
        ("RecomputeMaterialization", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ReasoningProjection, authz_action: "reasoning:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "fenced recompute/writeback resolves provenance from the authoritative graph and fsyncs the per-graph projection"),
        ("MaterializationStatus", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "read-only status from the durable per-graph incremental reasoning authority"),
        ("StaleMaterializations", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "bulk opaque stale references from the durable per-graph incremental reasoning authority"),
        ("ResolveConflict", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "EPI-P3-7 (gap-fill) standalone Dung argumentation (grounded/preferred/stable) conflict resolution over a BeliefGraph snapshot; handler additionally gated `epistemic-tms`"),
        ("ExplainEvidence", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "CONCEPT:EG-X1 multimodal-citation resolver; handler additionally gated `evidence-graph`"),
        ("CausalEstimate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "EPI-P3-3/P3-6 do-calculus intervention OR observational conditioning (selected by `mode`) over a request-carried SCM; handler additionally gated `epistemic-causal`"),
        ("CausalCounterfactual", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "EPI-P3-6 Pearl point-counterfactual over a request-carried SCM + a fully-observed unit; handler additionally gated `epistemic-causal`"),
        ("RankByProvenance", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "EPI-P3-3 provenance-aware retrieval ranking; handler additionally gated `epistemic-causal`"),
        ("NlQuery", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:nl", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("RegisterForeignSource", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "federation:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed control receipt; endpoint configuration is not duplicated in the ledger"),
        ("RegisterUdf", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "udf:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed control receipt; module bytes are not duplicated in the ledger"),
        ("RunUdf", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "udf:exec", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "executes a registered sandboxed function; treated as read/compute unless the UDF itself writes back (not modeled -- the wire protocol has no writeback flag here)"),
        ("DistributedCompute", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "distcompute:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "read-only Pregel/GAS computation; materialization uses the distinct Create*/Refresh* methods"),
        ("CreateMatView", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed control-plane MutationBatch saga"),
        ("GetMatView", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "matview:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("RefreshMatView", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed control-plane MutationBatch saga"),
        ("PlanMatViewDefine", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed control-plane MutationBatch saga"),
        ("PlanMatViewGet", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "matview:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("PlanMatViewRefresh", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed control-plane MutationBatch saga"),
        ("PlanMatViewDrop", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed control-plane MutationBatch saga"),
        ("BeginTxn", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:control", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native transaction staging authority"),
        ("TxnAddNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native staging; Commit owns graph publication"),
        ("TxnRemoveNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native staging; Commit owns graph publication"),
        ("TxnAddEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native staging; Commit owns graph publication"),
        ("TxnRemoveEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native staging; Commit owns graph publication"),
        ("TxnCas", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native staging; Commit owns graph publication"),
        ("TxnAddEmbedding", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
        ("TxnBlobRef", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
        ("TxnAddMeasurement", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
        ("TxnAxiom", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
        ("TxnConstruct", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
        ("TxnPlanWriteback", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
        ("TxnMaterializeBelief", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
        ("TxnUnifiedQuery", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "txn:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, ""),
        ("TxnUnifiedQueryText", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "txn:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, ""),
        ("Commit", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "named parent receipt plus atomic graph/cross-modal child batches"),
        ("Rollback", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:control", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native transaction staging removal"),
        ("TsAppend", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::SeriesRedb, authz_action: "timeseries:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "graph ACL + placement policy precede the tenant/graph/series-scoped series.redb write"),
        ("TsRange", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "timeseries:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("TsAsofJoin", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "timeseries:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("TsWindow", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "timeseries:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("TsGapFill", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "timeseries:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("BlobBegin", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "multi-call chunked-upload protocol (Begin ... ChunkPut* ... Commit); no single-call atomicity; durable via its own blob.redb (group-committed Immediate), self-routes before dispatch_graph_op"),
        ("BlobChunkPut", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "durable via its own blob.redb (group-committed Immediate); self-routes before dispatch_graph_op"),
        ("BlobCommit", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "multi-call chunked-upload protocol (Begin ... ChunkPut* ... Commit); no single-call atomicity; durable via its own blob.redb (group-committed Immediate), self-routes before dispatch_graph_op"),
        ("BlobFetchBegin", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "blob:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("BlobChunkGet", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "blob:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("BlobFetchEnd", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "blob:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("BlobRef", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "refcount increment; idempotent-ish but re-invocation adds another ref, so not idempotent; durable via blob.redb"),
        ("BlobUnref", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via blob.redb"),
        ("BlobGc", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via blob.redb"),
        ("KvGet", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "kv:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("KvPut", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::KvRedb, authz_action: "kv:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack); self-routes before graph dispatch"),
        ("KvDelete", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::KvRedb, authz_action: "kv:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via its own kv.redb (redb::Durability::Immediate); self-routes before dispatch_graph_op"),
        ("KvScan", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "kv:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("KvCas", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::KvRedb, authz_action: "kv:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack); self-routes before graph dispatch"),
        ("ImportSqliteFile", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:sqlite-file", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "native SQL-catalog MutationBatch; logical transfer name is excluded from the durable receipt"),
        ("ExportSqliteFile", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:sqlite-file", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "operator-provisioned transfer root; logical filenames only"),
        ("AddTriples", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "rdf:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("GetRdf", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "rdf:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("RemoveTriples", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "rdf:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("DropNamedGraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "rdf:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("Sparql", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "sparql:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("SparqlVirtual", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "sparql:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("OwlReason", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "owl:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("OwlReasonDistributed", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "owl:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("OwlExplain", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "owl:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("RunRules", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "reasoning:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "READ-ONLY (EG-P0-2/L11 handler audit): handle_run_rules reasons over an off-lock analysis_snapshot and returns inferred triples, no writeback -- unlike its sibling RunDatalogReasoning which materialises in-place. Corrected from a prior mutates=true semantic guess; now agrees with access.rs (never a write there)"),
        ("ShaclValidate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "validation:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("IcvConfigure", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "security:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch"),
        ("ShexValidate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "validation:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("CdcRead", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("RegisterContinuousQuery", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
        ("ReadContinuousQuery", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("DropContinuousQuery", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
        ("Watch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, "opens a push subscription; not a snapshot read nor a mutation"),
        ("RegisterTrigger", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
        ("DropTrigger", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
        ("ListTriggers", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("FiredTriggers", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("CepSubscribe", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "cep:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
        ("CepPoll", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cep:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("CepUnsubscribe", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "cep:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
        ("MineAssociate", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineCluster", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineAnomaly", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineClassifyFit", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "mining:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "the one Mine* family member that is unconditionally read-only (produces a model blob, never writes back)"),
        ("MineClassifyPredict", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineReduce", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("GraphLearnFit", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graphlearn:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("GraphLearnPredict", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graphlearn:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineSequence", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound; writeback=true enters the canonical durable mutation path"),
        ("MineForecast", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound; writeback=true enters the canonical durable mutation path"),
        ("MineText", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound; writeback=true for lda/nmf enters the canonical durable mutation path"),
        ("MineSubgraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound; writeback=true for gspan enters the canonical durable mutation path"),
        ("MineEntityResolve", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineCausalImpact", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineProcess", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineRootCause", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineRiskPropagation", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineOntologyGap", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineRetrievalQuality", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineCommunity", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
];

/// Render the full capability ledger as a Markdown table, one row per `Method` variant.
pub fn gen_ledger() -> String {
    let mut out = String::new();
    out.push_str("# Epistemic Graph -- Generated Capability Ledger\n\n");
    out.push_str(
        "> **This file is GENERATED and is the AUTHORITATIVE machine-checked capability \n\
         > truth (CONCEPT:EG-P0-1)** -- regenerate with `cargo run -p eg-capabilities --bin \n\
         > gen_ledger`. It is derived from the exhaustive, no-wildcard `policy()` match in \n\
         > `crates/eg-capabilities/src/lib.rs`, which the compiler forces to stay in sync with \n\
         > every `Method` variant. `docs/capabilities.md` describes surface-level feature \n\
         > parity; this generated table is authoritative for per-method policy.\n\n\
         > `mutates` marked `~true` means the value is a conservative UPPER BOUND: the real \n\
         > runtime answer is conditional (an operation, a `writeback` flag, or a parsed \n\
         > query) -- see the `note` column. `VolatileControl` is explicit non-durable \n\
         > process/session state; `None` is reserved for methods with no state transition.\n\n",
    );
    out.push_str(
        "| Method | Mutates | Durability | Authz action | Idempotent | Audited | Emits CDC | Txn participation | Note |\n",
    );
    out.push_str("|---|---|---|---|---|---|---|---|---|\n");
    for (name, p, note) in ALL_METHODS {
        let runtime_conditional =
            note.contains("runtime-conditional") || note.contains("conservative upper bound");
        let mutates_cell = if runtime_conditional && p.mutates {
            "~true".to_string()
        } else {
            p.mutates.to_string()
        };
        out.push_str(&format!(
            "| `{name}` | {mutates_cell} | {durability:?} | `{authz}` | {idempotent} | {audited} | {cdc} | {txn:?} | {note} |\n",
            name = name,
            mutates_cell = mutates_cell,
            durability = p.durability_domain,
            authz = p.authz_action,
            idempotent = p.idempotent,
            audited = p.audited,
            cdc = p.emits_cdc,
            txn = p.txn_participation,
            note = note,
        ));
    }
    // Strip per-line trailing whitespace (the header blockquote hard-wraps with a
    // trailing space on each continuation line) so the generated ledger matches the
    // `trailing-whitespace` pre-commit hook — otherwise the hook and the
    // `generated_ledger_is_not_stale` test fight over the same file forever.
    let mut out: String = out
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n");
    out.push('\n');
    out
}

#[cfg(test)]
mod smoke_tests {
    use super::*;

    #[test]
    fn all_methods_table_matches_policy_fn_and_has_no_duplicates() {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for (name, table_policy, _note) in ALL_METHODS {
            assert!(
                seen.insert(*name),
                "duplicate variant name in ALL_METHODS: {name}"
            );
            // We cannot construct a real `Method` value for every variant generically
            // (many carry required, non-Default fields), so this smoke test only checks
            // internal self-consistency of the static documentation table. The exhaustive
            // `policy()` match and its return-path invariants guard the served policy; the
            // mirrored classifier comparisons live in `tests/consistency.rs`.
            let _ = table_policy;
        }
        // Current-only table after the strict removal of four deprecated methods.
        let expected = 352
            + usize::from(cfg!(feature = "jobs"))
            + usize::from(cfg!(feature = "modality-serving"))
            + usize::from(cfg!(feature = "knowledge-batch"));
        assert_eq!(
            seen.len(),
            expected,
            "expected exactly {expected} Method variants"
        );
    }

    #[test]
    fn gen_ledger_renders_every_variant() {
        let md = gen_ledger();
        for (name, _, _) in ALL_METHODS {
            assert!(
                md.contains(&format!("`{name}`")),
                "gen_ledger() output is missing a row for {name}"
            );
        }
    }

    #[test]
    fn is_durable_implies_a_non_none_domain() {
        for (name, p, _) in ALL_METHODS {
            if p.is_durable() {
                assert_ne!(
                    p.durability_domain,
                    DurabilityDomain::None,
                    "{name}: is_durable() true but durability_domain is None"
                );
            }
        }
    }

    #[test]
    fn every_mutation_has_an_explicit_state_domain() {
        for (name, p, _) in ALL_METHODS {
            assert!(
                !p.mutates || !matches!(p.durability_domain, DurabilityDomain::None),
                "{name}: a mutating Method must name its durable or volatile state domain"
            );
        }
    }

    #[test]
    fn volatile_control_is_narrow_and_never_claims_durability() {
        const VOLATILE_METHODS: &[&str] = &["Shutdown"];
        for (name, p, _) in ALL_METHODS {
            let volatile = matches!(p.durability_domain, DurabilityDomain::VolatileControl);
            assert_eq!(
                volatile,
                VOLATILE_METHODS.contains(name),
                "{name}: VolatileControl may only describe explicit process/session state"
            );
            if volatile {
                assert!(
                    p.mutates,
                    "{name}: volatile control must change session state"
                );
                assert!(!p.is_durable(), "{name}: volatile control is not durable");
                assert!(
                    !p.audited,
                    "{name}: volatile control must not claim a durable audit"
                );
                assert!(
                    !p.emits_cdc,
                    "{name}: volatile control must not emit data CDC"
                );
            }
        }
    }

    #[test]
    fn generated_protocol_policy_inventory_covers_every_primitive() {
        let inventory = protocol_policy_inventory();
        assert_eq!(inventory.len(), ALL_METHODS.len());
        for row in &inventory {
            assert!(
                !row.primitive.is_empty(),
                "{} has empty primitive",
                row.method
            );
            assert!(!row.verb.is_empty(), "{} has empty policy verb", row.method);
            if !row.policy.mutates {
                assert_eq!(
                    row.access,
                    PolicyAccess::Read,
                    "{} is a read primitive without a read policy",
                    row.method
                );
            }
        }

        let ts: Vec<_> = inventory
            .iter()
            .filter(|row| row.primitive == "timeseries")
            .collect();
        assert_eq!(ts.len(), 5);
        assert_eq!(
            ts.iter()
                .filter(|row| row.access == PolicyAccess::Write)
                .count(),
            1
        );
        assert_eq!(
            ts.iter()
                .filter(|row| row.access == PolicyAccess::Read)
                .count(),
            4
        );
    }
}
