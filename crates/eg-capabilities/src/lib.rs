//! MethodPolicy: the machine-checked capability ledger for every `eg_types::protocol::Method`
//! wire-protocol operation (CONCEPT:EG-P0-1).
//!
//! ## Why this exists
//!
//! Today the engine's capability truth is scattered across a hand-maintained
//! `docs/capabilities.md` plus three independent classifiers that each answer a
//! DIFFERENT question about the same `Method` enum:
//!
//!   - `src/server/access.rs::requires_write`  -- does this mutate the target graph?
//!   - `src/wal.rs::is_durable_mutation`        -- does this survive a crash via the WAL?
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
//! ## Scope of this workstream (EG-P0-1)
//!
//! This crate defines the policy table + the exhaustiveness guarantee + a generated
//! Markdown ledger (see [`gen_ledger`]) + a consistency test that cross-checks this table
//! against the real classifiers above. It deliberately does **not** refactor
//! `access.rs`/`wal.rs`/`audit.rs`/`cdc.rs` to CONSUME this table (that is EG-P0-2/EG-P0-6);
//! today the relationship is a one-way audit (does the new declarative table agree with the
//! existing imperative code?), not yet a single implementation.
//!
//! ## Crate shape
//!
//! A leaf crate: the ONLY dependency is `eg-types` (with every one of its optional features
//! turned on -- see `Cargo.toml` for why). It is not a dependency of the main
//! `epistemic-graph` package's default build; see the root `Cargo.toml`'s `members` comment.

use eg_types::protocol::Method;

/// Where (if anywhere) a mutation's effect survives a process/host crash.
///
/// These are REAL, distinct persistence domains in the engine today, not a
/// theoretical taxonomy:
///   - [`GraphRedb`](DurabilityDomain::GraphRedb): the per-graph WAL + `graph.redb`
///     snapshot (`src/wal.rs`, `src/redb_store.rs`).
///   - [`SeriesRedb`](DurabilityDomain::SeriesRedb): the timeseries store's own
///     `series.redb`, entirely separate from `graph.redb` (`src/server/handlers/timeseries.rs`).
///   - [`KvRedb`](DurabilityDomain::KvRedb): the namespaced KV surface's own `kv.redb`,
///     committed with `redb::Durability::Immediate` (`src/server/kv.rs`).
///   - [`BlobRedb`](DurabilityDomain::BlobRedb): the content-addressed blob store's own
///     `blob.redb`, group-committed `Immediate` (`src/server/blob/store.rs`).
///   - [`Outbox`](DurabilityDomain::Outbox): the message-broker/stream control-graph state
///     (EG-275..284) -- physically stored as graph nodes and WAL-logged alongside
///     `GraphRedb`, but called out separately because it is a semantically distinct
///     domain (queues/exchanges/streams, not node/edge content).
///   - [`JobsRedb`](DurabilityDomain::JobsRedb): the durable analytics-job plane's own
///     `jobs.redb` (CONCEPT:INT-P2-1), entirely separate from `graph.redb` (`eg-jobs`,
///     wired by the facade's `src/server/handlers/jobs.rs`, feature `jobs`).
///   - [`None`](DurabilityDomain::None): no durability at all -- a crash loses the effect
///     (this includes ordinary reads, but ALSO includes several real mutations; see the
///     `EG-P0-3` divergence entries in the consistency test).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurabilityDomain {
    GraphRedb,
    SeriesRedb,
    KvRedb,
    JobsRedb,
    BlobRedb,
    Outbox,
    None,
}

/// The isolation/consistency contract a method's storage effect (if any) participates in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxnParticipation {
    /// A single, all-or-nothing engine-internal transition (one WAL record / one redb
    /// commit / one in-memory swap) -- the common case for CRUD-shaped ops.
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
///     `writeback: bool` field, or a parsed SQL/Cypher/GraphQL statement), this is the
///     conservative UPPER BOUND (`true` if it can ever mutate) -- see the
///     `RUNTIME_CONDITIONAL` divergence entries in the consistency test.
///   - `durability_domain`: see [`DurabilityDomain`].
///   - `authz_action`: a `<domain>:<verb>` scope string for a future capability-based
///     authorizer. Not yet wired to any real ACL check (today's isolation layer is
///     coarse Read/Write per graph, not per-action) -- a judgment call, not
///     cross-checked against existing code.
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
    /// engine's real persistence domains -- the per-graph WAL, or one of the parallel
    /// KV/blob/series redb stores. `None` means a crash loses the effect outright.
    pub const fn is_durable(&self) -> bool {
        !matches!(self.durability_domain, DurabilityDomain::None)
    }
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
    match m {
// AUTO-GENERATED reference: this match was authored by hand (Codex/Claude workstream
// EG-P0-1), grouping variants that share an identical MethodPolicy. Grouping is by
// IDENTICAL policy value among declaration-order-adjacent variants only -- it is a
// readability aid, not a semantic claim that ungrouped variants elsewhere differ.
        Method::AddNode { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic },
        Method::RemoveNode { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic },
        Method::HasNode { .. } | Method::GetNodes | Method::GetNodesByLabel { .. } | Method::GetNodeProperties { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::CompareAndSetNodeFields { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic },
        Method::ClaimNext { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::DeclareExchange { .. } | Method::DeleteExchange { .. } | Method::BindQueue { .. } | Method::UnbindQueue { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::Publish { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::DeclareQueue { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::PublishEx { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::BrokerConsume { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:consume", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::BrokerAck { .. } | Method::BrokerReject { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::SweepExpired { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::StreamDeclare { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::StreamPublish { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::StreamRead { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "stream:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::StreamTrim { .. } | Method::StreamCommitOffset { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::StreamCommittedOffset { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "stream:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::PublishConfirmed { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::PublishIdempotent { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::BrokerAckTag { .. } | Method::BrokerNackTag { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::CreateSummaryNode { .. } | Method::Consolidate { .. } | Method::Reinforce { .. } | Method::DecayNode { .. } | Method::DecayMemories { .. } | Method::EvictBelow { .. } | Method::Maintain { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::SummaryChildren { .. } | Method::SummariesAtLevel { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "memory:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::AddSceneObject { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "scene:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::SetPose { .. } | Method::Reparent { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "scene:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::WorldTransform { .. } | Method::SceneChildren { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "scene:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::StartTrajectory { .. } | Method::AppendStep { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::DiscountedReturn { .. } | Method::BestTrajectory { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "memory:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::GetNodePropertiesBatch { .. } | Method::HasNodesBatch { .. } | Method::NodeCount | Method::NodeIds => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::AddEdge { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "edge:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic },
        Method::RemoveEdge { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "edge:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic },
        Method::InvalidateEdge { .. } | Method::SupersedeEdge { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "edge:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::HasEdge { .. } | Method::GetEdges => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::GetTriples => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "rdf:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::ClearGraph => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:admin", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic },
        Method::GetEdgeProperties { .. } | Method::GetEdgePropertiesBatch { .. } | Method::EdgeCount | Method::InDegree { .. } | Method::OutDegree { .. } | Method::GetPredecessors { .. } | Method::GetSuccessors { .. } | Method::GetNeighbors { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::UnionGetNodeProperties { .. } | Method::UnionGetNodesByLabel { .. } | Method::UnionGetNeighbors { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::TopologicalSort | Method::FindCycle | Method::GetShortestPath { .. } | Method::GetBlastRadius { .. } | Method::DegreeCentrality { .. } | Method::DegreeCentralityAll | Method::BetweennessCentrality | Method::PageRank { .. } | Method::PersonalizedPageRank { .. } | Method::ConnectedComponents | Method::StronglyConnectedComponents | Method::MinimumSpanningTree | Method::CommunityDetection { .. } | Method::CommunityDetectEphemeral { .. } | Method::GraphColoring | Method::ComputeSimilarityEdges { .. } | Method::ResolveCandidates { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::PruneByLifecycle { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::GetContextView { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::BatchUpdate { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::MultiGraphBatchUpdate { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "node:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::Metrics => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::EvictLRU { .. } | Method::DecaySweep { .. } | Method::TouchNodes { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::ToMsgpack => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::FromMsgpack { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "graph:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::GetLedger => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "ledger:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::ClearLedger => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "ledger:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::ApplyLedger { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "ledger:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::AuditVerify => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "security:audit", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::GetSubgraph { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::Fork | Method::DiffAgainst { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::CompactNodesByType { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::RunDatalogReasoning { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "reasoning:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::CreateGraph { .. } | Method::DeleteGraph { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "graph:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::ListGraphs => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::Reshard { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::CatalogAssign { .. } | Method::CatalogReassign { .. } | Method::CatalogRemove { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::CatalogList | Method::RebalancePlan { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster-read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::RebalanceExecute { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::PlacementRoute { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster-read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::Backup { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:backup", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::Restore { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "admin:backup", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::CreateChannel { .. } | Method::JoinChannel { .. } | Method::LeaveChannel { .. } | Method::CloseChannel { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::SendMessage { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "channel:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::GetChannelMessages { .. } | Method::ListChannels | Method::GetChannelMembers { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "channel:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::Ping | Method::Health => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::CancelRequest { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::Shutdown | Method::Checkpoint => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "service:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::ResourceStats => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::Reconcile { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "graph:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::ApplyMutation { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "graph:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::ParseRepository { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::Vf2SubgraphMatch { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::ParseFile { .. } | Method::ParseFiles { .. } | Method::IndexRepository { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:parse", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::ObserveScreen { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:vision", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        // EG-P0-3 fixed `wal.rs::is_durable_mutation` to cover this (was previously
        // absent -- an acknowledged embedding write was silently lost on crash);
        // EG-P0-6/L14 catches up the ledger to match (was `DurabilityDomain::None`).
        Method::AddEmbedding { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::SemanticSearch { .. } | Method::Discover { .. } | Method::MatchOntologyTerms { .. } | Method::SpectralCluster { .. } | Method::HypergraphEncodeInteraction { .. } | Method::BatchCosineSimilarity { .. } | Method::BatchL2Normalize { .. } | Method::FindSimilarPairs { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::FinanceOptimizePortfolio { .. } | Method::FinanceRiskParity { .. } | Method::FinanceBlackLitterman { .. } | Method::FinanceEfficientFrontier { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::DsLinearRegression { .. } | Method::DsKMeans { .. } | Method::DsPca { .. } | Method::DsComputeStats { .. } | Method::DsTrainTestSplit { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::DsFitEstimator { .. } | Method::DsPredictEstimator { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::DsSoftmax { .. } | Method::DsLogSoftmax { .. } | Method::DsCrossEntropy { .. } | Method::DsDpoLoss { .. } | Method::DsGrpoSurrogate { .. } | Method::DsKlDivergence { .. } | Method::DsAdamStep { .. } | Method::DsSgdStep { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::FinanceVar { .. } | Method::FinanceCvar { .. } | Method::FinanceMaxDrawdown { .. } | Method::FinanceDrawdownSeries { .. } | Method::FinanceDownsideDeviation { .. } | Method::FinanceRiskMetrics { .. } | Method::FinanceMonteCarloVar { .. } | Method::FinanceStressTest { .. } | Method::FinanceDetectRegimes { .. } | Method::FinanceRollingZscore { .. } | Method::FinanceEwma { .. } | Method::FinanceSignalDecay { .. } | Method::FinanceCombineAlphas { .. } | Method::FinanceCrossSectionalRank { .. } | Method::FinanceMomentum { .. } | Method::FinanceMeanReversion { .. } | Method::FinanceInformationCoefficient { .. } | Method::FinanceTwap { .. } | Method::FinanceVwap { .. } | Method::FinanceMarketImpact { .. } | Method::FinancePairsTrading { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::FinanceMatchOrders { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::FinanceAvellanedaStoikov { .. } | Method::FinanceGltQuotes { .. } | Method::FinanceLogitQuotes { .. } | Method::FinanceGlostenMilgromSpread { .. } | Method::FinanceExpectedPnlRate { .. } | Method::FinanceBreakevenAlpha { .. } | Method::FinanceOfiSeries { .. } | Method::FinanceMicropriceSeries { .. } | Method::FinanceVpinPm { .. } | Method::FinanceHawkesMle { .. } | Method::FinanceHardimanBouchaud { .. } | Method::FinanceKyleLambda { .. } | Method::FinanceSurveillanceRisk { .. } | Method::FinanceKellyFraction { .. } | Method::FinanceBayesianKelly { .. } | Method::FinancePosteriorCredibleInterval { .. } | Method::FinancePurgedCpcv { .. } | Method::FinanceDeflatedSharpe { .. } | Method::FinanceProbabilityBacktestOverfit { .. } | Method::FinanceDieboldMariano { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::FinanceForensicReport { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::FinanceKalmanFilter1d { .. } | Method::FinanceKalmanBeta { .. } | Method::FinanceKalmanVolatility { .. } | Method::FinanceAdfTest { .. } | Method::FinanceOuCalibrate { .. } | Method::FinanceOuOptimalThresholds { .. } | Method::FinanceMarkovTransitionMatrix { .. } | Method::FinanceOrderBookImbalance { .. } | Method::FinanceQueueImbalance { .. } | Method::FinanceRealizedVolTick { .. } | Method::FinanceSpreadReversion { .. } | Method::FinanceInformationRatio { .. } | Method::FinanceEffectiveIndependentN { .. } | Method::FinanceAlphaCombinationEngine { .. } | Method::FinanceBrierScore { .. } | Method::FinanceConvergenceGate { .. } | Method::FinanceEmpiricalKelly { .. } | Method::FinanceSabrImpliedVol { .. } | Method::FinanceSabrSmile { .. } | Method::FinanceSabrCalibrate { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::RegisterIdentity { .. } | Method::RbacAdmin { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "security:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::ApplyMultisigMutation { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "security:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        // CONCEPT:INT-P2-1 -- the durable analytics-job plane. `mutates: true` is the
        // conservative upper bound (mirrors `RbacAdmin { op }` above): the real
        // per-op answer is runtime-conditional on `JobOp` (`Status` is a pure read;
        // `Submit`/`Cancel`/`Resume` mutate the job store). Own durability domain
        // (`JobsRedb`, its own `jobs.redb`) -- NOT `GraphRedb`, so it is excluded from
        // the `wal.rs` cross-check the same way `SeriesRedb`/`KvRedb`/`BlobRedb` are
        // (see the consistency test). Not audited/CDC-emitted for the same reason
        // `TsAppend`/`Kv*` aren't: it self-manages its own durability out of band of
        // the graph tamper-evident chain, and self-routes before `dispatch_graph_op`
        // (see `mutation.rs`'s `JUSTIFIED_NA` entry), so it never reaches
        // `wal.rs::apply`/`audit.rs::audit_line` at all.
        #[cfg(feature = "jobs")]
        Method::AnalyticsJob { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::JobsRedb, authz_action: "jobs:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::Sql { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "query:sql", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::CypherQuery { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "query:cypher", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::GraphQl { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "query:graphql", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::UnifiedQuery { .. } | Method::UnifiedQueryText { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:unified", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::ExplainPlan { .. } | Method::ExplainProvenance { .. } | Method::ExplainPolicy { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::ExplainBelief { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        // L53 (EPI-P3-5 UQL wiring): the acceptance-capstone + temporal-diff read ops.
        // Both read-only, no durability, no audit/CDC — same profile as `ExplainBelief`.
        Method::EpistemicStatus { .. } | Method::WhatChanged { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::NlQuery { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:nl", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::RegisterForeignSource { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "federation:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::RegisterUdf { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "udf:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::RunUdf { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "udf:exec", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::DistributedCompute { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "distcompute:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::CreateMatView { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "matview:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::GetMatView { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "matview:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::RefreshMatView { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "matview:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::PlanMatViewDefine { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::PlanMatViewGet { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "matview:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::PlanMatViewRefresh { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "matview:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::PlanMatViewDrop { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::BeginTxn { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "txn:control", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::TxnAddNode { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::TxnRemoveNode { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::TxnAddEdge { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::TxnRemoveEdge { .. } | Method::TxnCas { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::TxnAddEmbedding { .. } | Method::TxnBlobRef { .. } | Method::TxnAddMeasurement { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::TxnAxiom { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::TxnConstruct { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::TxnPlanWriteback { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::TxnMaterializeBelief { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::TxnUnifiedQuery { .. } | Method::TxnUnifiedQueryText { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "txn:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::Commit { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:control", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::Rollback { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "txn:control", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::TsAppend { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::SeriesRedb, authz_action: "timeseries:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::TsRange { .. } | Method::TsAsofJoin { .. } | Method::TsWindow { .. } | Method::TsGapFill { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "timeseries:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::BlobBegin { .. } | Method::BlobChunkPut { .. } | Method::BlobCommit { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga },
        Method::BlobFetchBegin { .. } | Method::BlobChunkGet { .. } | Method::BlobFetchEnd { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "blob:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::BlobRef { .. } | Method::BlobUnref { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::BlobGc => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::KvGet { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "kv:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::KvPut { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::KvRedb, authz_action: "kv:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::KvDelete { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::KvRedb, authz_action: "kv:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::KvScan { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "kv:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::KvCas { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::KvRedb, authz_action: "kv:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::ImportSqliteFile { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "sqlite:import", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::ExportSqliteFile { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "sqlite:export", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::AddTriples { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "rdf:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::GetRdf => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "rdf:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::RemoveTriples { .. } | Method::DropNamedGraph => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "rdf:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::Sparql { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "sparql:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::SparqlVirtual { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "sparql:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::OwlReason { .. } | Method::OwlReasonDistributed { .. } | Method::OwlExplain { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "owl:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        // EG-P0-2/L11: RunRules is READ-ONLY (unlike its sibling RunDatalogReasoning, which
        // materialises inferred edges in-place). `handle_run_rules` (src/server/handlers/rdf.rs)
        // runs `eg_rdf::rules::run_rule_reasoning_on_view(view: &GraphView, ..)` over an OFF-LOCK
        // `analysis_snapshot()` and RETURNS the inferred triples -- it never calls add_node/
        // add_edge/any writeback. The earlier `mutates: true` was a semantic guess that the
        // L11 handler audit disproved; corrected to a read (matches access.rs, which never
        // classified it as a write).
        Method::RunRules { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "reasoning:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::ShaclValidate { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "validation:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::IcvConfigure { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "security:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::ShexValidate { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "validation:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::CdcRead { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::RegisterContinuousQuery { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::ReadContinuousQuery { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::DropContinuousQuery { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::Watch { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None },
        Method::RegisterTrigger { .. } | Method::DropTrigger { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::ListTriggers { .. } | Method::FiredTriggers { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::CepSubscribe { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "cep:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::CepPoll { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cep:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::CepUnsubscribe { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "cep:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::MineAssociate { .. } | Method::MineCluster { .. } | Method::MineAnomaly { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::MineClassifyFit { .. } => MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "mining:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot },
        Method::MineClassifyPredict { .. } | Method::MineReduce { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::GraphLearnFit { .. } | Method::GraphLearnPredict { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graphlearn:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        // EG-P0-3 fixed `wal.rs::is_durable_mutation` to cover these four (each
        // was previously absent for its writeback=true case); EG-P0-6/L14 catches
        // up the ledger to match (was `DurabilityDomain::None`).
        Method::MineSequence { .. } | Method::MineForecast { .. } | Method::MineText { .. } | Method::MineSubgraph { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
        Method::MineEntityResolve { .. } | Method::MineCausalImpact { .. } | Method::MineProcess { .. } | Method::MineRootCause { .. } | Method::MineRiskPropagation { .. } | Method::MineOntologyGap { .. } | Method::MineRetrievalQuality { .. } | Method::MineCommunity { .. } => MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic },
    }
}

/// `(variant name, policy, note)` for every `Method` variant, in the SAME declaration
/// order as `eg_types::protocol::Method` (mirrors `crates/eg-types/src/protocol.rs`).
/// `note` is a non-empty, human-readable explanation whenever this variant's policy is a
/// documented judgment call or a known divergence from an existing classifier; empty
/// otherwise. Used by [`gen_ledger`] and by the consistency test (`tests/consistency.rs`).
pub const ALL_METHODS: &[(&str, MethodPolicy, &str)] = &[
        ("AddNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
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
        ("SweepExpired", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("StreamDeclare", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("StreamPublish", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("StreamRead", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "stream:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("StreamTrim", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("StreamCommitOffset", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("StreamCommittedOffset", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "stream:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("PublishConfirmed", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)"),
        ("PublishIdempotent", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)"),
        ("BrokerAckTag", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("BrokerNackTag", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
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
        ("GetTriples", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "rdf:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
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
        ("PruneByLifecycle", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "write per access.rs but intentionally excluded from wal.rs (recomputable maintenance op, per wal.rs doc comment)"),
        ("GetContextView", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("BatchUpdate", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("MultiGraphBatchUpdate", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "node:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "spans multiple graphs in one call (not a single-graph Atomic unit); NOT present in access.rs's write classifier or wal.rs's durable set at all -- flagged as a possible coverage gap in both"),
        ("Metrics", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("EvictLRU", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "write per access.rs but intentionally excluded from wal.rs (recomputable maintenance op, per wal.rs doc comment)"),
        ("DecaySweep", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "write per access.rs but intentionally excluded from wal.rs (recomputable maintenance op, per wal.rs doc comment)"),
        ("TouchNodes", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "write per access.rs but intentionally excluded from wal.rs (recomputable maintenance op, per wal.rs doc comment)"),
        ("ToMsgpack", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("FromMsgpack", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "graph:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "write per access.rs but absent from wal.rs durable set (EG-P0-3); typically used for bulk restore, not incremental WAL logging"),
        ("GetLedger", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "ledger:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ClearLedger", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "ledger:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "write per access.rs but absent from wal.rs durable set (EG-P0-3)"),
        ("ApplyLedger", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "ledger:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "write per access.rs but absent from wal.rs durable set (EG-P0-3)"),
        ("AuditVerify", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "security:audit", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetSubgraph", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("Fork", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "returns the forked snapshot to the caller; never registers/persists it server-side"),
        ("DiffAgainst", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("CompactNodesByType", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "write per access.rs but absent from wal.rs durable set (EG-P0-3); recomputable"),
        ("RunDatalogReasoning", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "reasoning:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "write per access.rs but absent from wal.rs durable set (EG-P0-3); inferred facts are recomputable"),
        ("CreateGraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "graph:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier (it targets the registry, not an existing graph's content) -- the registry-level ACL for this is out of this workstream's scope"),
        ("DeleteGraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "graph:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("ListGraphs", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("Reshard", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "NOT present in access.rs's write classifier at all (server-admin action, not a per-graph content write) -- governed by a separate admin gate, out of this workstream's scope"),
        ("CatalogAssign", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "NOT present in access.rs's write classifier at all -- governed elsewhere"),
        ("CatalogReassign", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "NOT present in access.rs's write classifier at all -- governed elsewhere"),
        ("CatalogRemove", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "NOT present in access.rs's write classifier at all -- governed elsewhere"),
        ("CatalogList", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster-read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("RebalancePlan", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster-read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("RebalanceExecute", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "NOT present in access.rs's write classifier at all -- governed elsewhere"),
        ("PlacementRoute", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster-read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "DIST-P2-4: always in the enum (pure serde); the real answer needs `raft` + a live MultiRaft cluster, else a well-formed {\"explicit\": false} JSON (not an error) -- see handlers/placement.rs"),
        ("Backup", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:backup", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "reads a consistent snapshot out to a bundle; does not mutate the live graph"),
        ("Restore", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "admin:backup", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "NOT present in access.rs's write classifier at all (server-admin action) -- governed elsewhere"),
        ("CreateChannel", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("JoinChannel", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("LeaveChannel", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("CloseChannel", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("SendMessage", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "channel:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("GetChannelMessages", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "channel:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ListChannels", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "channel:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("GetChannelMembers", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "channel:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("Ping", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("Health", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("Shutdown", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "service:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, "a server lifecycle action, not a graph-content mutation -- governed by a separate admin gate"),
        ("Checkpoint", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "service:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, "flushes the ALREADY-durable WAL tail into a snapshot; does not itself add new data"),
        ("ResourceStats", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("Reconcile", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "graph:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "CRDT merge of remote state; write per access.rs but absent from wal.rs durable set (EG-P0-3)"),
        ("ApplyMutation", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "graph:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "write per access.rs but absent from wal.rs durable set (EG-P0-3)"),
        ("ParseRepository", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "write per access.rs but intentionally excluded from wal.rs (recomputable from source tree, per wal.rs doc comment)"),
        ("Vf2SubgraphMatch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ParseFile", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:parse", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("ParseFiles", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:parse", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("IndexRepository", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:parse", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("ObserveScreen", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:vision", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
        ("AddEmbedding", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
        ("SemanticSearch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("Discover", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("MatchOntologyTerms", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("SpectralCluster", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("HypergraphEncodeInteraction", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("BatchCosineSimilarity", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("BatchL2Normalize", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("FindSimilarPairs", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
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
        ("RegisterIdentity", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "security:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("RbacAdmin", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "security:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("ApplyMultisigMutation", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "security:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "write per access.rs but absent from wal.rs durable set (EG-P0-3); multisig threshold-gated, multi-party"),
        #[cfg(feature = "jobs")]
        ("AnalyticsJob", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::JobsRedb, authz_action: "jobs:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "own jobs.redb (CONCEPT:INT-P2-1); self-routes before dispatch_graph_op like TsAppend/Kv*, never reaches wal.rs/audit.rs"),
        ("Sql", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "query:sql", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) depends on the parsed statement kind (eg_query::classify); write-but-absent-from-wal.rs's durable set too (EG-P0-3) -- SQL DML durability rides the user-table store's own persistence, not the graph WAL"),
        ("CypherQuery", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "query:cypher", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) depends on a keyword scan of the query text (access::cypher_is_write); also write-but-absent-from-wal.rs's durable set (EG-P0-3)"),
        ("GraphQl", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "query:graphql", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) depends on the parsed operation kind (query vs mutation); also write-but-absent-from-wal.rs's durable set (EG-P0-3)"),
        ("UnifiedQuery", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:unified", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("UnifiedQueryText", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:unified", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ExplainPlan", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ExplainProvenance", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ExplainPolicy", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("ExplainBelief", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("EpistemicStatus", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "L53 (EPI-P3-5) acceptance capstone; handler additionally gated `epistemic-tms`"),
        ("WhatChanged", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "L53 (EPI-P3-5) bitemporal diff; handler additionally gated `epistemic-tms`"),
        ("NlQuery", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:nl", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("RegisterForeignSource", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "federation:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all; policy marks it mutates=true on semantic grounds (registers a foreign-source config) -- flagged as a possible access.rs coverage gap"),
        ("RegisterUdf", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "udf:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all; policy marks it mutates=true on semantic grounds -- flagged as a possible access.rs coverage gap"),
        ("RunUdf", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "udf:exec", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "executes a registered sandboxed function; treated as read/compute unless the UDF itself writes back (not modeled -- the wire protocol has no writeback flag here)"),
        ("DistributedCompute", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "distcompute:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "NOT present in access.rs's write classifier at all; policy marks it mutates=true on semantic grounds (cross-shard Pregel writeback) -- flagged as a possible access.rs coverage gap"),
        ("CreateMatView", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "matview:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("GetMatView", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "matview:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("RefreshMatView", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "matview:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("PlanMatViewDefine", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("PlanMatViewGet", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "matview:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("PlanMatViewRefresh", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "matview:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("PlanMatViewDrop", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("BeginTxn", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "txn:control", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, ""),
        ("TxnAddNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify)"),
        ("TxnRemoveNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "self-routes before dispatch_graph_op; see TxnAddNode note"),
        ("TxnAddEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify)"),
        ("TxnRemoveEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "self-routes before dispatch_graph_op; see TxnAddNode note"),
        ("TxnCas", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "self-routes before dispatch_graph_op; see TxnAddNode note"),
        ("TxnAddEmbedding", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify)"),
        ("TxnBlobRef", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify)"),
        ("TxnAddMeasurement", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify)"),
        ("TxnAxiom", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify)"),
        ("TxnConstruct", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify)"),
        ("TxnPlanWriteback", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify)"),
        ("TxnMaterializeBelief", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify)"),
        ("TxnUnifiedQuery", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "txn:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, ""),
        ("TxnUnifiedQueryText", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "txn:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, ""),
        ("Commit", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "txn:control", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "the durable-apply moment of the multi-op OCC transaction; self-routes before dispatch_graph_op"),
        ("Rollback", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "txn:control", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, ""),
        ("TsAppend", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::SeriesRedb, authz_action: "timeseries:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "self-routes before dispatch_graph_op, targeting its own series.redb (separate from graph.redb/blob.redb/kv.redb)"),
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
        ("KvPut", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::KvRedb, authz_action: "kv:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack) -- self-routes before dispatch_graph_op and before wal.rs's per-graph WAL entirely; NOT a wal.rs gap, just a parallel durability domain"),
        ("KvDelete", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::KvRedb, authz_action: "kv:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via its own kv.redb (redb::Durability::Immediate); self-routes before dispatch_graph_op"),
        ("KvScan", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "kv:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("KvCas", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::KvRedb, authz_action: "kv:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack) -- self-routes before dispatch_graph_op and before wal.rs's per-graph WAL entirely; NOT a wal.rs gap, just a parallel durability domain"),
        ("ImportSqliteFile", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "sqlite:import", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("ExportSqliteFile", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "sqlite:export", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
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
        ("IcvConfigure", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "security:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "write per access.rs but absent from wal.rs durable set (EG-P0-3); a build without `security` drops audit entirely"),
        ("ShexValidate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "validation:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("CdcRead", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("RegisterContinuousQuery", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("ReadContinuousQuery", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("DropContinuousQuery", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("Watch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, "opens a push subscription; not a snapshot read nor a mutation"),
        ("RegisterTrigger", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("DropTrigger", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("ListTriggers", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("FiredTriggers", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("CepSubscribe", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "cep:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("CepPoll", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cep:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
        ("CepUnsubscribe", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::None, authz_action: "cep:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap"),
        ("MineAssociate", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineCluster", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineAnomaly", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineClassifyFit", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "mining:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "the one Mine* family member that is unconditionally read-only (produces a model blob, never writes back)"),
        ("MineClassifyPredict", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineReduce", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("GraphLearnFit", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graphlearn:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("GraphLearnPredict", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graphlearn:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
        ("MineSequence", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound (writeback-conditional per access.rs); wal.rs::is_durable_mutation now covers the writeback=true case (EG-P0-3 fixed)"),
        ("MineForecast", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound (writeback-conditional per access.rs); wal.rs::is_durable_mutation now covers the writeback=true case (EG-P0-3 fixed)"),
        ("MineText", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound (writeback AND non-tfidf/non-motif algorithm-conditional per access.rs); wal.rs::is_durable_mutation now covers the lda/nmf writeback=true case (EG-P0-3 fixed)"),
        ("MineSubgraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound (writeback AND non-tfidf/non-motif algorithm-conditional per access.rs); wal.rs::is_durable_mutation now covers the gspan writeback=true case (EG-P0-3 fixed)"),
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
         > every `Method` variant. The hand-maintained `docs/capabilities.md` predates this \n\
         > table and is NOT authoritative; it has not yet been reconciled/retired (out of \n\
         > scope for this workstream).\n\n\
         > `mutates` marked `~true` means the value is a conservative UPPER BOUND: the real \n\
         > runtime answer is conditional (a `writeback` flag, or a parsed query) -- see the \n\
         > `note` column and the consistency test's `RUNTIME_CONDITIONAL` table.\n\n",
    );
    out.push_str(
        "| Method | Mutates | Durability | Authz action | Idempotent | Audited | Emits CDC | Txn participation | Note |\n",
    );
    out.push_str(
        "|---|---|---|---|---|---|---|---|---|\n",
    );
    for (name, p, note) in ALL_METHODS {
        let mutates_cell = if !note.is_empty() && p.mutates {
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
            assert!(seen.insert(*name), "duplicate variant name in ALL_METHODS: {name}");
            // We cannot construct a real `Method` value for every variant generically
            // (many carry required, non-Default fields), so this smoke test only checks
            // internal self-consistency of the static table; the REAL cross-check against
            // `policy()`'s match arms (and against the mirrored classifier snapshots) lives
            // in `tests/consistency.rs`.
            let _ = table_policy;
        }
        // CONCEPT:INT-P2-1: +1 (340/341) when `jobs` adds `Method::AnalyticsJob`.
        // CONCEPT:EG-KG.sharding.placement-route-rpc (DIST-P2-4): +1 (338 base) for the
        // always-in-the-enum `Method::PlacementRoute`.
        // L53 (EPI-P3-5): +2 (338 -> 340 base) for `Method::EpistemicStatus` /
        // `Method::WhatChanged`.
        let expected = if cfg!(feature = "jobs") { 341 } else { 340 };
        assert_eq!(seen.len(), expected, "expected exactly {expected} Method variants");
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
}
