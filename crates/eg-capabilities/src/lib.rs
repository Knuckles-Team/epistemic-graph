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
//! idempotency/audit/CDC/transaction-participation profile, and [`policy`] resolves each
//! variant through a no-wildcard `Method` match emitted from the ordered `ALL_METHODS`
//! ledger. Adding a `Method` variant without declaring its policy is a compile error, not a
//! silent gap.
//!
//! This crate defines the policy table, its exhaustiveness guarantee, the generated
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
///   - [`StatechartRedb`](DurabilityDomain::StatechartRedb): the native statechart
///     engine's own `statecharts.redb` (CONCEPT:INT-P2-2), entirely separate from graph
///     shards (`eg-statechart`, wired by `src/server/handlers/statechart.rs`, feature
///     `statechart`) — the exact sibling of `JobsRedb`.
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
    StatechartRedb,
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

/// Generate the protocol-method -> primitive-policy inventory from the canonical
/// `ALL_METHODS` table. There is no second policy source: the exhaustive dispatcher and
/// this inventory are emitted from the same row declarations.
pub fn protocol_policy_inventory() -> Vec<ProtocolPolicyInventoryEntry> {
    ALL_METHODS
        .iter()
        .map(|(method, policy, _)| {
            let (primitive, verb) = policy
                .authz_action
                .split_once(':')
                .expect("MethodPolicy.authz_action must be primitive:verb");
            ProtocolPolicyInventoryEntry {
                method,
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

/// Resolve runtime-conditional policy fields before the generated exhaustive match.
///
/// The `define_method_policy_table!` invocation below is the single source for both the
/// stable ledger order and the no-wildcard `Method` match. Adding a `Method` variant
/// without adding its policy row is therefore a compile error; adding a row without a
/// corresponding enum pattern is also a compile error.
const VOLATILE_CONTROL_METHODS: &[&str] = &[
    "Shutdown",
    "TxnAddNode",
    "TxnRemoveNode",
    "TxnAddEdge",
    "TxnRemoveEdge",
    "TxnCas",
    "TxnAddEmbedding",
    "TxnBlobRef",
    "TxnAddMeasurement",
    "TxnAxiom",
    "TxnConstruct",
    "TxnPlanWriteback",
    "TxnMaterializeBelief",
];

fn cypher_policy(mode: &CypherMode) -> MethodPolicy {
    match mode {
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
    }
}

#[cfg(feature = "modality-serving")]
fn served_modality_policy(op: &eg_types::modality::ServedModalityOp) -> MethodPolicy {
    if op.mutates() {
        MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "modality:write",
            idempotent: op.is_idempotent_mutation(),
            audited: true,
            emits_cdc: true,
            txn_participation: TxnParticipation::Atomic,
        }
    } else {
        MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "modality:read",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        }
    }
}

fn policy_for_method(method: &Method) -> MethodPolicy {
    if let Method::CypherQuery { mode, .. } = method {
        return cypher_policy(mode);
    }
    #[cfg(feature = "modality-serving")]
    if let Method::ServedModality { op } = method {
        return served_modality_policy(op);
    }
    policy_from_method(method)
}

fn validate_policy(method_name: &str, result: MethodPolicy) -> MethodPolicy {
    assert!(
        !result.mutates || !matches!(result.durability_domain, DurabilityDomain::None),
        "a mutating Method must name its durable or volatile state domain"
    );
    if matches!(result.durability_domain, DurabilityDomain::VolatileControl) {
        assert!(
            VOLATILE_CONTROL_METHODS.contains(&method_name),
            "VolatileControl is restricted to process lifecycle and transaction staging"
        );
    }
    result
}

pub fn policy(m: &Method) -> MethodPolicy {
    let method_name: &'static str = m.into();
    validate_policy(method_name, policy_for_method(m))
}

/// Declare the policy ledger and its exhaustive runtime dispatcher from one row source.
///
/// Each row supplies the exact enum pattern, public ledger name, policy value, and note.
/// The macro emits both the stable-order `ALL_METHODS` inventory and a no-wildcard
/// `Method` match, so adding a protocol variant without a policy row fails compilation.
macro_rules! define_method_policy_table {
    (
        $(
            $(#[$attr:meta])*
            ($pattern:pat, $name:literal, $policy:expr, $note:literal)
        ),* $(,)?
    ) => {
        /// `(variant name, policy, note)` for every `Method` variant, in the stable ledger
        /// order retained for generated output and inventory consumers. The ledger order is
        /// intentionally not required to mirror `eg_types::protocol::Method`: several
        /// feature/domain blocks predate later protocol insertions. `note` is a non-empty,
        /// human-readable explanation whenever this variant's policy is a documented
        /// judgment call or known classifier divergence; empty otherwise. Used by
        /// [`gen_ledger`] and the consistency test.
        pub const ALL_METHODS: &[(&str, MethodPolicy, &str)] = &[
            $(
                $(#[$attr])*
                ($name, $policy, $note),
            )*
        ];

        fn policy_from_method(method: &Method) -> MethodPolicy {
            match method {
                $(
                    $(#[$attr])*
                    $pattern => $policy,
                )*
            }
        }
    };
}

define_method_policy_table! {
    (Method::AddNode { .. }, "AddNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::CreateNodeIfAbsent { .. }, "CreateNodeIfAbsent", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "atomic create returns true only to the inserting writer, so its result is not cross-request cacheable"),
    (Method::RemoveNode { .. }, "RemoveNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::HasNode { .. }, "HasNode", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetNodes, "GetNodes", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetNodesByLabel { .. }, "GetNodesByLabel", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetNodeProperties { .. }, "GetNodeProperties", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::CompareAndSetNodeFields { .. }, "CompareAndSetNodeFields", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::ClaimNext { .. }, "ClaimNext", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::DeclareExchange { .. }, "DeclareExchange", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::DeleteExchange { .. }, "DeleteExchange", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::BindQueue { .. }, "BindQueue", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::UnbindQueue { .. }, "UnbindQueue", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::Publish { .. }, "Publish", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)"),
    (Method::DeclareQueue { .. }, "DeclareQueue", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::PublishEx { .. }, "PublishEx", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)"),
    (Method::BrokerConsume { .. }, "BrokerConsume", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:consume", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::BrokerAck { .. }, "BrokerAck", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::BrokerReject { .. }, "BrokerReject", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::ClaimWorkItem { .. }, "ClaimWorkItem", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:claim", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "engine-native tenant/fair WorkItem lease claim"),
    (Method::AcquireCapacity { .. }, "AcquireCapacity", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "capacity:lease", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "atomic multi-dimensional capacity admission with epoch/fence ownership"),
    (Method::RenewCapacity { .. }, "RenewCapacity", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "capacity:lease", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "bounded all-or-nothing lease renewal"),
    (Method::ReleaseCapacity { .. }, "ReleaseCapacity", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "capacity:lease", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "bounded all-or-nothing lease release"),
    (Method::ReclaimExpiredCapacity { .. }, "ReclaimExpiredCapacity", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "capacity:lease", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "bounded expiry reclaim with native aggregate accounting"),
    (Method::ReconcileCapacity { .. }, "ReconcileCapacity", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "capacity:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "bounded native cells/leases reconciliation page"),
    (Method::CapacityStatus { .. }, "CapacityStatus", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "capacity:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "exact tenant-scoped native capacity status"),
    (Method::UpdateCapacityCell { .. }, "UpdateCapacityCell", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "capacity:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "controller epoch CAS for resource dimension/capacity policy"),
    (Method::SubmitWorkItem { .. }, "SubmitWorkItem", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:submit", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "native tenant-scoped WorkItem command-log admission and outbox commit"),
    (Method::SubmitWorkItems { .. }, "SubmitWorkItems", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:submit", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "bounded all-or-nothing WorkItem admission batch"),
    (Method::MintWorkItemClaimCapability { .. }, "MintWorkItemClaimCapability", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:claim-capability", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "opaque native capability is retained in a private ledger and never projected"),
    (Method::VerifyWorkItemClaimCapability { .. }, "VerifyWorkItemClaimCapability", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:claim-capability", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "linearizable live-lease check precedes private capability lookup"),
    (Method::RenewWorkItemLease { .. }, "RenewWorkItemLease", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "lease epoch and fencing token are validated atomically"),
    (Method::CommitWorkItemResult { .. }, "CommitWorkItemResult", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "terminal result references and outbox commit atomically"),
    (Method::CancelWorkItem { .. }, "CancelWorkItem", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "pending cancellation never steals an active lease"),
    (Method::DeferWorkItem { .. }, "DeferWorkItem", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "fenced lease release schedules retry without consuming an attempt"),
    (Method::CasWorkItemMetadata { .. }, "CasWorkItemMetadata", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "work:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "BUG-111: atomic single-field CAS on non-authority scheduling metadata (checkpoint_id/metadata/prio_bucket); status/lease/tenant are fenced but never written"),
    (Method::ReserveWorkItemResources { .. }, "ReserveWorkItemResources", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "resource:reserve", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "controller-only atomic host admission and WorkItem fence validation"),
    (Method::ReleaseWorkItemResources { .. }, "ReleaseWorkItemResources", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "resource:reserve", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "controller-only lifecycle release with retained tombstone"),
    (Method::ReclaimWorkItemResources { .. }, "ReclaimWorkItemResources", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "resource:reserve", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "controller-only expiry/supersession reclaim with retained tombstone"),
    (Method::QueryWorkItemReservation { .. }, "QueryWorkItemReservation", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "resource:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "linearizable exact native authority read"),
    (Method::ResourceReservationStatus { .. }, "ResourceReservationStatus", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "resource:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "bounded linearizable reconciliation read"),
    (Method::UpdateResourceHost { .. }, "UpdateResourceHost", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "resource:host", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "controller-only monotonic host telemetry update"),
    (Method::ReserveDevelopmentLane { .. }, "ReserveDevelopmentLane", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "lane:reserve", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "controller-only atomic branch/worktree uniqueness and multi-scope quota hold; now_ms is authority-normalized"),
    (Method::RenewDevelopmentLane { .. }, "RenewDevelopmentLane", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "lane:reserve", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "in-place O(1) hold renewal bound to the current WorkItem lease; now_ms is authority-normalized"),
    (Method::ObserveDevelopmentLane { .. }, "ObserveDevelopmentLane", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "lane:reserve", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "monotonic retained-footprint observation replaces the prior native charge; now_ms is authority-normalized"),
    (Method::FinishDevelopmentLane { .. }, "FinishDevelopmentLane", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "lane:reserve", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "terminal lifecycle releases active count but retains cleanup charges and identity; now_ms is authority-normalized"),
    (Method::CleanupDevelopmentLane { .. }, "CleanupDevelopmentLane", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "lane:cleanup", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "distinct cleanup WorkItem fence releases retained disk and exclusivity; now_ms is authority-normalized"),
    (Method::QueryDevelopmentLane { .. }, "QueryDevelopmentLane", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "lane:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "linearizable exact lane hold/tombstone read"),
    (Method::DevelopmentLaneStatus { .. }, "DevelopmentLaneStatus", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "lane:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "bounded tenant-scoped status with maintained counters"),
    (Method::UpdateDevelopmentLaneQuota { .. }, "UpdateDevelopmentLaneQuota", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "lane:quota", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "controller/admin-only monotonic server-owned quota policy with numeric expected_policy_revision CAS; now_ms is authority-normalized"),
    (Method::SweepExpired { .. }, "SweepExpired", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::StreamDeclare { .. }, "StreamDeclare", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::StreamPublish { .. }, "StreamPublish", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::StreamRead { .. }, "StreamRead", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "stream:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::StreamTrim { .. }, "StreamTrim", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::StreamCommitOffset { .. }, "StreamCommitOffset", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "stream:admin", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::StreamCommittedOffset { .. }, "StreamCommittedOffset", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "stream:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::PublishConfirmed { .. }, "PublishConfirmed", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)"),
    (Method::PublishIdempotent { .. }, "PublishIdempotent", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:publish", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)"),
    (Method::BrokerAckTag { .. }, "BrokerAckTag", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "current-generation result must not be replay-cached across requests"),
    (Method::BrokerNackTag { .. }, "BrokerNackTag", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "current-generation result must not be replay-cached across requests"),
    (Method::BrokerRenewTag { .. }, "BrokerRenewTag", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::Outbox, authz_action: "broker:ack", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "current-generation result must not be replay-cached across requests"),
    (Method::CreateSummaryNode { .. }, "CreateSummaryNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::Consolidate { .. }, "Consolidate", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::Reinforce { .. }, "Reinforce", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::DecayNode { .. }, "DecayNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::DecayMemories { .. }, "DecayMemories", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::EvictBelow { .. }, "EvictBelow", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::Maintain { .. }, "Maintain", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::SummaryChildren { .. }, "SummaryChildren", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "memory:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::SummariesAtLevel { .. }, "SummariesAtLevel", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "memory:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::AddSceneObject { .. }, "AddSceneObject", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "scene:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::SetPose { .. }, "SetPose", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "scene:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::Reparent { .. }, "Reparent", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "scene:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::WorldTransform { .. }, "WorldTransform", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "scene:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::SceneChildren { .. }, "SceneChildren", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "scene:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::StartTrajectory { .. }, "StartTrajectory", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::AppendStep { .. }, "AppendStep", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "memory:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::DiscountedReturn { .. }, "DiscountedReturn", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "memory:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::BestTrajectory { .. }, "BestTrajectory", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "memory:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetNodePropertiesBatch { .. }, "GetNodePropertiesBatch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::HasNodesBatch { .. }, "HasNodesBatch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::NodeCount, "NodeCount", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::NodeIds, "NodeIds", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::AddEdge { .. }, "AddEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "edge:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::RemoveEdge { .. }, "RemoveEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "edge:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::InvalidateEdge { .. }, "InvalidateEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "edge:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::SupersedeEdge { .. }, "SupersedeEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "edge:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::HasEdge { .. }, "HasEdge", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetEdges, "GetEdges", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetEdgesPage { .. }, "GetEdgesPage", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::ClearGraph, "ClearGraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:admin", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::GetEdgeProperties { .. }, "GetEdgeProperties", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetEdgePropertiesBatch { .. }, "GetEdgePropertiesBatch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::EdgeCount, "EdgeCount", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::InDegree { .. }, "InDegree", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::OutDegree { .. }, "OutDegree", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetPredecessors { .. }, "GetPredecessors", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetSuccessors { .. }, "GetSuccessors", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetNeighbors { .. }, "GetNeighbors", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetNeighborsBatch { .. }, "GetNeighborsBatch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "edge:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::UnionGetNodeProperties { .. }, "UnionGetNodeProperties", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::UnionGetNodesByLabel { .. }, "UnionGetNodesByLabel", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::UnionGetNeighbors { .. }, "UnionGetNeighbors", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::TopologicalSort, "TopologicalSort", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::FindCycle, "FindCycle", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetShortestPath { .. }, "GetShortestPath", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetBlastRadius { .. }, "GetBlastRadius", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::DegreeCentrality { .. }, "DegreeCentrality", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::DegreeCentralityAll, "DegreeCentralityAll", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::BetweennessCentrality, "BetweennessCentrality", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::PageRank { .. }, "PageRank", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::PersonalizedPageRank { .. }, "PersonalizedPageRank", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::ConnectedComponents, "ConnectedComponents", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::StronglyConnectedComponents, "StronglyConnectedComponents", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::MinimumSpanningTree, "MinimumSpanningTree", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::CommunityDetection { .. }, "CommunityDetection", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::CommunityDetectEphemeral { .. }, "CommunityDetectEphemeral", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GraphColoring, "GraphColoring", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::ComputeSimilarityEdges { .. }, "ComputeSimilarityEdges", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::ResolveCandidates { .. }, "ResolveCandidates", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::ClusterHierarchyRefresh { .. }, "ClusterHierarchyRefresh", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "VIZ-1 hierarchical Leiden clustering for million-node graph visualization: (re)computes and durably caches the cluster hierarchy in its own non-authoritative store (server::persistence::cluster_hierarchy_store), never as graph nodes/edges -- see ClusterHierarchyClusters/ClusterHierarchyExpand"),
    (Method::ClusterHierarchyClusters { .. }, "ClusterHierarchyClusters", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "VIZ-1: GET clusters(graph, level, parent_cluster_id?) from the cached hierarchy"),
    (Method::ClusterHierarchyExpand { .. }, "ClusterHierarchyExpand", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "VIZ-1: GET expand(graph, cluster_id) -- a level-1 cluster's member nodes/edges read live off the graph"),
    (Method::PruneByLifecycle { .. }, "PruneByLifecycle", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch commits the resulting authoritative image"),
    (Method::GetContextView { .. }, "GetContextView", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::BatchUpdate { .. }, "BatchUpdate", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::MultiGraphBatchUpdate { .. }, "MultiGraphBatchUpdate", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "node:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "durable parent coordinator with per-graph MutationBatch children"),
    (Method::Metrics, "Metrics", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::EvictLRU { .. }, "EvictLRU", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch commits the resulting authoritative image"),
    (Method::DecaySweep { .. }, "DecaySweep", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch commits the resulting authoritative image"),
    (Method::TouchNodes { .. }, "TouchNodes", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:admin", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch commits the resulting authoritative image"),
    (Method::ToMsgpack, "ToMsgpack", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::FromMsgpack { .. }, "FromMsgpack", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:admin", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch commits the imported authoritative image"),
    (Method::GetLedger, "GetLedger", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "ledger:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::ClearLedger, "ClearLedger", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "ledger:admin", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch"),
    (Method::ApplyLedger { .. }, "ApplyLedger", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "ledger:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch"),
    (Method::AuditVerify, "AuditVerify", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "security:audit", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::AuditProveInclusion { .. }, "AuditProveInclusion", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "security:audit", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "provenance anchoring: Merkle inclusion proof for one node against a prior PROVENANCE_ANCHOR audit-chain entry"),
    (Method::GetSubgraph { .. }, "GetSubgraph", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "node:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::Fork, "Fork", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "returns the forked snapshot to the caller; never registers/persists it server-side"),
    (Method::DiffAgainst { .. }, "DiffAgainst", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::CompactNodesByType { .. }, "CompactNodesByType", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:admin", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch"),
    (Method::RunDatalogReasoning { .. }, "RunDatalogReasoning", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "reasoning:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch commits inferred facts"),
    (Method::ApplyChangeEnvelope { .. }, "ApplyChangeEnvelope", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "ingest:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "Engine-native object/material/governance/version/cursor/outbox commit; verified context is mandatory"),
    (Method::ApplyChangeEnvelopes { .. }, "ApplyChangeEnvelopes", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "ingest:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "Batch envelope coordinator: one coalesced graph transaction per shard-partition; same policy class as ApplyChangeEnvelope"),
    (Method::GetChangeEnvelope { .. }, "GetChangeEnvelope", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "ingest:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "Verified tenant-scoped reconciliation read"),
    (Method::GetContentVersion { .. }, "GetContentVersion", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "ingest:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "Typed content versions are never compared lexically"),
    (Method::GetChangeCursor { .. }, "GetChangeCursor", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "ingest:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "Typed source cursors are tenant/graph/partition scoped"),
    #[cfg(feature = "modality-serving")]
    (Method::ServedModality { .. }, "ServedModality", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "modality:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "runtime-conditional: authority/query/events/capabilities are verified read snapshots; ingest/delete/cold/restore commit an encrypted state-backed MutationBatch"),
    (Method::CreateGraph { .. }, "CreateGraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "native lifecycle MutationBatch before registry publication"),
    (Method::DeleteGraph { .. }, "DeleteGraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "native lifecycle MutationBatch before registry eviction"),
    (Method::ListGraphs, "ListGraphs", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "graph:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::Reshard { .. }, "Reshard", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed admin MutationBatch saga"),
    (Method::CatalogAssign { .. }, "CatalogAssign", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed admin MutationBatch saga"),
    (Method::CatalogReassign { .. }, "CatalogReassign", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed admin MutationBatch saga"),
    (Method::CatalogRemove { .. }, "CatalogRemove", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed admin MutationBatch saga"),
    (Method::CatalogList, "CatalogList", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster-read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::RebalancePlan { .. }, "RebalancePlan", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:cluster-read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::RebalanceExecute { .. }, "RebalanceExecute", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed admin MutationBatch saga"),
    (Method::PlacementRoute { .. }, "PlacementRoute", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cluster:placement-read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "engine-authoritative complete route; single-node returns authoritative unplaced group 0/epoch 0, while clustered routing requires a live MultiRaft control leader; GOC-15/BUG-030 narrowed off admin:cluster-read (2026-08-17) -- ordinary kg:read/kg:write routes their OWN tenant, handlers::placement::handle_route requires kg:admin for any other tenant's route"),
    (Method::RaftAddLearner { .. }, "RaftAddLearner", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "leader-only openraft add_learner; attaches a non-voting replica without changing the voter set"),
    (Method::RaftChangeMembership { .. }, "RaftChangeMembership", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "leader-only openraft change_membership; sets the group's exact voter set (the usual way to promote a learner added via RaftAddLearner)"),
    (Method::ClusterMembers, "ClusterMembers", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cluster:topology-read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "ADR-1/W1.1 engine-authoritative client topology; deliberately NOT admin:cluster-read -- ordinary service roles need it to re-resolve after a failover; answered from any node, not just the leader"),
    (Method::NodeInfoUpsert { .. }, "NodeInfoUpsert", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "ADR-1/W1.1 per-node self-report into the durable cluster-topology store (server::persistence::node_info_store); issued only by the node's own Raft startup path, like CatalogAssign above -- NOT graph nodes (placement's O(N) lesson)"),
    (Method::RegisterServer { .. }, "RegisterServer", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "registry:write", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "W2.5 fleet server push-registration/heartbeat: self-translates into Method::AddNode against __commons__ (dispatch.rs), writing a REAL :Server graph node -- unlike NodeInfoUpsert above, this one IS a KG entity the fleet queries"),
    (Method::PlacementAdmin { .. }, "PlacementAdmin", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:cluster", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "raft-replicated placement-catalog admin op (Assign/Move/AbortMove, the placement DECISION + PLAN->EXECUTE->CATALOG-UPDATE legs): MultiRaft::placement_assign / TenantManager::move_partition / abort_move commit through the DEFAULT group's own client_write / commit_placement, not this gateway's per-graph MutationBatch"),
    (Method::Backup { .. }, "Backup", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:backup", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "reads a consistent snapshot out to a bundle; does not mutate the live graph"),
    (Method::Restore { .. }, "Restore", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:backup", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed admin MutationBatch saga"),
    (Method::CreateChannel { .. }, "CreateChannel", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch; message/member payloads stay out of the ledger"),
    (Method::JoinChannel { .. }, "JoinChannel", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
    (Method::LeaveChannel { .. }, "LeaveChannel", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
    (Method::CloseChannel { .. }, "CloseChannel", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "channel:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
    (Method::SendMessage { .. }, "SendMessage", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "channel:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "request-scoped opaque session-control receipt prevents acknowledgement-lost duplicate sends"),
    (Method::GetChannelMessages { .. }, "GetChannelMessages", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "channel:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::ListChannels, "ListChannels", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "channel:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::GetChannelMembers { .. }, "GetChannelMembers", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "channel:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::Ping, "Ping", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::Health, "Health", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::Shutdown, "Shutdown", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::VolatileControl, authz_action: "service:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, "explicitly ephemeral process control; never acknowledges a user-data commit"),
    (Method::CancelRequest { .. }, "CancelRequest", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::ResourceStats, "ResourceStats", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::ResourceStatsPage { .. }, "ResourceStatsPage", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "service:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, "bounded ACL-filtered keyset page; summary suppresses detail arrays"),
    (Method::Reconcile { .. }, "Reconcile", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Saga }, "state-backed MutationBatch commits the merged image"),
    (Method::ApplyMutation { .. }, "ApplyMutation", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graph:write", idempotent: false, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch"),
    (Method::Vf2SubgraphMatch { .. }, "Vf2SubgraphMatch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:graph-algo", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::ParseFile { .. }, "ParseFile", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:parse", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::ParseFiles { .. }, "ParseFiles", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:parse", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::IndexRepository { .. }, "IndexRepository", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:parse", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::ObserveScreen { .. }, "ObserveScreen", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:vision", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::AddEmbedding { .. }, "AddEmbedding", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "node:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::SemanticSearch { .. }, "SemanticSearch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::Discover { .. }, "Discover", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::MatchOntologyTerms { .. }, "MatchOntologyTerms", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::BatchL2Normalize { .. }, "BatchL2Normalize", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:semantic", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::FinanceOptimizePortfolio { .. }, "FinanceOptimizePortfolio", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceRiskParity { .. }, "FinanceRiskParity", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceBlackLitterman { .. }, "FinanceBlackLitterman", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceEfficientFrontier { .. }, "FinanceEfficientFrontier", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsLinearRegression { .. }, "DsLinearRegression", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsKMeans { .. }, "DsKMeans", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsPca { .. }, "DsPca", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsComputeStats { .. }, "DsComputeStats", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsTrainTestSplit { .. }, "DsTrainTestSplit", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsFitEstimator { .. }, "DsFitEstimator", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsPredictEstimator { .. }, "DsPredictEstimator", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsSoftmax { .. }, "DsSoftmax", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsLogSoftmax { .. }, "DsLogSoftmax", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsCrossEntropy { .. }, "DsCrossEntropy", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsDpoLoss { .. }, "DsDpoLoss", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsGrpoSurrogate { .. }, "DsGrpoSurrogate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsKlDivergence { .. }, "DsKlDivergence", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsAdamStep { .. }, "DsAdamStep", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::DsSgdStep { .. }, "DsSgdStep", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:datascience", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceVar { .. }, "FinanceVar", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceCvar { .. }, "FinanceCvar", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceMaxDrawdown { .. }, "FinanceMaxDrawdown", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceDrawdownSeries { .. }, "FinanceDrawdownSeries", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceDownsideDeviation { .. }, "FinanceDownsideDeviation", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceRiskMetrics { .. }, "FinanceRiskMetrics", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceMonteCarloVar { .. }, "FinanceMonteCarloVar", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceStressTest { .. }, "FinanceStressTest", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceDetectRegimes { .. }, "FinanceDetectRegimes", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceRollingZscore { .. }, "FinanceRollingZscore", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceEwma { .. }, "FinanceEwma", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceSignalDecay { .. }, "FinanceSignalDecay", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceCombineAlphas { .. }, "FinanceCombineAlphas", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceCrossSectionalRank { .. }, "FinanceCrossSectionalRank", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceMomentum { .. }, "FinanceMomentum", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceMeanReversion { .. }, "FinanceMeanReversion", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceInformationCoefficient { .. }, "FinanceInformationCoefficient", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceTwap { .. }, "FinanceTwap", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceVwap { .. }, "FinanceVwap", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceMarketImpact { .. }, "FinanceMarketImpact", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinancePairsTrading { .. }, "FinancePairsTrading", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceMatchOrders { .. }, "FinanceMatchOrders", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceAvellanedaStoikov { .. }, "FinanceAvellanedaStoikov", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceGltQuotes { .. }, "FinanceGltQuotes", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceLogitQuotes { .. }, "FinanceLogitQuotes", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceGlostenMilgromSpread { .. }, "FinanceGlostenMilgromSpread", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceExpectedPnlRate { .. }, "FinanceExpectedPnlRate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceBreakevenAlpha { .. }, "FinanceBreakevenAlpha", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceOfiSeries { .. }, "FinanceOfiSeries", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceMicropriceSeries { .. }, "FinanceMicropriceSeries", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceVpinPm { .. }, "FinanceVpinPm", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceHawkesMle { .. }, "FinanceHawkesMle", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceHardimanBouchaud { .. }, "FinanceHardimanBouchaud", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceKyleLambda { .. }, "FinanceKyleLambda", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceSurveillanceRisk { .. }, "FinanceSurveillanceRisk", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceKellyFraction { .. }, "FinanceKellyFraction", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceBayesianKelly { .. }, "FinanceBayesianKelly", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinancePosteriorCredibleInterval { .. }, "FinancePosteriorCredibleInterval", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinancePurgedCpcv { .. }, "FinancePurgedCpcv", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceDeflatedSharpe { .. }, "FinanceDeflatedSharpe", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceProbabilityBacktestOverfit { .. }, "FinanceProbabilityBacktestOverfit", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceDieboldMariano { .. }, "FinanceDieboldMariano", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceForensicReport { .. }, "FinanceForensicReport", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceKalmanFilter1d { .. }, "FinanceKalmanFilter1d", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceKalmanBeta { .. }, "FinanceKalmanBeta", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceKalmanVolatility { .. }, "FinanceKalmanVolatility", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceAdfTest { .. }, "FinanceAdfTest", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceOuCalibrate { .. }, "FinanceOuCalibrate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceOuOptimalThresholds { .. }, "FinanceOuOptimalThresholds", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceMarkovTransitionMatrix { .. }, "FinanceMarkovTransitionMatrix", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceOrderBookImbalance { .. }, "FinanceOrderBookImbalance", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceQueueImbalance { .. }, "FinanceQueueImbalance", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceRealizedVolTick { .. }, "FinanceRealizedVolTick", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceSpreadReversion { .. }, "FinanceSpreadReversion", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceInformationRatio { .. }, "FinanceInformationRatio", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceEffectiveIndependentN { .. }, "FinanceEffectiveIndependentN", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceAlphaCombinationEngine { .. }, "FinanceAlphaCombinationEngine", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceBrierScore { .. }, "FinanceBrierScore", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceConvergenceGate { .. }, "FinanceConvergenceGate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceEmpiricalKelly { .. }, "FinanceEmpiricalKelly", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceSabrImpliedVol { .. }, "FinanceSabrImpliedVol", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceSabrSmile { .. }, "FinanceSabrSmile", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::FinanceSabrCalibrate { .. }, "FinanceSabrCalibrate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "compute:finance", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, ""),
    (Method::RegisterIdentity { .. }, "RegisterIdentity", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "security:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "RBAC/identity snapshot and MutationBatch metadata share one rbac.redb WTX"),
    (Method::RbacAdmin { .. }, "RbacAdmin", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "security:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "runtime-conditional: List is a read; role and grant updates share one rbac.redb WTX with MutationBatch metadata"),
    (Method::GetIdentity { .. }, "GetIdentity", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "security:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "identity read-back closing the RegisterIdentity blind-upsert gap: None means unregistered/unknown, Some(identity) with empty roles means registered-and-confirmed-empty -- gated security:admin like RegisterIdentity/RbacAdmin so it grants no caller new privilege"),
    (Method::ApplyMultisigMutation { .. }, "ApplyMultisigMutation", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "security:admin", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Saga }, "threshold validation translates into the graph MutationBatch gateway"),
    #[cfg(feature = "jobs")]
    (Method::AnalyticsJob { .. }, "AnalyticsJob", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::JobsRedb, authz_action: "jobs:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "runtime-conditional: Status is a read; Submit/Cancel/Resume commit through the native jobs.redb MutationBatch gateway"),
    #[cfg(feature = "statechart")]
    (Method::Statechart { .. }, "Statechart", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::StatechartRedb, authz_action: "statechart:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "runtime-conditional: GetState/List are reads; Define/Instantiate/SendEvent commit to the native statecharts.redb store (CONCEPT:INT-P2-2)"),
    #[cfg(feature = "quantum")]
    (Method::Quantum { .. }, "Quantum", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "quantum:run", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, "self-routes before dispatch_graph_op like AnalyticsJob/Statechart, never reaches the graph tamper-evident audit chain; R5 override audit instead rides the response's PlannerDecision.audit trail into the agent-utilities :ToolCall/:QuantumJob provenance"),
    #[cfg(feature = "asr-native")]
    (Method::Asr { .. }, "Asr", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "asr:transcribe", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, "self-routes before dispatch_graph_op like Quantum/Viz; direct non-durable whisper-rs transcription, commits no asr.result.v1 (that governed commit is future worker/AU-orchestration work, W03/W06)"),
    #[cfg(feature = "viz")]
    (Method::Viz { .. }, "Viz", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "viz:render", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, "pure compute: resolves a fresh per-request ColumnStore and returns rendered bytes, no durable write (D-VZ-1 lanes V4/V6)"),
    #[cfg(feature = "policy_export")]
    (Method::PolicyExport { .. }, "PolicyExport", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "policy:export", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "CA-16 (DEC-CA-04): renders the live IsolationLayer M1 row-visibility predicate set + a caller-supplied Marking bridge as one policy bundle; never reads GraphView/project_core, so no per-row RLS applies to the bundle's OWN contents -- see server::policy_export's module doc"),
    #[cfg(feature = "tts-piper")]
    (Method::TtsSynthesize { .. }, "TtsSynthesize", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "tts:synthesize", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::None }, "pure compute: native Piper-ONNX synthesis runs inline and returns audio, no durable graph write (GOC-34, no CAS/rendition publication exists yet)"),
    (Method::Sql { .. }, "Sql", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "query:sql", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "runtime-conditional; graph DML uses staged graph state while table/catalog writes atomically commit SQL rows plus MutationBatch status/fence/idempotency/outbox"),
    (Method::CypherQuery { .. }, "CypherQuery", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "query:cypher", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "runtime-conditional; writes execute against a staged graph and publish only after durable MutationBatch commit"),
    (Method::GraphQl { .. }, "GraphQl", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "query:graphql", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "runtime-conditional; ordinary writes stage through MutationBatch and cross-modal commit atomically includes universal status/fence/idempotency/outbox"),
    #[cfg(feature = "knowledge-batch")]
    (Method::KnowledgeStream { .. }, "KnowledgeStream", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:stream", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "one RequestContext/RLS/placement-bound stream with the sole native Arrow IPC projection for all seven query families"),
    (Method::UnifiedQuery { .. }, "UnifiedQuery", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:unified", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::UnifiedQueryText { .. }, "UnifiedQueryText", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:unified", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::ExplainPlan { .. }, "ExplainPlan", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::ExplainProvenance { .. }, "ExplainProvenance", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::ExplainProvenanceByIds { .. }, "ExplainProvenanceByIds", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "CONCEPT:EG-KB-CURRENCY — ID-seeded sibling of ExplainProvenance, same policy profile"),
    (Method::ExplainPolicy { .. }, "ExplainPolicy", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::ExplainBelief { .. }, "ExplainBelief", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::EpistemicStatus { .. }, "EpistemicStatus", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "L53 (EPI-P3-5) acceptance capstone; handler additionally gated `epistemic-tms`"),
    (Method::WhatChanged { .. }, "WhatChanged", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "L53 (EPI-P3-5) bitemporal diff; handler additionally gated `epistemic-tms`"),
    (Method::RecomputeMaterialization { .. }, "RecomputeMaterialization", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ReasoningProjection, authz_action: "reasoning:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "fenced recompute/writeback resolves provenance from the authoritative graph and fsyncs the per-graph projection"),
    (Method::MaterializationStatus { .. }, "MaterializationStatus", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "read-only status from the durable per-graph incremental reasoning authority"),
    (Method::StaleMaterializations, "StaleMaterializations", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "bulk opaque stale references from the durable per-graph incremental reasoning authority"),
    (Method::ResolveConflict { .. }, "ResolveConflict", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "EPI-P3-7 (gap-fill) standalone Dung argumentation (grounded/preferred/stable) conflict resolution over a BeliefGraph snapshot; handler additionally gated `epistemic-tms`"),
    (Method::ExplainEvidence { .. }, "ExplainEvidence", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "CONCEPT:EG-X1 multimodal-citation resolver; handler additionally gated `evidence-graph`"),
    (Method::CausalEstimate { .. }, "CausalEstimate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "EPI-P3-3/P3-6 do-calculus intervention OR observational conditioning (selected by `mode`) over a request-carried SCM; handler additionally gated `epistemic-causal`"),
    (Method::CausalCounterfactual { .. }, "CausalCounterfactual", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "EPI-P3-6 Pearl point-counterfactual over a request-carried SCM + a fully-observed unit; handler additionally gated `epistemic-causal`"),
    (Method::RankByProvenance { .. }, "RankByProvenance", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "explain:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "EPI-P3-3 provenance-aware retrieval ranking; handler additionally gated `epistemic-causal`"),
    (Method::NlQuery { .. }, "NlQuery", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "query:nl", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::RegisterForeignSource { .. }, "RegisterForeignSource", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "federation:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed control receipt; endpoint configuration is not duplicated in the ledger"),
    (Method::RegisterUdf { .. }, "RegisterUdf", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "udf:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed control receipt; module bytes are not duplicated in the ledger"),
    (Method::RunUdf { .. }, "RunUdf", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "udf:exec", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "executes a registered sandboxed function; treated as read/compute unless the UDF itself writes back (not modeled -- the wire protocol has no writeback flag here)"),
    (Method::DistributedCompute { .. }, "DistributedCompute", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "distcompute:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "read-only Pregel/GAS computation; materialization uses the distinct Create*/Refresh* methods"),
    (Method::CreateMatView { .. }, "CreateMatView", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed control-plane MutationBatch saga"),
    (Method::GetMatView { .. }, "GetMatView", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "matview:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::RefreshMatView { .. }, "RefreshMatView", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed control-plane MutationBatch saga"),
    (Method::PlanMatViewDefine { .. }, "PlanMatViewDefine", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed control-plane MutationBatch saga"),
    (Method::PlanMatViewGet { .. }, "PlanMatViewGet", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "matview:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::PlanMatViewRefresh { .. }, "PlanMatViewRefresh", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed control-plane MutationBatch saga"),
    (Method::PlanMatViewDrop { .. }, "PlanMatViewDrop", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "matview:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "prepared/committed control-plane MutationBatch saga"),
    (Method::BeginTxn { .. }, "BeginTxn", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:control", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native transaction staging authority"),
    (Method::TxnAddNode { .. }, "TxnAddNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native staging; Commit owns graph publication"),
    (Method::TxnRemoveNode { .. }, "TxnRemoveNode", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native staging; Commit owns graph publication"),
    (Method::TxnAddEdge { .. }, "TxnAddEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native staging; Commit owns graph publication"),
    (Method::TxnRemoveEdge { .. }, "TxnRemoveEdge", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native staging; Commit owns graph publication"),
    (Method::TxnCas { .. }, "TxnCas", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native staging; Commit owns graph publication"),
    (Method::TxnAddEmbedding { .. }, "TxnAddEmbedding", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
    (Method::TxnBlobRef { .. }, "TxnBlobRef", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
    (Method::TxnAddMeasurement { .. }, "TxnAddMeasurement", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
    (Method::TxnAxiom { .. }, "TxnAxiom", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
    (Method::TxnConstruct { .. }, "TxnConstruct", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
    (Method::TxnPlanWriteback { .. }, "TxnPlanWriteback", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
    (Method::TxnMaterializeBelief { .. }, "TxnMaterializeBelief", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native cross-modal staging"),
    (Method::TxnUnifiedQuery { .. }, "TxnUnifiedQuery", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "txn:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, ""),
    (Method::TxnUnifiedQueryText { .. }, "TxnUnifiedQueryText", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "txn:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, ""),
    (Method::Commit { .. }, "Commit", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:control", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "named parent receipt plus atomic graph/cross-modal child batches"),
    (Method::Rollback { .. }, "Rollback", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "txn:control", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "encrypted Raft-native transaction staging removal"),
    (Method::TsAppend { .. }, "TsAppend", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::SeriesRedb, authz_action: "timeseries:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "graph ACL + placement policy precede the tenant/graph/series-scoped series.redb write"),
    (Method::TsRange { .. }, "TsRange", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "timeseries:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::TsAsofJoin { .. }, "TsAsofJoin", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "timeseries:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::TsWindow { .. }, "TsWindow", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "timeseries:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::TsGapFill { .. }, "TsGapFill", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "timeseries:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::TsEvict { .. }, "TsEvict", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::SeriesRedb, authz_action: "timeseries:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "content-idempotent unlike TsAppend: re-evicting an already-past cutoff is a safe no-op (see SeriesStore::evict_before)"),
    (Method::TsDeleteSeries { .. }, "TsDeleteSeries", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::SeriesRedb, authz_action: "timeseries:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "content-idempotent unlike TsAppend: re-deleting an already-gone series is a safe no-op (see SeriesStore::delete_series)"),
    (Method::TsListSeries, "TsListSeries", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "timeseries:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::BlobBegin { .. }, "BlobBegin", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "multi-call chunked-upload protocol (Begin ... ChunkPut* ... Commit); no single-call atomicity; durable via its own blob.redb (group-committed Immediate), self-routes before dispatch_graph_op"),
    (Method::BlobChunkPut { .. }, "BlobChunkPut", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "durable via its own blob.redb (group-committed Immediate); self-routes before dispatch_graph_op"),
    (Method::BlobCommit { .. }, "BlobCommit", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "multi-call chunked-upload protocol (Begin ... ChunkPut* ... Commit); no single-call atomicity; durable via its own blob.redb (group-committed Immediate), self-routes before dispatch_graph_op"),
    (Method::BlobFetchBegin { .. }, "BlobFetchBegin", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "blob:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::BlobChunkGet { .. }, "BlobChunkGet", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "blob:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::BlobFetchEnd { .. }, "BlobFetchEnd", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "blob:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::BlobRef { .. }, "BlobRef", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "refcount increment; idempotent-ish but re-invocation adds another ref, so not idempotent; durable via blob.redb"),
    (Method::BlobUnref { .. }, "BlobUnref", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via blob.redb"),
    (Method::BlobGc, "BlobGc", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::BlobRedb, authz_action: "blob:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via blob.redb"),
    (Method::KvGet { .. }, "KvGet", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "kv:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::KvPut { .. }, "KvPut", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::KvRedb, authz_action: "kv:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack); self-routes before graph dispatch"),
    (Method::KvDelete { .. }, "KvDelete", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::KvRedb, authz_action: "kv:write", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via its own kv.redb (redb::Durability::Immediate); self-routes before dispatch_graph_op"),
    (Method::KvScan { .. }, "KvScan", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "kv:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::KvCas { .. }, "KvCas", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::KvRedb, authz_action: "kv:write", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack); self-routes before graph dispatch"),
    (Method::ImportSqliteFile { .. }, "ImportSqliteFile", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "admin:sqlite-file", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "native SQL-catalog MutationBatch; logical transfer name is excluded from the durable receipt"),
    (Method::ExportSqliteFile { .. }, "ExportSqliteFile", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "admin:sqlite-file", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "operator-provisioned transfer root; logical filenames only"),
    (Method::AddTriples { .. }, "AddTriples", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "rdf:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::GetRdf, "GetRdf", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "rdf:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::RemoveTriples { .. }, "RemoveTriples", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "rdf:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::DropNamedGraph, "DropNamedGraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "rdf:write", idempotent: true, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, ""),
    (Method::Sparql { .. }, "Sparql", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "sparql:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::SparqlVirtual { .. }, "SparqlVirtual", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "sparql:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::OwlReason { .. }, "OwlReason", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "owl:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::OwlReasonDistributed { .. }, "OwlReasonDistributed", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "owl:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::OwlExplain { .. }, "OwlExplain", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "owl:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::RunRules { .. }, "RunRules", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "reasoning:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "READ-ONLY (EG-P0-2/L11 handler audit): handle_run_rules reasons over an off-lock analysis_snapshot and returns inferred triples, no writeback -- unlike its sibling RunDatalogReasoning which materialises in-place. Corrected from a prior mutates=true semantic guess; now agrees with access.rs (never a write there)"),
    (Method::ShaclValidate { .. }, "ShaclValidate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "validation:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::IcvConfigure { .. }, "IcvConfigure", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "security:admin", idempotent: true, audited: true, emits_cdc: true, txn_participation: TxnParticipation::Atomic }, "state-backed MutationBatch"),
    (Method::ShexValidate { .. }, "ShexValidate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "validation:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::CdcRead { .. }, "CdcRead", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::RegisterContinuousQuery { .. }, "RegisterContinuousQuery", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
    (Method::ReadContinuousQuery { .. }, "ReadContinuousQuery", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::DropContinuousQuery { .. }, "DropContinuousQuery", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
    (Method::Watch { .. }, "Watch", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: false, audited: false, emits_cdc: false, txn_participation: TxnParticipation::None }, "opens a push subscription; not a snapshot read nor a mutation"),
    (Method::RegisterTrigger { .. }, "RegisterTrigger", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
    (Method::DropTrigger { .. }, "DropTrigger", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "cdc:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
    (Method::ListTriggers { .. }, "ListTriggers", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::FiredTriggers { .. }, "FiredTriggers", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cdc:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::CepSubscribe { .. }, "CepSubscribe", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "cep:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
    (Method::CepPoll { .. }, "CepPoll", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "cep:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, ""),
    (Method::CepUnsubscribe { .. }, "CepUnsubscribe", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::ControlRedb, authz_action: "cep:admin", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Saga }, "opaque prepared/committed session-control MutationBatch"),
    (Method::MineAssociate { .. }, "MineAssociate", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::MineCluster { .. }, "MineCluster", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::MineAnomaly { .. }, "MineAnomaly", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::MineClassifyFit { .. }, "MineClassifyFit", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "mining:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "the one Mine* family member that is unconditionally read-only (produces a model blob, never writes back)"),
    (Method::MineClassifyPredict { .. }, "MineClassifyPredict", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::MineReduce { .. }, "MineReduce", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::GraphLearnFit { .. }, "GraphLearnFit", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graphlearn:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::GraphLearnPredict { .. }, "GraphLearnPredict", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "graphlearn:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::MiningPipelineTrain { .. }, "MiningPipelineTrain", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field (persists a versioned :Model artifact)"),
    (Method::MiningPipelineServe { .. }, "MiningPipelineServe", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "always writes the :ServedModel pointer to deploy a version"),
    (Method::MiningPipelinePredict { .. }, "MiningPipelinePredict", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field (materializes :Prediction nodes)"),
    (Method::MiningPipelineEvaluate { .. }, "MiningPipelineEvaluate", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "mining:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "read-only: scores a stored versioned model against a labeled set"),
    (Method::MiningPipelineCompare { .. }, "MiningPipelineCompare", MethodPolicy { mutates: false, durability_domain: DurabilityDomain::None, authz_action: "mining:read", idempotent: true, audited: false, emits_cdc: false, txn_participation: TxnParticipation::Snapshot }, "read-only: diffs two model versions' held-out metrics"),
    (Method::MineSequence { .. }, "MineSequence", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound; writeback=true enters the canonical durable mutation path"),
    (Method::MineForecast { .. }, "MineForecast", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound; writeback=true enters the canonical durable mutation path"),
    (Method::MineText { .. }, "MineText", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound; writeback=true for lda/nmf enters the canonical durable mutation path"),
    (Method::MineSubgraph { .. }, "MineSubgraph", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound; writeback=true for gspan enters the canonical durable mutation path"),
    (Method::MineEntityResolve { .. }, "MineEntityResolve", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::MineCausalImpact { .. }, "MineCausalImpact", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::MineProcess { .. }, "MineProcess", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::MineRootCause { .. }, "MineRootCause", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::MineRiskPropagation { .. }, "MineRiskPropagation", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::MineOntologyGap { .. }, "MineOntologyGap", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::MineRetrievalQuality { .. }, "MineRetrievalQuality", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
    (Method::MineCommunity { .. }, "MineCommunity", MethodPolicy { mutates: true, durability_domain: DurabilityDomain::GraphRedb, authz_action: "mining:write", idempotent: false, audited: true, emits_cdc: false, txn_participation: TxnParticipation::Atomic }, "mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field"),
}

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
            // internal self-consistency of the static documentation table. The policy
            // lookup and its return-path invariants guard the served policy; the mirrored
            // classifier comparisons live in `tests/consistency.rs`.
            let _ = table_policy;
        }
        // The ledger currently contains 412 rows: 403 unconditional rows plus one
        // row for each of the nine feature-gated surfaces below. Keep this formula
        // aligned with the cfg rows in the macro invocation so every supported
        // feature combination checks the same coverage invariant.
        let expected = 403
            + usize::from(cfg!(feature = "jobs"))
            + usize::from(cfg!(feature = "statechart"))
            + usize::from(cfg!(feature = "modality-serving"))
            + usize::from(cfg!(feature = "knowledge-batch"))
            + usize::from(cfg!(feature = "quantum"))
            + usize::from(cfg!(feature = "viz"))
            + usize::from(cfg!(feature = "asr-native"))
            + usize::from(cfg!(feature = "tts-piper"))
            + usize::from(cfg!(feature = "policy_export"));
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
        // TsAppend/TsRange/TsAsofJoin/TsWindow/TsGapFill (5) plus the retention-
        // reachability wiring's TsEvict/TsDeleteSeries/TsListSeries (3) = 8.
        assert_eq!(ts.len(), 8);
        assert_eq!(
            ts.iter()
                .filter(|row| row.access == PolicyAccess::Write)
                .count(),
            // TsAppend, plus TsEvict/TsDeleteSeries (content-idempotent unlike
            // TsAppend, but still series.redb WRITES -- see their MethodPolicy).
            3
        );
        assert_eq!(
            ts.iter()
                .filter(|row| row.access == PolicyAccess::Read)
                .count(),
            // TsRange/TsAsofJoin/TsWindow/TsGapFill, plus TsListSeries.
            5
        );
    }

    #[test]
    fn runtime_conditional_policy_uses_query_mode_and_modality_operation() {
        let read = Method::CypherQuery {
            query: String::new(),
            mode: CypherMode::Read,
        };
        let write = Method::CypherQuery {
            query: String::new(),
            mode: CypherMode::Write,
        };
        assert_eq!(
            policy(&read),
            MethodPolicy {
                mutates: false,
                durability_domain: DurabilityDomain::None,
                authz_action: "query:cypher",
                idempotent: true,
                audited: false,
                emits_cdc: false,
                txn_participation: TxnParticipation::Snapshot,
            }
        );
        assert_eq!(
            policy(&write),
            MethodPolicy {
                mutates: true,
                durability_domain: DurabilityDomain::GraphRedb,
                authz_action: "query:cypher",
                idempotent: false,
                audited: true,
                emits_cdc: false,
                txn_participation: TxnParticipation::Atomic,
            }
        );

        #[cfg(feature = "modality-serving")]
        {
            use eg_types::modality::{ServedModalityKind, ServedModalityOp};

            let query = Method::ServedModality {
                op: ServedModalityOp::Query {
                    modality: ServedModalityKind::Document,
                    segment_kind: None,
                    after_occurrence_id: None,
                    limit: 1,
                    include_cold: false,
                },
            };
            let cold = Method::ServedModality {
                op: ServedModalityOp::MoveToCold {
                    modality: ServedModalityKind::Document,
                    occurrence_id: String::new(),
                },
            };
            assert_eq!(
                policy(&query),
                MethodPolicy {
                    mutates: false,
                    durability_domain: DurabilityDomain::None,
                    authz_action: "modality:read",
                    idempotent: true,
                    audited: false,
                    emits_cdc: false,
                    txn_participation: TxnParticipation::Snapshot,
                }
            );
            assert_eq!(
                policy(&cold),
                MethodPolicy {
                    mutates: true,
                    durability_domain: DurabilityDomain::GraphRedb,
                    authz_action: "modality:write",
                    idempotent: false,
                    audited: true,
                    emits_cdc: true,
                    txn_participation: TxnParticipation::Atomic,
                }
            );
        }
    }
}
