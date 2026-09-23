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
//! idempotency/audit/CDC/transaction-participation profile. Each domain owns its current
//! declarations; [`method_policy_entries`] is the sole deterministic registry iterator, and
//! [`policy`] resolves runtime requests through that registry. Inventory parity checks reject
//! a missing or duplicate `Method` declaration.
//!
//! This crate defines the domain policy registry, the generated
//! Markdown ledger (rendered by [`gen_ledger`], written by the `gen_contract`
//! generator), and consistency tests against the remaining
//! classifiers. Served mutation planning consumes this policy directly; the snapshot
//! cross-checks remain as drift alarms for classifiers that have not yet been deleted.
//!
//! ## Crate shape
//!
//! A leaf crate: the ONLY dependency is `eg-types` (with every one of its optional features
//! turned on -- see `Cargo.toml` for why). It is not a dependency of the main
//! `epistemic-graph` package's default build; see the root `Cargo.toml`'s `members` comment.

pub mod catalog;
#[cfg(feature = "contract-schema")]
pub mod contract;
mod descriptor;
mod domains;

pub use catalog::{method_schema, CONTRACT_CATALOG_DIGEST, METHOD_CATALOG};

pub use descriptor::{
    error_set_for, format_identities_for, replay_class_for, ConsumerProfile, MethodDescriptor,
    MethodId, MethodSpec, ReplayClass, Stability,
};

use eg_types::protocol::{CypherMode, Method};

/// The shape EVERY runtime-conditional native owner has: a snapshot read when
/// its op reads, and one write in its own durable domain when its op writes.
///
/// Eight surfaces were each spelling this out, which is eight places for the
/// read half to drift. What actually differs between them is the three values
/// this takes, so those are what each caller now states.
///
/// `audited`/`emits_cdc` are false for all of them: a native owner's typed
/// revision, action-provenance, replay and outbox rows ARE its audit and
/// projection authority, and the generic graph classifiers do not apply to a
/// self-routing surface.
fn native_owner_policy(
    mutates: bool,
    durable: DurabilityDomain,
    authz_action: &'static str,
    committed: TxnParticipation,
) -> MethodPolicy {
    MethodPolicy {
        mutates,
        durability_domain: if mutates {
            durable
        } else {
            DurabilityDomain::None
        },
        authz_action,
        idempotent: true,
        audited: false,
        emits_cdc: false,
        txn_participation: if mutates {
            committed
        } else {
            TxnParticipation::Snapshot
        },
    }
}

fn agent_library_policy(op: &eg_types::agent_library::AgentLibraryOp) -> MethodPolicy {
    let mutates = matches!(
        op,
        eg_types::agent_library::AgentLibraryOp::Publish { .. }
            | eg_types::agent_library::AgentLibraryOp::Retire { .. }
    );
    native_owner_policy(
        mutates,
        DurabilityDomain::ControlRedb,
        if mutates {
            "agent:library-write"
        } else {
            "agent:library-read"
        },
        TxnParticipation::Atomic,
    )
}

/// An agent graph is published into the SAME ControlRedb owner as an agent
/// entry, so it takes the same policy shape. The one thing it does not share is
/// the authz action: composing agents into a runnable graph is a distinct
/// privilege from publishing one agent, and collapsing them would mean anyone
/// who can publish an agent can also wire arbitrary agents together.
fn agent_graph_policy(op: &eg_types::agent_graph::AgentGraphOp) -> MethodPolicy {
    let mutates = op.is_mutation();
    native_owner_policy(
        mutates,
        DurabilityDomain::ControlRedb,
        if mutates {
            "agent:graph-write"
        } else {
            "agent:graph-read"
        },
        TxnParticipation::Atomic,
    )
}

/// Components take the same shape as the two layers above them, with their own
/// authz action: curating what agents may be BUILT from is a distinct privilege
/// from assembling an agent out of already-approved parts. Collapsing them
/// would mean anyone who can publish an agent can also introduce a new
/// side-effecting tool for it to use.
fn agent_component_policy(op: &eg_types::agent_component::AgentComponentOp) -> MethodPolicy {
    // The op owns the action mapping, because publishing a decision policy,
    // head, feature schema, rubric or NL template is administrative while
    // publishing a tool is not -- and that distinction is a property of the
    // KIND, which only the op can see.
    native_owner_policy(
        op.is_mutation(),
        DurabilityDomain::ControlRedb,
        op.authz_action(),
        TxnParticipation::Atomic,
    )
}

/// The semantic index takes the same runtime-conditional shape as the four agent
/// layers above, but with SIX authz actions rather than two. `is_mutation` and
/// `authz_action` both live on the op itself so this policy and
/// `server::access::requires_write` cannot drift apart about an operation --
/// and so the privilege split survives a new variant: curating what is indexed,
/// feeding rows into an approved binding, taking work off the queue and
/// declaring a stage durable are four different grants, and one action would
/// hand every holder the union.
fn semantic_index_policy(op: &eg_types::semantic_index::SemanticIndexOp) -> MethodPolicy {
    native_owner_policy(
        op.is_mutation(),
        DurabilityDomain::SemanticIndexRedb,
        op.authz_action(),
        TxnParticipation::Atomic,
    )
}

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
///   - [`SemanticIndexRedb`](DurabilityDomain::SemanticIndexRedb): the semantic
///     index's own `OwnerLayout::SemanticIndex` owner file -- bindings, the S1-S6
///     stage queue and its leases, stage artifacts and the durable ANN code tier
///     (`eg-core`'s `compute::semantic_ann_codes` / `compute::semantic_index_service`).
///     Entirely separate from graph shards; the exact sibling of `JobsRedb` and
///     `StatechartRedb`.
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
    SemanticIndexRedb,
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

/// Iterate every current method policy in deterministic domain declaration order.
///
/// This is the only public policy inventory. It is derived directly from the eleven
/// domain-owned `ROWS` declarations and has no separately ordered projection.
pub fn method_policy_entries() -> impl Iterator<Item = (&'static str, MethodPolicy, &'static str)> {
    domains::rows().map(|(name, spec, note)| (*name, spec.policy, *note))
}

/// Iterate the full engine contract, one [`MethodDescriptor`] per `Method` variant, in
/// the same deterministic domain declaration order.
///
/// This is the canonical registry RF-RULING-003 makes EG the sole owner of: every
/// generated artifact under `contract/`, `docs/capabilities.generated.md`, and the
/// generated Python client are projections of this one iterator.
pub fn method_descriptors() -> impl Iterator<Item = MethodDescriptor> {
    domains::descriptors()
}

/// Generate the protocol-method -> primitive-policy inventory from the domain registry.
pub fn protocol_policy_inventory() -> Vec<ProtocolPolicyInventoryEntry> {
    method_policy_entries()
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
                policy,
            }
        })
        .collect()
}

/// Resolve runtime-conditional policy fields before the domain registry lookup.
///
/// Domain declarations are the single source for policy values and deterministic order.
/// This function handles runtime-conditional fields that cannot be represented by one
/// static row.
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

/// Templates take the same shape as the three layers below them, with their own
/// authz action: publishing a parameterized FAMILY of agents is a distinct
/// privilege from publishing one agent. Collapsing them onto the library's
/// actions would mean anyone who can publish a single agent can also mint a
/// generator that produces an unbounded number of them -- each one an ordinary
/// library entry downstream, because that is exactly what instantiation is for.
///
/// `Instantiate` is on the READ side: it resolves a stored template and returns
/// a draft. The separate `AgentLibrary::Publish` that stores the result is
/// where the write privilege is spent, so nothing is admitted without a
/// library write.
fn agent_template_policy(op: &eg_types::agent_template::AgentTemplateOp) -> MethodPolicy {
    let mutates = op.is_mutation();
    native_owner_policy(
        mutates,
        DurabilityDomain::ControlRedb,
        if mutates {
            "agent:template-write"
        } else {
            "agent:template-read"
        },
        TxnParticipation::Atomic,
    )
}

/// The connector pack's policy. Runtime-conditional in both axes: `status` is
/// a read, and the authz action separates reading a connector's surface from
/// publishing to it from administering what it may publish.
fn connector_pack_policy(op: &eg_types::connector_pack::ConnectorPackOp) -> MethodPolicy {
    native_owner_policy(
        op.is_mutation(),
        DurabilityDomain::ControlRedb,
        op.authz_action(),
        TxnParticipation::Atomic,
    )
}

fn write_back_policy(op: &eg_types::write_back::WriteBackOp) -> MethodPolicy {
    native_owner_policy(
        op.is_mutation(),
        DurabilityDomain::ControlRedb,
        op.authz_action(),
        TxnParticipation::Atomic,
    )
}

/// One decision job's policy: `submit` writes a `jobs.redb` row, `status`
/// reads one. Both jobs share this shape and differ only in their action, so
/// the caller passes the action rather than the shape being written twice.
fn decision_job_policy(mutates: bool, authz_action: &'static str) -> MethodPolicy {
    native_owner_policy(
        mutates,
        DurabilityDomain::JobsRedb,
        authz_action,
        TxnParticipation::Atomic,
    )
}

/// The mutation outbox's policy. `rewind` resets a durable cursor through
/// bounded transactions, so it is a saga rather than one atomic write.
fn mutation_outbox_policy(op: &eg_types::mutation_outbox::MutationOutboxOp) -> MethodPolicy {
    native_owner_policy(
        op.is_mutation(),
        DurabilityDomain::ControlRedb,
        op.authz_action(),
        // A rewind resets a durable cursor through bounded transactions, so it
        // is a saga rather than one atomic write.
        TxnParticipation::Saga,
    )
}

/// The agent-hierarchy family: every surface whose read/write split and authz
/// action live on its own op.
fn agent_family_policy(method: &Method) -> Option<MethodPolicy> {
    match method {
        Method::AgentLibrary { op } => Some(agent_library_policy(op)),
        Method::AgentGraph { op } => Some(agent_graph_policy(op)),
        Method::AgentComponent { op } => Some(agent_component_policy(op)),
        Method::AgentTemplate { op } => Some(agent_template_policy(op)),
        Method::SemanticIndex { op } => Some(semantic_index_policy(op)),
        _ => None,
    }
}

/// Connector administration lives beside the agent hierarchy but has its own
/// policy vocabulary. Keep it out of the hierarchy dispatcher so adding a new
/// governed connector operation cannot make agent selection more complex.
fn connector_family_policy(method: &Method) -> Option<MethodPolicy> {
    if let Method::ConnectorPack { op } = method {
        return Some(connector_pack_policy(op));
    }
    if let Method::WriteBack { op } = method {
        return Some(write_back_policy(op));
    }
    None
}

/// The fleet catalog's policy. Its writes ARE registry writes: they
/// self-translate into `CreateNodeIfAbsent`/`CompareAndSetNodeFields` against
/// `__commons__`, so they are graph-durable, audited and CDC-emitted exactly as
/// `RegisterServer` is. Its reads are snapshot reads with no state transition.
fn fleet_catalog_policy(op: &eg_types::fleet_catalog::FleetCatalogOp) -> MethodPolicy {
    let mutates = op.is_mutation();
    MethodPolicy {
        mutates,
        durability_domain: if mutates {
            DurabilityDomain::GraphRedb
        } else {
            DurabilityDomain::None
        },
        authz_action: op.authz_action(),
        idempotent: true,
        audited: mutates,
        emits_cdc: mutates,
        txn_participation: if mutates {
            TxnParticipation::Atomic
        } else {
            TxnParticipation::Snapshot
        },
    }
}

/// The control family: the remaining runtime-conditional surfaces.
fn control_family_policy(method: &Method) -> Option<MethodPolicy> {
    match method {
        Method::CypherQuery { mode, .. } => Some(cypher_policy(mode)),
        #[cfg(feature = "modality-serving")]
        Method::ServedModality { op } => Some(served_modality_policy(op)),
        Method::DecisionFit { op } => {
            Some(decision_job_policy(op.is_mutation(), "admin:decision-fit"))
        }
        Method::DecisionEval { op } => {
            Some(decision_job_policy(op.is_mutation(), "admin:decision-eval"))
        }
        Method::MutationOutbox { op } => Some(mutation_outbox_policy(op)),
        _ => None,
    }
}

/// The fleet registry family: every registry surface whose read/write split
/// and authz action live on its own op. Its own link, so the fleet catalog adds
/// no branch to any other family's dispatcher.
fn registry_family_policy(method: &Method) -> Option<MethodPolicy> {
    if let Method::FleetCatalog { op } = method {
        return Some(fleet_catalog_policy(op));
    }
    None
}

fn policy_for_method(method: &Method) -> MethodPolicy {
    if let Some(policy) = agent_family_policy(method)
        .or_else(|| connector_family_policy(method))
        .or_else(|| registry_family_policy(method))
    {
        return policy;
    }
    if let Some(policy) = control_family_policy(method) {
        return policy;
    }
    let method_name: &'static str = method.into();
    method_policy_entries()
        .find(|(name, _, _)| *name == method_name)
        .map(|(_, policy, _)| policy)
        .unwrap_or_else(|| panic!("missing {method_name} in method-policy registry"))
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

/// Render the full capability ledger as a Markdown table, one row per `Method` variant.
pub fn gen_ledger() -> String {
    let mut out = String::new();
    out.push_str("# Epistemic Graph -- Generated Capability Ledger\n\n");
    out.push_str(
        "> **This file is GENERATED and is the AUTHORITATIVE machine-checked capability \n\
         > truth (CONCEPT:EG-P0-1)** -- regenerate with `cargo run -p eg-capabilities \n\
         > --features contract --bin gen_contract`. It is derived directly from the eleven current domain `ROWS` \n\
         > declarations under `crates/eg-capabilities/src/domains/`. Inventory gates keep \n\
         > them exact for every `Method` variant. `docs/capabilities.md` describes surface-level feature \n\
         > parity; this generated table is authoritative for per-method policy.\n>\n\
         > `mutates` marked `~true` means the value is a conservative UPPER BOUND: the real \n\
         > runtime answer is conditional (an operation, a `writeback` flag, or a parsed \n\
         > query) -- see the `note` column. `VolatileControl` is explicit non-durable \n\
         > process/session state; `None` is reserved for methods with no state transition.\n\n",
    );
    out.push_str(
        "| Method | Mutates | Durability | Authz action | Idempotent | Audited | Emits CDC | Txn participation | Note |\n",
    );
    out.push_str("|---|---|---|---|---|---|---|---|---|\n");
    for (name, p, note) in method_policy_entries() {
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
