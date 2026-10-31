#![allow(non_local_definitions)]
#![allow(dead_code)]
// The all-features dispatch future needs query depth 130. Use the same
// bounded compiler budget for production and test builds.
#![recursion_limit = "256"]
// No unsafe anywhere except a few platform/C-FFI boundaries, each carrying a
// scoped #[allow(unsafe_code)] with a soundness note: the tree-sitter block
// (medulla-compute ast/parser.rs), the CycloneDDS loan (server/dds.rs, ros2-rmw only),
// and the Win32 atomic-replace (server/reasoning_projection.rs, cfg(windows)).
// Any new unsafe is a compile error until it is explicitly justified the same way.
#![deny(unsafe_code)]

// CONCEPT:MEDULLA-KG.compute.graph-compute-engine - High-Performance Graph Compute Engine
// CONCEPT:MEDULLA-ORCH.execution.compiled-orchestration-kernel - Compiled Orchestration Kernel
// CONCEPT:MEDULLA-KG.query.tokio-service-server - Tokio Service Layer
//
// Tokio service layer handling MessagePack RPC.
// All logic delegated to graph, algorithms, and reasoning modules.
// Service-layer modules (protocol, registry, isolation, channels, server)
// are used by the medulla-server binary.

// Bottom-of-DAG wire types live in `medulla-types`; the graph core (storage, registry,
// isolation, semantic search) lives in `medulla-core`. Re-export both under the
// historical `crate::` paths so every module's `crate::protocol::` /
// `crate::graph::` / `crate::registry::` reference resolves unchanged.
pub use medulla_core::{compute, decay, graph, index, isolation, registry};
// CONCEPT:MEDULLA-KG.compute.message-broker-exchanges — message-broker primitives, re-exported under `crate::broker`.
#[cfg(feature = "broker")]
pub use medulla_core::broker;
#[cfg(feature = "knowledge-batch")]
pub use medulla_types::knowledge_stream;
pub use medulla_types::{
    acl, change_envelope, epistemic_operations, epistemic_operations_ext, mutation_batch, protocol,
    types, wire,
};
// Compute domains live in `medulla-compute`; re-export under the historical `crate::`
// paths. algorithms/ast/parser are always present; the feature-gated domains are
// re-exported only when their facade feature (→ medulla-compute feature) is on.
#[cfg(feature = "datascience")]
pub use medulla_compute::datascience;
#[cfg(feature = "finance")]
pub use medulla_compute::finance;
#[cfg(feature = "reasoning")]
pub use medulla_compute::reasoning;
pub use medulla_compute::{algorithms, ast, parser, screen};

/// The native WorkItem-kernel lease and lifecycle writes (claim, renew,
/// cancel, defer, metadata CAS, the graph-os MEDULLA-2 control-lease writes and the
/// EH-558 sealed-record retirement),
/// as ONE pattern. The apply and classification sites match through it, so a
/// new family member is added once and every exhaustive `match` using it
/// still fails to compile until it is classified. The dispatch arm in
/// `server::handlers::work_item` stays enumerated: the contract-reachability
/// gate reads dispatch arms by variant name.
macro_rules! work_item_kernel_writes {
    () => {
        $crate::protocol::Method::ClaimWorkItem { .. }
            | $crate::protocol::Method::RenewWorkItemLease { .. }
            | $crate::protocol::Method::CancelWorkItem { .. }
            | $crate::protocol::Method::DeferWorkItem { .. }
            | $crate::protocol::Method::CasWorkItemMetadata { .. }
            | $crate::protocol::Method::PublishCurrentPlan { .. }
            | $crate::protocol::Method::IssueHostStartGrant { .. }
            | $crate::protocol::Method::PublishRunCheckpoint { .. }
            | $crate::protocol::Method::IssueControlLease { .. }
            | $crate::protocol::Method::TransitionControlLease { .. }
            | $crate::protocol::Method::PolicyEvolutionStore { .. }
            | $crate::protocol::Method::GapUpsert { .. }
            | $crate::protocol::Method::GapTransition { .. }
            | $crate::protocol::Method::GapSettle { .. }
            | $crate::protocol::Method::WorkOfferPut { .. }
            | $crate::protocol::Method::RetireSealedRecord { .. }
    };
}

/// The WorkItem resource-reservation writes, as one pattern (see
/// `work_item_kernel_writes`).
macro_rules! work_item_resource_writes {
    () => {
        $crate::protocol::Method::ReserveWorkItemResources { .. }
            | $crate::protocol::Method::ReleaseWorkItemResources { .. }
            | $crate::protocol::Method::ReclaimWorkItemResources { .. }
            | $crate::protocol::Method::UpdateResourceHost { .. }
    };
}

#[cfg(feature = "server")]
pub mod channels;
// CONCEPT:MEDULLA-OS.observability.slow-query-descriptor — slow-query log. Server-only (only the server dispatch/pgwire
// paths execute queries).
#[cfg(feature = "server")]
pub mod persist;
#[cfg(feature = "redb")]
pub(crate) mod redb_layout;
// The composition root's ONE scope-grant authority. `medulla-storage` opens every
// durable file; only this binary decides which principal may serve which scope
// on which of them (RF-RULING-004).
#[cfg(feature = "redb")]
pub mod store_authority;
#[cfg(feature = "redb")]
pub mod redb_store;
// The one shape every small kernel-owned sidecar file in this binary takes.
#[cfg(feature = "redb")]
pub mod sidecar_store;

// CONCEPT:INT-P2-2 -- the Loop statechart definition (W2.5 control-plane migration):
// `LoopStatus`'s 16 values as a reusable `medulla_statechart::StatechartDef`, instantiated
// via the EXISTING `Method::Statechart` surface. Pure data + tests; no wire/dispatch
// change. See `loop_statechart.rs`'s module doc for the full derivation.
#[cfg(feature = "statechart")]
pub mod loop_statechart;

// CONCEPT:INT-P2-2 / ADR-5 (W2.2) -- the WorkItem statechart definition: the durable
// unit-of-work lifecycle (`submitted → ready → leased → running → {succeeded|failed|
// cancelled|dead_letter}`) as ONE reusable `medulla_statechart::StatechartDef`, plus the
// phase-1 dual-write mirror decision + divergence alarm the native `redb_store.rs`
// WorkItem handlers drive. Pure data + tests; the CAS/fencing row stays the lease
// authority. See `work_item_statechart.rs`'s module doc for the full derivation.
#[cfg(feature = "statechart")]
pub mod work_item_statechart;

// CONCEPT:INT-P2-2 / ADR-6 (W2.3) -- the agent-instance lifecycle statechart: an agent
// instance's own `dormant ⇄ active` (+ `terminated`) lifecycle as ONE reusable
// `medulla_statechart::StatechartDef`, the canonical "statechart template" the agents-as-data
// activation layer instantiates per dormant instance. Distinct from (and composed with)
// the WorkItem chart above — this owns "is the instance dormant or active", the WorkItem
// owns "is an activation submitted/leased/running/done". Pure data + tests; instantiated
// via the EXISTING `Method::Statechart` surface, mirrored as a Python dict in
// `agent_utilities.orchestration.agent_activation`. See the module doc for the full
// derivation.
#[cfg(feature = "statechart")]
pub mod agent_lifecycle_statechart;

/// One staged time-series measurement batch for a cross-modal ACID commit
/// (CONCEPT:MEDULLA-KG.backend.cross-modal-atomic-commit): `(series_id, n_fields, bucket_ns, field_names, points)` where each
/// point is `(ts_nanos, field_values)`. Carried as PLAIN data (no `medulla-tsdb` type) so it
/// threads through the `PersistenceBackend` trait + the server-independent `redb_store`
/// without pulling the `tsdb` feature into either — the redb backend converts it to
/// `medulla_tsdb::point::Point` only where the SERIES tables are actually written. Kept as a
/// named alias so the `&[MeasurementBatch]` params don't trip the `type_complexity` lint.
pub type MeasurementBatch = (String, usize, u64, Vec<String>, Vec<(i64, Vec<f64>)>);

// In-process embedded library API (CONCEPT:MEDULLA-KG.backend.engine-modes) — SQLite/DuckDB-style. Drives
// the SAME GraphCore + redb durable rows the socket dispatch does, with NO Tokio
// server/socket/HMAC. Gated on `embedded` (→ `redb`); needs NO `server` feature.
#[cfg(feature = "embedded")]
pub mod embedded;
// Shared test-only filesystem helpers. Keeping the process/epoch-nanos path
// construction in one owner prevents unit-test modules from drifting while
// preserving each caller's existing path prefix and cleanup contract.
#[cfg(feature = "server")]
pub mod server;
#[cfg(test)]
pub(crate) mod test_support;
// Canonical durable mutation classification/application is shared by the socket,
// embedded, and Raft paths.
#[cfg(any(feature = "server", feature = "redb"))]
pub mod mutation_apply;
// Per-tenant memory budget + autoscale signals (CONCEPT:MEDULLA-KG.compute.lane-v, Lane V). The budget
// enforcer (periodic over-budget eviction/hibernation), the per-tenant resident-RAM
// tracking, the ResourceStats snapshot, and the capacity-planning cost model. PURE-RUST
// (a sweep over the registry reusing the existing evict/hibernate ops); gated on `cost`.
#[cfg(feature = "cost")]
pub mod cost;
// In-engine Raft replication (CONCEPT:AU-KG.ingest.source-sync-canonical) — cluster tier only, behind the
// `raft` feature. A default/pi/full build links no openraft. The module is
// self-`#![cfg(feature = "raft")]`-gated; this `mod` line is also gated so it does
// not even appear in a non-raft build.
#[cfg(feature = "raft")]
pub mod raft;

// Unified single binary — the opt-in in-process pyo3-engine (unified-binary program,
// workstream W-A; docs/architecture/unified-inprocess-engine.md). Re-exported ONLY
// under the opt-in `pyo3-engine` feature (never in default/full/cluster) so a
// Rust-only embedder can construct the SAME in-process engine handle the compiled
// `medulla.engine` extension wraps, with no Python involved. The
// out-of-process scale-out build links none of this crate and none of its optional
// dependencies (`scripts/check_no_pyo3.sh` scopes to exactly that build).
#[cfg(feature = "pyo3-engine")]
pub use medulla_pyengine;
