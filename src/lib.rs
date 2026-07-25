#![allow(non_local_definitions)]
#![allow(dead_code)]
// No unsafe anywhere except the single tree-sitter FFI block (ast/parser.rs),
// which carries a scoped #[allow(unsafe_code)] with a soundness note. Any new
// unsafe is a compile error until it is explicitly justified the same way.
#![deny(unsafe_code)]

// CONCEPT:EG-KG.compute.graph-compute-engine - High-Performance Graph Compute Engine
// CONCEPT:EG-ORCH.execution.compiled-orchestration-kernel - Compiled Orchestration Kernel
// CONCEPT:EG-KG.query.tokio-service-server - Tokio Service Layer
//
// Tokio service layer handling MessagePack RPC.
// All logic delegated to graph, algorithms, and reasoning modules.
// Service-layer modules (protocol, registry, isolation, channels, server)
// are used by the epistemic-graph-server binary.

// Bottom-of-DAG wire types live in `eg-types`; the graph core (storage, registry,
// isolation, semantic search) lives in `eg-core`. Re-export both under the
// historical `crate::` paths so every module's `crate::protocol::` /
// `crate::graph::` / `crate::registry::` reference resolves unchanged.
pub use eg_core::{compute, decay, graph, index, isolation, registry};
// CONCEPT:EG-KG.compute.message-broker-exchanges — message-broker primitives, re-exported under `crate::broker`.
#[cfg(feature = "broker")]
pub use eg_core::broker;
#[cfg(feature = "knowledge-batch")]
pub use eg_types::knowledge_stream;
pub use eg_types::{
    acl, change_envelope, epistemic_operations, mutation_batch, protocol, types, wire,
};
// Compute domains live in `eg-compute`; re-export under the historical `crate::`
// paths. algorithms/ast/parser are always present; the feature-gated domains are
// re-exported only when their facade feature (→ eg-compute feature) is on.
#[cfg(feature = "datascience")]
pub use eg_compute::datascience;
#[cfg(feature = "finance")]
pub use eg_compute::finance;
#[cfg(feature = "reasoning")]
pub use eg_compute::reasoning;
pub use eg_compute::{algorithms, ast, parser, screen};

#[cfg(any(feature = "server", feature = "redb"))]
pub mod durability;

#[cfg(feature = "server")]
pub mod channels;
pub mod metrics;
// CONCEPT:EG-OS.observability.slow-query-descriptor — OTLP span export + tracing init (server-only: the subscriber
// init needs tracing-subscriber, which is server-gated).
#[cfg(feature = "server")]
pub mod otel;
// CONCEPT:EG-OS.observability.slow-query-descriptor — slow-query log. Server-only (only the server dispatch/pgwire
// paths execute queries).
#[cfg(feature = "server")]
pub mod persist;
#[cfg(feature = "server")]
pub mod persist_lock;
#[cfg(feature = "server")]
pub mod slow_query;
// Pure redb durable-row machinery (CONCEPT:EG-KG.backend.engine-modes) — server-INDEPENDENT, gated on
// `redb` ALONE so the embedded API (and the server's `redb_backend`) share ONE
// durable format with no Tokio. Built whenever the `redb` crate is linked.
pub(crate) mod graph_delta;
#[cfg(feature = "redb")]
pub(crate) mod redb_layout;
#[cfg(feature = "redb")]
pub mod redb_store;

// CONCEPT:INT-P2-2 -- the Loop statechart definition (W2.5 control-plane migration):
// `LoopStatus`'s 16 values as a reusable `eg_statechart::StatechartDef`, instantiated
// via the EXISTING `Method::Statechart` surface. Pure data + tests; no wire/dispatch
// change. See `loop_statechart.rs`'s module doc for the full derivation.
#[cfg(feature = "statechart")]
pub mod loop_statechart;

// CONCEPT:INT-P2-2 / ADR-5 (W2.2) -- the WorkItem statechart definition: the durable
// unit-of-work lifecycle (`submitted → ready → leased → running → {succeeded|failed|
// cancelled|dead_letter}`) as ONE reusable `eg_statechart::StatechartDef`, plus the
// phase-1 dual-write mirror decision + divergence alarm the native `redb_store.rs`
// WorkItem handlers drive. Pure data + tests; the CAS/fencing row stays the lease
// authority. See `work_item_statechart.rs`'s module doc for the full derivation.
#[cfg(feature = "statechart")]
pub mod work_item_statechart;

/// One staged time-series measurement batch for a cross-modal ACID commit
/// (CONCEPT:EG-KG.backend.cross-modal-atomic-commit): `(series_id, n_fields, bucket_ns, field_names, points)` where each
/// point is `(ts_nanos, field_values)`. Carried as PLAIN data (no `eg-tsdb` type) so it
/// threads through the `PersistenceBackend` trait + the server-independent `redb_store`
/// without pulling the `tsdb` feature into either — the redb backend converts it to
/// `eg_tsdb::point::Point` only where the SERIES tables are actually written. Kept as a
/// named alias so the `&[MeasurementBatch]` params don't trip the `type_complexity` lint.
pub type MeasurementBatch = (String, usize, u64, Vec<String>, Vec<(i64, Vec<f64>)>);

// Engine-level security (CONCEPT:EG-KG.sharding.row-level-security, Lane O): encryption-at-rest for the redb
// durable value blobs (`crypto`) + the hash-chained tamper-evident audit log over the
// ledger (`audit`). PURE-RUST (RustCrypto chacha20poly1305 + sha2). Gated on
// `security` (→ `redb`).
#[cfg(feature = "security")]
pub mod audit;
#[cfg(any(feature = "security", feature = "raft"))]
pub mod crypto;
// In-process embedded library API (CONCEPT:EG-KG.backend.engine-modes) — SQLite/DuckDB-style. Drives
// the SAME GraphCore + redb durable rows the socket dispatch does, with NO Tokio
// server/socket/HMAC. Gated on `embedded` (→ `redb`); needs NO `server` feature.
#[cfg(feature = "embedded")]
pub mod embedded;
#[cfg(feature = "server")]
pub mod server;
// Canonical durable mutation classification/application is shared by the socket,
// embedded, and Raft paths.
#[cfg(any(feature = "server", feature = "redb"))]
pub mod mutation_apply;
// Per-graph write coalescer (CONCEPT:EG-KG.sharding.per-graph-write-coalescer): batches concurrent single-op
// writes to one graph into a single topology-lock acquisition. Tokio-based, so it
// lives in the server-gated top-level crate.
#[cfg(feature = "server")]
pub mod write_coalescer;
// Hardware capacity auto-detection (CONCEPT:AU-KG.backend.b-auto-size). Derives the concurrency / buffer /
// per-graph node-cap DEFAULTS from (cpu_count, total_RAM) so the SAME binary is lean +
// OOM-safe on a Raspberry Pi 3 (4 cores / 1 GiB) and exploits a 64-core / 247 GiB box.
// PURE-RUST (available_parallelism + /proc/meminfo), no deps; always compiled.
pub mod autosize;
// Per-tenant memory budget + autoscale signals (CONCEPT:EG-KG.compute.lane-v, Lane V). The budget
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
