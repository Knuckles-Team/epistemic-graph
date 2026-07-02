#![allow(non_local_definitions)]
#![allow(dead_code)]
// No unsafe anywhere except the single tree-sitter FFI block (ast/parser.rs),
// which carries a scoped #[allow(unsafe_code)] with a soundness note. Any new
// unsafe is a compile error until it is explicitly justified the same way.
#![deny(unsafe_code)]

// CONCEPT:KG-2.16 - High-Performance Graph Compute Engine
// CONCEPT:ORCH-1.29 - Compiled Orchestration Kernel
// CONCEPT:KG-2.19 - Tokio Service Layer
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
// CONCEPT:EG-275 — message-broker primitives, re-exported under `crate::broker`.
#[cfg(feature = "broker")]
pub use eg_core::broker;
pub use eg_types::{acl, protocol, types, wire};
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

#[cfg(feature = "server")]
pub mod channels;
pub mod metrics;
// CONCEPT:EG-091 — OTLP span export + tracing init (server-only: the subscriber
// init needs tracing-subscriber, which is server-gated).
#[cfg(feature = "server")]
pub mod otel;
// CONCEPT:EG-091 — slow-query log. Server-only (only the server dispatch/pgwire
// paths execute queries).
#[cfg(feature = "server")]
pub mod slow_query;
#[cfg(feature = "server")]
pub mod persist;
#[cfg(feature = "server")]
pub mod persist_lock;
// Pure redb durable-row machinery (CONCEPT:KG-2.216) — server-INDEPENDENT, gated on
// `redb` ALONE so the embedded API (and the server's `redb_backend`) share ONE
// durable format with no Tokio. Built whenever the `redb` crate is linked.
#[cfg(feature = "redb")]
pub mod redb_store;
// Engine-level security (CONCEPT:KG-2.231, Lane O): encryption-at-rest for the redb
// durable value blobs (`crypto`) + the hash-chained tamper-evident audit log over the
// ledger (`audit`). PURE-RUST (RustCrypto chacha20poly1305 + sha2). Gated on
// `security` (→ `redb`).
#[cfg(feature = "security")]
pub mod audit;
#[cfg(feature = "security")]
pub mod crypto;
// In-process embedded library API (CONCEPT:KG-2.216) — SQLite/DuckDB-style. Drives
// the SAME GraphCore + redb durable rows the socket dispatch does, with NO Tokio
// server/socket/HMAC. Gated on `embedded` (→ `redb`); needs NO `server` feature.
#[cfg(feature = "embedded")]
pub mod embedded;
#[cfg(feature = "server")]
pub mod server;
// The WAL module: `WalWriter`/`replay` (snapshot+WAL tier) need `server`, but its
// canonical "durable Method → GraphCore" applier (`wal::apply`) + the
// `is_durable_mutation` predicate are PURE (no Tokio) and are the SAME core-apply
// the socket dispatch, WAL replay, and the Raft state machine use. The embedded API
// drives them too, so the module is available without `server` whenever `redb` is on.
#[cfg(any(feature = "server", feature = "redb"))]
pub mod wal;
#[cfg(feature = "server")]
pub mod wal_service;
// Per-graph write coalescer (CONCEPT:KG-2.182): batches concurrent single-op
// writes to one graph into a single topology-lock acquisition. Tokio-based, so it
// lives in the server-gated top-level crate alongside wal_service.
#[cfg(feature = "server")]
pub mod write_coalescer;
// Hardware capacity auto-detection (CONCEPT:EG-028). Derives the concurrency / buffer /
// per-graph node-cap DEFAULTS from (cpu_count, total_RAM) so the SAME binary is lean +
// OOM-safe on a Raspberry Pi 3 (4 cores / 1 GiB) and exploits a 64-core / 247 GiB box.
// PURE-RUST (available_parallelism + /proc/meminfo), no deps; always compiled.
pub mod autosize;
// Per-tenant memory budget + autoscale signals (CONCEPT:KG-2.234, Lane V). The budget
// enforcer (periodic over-budget eviction/hibernation), the per-tenant resident-RAM
// tracking, the ResourceStats snapshot, and the capacity-planning cost model. PURE-RUST
// (a sweep over the registry reusing the existing evict/hibernate ops); gated on `cost`.
#[cfg(feature = "cost")]
pub mod cost;
// In-engine Raft replication (CONCEPT:KG-2.188) — cluster tier only, behind the
// `raft` feature. A default/pi/full build links no openraft. The module is
// self-`#![cfg(feature = "raft")]`-gated; this `mod` line is also gated so it does
// not even appear in a non-raft build.
#[cfg(feature = "raft")]
pub mod raft;
