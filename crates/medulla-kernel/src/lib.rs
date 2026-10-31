//! medulla-kernel: the engine's small, server-independent building blocks
//! (metrics, tracing export, encryption at rest, the audit chain, the write
//! coalescer, sealed-row rules, row deltas, lock recovery, bounded joins and
//! host autosizing). Split out of the `medulla-engine` facade so edits here do
//! not recompile the server, and so the server compiles against a cached crate.
//!
//! Every module keeps the feature gate it had in the facade; the facade forwards
//! its features of the same name to this crate.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

// The leaf-crate modules these files name with their historical `crate::` paths.
pub(crate) use medulla_compute::algorithms;
pub(crate) use medulla_core::{compute, graph, index};
pub(crate) use medulla_types::{acl, mutation_batch, protocol};

/// Prometheus metrics for the engine.
pub mod metrics;

// CONCEPT:MEDULLA-OS.observability.slow-query-descriptor — OTLP span export + tracing init (server-only: the subscriber
// init needs tracing-subscriber, which is server-gated).
#[cfg(feature = "server")]
/// OTLP span export and tracing initialisation.
pub mod otel;

#[cfg(feature = "server")]
pub mod persist_lock;

#[cfg(feature = "server")]
/// The slow-query log.
pub mod slow_query;

// Pure redb durable-row machinery (CONCEPT:MEDULLA-KG.backend.engine-modes) — server-INDEPENDENT, gated on
// `redb` ALONE so the embedded API (and the server's `redb_backend`) share ONE
// durable format with no Tokio. Built whenever the `redb` crate is linked.
pub mod graph_delta;

// Bounded production thread joins; every caller is a redb writer shutdown path.
#[cfg(feature = "redb")]
pub mod bounded_join;

/// Taking a lock whose previous holder panicked, without turning one bug into a
/// permanent outage. A bare `.unwrap()` on a poisoned lock is the absence of a
/// decision; these make the decision explicit and visible.
pub mod lock_recovery;

// Pure mutation outbox encoding shared by redb-only retry and server-only
// compilation; neither feature may import the other's gated module.
pub mod projection_wakeup;

/// Bounded rendezvous and joins for concurrency tests. A test that hangs takes
/// the whole binary with it; a test that fails names itself.
#[cfg(any(test, feature = "test-support"))]
pub mod test_rendezvous;

// Engine-level security (CONCEPT:MEDULLA-KG.sharding.row-level-security, Lane O): encryption-at-rest for the redb
// durable value blobs (`crypto`) + the hash-chained tamper-evident audit log over the
// ledger (`audit`). PURE-RUST (RustCrypto chacha20poly1305 + sha2). Gated on
// `security` (→ `redb`).
#[cfg(feature = "security")]
pub mod audit;

#[cfg(any(feature = "security", feature = "raft"))]
pub mod crypto;

// EH-558: sealed record rows are create-only for every generic writer, in every mode.
#[cfg(any(feature = "server", feature = "redb"))]
pub mod sealed_guard;

// Per-graph write coalescer (CONCEPT:MEDULLA-KG.sharding.per-graph-write-coalescer): batches concurrent single-op
// writes to one graph into a single topology-lock acquisition. Tokio-based, so it
// lives in the server-gated top-level crate.
#[cfg(feature = "server")]
pub mod write_coalescer;

// Hardware capacity auto-detection (CONCEPT:AU-KG.backend.b-auto-size). Derives the concurrency / buffer /
// per-graph node-cap DEFAULTS from the effective host/cgroup CPU+RAM capacity so
// the SAME binary is lean + OOM-safe in a constrained pod and exploits a large
// host. The dependency-free implementation is shared with lower provider crates
// through `medulla-resource`; this facade module preserves the existing API path.
pub mod autosize;
