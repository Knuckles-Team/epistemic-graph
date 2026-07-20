// CONCEPT:EG-KG.storage.read-through-seam-exercised — read-through-on-RAM-miss seam (eg-core side).
//
// Under redb-AUTHORITATIVE mode (CONCEPT:EG-KG.backend.authoritative-dispatch) the per-graph node cap may
// EVICT a node from RAM (drop it from `GraphCore`) once it is provably durable in
// redb, to keep a shard's memory bounded. Without a way to serve an evicted node,
// dropping it would make it unreadable — so M2 made eviction advisory. This module
// restores safe eviction by giving `GraphCore` a way to fetch a missing node's
// stored properties back from the durable tier on a RAM miss.
//
// DAG-CRITICAL: eg-core sits BELOW the facade in the crate DAG and must NOT depend
// on the facade's `PersistenceBackend` (that would form a cycle eg-core → facade).
// So the durable read is abstracted behind these two traits, DEFINED HERE in
// eg-core. The facade implements them over its redb backend and injects a factory
// into the registry at startup; eg-core only ever sees the trait objects. No
// facade type leaks downward — the DAG stays acyclic.

use std::sync::Arc;

/// A per-graph read-through into the durable tier. Implemented in the facade over
/// the redb backend, scoped to ONE graph (it already holds that graph's sanitized
/// file name), and injected into each `GraphCore`.
///
/// `read_node_blob` returns the node's stored property MessagePack blob — exactly
/// the bytes a `GetNodeProperties` read serves and the same shape the redb backend
/// persisted on `AddNode`. `None` means the node is not present durably (a genuine
/// absence, not an eviction). The call is allowed to BLOCK (it does a durable
/// point-read); it is only ever invoked on a RAM MISS — never on the resident hot
/// path — so the cost is paid only when a node was actually evicted.
pub trait ReadThrough: std::fmt::Debug + Send + Sync {
    fn read_node_blob(&self, node_id: &str) -> Option<Vec<u8>>;
}

/// Builds a per-graph [`ReadThrough`] for a logical graph name. One factory is set
/// on the registry at startup; the registry
/// asks it for a read-through whenever a `GraphCore` is created, so every graph —
/// existing and future — transparently gains read-through with a single wiring
/// point. The factory sanitizes the logical name to the durable key itself, so
/// eg-core never needs the facade's filename-sanitization rule.
pub trait ReadThroughFactory: Send + Sync {
    fn for_graph(&self, graph_name: &str) -> Arc<dyn ReadThrough>;
}
