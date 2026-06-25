//! Distributed cache-coherence over the CDC feed (CONCEPT:KG-2.233).
//!
//! The result cache (CONCEPT:KG-2.233) is version-keyed PER `GraphCore`: a LOCAL
//! write bumps that node's `version()` and retires its own cached results. But in a
//! replicated deployment a write that lands on replica A must also retire B's cached
//! results for the same graph — otherwise B would keep serving a stale answer until
//! its own version happened to move.
//!
//! The CDC feed (`src/server/cdc.rs`, CONCEPT:KG-2.229) is the cross-replica
//! invalidation signal. Every durable mutation already emits an ordered, cursor-
//! addressable [`CdcEvent`] into the per-graph feed. A replica that TAILS a peer's
//! CDC feed (or its own, when changes arrive via replication/mirroring) drives this
//! consumer: for each event it observes, it calls
//! [`GraphCore::invalidate_for_remote_change`] on the matching local graph, which
//! bumps the local version and drops the cache — so B's next identical query MISSES
//! and recomputes against B's (separately replicated) data.
//!
//! No new transport: the consumer is fed [`CdcEvent`]s a replica already reads from
//! the feed via `CdcRead`/`Watch`. The in-process 2-instance test harness wires
//! A's `CdcHub` to B's registry directly to prove the invariant.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::server::state::ServerState;
use crate::wire::CdcEvent;

/// Apply one remote [`CdcEvent`] as a cache-coherence invalidation against the local
/// registry. Looks up the event's graph; if it exists locally, retires its cached
/// query results (and bumps its version). A change for an unknown graph is a no-op
/// (nothing cached for it yet). Returns `true` when a local graph was invalidated.
///
/// Idempotent and order-insensitive at the cache layer: invalidation only ever
/// RETIRES cached reads, so replaying or reordering events is safe — the worst case
/// is an extra recompute, never a stale serve.
pub async fn apply_remote_change(state: &Arc<RwLock<ServerState>>, event: &CdcEvent) -> bool {
    let core = {
        let s = state.read().await;
        s.registry.get(&event.graph).map(|e| e.core.clone())
    };
    match core {
        Some(core) => {
            core.invalidate_for_remote_change();
            true
        }
        None => false,
    }
}

/// Drain a peer's CDC feed from `from_seq` and invalidate every changed local graph,
/// returning the next cursor to resume from. The replication loop calls this each
/// time it pulls a batch off a peer's feed (the same `CdcRead` a CDC consumer uses),
/// so a remote write propagates as a local cache invalidation. `limit == 0` ⇒ the
/// hub default.
pub async fn drain_and_invalidate(
    state: &Arc<RwLock<ServerState>>,
    peer_feed: &crate::server::cdc::CdcHub,
    graph: &str,
    from_seq: u64,
    limit: u32,
) -> Result<u64, String> {
    let events = peer_feed.read(graph, from_seq, limit)?;
    let mut next = from_seq;
    for ev in &events {
        apply_remote_change(state, ev).await;
        next = ev.seq + 1;
    }
    Ok(next)
}
