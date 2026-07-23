//! Warm-on-demand for the semantic ANN index (W0.4, CONCEPT:EG-KG.storage.semantic-index-directory).
//!
//! `main.rs`'s boot-time task snapshots the registry ONCE at startup and warms
//! every graph already at/above `ANN_BUILD_THRESHOLD`. A graph created — or one
//! whose embedding count crosses the threshold — AFTER that snapshot never gets
//! a trigger from it, so it serves brute-force kNN forever (still correct, just
//! never ANN-accelerated). This module supplies the two triggers the boot task
//! cannot:
//!
//! 1. [`maybe_warm_after_write`] — called from the dispatch write-path tail
//!    (`dispatch_graph_op_inner`) after any graph mutation. `AddEmbedding` and
//!    every writeback path that adds embeddings (mining, graph-learning, …) all
//!    funnel through that same tail, so one hook there covers them all.
//! 2. [`sweep_resident_graphs`] — a periodic backstop mirroring the engine's
//!    existing interval-task cadence (cold-offload, budget enforcer, decay
//!    sweep): catches a graph the post-write trigger misses — a
//!    Raft-replicated follower apply or a redb-recovery replay never runs
//!    through the live dispatch tail.
//!
//! Both share [`spawn_warm`], the exact reopen-or-build-then-persist body the
//! boot task already uses, so all three trigger points stay in sync rather than
//! drifting. `SemanticStore`'s `STATE_WARMING` claim (`is_ready`/`is_warming`) is
//! the single source of truth for "already in flight": a redundant trigger here
//! costs an instant no-op task, never a second multi-minute build.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::compute::semantic_ann::ANN_BUILD_THRESHOLD;
use crate::graph::GraphCore;
use crate::server::state::ServerState;

/// Spawn an off-request-path warm for one graph. Never blocks the caller: the
/// qualification check is O(1) and the actual (possibly expensive) build runs on
/// a fresh `spawn_blocking` task, mirroring the boot-time warm task's own body.
fn spawn_warm(name: String, core: Arc<GraphCore>, persist_dir: Option<String>) {
    tokio::spawn(async move {
        let _ = tokio::task::spawn_blocking(move || {
            let store = core.semantic_store.read();
            if store.len() < ANN_BUILD_THRESHOLD || store.is_ready() || store.is_warming() {
                return;
            }
            let idx_dir = persist_dir
                .as_ref()
                .map(|dir| crate::persist::annidx_dir(dir, &name));
            // 1. Try the no-rebuild reopen of a persisted index first.
            if let Some(dir) = &idx_dir {
                if dir.exists() && store.load_index(dir).is_ok() && store.index_matches_len() {
                    tracing::info!(
                        "semantic ANN index reopened (no rebuild) for graph '{}' \
                         ({} vectors, warm-on-demand)",
                        name,
                        store.len()
                    );
                    return;
                }
            }
            // 2. Build off the query path. The expensive path — de-duplicated
            // against a racing trigger by `SemanticStore::ensure_index`'s
            // `STATE_WARMING` claim, so at most one of any concurrent callers
            // for this graph actually pays for it.
            let started = std::time::Instant::now();
            store.warm(&name);
            if store.is_ready() {
                tracing::info!(
                    "semantic ANN index warmed for graph '{}' ({} vectors) in {:.1}s \
                     (warm-on-demand)",
                    name,
                    store.len(),
                    started.elapsed().as_secs_f64()
                );
                // 3. Persist so a future restart reopens it without rebuilding.
                if let Some(dir) = &idx_dir {
                    if let Err(e) = store.save_index(dir) {
                        tracing::warn!(
                            "semantic ANN index persist failed for graph '{}': {}",
                            name,
                            e
                        );
                    }
                }
            }
        })
        .await;
    });
}

/// Post-write trigger (W0.4 mechanism 1): call from the dispatch write-path tail
/// after any graph mutation. Cheap enough to call unconditionally — a
/// length/readiness/warming check under the existing `semantic_store` read lock,
/// no allocation on the common (below-threshold or already-warm) path.
pub(crate) async fn maybe_warm_after_write(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    core: &Arc<GraphCore>,
) {
    let qualifies = {
        let store = core.semantic_store.read();
        store.len() >= ANN_BUILD_THRESHOLD && !store.is_ready() && !store.is_warming()
    };
    if !qualifies {
        return;
    }
    let persist_dir = state.read().await.persist_dir.clone();
    spawn_warm(graph_name.to_string(), core.clone(), persist_dir);
}

/// Periodic backstop (W0.4 mechanism 2): scan every currently RESIDENT graph
/// (never forces a cold/catalog-only graph to hydrate) and spawn a warm for any
/// whose embedding count has reached the threshold but whose index is not ready
/// — the case the post-write trigger can miss (a Raft-replicated follower apply,
/// or a redb-recovery replay, never runs through the live dispatch tail). Returns
/// the number of graphs a warm was spawned for, for the caller's log line. `pub`
/// (not `pub(crate)`): the periodic sweep task lives in `main.rs`, a SEPARATE
/// `[[bin]]` crate from this `[lib]`, so it reaches this through the external
/// `epistemic_graph::server::ann_warm::` path like `persistence::cold_offload`
/// already does for the analogous cold-offload sweep.
pub async fn sweep_resident_graphs(state: &Arc<RwLock<ServerState>>) -> usize {
    let (candidates, persist_dir) = {
        let s = state.read().await;
        let candidates: Vec<(String, Arc<GraphCore>)> = s
            .registry
            .all_entries()
            .into_iter()
            .filter_map(|entry| {
                let store = entry.core.semantic_store.read();
                let needs_warm =
                    store.len() >= ANN_BUILD_THRESHOLD && !store.is_ready() && !store.is_warming();
                needs_warm.then(|| (entry.name.clone(), entry.core.clone()))
            })
            .collect();
        (candidates, s.persist_dir.clone())
    };
    let n = candidates.len();
    for (name, core) in candidates {
        spawn_warm(name, core, persist_dir.clone());
    }
    n
}
