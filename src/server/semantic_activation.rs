//! Semantic ANN index activation (W0.4, CONCEPT:EG-KG.storage.semantic-index-directory).
//!
//! THREE triggers can put a graph's ANN index into service, and all three run
//! [`activate_one`] — the one reopen-else-build-else-activate body — so they
//! cannot drift apart:
//!
//! 1. `main.rs`'s boot task, over the registry snapshot taken at startup. It
//!    covers only graphs resident at that instant: a graph created — or one
//!    whose embedding count crosses `ANN_BUILD_THRESHOLD` — afterwards never
//!    gets a trigger from it, so it would serve brute-force kNN forever (still
//!    correct, just never ANN-accelerated).
//! 2. [`maybe_activate_after_write`], called from the dispatch write-path tail
//!    (`finalize_graph_op_response`) after any graph mutation. `AddEmbedding`
//!    and every writeback path that adds embeddings (mining, graph-learning, …)
//!    funnel through that same tail, so one hook there covers them all.
//! 3. [`sweep_resident_graphs`], a periodic backstop mirroring the engine's
//!    existing interval-task cadence: it catches what the post-write trigger
//!    misses, because a Raft-replicated follower apply or a redb-recovery
//!    replay never runs through the live dispatch tail.
//!
//! `SemanticStore`'s `STATE_WARMING` claim (`is_ready`/`is_warming`) is the
//! single source of truth for "already in flight": a redundant trigger costs an
//! instant no-op, never a second multi-minute build.
//!
//! The durable tier is [`crate::compute::semantic_ann_codes::SemanticCodeStore`]
//! (feature `ann-redb`): one index generation becomes durable as ONE admitted
//! `Native(SemanticIndex)` maintenance mutation, and the binding's live pointer
//! flips inside that same transaction, so a generation never becomes live
//! without its codes. The retired `SemanticStore::{save_index, load_index}` pair
//! wrote and read three files plus a manifest beside the process under no
//! admission at all, and its fixed keys meant one binding could hold exactly one
//! generation — building `N+1` overwrote the `N` that was still serving. Under
//! `ann` without `ann-redb` there is no durable tier at all and every activation
//! is a build.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::compute::semantic_ann::ANN_BUILD_THRESHOLD;
use crate::graph::GraphCore;
use crate::server::state::ServerState;

#[cfg(feature = "ann-redb")]
use crate::compute::semantic::SemanticStore;
#[cfg(feature = "ann-redb")]
use crate::compute::semantic_ann_codes::SemanticCodeStore;

/// The one directory under the persist dir that holds every binding's
/// semantic-index owner file. `SemanticCodeStore::open` derives the file name
/// from `(tenant, binding)` itself and creates the directory, so a graph name is
/// never a path component — which is what the retired `persist::annidx_dir`
/// needed `persist::sanitize` for.
#[cfg(feature = "ann-redb")]
const SEMANTIC_INDEX_DIR: &str = "semantic-index";

/// The tenant every engine-owned index generation is activated under. Activation
/// is maintenance and carries no caller identity (RF-RULING-005), so the binding
/// — the graph name — is what distinguishes one index from another.
#[cfg(feature = "ann-redb")]
const SEMANTIC_INDEX_TENANT: &str = "semantic-index";

/// Put one graph's ANN index into service: reopen the live durable generation
/// if there is one, else build and activate a new generation. Returns whether
/// the graph ended up serving an index.
///
/// Blocking and possibly multi-minute — every caller runs it on a blocking
/// thread, never on the request path or the async runtime.
pub fn activate_one(name: &str, core: &GraphCore, persist_dir: Option<&str>) -> bool {
    let store = core.semantic_store.read();
    if store.len() < ANN_BUILD_THRESHOLD || store.is_ready() || store.is_warming() {
        // Brute force is exact and fast below the threshold, and an index that
        // is ready or already being built needs no second trigger.
        return false;
    }
    #[cfg(not(feature = "ann-redb"))]
    let _ = persist_dir;
    // 1. Reopen the live durable generation first — no rebuild.
    #[cfg(feature = "ann-redb")]
    if adopt_live_generation(name, &store, persist_dir) {
        return true;
    }
    // 2. Build off the query path. The expensive path, de-duplicated against a
    // racing trigger by `SemanticStore::ensure_index`'s `STATE_WARMING` claim,
    // so at most one of any concurrent callers for this graph pays for it.
    let started = std::time::Instant::now();
    store.warm(name);
    if !store.is_ready() {
        return false;
    }
    tracing::info!(
        "semantic ANN index warmed for graph '{}' ({} vectors) in {:.1}s",
        name,
        store.len(),
        started.elapsed().as_secs_f64()
    );
    // 3. Activate it durably so a future restart reopens it without rebuilding.
    #[cfg(feature = "ann-redb")]
    activate_generation(name, &store, persist_dir);
    true
}

/// Reopen the binding's live generation and make it serving. `false` when there
/// is no persist dir, no live generation, or the stored generation does not
/// describe the resident arena — each of which falls through to a build rather
/// than failing the caller.
#[cfg(feature = "ann-redb")]
fn adopt_live_generation(name: &str, store: &SemanticStore, persist_dir: Option<&str>) -> bool {
    let Some(dir) = persist_dir else {
        return false;
    };
    match try_adopt_live_generation(name, store, dir) {
        Ok(Some(generation)) => {
            tracing::info!(
                "semantic ANN index reopened (no rebuild) for graph '{}' \
                 (generation {}, {} vectors)",
                name,
                generation,
                store.len()
            );
            true
        }
        Ok(None) => false,
        Err(error) => {
            tracing::warn!("semantic ANN index reopen failed for graph '{name}': {error}");
            false
        }
    }
}

/// One `read_live` — the serving read, which serves only the live generation —
/// adopted into the resident store. `adopt_generation` re-checks the image's
/// width, member set and embedding space against the resident arena, exactly as
/// the retired `load_index` did.
#[cfg(feature = "ann-redb")]
fn try_adopt_live_generation(
    name: &str,
    store: &SemanticStore,
    dir: &str,
) -> Result<Option<u64>, String> {
    let codes = open_code_store(dir, name)?;
    let Some((generation, image)) = codes.read_live().map_err(|error| error.to_string())? else {
        return Ok(None);
    };
    store
        .adopt_generation(&image)
        .map_err(|error| error.to_string())?;
    Ok(store.index_matches_len().then_some(generation))
}

/// Make the resident index the binding's new live generation.
#[cfg(feature = "ann-redb")]
fn activate_generation(name: &str, store: &SemanticStore, persist_dir: Option<&str>) {
    let Some(dir) = persist_dir else {
        return;
    };
    if let Err(error) = try_activate_generation(name, store, dir) {
        tracing::warn!("semantic ANN index persist failed for graph '{name}': {error}");
    }
}

/// Export the resident generation, activate it as `live + 1`, then retire the
/// generation it superseded.
///
/// The retirement is not housekeeping bolted on: `activate` flips the live
/// pointer inside its own transaction, so the superseded generation is already
/// unreachable through `read_live`, and `retire` takes its ledger authority and
/// its owner rows together or not at all. It keeps one binding's durable
/// footprint at one generation, which is the footprint the retired file format
/// had — the difference is that the new generation is written and made live
/// before the old one is dropped, instead of the old one being overwritten in
/// place while it was still serving.
///
/// A failed retirement is therefore warned, not returned: the activation it
/// follows has already committed, and reporting "persist failed" for a
/// generation that IS live and serving would be the wrong answer. What it leaves
/// behind is one unreachable generation's rows, not a lost index.
#[cfg(feature = "ann-redb")]
fn try_activate_generation(name: &str, store: &SemanticStore, dir: &str) -> Result<(), String> {
    let image = store
        .export_generation()
        .map_err(|error| error.to_string())?;
    let codes = open_code_store(dir, name)?;
    let superseded = codes.live_generation().map_err(|error| error.to_string())?;
    let generation = superseded
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| "semantic index generation counter overflowed".to_string())?;
    codes
        .activate(generation, &image)
        .map_err(|error| error.to_string())?;
    if let Some(superseded) = superseded {
        if let Err(error) = codes.retire(superseded) {
            tracing::warn!(
                "semantic ANN index generation {} for graph '{}' was superseded by {} but \
                 could not be retired: {}",
                superseded,
                name,
                generation,
                error
            );
        }
    }
    Ok(())
}

/// Open one binding's kernel-owned semantic-index file under the composition
/// root's ONE scope-grant authority (RF-RULING-004).
#[cfg(feature = "ann-redb")]
fn open_code_store(persist_dir: &str, binding: &str) -> Result<SemanticCodeStore, String> {
    let authority = crate::store_authority::process_authority();
    SemanticCodeStore::open(
        &std::path::Path::new(persist_dir).join(SEMANTIC_INDEX_DIR),
        crate::store_authority::process_verifier(),
        authority.principal(),
        &authority.proof(),
        SEMANTIC_INDEX_TENANT,
        binding,
    )
    .map_err(|error| error.to_string())
}

/// Spawn an off-request-path activation for one graph. Never blocks the caller:
/// the qualification check inside [`activate_one`] is O(1) and the build runs on
/// a fresh `spawn_blocking` task.
fn spawn_activation(name: String, core: Arc<GraphCore>, persist_dir: Option<String>) {
    tokio::spawn(async move {
        let _ = tokio::task::spawn_blocking(move || {
            activate_one(&name, &core, persist_dir.as_deref());
        })
        .await;
    });
}

/// Post-write trigger (W0.4 mechanism 1): call from the dispatch write-path tail
/// after any graph mutation. Cheap enough to call unconditionally — a
/// length/readiness/warming check under the existing `semantic_store` read lock,
/// no allocation on the common (below-threshold or already-warm) path.
pub(crate) async fn maybe_activate_after_write(
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
    spawn_activation(graph_name.to_string(), core.clone(), persist_dir);
}

/// Periodic backstop (W0.4 mechanism 2): scan every currently RESIDENT graph
/// (never forces a cold/catalog-only graph to hydrate) and spawn an activation
/// for any whose embedding count has reached the threshold but whose index is
/// not ready — the case the post-write trigger can miss (a Raft-replicated
/// follower apply, or a redb-recovery replay, never runs through the live
/// dispatch tail). Returns the number of graphs an activation was spawned for,
/// for the caller's log line. `pub` (not `pub(crate)`): the periodic sweep task
/// lives in `main.rs`, a SEPARATE `[[bin]]` crate from this `[lib]`, so it
/// reaches this through the external `epistemic_graph::server::` path like
/// `persistence::cold_offload` already does for the analogous cold-offload sweep.
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
        spawn_activation(name, core, persist_dir.clone());
    }
    n
}
