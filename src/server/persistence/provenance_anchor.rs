//! Provenance anchoring (CONCEPT:EG-KG.sharding.row-level-security): periodic Merkle-root anchor over each
//! resident graph's `:ToolCall`/`:RunTrace` provenance-node window into the SAME
//! tamper-evident audit chain the `security` feature already maintains
//! (`crate::audit`, `crate::redb_store::{provenance_anchor_commit, prove_inclusion}`).
//!
//! ## Why "resident graphs" + the in-RAM label index
//!
//! `GraphCore::get_nodes_by_label` already maintains a lazily-built,
//! incrementally-maintained `label -> node ids` index for every resident graph —
//! an O(1) lookup, no scan. This sweep reuses it purely to pick CANDIDATE ids;
//! every candidate's actual leaf hash is re-derived from its CURRENT DURABLE
//! content (`RedbBackend::provenance_leaf_hashes_blocking`, off the writer
//! thread), never from the in-RAM property blob, so what gets anchored is always
//! what is actually durable. A graph that is hibernated/offloaded simply has
//! nothing new to consider until it becomes resident again — the same
//! "resident work only" scope the cold-offload and budget sweeps already use.
//!
//! ## Overhead budget
//!
//! The window-size-dependent work (reading + hashing up to
//! [`max_window_nodes`] candidates) happens entirely OFF the writer thread. Only
//! the FINAL commit — and only when the computed root actually differs from the
//! graph's last anchored root — ever touches the writer thread, and that
//! commit's own cost is O(1) in window size
//! (`RedbBackend::provenance_anchor_commit_blocking`). An idle graph (unchanged
//! window) costs the writer thread nothing at all: the fast path never opens a
//! transaction. See `benches/provenance_anchor_bench.rs` for the measured delta.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::server::ServerState;

/// Default cap on how many `:ToolCall`/`:RunTrace` node ids a single anchor tick
/// considers PER LABEL for one graph — bounds both the off-writer-thread hashing
/// pass and the eventual `PROVENANCE_ANCHOR_MEMBERS` row. Mirrors the scale of
/// `server::state::DEFAULT_MAX_RESPONSE_NODES`.
pub const DEFAULT_MAX_WINDOW_NODES: usize = 50_000;

/// The configured positive per-tick window cap, read from
/// `EPISTEMIC_GRAPH_PROVENANCE_ANCHOR_MAX_NODES`. Zero, absent, and
/// non-parsable values resolve to [`DEFAULT_MAX_WINDOW_NODES`].
pub fn max_window_nodes() -> usize {
    std::env::var("EPISTEMIC_GRAPH_PROVENANCE_ANCHOR_MAX_NODES")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_WINDOW_NODES)
}

/// Provenance-node label vocabulary this sweep anchors. Matches `GraphCore`'s own
/// `type`/`node_type`/`label`/`labels` label semantics (`labels_of` in
/// `eg-core::graph`) — a node carrying either value as a label is in scope.
const PROVENANCE_LABELS: [&str; 2] = ["ToolCall", "RunTrace"];

/// Candidate node ids for one graph's provenance window: the union of its
/// `:ToolCall` and `:RunTrace` label-index postings, deterministically ordered
/// (sorted, deduplicated) and bounded to `max_nodes` total. Pure in-RAM work (no
/// I/O) — `get_nodes_by_label` is an O(1) index lookup per label.
fn candidate_ids(core: &GraphCore, max_nodes: usize) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for label in PROVENANCE_LABELS {
        ids.extend(
            core.get_nodes_by_label(label, max_nodes)
                .into_iter()
                .map(|(id, _)| id),
        );
    }
    ids.sort_unstable();
    ids.dedup();
    ids.truncate(max_nodes);
    ids
}

/// Sweep every resident graph and anchor any `:ToolCall`/`:RunTrace` window whose
/// Merkle root has changed since that graph's last anchor. Returns the number of
/// graphs actually anchored this tick — 0 when every window is unchanged, when no
/// resident graph carries a provenance node, or when no durable redb backend is
/// configured (mirrors `offload_cold_tenants`'s no-op-when-unconfigured
/// contract). A read failure or commit failure for one graph is logged and
/// skipped; it does not abort the sweep for the remaining graphs.
pub async fn sweep(state: &Arc<RwLock<ServerState>>) -> u64 {
    let (entries, persistence) = {
        let s = state.read().await;
        let entries: Vec<(String, Arc<GraphCore>)> = s
            .registry
            .all_entries()
            .iter()
            .map(|e| (e.name.clone(), e.core.clone()))
            .collect();
        (entries, s.persistence.clone())
    };
    let Some(redb) = persistence.as_ref().and_then(|p| p.as_redb()) else {
        return 0;
    };

    let max_nodes = max_window_nodes();
    let mut anchored = 0u64;
    for (name, core) in entries {
        let ids = candidate_ids(&core, max_nodes);
        if ids.is_empty() {
            continue;
        }
        let fname = crate::persist::sanitize(&name);
        let members = match redb.provenance_leaf_hashes_blocking(&fname, &ids) {
            Ok(m) => m,
            Err(error) => {
                tracing::warn!(
                    graph = %name,
                    %error,
                    "provenance anchor: leaf-hash read failed, skipping this tick"
                );
                continue;
            }
        };
        if members.is_empty() {
            continue;
        }
        let leaf_hashes: Vec<crate::audit::Hash> = members.iter().map(|(_, h)| *h).collect();
        let root = crate::audit::mth_from_hashes(&leaf_hashes);
        match redb.provenance_anchor_commit_blocking(&fname, root, members.clone()) {
            Ok(Some(seq)) => {
                anchored += 1;
                tracing::info!(
                    graph = %name,
                    anchor_seq = seq,
                    window_size = members.len(),
                    root = %hex::encode(root),
                    "provenance anchor: appended"
                );
            }
            Ok(None) => {
                // Unchanged since the last anchor for this graph -- nothing written.
            }
            Err(error) => {
                tracing::warn!(
                    graph = %name,
                    %error,
                    "provenance anchor: commit failed, skipping this tick"
                );
            }
        }
    }
    anchored
}
