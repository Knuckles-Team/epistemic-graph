//! Resident-projection maintenance for the authoritative graph store.
//!
//! Durable graph state lives only in redb. This module owns the small set of
//! filesystem-key and in-memory maintenance helpers that sit above that store;
//! it does not implement a second snapshot or journal format.

use std::{path::Path, sync::Arc};

use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use crate::{graph::GraphCore, server::ServerState};

/// Convert a logical graph name into a bounded, path-safe durable key.
///
/// The previous character-replacement scheme was lossy (`a:b` and `a/b` shared
/// a key). Byte escaping is reversible for ordinary names; very long names use a
/// domain-separated SHA-256 key to respect filesystem component limits.
pub(crate) fn sanitize(name: &str) -> String {
    let mut key = String::with_capacity(name.len());
    for &byte in name.as_bytes() {
        use std::fmt::Write as _;
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            key.push(char::from(byte));
        } else {
            write!(&mut key, "~{byte:02x}").expect("writing to String cannot fail");
        }
    }
    if key.len() <= 200 {
        return key;
    }
    let digest = Sha256::digest(name.as_bytes());
    let mut bounded = String::with_capacity(66);
    bounded.push_str("~h");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut bounded, "{byte:02x}").expect("writing to String cannot fail");
    }
    bounded
}

/// Directory containing the persisted native ANN index for a graph.
pub fn annidx_dir(persist_dir: &str, name: &str) -> std::path::PathBuf {
    Path::new(persist_dir).join(format!("{}.annidx", sanitize(name)))
}

/// Apply an Ebbinghaus decay sweep across every registered graph.
pub async fn decay_all(
    state: &Arc<RwLock<ServerState>>,
    half_life_secs: f64,
    floor: f64,
    prune: bool,
) -> crate::types::DecayStats {
    let entries: Vec<Arc<GraphCore>> = {
        let state = state.read().await;
        state
            .registry
            .all_entries()
            .iter()
            .map(|entry| entry.core.clone())
            .collect()
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut total = crate::types::DecayStats::default();
    for core in entries {
        let stats = core.decay_sweep(now, half_life_secs, floor, prune);
        total.nodes_decayed += stats.nodes_decayed;
        total.edges_decayed += stats.edges_decayed;
        total.nodes_pruned += stats.nodes_pruned;
        total.edges_pruned += stats.edges_pruned;
    }
    total
}

/// Evict each graph's oldest resident nodes down to `max_nodes`.
///
/// Every candidate must first be confirmed in the authoritative store. Missing
/// persistence or a failed/partial presence query fails closed and leaves the
/// resident projection intact.
pub async fn evict_oversized_all(state: &Arc<RwLock<ServerState>>, max_nodes: usize) -> usize {
    let (backend, entries) = {
        let state = state.read().await;
        let entries = state
            .registry
            .all_entries()
            .iter()
            .map(|entry| (entry.name.clone(), entry.core.clone()))
            .collect::<Vec<_>>();
        (state.persistence.clone(), entries)
    };
    let Some(backend) = backend else {
        return 0;
    };

    let mut evicted = 0usize;
    let mut deferred = 0usize;
    for (name, core) in entries {
        let candidates = core.lru_eviction_candidates(max_nodes);
        if candidates.is_empty() {
            continue;
        }
        let presence = match backend.durable_node_presence(&sanitize(&name), &candidates) {
            Ok(presence) if presence.len() == candidates.len() => presence,
            Ok(_) | Err(_) => {
                deferred += candidates.len();
                continue;
            }
        };
        deferred += presence.iter().filter(|present| !**present).count();
        let durable = candidates
            .into_iter()
            .zip(presence)
            .filter_map(|(node_id, present)| present.then_some(node_id))
            .collect::<Vec<_>>();
        evicted += core.evict_resident_nodes(&durable);
    }
    if deferred > 0 {
        tracing::warn!(
            deferred,
            "resident nodes retained because authoritative durability was not confirmed"
        );
    }
    evicted
}

#[cfg(test)]
mod tests {
    use super::sanitize;

    #[test]
    fn durable_keys_are_path_safe_bounded_and_non_aliasing() {
        assert_eq!(sanitize("graph-a"), "graph-a");
        assert_ne!(sanitize("a:b"), sanitize("a/b"));
        assert!(!sanitize("a/b").contains('/'));
        assert!(sanitize(&"x".repeat(4_096)).len() <= 66);
    }
}
