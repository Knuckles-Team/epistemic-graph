//! Durable cache for one graph's computed hierarchical-Leiden cluster tree
//! (CONCEPT:EG-KG.compute.leiden-hierarchy, VIZ-1).
//!
//! ## Why this exists
//!
//! `Method::ClusterHierarchyRefresh` computes `eg_compute::algorithms::
//! cluster_hierarchy` (server-side clustering for million-node graph
//! visualization) and needs to hand that result to `ClusterHierarchyClusters`/
//! `ClusterHierarchyExpand` WITHOUT recomputing it per request — that's the whole
//! point of a "refresh, then serve" contract instead of "cluster on every read".
//!
//! Deliberately NOT graph nodes/edges: the engine holds at most one edge per
//! ordered node pair and `upsert_edge` REPLACES the relationship type, so
//! representing cluster membership as edges between existing nodes would
//! silently destroy asserted relationships (a measured defect on this program).
//! Deliberately NOT a `mutation::GATEWAY_ROUTED` write either — the cached value
//! is a non-authoritative, always-recomputable-from-the-graph derived artifact
//! (same classification the plan-backed matview result cache already gets), so
//! it gets its own small, dedicated, own-file redb table instead of a WAL entry.
//!
//! Own-file, own-table, mirroring [`super::node_info_store`]'s shape (opened
//! under the same `persist_dir` a `RedbBackend` already resolves) but WITHOUT
//! node_info's cluster-wide Raft self-report/replication story: a stale or
//! missing cache entry is never a correctness problem, only a "recompute is
//! needed" signal, so there is nothing here to reconcile across replicas.

use std::collections::HashMap;
use std::sync::RwLock;

use redb::TableDefinition;

/// The one owner table of `OwnerLayout::ClusterHierarchy`. `eg-storage`
/// declares it; this module only names it (RF-RULING-004).
///
/// `graph_name -> msgpack(ClusterHierarchyResult)`. One row per graph that has
/// ever called `ClusterHierarchyRefresh` — bounded by [`MAX_ENTRIES`], never
/// per-node data (the row VALUE scales with graph size, the table's ROW COUNT
/// does not).
const CLUSTER_HIERARCHY: TableDefinition<&str, &[u8]> = TableDefinition::new("cluster_hierarchy");
const CLUSTER_HIERARCHY_PHYSICAL_STORE: &str = "epistemic-graph:cluster-hierarchy";
const CLUSTER_HIERARCHY_SCOPE_RESOURCE: &str = "cluster-hierarchy";
const CLUSTER_HIERARCHY_SCOPE_INCARNATION: &str = "cluster-hierarchy:v1";

/// Bounds how many distinct graphs may have a cached hierarchy at once — a
/// resource limit, not a product limit (mirrors `node_info_store::
/// MAX_NODE_INFO_ENTRIES`'s reasoning): this store is a cache, never the
/// authoritative graph catalog, so an operator running far more tenants than
/// this should evict/prune rather than expect every graph cached forever.
const MAX_ENTRIES: usize = 8_192;

/// One graph's hierarchy is a single MessagePack blob whose size scales with
/// graph size (`leaf_membership` is `O(base_node_count)`) — bounded generously
/// (a 1M-node graph's measured hierarchy blob is tens of MB, see the VIZ-1
/// benchmark) so a single caller can never force unbounded resident/durable
/// growth per row.
const MAX_BLOB_BYTES: usize = 512 * 1024 * 1024;

/// Durable per-graph cluster-hierarchy cache (CONCEPT:EG-KG.compute.leiden-hierarchy, VIZ-1).
/// Cheap in-RAM `get`/`put`; the optional `durable` sidecar store mirrors every
/// write through so it survives a restart. `PersistenceBackend::{save,load}_cluster_hierarchy`
/// (`RedbBackend`'s impl) are this store's only callers.
pub struct ClusterHierarchyStore {
    entries: RwLock<HashMap<String, Vec<u8>>>,
    durable: Option<crate::sidecar_store::SidecarStore<eg_storage::ClusterHierarchyOwner>>,
}

impl ClusterHierarchyStore {
    /// A cache with no durable backing — entries are visible immediately but do
    /// NOT survive a restart. Used by test/memory backends.
    pub fn in_memory() -> Self {
        ClusterHierarchyStore {
            entries: RwLock::new(HashMap::new()),
            durable: None,
        }
    }

    /// Open (or create) `cluster_hierarchy.redb` under `persist_dir` and load
    /// every row into memory. A fresh dir yields an empty cache — the first
    /// `ClusterHierarchyClusters`/`Expand` call for any graph then reports "no
    /// hierarchy cached yet, call ClusterHierarchyRefresh first".
    pub fn open(persist_dir: &str) -> Result<Self, String> {
        let path = std::path::Path::new(persist_dir).join("cluster_hierarchy.redb");
        let durable = crate::sidecar_store::SidecarStore::open(
            &path,
            CLUSTER_HIERARCHY_PHYSICAL_STORE,
            CLUSTER_HIERARCHY_SCOPE_RESOURCE,
            CLUSTER_HIERARCHY_SCOPE_INCARNATION,
            crate::store_authority::process_authority(),
        )?;
        let mut entries = HashMap::new();
        {
            let read = durable.read()?;
            let table = read.open_owner_table(CLUSTER_HIERARCHY)?;
            for row in table.iter()? {
                if entries.len() >= MAX_ENTRIES {
                    return Err("cluster hierarchy store exceeds resource limits".to_string());
                }
                let (k, v) = row.map_err(|e| e.to_string())?;
                let value = v.value();
                if value.len() > MAX_BLOB_BYTES {
                    return Err("cluster hierarchy row exceeds resource limits".to_string());
                }
                entries.insert(k.value().to_string(), value.to_vec());
            }
        }
        Ok(ClusterHierarchyStore {
            entries: RwLock::new(entries),
            durable: Some(durable),
        })
    }

    /// Replace the cached hierarchy for `graph`. Overwrites any existing entry
    /// (a refresh always supersedes the prior cache, never merges with it).
    pub fn put(&self, graph: &str, blob: Vec<u8>) -> Result<(), String> {
        if graph.is_empty() {
            return Err("cluster hierarchy graph name must not be empty".to_string());
        }
        if blob.len() > MAX_BLOB_BYTES {
            return Err("cluster hierarchy blob exceeds resource limits".to_string());
        }
        if let Some(durable) = &self.durable {
            durable.maintain("cluster_hierarchy_put", |owner| {
                owner
                    .open_table(CLUSTER_HIERARCHY)?
                    .insert(graph, blob.as_slice())
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })?;
        }
        let mut entries = self
            .entries
            .write()
            .map_err(|_| "cluster hierarchy store lock poisoned".to_string())?;
        if !entries.contains_key(graph) && entries.len() >= MAX_ENTRIES {
            return Err("cluster hierarchy store exceeds resource limits".to_string());
        }
        entries.insert(graph.to_string(), blob);
        Ok(())
    }

    /// Fetch the cached hierarchy blob for `graph`, if any.
    pub fn get(&self, graph: &str) -> Option<Vec<u8>> {
        self.entries.read().ok()?.get(graph).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_roundtrips() {
        let store = ClusterHierarchyStore::in_memory();
        assert!(store.get("g1").is_none());
        store.put("g1", vec![1, 2, 3]).unwrap();
        assert_eq!(store.get("g1"), Some(vec![1, 2, 3]));
        // A refresh REPLACES, never merges.
        store.put("g1", vec![4, 5]).unwrap();
        assert_eq!(store.get("g1"), Some(vec![4, 5]));
        assert!(store.get("g2").is_none());
    }

    #[test]
    fn open_persists_across_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "eg-cluster-hierarchy-store-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let dir_s = dir.to_string_lossy().to_string();
        {
            let store = ClusterHierarchyStore::open(&dir_s).expect("open store");
            store.put("tenant-a", vec![9, 9, 9]).unwrap();
        }
        {
            let reopened = ClusterHierarchyStore::open(&dir_s).expect("reopen store");
            assert_eq!(reopened.get("tenant-a"), Some(vec![9, 9, 9]));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_empty_graph_name_and_oversized_blob() {
        let store = ClusterHierarchyStore::in_memory();
        assert!(store.put("", vec![1]).is_err());
        assert!(store.put("g1", vec![0u8; MAX_BLOB_BYTES + 1]).is_err());
    }
}
