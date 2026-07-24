//! Cluster node-info store — durable `node_id -> {raft_addr,
//! advertised_client_addr, tls_server_name}` map (CONCEPT:EG-KG.sharding.cluster-topology,
//! ADR-1 / W1.1, `reports/wave1/ADR-scale-trio.md` §ADR-1).
//!
//! ## Why this exists
//!
//! `Method::ClusterMembers` and `PlacementRoute`'s `endpoints` field need every
//! cluster node's client-reachable address to hand back to a discovering client,
//! replacing the static hand-maintained `GRAPH_RAFT_GROUP_ENDPOINTS` map. Each node
//! self-reports its own identity through `Method::NodeInfoUpsert`, a
//! `ClusterAdmin`-domain native Raft command (mirrors `CatalogAssign`): the SAME
//! committed log entry applies deterministically on every replica (every node runs
//! the identical apply-time write with the identical field values), so every
//! node's LOCAL copy of this store converges to hold every OTHER node's row too —
//! replication via re-execution, not byte-shipping. This is the exact mechanism
//! [`super::tenant_catalog::TenantCatalog`] already uses for M3 placements; this
//! store mirrors its shape closely.
//!
//! Deliberately NOT graph nodes (placement's O(N) full-catalog-scan lesson,
//! documented on [`crate::raft::placement::PlacementCatalog`]): this is a small,
//! dedicated, own-file redb table — at most one row per cluster node, never
//! per-tenant/per-graph data.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

/// Durable table: `node_id -> msgpack(NodeInfo)` (CONCEPT:EG-KG.sharding.cluster-topology). One row per
/// cluster node — bounded by [`MAX_NODE_INFO_ENTRIES`], never per-tenant data.
const NODE_INFO: TableDefinition<u64, &[u8]> = TableDefinition::new("node_info");
const MAX_NODE_INFO_ENTRIES: usize = 4_096;
const MAX_NODE_INFO_FIELD_BYTES: usize = 4 * 1024;

/// One cluster node's self-reported identity (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: u64,
    /// This node's Raft-RPC `host:port` — the SAME address it advertises in
    /// `EPISTEMIC_GRAPH_RAFT_PEERS`.
    pub raft_addr: String,
    /// This node's client-reachable address for the SERVED wire protocol
    /// (`EPISTEMIC_GRAPH_ADVERTISED_CLIENT_ADDR`) — what `Method::ClusterMembers`
    /// and `PlacementRoute.endpoints` hand back to a discovering client.
    pub advertised_client_addr: String,
    /// TLS server name (SNI / certificate hostname) a client should present when
    /// connecting to `advertised_client_addr` over `tls://`. `None` ⇒ verify
    /// against the address's own host (the TLS default).
    pub tls_server_name: Option<String>,
}

fn validate_node_info(info: &NodeInfo) -> Result<(), String> {
    if info.raft_addr.is_empty() || info.raft_addr.len() > MAX_NODE_INFO_FIELD_BYTES {
        return Err("node info raft_addr exceeds resource limits".to_string());
    }
    if info.advertised_client_addr.is_empty()
        || info.advertised_client_addr.len() > MAX_NODE_INFO_FIELD_BYTES
    {
        return Err("node info advertised_client_addr exceeds resource limits".to_string());
    }
    if info
        .tls_server_name
        .as_deref()
        .is_some_and(|name| name.is_empty() || name.len() > MAX_NODE_INFO_FIELD_BYTES)
    {
        return Err("node info tls_server_name exceeds resource limits".to_string());
    }
    Ok(())
}

fn decode_node_info(bytes: &[u8]) -> Result<NodeInfo, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_NODE_INFO_FIELD_BYTES * 4,
            32,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "node info row is invalid or exceeds resource limits".to_string())
}

/// Durable, replicated (via deterministic command re-execution — see module docs)
/// `node_id -> `[`NodeInfo`] map (CONCEPT:EG-KG.sharding.cluster-topology).
///
/// Thread-safe (an internal `RwLock`): `all`/`get` read the in-memory cache while
/// `upsert` writes through to the durable table when backed. Mirrors
/// [`super::tenant_catalog::TenantCatalog`]'s in-memory-cache-over-an-own-file
/// shape — infrequent, small, cluster-wide admin state, not the K-way sharded
/// group-commit writer's hot path.
pub struct NodeInfoStore {
    entries: RwLock<HashMap<u64, NodeInfo>>,
    db: Option<Database>,
    /// Local monotonic generation counter, bumped on every successful `upsert`
    /// (CONCEPT:EG-KG.sharding.cluster-topology). Since an upsert replicates identically to every
    /// node (see module docs), this counter converges cluster-wide too. Exposed
    /// as `Method::ClusterMembers`' `epoch` — a cheap "has the known member set
    /// changed" freshness signal for a discovering client's cache; it is NOT a
    /// per-partition routing fence (that remains `PlacementCatalog`'s epoch).
    generation: AtomicU64,
}

impl NodeInfoStore {
    /// A non-durable store (tests / a build without `redb`). Mutations are
    /// visible immediately but do NOT survive a restart.
    pub fn in_memory() -> Self {
        NodeInfoStore {
            entries: RwLock::new(HashMap::new()),
            db: None,
            generation: AtomicU64::new(0),
        }
    }

    /// Open (or create) a durable store at `node_info.redb` under `persist_dir`
    /// and load every row into memory (CONCEPT:EG-KG.sharding.cluster-topology). A fresh dir yields an
    /// empty store — `Method::ClusterMembers` answers an empty topology until a
    /// node self-reports.
    pub fn open(persist_dir: &str) -> Result<Self, String> {
        std::fs::create_dir_all(persist_dir).map_err(|e| e.to_string())?;
        let path = std::path::Path::new(persist_dir).join("node_info.redb");
        let db = Database::create(&path).map_err(|e| e.to_string())?;
        {
            let wtx = db.begin_write().map_err(|e| e.to_string())?;
            wtx.open_table(NODE_INFO).map_err(|e| e.to_string())?;
            wtx.commit().map_err(|e| e.to_string())?;
        }
        let mut entries = HashMap::new();
        {
            let rtx = db.begin_read().map_err(|e| e.to_string())?;
            let table = rtx.open_table(NODE_INFO).map_err(|e| e.to_string())?;
            for row in table.iter().map_err(|e| e.to_string())? {
                if entries.len() >= MAX_NODE_INFO_ENTRIES {
                    return Err("node info store exceeds resource limits".to_string());
                }
                let (k, v) = row.map_err(|e| e.to_string())?;
                let info = decode_node_info(v.value())?;
                if info.node_id != k.value() {
                    return Err("node info row key/value node_id mismatch".to_string());
                }
                validate_node_info(&info)?;
                entries.insert(k.value(), info);
            }
        }
        let generation = entries.len() as u64;
        Ok(NodeInfoStore {
            entries: RwLock::new(entries),
            db: Some(db),
            generation: AtomicU64::new(generation),
        })
    }

    /// Upsert `info` (CONCEPT:EG-KG.sharding.cluster-topology). Idempotent: re-reporting the same
    /// `node_id` (e.g. on every restart) simply overwrites its row. Writing
    /// through to the durable table when backed.
    pub fn upsert(&self, info: NodeInfo) -> Result<(), String> {
        validate_node_info(&info)?;
        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
        if !entries.contains_key(&info.node_id) && entries.len() >= MAX_NODE_INFO_ENTRIES {
            return Err("node info store exceeds resource limits".to_string());
        }
        if let Some(db) = &self.db {
            let blob = rmp_serde::to_vec_named(&info).map_err(|e| e.to_string())?;
            let wtx = db.begin_write().map_err(|e| e.to_string())?;
            {
                let mut t = wtx.open_table(NODE_INFO).map_err(|e| e.to_string())?;
                t.insert(info.node_id, blob.as_slice())
                    .map_err(|e| e.to_string())?;
            }
            wtx.commit().map_err(|e| e.to_string())?;
        }
        entries.insert(info.node_id, info);
        self.generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Every known node, sorted by `node_id` (deterministic ordering for a stable
    /// `Method::ClusterMembers` response).
    pub fn all(&self) -> Vec<NodeInfo> {
        let mut v: Vec<NodeInfo> = self
            .entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        v.sort_by_key(|i| i.node_id);
        v
    }

    /// One node's info, if known.
    pub fn get(&self, node_id: u64) -> Option<NodeInfo> {
        self.entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&node_id)
            .cloned()
    }

    /// The local generation counter (see the field doc above).
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Resolve `voters ++ learners` to their known [`NodeInfo`] rows, LEADER
    /// FIRST, then the remaining voters (sorted), then learners (sorted)
    /// (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 — shared by `Method::ClusterMembers`'
    /// per-group member list and `PlacementRoute.endpoints`, so both surfaces
    /// agree on ordering). A member with no durable self-report yet is skipped
    /// (best effort — its endpoint is genuinely unknown), never fabricated.
    pub fn ordered_members(
        &self,
        voters: &[u64],
        learners: &[u64],
        leader: Option<u64>,
    ) -> Vec<NodeInfo> {
        let mut ordered_ids: Vec<u64> = Vec::with_capacity(voters.len() + learners.len());
        if let Some(leader) = leader {
            if voters.contains(&leader) || learners.contains(&leader) {
                ordered_ids.push(leader);
            }
        }
        let mut voters_sorted: Vec<u64> = voters
            .iter()
            .copied()
            .filter(|id| Some(*id) != leader)
            .collect();
        voters_sorted.sort_unstable();
        ordered_ids.extend(voters_sorted);
        let mut learners_sorted: Vec<u64> = learners
            .iter()
            .copied()
            .filter(|id| Some(*id) != leader)
            .collect();
        learners_sorted.sort_unstable();
        ordered_ids.extend(learners_sorted);
        ordered_ids
            .into_iter()
            .filter_map(|id| self.get(id))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(node_id: u64) -> NodeInfo {
        NodeInfo {
            node_id,
            raft_addr: format!("127.0.0.1:710{node_id}"),
            advertised_client_addr: format!("tcp://127.0.0.1:810{node_id}"),
            tls_server_name: None,
        }
    }

    #[test]
    fn empty_store_answers_empty() {
        let store = NodeInfoStore::in_memory();
        assert!(store.all().is_empty());
        assert_eq!(store.get(1), None);
        assert_eq!(store.generation(), 0);
    }

    #[test]
    fn upsert_is_idempotent_and_overwrites() {
        let store = NodeInfoStore::in_memory();
        store.upsert(info(1)).unwrap();
        let gen_after_first = store.generation();
        assert!(gen_after_first > 0);
        let mut updated = info(1);
        updated.advertised_client_addr = "tcp://127.0.0.1:9999".to_string();
        store.upsert(updated.clone()).unwrap();
        assert_eq!(store.all(), vec![updated]);
        assert!(store.generation() > gen_after_first);
    }

    #[test]
    fn all_is_sorted_by_node_id() {
        let store = NodeInfoStore::in_memory();
        store.upsert(info(3)).unwrap();
        store.upsert(info(1)).unwrap();
        store.upsert(info(2)).unwrap();
        let ids: Vec<u64> = store.all().iter().map(|i| i.node_id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn rejects_oversized_and_empty_fields() {
        let store = NodeInfoStore::in_memory();
        let mut bad = info(1);
        bad.raft_addr = String::new();
        assert!(store.upsert(bad).is_err());
        let mut bad = info(1);
        bad.advertised_client_addr = "x".repeat(MAX_NODE_INFO_FIELD_BYTES + 1);
        assert!(store.upsert(bad).is_err());
        let mut bad = info(1);
        bad.tls_server_name = Some(String::new());
        assert!(store.upsert(bad).is_err());
    }

    #[test]
    fn durable_store_survives_reopen() {
        let dir = std::env::temp_dir().join(format!("eg-node-info-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        {
            let store = NodeInfoStore::open(&dir_s).expect("open store");
            store.upsert(info(1)).unwrap();
            store.upsert(info(2)).unwrap();
        }
        let reopened = NodeInfoStore::open(&dir_s).expect("reopen store");
        assert_eq!(reopened.all(), vec![info(1), info(2)]);
        // A restart's first self-report continues the SAME generation lineage
        // (the counter is seeded from the reloaded row count) rather than
        // resetting to zero, so a discovering client never sees `epoch` regress.
        assert_eq!(reopened.generation(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn decode_node_info_rejects_allocation_bombs() {
        assert!(decode_node_info(&[0xdd, 0xff, 0xff, 0xff, 0xff]).is_err());
    }

    #[test]
    fn ordered_members_puts_leader_first_then_sorted_voters_then_learners() {
        let store = NodeInfoStore::in_memory();
        for id in [1, 2, 3, 4] {
            store.upsert(info(id)).unwrap();
        }
        let ordered = store.ordered_members(&[3, 1, 2], &[4], Some(2));
        let ids: Vec<u64> = ordered.iter().map(|i| i.node_id).collect();
        assert_eq!(ids, vec![2, 1, 3, 4]);
    }

    #[test]
    fn ordered_members_skips_unknown_ids_without_fabricating() {
        let store = NodeInfoStore::in_memory();
        store.upsert(info(1)).unwrap();
        // node 2 is a raft voter but has never self-reported.
        let ordered = store.ordered_members(&[1, 2], &[], Some(1));
        let ids: Vec<u64> = ordered.iter().map(|i| i.node_id).collect();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn ordered_members_handles_no_leader() {
        let store = NodeInfoStore::in_memory();
        store.upsert(info(1)).unwrap();
        store.upsert(info(2)).unwrap();
        let ordered = store.ordered_members(&[2, 1], &[], None);
        let ids: Vec<u64> = ordered.iter().map(|i| i.node_id).collect();
        assert_eq!(ids, vec![1, 2]);
    }
}
