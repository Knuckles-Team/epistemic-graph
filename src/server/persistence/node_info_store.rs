//! Cluster node-info store — durable `node_id -> {cluster_id, member_identity,
//! raft_addr, advertised_client_addr, TLS/certificate metadata}` map
//! (CONCEPT:EG-KG.sharding.cluster-topology,
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
use sha2::{Digest, Sha256};

/// Durable table: `node_id -> msgpack(NodeInfo)` (CONCEPT:EG-KG.sharding.cluster-topology). One row per
/// cluster node — bounded by [`MAX_NODE_INFO_ENTRIES`], never per-tenant data.
const NODE_INFO: TableDefinition<u64, &[u8]> = TableDefinition::new("node_info");
const NODE_INFO_META: TableDefinition<&str, &[u8]> = TableDefinition::new("node_info_meta");
const NODE_INFO_META_KEY: &str = "v1";
const MAX_NODE_INFO_ENTRIES: usize = 4_096;
const MAX_NODE_INFO_FIELD_BYTES: usize = 4 * 1024;
const MAX_CERTIFICATE_ID_BYTES: usize = 512;

/// Stable identity for a member. Endpoint and certificate rotation metadata
/// are intentionally absent: those values are mutable observations, while
/// this digest remains the member's immutable cluster-scoped identity.
pub(crate) fn member_identity_for(cluster_id: &str, node_id: u64) -> String {
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph/member-identity/v1\0");
    digest.update((cluster_id.len() as u64).to_be_bytes());
    digest.update(cluster_id.as_bytes());
    digest.update(node_id.to_be_bytes());
    format!("sha256:{}", hex::encode(digest.finalize()))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct NodeInfoMeta {
    cluster_id: String,
    generation: u64,
}

/// One cluster node's self-reported identity (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Stable identity of the configured Raft cluster. This is an operator /
    /// Raft authority value, never accepted from a discovery caller.
    #[serde(default)]
    pub cluster_id: String,
    pub node_id: u64,
    /// Immutable member identity derived from `(cluster_id, node_id)`.
    #[serde(default)]
    pub member_identity: String,
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
    /// Opaque certificate fingerprint/reference. No PEM, private key or bearer
    /// credential is ever stored or returned by this surface.
    #[serde(default)]
    pub certificate_id: Option<String>,
    #[serde(default)]
    pub certificate_rotation_epoch: u64,
    #[serde(default)]
    pub certificate_not_before_ms: Option<u64>,
    #[serde(default)]
    pub certificate_not_after_ms: Option<u64>,
}

fn valid_host_port(value: &str, scheme: Option<&str>) -> bool {
    if value.is_empty()
        || value.len() > MAX_NODE_INFO_FIELD_BYTES
        || value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return false;
    }
    let address = if let Some(expected_scheme) = scheme {
        let Some(rest) = value.strip_prefix(expected_scheme) else {
            return false;
        };
        rest
    } else {
        value
    };
    // Reject path/query/fragment/userinfo AFTER the scheme is stripped. Running
    // this on the raw value rejected every scheme-qualified address, because
    // `tcp://` contains `/` -- so a perfectly valid `tcp://127.0.0.1:9999` never
    // reached the host/port parse at all. The guard itself is still needed: it
    // is what stops `tcp://host:1/path`, `tcp://user@host:1` and query/fragment
    // smuggling from being accepted as a bare host:port.
    if address
        .chars()
        .any(|character| matches!(character, '/' | '?' | '#' | '@'))
    {
        return false;
    }
    let (_host, port) = if let Some(rest) = address.strip_prefix('[') {
        let Some((host, port)) = rest.split_once("]:") else {
            return false;
        };
        if host.is_empty() || host.contains(']') {
            return false;
        }
        (host, port)
    } else {
        let Some((host, port)) = address.rsplit_once(':') else {
            return false;
        };
        if host.is_empty() || host.contains(':') {
            return false;
        }
        (host, port)
    };
    port.parse::<u16>().is_ok_and(|port| port > 0)
}

fn validate_node_info(info: &NodeInfo) -> Result<(), String> {
    if info.cluster_id.is_empty()
        || info.cluster_id.len() > MAX_NODE_INFO_FIELD_BYTES
        || info
            .cluster_id
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err("node info cluster_id exceeds resource limits".to_string());
    }
    if info.member_identity != member_identity_for(&info.cluster_id, info.node_id) {
        return Err("node info member_identity does not match cluster identity".to_string());
    }
    if !valid_host_port(&info.raft_addr, None) {
        return Err("node info raft_addr is not a bounded host:port endpoint".to_string());
    }
    if !valid_host_port(&info.advertised_client_addr, Some("tcp://"))
        && !valid_host_port(&info.advertised_client_addr, Some("tls://"))
    {
        return Err(
            "node info advertised_client_addr must be tcp:// or tls:// host:port".to_string(),
        );
    }
    if info.tls_server_name.as_deref().is_some_and(|name| {
        name.is_empty()
            || name.len() > MAX_NODE_INFO_FIELD_BYTES
            || name
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
    }) {
        return Err("node info tls_server_name exceeds resource limits".to_string());
    }
    if info
        .certificate_id
        .as_deref()
        .is_some_and(|id| id.is_empty() || id.len() > MAX_CERTIFICATE_ID_BYTES)
    {
        return Err("node info certificate_id exceeds resource limits".to_string());
    }
    if info.certificate_id.as_deref().is_some_and(|id| {
        id.chars()
            .any(|character| character.is_whitespace() || character.is_control())
    }) {
        return Err("node info certificate_id contains whitespace".to_string());
    }
    if info
        .certificate_not_before_ms
        .zip(info.certificate_not_after_ms)
        .is_some_and(|(before, after)| before > after)
    {
        return Err("node info certificate validity interval is inverted".to_string());
    }
    if info.certificate_rotation_epoch > 0 && info.certificate_id.is_none() {
        return Err("certificate rotation epoch requires certificate_id".to_string());
    }
    Ok(())
}

fn validate_legacy_node_info(info: &NodeInfo) -> Result<(), String> {
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
    cluster_id: RwLock<Option<String>>,
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
            cluster_id: RwLock::new(None),
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
            wtx.open_table(NODE_INFO_META).map_err(|e| e.to_string())?;
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
                if info.cluster_id.is_empty() || info.member_identity.is_empty() {
                    validate_legacy_node_info(&info)?;
                } else {
                    validate_node_info(&info)?;
                }
                entries.insert(k.value(), info);
            }
        }
        let (metadata_cluster_id, metadata_generation) = {
            let rtx = db.begin_read().map_err(|e| e.to_string())?;
            let table = rtx.open_table(NODE_INFO_META).map_err(|e| e.to_string())?;
            table
                .get(NODE_INFO_META_KEY)
                .map_err(|e| e.to_string())?
                .map(|value| {
                    eg_types::msgpack::decode_bounded::<NodeInfoMeta>(
                        value.value(),
                        eg_types::msgpack::MsgpackLimits::new(1024, 16, 8),
                    )
                    .map_err(|_| "node info metadata is invalid".to_string())
                })
                .transpose()?
                .map(|meta| (Some(meta.cluster_id), meta.generation))
                .unwrap_or((None, entries.len() as u64))
        };
        if metadata_cluster_id.as_deref().is_some_and(|cluster_id| {
            cluster_id.is_empty()
                || cluster_id.len() > MAX_NODE_INFO_FIELD_BYTES
                || cluster_id
                    .chars()
                    .any(|character| character.is_whitespace() || character.is_control())
        }) {
            return Err("node info metadata cluster_id exceeds resource limits".to_string());
        }
        if metadata_generation < entries.len() as u64 {
            return Err("node info metadata generation regressed below row count".to_string());
        }
        let mut entry_cluster_id: Option<String> = None;
        for info in entries.values().filter(|info| !info.cluster_id.is_empty()) {
            if entry_cluster_id
                .as_deref()
                .is_some_and(|cluster_id| cluster_id != info.cluster_id)
            {
                return Err("node info rows disagree on cluster identity".to_string());
            }
            entry_cluster_id = Some(info.cluster_id.clone());
        }
        if metadata_cluster_id
            .as_deref()
            .zip(entry_cluster_id.as_deref())
            .is_some_and(|(metadata, entry)| metadata != entry)
        {
            return Err("node info metadata cluster_id disagrees with a member row".to_string());
        }
        let cluster_id = metadata_cluster_id.or(entry_cluster_id);
        Ok(NodeInfoStore {
            entries: RwLock::new(entries),
            db: Some(db),
            cluster_id: RwLock::new(cluster_id),
            generation: AtomicU64::new(metadata_generation),
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
        let mut cluster_id = self.cluster_id.write().unwrap_or_else(|e| e.into_inner());
        if cluster_id
            .as_deref()
            .is_some_and(|current| current != info.cluster_id)
        {
            return Err("node info belongs to a different cluster".to_string());
        }
        if entries.values().any(|existing| {
            !existing.cluster_id.is_empty() && existing.cluster_id != info.cluster_id
        }) {
            return Err("node info store contains a different cluster identity".to_string());
        }
        let next_generation = self
            .generation
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or_else(|| "node info generation exhausted".to_string())?;
        if let Some(db) = &self.db {
            let blob = rmp_serde::to_vec_named(&info).map_err(|e| e.to_string())?;
            let meta_blob = rmp_serde::to_vec_named(&NodeInfoMeta {
                cluster_id: info.cluster_id.clone(),
                generation: next_generation,
            })
            .map_err(|e| e.to_string())?;
            let wtx = db.begin_write().map_err(|e| e.to_string())?;
            {
                let mut t = wtx.open_table(NODE_INFO).map_err(|e| e.to_string())?;
                t.insert(info.node_id, blob.as_slice())
                    .map_err(|e| e.to_string())?;
                let mut meta = wtx.open_table(NODE_INFO_META).map_err(|e| e.to_string())?;
                meta.insert(NODE_INFO_META_KEY, meta_blob.as_slice())
                    .map_err(|e| e.to_string())?;
            }
            wtx.commit().map_err(|e| e.to_string())?;
        }
        entries.insert(info.node_id, info);
        *cluster_id = entries
            .values()
            .find(|entry| !entry.cluster_id.is_empty())
            .map(|entry| entry.cluster_id.clone());
        self.generation.store(next_generation, Ordering::Release);
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

    /// The one cluster identity established by the Raft self-report authority.
    pub fn cluster_id(&self) -> Option<String> {
        self.cluster_id
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
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
            // Never let a legacy row, or a row corrupted after recovery, act
            // as endpoint authority.  `ClusterMembers` returns an explicit
            // incomplete/invalid error; the older PlacementRoute surface is
            // best-effort and therefore omits the untrusted row.
            .filter(|info| validate_node_info(info).is_ok())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(node_id: u64) -> NodeInfo {
        let cluster_id = "sha256:node-info-test".to_string();
        NodeInfo {
            member_identity: member_identity_for(&cluster_id, node_id),
            cluster_id,
            node_id,
            raft_addr: format!("127.0.0.1:710{node_id}"),
            advertised_client_addr: format!("tcp://127.0.0.1:810{node_id}"),
            tls_server_name: None,
            certificate_id: None,
            certificate_rotation_epoch: 0,
            certificate_not_before_ms: None,
            certificate_not_after_ms: None,
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
    fn valid_host_port_accepts_schemes_and_still_rejects_smuggling() {
        // Regression: the forbidden-character guard ran on the RAW value, so
        // `/` in `tcp://` rejected every scheme-qualified address before the
        // host/port parse was ever reached.
        assert!(valid_host_port("tcp://127.0.0.1:9999", Some("tcp://")));
        assert!(valid_host_port("tls://node-1:7000", Some("tls://")));
        assert!(valid_host_port("[::1]:7000", None));
        assert!(valid_host_port("host:1", None));

        // The guard is still load-bearing after the scheme is stripped.
        assert!(!valid_host_port("tcp://host:1/path", Some("tcp://")));
        assert!(!valid_host_port("tcp://user@host:1", Some("tcp://")));
        assert!(!valid_host_port("tcp://host:1?q=1", Some("tcp://")));
        assert!(!valid_host_port("tcp://host:1#f", Some("tcp://")));
        assert!(!valid_host_port("tcp://host:0", Some("tcp://")));
        assert!(!valid_host_port("http://host:1", Some("tcp://")));
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
        let mut bad = info(1);
        bad.member_identity = "sha256:forged".to_string();
        assert!(store.upsert(bad).is_err());
        let mut bad = info(1);
        bad.advertised_client_addr = "https://caller-supplied.invalid:443/path".to_string();
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
        // A restart preserves the durable generation rather than seeding from
        // row count, so repeated endpoint/certificate updates cannot make a
        // discovering client observe an epoch regression.
        assert_eq!(reopened.generation(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_generation_advances_across_repeated_updates_and_restart() {
        let dir =
            std::env::temp_dir().join(format!("eg-node-info-generation-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        {
            let store = NodeInfoStore::open(&dir_s).expect("open store");
            for port in [8201, 8202, 8203, 8204] {
                let mut current = info(1);
                current.advertised_client_addr = format!("tcp://127.0.0.1:{port}");
                store.upsert(current).unwrap();
            }
            assert_eq!(store.generation(), 4);
        }
        let reopened = NodeInfoStore::open(&dir_s).expect("reopen store");
        assert_eq!(reopened.generation(), 4);
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
