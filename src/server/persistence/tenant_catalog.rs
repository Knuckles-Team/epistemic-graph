//! Tenant catalog — durable graph/tenant → shard (and future → node) map
//! (CONCEPT:EG-KG.sharding.empty-catalog-routing, M3 catalog-driven resharding).
//!
//! ## Why this exists
//!
//! EG-026 routes a graph to a durable shard with a STATELESS hash
//! (`FNV-1a(sanitized_name) % K`). That is correct and fast, but it is NOT
//! rebalanceable: you cannot move ONE hot tenant off an overloaded shard without
//! changing K and migrating every other graph too. For 100M agents/tenants across N
//! nodes (the master-gaps "scalable tenant catalog", P0) routing must become an
//! explicit, mutable MAP — `tenant/graph → {shard, node}` — so a single tenant can be
//! split/moved (online resharding) while everyone else stays put.
//!
//! ## The design that keeps EG-026 safe
//!
//! This catalog is an **override layer**, not a replacement:
//!
//! * [`TenantCatalog::resolve_shard`] returns the catalog's explicit shard for a graph
//!   IF one is assigned, **else falls back to the exact EG-026 `shard_index`**. So an
//!   EMPTY catalog is byte-for-byte identical to no catalog — pure FNV-1a. The routing
//!   seam in [`super::redb_backend::RedbBackend::shard_for`] is gated on the catalog
//!   being attached AND populated; the default deployment never sees a behavior change.
//! * The map is durable (an optional `catalog.redb`) so an assignment survives restart,
//!   but the durability is OPT-IN: [`TenantCatalog::in_memory`] gives a non-durable
//!   catalog for tests / ephemeral use.
//!
//! ## What it is NOT (yet) — see `docs/architecture/m3-resharding.md`
//!
//! This is the CORE (lookup / assign / reassign / fallback + durability). It does NOT
//! yet perform the actual data movement of a reassignment — that is online resharding
//! execution, which composes this catalog with the offline [`super::shard_migrate`]
//! tool and (for cross-node moves) M2 multi-Raft. Reassigning a populated tenant here
//! only flips the ROUTE; moving its rows is the remaining M3 work.

use std::collections::HashMap;
use std::sync::RwLock;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

/// Durable catalog table: `sanitized_graph_name → msgpack(ShardAssignment)`
/// (CONCEPT:EG-KG.sharding.empty-catalog-routing). One row per explicitly-placed tenant/graph. A graph with NO row
/// is routed by EG-026 FNV-1a, so the table only ever holds the *exceptions* to the
/// hash (moved / rebalanced tenants) — it does not have to enumerate all 100M graphs.
const CATALOG: TableDefinition<&str, &[u8]> = TableDefinition::new("tenant_catalog");

/// Where a tenant/graph is placed (CONCEPT:EG-KG.sharding.empty-catalog-routing). `shard` is the durable redb shard
/// index (matches EG-026's `graph-<shard>.redb`); `node` is the FUTURE cluster node id
/// for multi-node distribution (M3 distributed storage) — `None` today = "this node".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardAssignment {
    /// Durable shard index this graph's rows live in (EG-026 `graph-<shard>.redb`).
    pub shard: u32,
    /// Cluster node owning the shard, for future multi-node distribution. `None` ⇒
    /// the local node (single-node today). Reserved for M3 distributed storage.
    pub node: Option<u32>,
}

impl ShardAssignment {
    /// A local (this-node) assignment to `shard`.
    pub fn local(shard: u32) -> Self {
        ShardAssignment { shard, node: None }
    }
}

/// Durable, rebalanceable graph→shard map with a FNV-1a fallback (CONCEPT:EG-KG.sharding.empty-catalog-routing).
///
/// Thread-safe (an internal `RwLock`): the routing seam reads it on the hot write path,
/// while `assign`/`reassign` mutate it off-band. Lookups are O(1) hash-map hits; the
/// durable write (when backed) is a single small redb commit, off the per-op path.
pub struct TenantCatalog {
    /// In-memory authoritative view (loaded from `db` at open, written-through on
    /// every mutation). Empty map ⇒ pure EG-026 routing.
    entries: RwLock<HashMap<String, ShardAssignment>>,
    /// Optional durable backing (`catalog.redb`). `None` ⇒ in-memory only.
    db: Option<Database>,
}

impl TenantCatalog {
    /// A non-durable catalog (tests / ephemeral). Mutations are visible immediately but
    /// do NOT survive a restart.
    pub fn in_memory() -> Self {
        TenantCatalog {
            entries: RwLock::new(HashMap::new()),
            db: None,
        }
    }

    /// Open (or create) a durable catalog at `catalog.redb` under `persist_dir` and
    /// load every assignment into memory (CONCEPT:EG-KG.sharding.empty-catalog-routing). A fresh dir yields an EMPTY
    /// catalog ⇒ pure EG-026 routing until something is assigned.
    pub fn open(persist_dir: &str) -> Result<Self, String> {
        std::fs::create_dir_all(persist_dir).map_err(|e| e.to_string())?;
        let path = std::path::Path::new(persist_dir).join("catalog.redb");
        let db = Database::create(&path).map_err(|e| e.to_string())?;
        // Ensure the table exists so a first-ever read txn doesn't error.
        {
            let wtx = db.begin_write().map_err(|e| e.to_string())?;
            wtx.open_table(CATALOG).map_err(|e| e.to_string())?;
            wtx.commit().map_err(|e| e.to_string())?;
        }
        let mut entries = HashMap::new();
        {
            let rtx = db.begin_read().map_err(|e| e.to_string())?;
            let table = rtx.open_table(CATALOG).map_err(|e| e.to_string())?;
            for row in table.iter().map_err(|e| e.to_string())? {
                let (k, v) = row.map_err(|e| e.to_string())?;
                if let Ok(a) = rmp_serde::from_slice::<ShardAssignment>(v.value()) {
                    entries.insert(k.value().to_string(), a);
                }
            }
        }
        Ok(TenantCatalog {
            entries: RwLock::new(entries),
            db: Some(db),
        })
    }

    /// Explicit assignment for `graph_fname`, or `None` when the graph is routed by the
    /// EG-026 hash (the common case). `graph_fname` must already be `sanitize`d — the
    /// catalog keys on the same durable graph key EG-026 routes on.
    pub fn lookup(&self, graph_fname: &str) -> Option<ShardAssignment> {
        self.entries.read().unwrap().get(graph_fname).copied()
    }

    /// Resolve the durable shard index for `graph_fname` against a live shard count `k`
    /// (CONCEPT:EG-KG.sharding.empty-catalog-routing — the ROUTING SEAM). An explicit catalog entry wins (clamped into
    /// `0..k` so a stale/over-K entry can never index out of range); otherwise this is
    /// the unchanged EG-026 `FNV-1a(graph_fname) % k`. Empty catalog ⇒ pure EG-026.
    pub fn resolve_shard(&self, graph_fname: &str, k: usize) -> usize {
        let k = k.max(1);
        if let Some(a) = self.lookup(graph_fname) {
            return (a.shard as usize) % k;
        }
        super::redb_backend::shard_index(graph_fname, k)
    }

    /// Assign / re-place `graph_fname` onto `shard` (optionally on cluster `node`),
    /// writing through to the durable table when backed (CONCEPT:EG-KG.sharding.empty-catalog-routing). This flips
    /// the ROUTE only — moving an already-populated graph's rows to the new shard is
    /// online-resharding execution (remaining M3; see the design doc).
    pub fn assign(&self, graph_fname: &str, shard: u32, node: Option<u32>) -> Result<(), String> {
        let a = ShardAssignment { shard, node };
        self.entries
            .write()
            .unwrap()
            .insert(graph_fname.to_string(), a);
        if let Some(db) = &self.db {
            let blob = rmp_serde::to_vec_named(&a).map_err(|e| e.to_string())?;
            let wtx = db.begin_write().map_err(|e| e.to_string())?;
            {
                let mut t = wtx.open_table(CATALOG).map_err(|e| e.to_string())?;
                t.insert(graph_fname, blob.as_slice())
                    .map_err(|e| e.to_string())?;
            }
            wtx.commit().map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Reassign `graph_fname` to a different `shard` (CONCEPT:EG-KG.sharding.empty-catalog-routing — the resharding
    /// primitive). Preserves the existing `node` placement. Convenience over `assign`.
    pub fn reassign(&self, graph_fname: &str, new_shard: u32) -> Result<(), String> {
        let node = self.lookup(graph_fname).and_then(|a| a.node);
        self.assign(graph_fname, new_shard, node)
    }

    /// Drop the explicit assignment for `graph_fname` (it reverts to EG-026 routing).
    pub fn remove(&self, graph_fname: &str) -> Result<(), String> {
        self.entries.write().unwrap().remove(graph_fname);
        if let Some(db) = &self.db {
            let wtx = db.begin_write().map_err(|e| e.to_string())?;
            {
                let mut t = wtx.open_table(CATALOG).map_err(|e| e.to_string())?;
                t.remove(graph_fname).map_err(|e| e.to_string())?;
            }
            wtx.commit().map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Number of explicit (non-hash) placements.
    pub fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    /// `true` when no tenant has an explicit placement ⇒ routing is pure EG-026.
    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap().is_empty()
    }

    /// Snapshot of every explicit assignment `(graph_fname, assignment)` (diagnostics /
    /// rebalancing planners).
    pub fn entries(&self) -> Vec<(String, ShardAssignment)> {
        self.entries
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::persistence::redb_backend::shard_index;

    #[test]
    fn empty_catalog_is_pure_fnv1a() {
        // CONCEPT:EG-KG.sharding.empty-catalog-routing — an empty catalog must route IDENTICALLY to EG-026 for every
        // graph, so attaching it is a no-op until something is assigned.
        let cat = TenantCatalog::in_memory();
        assert!(cat.is_empty());
        for k in [1usize, 2, 4, 8, 16] {
            for g in ["__commons__", "agent:42", "tenant_a", "g-xyz", "ZZZ"] {
                assert_eq!(
                    cat.resolve_shard(g, k),
                    shard_index(g, k),
                    "graph {g} at K={k} must match FNV-1a when catalog empty"
                );
            }
        }
    }

    #[test]
    fn assign_overrides_then_falls_back() {
        let cat = TenantCatalog::in_memory();
        // Pick a graph whose natural shard is NOT 3 at K=4, then pin it to 3.
        let g = "hot-tenant";
        let natural = shard_index(g, 4);
        let pinned = (natural + 1) % 4;
        cat.assign(g, pinned as u32, None).unwrap();
        assert_eq!(cat.resolve_shard(g, 4), pinned, "explicit entry wins");
        assert_eq!(cat.lookup(g), Some(ShardAssignment::local(pinned as u32)));
        // A DIFFERENT graph with no entry still uses the hash.
        assert_eq!(cat.resolve_shard("cold", 4), shard_index("cold", 4));
        assert_eq!(cat.len(), 1);
    }

    #[test]
    fn reassign_moves_route_and_clamps() {
        let cat = TenantCatalog::in_memory();
        cat.assign("g", 1, Some(7)).unwrap();
        cat.reassign("g", 2).unwrap();
        assert_eq!(
            cat.lookup("g"),
            Some(ShardAssignment {
                shard: 2,
                node: Some(7)
            })
        );
        // An over-K shard clamps into range (defensive — never out-of-bounds).
        cat.assign("g", 99, None).unwrap();
        assert_eq!(cat.resolve_shard("g", 4), 99 % 4);
    }

    #[test]
    fn remove_reverts_to_hash() {
        let cat = TenantCatalog::in_memory();
        cat.assign("g", 3, None).unwrap();
        assert_eq!(cat.resolve_shard("g", 4), 3);
        cat.remove("g").unwrap();
        assert_eq!(cat.resolve_shard("g", 4), shard_index("g", 4));
        assert!(cat.is_empty());
    }

    #[test]
    fn durable_catalog_survives_reopen() {
        // CONCEPT:EG-KG.sharding.empty-catalog-routing — assignments persist to catalog.redb and reload on open.
        let dir = std::env::temp_dir().join(format!("eg-catalog-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        {
            let cat = TenantCatalog::open(&dir_s).expect("open catalog");
            cat.assign("alpha", 2, Some(1)).unwrap();
            cat.assign("beta", 5, None).unwrap();
        }
        let cat2 = TenantCatalog::open(&dir_s).expect("reopen catalog");
        assert_eq!(cat2.len(), 2);
        assert_eq!(
            cat2.lookup("alpha"),
            Some(ShardAssignment {
                shard: 2,
                node: Some(1)
            })
        );
        assert_eq!(cat2.lookup("beta"), Some(ShardAssignment::local(5)));
        // A fresh (different) dir is empty ⇒ pure EG-026.
        let _ = std::fs::remove_dir_all(&dir);
    }
}
