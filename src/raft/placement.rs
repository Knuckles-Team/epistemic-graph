//! Placement catalog — the ONE placement authority for virtual partitions
//! (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-1).
//!
//! ## Why this exists
//!
//! [`super::multi::GroupRouter`] gives every un-pinned graph a STATELESS hash-ring
//! route (`graph_name → GroupId`), and its per-graph `assign` override plus
//! [`super::reshard::TenantManager`] give ONE graph an online move. That is not yet a
//! placement AUTHORITY: there is no durable, replicated, versioned record of "this
//! tenant's keyspace lives here" that spans multiple graphs, supports splitting a
//! hot tenant across groups, or lets a caller detect it is talking to a group that no
//! longer owns the data.
//!
//! [`PlacementCatalog`] is that authority. A **virtual partition** is a tenant plus an
//! (optional) sub-range of its workspace/session/entity keyspace
//! ([`PartitionKey`]) — the SAME tenant can have ONE whole-keyspace partition (the
//! common case: one tenant, one group) or be [`split`](MultiRaft::placement_split)
//! into several ranged partitions on DIFFERENT groups (one tenant spans shards); many
//! small tenants can independently point their whole-keyspace partition at the SAME
//! group (small tenants share). Every placement change bumps a single, cluster-wide,
//! monotonic **routing epoch** ([`next_epoch`](PlacementCatalog::next_epoch)), so a
//! caller holding a stale `(group, epoch)` pair can be told to redirect
//! ([`PlacementCatalog::redirect_if_stale`]) instead of being served against the
//! wrong shard.
//!
//! ## Persisted AND Raft-replicated — reusing the engine's own graph machinery
//!
//! Rather than inventing a second replicated store, the catalog IS a graph:
//! [`PLACEMENT_GRAPH`] (`__placement_catalog__`) is an ordinary control graph whose
//! nodes are `msgpack([`PlacementEntry`])` blobs written through
//! [`Method::AddNode`]/[`Method::RemoveNode`] — the SAME durable mutation type every
//! tenant graph uses. [`MultiRaft`](super::multi::MultiRaft)'s placement admin methods
//! commit these through the DEFAULT group's `client_write`, so:
//!
//! * **Durable** — the mutation lands in `graph.redb` via the same
//!   `record_durable`/WAL path as any other graph (CONCEPT:EG-KG.storage.one-fsync-covers-raft), so a restart
//!   reloads it with `load_all` like any graph.
//! * **Raft-replicated** — when Raft is active, the SAME committed log entry applies
//!   on every node via [`super::store::EgStore::apply_request`] (unmodified — no new
//!   `RaftRequest`/`AppCtx` plumbing needed), so every node's local
//!   `__placement_catalog__` graph converges identically.
//!
//! [`PlacementCatalog`] itself holds no separate cache: [`PlacementCatalog::route`]
//! reads the control graph fresh (cheap — the catalog holds far fewer rows than user
//! data), so a follower that just applied a replicated placement change sees it on the
//! very next `route` call with no extra invalidation plumbing.
//!
//! ## Online move — snapshot → CDC catch-up → fenced cutover
//!
//! [`super::reshard::TenantManager::move_partition`] is the online-move state machine,
//! reusing [`super::reshard::TenantManager::reshard_graph`] (already proven: quiesce →
//! durability-barrier snapshot → re-point → resume) as the per-graph data-move
//! primitive:
//!
//! 1. **Start move** ([`MultiRaft::placement_start_move`]) marks the partition
//!    [`PartitionState::Moving`] — [`route`](PlacementCatalog::route) still returns the
//!    SOURCE group/epoch (no client is redirected yet); this is the window the source
//!    keeps serving while the target "catches up".
//! 2. **Snapshot + catch-up** — for every graph the partition covers,
//!    `reshard_graph` durably checkpoints it and re-points that graph's OWN
//!    [`GroupRouter`](super::multi::GroupRouter) entry to the target group. Because
//!    every group shares ONE `graph.redb`/registry, this durability barrier IS the
//!    data transfer (no bulk copy needed) — the same invariant `reshard_graph`
//!    documents.
//! 3. **Fenced cutover** ([`MultiRaft::placement_fence_cutover`]) bumps the epoch and
//!    flips the partition's authoritative group to the target, atomically (one
//!    control-graph write). From this instant, [`PlacementCatalog::redirect_if_stale`]
//!    answers ANY caller still presenting the pre-cutover epoch with a redirect to
//!    `(target, new_epoch)` rather than serving it — the fence.
//!
//! A crash between steps preserves data: step 2's `reshard_graph` barrier is durable
//! before the router re-point, and step 3 only ever *adds* a monotonically newer
//! catalog entry — there is no window where a committed write is lost or served
//! against a partition that no longer owns it.
//!
//! ## What this increment does NOT do (documented follow-ups)
//!
//! * **Concurrent admin ops on a live multi-writer cluster.** `next_epoch` reads the
//!   catalog then writes the incremented value under a LOCAL (this-node) lock; two
//!   placement admin calls issued concurrently on the SAME node serialize correctly,
//!   but Raft's single-leader-writer property is what actually prevents two DIFFERENT
//!   nodes computing the same next epoch — fine for the low-frequency admin path this
//!   targets, but a documented limitation vs. a CAS-style engine-level guard.
//! * **AU-side consumption.** This is the engine's authority; the client-side HRW
//!   ring becoming a *consumer* of `route`/`redirect_if_stale` (so `agent-utilities`
//!   stops guessing placement independently) is the separate Wave-2 workstream.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard, RwLock};

use super::GroupId;
use crate::protocol::Method;
use crate::server::ServerState;

/// The control graph every placement entry is durably stored in as one node per
/// virtual partition (CONCEPT:EG-KG.sharding.placement-catalog). An ordinary graph — created
/// on first write via the SAME registry auto-create every Raft-applied graph
/// mutation uses — so it rides the engine's existing persistence + replication with
/// no new storage code.
pub const PLACEMENT_GRAPH: &str = "__placement_catalog__";

/// A virtual partition key (CONCEPT:EG-KG.sharding.placement-catalog): a tenant plus an inclusive
/// `[range_start, range_end]` sub-range over a stable hash of the tenant's
/// workspace/session/entity id space. `range_start == 0 && range_end == u64::MAX` is
/// the common "whole tenant, one group" partition; splitting a tenant produces two (or
/// more) narrower, non-overlapping ranges so one tenant can span multiple groups.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionKey {
    pub tenant: String,
    pub range_start: u64,
    pub range_end: u64,
}

impl PartitionKey {
    /// The whole-keyspace partition for `tenant` — every workspace/session/entity id
    /// of this tenant hashes into it.
    pub fn whole(tenant: &str) -> Self {
        PartitionKey {
            tenant: tenant.to_string(),
            range_start: 0,
            range_end: u64::MAX,
        }
    }

    /// `true` when the stable hash `h` of a workspace/session/entity id falls inside
    /// this partition's range.
    fn contains(&self, h: u64) -> bool {
        h >= self.range_start && h <= self.range_end
    }

    /// The durable control-graph node id for this key. Unique per (tenant, range) —
    /// the actual key/state live in the node's `properties_msgpack` ([`PlacementEntry`]),
    /// so this id never needs to be parsed back.
    fn node_id(&self) -> String {
        format!(
            "{}\u{1}{:020}\u{1}{:020}",
            self.tenant, self.range_start, self.range_end
        )
    }
}

/// A partition's lifecycle state (CONCEPT:EG-KG.sharding.placement-catalog — the online-move state
/// machine). `Active` is steady-state; `Moving` is the snapshot/CDC-catch-up window
/// BEFORE the fenced cutover — [`PlacementCatalog::route`] still resolves to the
/// CURRENT (source) group while `Moving`, only the cutover flips it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionState {
    Active,
    Moving { target: GroupId },
}

/// One durable placement record (CONCEPT:EG-KG.sharding.placement-catalog): the control-graph node
/// body for a [`PartitionKey`]. `epoch` is this entry's routing epoch — the value of
/// the cluster-wide monotonic counter ([`PlacementCatalog::next_epoch`]) at the moment
/// this entry's AUTHORITATIVE group was last set (assign/split/merge/fenced cutover;
/// `start_move` does NOT bump it — the group hasn't changed yet).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementEntry {
    pub key: PartitionKey,
    pub group: GroupId,
    pub epoch: u64,
    pub state: PartitionState,
}

/// The routing answer for an explicit placement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacementRoute {
    pub group: GroupId,
    pub epoch: u64,
}

/// The outcome of [`PlacementCatalog::route`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteOutcome {
    /// The catalog has an explicit placement for this tenant/key.
    Explicit(PlacementRoute),
    /// No explicit placement — the caller must fall back to the hash ring/
    /// [`super::multi::GroupRouter`] default (CONCEPT:EG-KG.sharding.placement-catalog: additive, no-regression
    /// default).
    Fallback,
}

/// Split `graph_name` into `(tenant, sub_key)` (CONCEPT:EG-KG.sharding.placement-catalog): the substring
/// before the FIRST `:` is the tenant, the rest is the workspace/session/entity
/// sub-key that hashes into a tenant's partition range. A name with no `:` is its own
/// tenant AND sub-key (so a whole-tenant placement degenerates to a per-graph pin,
/// consistent with [`super::multi::GroupRouter::assign`]'s per-graph override).
pub fn split_tenant_key(graph_name: &str) -> (&str, &str) {
    match graph_name.split_once(':') {
        Some((tenant, rest)) if !tenant.is_empty() => (tenant, rest),
        _ => (graph_name, graph_name),
    }
}

/// A batch of durable control-graph writes plus the local write-serialization guard
/// (CONCEPT:EG-KG.sharding.placement-catalog). Held by the caller across the ACTUAL Raft commit (see
/// [`super::multi::MultiRaft::commit_placement`]) so a second placement admin call on
/// THIS node cannot compute the same "next epoch" concurrently. Dropping it (after the
/// commit resolves) releases the lock.
pub struct PendingWrite<'a> {
    _guard: MutexGuard<'a, ()>,
    /// The `Method::AddNode`/`Method::RemoveNode` mutations to commit, in order, to
    /// [`PLACEMENT_GRAPH`].
    pub methods: Vec<Method>,
    /// The epoch this plan settles on (unchanged from the pre-op max for a
    /// `start_move`, which does not bump it).
    pub epoch: u64,
}

/// The ONE placement authority (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-1). Reads/plans are
/// pure queries over [`PLACEMENT_GRAPH`]'s current nodes; committing a plan's
/// `methods` through Raft is the caller's job (see
/// [`super::multi::MultiRaft`]'s `placement_*` methods) — this keeps the catalog's
/// logic testable without a running cluster.
pub struct PlacementCatalog {
    state: Arc<RwLock<ServerState>>,
    write_lock: Mutex<()>,
}

impl PlacementCatalog {
    pub fn new(state: Arc<RwLock<ServerState>>) -> Self {
        PlacementCatalog {
            state,
            write_lock: Mutex::new(()),
        }
    }

    /// Every durably-recorded placement entry, decoded from [`PLACEMENT_GRAPH`]'s
    /// current nodes. Empty (not an error) when the graph doesn't exist yet — an
    /// untouched catalog.
    pub async fn all_entries(&self) -> Vec<PlacementEntry> {
        let core = {
            let s = self.state.read().await;
            s.registry.get(PLACEMENT_GRAPH).map(|e| e.core.clone())
        };
        match core {
            Some(core) => core
                .get_nodes()
                .iter()
                .filter_map(|(_, blob)| rmp_serde::from_slice::<PlacementEntry>(blob).ok())
                .collect(),
            None => Vec::new(),
        }
    }

    /// This tenant's placement entries (0, 1 whole-keyspace, or several ranged after a
    /// split).
    pub async fn tenant_entries(&self, tenant: &str) -> Vec<PlacementEntry> {
        self.all_entries()
            .await
            .into_iter()
            .filter(|e| e.key.tenant == tenant)
            .collect()
    }

    /// Resolve `(tenant, sub_key)` (CONCEPT:EG-KG.sharding.placement-catalog — the routing seam). Hashes
    /// `sub_key` with the SAME stable FNV-1a [`super::multi::GroupRouter`] uses and
    /// finds the tenant partition whose range contains it. `Moving` partitions still
    /// resolve to their CURRENT (pre-cutover) group — only a fenced cutover changes
    /// the answer.
    pub async fn route(&self, tenant: &str, sub_key: &str) -> RouteOutcome {
        let h = super::multi::fnv1a(sub_key);
        for e in self.tenant_entries(tenant).await {
            if e.key.contains(h) {
                return RouteOutcome::Explicit(PlacementRoute {
                    group: e.group,
                    epoch: e.epoch,
                });
            }
        }
        RouteOutcome::Fallback
    }

    /// The fenced-cutover redirect check (CONCEPT:EG-KG.sharding.placement-catalog): `None` when the
    /// caller's `client_epoch` is current (or there is no explicit placement — nothing
    /// to be stale against); `Some(route)` with the NEW group+epoch when the caller is
    /// behind, so it can be redirected instead of served against a partition that has
    /// already moved.
    pub async fn redirect_if_stale(
        &self,
        tenant: &str,
        sub_key: &str,
        client_epoch: u64,
    ) -> Option<PlacementRoute> {
        if let RouteOutcome::Explicit(r) = self.route(tenant, sub_key).await {
            if client_epoch < r.epoch {
                return Some(r);
            }
        }
        None
    }

    fn entry_method(entry: &PlacementEntry) -> Method {
        Method::AddNode {
            node_id: entry.key.node_id(),
            properties_msgpack: rmp_serde::to_vec_named(entry)
                .expect("PlacementEntry always encodes"),
        }
    }

    fn remove_method(key: &PartitionKey) -> Method {
        Method::RemoveNode {
            node_id: key.node_id(),
        }
    }

    /// Plan a whole-tenant assignment to `group` (CONCEPT:EG-KG.sharding.placement-catalog). Collapses any
    /// prior split/ranged entries for this tenant back to one whole-keyspace entry —
    /// also used by `merge` (assigning to the merge target is the same operation).
    /// Bumps the cluster-wide epoch (the authoritative group changed).
    pub async fn plan_assign(&self, tenant: &str, group: GroupId) -> PendingWrite<'_> {
        let guard = self.write_lock.lock().await;
        let all = self.all_entries().await;
        let epoch = all.iter().map(|e| e.epoch).max().unwrap_or(0) + 1;
        let mut methods: Vec<Method> = all
            .iter()
            .filter(|e| e.key.tenant == tenant)
            .map(|e| Self::remove_method(&e.key))
            .collect();
        let entry = PlacementEntry {
            key: PartitionKey::whole(tenant),
            group,
            epoch,
            state: PartitionState::Active,
        };
        methods.push(Self::entry_method(&entry));
        PendingWrite {
            _guard: guard,
            methods,
            epoch,
        }
    }

    /// Merge every one of `tenant`'s ranged partitions back onto a single group
    /// (CONCEPT:EG-KG.sharding.placement-catalog) — the inverse of `split`. Same operation as `assign`.
    pub async fn plan_merge(&self, tenant: &str, group: GroupId) -> PendingWrite<'_> {
        self.plan_assign(tenant, group).await
    }

    /// Plan splitting `tenant`'s partition covering `at` into `[start, at-1] → group_a`
    /// and `[at, end] → group_b` (CONCEPT:EG-KG.sharding.placement-catalog — lets one tenant span two
    /// groups). Splits the IMPLICIT whole range `[0, u64::MAX]` when the tenant has no
    /// explicit placement yet. Bumps the cluster-wide epoch once for the pair.
    pub async fn plan_split(
        &self,
        tenant: &str,
        at: u64,
        group_a: GroupId,
        group_b: GroupId,
    ) -> Result<PendingWrite<'_>, String> {
        let guard = self.write_lock.lock().await;
        let all = self.all_entries().await;
        let epoch = all.iter().map(|e| e.epoch).max().unwrap_or(0) + 1;
        let tenant_entries: Vec<&PlacementEntry> =
            all.iter().filter(|e| e.key.tenant == tenant).collect();
        let covering = tenant_entries
            .iter()
            .find(|e| e.key.contains(at) && at > e.key.range_start)
            .copied();
        let (old_start, old_end) = match covering {
            Some(e) => (e.key.range_start, e.key.range_end),
            None if tenant_entries.is_empty() => (0u64, u64::MAX),
            None => {
                return Err(format!(
                    "split point {at} is not the interior of any of tenant '{tenant}''s existing partitions"
                ))
            }
        };
        if !(at > old_start && at <= old_end) {
            return Err(format!(
                "split point {at} is not inside range [{old_start}, {old_end}] for tenant '{tenant}'"
            ));
        }
        let mut methods = Vec::new();
        if let Some(e) = covering {
            methods.push(Self::remove_method(&e.key));
        }
        let entry_a = PlacementEntry {
            key: PartitionKey {
                tenant: tenant.to_string(),
                range_start: old_start,
                range_end: at - 1,
            },
            group: group_a,
            epoch,
            state: PartitionState::Active,
        };
        let entry_b = PlacementEntry {
            key: PartitionKey {
                tenant: tenant.to_string(),
                range_start: at,
                range_end: old_end,
            },
            group: group_b,
            epoch,
            state: PartitionState::Active,
        };
        methods.push(Self::entry_method(&entry_a));
        methods.push(Self::entry_method(&entry_b));
        Ok(PendingWrite {
            _guard: guard,
            methods,
            epoch,
        })
    }

    /// Plan marking the partition `(tenant, range)` [`PartitionState::Moving`] to
    /// `target` (CONCEPT:EG-KG.sharding.placement-catalog — online-move step 1). Does NOT bump the epoch:
    /// the authoritative group is unchanged until [`plan_fence_cutover`](Self::plan_fence_cutover)
    /// — `route` keeps answering with the SOURCE group while the target catches up.
    pub async fn plan_start_move(
        &self,
        tenant: &str,
        range: (u64, u64),
        target: GroupId,
    ) -> Result<PendingWrite<'_>, String> {
        let guard = self.write_lock.lock().await;
        let entries = self.tenant_entries(tenant).await;
        let entry = entries
            .into_iter()
            .find(|e| e.key.range_start == range.0 && e.key.range_end == range.1)
            .ok_or_else(|| {
                format!("no placement entry for tenant '{tenant}' range {range:?} — assign/split it first")
            })?;
        let moving = PlacementEntry {
            state: PartitionState::Moving { target },
            ..entry
        };
        let epoch = moving.epoch;
        Ok(PendingWrite {
            _guard: guard,
            methods: vec![Self::entry_method(&moving)],
            epoch,
        })
    }

    /// Plan the fenced cutover of `(tenant, range)` to `target` (CONCEPT:EG-KG.sharding.placement-catalog —
    /// online-move step 3): bumps the cluster-wide epoch AND flips the partition's
    /// authoritative group, atomically (one control-graph write). After this commits,
    /// [`redirect_if_stale`](Self::redirect_if_stale) answers any caller still on the
    /// pre-cutover epoch with a redirect to `(target, new_epoch)`.
    pub async fn plan_fence_cutover(
        &self,
        tenant: &str,
        range: (u64, u64),
        target: GroupId,
    ) -> Result<PendingWrite<'_>, String> {
        let guard = self.write_lock.lock().await;
        let all = self.all_entries().await;
        let epoch = all.iter().map(|e| e.epoch).max().unwrap_or(0) + 1;
        let entry = all
            .into_iter()
            .find(|e| {
                e.key.tenant == tenant && e.key.range_start == range.0 && e.key.range_end == range.1
            })
            .ok_or_else(|| {
                format!("no placement entry for tenant '{tenant}' range {range:?} to cut over")
            })?;
        let cut = PlacementEntry {
            key: entry.key,
            group: target,
            epoch,
            state: PartitionState::Active,
        };
        Ok(PendingWrite {
            _guard: guard,
            methods: vec![Self::entry_method(&cut)],
            epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_key_whole_contains_everything() {
        let k = PartitionKey::whole("acme");
        assert!(k.contains(0));
        assert!(k.contains(u64::MAX));
        assert!(k.contains(123456789));
    }

    #[test]
    fn split_tenant_key_parses_prefix() {
        assert_eq!(split_tenant_key("acme:ws1"), ("acme", "ws1"));
        assert_eq!(split_tenant_key("acme:ws:nested"), ("acme", "ws:nested"));
        assert_eq!(split_tenant_key("solo"), ("solo", "solo"));
        assert_eq!(split_tenant_key(":leading"), (":leading", ":leading"));
    }

    #[test]
    fn node_id_is_stable_and_unique_per_range() {
        let a = PartitionKey {
            tenant: "acme".into(),
            range_start: 0,
            range_end: 10,
        };
        let b = PartitionKey {
            tenant: "acme".into(),
            range_start: 11,
            range_end: 20,
        };
        assert_ne!(a.node_id(), b.node_id());
        assert_eq!(a.node_id(), a.node_id());
    }
}
