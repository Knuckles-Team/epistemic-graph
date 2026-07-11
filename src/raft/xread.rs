//! Cross-shard READ fan-out + merge (DIST-P2-2, CONCEPT:EG-KG.sharding.placement-catalog).
//!
//! ## The problem this closes
//!
//! [`super::cross_shard_txn`] gives a WRITE that spans graphs in different Raft
//! groups an atomic commit (2PC / Paxos-Commit-lite / Calvin), because a write must
//! land on exactly one group and the group is the transaction boundary
//! (CONCEPT:EG-KG.sharding.semantic-embedding-store-backed — the `multi` module's documented "group = transaction
//! boundary" invariant). A READ has no such constraint: the module docs are explicit
//! that "a READ can span groups by reading each group's snapshot" — there is nothing
//! to make atomic, because nothing is mutated. What was MISSING is the mechanical
//! fan-out: given a set of graphs that resolve to different groups (via the
//! [`super::placement::PlacementCatalog`], DIST-P2-1's placement authority, falling
//! back to the [`super::multi::GroupRouter`] hash ring exactly like a write's
//! routing), gather each group's current view of its graph and COMBINE them into one
//! answer.
//!
//! [`CrossShardReader`] is that fan-out. It is deliberately much simpler than
//! [`super::cross_shard_txn::CrossShardCoordinator`]: no OCC validation, no durable
//! prepare log, no atomic commit point, no recovery — a read has nothing to roll back
//! and nothing to make crash-safe (it observes state, it does not change it), so
//! reusing 2PC's write-side machinery here would add ceremony without buying any
//! correctness the read needs. What IS reused is the ROUTING: [`read_cross_shard`]
//! resolves every leg through [`super::multi::MultiRaft::route_graph`] — the SAME
//! placement-catalog-first, ring-fallback resolution `route_graph` gives writes — so a
//! reshard/split/move that repoints a graph's group is picked up by the NEXT read with
//! zero extra plumbing, exactly as it already is for writes.
//!
//! ## The merge
//!
//! Each leg's read is a full snapshot of its graph's current nodes (`GraphCore::get_nodes`)
//! — the "read each group's snapshot" the module docs describe. The legs are combined
//! with a UNION, deduplicated by node id (first-leg-wins on a collision — legs are
//! visited in the caller's own graph-name order, so the merge is deterministic run to
//! run). A union is the correct combinator for a query whose graphs are PARTITIONS of
//! one logical keyspace (e.g. a tenant [`super::placement::PlacementCatalog::split`]
//! spreads across two groups: reading "the whole tenant" is reading every partition and
//! UNIONING the rows, never intersecting them — an intersection is the cross-MODALITY
//! join [`eg_plan`]'s `RowSet::intersect_keep_order` already provides for a SINGLE
//! graph's multi-branch plan, an orthogonal concern to this module).
//!
//! ## Scope (honest — matches the posture already documented in this crate)
//!
//! Every leg must resolve to a group RUNNING ON THIS NODE. Like
//! [`super::cross_shard_txn::CrossShardCoordinator`]'s documented "participants are
//! local groups on the coordinator node today" and [`super::exchange`]'s
//! `ExchangeGraphResolver` seam, a genuinely cross-NODE leg (a group that lives on a
//! DIFFERENT physical node) is not fetched over the network by this module — that
//! transport already exists ([`super::exchange::call_remote_branch`], the X4 DAG
//! exchange operator) and is the natural place to plug in a remote leg later; wiring it
//! in is a documented follow-up, not a gap this increment silently papers over (a local
//! leg whose group is not yet running here is a loud `Err`, never a partial/empty
//! answer presented as complete).

use std::collections::HashSet;
use std::sync::Arc;

use super::multi::MultiRaft;
use super::GroupId;

/// One graph's contribution to a cross-shard read: which group/epoch
/// [`super::multi::MultiRaft::route_graph`] resolved it to, plus every node currently
/// in its snapshot (`(node_id, properties_msgpack)`, in the graph's own iteration
/// order).
#[derive(Debug, Clone)]
pub struct ReadLeg {
    pub graph_name: String,
    pub group: GroupId,
    pub epoch: u64,
    pub nodes: Vec<(String, Vec<u8>)>,
}

/// The outcome of [`CrossShardReader::read_cross_shard`]: the per-leg snapshots (for
/// callers that care which group answered what — observability, epoch-checking a
/// fenced move) plus the UNION-merged rows.
#[derive(Debug, Clone, Default)]
pub struct CrossShardReadResult {
    pub legs: Vec<ReadLeg>,
    /// The union of every leg's nodes, deduplicated by id (first occurrence wins, legs
    /// visited in the caller's own graph-name order — deterministic).
    pub merged: Vec<(String, Vec<u8>)>,
}

impl CrossShardReadResult {
    /// The distinct groups this read actually spanned. `len() >= 2` is the definition
    /// of "this was a genuine cross-shard read" (mirrors
    /// [`super::multi::GroupRouter::is_cross_shard`] on the write side).
    pub fn groups_spanned(&self) -> std::collections::BTreeSet<GroupId> {
        self.legs.iter().map(|l| l.group).collect()
    }

    /// `true` when the legs resolved to 2 or more DISTINCT groups — i.e. this read
    /// genuinely fanned out across shards rather than degenerating to a single-group
    /// (or single-graph) read.
    pub fn is_cross_shard(&self) -> bool {
        self.groups_spanned().len() >= 2
    }
}

/// The cross-shard READ coordinator (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-2). Holds
/// only the [`MultiRaft`] manager — unlike the write-side 2PC coordinator it needs no
/// durable backend, because a read persists nothing.
pub struct CrossShardReader {
    multi: Arc<MultiRaft>,
}

impl CrossShardReader {
    pub fn new(multi: Arc<MultiRaft>) -> Self {
        Self { multi }
    }

    /// Gather + union-merge `graph_names` (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-2 — the
    /// cross-shard query this increment adds). For each graph:
    ///
    /// 1. resolve its `(GroupId, epoch)` via [`MultiRaft::route_graph`] — the
    ///    catalog-first, ring-fallback resolution writes already use, so a leg is
    ///    ALWAYS routed exactly where a write to that graph would land;
    /// 2. confirm that group is running on this node (`Err` otherwise — see the module
    ///    docs' "Scope" section: a cross-node leg is a documented follow-up, not
    ///    silently dropped);
    /// 3. read the graph's CURRENT snapshot directly from the shared registry (every
    ///    group's `EgStore` applies into the SAME `ServerState`/registry — see
    ///    `reshard`'s module docs — so no network hop is needed for a same-node group,
    ///    and there is nothing to lock: this is a plain, un-isolated point-in-time read
    ///    of each graph's live state, exactly the "read each group's snapshot"
    ///    the `multi` module's group-boundary docs describe).
    ///
    /// A graph absent from the registry contributes an empty leg (not an error) — the
    /// same "not found yet" tolerance a fresh/empty graph gets elsewhere in this crate.
    pub async fn read_cross_shard(
        &self,
        graph_names: &[String],
    ) -> Result<CrossShardReadResult, String> {
        let mut legs = Vec::with_capacity(graph_names.len());
        for name in graph_names {
            let (gid, epoch) = self.multi.route_graph(name).await;
            if self.multi.group(gid).await.is_none() {
                return Err(format!(
                    "cross-shard read: graph '{name}' routes to group {gid}, which is not \
                     running on this node — a cross-node leg is a documented follow-up \
                     (see raft::exchange for the transport it would reuse)"
                ));
            }
            let nodes = {
                let state = self.multi.app_state();
                let s = state.read().await;
                match s.registry.get(name) {
                    Some(entry) => entry.core.get_nodes(),
                    None => Vec::new(),
                }
            };
            legs.push(ReadLeg {
                graph_name: name.clone(),
                group: gid,
                epoch,
                nodes,
            });
        }
        let merged = union_merge(&legs);
        Ok(CrossShardReadResult { legs, merged })
    }
}

/// Union every leg's nodes, deduplicated by id — first occurrence wins, legs visited
/// in their given order, so the merge is deterministic run to run.
fn union_merge(legs: &[ReadLeg]) -> Vec<(String, Vec<u8>)> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut out = Vec::new();
    for leg in legs {
        for (id, props) in &leg.nodes {
            if seen.insert(id.as_str()) {
                out.push((id.clone(), props.clone()));
            }
        }
    }
    out
}
