//! In-engine Raft replication (CONCEPT:KG-2.188) — the `cluster` tier.
//!
//! Runs the engine as a multi-node, highly-available cluster that replicates its
//! AUTHORITATIVE state through [`openraft`]. This whole module is behind the
//! `raft` cargo feature (cluster-tier only): a default / `pi` / `full` build links
//! NO openraft, so the Raspberry-Pi contract (no DataFusion AND no openraft) holds.
//!
//! ## What is replicated, and how it stays durable
//!
//! The replicated app data is a **durable [`Method`] for a graph** — exactly the
//! `is_durable_mutation` set the WAL logs. A committed Raft log entry is applied to
//! the local engine by the SAME path a replayed WAL record uses
//! ([`crate::wal::apply`] → [`GraphCore`]), then made durable through the SAME
//! [`PersistenceBackend::record_durable`] M2 authoritative path
//! (CONCEPT:KG-2.187). So a Raft node IS an M2 authoritative node — its graph data
//! lives in `graph.redb`, committed-before-applied.
//!
//! ## Durable redb Raft log (CONCEPT:KG-2.204)
//!
//! The Raft LOG, the vote, the applied state, and the graph data are ALL durable in
//! the SAME `graph.redb` Database (the M2 store), keyed by `(group_id, index)` /
//! `(group_id, key)`. The log shares M2's off-reactor group-commit writer, so a log
//! append and its graph mutation ride ONE `WriteTransaction` / one fsync. A restarted
//! node recovers its log tail LOCALLY from redb — it no longer needs the leader to
//! refill an un-snapshotted tail. (The old separate `raft.redb` sidecar is gone.)
//!
//! ## Multi-Raft scaffold (CONCEPT:KG-2.205)
//!
//! [`multi::MultiRaft`] holds N openraft groups keyed by [`GroupId`], each its own
//! state machine + `GraphCore`, sharing ONE TCP listener per node (RPC frames are
//! tagged and demuxed by group id) and ONE shared `graph.redb` (composite-key
//! log/meta — NOT a file per group). [`multi::GroupRouter`] maps `graph_name →
//! GroupId`.
//!
//! ## Scope of this increment (documented follow-ups)
//!
//! * **One group by default.** With no ring configured every graph routes to
//!   [`DEFAULT_GROUP`] so behavior matches the single-group path; the manager +
//!   routing + group lifecycle + multi-group isolation are exercised by tests.
//!   [`multi::GroupRouter::set_group_ring`] / [`multi::MultiRaft::configure_group_ring`]
//!   now SPREAD un-pinned graphs across N groups on a stable hash ring
//!   (CONCEPT:KG-2.266) — splitting a keyspace is a routing change, not a storage
//!   change.
//! * **Group = transaction boundary.** One graph belongs to one group; a txn stays
//!   inside a group. Cross-group (2-phase) transactions are a SEPARATE, larger
//!   project — NOT in this increment (documented follow-up CONCEPT:KG-2.207).
//! * **M2 hardening (done):** pooled per-peer connections (CONCEPT:KG-2.265) and
//!   per-group snapshot scoping (CONCEPT:KG-2.267) — see `docs/architecture/m2-raft-status.md`.
//! * **M2 follow-ups (remaining):** leader balancing across groups and heartbeat
//!   coalescing — both need a real multi-node cluster to exercise; scoped as
//!   independent pick-up tasks in `docs/architecture/m2-raft-status.md`.
//!
//! ## Write-routing barrier
//!
//! When Raft is active (built `--features raft` AND configured), a durable write is
//! routed through [`RaftHandle::client_write`] on the leader BEFORE it is
//! applied+acked — consensus is the replication barrier. Followers redirect the
//! client to the leader. When Raft is NOT active the dispatch path is byte-for-byte
//! unchanged (the `Option<RaftHandle>` is `None` and the normal apply path runs).

#![cfg(feature = "raft")]

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::protocol::Method;
use crate::server::ServerState;

pub mod config;
pub mod cross_shard_txn;
pub mod multi;
pub mod network;
pub mod node;
/// Distributed graph compute — the Pregel/GAS cross-shard superstep engine
/// (CONCEPT:KG-2.227). Behind `compute-dist` (which implies `raft`): runs PageRank /
/// connected-components / BFS across graphs spanning multiple Raft groups, plus the
/// incremental/streaming variant and materialized views.
#[cfg(feature = "compute-dist")]
pub mod pregel;
pub mod reshard;
pub mod store;

/// Correctness + load harness (CONCEPT:KG-2.212) — the standing proof-engine that
/// gates every distributed/durability claim. Compiled under tests OR the explicit
/// `harness` feature; never in a production tier build.
#[cfg(any(test, feature = "harness"))]
pub mod harness;

#[cfg(test)]
mod tests;

// The cross-shard 2PC atomicity + recovery gauntlet (CONCEPT:KG-2.222) — the nemesis
// harness proving NO PARTIAL COMMIT under participant-kill + partition. Gated behind
// the `harness` feature so the default `raft` test set is unchanged; run it with
// `cargo test --features "raft harness"`.
#[cfg(all(test, feature = "harness"))]
mod xshard_harness;

// The online-resharding + tenant-hibernation gauntlet (CONCEPT:KG-2.224) — proves a
// graph reshareded A→B keeps all data + serves correctly, and a hibernated graph
// rehydrates intact. Gated behind `harness` so a normal `raft` build links nothing.
#[cfg(all(test, feature = "harness"))]
mod reshard_harness;

/// Raft node id — a small integer assigned per cluster member.
pub type NodeId = u64;

/// A Raft GROUP id (CONCEPT:KG-2.205). One consensus group per keyspace; today one
/// graph maps to one group via the [`multi::GroupRouter`]. It is the composite-key
/// prefix the durable redb log + meta rows are keyed by, so ONE `graph.redb` serves
/// every group's log (the spike's FD-ceiling fix — no file per group).
pub type GroupId = u64;

/// The single default group every graph routes to in this increment (one group =
/// today's single-group behavior, now with a durable redb log). Multi-group routing
/// machinery exists ([`multi`]) and is proven by tests, but the default router maps
/// all graphs here so behavior is unchanged from the single-group path.
pub const DEFAULT_GROUP: GroupId = 0;

/// The application request replicated through Raft: a durable [`Method`] targeted
/// at a named graph. This is the log-entry payload — applying it = applying the
/// Method to the target graph's [`crate::graph::GraphCore`] + the M2 durable path.
///
/// `Method` is not `Eq` (it embeds `serde_json::Value`), so this is not `Eq` either
/// — openraft does not require app data to be comparable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftRequest {
    /// The SANITIZED graph file-name (the same key the persistence tier uses).
    pub graph_fname: String,
    /// Human-readable graph name (used to create the graph in the registry if a
    /// follower has never seen it). For `__commons__` both are the same.
    pub graph_name: String,
    /// The graph's type, used only when the follower must create the graph.
    pub graph_type: crate::protocol::GraphType,
    /// The durable mutation to apply (one of the `is_durable_mutation` set).
    pub method: Method,
}

/// The application response from applying a [`RaftRequest`]. The dispatch path only
/// needs success/failure (the in-memory apply already produced the client-facing
/// Response), so this is a thin ack.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftResponse {
    /// `true` when the committed Method applied cleanly on this node.
    pub applied: bool,
}

openraft::declare_raft_types!(
    /// The single Raft type configuration for the engine cluster.
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
        NodeId = NodeId,
        Node = BasicNode,
);

/// A running Raft instance (`openraft::Raft`) for our [`TypeConfig`].
pub type EgRaft = openraft::Raft<TypeConfig>;

/// Cloneable handle the dispatch path uses to route writes through consensus.
///
/// Held in `ServerState` as `Option<RaftHandle>`: `None` ⇒ single-node (the normal
/// path, unchanged); `Some` ⇒ the cluster path routes writes through Raft.
#[derive(Clone)]
pub struct RaftHandle {
    pub raft: EgRaft,
    pub node_id: NodeId,
}

impl RaftHandle {
    /// Route a durable mutation through Raft consensus. On the LEADER this awaits
    /// a quorum-committed + locally-applied write (the replication barrier). On a
    /// FOLLOWER, openraft returns a `ForwardToLeader` error carrying the current
    /// leader id, which the caller surfaces so the client retries against the
    /// leader. Returns `Ok` only after the entry is committed AND applied here.
    pub async fn client_write(&self, req: RaftRequest) -> Result<RaftResponse, String> {
        match self.raft.client_write(req).await {
            Ok(resp) => Ok(resp.data),
            Err(e) => Err(format!("raft client_write: {e}")),
        }
    }

    /// The current cluster leader as this node sees it (for redirect hints).
    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }
}

/// Parsed peer set: node id → MessagePack-RPC address (`host:port`).
pub type PeerMap = BTreeMap<NodeId, BasicNode>;

/// Shared application context the state machine needs to APPLY a committed entry:
/// the live `ServerState` (registry + persistence). Cloned into the store.
#[derive(Clone)]
pub struct AppCtx {
    pub state: Arc<RwLock<ServerState>>,
    /// The group router (CONCEPT:KG-2.266), present when the store runs under a
    /// [`multi::MultiRaft`]. A group's snapshot dump uses it to SCOPE the dump to the
    /// graphs in THIS group's tenant range (CONCEPT:KG-2.267). `None` ⇒ a direct /
    /// single-store open dumps the whole registry (the unscoped scaffold behavior).
    pub router: Option<Arc<multi::GroupRouter>>,
}
