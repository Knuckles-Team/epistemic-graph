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
//! Raft's own metadata (vote, last-applied log id, membership, snapshot) is kept
//! durable in a SEPARATE `raft.redb` ([`store::RaftMeta`]) so this module never has
//! to reach into the M2 backend's file (redb is single-handle-per-process).
//!
//! ## Scope of this increment (documented follow-ups)
//!
//! * **Single Raft group.** One replicated keyspace covers every graph on the node
//!   (the app-data carries the target graph name). Per-graph / per-shard Raft
//!   groups are a documented follow-up — the app-data shape already carries the
//!   graph name, so splitting groups later is a routing change, not a data change.
//! * **In-memory Raft log.** The log entries live in RAM ([`store::LogStore`]); the
//!   vote + applied state + membership ARE durable in `raft.redb`, and the GRAPH
//!   DATA is durable via M2. So a node that restarts recovers its applied data and
//!   its vote; it re-replicates any un-snapshotted log tail from the leader (Raft's
//!   normal catch-up). Moving the log itself into `raft.redb` (so a node never
//!   needs the leader to refill an un-snapshotted tail) is the documented
//!   durability follow-up; in-memory is acceptable for a first increment because
//!   committed state is never lost (it is on a quorum + durable via M2).
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
pub mod network;
pub mod node;
pub mod store;

#[cfg(test)]
mod tests;

/// Raft node id — a small integer assigned per cluster member.
pub type NodeId = u64;

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
}
