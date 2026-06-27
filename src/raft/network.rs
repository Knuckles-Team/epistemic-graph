//! Raft network (CONCEPT:KG-2.188 + KG-2.205) — a group-multiplexed Raft TCP channel.
//!
//! A small purpose-built TCP channel rather than reusing the engine's auth'd
//! MessagePack RPC, because the Raft RPC payloads are openraft's own request/
//! response types (append-entries / vote / install-snapshot) and routing them
//! through the engine's `Method` enum would mean embedding consensus types into the
//! client-facing protocol — a layering violation. The framing convention is
//! IDENTICAL to the engine transport (4-byte big-endian length prefix + a
//! MessagePack body), so binary payloads survive intact.
//!
//! Every RPC frame is TAGGED with its [`GroupId`] ([`GroupRpc`]) so ONE listener per
//! node ([`super::multi::MultiRaft`]) serves ALL groups, demuxing by id — the
//! spike's shared-channel design. A single-group cluster is just one group on that
//! shared listener.
//!
//! ### How election / replication / failover use it
//! * **Election:** a candidate's [`RaftNetwork::vote`] fans a `Vote` RPC out to
//!   every peer; a quorum of grants makes it leader. On a leader's silence the
//!   follower election timer fires and a new term's vote runs — automatic failover.
//! * **Replication:** the leader's [`RaftNetwork::append_entries`] streams committed
//!   log entries to followers; once a quorum has an entry it commits and applies.
//! * **Catch-up:** a lagging/just-restarted follower is brought current with
//!   [`RaftNetwork::install_snapshot`] (openraft's default chunked `full_snapshot`
//!   drives it).
//!
//! ### Pooled per-peer connections (CONCEPT:KG-2.265)
//! The scaffold opened a FRESH `TcpStream` for every append/vote/snapshot RPC and
//! dropped it after one round-trip — under steady replication that churns a
//! connect+handshake per heartbeat, per peer, per group. A [`PeerPool`] now keeps a
//! small set of WARM connections per peer ADDRESS and reuses them across RPCs and
//! across ALL groups on the node (the inbound listener is already shared, so the
//! outbound side is shared too). Correctness is unchanged: the wire is strict
//! request→response on one stream with no correlation id, so a pooled connection is
//! handed out EXCLUSIVELY for one round-trip and only returned to the idle set if
//! that round-trip SUCCEEDED. A stale idle entry (peer closed it while idle) surfaces
//! as an IO error on the first frame, and the caller transparently retries ONCE on a
//! guaranteed-fresh connection — so pooling never changes what openraft observes.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{EgRaft, GroupId, NodeId, TypeConfig};

/// A transport failure → `Unreachable` so openraft backs off and retries (correct
/// for connection refused / a peer that is down — the failover-survival path).
fn unreachable<E: std::error::Error + 'static>(
    e: &E,
) -> RPCError<NodeId, BasicNode, RaftError<NodeId>> {
    RPCError::Unreachable(Unreachable::new(e))
}

/// A remote-reported failure (a follower's append/vote errored — rare). Surfaced as
/// a `Network` error so openraft retries; the remote `RaftError` does not need to be
/// reconstructed because append/vote carry no leader-forward hint (that lives in
/// `client_write`, handled locally on the leader). Keeps the channel minimal.
fn net_err(msg: &str) -> RPCError<NodeId, BasicNode, RaftError<NodeId>> {
    RPCError::Network(NetworkError::new(&StrErr(msg.to_string())))
}

#[derive(Debug)]
struct StrErr(String);
impl std::fmt::Display for StrErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for StrErr {}

// ── pooled per-peer connections (CONCEPT:KG-2.265) ────────────────────────

/// Default warm connections kept idle PER PEER address. A node replicates to each
/// peer over a small number of concurrent streams (append + the occasional
/// vote/snapshot), so a handful covers steady state; the set is bounded so a burst
/// never leaks file descriptors.
const DEFAULT_MAX_IDLE_PER_PEER: usize = 4;

/// A per-peer pool of idle Raft-RPC connections (CONCEPT:KG-2.265). Keyed by the
/// peer's `host:port`, shared by every group on the node (one pool per
/// [`super::multi::MultiRaft`]). See the module docs for the reuse + stale-retry
/// contract.
pub struct PeerPool {
    idle: std::sync::Mutex<HashMap<String, Vec<TcpStream>>>,
    max_idle_per_peer: usize,
    /// Brand-new TCP connections opened (a connect cost actually paid).
    opens: AtomicU64,
    /// Round-trips served on a reused warm connection (a connect cost AVOIDED).
    reuses: AtomicU64,
}

impl PeerPool {
    /// A pool keeping up to [`DEFAULT_MAX_IDLE_PER_PEER`] warm connections per peer.
    pub fn new() -> Arc<Self> {
        Self::with_capacity(DEFAULT_MAX_IDLE_PER_PEER)
    }

    /// A pool with an explicit idle cap per peer (used by tests).
    pub fn with_capacity(max_idle_per_peer: usize) -> Arc<Self> {
        Arc::new(Self {
            idle: std::sync::Mutex::new(HashMap::new()),
            max_idle_per_peer: max_idle_per_peer.max(1),
            opens: AtomicU64::new(0),
            reuses: AtomicU64::new(0),
        })
    }

    /// Count of brand-new connections opened (test/metrics visibility).
    pub fn opens(&self) -> u64 {
        self.opens.load(Ordering::Relaxed)
    }

    /// Count of round-trips that reused a warm connection (test/metrics visibility).
    pub fn reuses(&self) -> u64 {
        self.reuses.load(Ordering::Relaxed)
    }

    /// Take a warm connection for `addr` if one is idle.
    fn take(&self, addr: &str) -> Option<TcpStream> {
        let mut idle = self.idle.lock().unwrap();
        idle.get_mut(addr).and_then(Vec::pop)
    }

    /// Return a healthy connection to the idle set (dropped if the peer is at cap, so
    /// the idle set stays bounded).
    fn put(&self, addr: &str, stream: TcpStream) {
        let mut idle = self.idle.lock().unwrap();
        let v = idle.entry(addr.to_string()).or_default();
        if v.len() < self.max_idle_per_peer {
            v.push(stream);
        }
    }

    /// One framed request→response round-trip to `addr`, reusing a warm connection
    /// when possible. A reused connection that fails is discarded and the call retries
    /// ONCE on a fresh connection, so a stale idle entry never surfaces to openraft.
    pub(crate) async fn round_trip(&self, addr: &str, body: &[u8]) -> Result<Vec<u8>, io::Error> {
        // 1) Try a warm connection first.
        if let Some(mut stream) = self.take(addr) {
            if let Ok(resp) = Self::exchange(&mut stream, body).await {
                self.reuses.fetch_add(1, Ordering::Relaxed);
                self.put(addr, stream);
                return Ok(resp);
            }
            // Stale/broken idle connection — drop it and fall through to a fresh one.
        }
        // 2) Fresh connection (first use, or after a stale reuse).
        let mut stream = TcpStream::connect(addr).await?;
        self.opens.fetch_add(1, Ordering::Relaxed);
        let resp = Self::exchange(&mut stream, body).await?;
        self.put(addr, stream);
        Ok(resp)
    }

    /// Write a framed body and read its framed reply on `stream`.
    async fn exchange(stream: &mut TcpStream, body: &[u8]) -> Result<Vec<u8>, io::Error> {
        write_frame(stream, body).await?;
        read_frame(stream).await
    }
}

impl Default for PeerPool {
    fn default() -> Self {
        Self {
            idle: std::sync::Mutex::new(HashMap::new()),
            max_idle_per_peer: DEFAULT_MAX_IDLE_PER_PEER,
            opens: AtomicU64::new(0),
            reuses: AtomicU64::new(0),
        }
    }
}

// ── group-multiplexed network (CONCEPT:KG-2.205) ──────────────────────────
//
// The multi-group path (`super::multi`) carries a `GroupId` in every RPC frame so
// ONE listener per node serves ALL groups (the spike's shared-channel design). The
// client tags every RPC with the group it serves; the shared listener demuxes by id.

/// A Raft RPC tagged with the group id it belongs to.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum GroupRpc {
    Append(GroupId, AppendEntriesRequest<TypeConfig>),
    Vote(GroupId, VoteRequest<NodeId>),
    Snapshot(GroupId, InstallSnapshotRequest<TypeConfig>),
}

impl GroupRpc {
    pub fn group_id(&self) -> GroupId {
        match self {
            GroupRpc::Append(g, _) | GroupRpc::Vote(g, _) | GroupRpc::Snapshot(g, _) => *g,
        }
    }
}

/// The group-tagged RPC reply.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum GroupRpcReply {
    Append(Result<AppendEntriesResponse<NodeId>, String>),
    Vote(Result<VoteResponse<NodeId>, String>),
    Snapshot(Result<InstallSnapshotResponse<NodeId>, String>),
}

/// Per-group network factory: tags every RPC with `gid`. Cloneable + cheap. Carries
/// the LOCAL node id so the (harness-only) partition gate can decide `(from, to)`
/// reachability — in a production build that id is simply unused.
#[derive(Clone)]
pub struct GroupNetworkFactory {
    gid: GroupId,
    local: NodeId,
    /// Shared per-peer connection pool (CONCEPT:KG-2.265) — one per node, reused by
    /// every group's clients so the outbound side mirrors the shared inbound listener.
    pool: Arc<PeerPool>,
}

impl GroupNetworkFactory {
    pub fn new(gid: GroupId, local: NodeId, pool: Arc<PeerPool>) -> Self {
        Self { gid, local, pool }
    }
}

impl RaftNetworkFactory<TypeConfig> for GroupNetworkFactory {
    type Network = GroupNetworkClient;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        GroupNetworkClient {
            gid: self.gid,
            local: self.local,
            target,
            addr: node.addr.clone(),
            pool: self.pool.clone(),
        }
    }
}

/// A client to ONE peer for ONE group. Reuses a pooled connection per round-trip
/// (CONCEPT:KG-2.265); tags each frame with `gid`.
pub struct GroupNetworkClient {
    gid: GroupId,
    /// The node this client runs ON (the RPC source). Unused in production; consulted
    /// by the harness partition gate to drop frames between partitioned subsets.
    #[allow(dead_code)]
    local: NodeId,
    #[allow(dead_code)]
    target: NodeId,
    addr: String,
    /// The node's shared per-peer connection pool.
    pool: Arc<PeerPool>,
}

impl GroupNetworkClient {
    async fn round_trip(&self, rpc: &GroupRpc) -> Result<GroupRpcReply, io::Error> {
        // ── harness fault-injection: partition gate (CONCEPT:KG-2.212) ──
        // A test/harness build can DROP the frame between two partitioned nodes,
        // simulating a network partition WITHOUT a real firewall. In a production
        // build this whole arm is compiled out, so the network path is byte-for-byte
        // unchanged. We surface it as the same `ConnectionReset`/EOF a real dropped
        // TCP frame would, so openraft treats it as `Unreachable` and backs off.
        #[cfg(any(test, feature = "harness"))]
        if !partition::reachable(self.local, self.target) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "partitioned (harness nemesis)",
            ));
        }
        let body = rmp_serde::to_vec_named(rpc)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let resp = self.pool.round_trip(&self.addr, &body).await?;
        rmp_serde::from_slice(&resp).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

impl RaftNetwork<TypeConfig> for GroupNetworkClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.round_trip(&GroupRpc::Append(self.gid, rpc)).await {
            Ok(GroupRpcReply::Append(Ok(r))) => Ok(r),
            Ok(GroupRpcReply::Append(Err(e))) => Err(net_err(&e)),
            Ok(_) => Err(net_err("unexpected reply variant")),
            Err(e) => Err(unreachable(&e)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.round_trip(&GroupRpc::Vote(self.gid, rpc)).await {
            Ok(GroupRpcReply::Vote(Ok(r))) => Ok(r),
            Ok(GroupRpcReply::Vote(Err(e))) => Err(net_err(&e)),
            Ok(_) => Err(net_err("unexpected reply variant")),
            Err(e) => Err(unreachable(&e)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        match self.round_trip(&GroupRpc::Snapshot(self.gid, rpc)).await {
            Ok(GroupRpcReply::Snapshot(Ok(r))) => Ok(r),
            Ok(GroupRpcReply::Snapshot(Err(e))) => {
                Err(RPCError::Network(NetworkError::new(&StrErr(e))))
            }
            Ok(_) => Err(RPCError::Network(NetworkError::new(&StrErr(
                "unexpected reply variant".to_string(),
            )))),
            Err(e) => Err(RPCError::Unreachable(Unreachable::new(&e))),
        }
    }
}

/// Dispatch one demuxed group RPC into the local group's [`EgRaft`] (or an error
/// reply if this node doesn't run the group). Used by `super::multi`'s listener.
pub async fn dispatch_group(raft: Option<EgRaft>, gid: GroupId, rpc: GroupRpc) -> GroupRpcReply {
    match raft {
        None => match rpc {
            GroupRpc::Append(..) => GroupRpcReply::Append(Err(format!("no group {gid} here"))),
            GroupRpc::Vote(..) => GroupRpcReply::Vote(Err(format!("no group {gid} here"))),
            GroupRpc::Snapshot(..) => GroupRpcReply::Snapshot(Err(format!("no group {gid} here"))),
        },
        Some(raft) => match rpc {
            GroupRpc::Append(_, req) => {
                GroupRpcReply::Append(raft.append_entries(req).await.map_err(|e| e.to_string()))
            }
            GroupRpc::Vote(_, req) => {
                GroupRpcReply::Vote(raft.vote(req).await.map_err(|e| e.to_string()))
            }
            GroupRpc::Snapshot(_, req) => {
                GroupRpcReply::Snapshot(raft.install_snapshot(req).await.map_err(|e| e.to_string()))
            }
        },
    }
}

// ── framing: 4-byte big-endian length prefix + MessagePack body ───────────

pub async fn write_frame(stream: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    let len = (body.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

pub async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    // Bound the frame the same way the engine transport does (defensive cap).
    if len > 256 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raft frame too large",
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

// ── harness partition gate (CONCEPT:KG-2.212) ─────────────────────────────────
//
// A process-global, test/harness-only controller that the per-RPC `round_trip`
// consults to decide whether a frame from node `from` may reach node `to`. The model
// is "islands": every node sits in an island id (default 0 — fully connected), and
// two nodes can exchange RPCs iff they share an island. A partition is just a
// re-assignment of islands; healing puts everyone back on island 0. This is the
// programmatic equivalent of dropping the group-tagged TCP frames between subsets —
// no firewall, no real netns — and it is compiled out of every production build.
#[cfg(any(test, feature = "harness"))]
pub mod partition {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    use super::NodeId;

    /// node id → island id. Absent ⇒ island 0 (the fully-connected default).
    fn table() -> &'static Mutex<HashMap<NodeId, u64>> {
        static T: OnceLock<Mutex<HashMap<NodeId, u64>>> = OnceLock::new();
        T.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn island_of(map: &HashMap<NodeId, u64>, n: NodeId) -> u64 {
        map.get(&n).copied().unwrap_or(0)
    }

    /// Can a frame from `from` reach `to`? True iff they share an island.
    pub fn reachable(from: NodeId, to: NodeId) -> bool {
        let map = table().lock().unwrap();
        island_of(&map, from) == island_of(&map, to)
    }

    /// Partition the cluster into the given groups: each inner slice becomes its own
    /// island. Nodes not listed land on island 0. Replaces any prior partition.
    pub fn partition(groups: &[&[NodeId]]) {
        let mut map = table().lock().unwrap();
        map.clear();
        for (idx, grp) in groups.iter().enumerate() {
            // island ids start at 1 so an unlisted node (island 0) is its own thing.
            for &n in grp.iter() {
                map.insert(n, (idx as u64) + 1);
            }
        }
    }

    /// Isolate a single node from everyone else (its own one-member island). Other
    /// nodes stay mutually reachable on island 0.
    pub fn isolate(node: NodeId) {
        let mut map = table().lock().unwrap();
        map.clear();
        map.insert(node, u64::MAX);
    }

    /// Heal: every node back to island 0 (fully connected).
    pub fn heal() {
        table().lock().unwrap().clear();
    }
}
