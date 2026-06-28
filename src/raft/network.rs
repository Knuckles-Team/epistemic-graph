//! Raft network (CONCEPT:KG-2.188 + KG-2.205 + KG-2.273) — a group-multiplexed Raft
//! TCP channel on openraft 0.10's [`RaftNetworkV2`] API.
//!
//! A small purpose-built TCP channel rather than reusing the engine's auth'd
//! MessagePack RPC, because the Raft RPC payloads are openraft's own request/
//! response types (append-entries / vote / snapshot / transfer-leader) and routing
//! them through the engine's `Method` enum would mean embedding consensus types into
//! the client-facing protocol — a layering violation. The framing convention is
//! IDENTICAL to the engine transport (4-byte big-endian length prefix + a
//! MessagePack body), so binary payloads survive intact.
//!
//! Every RPC frame is TAGGED with its [`GroupId`] ([`GroupRpc`]) so ONE listener per
//! node ([`super::multi::MultiRaft`]) serves ALL groups, demuxing by id — the
//! spike's shared-channel design. A single-group cluster is just one group on that
//! shared listener.
//!
//! ### openraft 0.10 changes (CONCEPT:KG-2.273)
//! * The deprecated v1 `RaftNetwork` trait was REMOVED. We now implement
//!   [`RaftNetworkV2`] (which blanket-derives the `NetAppend`/`NetVote`/`NetSnapshot`/
//!   `NetTransferLeader`/… sub-traits the factory requires).
//! * The chunked `install_snapshot(InstallSnapshotRequest)` RPC is replaced by
//!   `full_snapshot(vote, Snapshot, …)`: the framework hands us the WHOLE snapshot and
//!   we transmit it however we like. We ship it as one tagged [`GroupRpc::Snapshot`]
//!   frame (vote + meta + the MessagePack body); the follower calls
//!   `install_full_snapshot`.
//! * `RPCError` is now single-generic (`RPCError<C>`); the response types are
//!   `…Response<C>` rather than `…Response<NodeId>`.
//! * We override [`RaftNetworkV2::transfer_leader`] to forward the leader-transfer
//!   notification (it backs the native graceful handoff — see `multi::rebalance_leaders`).
//!
//! ### How election / replication / failover use it
//! * **Election:** a candidate's `vote` RPC fans a `Vote` out to every peer; a quorum
//!   of grants makes it leader. On a leader's silence the follower election timer fires
//!   and a new term's vote runs — automatic failover.
//! * **Replication:** the leader's `append_entries` streams committed log entries to
//!   followers; once a quorum has an entry it commits and applies.
//! * **Catch-up:** a lagging/just-restarted follower is brought current with
//!   `full_snapshot`.
//! * **Graceful handoff:** the leader's `transfer_leader` notifies the target so it
//!   campaigns immediately (the 0.10 native instant handoff).
//!
//! ### Pooled per-peer connections (CONCEPT:KG-2.265)
//! A [`PeerPool`] keeps a small set of WARM connections per peer ADDRESS and reuses
//! them across RPCs and across ALL groups on the node. The wire is strict
//! request→response on one stream with no correlation id, so a pooled connection is
//! handed out EXCLUSIVELY for one round-trip and only returned to the idle set if that
//! round-trip SUCCEEDED; a stale idle entry surfaces as an IO error on the first frame
//! and the caller retries ONCE on a fresh connection.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RaftNetworkFactory;
use openraft::network::{RPCOption, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{EgRaft, GroupId, NodeId, TypeConfig};

/// A transport failure → `Unreachable` so openraft backs off and retries (correct
/// for connection refused / a peer that is down — the failover-survival path).
fn unreachable<E: std::error::Error + 'static>(e: &E) -> RPCError<TypeConfig> {
    RPCError::Unreachable(Unreachable::new(e))
}

/// A remote-reported failure (a follower's append/vote errored — rare). Surfaced as a
/// `Network` error so openraft retries.
fn net_err(msg: &str) -> RPCError<TypeConfig> {
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

/// Default warm connections kept idle PER PEER address.
const DEFAULT_MAX_IDLE_PER_PEER: usize = 4;

/// A per-peer pool of idle Raft-RPC connections (CONCEPT:KG-2.265). Keyed by the
/// peer's `host:port`, shared by every group on the node (one pool per
/// [`super::multi::MultiRaft`]).
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
// ONE listener per node serves ALL groups. The client tags every RPC with the group
// it serves; the shared listener demuxes by id.

/// A Raft RPC tagged with the group id it belongs to (openraft 0.10 types). The
/// snapshot RPC carries the full snapshot (vote + meta + MessagePack body) rather than
/// a chunk, matching the v2 `full_snapshot` model.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum GroupRpc {
    Append(GroupId, AppendEntriesRequest<TypeConfig>),
    Vote(GroupId, VoteRequest<TypeConfig>),
    Snapshot(
        GroupId,
        VoteOf<TypeConfig>,
        SnapshotMetaOf<TypeConfig>,
        Vec<u8>,
    ),
    TransferLeader(GroupId, TransferLeaderRequest<TypeConfig>),
}

impl GroupRpc {
    pub fn group_id(&self) -> GroupId {
        match self {
            GroupRpc::Append(g, _)
            | GroupRpc::Vote(g, _)
            | GroupRpc::Snapshot(g, _, _, _)
            | GroupRpc::TransferLeader(g, _) => *g,
        }
    }
}

/// The group-tagged RPC reply.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum GroupRpcReply {
    Append(Result<AppendEntriesResponse<TypeConfig>, String>),
    Vote(Result<VoteResponse<TypeConfig>, String>),
    Snapshot(Result<SnapshotResponse<TypeConfig>, String>),
    /// Best-effort transfer-leader ack — `Ok(())` accepted, `Err(msg)` rejected/failed.
    TransferLeader(Result<(), String>),
}

// ── heartbeat coalescing wire envelope (CONCEPT:KG-2.271) ──────────────────

/// The top-level Raft wire frame. Either a SINGLE group-tagged RPC (the per-group
/// openraft path) or a BATCH of them coalesced to one peer (CONCEPT:KG-2.271).
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum RaftFrame {
    One(GroupRpc),
    Batch(Vec<GroupRpc>),
}

/// The reply to a [`RaftFrame`]: one reply for `One`, an ORDERED reply-per-RPC for
/// `Batch` (same order the batch was sent, so each awaiting caller matches its own).
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum RaftFrameReply {
    One(GroupRpcReply),
    Batch(Vec<GroupRpcReply>),
}

/// Coalesces per-peer Raft HEARTBEATS across groups into one batched frame
/// (CONCEPT:KG-2.271). Only heartbeats coalesce — log-bearing appends, votes,
/// snapshots, and transfer-leader notifications are latency/ordering-sensitive and
/// pass through individually ([`is_heartbeat`] gates this).
///
/// [`is_heartbeat`]: HeartbeatCoalescer::is_heartbeat
pub struct HeartbeatCoalescer {
    /// peer `host:port` → queued heartbeat RPCs awaiting the next flush.
    pending: std::sync::Mutex<HashMap<String, Vec<GroupRpc>>>,
    /// Total heartbeats folded into a batch (a frame AVOIDED vs sending individually).
    coalesced: AtomicU64,
    /// Flush passes performed (one batched frame emitted per non-empty peer per flush).
    flushes: AtomicU64,
}

impl HeartbeatCoalescer {
    pub fn new() -> Self {
        Self {
            pending: std::sync::Mutex::new(HashMap::new()),
            coalesced: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
        }
    }

    /// Is `rpc` a heartbeat — an `AppendEntries` carrying NO log entries? Only these
    /// coalesce; a log-bearing append / vote / snapshot / transfer must go out alone.
    pub fn is_heartbeat(rpc: &GroupRpc) -> bool {
        matches!(rpc, GroupRpc::Append(_, req) if req.entries.is_empty())
    }

    /// Offer `rpc` destined for `addr` to the coalescer. Returns `true` if it was a
    /// heartbeat and is now BUFFERED for the next flush; `false` if it is not a
    /// heartbeat and the caller must send it directly.
    pub fn offer(&self, addr: &str, rpc: GroupRpc) -> bool {
        if !Self::is_heartbeat(&rpc) {
            return false;
        }
        self.pending
            .lock()
            .unwrap()
            .entry(addr.to_string())
            .or_default()
            .push(rpc);
        true
    }

    /// Drain every buffered peer into one batch per peer (CONCEPT:KG-2.271).
    pub fn drain_batches(&self) -> Vec<(String, Vec<GroupRpc>)> {
        let mut pending = self.pending.lock().unwrap();
        let drained: Vec<(String, Vec<GroupRpc>)> = pending.drain().collect();
        let folded: u64 = drained.iter().map(|(_, v)| v.len() as u64).sum();
        if folded > 0 {
            self.coalesced.fetch_add(folded, Ordering::Relaxed);
            self.flushes.fetch_add(1, Ordering::Relaxed);
        }
        drained
    }

    /// Heartbeats buffered for `addr` right now (test/metrics visibility).
    pub fn pending_for(&self, addr: &str) -> usize {
        self.pending.lock().unwrap().get(addr).map_or(0, Vec::len)
    }

    /// Total heartbeats ever folded into a batch (a frame avoided).
    pub fn coalesced(&self) -> u64 {
        self.coalesced.load(Ordering::Relaxed)
    }

    /// Total flush passes performed.
    pub fn flushes(&self) -> u64 {
        self.flushes.load(Ordering::Relaxed)
    }

    /// Send one coalesced batch to `addr` over the shared [`PeerPool`] and return the
    /// ORDERED per-RPC replies (CONCEPT:KG-2.271).
    pub(crate) async fn send_batch(
        pool: &PeerPool,
        addr: &str,
        batch: Vec<GroupRpc>,
    ) -> Result<Vec<GroupRpcReply>, io::Error> {
        let body = rmp_serde::to_vec_named(&RaftFrame::Batch(batch))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let resp = pool.round_trip(addr, &body).await?;
        match rmp_serde::from_slice::<RaftFrameReply>(&resp)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        {
            RaftFrameReply::Batch(replies) => Ok(replies),
            RaftFrameReply::One(reply) => Ok(vec![reply]),
        }
    }
}

impl Default for HeartbeatCoalescer {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-group network factory: tags every RPC with `gid`. Cloneable + cheap. Carries
/// the LOCAL node id so the (harness-only) partition gate can decide `(from, to)`
/// reachability — in a production build that id is simply unused.
#[derive(Clone)]
pub struct GroupNetworkFactory {
    gid: GroupId,
    local: NodeId,
    /// Shared per-peer connection pool (CONCEPT:KG-2.265).
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
    async fn round_trip(&self, rpc: GroupRpc) -> Result<GroupRpcReply, io::Error> {
        // ── harness fault-injection: partition gate (CONCEPT:KG-2.212) ──
        #[cfg(any(test, feature = "harness"))]
        if !partition::reachable(self.local, self.target) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "partitioned (harness nemesis)",
            ));
        }
        // A per-group RPC goes out as a single-RPC frame (CONCEPT:KG-2.271); coalesced
        // heartbeats use `RaftFrame::Batch` via `HeartbeatCoalescer::send_batch`.
        let body = rmp_serde::to_vec_named(&RaftFrame::One(rpc))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let resp = self.pool.round_trip(&self.addr, &body).await?;
        match rmp_serde::from_slice::<RaftFrameReply>(&resp)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        {
            RaftFrameReply::One(reply) => Ok(reply),
            RaftFrameReply::Batch(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected a single reply to a single-RPC frame",
            )),
        }
    }
}

impl RaftNetworkV2<TypeConfig> for GroupNetworkClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self.round_trip(GroupRpc::Append(self.gid, rpc)).await {
            Ok(GroupRpcReply::Append(Ok(r))) => Ok(r),
            Ok(GroupRpcReply::Append(Err(e))) => Err(net_err(&e)),
            Ok(_) => Err(net_err("unexpected reply variant")),
            Err(e) => Err(unreachable(&e)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self.round_trip(GroupRpc::Vote(self.gid, rpc)).await {
            Ok(GroupRpcReply::Vote(Ok(r))) => Ok(r),
            Ok(GroupRpcReply::Vote(Err(e))) => Err(net_err(&e)),
            Ok(_) => Err(net_err("unexpected reply variant")),
            Err(e) => Err(unreachable(&e)),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        // v2 full-snapshot transfer: ship the WHOLE snapshot as one tagged frame
        // (vote + meta + the MessagePack body). The follower calls
        // `install_full_snapshot` and replies with its current vote.
        let data = snapshot.snapshot.into_inner();
        let rpc = GroupRpc::Snapshot(self.gid, vote, snapshot.meta, data);
        match self.round_trip(rpc).await {
            Ok(GroupRpcReply::Snapshot(Ok(r))) => Ok(r),
            Ok(GroupRpcReply::Snapshot(Err(e))) => {
                Err(StreamingError::Network(NetworkError::new(&StrErr(e))))
            }
            Ok(_) => Err(StreamingError::Network(NetworkError::new(&StrErr(
                "unexpected reply variant".to_string(),
            )))),
            Err(e) => Err(StreamingError::Unreachable(Unreachable::new(&e))),
        }
    }

    /// Forward a leader-transfer notification to the target (CONCEPT:KG-2.273). This
    /// backs the native graceful handoff: the old leader tells the target to campaign
    /// at once instead of waiting for its lease to time out.
    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self
            .round_trip(GroupRpc::TransferLeader(self.gid, req))
            .await
        {
            Ok(GroupRpcReply::TransferLeader(Ok(()))) => Ok(Ok(())),
            Ok(GroupRpcReply::TransferLeader(Err(e))) => Err(net_err(&e)),
            Ok(_) => Err(net_err("unexpected reply variant")),
            Err(e) => Err(unreachable(&e)),
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
            GroupRpc::TransferLeader(..) => {
                GroupRpcReply::TransferLeader(Err(format!("no group {gid} here")))
            }
        },
        Some(raft) => match rpc {
            GroupRpc::Append(_, req) => {
                GroupRpcReply::Append(raft.append_entries(req).await.map_err(|e| e.to_string()))
            }
            GroupRpc::Vote(_, req) => {
                GroupRpcReply::Vote(raft.vote(req).await.map_err(|e| e.to_string()))
            }
            GroupRpc::Snapshot(_, vote, meta, data) => {
                let snap = Snapshot {
                    meta,
                    snapshot: std::io::Cursor::new(data),
                };
                GroupRpcReply::Snapshot(
                    raft.install_full_snapshot(vote, snap)
                        .await
                        .map_err(|e| e.to_string()),
                )
            }
            GroupRpc::TransferLeader(_, req) => {
                // Flatten `Result<Result<(), TransferLeaderError>, Fatal>` to a small
                // wire ack — this notification is best-effort.
                let reply = match raft.handle_transfer_leader(req).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(e.to_string()),
                    Err(e) => Err(e.to_string()),
                };
                GroupRpcReply::TransferLeader(reply)
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
// two nodes can exchange RPCs iff they share an island. Compiled out of every
// production build.
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
