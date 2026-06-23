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
//! Each client connects per-RPC (connect → send framed request → read framed
//! response → close). Simple + correct for a first increment; a pooled connection
//! per peer is a follow-up optimization, not a correctness change.

use std::io;

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

/// Per-group network factory: tags every RPC with `gid`. Cloneable + cheap.
#[derive(Clone)]
pub struct GroupNetworkFactory {
    gid: GroupId,
}

impl GroupNetworkFactory {
    pub fn new(gid: GroupId) -> Self {
        Self { gid }
    }
}

impl RaftNetworkFactory<TypeConfig> for GroupNetworkFactory {
    type Network = GroupNetworkClient;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        GroupNetworkClient {
            gid: self.gid,
            target,
            addr: node.addr.clone(),
        }
    }
}

/// A client to ONE peer for ONE group. Connects per-RPC; tags each frame with `gid`.
pub struct GroupNetworkClient {
    gid: GroupId,
    #[allow(dead_code)]
    target: NodeId,
    addr: String,
}

impl GroupNetworkClient {
    async fn round_trip(&self, rpc: &GroupRpc) -> Result<GroupRpcReply, io::Error> {
        let mut stream = TcpStream::connect(&self.addr).await?;
        let body = rmp_serde::to_vec_named(rpc)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        write_frame(&mut stream, &body).await?;
        let resp = read_frame(&mut stream).await?;
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
