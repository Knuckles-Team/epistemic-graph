//! Raft network (CONCEPT:KG-2.188) — a minimal dedicated Raft TCP channel.
//!
//! A small purpose-built TCP channel rather than reusing the engine's auth'd
//! MessagePack RPC, because the Raft RPC payloads are openraft's own request/
//! response types (append-entries / vote / install-snapshot) and routing them
//! through the engine's `Method` enum would mean embedding consensus types into the
//! client-facing protocol — a layering violation. The framing convention is
//! IDENTICAL to the engine transport (4-byte big-endian length prefix + a
//! MessagePack body), so binary payloads survive intact.
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

use super::{EgRaft, NodeId, TypeConfig};

/// One Raft RPC carried over the dedicated channel. MessagePack-encoded.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum RaftRpc {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    Snapshot(InstallSnapshotRequest<TypeConfig>),
}

/// The RPC reply. `Err(String)` carries a remote `RaftError` rendered to a string
/// (the caller wraps it as a `RemoteError` so openraft can still parse a
/// forward-to-leader hint from append/vote — those map cleanly; snapshot uses its
/// own error type).
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum RaftRpcReply {
    Append(Result<AppendEntriesResponse<NodeId>, String>),
    Vote(Result<VoteResponse<NodeId>, String>),
    Snapshot(Result<InstallSnapshotResponse<NodeId>, String>),
}

/// Network factory: produces a per-target client. Cloneable + cheap (it only holds
/// the peer addressing, which lives on each `BasicNode`).
#[derive(Clone, Default)]
pub struct EgNetworkFactory;

impl RaftNetworkFactory<TypeConfig> for EgNetworkFactory {
    type Network = EgNetworkClient;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        EgNetworkClient {
            target,
            addr: node.addr.clone(),
        }
    }
}

/// A client to ONE peer. Connects per-RPC.
pub struct EgNetworkClient {
    target: NodeId,
    addr: String,
}

impl EgNetworkClient {
    async fn round_trip(&self, rpc: &RaftRpc) -> Result<RaftRpcReply, io::Error> {
        let mut stream = TcpStream::connect(&self.addr).await?;
        let body = rmp_serde::to_vec_named(rpc)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        write_frame(&mut stream, &body).await?;
        let resp = read_frame(&mut stream).await?;
        rmp_serde::from_slice(&resp).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

impl RaftNetwork<TypeConfig> for EgNetworkClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.round_trip(&RaftRpc::Append(rpc)).await {
            Ok(RaftRpcReply::Append(Ok(r))) => Ok(r),
            Ok(RaftRpcReply::Append(Err(e))) => Err(net_err(&e)),
            Ok(_) => Err(net_err("unexpected reply variant")),
            Err(e) => Err(unreachable(&e)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.round_trip(&RaftRpc::Vote(rpc)).await {
            Ok(RaftRpcReply::Vote(Ok(r))) => Ok(r),
            Ok(RaftRpcReply::Vote(Err(e))) => Err(net_err(&e)),
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
        match self.round_trip(&RaftRpc::Snapshot(rpc)).await {
            Ok(RaftRpcReply::Snapshot(Ok(r))) => Ok(r),
            Ok(RaftRpcReply::Snapshot(Err(e))) => {
                Err(RPCError::Network(NetworkError::new(&StrErr(e))))
            }
            Ok(_) => Err(RPCError::Network(NetworkError::new(&StrErr(
                "unexpected reply variant".to_string(),
            )))),
            Err(e) => Err(RPCError::Unreachable(Unreachable::new(&e))),
        }
    }
}

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

// ── server side: the Raft-RPC listener ────────────────────────────────────

/// Serve the dedicated Raft RPC channel on `bind_addr`, dispatching each framed
/// request into the local [`EgRaft`]. Runs until the process exits.
pub async fn serve(bind_addr: String, raft: EgRaft) -> io::Result<()> {
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("Raft RPC listening on {}", bind_addr);
    loop {
        let (stream, _peer) = listener.accept().await?;
        let raft = raft.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, raft).await {
                tracing::debug!("raft rpc conn ended: {}", e);
            }
        });
    }
}

async fn handle_conn(mut stream: TcpStream, raft: EgRaft) -> io::Result<()> {
    // One connection may carry multiple sequential RPCs (the client closes when
    // done); loop until the peer hangs up.
    loop {
        let body = match read_frame(&mut stream).await {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };
        let rpc: RaftRpc = rmp_serde::from_slice(&body)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let reply = dispatch(&raft, rpc).await;
        let out = rmp_serde::to_vec_named(&reply)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        write_frame(&mut stream, &out).await?;
    }
}

async fn dispatch(raft: &EgRaft, rpc: RaftRpc) -> RaftRpcReply {
    match rpc {
        RaftRpc::Append(req) => {
            RaftRpcReply::Append(raft.append_entries(req).await.map_err(|e| e.to_string()))
        }
        RaftRpc::Vote(req) => RaftRpcReply::Vote(raft.vote(req).await.map_err(|e| e.to_string())),
        RaftRpc::Snapshot(req) => {
            RaftRpcReply::Snapshot(raft.install_snapshot(req).await.map_err(|e| e.to_string()))
        }
    }
}

// ── framing: 4-byte big-endian length prefix + MessagePack body ───────────

async fn write_frame(stream: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    let len = (body.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
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
