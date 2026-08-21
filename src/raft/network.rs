//! Raft network (CONCEPT:AU-KG.ingest.source-sync-canonical + KG-2.205 + KG-2.273) — a group-multiplexed Raft
//! TCP channel on openraft 0.10's [`RaftNetworkV2`] API.
//!
//! A small purpose-built TCP channel rather than reusing the engine's authenticated
//! MessagePack RPC, because the Raft RPC payloads are openraft's own request/
//! response types (append-entries / vote / snapshot / transfer-leader) and routing
//! them through the engine's `Method` enum would mean embedding consensus types into
//! the client-facing protocol — a layering violation. A nonce challenge authenticates
//! both peer ids with a runtime pre-shared key, derives a unique per-connection
//! XChaCha20-Poly1305 key, and every length-prefixed frame is then encrypted and bound
//! to a strictly monotonic sequence number. Recorded handshakes or frames therefore
//! cannot be replayed on either the same connection or a later connection.
//!
//! Every RPC frame is TAGGED with its [`GroupId`] ([`GroupRpc`]) so ONE listener per
//! node ([`super::multi::MultiRaft`]) serves ALL groups, demuxing by id — the
//! spike's shared-channel design. A single-group cluster is just one group on that
//! shared listener.
//!
//! ### openraft 0.10 changes (CONCEPT:AU-KG.backend.authority-has-already-acked)
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
//! ### Pooled per-peer connections (CONCEPT:AU-KG.ontology.manage-arbitrary)
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

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use hmac::{Hmac, Mac};
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
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Notify};

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

// ── pooled per-peer connections (CONCEPT:AU-KG.ontology.manage-arbitrary) ────────────────────────

/// Default warm connections kept idle PER PEER address.
const DEFAULT_MAX_IDLE_PER_PEER: usize = 4;
pub(crate) const MAX_RAFT_FRAME_BYTES: usize = 256 * 1024 * 1024;
const SECURE_FRAME_HEADER_BYTES: usize = 4 + 1 + 8;
const AEAD_TAG_BYTES: usize = 16;
pub(crate) const MAX_RAFT_PAYLOAD_BYTES: usize =
    MAX_RAFT_FRAME_BYTES - SECURE_FRAME_HEADER_BYTES - AEAD_TAG_BYTES;
const MAX_RAFT_FRAME_ITEMS: usize = 4_000_000;
pub(crate) const MAX_RAFT_BATCH_RPCS: usize = 1_024;
pub(crate) const RAFT_FRAME_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
const RAFT_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const RAFT_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const MAX_RAFT_POOL_PEERS: usize = 1_024;
const MAX_RAFT_PEER_ADDRESS_BYTES: usize = 1_024;
const RAFT_FRAME_BUDGET_UNIT_BYTES: usize = 64 * 1024;
pub(crate) const RAFT_FRAME_BUDGET_UNITS: usize =
    MAX_RAFT_FRAME_BYTES / RAFT_FRAME_BUDGET_UNIT_BYTES;
/// Bounded coalescing window.  OpenRaft's configured heartbeat interval is 250 ms;
/// this short window lets concurrent group heartbeats share a frame without turning
/// a heartbeat into an unbounded queue or materially delaying failure detection.
pub(crate) const HEARTBEAT_COALESCE_WINDOW: std::time::Duration =
    std::time::Duration::from_millis(5);
const CLIENT_HELLO_MAGIC: &[u8; 4] = b"EGRC";
const SERVER_HELLO_MAGIC: &[u8; 4] = b"EGRS";
const SECURE_FRAME_MAGIC: &[u8; 4] = b"EGRF";
const RAFT_WIRE_VERSION: u8 = 1;
const HANDSHAKE_NONCE_BYTES: usize = 32;
const AUTH_TAG_BYTES: usize = 32;
const CLIENT_HELLO_BYTES: usize = 4 + 1 + 8 + 8 + HANDSHAKE_NONCE_BYTES + AUTH_TAG_BYTES;
const SERVER_HELLO_BYTES: usize = 4 + 1 + 8 + 8 + HANDSHAKE_NONCE_BYTES * 2 + AUTH_TAG_BYTES;

type HmacSha256 = Hmac<Sha256>;

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(crate) fn decode_wire<T: serde::de::DeserializeOwned>(body: &[u8]) -> io::Result<T> {
    eg_types::msgpack::decode_bounded(
        body,
        eg_types::msgpack::MsgpackLimits::new(MAX_RAFT_FRAME_BYTES, MAX_RAFT_FRAME_ITEMS, 64),
    )
    .map_err(|_| invalid_data("invalid raft frame"))
}

/// Runtime-only peer registry and pre-shared key for Raft transport security.
/// Debug output never exposes key material or endpoint configuration.
pub(crate) struct RaftTransportAuth {
    local_node: NodeId,
    key: [u8; 32],
    peers_by_addr: std::sync::RwLock<HashMap<String, NodeId>>,
    allowed_peers: std::sync::RwLock<std::collections::HashSet<NodeId>>,
}

impl std::fmt::Debug for RaftTransportAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RaftTransportAuth(<redacted>)")
    }
}

impl Drop for RaftTransportAuth {
    fn drop(&mut self) {
        self.key.fill(0);
    }
}

impl RaftTransportAuth {
    pub(crate) fn new(
        local_node: NodeId,
        key: &[u8; 32],
        peers: impl IntoIterator<Item = (NodeId, String)>,
    ) -> io::Result<Arc<Self>> {
        let auth = Arc::new(Self {
            local_node,
            key: *key,
            peers_by_addr: std::sync::RwLock::new(HashMap::new()),
            allowed_peers: std::sync::RwLock::new(std::collections::HashSet::new()),
        });
        for (node, addr) in peers {
            auth.register_peer(node, &addr)?;
        }
        if !auth.is_allowed(local_node) {
            return Err(invalid_data("raft peer registry excludes the local node"));
        }
        Ok(auth)
    }

    pub(crate) fn register_peer(&self, node: NodeId, addr: &str) -> io::Result<()> {
        if addr.is_empty() || addr.len() > MAX_RAFT_PEER_ADDRESS_BYTES {
            return Err(invalid_data("invalid raft peer address"));
        }
        let mut by_addr = self.peers_by_addr.write().unwrap();
        let mut allowed = self.allowed_peers.write().unwrap();
        if !allowed.contains(&node) && allowed.len() >= MAX_RAFT_POOL_PEERS {
            return Err(invalid_data("raft peer registry exceeds limit"));
        }
        if let Some(existing) = by_addr.get(addr) {
            if *existing != node {
                return Err(invalid_data(
                    "raft peer address is assigned to another node",
                ));
            }
        } else if by_addr.len() >= MAX_RAFT_POOL_PEERS {
            return Err(invalid_data("raft peer address registry exceeds limit"));
        }
        allowed.insert(node);
        by_addr.insert(addr.to_string(), node);
        Ok(())
    }

    fn is_allowed(&self, node: NodeId) -> bool {
        self.allowed_peers.read().unwrap().contains(&node)
    }

    fn peer_for_addr(&self, addr: &str) -> io::Result<NodeId> {
        self.peers_by_addr
            .read()
            .unwrap()
            .get(addr)
            .copied()
            .ok_or_else(|| invalid_data("raft peer address is not authorized"))
    }

    fn tag(&self, body: &[u8]) -> io::Result<[u8; AUTH_TAG_BYTES]> {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.key)
            .map_err(|_| invalid_data("invalid raft transport key"))?;
        mac.update(body);
        let mut out = [0u8; AUTH_TAG_BYTES];
        out.copy_from_slice(&mac.finalize().into_bytes());
        Ok(out)
    }

    fn verify_tag(&self, body: &[u8], tag: &[u8]) -> io::Result<()> {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.key)
            .map_err(|_| invalid_data("invalid raft transport key"))?;
        mac.update(body);
        mac.verify_slice(tag)
            .map_err(|_| invalid_data("raft peer authentication failed"))
    }

    fn session_key(
        &self,
        client: NodeId,
        server: NodeId,
        client_nonce: &[u8; HANDSHAKE_NONCE_BYTES],
        server_nonce: &[u8; HANDSHAKE_NONCE_BYTES],
    ) -> io::Result<[u8; 32]> {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.key)
            .map_err(|_| invalid_data("invalid raft transport key"))?;
        mac.update(b"epistemic-graph/raft-session/v1\0");
        mac.update(&client.to_be_bytes());
        mac.update(&server.to_be_bytes());
        mac.update(client_nonce);
        mac.update(server_nonce);
        let mut digest = mac.finalize().into_bytes();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        digest.fill(0);
        Ok(out)
    }
}

pub(crate) struct SecureRaftConnection {
    stream: TcpStream,
    cipher: XChaCha20Poly1305,
    write_nonce_prefix: [u8; 16],
    read_nonce_prefix: [u8; 16],
    write_sequence: u64,
    read_sequence: u64,
    read_budget: Option<Arc<tokio::sync::Semaphore>>,
}

impl SecureRaftConnection {
    async fn connect(
        mut stream: TcpStream,
        auth: &RaftTransportAuth,
        expected_peer: NodeId,
    ) -> io::Result<Self> {
        tokio::time::timeout(RAFT_HANDSHAKE_TIMEOUT, async {
            let mut client_nonce = [0u8; HANDSHAKE_NONCE_BYTES];
            rand::rngs::OsRng.fill_bytes(&mut client_nonce);
            let mut hello = [0u8; CLIENT_HELLO_BYTES];
            hello[0..4].copy_from_slice(CLIENT_HELLO_MAGIC);
            hello[4] = RAFT_WIRE_VERSION;
            hello[5..13].copy_from_slice(&auth.local_node.to_be_bytes());
            hello[13..21].copy_from_slice(&expected_peer.to_be_bytes());
            hello[21..21 + HANDSHAKE_NONCE_BYTES].copy_from_slice(&client_nonce);
            let tag_offset = CLIENT_HELLO_BYTES - AUTH_TAG_BYTES;
            let tag = auth.tag(&hello[..tag_offset])?;
            hello[tag_offset..].copy_from_slice(&tag);
            stream.write_all(&hello).await?;
            stream.flush().await?;

            let mut response = [0u8; SERVER_HELLO_BYTES];
            stream.read_exact(&mut response).await?;
            let response_tag_offset = SERVER_HELLO_BYTES - AUTH_TAG_BYTES;
            if &response[0..4] != SERVER_HELLO_MAGIC
                || response[4] != RAFT_WIRE_VERSION
                || read_u64(&response[5..13])? != expected_peer
                || read_u64(&response[13..21])? != auth.local_node
                || response[21..21 + HANDSHAKE_NONCE_BYTES] != client_nonce
            {
                return Err(invalid_data("invalid raft server handshake"));
            }
            auth.verify_tag(
                &response[..response_tag_offset],
                &response[response_tag_offset..],
            )?;
            let mut server_nonce = [0u8; HANDSHAKE_NONCE_BYTES];
            server_nonce.copy_from_slice(
                &response[21 + HANDSHAKE_NONCE_BYTES..21 + HANDSHAKE_NONCE_BYTES * 2],
            );
            let session_key =
                auth.session_key(auth.local_node, expected_peer, &client_nonce, &server_nonce)?;
            Self::from_session(stream, session_key, true, None)
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "raft handshake timed out"))?
    }

    async fn accept(
        mut stream: TcpStream,
        auth: &RaftTransportAuth,
        read_budget: Option<Arc<tokio::sync::Semaphore>>,
    ) -> io::Result<Self> {
        tokio::time::timeout(RAFT_HANDSHAKE_TIMEOUT, async {
            let mut hello = [0u8; CLIENT_HELLO_BYTES];
            stream.read_exact(&mut hello).await?;
            let tag_offset = CLIENT_HELLO_BYTES - AUTH_TAG_BYTES;
            let client = read_u64(&hello[5..13])?;
            let target = read_u64(&hello[13..21])?;
            if &hello[0..4] != CLIENT_HELLO_MAGIC
                || hello[4] != RAFT_WIRE_VERSION
                || target != auth.local_node
                || !auth.is_allowed(client)
            {
                return Err(invalid_data("invalid raft client handshake"));
            }
            auth.verify_tag(&hello[..tag_offset], &hello[tag_offset..])?;
            let mut client_nonce = [0u8; HANDSHAKE_NONCE_BYTES];
            client_nonce.copy_from_slice(&hello[21..21 + HANDSHAKE_NONCE_BYTES]);
            let mut server_nonce = [0u8; HANDSHAKE_NONCE_BYTES];
            rand::rngs::OsRng.fill_bytes(&mut server_nonce);

            let mut response = [0u8; SERVER_HELLO_BYTES];
            response[0..4].copy_from_slice(SERVER_HELLO_MAGIC);
            response[4] = RAFT_WIRE_VERSION;
            response[5..13].copy_from_slice(&auth.local_node.to_be_bytes());
            response[13..21].copy_from_slice(&client.to_be_bytes());
            response[21..21 + HANDSHAKE_NONCE_BYTES].copy_from_slice(&client_nonce);
            response[21 + HANDSHAKE_NONCE_BYTES..21 + HANDSHAKE_NONCE_BYTES * 2]
                .copy_from_slice(&server_nonce);
            let response_tag_offset = SERVER_HELLO_BYTES - AUTH_TAG_BYTES;
            let tag = auth.tag(&response[..response_tag_offset])?;
            response[response_tag_offset..].copy_from_slice(&tag);
            stream.write_all(&response).await?;
            stream.flush().await?;
            let session_key =
                auth.session_key(client, auth.local_node, &client_nonce, &server_nonce)?;
            Self::from_session(stream, session_key, false, read_budget)
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "raft handshake timed out"))?
    }

    fn from_session(
        stream: TcpStream,
        mut session_key: [u8; 32],
        client: bool,
        read_budget: Option<Arc<tokio::sync::Semaphore>>,
    ) -> io::Result<Self> {
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&session_key));
        let client_to_server = nonce_prefix(&session_key, b"client-to-server")?;
        let server_to_client = nonce_prefix(&session_key, b"server-to-client")?;
        session_key.fill(0);
        let (write_nonce_prefix, read_nonce_prefix) = if client {
            (client_to_server, server_to_client)
        } else {
            (server_to_client, client_to_server)
        };
        Ok(Self {
            stream,
            cipher,
            write_nonce_prefix,
            read_nonce_prefix,
            write_sequence: 0,
            read_sequence: 0,
            read_budget,
        })
    }

    async fn write_payload(&mut self, plaintext: &[u8]) -> io::Result<()> {
        if plaintext.is_empty() || plaintext.len() > MAX_RAFT_PAYLOAD_BYTES {
            return Err(invalid_data("invalid raft payload length"));
        }
        let sequence = self
            .write_sequence
            .checked_add(1)
            .ok_or_else(|| invalid_data("raft frame sequence exhausted"))?;
        let mut header = [0u8; SECURE_FRAME_HEADER_BYTES];
        header[0..4].copy_from_slice(SECURE_FRAME_MAGIC);
        header[4] = RAFT_WIRE_VERSION;
        header[5..13].copy_from_slice(&sequence.to_be_bytes());
        let nonce = frame_nonce(&self.write_nonce_prefix, sequence);
        let ciphertext = self
            .cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &header,
                },
            )
            .map_err(|_| invalid_data("raft frame encryption failed"))?;
        let mut wire = Vec::with_capacity(header.len() + ciphertext.len());
        wire.extend_from_slice(&header);
        wire.extend_from_slice(&ciphertext);
        self.write_sequence = sequence;
        write_frame(&mut self.stream, &wire).await
    }

    async fn read_payload(&mut self) -> io::Result<RaftPayload> {
        let guarded = read_frame_guarded(&mut self.stream, self.read_budget.as_ref()).await?;
        let GuardedFrame {
            bytes: wire,
            _permit: permit,
        } = guarded;
        if wire.len() <= SECURE_FRAME_HEADER_BYTES + AEAD_TAG_BYTES
            || &wire[0..4] != SECURE_FRAME_MAGIC
            || wire[4] != RAFT_WIRE_VERSION
        {
            return Err(invalid_data("invalid encrypted raft frame"));
        }
        let sequence = read_u64(&wire[5..13])?;
        let expected = self
            .read_sequence
            .checked_add(1)
            .ok_or_else(|| invalid_data("raft frame sequence exhausted"))?;
        if sequence != expected {
            return Err(invalid_data("replayed or out-of-order raft frame"));
        }
        let nonce = frame_nonce(&self.read_nonce_prefix, sequence);
        let plaintext = self
            .cipher
            .decrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &wire[SECURE_FRAME_HEADER_BYTES..],
                    aad: &wire[..SECURE_FRAME_HEADER_BYTES],
                },
            )
            .map_err(|_| invalid_data("raft frame authentication failed"))?;
        if plaintext.is_empty() || plaintext.len() > MAX_RAFT_PAYLOAD_BYTES {
            return Err(invalid_data("invalid raft payload length"));
        }
        self.read_sequence = sequence;
        Ok(RaftPayload {
            bytes: plaintext,
            _permit: permit,
        })
    }
}

fn read_u64(bytes: &[u8]) -> io::Result<u64> {
    let raw: [u8; 8] = bytes
        .try_into()
        .map_err(|_| invalid_data("invalid raft handshake integer"))?;
    Ok(u64::from_be_bytes(raw))
}

fn nonce_prefix(key: &[u8; 32], direction: &[u8]) -> io::Result<[u8; 16]> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key)
        .map_err(|_| invalid_data("invalid raft session key"))?;
    mac.update(b"epistemic-graph/raft-nonce/v1\0");
    mac.update(direction);
    let mut digest = mac.finalize().into_bytes();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    digest.fill(0);
    Ok(out)
}

fn frame_nonce(prefix: &[u8; 16], sequence: u64) -> [u8; 24] {
    let mut nonce = [0u8; 24];
    nonce[..16].copy_from_slice(prefix);
    nonce[16..].copy_from_slice(&sequence.to_be_bytes());
    nonce
}

pub(crate) enum RaftConnection {
    Plain(TcpStream, Option<Arc<tokio::sync::Semaphore>>),
    Secure(SecureRaftConnection),
}

impl RaftConnection {
    async fn connect(
        stream: TcpStream,
        auth: Option<&RaftTransportAuth>,
        expected_peer: Option<NodeId>,
    ) -> io::Result<Self> {
        match (auth, expected_peer) {
            (Some(auth), Some(peer)) => SecureRaftConnection::connect(stream, auth, peer)
                .await
                .map(Self::Secure),
            (None, None) => Ok(Self::Plain(stream, None)),
            _ => Err(invalid_data("incomplete raft transport security context")),
        }
    }

    pub(crate) async fn accept(
        stream: TcpStream,
        auth: Option<&RaftTransportAuth>,
        read_budget: Arc<tokio::sync::Semaphore>,
    ) -> io::Result<Self> {
        match auth {
            Some(auth) => SecureRaftConnection::accept(stream, auth, Some(read_budget))
                .await
                .map(Self::Secure),
            None => Ok(Self::Plain(stream, Some(read_budget))),
        }
    }

    pub(crate) async fn write_payload(&mut self, body: &[u8]) -> io::Result<()> {
        match self {
            Self::Plain(stream, _) => write_frame(stream, body).await,
            Self::Secure(stream) => stream.write_payload(body).await,
        }
    }

    pub(crate) async fn read_payload(&mut self) -> io::Result<RaftPayload> {
        match self {
            Self::Plain(stream, budget) => read_frame_guarded(stream, budget.as_ref())
                .await
                .map(GuardedFrame::into_payload),
            Self::Secure(stream) => stream.read_payload().await,
        }
    }
}

/// A per-peer pool of idle Raft-RPC connections (CONCEPT:AU-KG.ontology.manage-arbitrary). Keyed by the
/// peer's `host:port`, shared by every group on the node (one pool per
/// [`super::multi::MultiRaft`]).
pub struct PeerPool {
    idle: std::sync::Mutex<HashMap<String, Vec<RaftConnection>>>,
    max_idle_per_peer: usize,
    auth: Option<Arc<RaftTransportAuth>>,
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
            auth: None,
            opens: AtomicU64::new(0),
            reuses: AtomicU64::new(0),
        })
    }

    pub(crate) fn with_auth(auth: Arc<RaftTransportAuth>) -> Arc<Self> {
        Arc::new(Self {
            idle: std::sync::Mutex::new(HashMap::new()),
            max_idle_per_peer: DEFAULT_MAX_IDLE_PER_PEER,
            auth: Some(auth),
            opens: AtomicU64::new(0),
            reuses: AtomicU64::new(0),
        })
    }

    pub(crate) fn register_peer(&self, node: NodeId, addr: &str) -> io::Result<()> {
        match &self.auth {
            Some(auth) => auth.register_peer(node, addr),
            None => Ok(()),
        }
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
    fn take(&self, addr: &str) -> Option<RaftConnection> {
        let mut idle = self.idle.lock().unwrap();
        idle.get_mut(addr).and_then(Vec::pop)
    }

    /// Return a healthy connection to the idle set (dropped if the peer is at cap, so
    /// the idle set stays bounded).
    fn put(&self, addr: &str, stream: RaftConnection) {
        let mut idle = self.idle.lock().unwrap();
        if addr.is_empty()
            || addr.len() > MAX_RAFT_PEER_ADDRESS_BYTES
            || (!idle.contains_key(addr) && idle.len() >= MAX_RAFT_POOL_PEERS)
        {
            return;
        }
        let v = idle.entry(addr.to_string()).or_default();
        if v.len() < self.max_idle_per_peer {
            v.push(stream);
        }
    }

    /// One framed request→response round-trip to `addr`, reusing a warm connection
    /// when possible. A reused connection that fails is discarded and the call retries
    /// ONCE on a fresh connection, so a stale idle entry never surfaces to openraft.
    pub(crate) async fn round_trip(&self, addr: &str, body: &[u8]) -> Result<Vec<u8>, io::Error> {
        if addr.is_empty()
            || addr.len() > MAX_RAFT_PEER_ADDRESS_BYTES
            || body.is_empty()
            || body.len() > MAX_RAFT_PAYLOAD_BYTES
        {
            return Err(invalid_data("invalid raft peer or frame"));
        }
        let expected_peer = match &self.auth {
            Some(auth) => Some(auth.peer_for_addr(addr)?),
            None => None,
        };
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
        let stream = tokio::time::timeout(RAFT_CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "raft connect timed out"))??;
        let mut stream =
            RaftConnection::connect(stream, self.auth.as_deref(), expected_peer).await?;
        self.opens.fetch_add(1, Ordering::Relaxed);
        let resp = Self::exchange(&mut stream, body).await?;
        self.put(addr, stream);
        Ok(resp)
    }

    /// Write a framed body and read its framed reply on `stream`.
    async fn exchange(stream: &mut RaftConnection, body: &[u8]) -> Result<Vec<u8>, io::Error> {
        tokio::time::timeout(RAFT_FRAME_IO_TIMEOUT, async {
            stream.write_payload(body).await?;
            stream.read_payload().await.map(RaftPayload::into_vec)
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "raft exchange timed out"))?
    }
}

impl Default for PeerPool {
    fn default() -> Self {
        Self {
            idle: std::sync::Mutex::new(HashMap::new()),
            max_idle_per_peer: DEFAULT_MAX_IDLE_PER_PEER,
            auth: None,
            opens: AtomicU64::new(0),
            reuses: AtomicU64::new(0),
        }
    }
}

// ── group-multiplexed network (CONCEPT:EG-KG.sharding.raft-resharding) ──────────────────────────
//
// The multi-group path (`super::multi`) carries a `GroupId` in every RPC frame so
// ONE listener per node serves ALL groups. The client tags every RPC with the group
// it serves; the shared listener demuxes by id.

/// A Raft RPC tagged with the group id it belongs to (openraft 0.10 types). The
/// snapshot RPC carries the full snapshot (vote + meta + MessagePack body) rather than
/// a chunk, matching the v2 `full_snapshot` model.
#[derive(Clone, Serialize, Deserialize)]
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
    /// Authenticated engine-internal client write forwarded to another group's
    /// current leader. Public clients cannot construct this transport frame.
    ClientWrite(GroupId, super::RaftRequest),
    /// Per-group ReadIndex barrier.  This is also used against the placement group
    /// before resolving a cross-graph route vector.
    ReadBarrier(GroupId),
    /// Bounded durable graph page, served only after a ReadIndex barrier and an
    /// epoch/fencing-token check on the destination leader.
    ReadPage(GroupId, super::xread::ReadPageRequest),
}

impl GroupRpc {
    pub fn group_id(&self) -> GroupId {
        match self {
            GroupRpc::Append(g, _)
            | GroupRpc::Vote(g, _)
            | GroupRpc::Snapshot(g, _, _, _)
            | GroupRpc::TransferLeader(g, _)
            | GroupRpc::ClientWrite(g, _)
            | GroupRpc::ReadBarrier(g)
            | GroupRpc::ReadPage(g, _) => *g,
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
    ClientWrite(Box<Result<super::RaftResponse, String>>),
    ReadBarrier(Result<u64, super::xread::ReadPageError>),
    ReadPage(Result<super::xread::ReadPageReply, super::xread::ReadPageError>),
}

// ── heartbeat coalescing wire envelope (CONCEPT:EG-KG.storage.concept-2) ──────────────────

/// The top-level Raft wire frame. Either a SINGLE group-tagged RPC (the per-group
/// openraft path) or a BATCH of them coalesced to one peer (CONCEPT:EG-KG.storage.concept-2).
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum RaftFrame {
    One(Box<GroupRpc>),
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
/// (CONCEPT:EG-KG.storage.concept-2). Only heartbeats coalesce — log-bearing appends, votes,
/// snapshots, and transfer-leader notifications are latency/ordering-sensitive and
/// pass through individually ([`is_heartbeat`] gates this).
///
/// [`is_heartbeat`]: HeartbeatCoalescer::is_heartbeat
pub struct HeartbeatCoalescer {
    /// peer `host:port` → queued heartbeat RPCs awaiting the next flush.  A waiter
    /// is present only for the live OpenRaft path; the public `offer`/`drain_batches`
    /// surface intentionally remains a side-effect-free construction fixture.
    pending: std::sync::Mutex<HashMap<String, Vec<PendingHeartbeat>>>,
    /// Total heartbeats folded into a batch (a frame AVOIDED vs sending individually).
    coalesced: AtomicU64,
    /// Flush passes performed (one batched frame emitted per non-empty peer per flush).
    flushes: AtomicU64,
    /// Wakes the bounded flush worker after a heartbeat is queued.
    wake: Notify,
    /// Set during shutdown so queued OpenRaft callers fail promptly rather than
    /// waiting forever if the manager is being torn down.
    stopping: std::sync::atomic::AtomicBool,
}

struct PendingHeartbeat {
    rpc: GroupRpc,
    completion: Option<oneshot::Sender<Result<GroupRpcReply, String>>>,
}

impl HeartbeatCoalescer {
    pub fn new() -> Self {
        Self {
            pending: std::sync::Mutex::new(HashMap::new()),
            coalesced: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            wake: Notify::new(),
            stopping: std::sync::atomic::AtomicBool::new(false),
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
        self.enqueue(addr, rpc, None)
    }

    fn enqueue(
        &self,
        addr: &str,
        rpc: GroupRpc,
        completion: Option<oneshot::Sender<Result<GroupRpcReply, String>>>,
    ) -> bool {
        if self.stopping.load(Ordering::Acquire) {
            return false;
        }
        if !Self::is_heartbeat(&rpc) || addr.is_empty() || addr.len() > MAX_RAFT_PEER_ADDRESS_BYTES
        {
            return false;
        }
        let mut pending = self.pending.lock().unwrap();
        if self.stopping.load(Ordering::Acquire) {
            return false;
        }
        if !pending.contains_key(addr) && pending.len() >= MAX_RAFT_POOL_PEERS {
            return false;
        }
        let peer = pending.entry(addr.to_string()).or_default();
        if peer.len() >= MAX_RAFT_BATCH_RPCS {
            return false;
        }
        peer.push(PendingHeartbeat { rpc, completion });
        self.wake.notify_one();
        true
    }

    /// Queue one live OpenRaft heartbeat and return its eventual ordered reply.
    /// The caller owns the await; the coalescer owns only the bounded queue and
    /// completion sender.  Non-heartbeats never enter this path.
    pub(crate) async fn heartbeat_round_trip(
        &self,
        addr: &str,
        rpc: GroupRpc,
    ) -> Result<GroupRpcReply, io::Error> {
        if !Self::is_heartbeat(&rpc) {
            return Err(invalid_data("only heartbeat RPCs may be coalesced"));
        }
        let (tx, rx) = oneshot::channel();
        if !self.enqueue(addr, rpc, Some(tx)) {
            return Err(invalid_data("raft heartbeat coalescer is full"));
        }
        match rx.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(error)) => Err(io::Error::new(io::ErrorKind::Other, error)),
            Err(_) => {
                // A standalone factory may not have a flush worker.  Keep this
                // branch fail-closed rather than silently sending a second frame;
                // production factories always install the worker in MultiRaft.
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "raft heartbeat coalescer stopped",
                ))
            }
        }
    }

    /// Run the bounded coalescing worker used by a live [`super::multi::MultiRaft`].
    /// Each wake gets one short window, then every peer is drained into at most one
    /// bounded batch and sent through the shared [`PeerPool`].
    pub(crate) async fn run(self: Arc<Self>, pool: Arc<PeerPool>) {
        loop {
            self.wake.notified().await;
            if self.stopping.load(Ordering::Acquire) {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(HEARTBEAT_COALESCE_WINDOW) => {}
                _ = self.wake.notified() => {}
            }
            if self.stopping.load(Ordering::Acquire) {
                break;
            }
            self.flush_pending(&pool).await;
        }
        self.fail_pending("raft heartbeat coalescer stopped");
    }

    /// Stop the worker and release every caller waiting on a queued heartbeat.
    pub(crate) fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        self.wake.notify_waiters();
        self.fail_pending("raft heartbeat coalescer stopped");
    }

    fn take_pending(&self) -> Vec<(String, Vec<PendingHeartbeat>)> {
        let mut pending = self.pending.lock().unwrap();
        pending.drain().collect()
    }

    fn drain_pending(&self) -> Vec<(String, Vec<PendingHeartbeat>)> {
        let drained = self.take_pending();
        let folded: u64 = drained.iter().map(|(_, v)| v.len() as u64).sum();
        if folded > 0 {
            self.coalesced.fetch_add(folded, Ordering::Relaxed);
            self.flushes.fetch_add(1, Ordering::Relaxed);
        }
        drained
    }

    async fn flush_pending(&self, pool: &PeerPool) {
        // Flush peers concurrently: one unavailable destination must not hold the
        // heartbeat cadence of every other peer behind the transport timeout.
        let jobs = self
            .drain_pending()
            .into_iter()
            .map(|(addr, pending)| async move {
                let batch: Vec<GroupRpc> = pending.iter().map(|item| item.rpc.clone()).collect();
                let result = Self::send_batch(pool, &addr, batch).await;
                match result {
                    Ok(replies) if replies.len() == pending.len() => {
                        for (item, reply) in pending.into_iter().zip(replies) {
                            if let Some(done) = item.completion {
                                let _ = done.send(Ok(reply));
                            }
                        }
                    }
                    Ok(replies) => {
                        let error = format!(
                            "raft heartbeat batch reply count mismatch: expected {}, got {}",
                            pending.len(),
                            replies.len()
                        );
                        for item in pending {
                            if let Some(done) = item.completion {
                                let _ = done.send(Err(error.clone()));
                            }
                        }
                    }
                    Err(error) => {
                        let error = format!("raft heartbeat batch failed: {error}");
                        for item in pending {
                            if let Some(done) = item.completion {
                                let _ = done.send(Err(error.clone()));
                            }
                        }
                    }
                }
            });
        futures::future::join_all(jobs).await;
    }

    fn fail_pending(&self, error: &str) {
        // These requests never reached a peer, so shutdown/failure cleanup must
        // not report them as emitted/coalesced frames in the live metrics.
        for (_, pending) in self.take_pending() {
            for item in pending {
                if let Some(done) = item.completion {
                    let _ = done.send(Err(error.to_string()));
                }
            }
        }
    }

    /// Drain every buffered peer into one batch per peer (CONCEPT:EG-KG.storage.concept-2).
    pub fn drain_batches(&self) -> Vec<(String, Vec<GroupRpc>)> {
        self.drain_pending()
            .into_iter()
            .map(|(addr, pending)| {
                (
                    addr,
                    pending.into_iter().map(|item| item.rpc).collect::<Vec<_>>(),
                )
            })
            .collect()
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
    /// ORDERED per-RPC replies (CONCEPT:EG-KG.storage.concept-2).
    pub(crate) async fn send_batch(
        pool: &PeerPool,
        addr: &str,
        batch: Vec<GroupRpc>,
    ) -> Result<Vec<GroupRpcReply>, io::Error> {
        if batch.is_empty()
            || batch.len() > MAX_RAFT_BATCH_RPCS
            || batch.iter().any(|rpc| !Self::is_heartbeat(rpc))
        {
            return Err(invalid_data("invalid raft heartbeat batch"));
        }
        let expected = batch.len();
        let body = rmp_serde::to_vec_named(&RaftFrame::Batch(batch))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let resp = pool.round_trip(addr, &body).await?;
        match decode_wire::<RaftFrameReply>(&resp)? {
            RaftFrameReply::Batch(replies)
                if replies.len() <= MAX_RAFT_BATCH_RPCS && replies.len() == expected =>
            {
                Ok(replies)
            }
            RaftFrameReply::One(reply) if expected == 1 => Ok(vec![reply]),
            RaftFrameReply::Batch(replies) if replies.len() <= MAX_RAFT_BATCH_RPCS => Err(
                invalid_data("raft reply batch does not match request batch"),
            ),
            RaftFrameReply::Batch(_) => Err(invalid_data("raft reply batch exceeds limits")),
            RaftFrameReply::One(_) => Err(invalid_data("raft reply is not a batch")),
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
    /// Shared per-peer connection pool (CONCEPT:AU-KG.ontology.manage-arbitrary).
    pool: Arc<PeerPool>,
    /// Installed only by a live MultiRaft manager.  The legacy constructor keeps
    /// standalone callers on the exact single-RPC behavior.
    coalescer: Option<Arc<HeartbeatCoalescer>>,
}

impl GroupNetworkFactory {
    pub fn new(gid: GroupId, local: NodeId, pool: Arc<PeerPool>) -> Self {
        Self {
            gid,
            local,
            pool,
            coalescer: None,
        }
    }

    pub(crate) fn with_coalescer(
        gid: GroupId,
        local: NodeId,
        pool: Arc<PeerPool>,
        coalescer: Arc<HeartbeatCoalescer>,
    ) -> Self {
        Self {
            gid,
            local,
            pool,
            coalescer: Some(coalescer),
        }
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
            coalescer: self.coalescer.clone(),
        }
    }
}

/// A client to ONE peer for ONE group. Reuses a pooled connection per round-trip
/// (CONCEPT:AU-KG.ontology.manage-arbitrary); tags each frame with `gid`.
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
    coalescer: Option<Arc<HeartbeatCoalescer>>,
}

impl GroupNetworkClient {
    async fn round_trip(&self, rpc: GroupRpc) -> Result<GroupRpcReply, io::Error> {
        // ── harness fault-injection: partition gate (CONCEPT:AU-KG.ontology.emits-database-ontology-entities) ──
        #[cfg(any(test, feature = "harness"))]
        if !partition::reachable(self.local, self.target) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "partitioned (harness nemesis)",
            ));
        }
        // A per-group RPC goes out as a single-RPC frame (CONCEPT:EG-KG.storage.concept-2); coalesced
        // heartbeats use `RaftFrame::Batch` via `HeartbeatCoalescer::send_batch`.
        let body = rmp_serde::to_vec_named(&RaftFrame::One(Box::new(rpc)))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let resp = self.pool.round_trip(&self.addr, &body).await?;
        match decode_wire::<RaftFrameReply>(&resp)? {
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
        let group_rpc = GroupRpc::Append(self.gid, rpc);
        let result = if let Some(coalescer) = &self.coalescer {
            if HeartbeatCoalescer::is_heartbeat(&group_rpc) {
                coalescer.heartbeat_round_trip(&self.addr, group_rpc).await
            } else {
                self.round_trip(group_rpc).await
            }
        } else {
            self.round_trip(group_rpc).await
        };
        match result {
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
        if data.len() > MAX_RAFT_FRAME_BYTES.saturating_sub(1024 * 1024) {
            return Err(StreamingError::Network(NetworkError::new(&StrErr(
                "raft snapshot exceeds the wire limit".to_string(),
            ))));
        }
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

    /// Forward a leader-transfer notification to the target (CONCEPT:AU-KG.backend.authority-has-already-acked). This
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
pub(crate) async fn dispatch_group(
    raft: Option<EgRaft>,
    gid: GroupId,
    rpc: GroupRpc,
    read_service: &super::xread::ReadPageService,
) -> GroupRpcReply {
    match raft {
        None => match rpc {
            GroupRpc::Append(..) => GroupRpcReply::Append(Err(format!("no group {gid} here"))),
            GroupRpc::Vote(..) => GroupRpcReply::Vote(Err(format!("no group {gid} here"))),
            GroupRpc::Snapshot(..) => GroupRpcReply::Snapshot(Err(format!("no group {gid} here"))),
            GroupRpc::TransferLeader(..) => {
                GroupRpcReply::TransferLeader(Err(format!("no group {gid} here")))
            }
            GroupRpc::ClientWrite(..) => {
                GroupRpcReply::ClientWrite(Box::new(Err(format!("no group {gid} here"))))
            }
            GroupRpc::ReadBarrier(..) => GroupRpcReply::ReadBarrier(Err(
                super::xread::ReadPageError::new(super::xread::ReadPageErrorCode::GroupUnavailable),
            )),
            GroupRpc::ReadPage(..) => GroupRpcReply::ReadPage(Err(
                super::xread::ReadPageError::new(super::xread::ReadPageErrorCode::GroupUnavailable),
            )),
        },
        Some(raft) => match rpc {
            GroupRpc::Append(_, req) => {
                GroupRpcReply::Append(raft.append_entries(req).await.map_err(|e| e.to_string()))
            }
            GroupRpc::Vote(_, req) => {
                GroupRpcReply::Vote(raft.vote(req).await.map_err(|e| e.to_string()))
            }
            GroupRpc::Snapshot(_, vote, meta, data) => {
                if data.len() > MAX_RAFT_FRAME_BYTES.saturating_sub(1024 * 1024) {
                    return GroupRpcReply::Snapshot(Err(
                        "raft snapshot exceeds the wire limit".to_string()
                    ));
                }
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
            GroupRpc::ClientWrite(_, req) => {
                let reply = match req.validate() {
                    Ok(()) => raft
                        .client_write(req)
                        .await
                        .map(|response| response.data)
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error),
                };
                GroupRpcReply::ClientWrite(Box::new(reply))
            }
            GroupRpc::ReadBarrier(_) => {
                GroupRpcReply::ReadBarrier(super::xread::linearizable_barrier(&raft).await)
            }
            GroupRpc::ReadPage(_, request) => {
                GroupRpcReply::ReadPage(read_service.read_page(raft, gid, request).await)
            }
        },
    }
}

/// Forward one engine-internal client write over the authenticated Raft peer
/// channel. The caller resolves the leader and address from committed membership;
/// a stale leader answer fails closed and is retried by the outer transaction.
pub(crate) async fn forward_client_write(
    pool: &PeerPool,
    addr: &str,
    group_id: GroupId,
    request: super::RaftRequest,
) -> Result<super::RaftResponse, String> {
    request.validate()?;
    let body = rmp_serde::to_vec_named(&RaftFrame::One(Box::new(GroupRpc::ClientWrite(
        group_id, request,
    ))))
    .map_err(|_| "unable to encode internal Raft client write".to_string())?;
    let response = pool
        .round_trip(addr, &body)
        .await
        .map_err(|error| format!("internal Raft client write transport failed: {error}"))?;
    match decode_wire::<RaftFrameReply>(&response)
        .map_err(|error| format!("internal Raft client write reply is invalid: {error}"))?
    {
        RaftFrameReply::One(GroupRpcReply::ClientWrite(reply)) => match *reply {
            Ok(response) => {
                response.validate()?;
                Ok(response)
            }
            Err(error) => Err(error),
        },
        _ => Err("internal Raft client write returned an unexpected reply".to_string()),
    }
}

/// Forward a per-group linearizable barrier over the authenticated Raft channel.
pub(crate) async fn forward_read_barrier(
    pool: &PeerPool,
    addr: &str,
    group_id: GroupId,
) -> Result<u64, super::xread::ReadPageError> {
    let body = rmp_serde::to_vec_named(&RaftFrame::One(Box::new(GroupRpc::ReadBarrier(group_id))))
        .map_err(|_| {
            super::xread::ReadPageError::new(super::xread::ReadPageErrorCode::InvalidRequest)
        })?;
    let response = pool.round_trip(addr, &body).await.map_err(|_| {
        super::xread::ReadPageError::new(super::xread::ReadPageErrorCode::TransportFailed)
    })?;
    match decode_wire::<RaftFrameReply>(&response) {
        Ok(RaftFrameReply::One(GroupRpcReply::ReadBarrier(result))) => result,
        _ => Err(super::xread::ReadPageError::new(
            super::xread::ReadPageErrorCode::InvalidResponse,
        )),
    }
}

/// Forward a bounded graph page over the authenticated Raft channel.
pub(crate) async fn forward_read_page(
    pool: &PeerPool,
    addr: &str,
    group_id: GroupId,
    request: super::xread::ReadPageRequest,
) -> Result<super::xread::ReadPageReply, super::xread::ReadPageError> {
    request.validate(group_id)?;
    let expected = request.clone();
    let body = rmp_serde::to_vec_named(&RaftFrame::One(Box::new(GroupRpc::ReadPage(
        group_id, request,
    ))))
    .map_err(|_| {
        super::xread::ReadPageError::new(super::xread::ReadPageErrorCode::InvalidRequest)
    })?;
    let response = pool.round_trip(addr, &body).await.map_err(|_| {
        super::xread::ReadPageError::new(super::xread::ReadPageErrorCode::TransportFailed)
    })?;
    match decode_wire::<RaftFrameReply>(&response) {
        Ok(RaftFrameReply::One(GroupRpcReply::ReadPage(Ok(reply)))) => {
            reply.validate_for(&expected, group_id)?;
            Ok(reply)
        }
        Ok(RaftFrameReply::One(GroupRpcReply::ReadPage(Err(error)))) => Err(error),
        _ => Err(super::xread::ReadPageError::new(
            super::xread::ReadPageErrorCode::InvalidResponse,
        )),
    }
}

// ── outer framing: 4-byte big-endian length prefix + opaque body ─────────
//
// In production the opaque body is an authenticated ciphertext emitted by
// `SecureRaftConnection`; plaintext framing exists only for one-member loopback mode
// and the in-process fault-injection harnesses.

pub(crate) struct RaftPayload {
    bytes: Vec<u8>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl RaftPayload {
    fn into_vec(self) -> Vec<u8> {
        self.bytes
    }
}

impl std::ops::Deref for RaftPayload {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

struct GuardedFrame {
    bytes: Vec<u8>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl GuardedFrame {
    fn into_payload(self) -> RaftPayload {
        RaftPayload {
            bytes: self.bytes,
            _permit: self._permit,
        }
    }
}

pub async fn write_frame(stream: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    if body.is_empty() || body.len() > MAX_RAFT_FRAME_BYTES {
        return Err(invalid_data("invalid raft frame length"));
    }
    let len = u32::try_from(body.len())
        .map_err(|_| invalid_data("invalid raft frame length"))?
        .to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

pub async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    read_frame_guarded(stream, None)
        .await
        .map(|frame| frame.bytes)
}

async fn read_frame_guarded(
    stream: &mut TcpStream,
    budget: Option<&Arc<tokio::sync::Semaphore>>,
) -> io::Result<GuardedFrame> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    // Bound the frame the same way the engine transport does (defensive cap).
    if len == 0 || len > MAX_RAFT_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raft frame too large",
        ));
    }
    let permit = match budget {
        Some(budget) => {
            let units = len
                .checked_add(RAFT_FRAME_BUDGET_UNIT_BYTES - 1)
                .ok_or_else(|| invalid_data("invalid raft frame length"))?
                / RAFT_FRAME_BUDGET_UNIT_BYTES;
            Some(
                budget
                    .clone()
                    .acquire_many_owned(
                        u32::try_from(units)
                            .map_err(|_| invalid_data("invalid raft frame length"))?,
                    )
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "raft frame budget closed")
                    })?,
            )
        }
        None => None,
    };
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(GuardedFrame {
        bytes: buf,
        _permit: permit,
    })
}

#[cfg(test)]
mod wire_limit_tests {
    use super::*;

    #[test]
    fn rejects_declared_messagepack_allocation_bomb_before_decode() {
        let array32_bomb = [0xdd, 0xff, 0xff, 0xff, 0xff];
        assert!(decode_wire::<RaftFrame>(&array32_bomb).is_err());
    }

    #[tokio::test]
    async fn authenticated_encrypted_connection_round_trips() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let key = [0x5au8; 32];
        let server_auth = RaftTransportAuth::new(
            2,
            &key,
            [(1, "127.0.0.1:1".to_string()), (2, address.clone())],
        )
        .unwrap();
        let client_auth = RaftTransportAuth::new(
            1,
            &key,
            [(1, "127.0.0.1:1".to_string()), (2, address.clone())],
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut secure = SecureRaftConnection::accept(stream, &server_auth, None)
                .await
                .unwrap();
            assert_eq!(&*secure.read_payload().await.unwrap(), b"request");
            secure.write_payload(b"response").await.unwrap();
        });
        let stream = TcpStream::connect(&address).await.unwrap();
        let mut secure = SecureRaftConnection::connect(stream, &client_auth, 2)
            .await
            .unwrap();
        secure.write_payload(b"request").await.unwrap();
        assert_eq!(&*secure.read_payload().await.unwrap(), b"response");
        server.await.unwrap();
    }
}

// ── harness partition gate (CONCEPT:AU-KG.ontology.emits-database-ontology-entities) ─────────────────────────────────
//
// A process-global, test/harness-only controller that the per-RPC `round_trip`
// consults to decide whether a frame from node `from` may reach node `to`. The model
// is "islands": every node sits in an island id (default 0 — fully connected), and
// two nodes can exchange RPCs iff they share an island. Compiled out of every
// production build.
#[cfg(any(test, feature = "harness"))]
pub mod partition {
    use std::collections::HashMap;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    use super::NodeId;

    /// node id → island id. Absent ⇒ island 0 (the fully-connected default).
    fn table() -> &'static Mutex<HashMap<NodeId, u64>> {
        static T: OnceLock<Mutex<HashMap<NodeId, u64>>> = OnceLock::new();
        T.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Serialize harnesses that use the process-global partition table. A test
    /// must hold this guard for the complete lifetime of its cluster, including
    /// teardown, because a concurrent `heal()` would otherwise affect another
    /// test's in-flight Raft RPCs.
    pub struct TestGuard {
        _lock: MutexGuard<'static, ()>,
    }

    /// Acquire exclusive ownership of the process-global partition table.
    pub fn test_guard() -> TestGuard {
        static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let lock = TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        heal();
        TestGuard { _lock: lock }
    }

    impl Drop for TestGuard {
        fn drop(&mut self) {
            heal();
        }
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
