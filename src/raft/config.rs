//! Raft cluster configuration parsed from the environment (CONCEPT:AU-KG.ingest.source-sync-canonical).
//!
//! Raft only ACTIVATES when built `--features raft` AND configured here. With the
//! feature on but no `EPISTEMIC_GRAPH_RAFT_NODE_ID`, [`RaftClusterConfig::from_env`]
//! returns `None` and the engine runs single-node exactly as before.

use std::collections::BTreeMap;
use std::io::Read;
use std::net::SocketAddr;

use openraft::BasicNode;
use sha2::{Digest, Sha256};

use super::{NodeId, PeerMap};

const RAFT_AUTH_SECRET_ENV: &str = "EPISTEMIC_GRAPH_RAFT_AUTH_SECRET";
const RAFT_AUTH_SECRET_FILE_ENV: &str = "EPISTEMIC_GRAPH_RAFT_AUTH_SECRET_FILE";
const MIN_RAFT_AUTH_SECRET_BYTES: usize = 32;
const MAX_RAFT_AUTH_SECRET_BYTES: usize = 4 * 1024;
const MAX_RAFT_PEERS: usize = 1_024;
const MAX_RAFT_PEER_ADDRESS_BYTES: usize = 1_024;

/// Derived pre-shared key for the authenticated, encrypted Raft transport.
///
/// The raw runtime secret is reduced immediately to a fixed-size domain-separated
/// key. Debug output is deliberately redacted and the bytes are cleared on drop.
#[derive(Clone)]
pub struct RaftTransportSecret([u8; 32]);

impl RaftTransportSecret {
    pub(crate) fn from_material(material: &[u8]) -> Result<Self, String> {
        if !(MIN_RAFT_AUTH_SECRET_BYTES..=MAX_RAFT_AUTH_SECRET_BYTES).contains(&material.len()) {
            return Err(format!(
                "Raft transport secret must contain {MIN_RAFT_AUTH_SECRET_BYTES}..={MAX_RAFT_AUTH_SECRET_BYTES} bytes"
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(b"epistemic-graph/raft-transport-key/v1\0");
        hasher.update(material);
        let mut digest = hasher.finalize();
        let mut key = [0u8; 32];
        key.copy_from_slice(&digest);
        digest.fill(0);
        Ok(Self(key))
    }

    pub(crate) fn expose(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for RaftTransportSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RaftTransportSecret(<redacted>)")
    }
}

impl Drop for RaftTransportSecret {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Resolved cluster configuration for THIS node.
#[derive(Clone)]
pub struct RaftClusterConfig {
    /// This node's id.
    pub node_id: NodeId,
    /// Every member (including self): id → Raft-RPC `host:port`.
    pub peers: PeerMap,
    /// The `host:port` this node binds its Raft-RPC listener to. Defaults to this
    /// node's own PEERS entry (the advertised address), but can be overridden by
    /// `EPISTEMIC_GRAPH_RAFT_BIND_ADDR` (CONCEPT:AU-KG.backend.authority-has-already-acked) so a CONTAINERIZED member can
    /// bind `0.0.0.0:9100` locally while still ADVERTISING its routable host IP
    /// (`10.0.0.x:9100`) to peers — a container on an overlay net cannot bind the host's
    /// external IP directly. Bare-metal members leave it unset and bind their own addr.
    pub bind_addr: String,
    /// Whether this node should `initialize` the cluster on first boot (only the
    /// lowest-id node does, and only when its raft store is empty). Computed, not
    /// configured.
    pub is_bootstrap: bool,
    /// Number of Raft groups THIS node stands up at boot (DIST-P2-2, CONCEPT:EG-KG.sharding.placement-catalog).
    /// `1` — the default when `EPISTEMIC_GRAPH_RAFT_GROUPS` is unset/absent — keeps
    /// production startup creating ONLY [`super::DEFAULT_GROUP`], byte-for-byte the
    /// pre-existing single-group behavior (`node::start` calling
    /// [`super::multi::MultiRaft::configure_group_ring`] with `groups <= 1` is a
    /// documented no-op). A value `> 1` additionally stands up groups `1..groups` on
    /// this node and spreads un-pinned graphs across the full `0..groups` set via the
    /// tenant-range ring — the [`super::placement::PlacementCatalog`] still takes
    /// priority over the ring for any graph with an explicit placement entry (see
    /// [`super::multi::MultiRaft::route_graph`]).
    pub groups: u64,
    /// Runtime-resolved key for peer authentication and frame encryption. This is
    /// optional only for a one-member cluster bound and advertised exclusively on
    /// loopback; every multi-member or routable deployment fails closed without it.
    pub transport_secret: Option<RaftTransportSecret>,
}

impl std::fmt::Debug for RaftClusterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftClusterConfig")
            .field("node_id", &self.node_id)
            .field("peer_count", &self.peers.len())
            .field("bind_addr", &"<redacted>")
            .field("is_bootstrap", &self.is_bootstrap)
            .field("groups", &self.groups)
            .field("transport_secret", &self.transport_secret)
            .finish()
    }
}

impl RaftClusterConfig {
    /// Parse `EPISTEMIC_GRAPH_RAFT_NODE_ID` + `EPISTEMIC_GRAPH_RAFT_PEERS`.
    ///
    /// * `EPISTEMIC_GRAPH_RAFT_NODE_ID` — this node's integer id. Absent ⇒ `None`
    ///   (Raft disabled, single-node).
    /// * `EPISTEMIC_GRAPH_RAFT_PEERS` — comma-separated `id@host:port` members,
    ///   e.g. `1@127.0.0.1:7001,2@127.0.0.1:7002,3@127.0.0.1:7003`. MUST include
    ///   this node's own id.
    /// * `EPISTEMIC_GRAPH_RAFT_AUTH_SECRET_FILE` — preferred reference to a 32-byte
    ///   or longer pre-shared key readable only by the service account. Alternatively
    ///   `EPISTEMIC_GRAPH_RAFT_AUTH_SECRET` may carry it inline. Exactly one source
    ///   may be set. The key is mandatory outside one-member loopback mode.
    ///
    /// Returns `Err` if the node id is set but the peer set is missing/malformed or
    /// does not contain this node — a loud misconfig rather than a silent half-up
    /// cluster.
    pub fn from_env() -> Result<Option<Self>, String> {
        let node_id = match std::env::var("EPISTEMIC_GRAPH_RAFT_NODE_ID") {
            Ok(v) if !v.trim().is_empty() => v
                .trim()
                .parse::<NodeId>()
                .map_err(|_| "EPISTEMIC_GRAPH_RAFT_NODE_ID is not an integer".to_string())?,
            _ => return Ok(None),
        };
        let peers_raw = std::env::var("EPISTEMIC_GRAPH_RAFT_PEERS").map_err(|_| {
            "EPISTEMIC_GRAPH_RAFT_NODE_ID is set but EPISTEMIC_GRAPH_RAFT_PEERS is missing \
             (need e.g. '1@host:port,2@host:port,3@host:port')"
                .to_string()
        })?;
        let peers = parse_peers(&peers_raw)?;
        let advertise_addr = peers.get(&node_id).map(|n| n.addr.clone()).ok_or_else(|| {
            "EPISTEMIC_GRAPH_RAFT_PEERS does not contain this node id".to_string()
        })?;
        // The listener bind address defaults to the advertised (PEERS) address, but a
        // containerized member overrides it (e.g. `0.0.0.0:9100`) while peers still dial
        // its routable host IP from PEERS — see `bind_addr` docs (CONCEPT:AU-KG.backend.authority-has-already-acked).
        let bind_addr = match std::env::var("EPISTEMIC_GRAPH_RAFT_BIND_ADDR") {
            Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
            _ => advertise_addr,
        };
        // The lowest-id member is the bootstrap candidate (deterministic, no extra
        // env). Whether it ACTUALLY initializes is also gated on an empty store at
        // boot, decided in `node::start`.
        let is_bootstrap = peers.keys().next() == Some(&node_id);
        // DIST-P2-2: optional multi-group production startup.
        let groups = parse_groups(std::env::var("EPISTEMIC_GRAPH_RAFT_GROUPS").ok())?;
        let transport_secret = resolve_transport_secret()?;
        let loopback_only = peers.len() == 1
            && is_loopback_endpoint(&bind_addr)
            && peers.values().all(|peer| is_loopback_endpoint(&peer.addr));
        if transport_secret.is_none() && !loopback_only {
            return Err(format!(
                "Raft transport key is required for multi-member or non-loopback clusters; set {RAFT_AUTH_SECRET_FILE_ENV} (preferred) or {RAFT_AUTH_SECRET_ENV}"
            ));
        }
        Ok(Some(Self {
            node_id,
            peers,
            bind_addr,
            is_bootstrap,
            groups,
            transport_secret,
        }))
    }
}

/// Resolve Raft key material from one runtime source. A file reference is preferred
/// so the secret does not appear in a process environment dump. The path and secret
/// are never included in diagnostics.
fn resolve_transport_secret() -> Result<Option<RaftTransportSecret>, String> {
    let inline = std::env::var(RAFT_AUTH_SECRET_ENV).ok();
    let file_ref = std::env::var(RAFT_AUTH_SECRET_FILE_ENV).ok();
    if inline.is_some() && file_ref.is_some() {
        return Err(format!(
            "configure exactly one of {RAFT_AUTH_SECRET_ENV} and {RAFT_AUTH_SECRET_FILE_ENV}"
        ));
    }
    if let Some(material) = inline {
        let mut material = material.into_bytes();
        let resolved = RaftTransportSecret::from_material(&material).map(Some);
        material.fill(0);
        return resolved;
    }
    let Some(path) = file_ref else {
        return Ok(None);
    };
    if path.trim().is_empty() {
        return Err(format!("{RAFT_AUTH_SECRET_FILE_ENV} is empty"));
    }
    let file = std::fs::File::open(path)
        .map_err(|_| "unable to open the configured Raft transport secret file".to_string())?;
    let metadata = file
        .metadata()
        .map_err(|_| "unable to inspect the configured Raft transport secret file".to_string())?;
    if !metadata.is_file() || metadata.len() > (MAX_RAFT_AUTH_SECRET_BYTES + 2) as u64 {
        return Err("invalid Raft transport secret file".to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(
                "Raft transport secret file must not be accessible by group or other users"
                    .to_string(),
            );
        }
    }
    let mut material = Vec::with_capacity(metadata.len() as usize);
    file.take((MAX_RAFT_AUTH_SECRET_BYTES + 3) as u64)
        .read_to_end(&mut material)
        .map_err(|_| "unable to read the configured Raft transport secret file".to_string())?;
    while material
        .last()
        .map(|byte| matches!(*byte, b'\r' | b'\n'))
        .unwrap_or(false)
    {
        material.pop();
    }
    let resolved = RaftTransportSecret::from_material(&material).map(Some);
    material.fill(0);
    resolved
}

fn is_loopback_endpoint(endpoint: &str) -> bool {
    if let Ok(addr) = endpoint.parse::<SocketAddr>() {
        return addr.ip().is_loopback();
    }
    let host = endpoint
        .rsplit_once(':')
        .map(|(host, _)| host.trim_matches(['[', ']']))
        .unwrap_or_default();
    host.eq_ignore_ascii_case("localhost")
}

/// Parse `id@host:port,id@host:port,…` into a peer map.
fn parse_peers(raw: &str) -> Result<PeerMap, String> {
    let mut peers: BTreeMap<NodeId, BasicNode> = BTreeMap::new();
    for (entry_index, part) in raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .enumerate()
    {
        let (id_s, addr) = part.split_once('@').ok_or_else(|| {
            format!("malformed peer entry {entry_index} (expected 'id@host:port')")
        })?;
        let id = id_s
            .trim()
            .parse::<NodeId>()
            .map_err(|_| format!("malformed peer id in entry {entry_index}"))?;
        let addr = addr.trim().to_string();
        if addr.is_empty() {
            return Err(format!("empty address in peer entry {entry_index}"));
        }
        if addr.len() > MAX_RAFT_PEER_ADDRESS_BYTES {
            return Err(format!(
                "peer address exceeds {MAX_RAFT_PEER_ADDRESS_BYTES} bytes"
            ));
        }
        if peers.insert(id, BasicNode::new(addr)).is_some() {
            return Err(format!("duplicate node id {id} in peer set"));
        }
        if peers.len() > MAX_RAFT_PEERS {
            return Err(format!("Raft peer set exceeds {MAX_RAFT_PEERS} members"));
        }
    }
    if peers.is_empty() {
        return Err("EPISTEMIC_GRAPH_RAFT_PEERS is empty".to_string());
    }
    Ok(peers)
}

/// Parse `EPISTEMIC_GRAPH_RAFT_GROUPS` (DIST-P2-2): `None`/empty/`"0"` all collapse to
/// `1` (single-group, unchanged default) rather than erroring — an operator who never
/// heard of this knob gets EXACTLY today's behavior. A non-empty, non-integer value is
/// a loud misconfig (same posture as `parse_peers`). Pure (no env access) so it is unit
/// tested directly without the process-global env-var races a `from_env` test would need.
fn parse_groups(raw: Option<String>) -> Result<u64, String> {
    match raw {
        Some(v) if !v.trim().is_empty() => v
            .trim()
            .parse::<u64>()
            .map_err(|_| "EPISTEMIC_GRAPH_RAFT_GROUPS is not an integer".to_string())
            .map(|n| n.max(1)),
        _ => Ok(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_three_node_peer_set() {
        let p = parse_peers("1@127.0.0.1:7001, 2@127.0.0.1:7002 ,3@127.0.0.1:7003").unwrap();
        assert_eq!(p.len(), 3);
        assert_eq!(p.get(&2).unwrap().addr, "127.0.0.1:7002");
    }

    #[test]
    fn rejects_malformed_and_duplicate() {
        assert!(parse_peers("oops").is_err());
        assert!(parse_peers("1@a:1,1@b:2").is_err());
        assert!(parse_peers("").is_err());
    }

    #[test]
    fn parse_groups_defaults_to_one_when_absent_or_empty_or_zero() {
        assert_eq!(parse_groups(None).unwrap(), 1);
        assert_eq!(parse_groups(Some("".into())).unwrap(), 1);
        assert_eq!(parse_groups(Some("  ".into())).unwrap(), 1);
        assert_eq!(parse_groups(Some("0".into())).unwrap(), 1);
    }

    #[test]
    fn parse_groups_accepts_n_and_rejects_garbage() {
        assert_eq!(parse_groups(Some("4".into())).unwrap(), 4);
        assert_eq!(parse_groups(Some(" 7 ".into())).unwrap(), 7);
        assert!(parse_groups(Some("nope".into())).is_err());
    }

    #[test]
    fn plaintext_exception_is_strictly_loopback() {
        assert!(is_loopback_endpoint("127.0.0.1:7001"));
        assert!(is_loopback_endpoint("[::1]:7001"));
        assert!(is_loopback_endpoint("localhost:7001"));
        assert!(!is_loopback_endpoint("0.0.0.0:7001"));
        assert!(!is_loopback_endpoint("localhost.example:7001"));
    }

    #[test]
    fn transport_secret_is_redacted_and_requires_entropy_budget() {
        assert!(RaftTransportSecret::from_material(b"short").is_err());
        let secret = RaftTransportSecret::from_material(&[0x5a; 32]).unwrap();
        assert_eq!(format!("{secret:?}"), "RaftTransportSecret(<redacted>)");
    }
}
