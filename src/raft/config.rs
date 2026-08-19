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
/// ADR-1 / W1.1 — this node's client-reachable address, self-reported into the
/// durable cluster-topology store (`NodeInfoUpsert`) and handed back by
/// `Method::ClusterMembers`/`PlacementRoute.endpoints`. Required once Raft peers
/// are configured (config-contract style, like the transport secret below):
/// without it, a discovering client would have no address to learn for THIS
/// node beyond its own seed contact.
const ADVERTISED_CLIENT_ADDR_ENV: &str = "EPISTEMIC_GRAPH_ADVERTISED_CLIENT_ADDR";
/// Optional TLS server name (SNI / certificate hostname) a client should verify
/// when connecting to `ADVERTISED_CLIENT_ADDR_ENV` over `tls://`. Unset ⇒ the
/// client verifies against the address's own host (the TLS default) — zero
/// friction for a deployment that doesn't need SNI override.
const ADVERTISED_TLS_SERVER_NAME_ENV: &str = "EPISTEMIC_GRAPH_ADVERTISED_TLS_SERVER_NAME";
const MAX_ADVERTISED_FIELD_BYTES: usize = 1_024;

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
    /// This node's client-reachable address (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1),
    /// self-reported into the durable cluster-topology store at startup. Required
    /// (`from_env` fails closed without it — config-contract style, like the
    /// transport secret below) whenever Raft peers are configured.
    pub advertised_client_addr: String,
    /// Optional TLS server name (SNI / cert hostname) a client should verify when
    /// connecting to `advertised_client_addr` over `tls://` (CONCEPT:EG-KG.sharding.cluster-topology).
    pub advertised_tls_server_name: Option<String>,
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
            .field("advertised_client_addr", &"<redacted>")
            .field(
                "advertised_tls_server_name",
                &self
                    .advertised_tls_server_name
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
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
        // DIST-P2-2 / ADR-2 W1.2: multi-group production startup. Absent ⇒ the
        // cores-derived default (K == N groups, write-sharded like the non-raft path);
        // set explicitly to size the pool. Clamped to MAX_SHARD_COUNT (K's ceiling).
        let groups = parse_groups(
            std::env::var("EPISTEMIC_GRAPH_RAFT_GROUPS").ok(),
            default_raft_group_count(),
        )?;
        let transport_secret = resolve_transport_secret()?;
        let loopback_only = peers.len() == 1
            && is_loopback_endpoint(&bind_addr)
            && peers.values().all(|peer| is_loopback_endpoint(&peer.addr));
        if transport_secret.is_none() && !loopback_only {
            return Err(format!(
                "Raft transport key is required for multi-member or non-loopback clusters; set {RAFT_AUTH_SECRET_FILE_ENV} (preferred) or {RAFT_AUTH_SECRET_ENV}"
            ));
        }
        // ADR-1 / W1.1: config-contract style -- refuse to start clustered
        // without a client-reachable address to self-report, exactly like the
        // transport secret check above. A raft build with peers configured but
        // no discoverable client address would silently strand
        // `ClusterMembers`/`PlacementRoute.endpoints` for this node forever.
        let advertised_client_addr = parse_advertised_client_addr()?;
        let advertised_tls_server_name = parse_advertised_tls_server_name()?;
        Ok(Some(Self {
            node_id,
            peers,
            bind_addr,
            advertised_client_addr,
            advertised_tls_server_name,
            is_bootstrap,
            groups,
            transport_secret,
        }))
    }
}

/// Parse the required `EPISTEMIC_GRAPH_ADVERTISED_CLIENT_ADDR` (CONCEPT:EG-KG.sharding.cluster-topology,
/// ADR-1). Called only once `EPISTEMIC_GRAPH_RAFT_NODE_ID`/`_PEERS` are already
/// known-present (see [`RaftClusterConfig::from_env`]), so "peers configured"
/// always holds here -- fail closed rather than silently omitting this node from
/// discovery.
fn parse_advertised_client_addr() -> Result<String, String> {
    let raw = std::env::var(ADVERTISED_CLIENT_ADDR_ENV).map_err(|_| {
        format!(
            "Raft peers are configured but {ADVERTISED_CLIENT_ADDR_ENV} is missing -- set it to \
             this node's client-reachable address (e.g. 'tcp://10.0.0.1:8765') so \
             ClusterMembers/PlacementRoute can discover it"
        )
    })?;
    let addr = raw.trim();
    if addr.is_empty() || addr.len() > MAX_ADVERTISED_FIELD_BYTES {
        return Err(format!(
            "{ADVERTISED_CLIENT_ADDR_ENV} must be a non-empty address within \
             {MAX_ADVERTISED_FIELD_BYTES} bytes"
        ));
    }
    Ok(addr.to_string())
}

fn parse_advertised_tls_server_name() -> Result<Option<String>, String> {
    match std::env::var(ADVERTISED_TLS_SERVER_NAME_ENV) {
        Ok(raw) => {
            let name = raw.trim();
            if name.is_empty() {
                return Ok(None);
            }
            if name.len() > MAX_ADVERTISED_FIELD_BYTES {
                return Err(format!(
                    "{ADVERTISED_TLS_SERVER_NAME_ENV} exceeds {MAX_ADVERTISED_FIELD_BYTES} bytes"
                ));
            }
            Ok(Some(name.to_string()))
        }
        Err(_) => Ok(None),
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

/// Parse `EPISTEMIC_GRAPH_RAFT_GROUPS` (DIST-P2-2 / ADR-2 W1.2): `None`/empty/`"0"` all
/// collapse to `default` rather than erroring — an operator who never heard of this knob
/// gets the cores-derived default ([`default_raft_group_count`]). A non-empty,
/// non-integer value is a loud misconfig (same posture as `parse_peers`). A configured
/// value is clamped to `1..=MAX_SHARD_COUNT` because under raft K (redb shards) == N
/// (groups), so N can never exceed the shard ceiling ([`crate::redb_layout::MAX_SHARD_COUNT`]).
/// Pure (no env access) so it is unit tested directly without the process-global
/// env-var races a `from_env` test would need.
fn parse_groups(raw: Option<String>, default: u64) -> Result<u64, String> {
    let ceiling = crate::redb_layout::MAX_SHARD_COUNT as u64;
    // Absent / empty / an explicit `0` all mean "unspecified" ⇒ the cores-derived
    // `default` (preserving the pre-ADR-2 "None/empty/0 collapse together" semantic,
    // only the collapse target changed from a hardcoded 1 to `default`). A positive
    // value is honored, clamped to `1..=MAX_SHARD_COUNT` (K == N ≤ the shard ceiling).
    let n = match raw {
        Some(v) if !v.trim().is_empty() => v
            .trim()
            .parse::<u64>()
            .map_err(|_| "EPISTEMIC_GRAPH_RAFT_GROUPS is not an integer".to_string())?,
        _ => 0,
    };
    let resolved = if n == 0 { default } else { n };
    Ok(resolved.clamp(1, ceiling))
}

/// The cores-derived default group/shard count when `EPISTEMIC_GRAPH_RAFT_GROUPS` is
/// unset (ADR-2 / W1.2, `reports/wave1/ADR-scale-trio.md` §ADR-2 decision 2): the raft
/// group count defaults to the non-raft durable-shard auto-size
/// `clamp(effective-cgroup-cpu/2, 1, …)` but
/// with the raised [`crate::redb_layout::MAX_SHARD_COUNT`] ceiling — so turning on raft
/// (for HA) gets the SAME write-sharding a single-node deployment gets by default rather
/// than collapsing to one group. Under raft K (redb shards) == N (groups), so this is the
/// ONE default both `resolve_shard_count` and [`raft_group_count`] derive from.
pub(crate) fn default_raft_group_count() -> u64 {
    let cpus = crate::autosize::detect_capacity().reserved_cpus() as u64;
    (cpus / 2).clamp(1, crate::redb_layout::MAX_SHARD_COUNT as u64)
}

/// The number of raft groups (== redb shards K, ADR-2 / W1.2) this node runs, resolved
/// from `EPISTEMIC_GRAPH_RAFT_GROUPS` with the [`default_raft_group_count`] fallback. Read
/// by `resolve_shard_count` so the durable store opens EXACTLY N shards — group `g` owns
/// shard `g`. A malformed value falls back to the default here; the loud validation error
/// is raised once by [`RaftClusterConfig::from_env`], which gates startup, so this
/// accessor (reached only on an already-accepted raft-configured node) never needs to
/// re-report it.
pub(crate) fn raft_group_count() -> u64 {
    parse_groups(
        std::env::var("EPISTEMIC_GRAPH_RAFT_GROUPS").ok(),
        default_raft_group_count(),
    )
    .unwrap_or_else(|_| default_raft_group_count())
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
    fn parse_groups_uses_default_when_absent_or_empty_or_zero() {
        // ADR-2 / W1.2: absent/empty/0 fall back to the supplied cores-derived default
        // (not a hardcoded 1), because under raft K == N and the default is the same
        // write-sharding the non-raft path uses.
        assert_eq!(parse_groups(None, 4).unwrap(), 4);
        assert_eq!(parse_groups(Some("".into()), 4).unwrap(), 4);
        assert_eq!(parse_groups(Some("  ".into()), 4).unwrap(), 4);
        assert_eq!(parse_groups(Some("0".into()), 4).unwrap(), 4);
        // A default of 0 (impossible from the resolver, defensive) still floors to 1.
        assert_eq!(parse_groups(None, 0).unwrap(), 1);
    }

    #[test]
    fn parse_groups_accepts_n_clamps_ceiling_and_rejects_garbage() {
        assert_eq!(parse_groups(Some("4".into()), 1).unwrap(), 4);
        assert_eq!(parse_groups(Some(" 7 ".into()), 1).unwrap(), 7);
        // K == N ≤ MAX_SHARD_COUNT: an over-ceiling group count clamps rather than
        // opening more shards than the durable layout allows.
        assert_eq!(
            parse_groups(Some("9999".into()), 1).unwrap(),
            crate::redb_layout::MAX_SHARD_COUNT as u64
        );
        assert!(parse_groups(Some("nope".into()), 1).is_err());
    }

    #[test]
    fn default_raft_group_count_is_bounded() {
        // Cores-derived, always in 1..=MAX_SHARD_COUNT regardless of the host.
        let n = default_raft_group_count();
        assert!((1..=crate::redb_layout::MAX_SHARD_COUNT as u64).contains(&n));
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
