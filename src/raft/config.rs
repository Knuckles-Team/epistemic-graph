//! Raft cluster configuration parsed from the environment (CONCEPT:AU-KG.ingest.source-sync-canonical).
//!
//! Raft only ACTIVATES when built `--features raft` AND configured here. With the
//! feature on but no `EPISTEMIC_GRAPH_RAFT_NODE_ID`, [`RaftClusterConfig::from_env`]
//! returns `None` and the engine runs single-node exactly as before.

use std::collections::BTreeMap;

use openraft::BasicNode;

use super::{NodeId, PeerMap};

/// Resolved cluster configuration for THIS node.
#[derive(Debug, Clone)]
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
}

impl RaftClusterConfig {
    /// Parse `EPISTEMIC_GRAPH_RAFT_NODE_ID` + `EPISTEMIC_GRAPH_RAFT_PEERS`.
    ///
    /// * `EPISTEMIC_GRAPH_RAFT_NODE_ID` — this node's integer id. Absent ⇒ `None`
    ///   (Raft disabled, single-node).
    /// * `EPISTEMIC_GRAPH_RAFT_PEERS` — comma-separated `id@host:port` members,
    ///   e.g. `1@127.0.0.1:7001,2@127.0.0.1:7002,3@127.0.0.1:7003`. MUST include
    ///   this node's own id.
    ///
    /// Returns `Err` if the node id is set but the peer set is missing/malformed or
    /// does not contain this node — a loud misconfig rather than a silent half-up
    /// cluster.
    pub fn from_env() -> Result<Option<Self>, String> {
        let node_id = match std::env::var("EPISTEMIC_GRAPH_RAFT_NODE_ID") {
            Ok(v) if !v.trim().is_empty() => v
                .trim()
                .parse::<NodeId>()
                .map_err(|_| format!("EPISTEMIC_GRAPH_RAFT_NODE_ID='{v}' is not an integer"))?,
            _ => return Ok(None),
        };
        let peers_raw = std::env::var("EPISTEMIC_GRAPH_RAFT_PEERS").map_err(|_| {
            "EPISTEMIC_GRAPH_RAFT_NODE_ID is set but EPISTEMIC_GRAPH_RAFT_PEERS is missing \
             (need e.g. '1@host:port,2@host:port,3@host:port')"
                .to_string()
        })?;
        let peers = parse_peers(&peers_raw)?;
        let advertise_addr = peers
            .get(&node_id)
            .map(|n| n.addr.clone())
            .ok_or_else(|| {
                format!(
                    "EPISTEMIC_GRAPH_RAFT_PEERS does not contain this node's id {node_id}: '{peers_raw}'"
                )
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
        Ok(Some(Self {
            node_id,
            peers,
            bind_addr,
            is_bootstrap,
        }))
    }
}

/// Parse `id@host:port,id@host:port,…` into a peer map.
fn parse_peers(raw: &str) -> Result<PeerMap, String> {
    let mut peers: BTreeMap<NodeId, BasicNode> = BTreeMap::new();
    for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (id_s, addr) = part
            .split_once('@')
            .ok_or_else(|| format!("malformed peer '{part}' (expected 'id@host:port')"))?;
        let id = id_s
            .trim()
            .parse::<NodeId>()
            .map_err(|_| format!("malformed peer id in '{part}'"))?;
        let addr = addr.trim().to_string();
        if addr.is_empty() {
            return Err(format!("empty address in peer '{part}'"));
        }
        if peers.insert(id, BasicNode::new(addr)).is_some() {
            return Err(format!("duplicate node id {id} in peer set"));
        }
    }
    if peers.is_empty() {
        return Err("EPISTEMIC_GRAPH_RAFT_PEERS is empty".to_string());
    }
    Ok(peers)
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
}
