//! Raft node lifecycle (CONCEPT:KG-2.188 + KG-2.205): build the [`MultiRaft`]
//! manager (shared listener + group map), open the DEFAULT group for this cluster
//! member, and return a [`RaftHandle`] the dispatch path routes writes through.
//!
//! Single-group today: the cluster runs ONE group ([`DEFAULT_GROUP`]) so behavior
//! matches the pre-multi single-group path — but it now runs over the production
//! multi-group machinery (one shared listener, durable redb log keyed by group id),
//! so adding more groups later is a routing change, not a rewrite.

use std::sync::Arc;

use tokio::sync::RwLock;

use super::config::RaftClusterConfig;
use super::multi::MultiRaft;
use super::{AppCtx, RaftHandle, DEFAULT_GROUP};
use crate::server::ServerState;

/// Build + start the Raft node for this cluster member.
///
/// * Builds the [`MultiRaft`] manager and its single shared RPC listener.
/// * Opens the DEFAULT group over the shared M2 redb backend (its durable log +
///   meta are keyed by the group id in `graph.redb` — CONCEPT:KG-2.204).
/// * On the bootstrap node (lowest id) the group `initialize`s the cluster.
///
/// Returns a [`StartedNode`]: the [`RaftHandle`] for routing writes + the
/// [`MultiRaft`] manager (so a controlled shutdown / the failover test can stop it).
pub async fn start(
    cfg: RaftClusterConfig,
    state: Arc<RwLock<ServerState>>,
) -> Result<StartedNode, String> {
    let backend = {
        let s = state.read().await;
        s.persistence
            .clone()
            .ok_or_else(|| "raft requires a configured persistence backend".to_string())?
    };
    let ctx = AppCtx {
        state: state.clone(),
    };

    let multi = MultiRaft::start(cfg.node_id, cfg.bind_addr.clone(), backend, ctx).await?;
    multi
        .create_group(DEFAULT_GROUP, cfg.peers.clone(), cfg.is_bootstrap)
        .await?;

    let handle = multi
        .handle_for_graph("__commons__")
        .await
        .ok_or_else(|| "default group not running after create".to_string())?;

    tracing::info!(
        "Raft node {} started ({} peers, bootstrap={}, group {})",
        cfg.node_id,
        cfg.peers.len(),
        cfg.is_bootstrap,
        DEFAULT_GROUP,
    );

    Ok(StartedNode { handle, multi })
}

/// A started Raft node: the [`RaftHandle`] for routing writes + the [`MultiRaft`]
/// manager (owns the shared listener + group map).
pub struct StartedNode {
    pub handle: RaftHandle,
    pub multi: Arc<MultiRaft>,
}
