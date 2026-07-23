//! Raft node lifecycle (CONCEPT:AU-KG.ingest.source-sync-canonical + KG-2.205): build the [`MultiRaft`]
//! manager (shared listener + group map), open the DEFAULT group for this cluster
//! member, and return a [`RaftHandle`] the dispatch path routes writes through.
//!
//! Single-group by default: with `cfg.groups <= 1` (no `EPISTEMIC_GRAPH_RAFT_GROUPS`,
//! DIST-P2-2) the cluster runs ONE group ([`DEFAULT_GROUP`]) so behavior matches the
//! pre-multi single-group path — but it now runs over the production multi-group
//! machinery (one shared listener, durable redb log keyed by group id).
//!
//! ## Multi-group production startup (DIST-P2-2, CONCEPT:EG-KG.sharding.placement-catalog)
//!
//! With `cfg.groups > 1`, `start` additionally stands up groups `1..groups` on this
//! node and spreads un-pinned graphs across the FULL `0..groups` set via the
//! tenant-range ring — [`MultiRaft::configure_group_ring`] (DIST-P2-1's pre-existing
//! multi-group machinery, previously only exercised by tests, now invoked from
//! PRODUCTION startup). The [`super::placement::PlacementCatalog`] still takes
//! priority over the ring for any graph with an explicit placement entry (assigned via
//! the `placement_*` admin API), so an operator who wants EXPLICIT tenant→group control
//! rather than hash-spread sets `groups` to size the pool and then calls
//! `placement_assign`/`placement_split` to pin tenants — the ring only ever catches the
//! un-pinned remainder.
//!
//! Every group starts with the complete configured peer set. Non-default groups
//! therefore have the same quorum replication, leader failover, authenticated remote
//! read routing, and membership contract as the default group.

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
///   meta are keyed by the group id in the authoritative shard — CONCEPT:EG-KG.storage.one-fsync-covers-raft).
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
        router: None,
    };

    let multi = MultiRaft::start_configured(
        cfg.node_id,
        cfg.bind_addr.clone(),
        &cfg.peers,
        cfg.transport_secret.as_ref(),
        backend.clone(),
        ctx,
    )
    .await?;
    multi
        .create_group(DEFAULT_GROUP, cfg.peers.clone(), cfg.is_bootstrap)
        .await?;

    // DIST-P2-2: multi-group production startup. `cfg.groups <= 1` (the default —
    // `EPISTEMIC_GRAPH_RAFT_GROUPS` unset) makes this call a documented no-op (see
    // `MultiRaft::configure_group_ring`): the ring stays empty and every graph falls
    // back to `DEFAULT_GROUP`, so a single-group deployment is BYTE-FOR-BYTE unchanged.
    // `cfg.groups > 1` stands up the additional groups and sets the ring, so the
    // router (consulted only when the PlacementCatalog has no explicit entry for a
    // graph's tenant) spreads un-pinned graphs across all of them.
    multi
        .configure_group_ring(cfg.groups, &cfg.peers, cfg.is_bootstrap)
        .await?;

    // Crash-safe online-move recovery is driven by the placement-group leader only.
    // Every other replica has the same journal, but must not race a second driver.
    let pending_moves = !multi
        .placement()
        .validate_move_recovery_state()
        .await?
        .is_empty();
    if pending_moves {
        let control = multi
            .group(DEFAULT_GROUP)
            .await
            .ok_or_else(|| "placement control group is unavailable during recovery".to_string())?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let leader = loop {
            if let Some(leader) = control.current_leader().await {
                break leader;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(
                    "placement control group has no leader during move recovery".to_string()
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        };
        if leader == cfg.node_id {
            let manager = super::reshard::TenantManager::new(multi.clone(), backend.clone());
            manager.reconcile_moves().await?;
        }
    }

    let handle = multi
        .handle_for_graph("__commons__")
        .await
        .ok_or_else(|| "default group not running after create".to_string())?
        .handle;

    tracing::info!(
        "Raft node {} started ({} peers, bootstrap={}, group {}, {} group(s) total)",
        cfg.node_id,
        cfg.peers.len(),
        cfg.is_bootstrap,
        DEFAULT_GROUP,
        cfg.groups.max(1),
    );

    Ok(StartedNode { handle, multi })
}

/// A started Raft node: the [`RaftHandle`] for routing writes + the [`MultiRaft`]
/// manager (owns the shared listener + group map).
pub struct StartedNode {
    pub handle: RaftHandle,
    pub multi: Arc<MultiRaft>,
}
