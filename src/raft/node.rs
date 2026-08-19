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

async fn shutdown_after_start_error<T>(
    multi: &Arc<MultiRaft>,
    result: Result<T, String>,
) -> Result<T, String> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            multi.shutdown().await;
            Err(error)
        }
    }
}

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
    let result = multi
        .create_group(DEFAULT_GROUP, cfg.peers.clone(), cfg.is_bootstrap)
        .await;
    shutdown_after_start_error(&multi, result).await?;

    // DIST-P2-2: multi-group production startup. `cfg.groups <= 1` (the default —
    // `EPISTEMIC_GRAPH_RAFT_GROUPS` unset) makes this call a documented no-op (see
    // `MultiRaft::configure_group_ring`): the ring stays empty and every graph falls
    // back to `DEFAULT_GROUP`, so a single-group deployment is BYTE-FOR-BYTE unchanged.
    // `cfg.groups > 1` stands up the additional groups and sets the ring, so the
    // router (consulted only when the PlacementCatalog has no explicit entry for a
    // graph's tenant) spreads un-pinned graphs across all of them.
    let result = multi
        .configure_group_ring(cfg.groups, &cfg.peers, cfg.is_bootstrap)
        .await;
    shutdown_after_start_error(&multi, result).await?;

    // Crash-safe online-move recovery is driven by the placement-group leader only.
    // Every other replica has the same journal, but must not race a second driver.
    // Keep all temporary Raft handles inside this scope so they are dropped before
    // startup-error cleanup drains the manager and stops persistence.
    let recovery_result: Result<(), String> = async {
        let pending_moves = !multi
            .placement()
            .validate_move_recovery_state()
            .await?
            .is_empty();
        if !pending_moves {
            return Ok(());
        }
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
        Ok(())
    }
    .await;
    shutdown_after_start_error(&multi, recovery_result).await?;

    let handle = shutdown_after_start_error(
        &multi,
        multi
            .handle_for_graph("__commons__")
            .await
            .ok_or_else(|| "default group not running after create".to_string()),
    )
    .await?
    .handle;

    // ADR-1 / W1.1 (CONCEPT:EG-KG.sharding.cluster-topology, `reports/wave1/ADR-scale-trio.md` §ADR-1):
    // self-report this node's identity into the durable, Raft-replicated
    // cluster-topology store so `Method::ClusterMembers`/`PlacementRoute.endpoints`
    // can discover it -- replacing the static `GRAPH_RAFT_GROUP_ENDPOINTS` client
    // map. SPAWNED, not awaited inline: immediately after `create_group`/
    // `join_group` this node (especially a fresh bootstrap node awaiting its
    // peers, or a follower that hasn't yet observed real Raft traffic) commonly
    // has no leader to commit through yet, and `node::start` itself must not
    // block on that settling -- a multi-node deployment starts each node as an
    // INDEPENDENT process, so blocking here would only cost latency, but an
    // in-process multi-node harness starting nodes sequentially would
    // deadlock/stall for the full retry budget on every node. Non-fatal on
    // exhaustion -- topology discovery is a best-effort convenience (the
    // client's static-map override / single-contact fallback still works,
    // ADR-1 decision 3b/3c), never a reason to fail this node's own startup.
    // The handle stays on `StartedNode` so harnesses can cancel it before
    // releasing the persistence backend during an in-process shutdown.
    let topology_report = {
        let raft_addr = cfg
            .peers
            .get(&cfg.node_id)
            .map(|node| node.addr.clone())
            .unwrap_or_else(|| cfg.bind_addr.clone());
        let cluster_id = cfg.cluster_id.clone();
        let node_id = cfg.node_id;
        let member_identity = crate::server::persistence::node_info_store::member_identity_for(
            &cluster_id,
            node_id,
        );
        let advertised_client_addr = cfg.advertised_client_addr.clone();
        let advertised_tls_server_name = cfg.advertised_tls_server_name.clone();
        let advertised_certificate_id = cfg.advertised_certificate_id.clone();
        let advertised_certificate_rotation_epoch = cfg.advertised_certificate_rotation_epoch;
        let advertised_certificate_not_before_ms = cfg.advertised_certificate_not_before_ms;
        let advertised_certificate_not_after_ms = cfg.advertised_certificate_not_after_ms;
        let multi_for_report = multi.clone();
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                match multi_for_report
                    .commit_node_info(
                        cluster_id.clone(),
                        node_id,
                        member_identity.clone(),
                        raft_addr.clone(),
                        advertised_client_addr.clone(),
                        advertised_tls_server_name.clone(),
                        advertised_certificate_id.clone(),
                        advertised_certificate_rotation_epoch,
                        advertised_certificate_not_before_ms,
                        advertised_certificate_not_after_ms,
                    )
                    .await
                {
                    Ok(()) => {
                        tracing::info!(
                            node_id,
                            "self-reported cluster-topology node info (ADR-1 / W1.1)"
                        );
                        break;
                    }
                    Err(error) if tokio::time::Instant::now() < deadline => {
                        tracing::debug!(
                            node_id,
                            %error,
                            "cluster-topology self-report not yet accepted, retrying"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                    Err(error) => {
                        tracing::warn!(
                            node_id,
                            %error,
                            "cluster-topology self-report failed after retrying; this node \
                             will be undiscoverable via ClusterMembers/PlacementRoute.endpoints \
                             until a later successful report"
                        );
                        break;
                    }
                }
            }
        })
    };

    tracing::info!(
        "Raft node {} started ({} peers, bootstrap={}, group {}, {} group(s) total)",
        cfg.node_id,
        cfg.peers.len(),
        cfg.is_bootstrap,
        DEFAULT_GROUP,
        cfg.groups.max(1),
    );

    // Publish the routing handle and the durable placement catalog together at
    // the process-owned construction seam.  There is no served interval in
    // which a configured Raft node can be mistaken for a single-node process
    // merely because `multi_raft` has not been copied into `ServerState` yet.
    state
        .write()
        .await
        .install_multi_raft_placement_authority(Some(handle.clone()), multi.clone());

    Ok(StartedNode {
        handle,
        multi,
        topology_report: Some(topology_report),
    })
}

/// A started Raft node: the [`RaftHandle`] for routing writes + the [`MultiRaft`]
/// manager (owns the shared listener + group map).
pub struct StartedNode {
    pub handle: RaftHandle,
    pub multi: Arc<MultiRaft>,
    topology_report: Option<tokio::task::JoinHandle<()>>,
}

impl StartedNode {
    /// Cancel and join the node-owned topology self-report task before dropping
    /// the node's persistence/backend handles. Idempotent: after the first call
    /// there is no task left to cancel.
    pub async fn stop_background_tasks(&mut self) {
        if let Some(task) = self.topology_report.take() {
            task.abort();
            let _ = task.await;
        }
    }
}
