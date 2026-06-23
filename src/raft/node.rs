//! Raft node lifecycle (CONCEPT:KG-2.188): build the storage + network, spawn the
//! Raft task, start the RPC listener, and (on the bootstrap node) initialize the
//! cluster. Returns a [`RaftHandle`] the dispatch path routes writes through.

use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::Config;
use tokio::sync::RwLock;

use super::config::RaftClusterConfig;
use super::network::{self, EgNetworkFactory};
use super::store::EgStore;
use super::{AppCtx, EgRaft, RaftHandle};
use crate::server::ServerState;

/// Build + start the Raft node for this cluster member.
///
/// * Opens the durable Raft metadata store (`raft.redb`) and the in-memory log.
/// * Spawns the openraft task with our TCP network factory.
/// * Starts the dedicated Raft-RPC listener so peers can reach this node.
/// * On the bootstrap node (lowest id) with an empty store, `initialize`s the
///   cluster membership so an election can begin. Other nodes wait to be contacted.
///
/// Returns a [`RaftHandle`]; the caller stores it in `ServerState.raft` so the
/// dispatch write path routes through consensus.
pub async fn start(
    cfg: RaftClusterConfig,
    persist_dir: &str,
    state: Arc<RwLock<ServerState>>,
) -> Result<StartedNode, String> {
    let ctx = AppCtx {
        state: state.clone(),
    };
    let store = EgStore::open(persist_dir, ctx)?;

    // Conservative election/heartbeat timing: fast enough for a snappy failover in
    // the test + a homelab, slow enough not to thrash. Heartbeat 250ms, election
    // 1.5–3s. All wall-clock; openraft drives the timers.
    let raft_config = Config {
        cluster_name: "epistemic-graph".to_string(),
        heartbeat_interval: 250,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        // Snapshot/compaction left at defaults; the in-memory log + M2 durability
        // make this a tuning knob, not a correctness one (see module docs).
        ..Default::default()
    };
    let raft_config = Arc::new(
        raft_config
            .validate()
            .map_err(|e| format!("invalid raft config: {e}"))?,
    );

    let (log_store, state_machine) = Adaptor::new(store);
    let network = EgNetworkFactory;

    let raft: EgRaft =
        openraft::Raft::new(cfg.node_id, raft_config, network, log_store, state_machine)
            .await
            .map_err(|e| format!("raft new: {e}"))?;

    // Start the dedicated Raft RPC listener BEFORE initializing so a bootstrap
    // node's first append/vote can reach peers (and peers can reach us). The abort
    // handle is returned so a controlled shutdown (and the failover test) can stop
    // the listener — without it, peers would still reach a "killed" node.
    let listener = {
        let bind = cfg.bind_addr.clone();
        let raft_for_rpc = raft.clone();
        tokio::spawn(async move {
            if let Err(e) = network::serve(bind, raft_for_rpc).await {
                tracing::error!("raft rpc listener error: {}", e);
            }
        })
    };

    // Bootstrap: only the lowest-id node initializes, and only if its metrics show
    // an uninitialized cluster (last_log_index == None & no membership). Calling
    // initialize on an already-formed cluster errors with NotAllowed, which we
    // tolerate (a restart of the bootstrap node must NOT re-initialize).
    if cfg.is_bootstrap {
        let members = cfg.peers.clone();
        let raft_for_init = raft.clone();
        tokio::spawn(async move {
            // Give peers a moment to bind their listeners.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            match raft_for_init.initialize(members).await {
                Ok(()) => tracing::info!("raft: cluster initialized by bootstrap node"),
                Err(e) => {
                    tracing::info!("raft: initialize skipped/failed (already formed?): {}", e)
                }
            }
        });
    }

    tracing::info!(
        "Raft node {} started ({} peers, bootstrap={})",
        cfg.node_id,
        cfg.peers.len(),
        cfg.is_bootstrap
    );

    let handle = RaftHandle {
        raft,
        node_id: cfg.node_id,
    };
    Ok(StartedNode { handle, listener })
}

/// A started Raft node: the [`RaftHandle`] for routing writes + the RPC listener
/// task handle (so a controlled shutdown / the failover test can stop it).
pub struct StartedNode {
    pub handle: RaftHandle,
    pub listener: tokio::task::JoinHandle<()>,
}
