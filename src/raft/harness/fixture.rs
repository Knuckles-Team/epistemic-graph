//! Common fixtures for the in-process Raft harnesses.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use tokio::sync::RwLock;

use crate::durability::DurabilityPolicy;
use crate::isolation::IsolationLayer;
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use crate::server::ServerState;

/// The concrete backend shape shared by the Raft harness fixtures.
pub(crate) type Backend = Arc<dyn PersistenceBackend>;

/// Open the authoritative test backend used by the Raft harnesses.
pub(crate) fn open_backend(dir: &str) -> Result<Backend, String> {
    RedbBackend::open(dir.to_string(), DurabilityPolicy::Each, 4096)
        .map(|backend| Arc::new(backend) as Backend)
        .map_err(|error| format!("open redb {dir}: {error}"))
}

/// Create a fresh authoritative backend for a named harness scenario.
pub(crate) fn fresh_backend(prefix: &str, tag: &str) -> (String, Backend) {
    let dir = fresh_dir(prefix, tag);
    let backend = open_backend(&dir).expect("open redb");
    (dir, backend)
}

/// Close an authoritative backend before reopening the same durable store.
pub(crate) fn reopen_backend(backend: Backend, dir: &str) -> Result<Backend, String> {
    backend.shutdown();
    drop(backend);
    open_backend(dir)
}

/// Open an authoritative test backend with an explicit shard count.
pub(crate) fn open_backend_with_shards(
    dir: &str,
    max_nodes: usize,
    shards: usize,
) -> Result<Backend, String> {
    RedbBackend::open_with_shards(dir.to_string(), DurabilityPolicy::Each, max_nodes, shards)
        .map(|backend| Arc::new(backend) as Backend)
        .map_err(|error| format!("open redb {dir}: {error}"))
}

/// Create a unique, empty temporary directory for a harness scenario.
pub(crate) fn fresh_dir(prefix: &str, tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!("{prefix}-{tag}-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).expect("create harness directory");
    dir.into_os_string()
        .into_string()
        .expect("harness path is valid UTF-8")
}

/// Wait for an asynchronous harness condition with an explicit polling cadence.
pub(crate) async fn wait_until<F, Fut>(timeout: Duration, mut predicate: F) -> Result<(), ()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if predicate().await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    Err(())
}

/// Build the peer map used by multi-node in-process cluster fixtures.
pub(crate) fn peer_map(ports: &[u16]) -> BTreeMap<crate::raft::NodeId, BasicNode> {
    ports
        .iter()
        .enumerate()
        .fold(BTreeMap::new(), |mut peers, (index, port)| {
            peers.insert(
                (index + 1) as crate::raft::NodeId,
                BasicNode::new(format!("127.0.0.1:{port}")),
            );
            peers
        })
}

/// Count the nodes currently resident in a named graph.
pub(crate) async fn node_count(state: &Arc<RwLock<ServerState>>, graph: &str) -> usize {
    state
        .read()
        .await
        .registry
        .get(graph)
        .map_or(0, |entry| entry.core.node_count())
}

/// Build and rehydrate the managed cluster's authoritative test state.
pub(crate) async fn make_rehydrated_state(dir: &str) -> Result<Arc<RwLock<ServerState>>, String> {
    let backend = open_backend(dir)?;
    let state = crate::raft::harness_support::make_state_with_cdc(
        dir,
        backend.clone(),
        IsolationLayer::new(),
        "harness",
    )
    .await;
    {
        let mut state = state.write().await;
        state.max_in_flight = Arc::new(tokio::sync::Semaphore::new(256));
        state.read_admission = Arc::new(tokio::sync::Semaphore::new(256));
        state.per_graph_inflight_limit = 64;
    }
    if let Err(error) = backend.load_all(&state).await {
        backend.shutdown();
        return Err(format!("load_all {dir}: {error}"));
    }
    Ok(state)
}

/// Start one local node with the requested groups and wait for each leader.
pub(crate) async fn start_single_node_groups(
    dir: &str,
    backend: Backend,
    isolation: IsolationLayer,
    auth_secret: &str,
    group_ids: &[crate::raft::GroupId],
) -> (Arc<crate::raft::multi::MultiRaft>, Arc<RwLock<ServerState>>) {
    crate::raft::harness_support::start_single_node_groups(
        dir,
        backend,
        isolation,
        auth_secret,
        group_ids,
    )
    .await
}

/// Start one local node, create groups, and assign named graphs to their owners.
pub(crate) async fn start_routed_groups(
    dir: &str,
    backend: Backend,
    isolation: IsolationLayer,
    auth_secret: &str,
    group_ids: &[crate::raft::GroupId],
    assignments: &[(&str, crate::raft::GroupId)],
) -> (Arc<crate::raft::multi::MultiRaft>, Arc<RwLock<ServerState>>) {
    let (multi, state) =
        start_single_node_groups(dir, backend, isolation, auth_secret, group_ids).await;
    for &(graph, group) in assignments {
        multi.router().assign(graph, group);
    }
    (multi, state)
}
