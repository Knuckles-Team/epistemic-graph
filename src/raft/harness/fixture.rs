//! Common fixtures for the in-process Raft harnesses.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use tokio::sync::RwLock;

use crate::isolation::IsolationLayer;
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use crate::server::ServerState;

/// The concrete backend shape shared by the Raft harness fixtures.
pub(crate) type Backend = Arc<dyn PersistenceBackend>;

/// Open the authoritative test backend used by the Raft harnesses.
pub(crate) fn open_backend(dir: &str) -> Result<Backend, String> {
    RedbBackend::open(dir.to_string(), 4096)
        .map(|backend| Arc::new(backend) as Backend)
        .map_err(|error| format!("open redb {dir}: {error}"))
}

/// Create a fresh authoritative backend for a named harness scenario.
pub(crate) fn fresh_backend(prefix: &str, tag: &str) -> (String, Backend) {
    let dir = fresh_dir(prefix, tag);
    let backend = open_backend(&dir).expect("open redb");
    (dir, backend)
}

/// How long a reopen waits for the last live reference to the old backend.
///
/// Generous: these harnesses run on shared build hosts under parallel load, so
/// the bound is here to catch a task that is never going to finish, not to
/// police latency.
const BACKEND_RELEASE_TIMEOUT: Duration = Duration::from_secs(30);

/// Close an authoritative backend before reopening the same durable store.
///
/// `Backend` is an `Arc`, and every caller hands CLONES of it to the nodes it
/// brings up. Dropping this function's own reference therefore does not
/// necessarily close the redb file: if any clone is still alive -- a spawned
/// node task that has been asked to stop but has not yet been polled to
/// completion -- the file lock is still held, and `open_backend` fails with
/// `Database already open. Cannot acquire lock.`
///
/// That made the reopen a race whose loser was whichever test happened to run
/// on a busier host, reported as a confusing storage error rather than as the
/// lifetime bug it is. So prove exclusivity rather than assume it: wait,
/// bounded, for this to be the last reference, and if it never is, say exactly
/// that.
pub(crate) fn reopen_backend(backend: Backend, dir: &str) -> Result<Backend, String> {
    backend.shutdown();
    release_sole_reference(backend, dir)?;
    open_backend(dir)
}

/// Drop `backend` once it is the only reference left, or fail naming the leak.
///
/// `Arc::try_unwrap` is unavailable here because `Backend` is
/// `Arc<dyn PersistenceBackend>` -- an unsized `T` cannot be returned by value
/// -- so the reference count is the available signal.
fn release_sole_reference(backend: Backend, dir: &str) -> Result<(), String> {
    let deadline = std::time::Instant::now() + BACKEND_RELEASE_TIMEOUT;
    while Arc::strong_count(&backend) > 1 {
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "reopen {dir}: {} other reference(s) to the backend were still alive after \
                 {}s, so its redb file lock is still held -- a node task outlived the group \
                 that was closed",
                Arc::strong_count(&backend) - 1,
                BACKEND_RELEASE_TIMEOUT.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(backend);
    Ok(())
}

/// Open an authoritative test backend with an explicit shard count.
pub(crate) fn open_backend_with_shards(
    dir: &str,
    max_nodes: usize,
    shards: usize,
) -> Result<Backend, String> {
    RedbBackend::open_with_shards(dir.to_string(), max_nodes, shards)
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

/// One cross-shard participant slice that adds `node` (properties `{"n": node}`)
/// to the global graph `graph` under the given placement fence.
pub(crate) fn add_node_slice(
    graph: &str,
    node: &str,
    placement_epoch: u64,
    fencing_token: Option<u64>,
) -> crate::raft::cross_shard_txn::GraphSlice {
    crate::raft::cross_shard_txn::GraphSlice {
        graph_name: graph.to_string(),
        graph_fname: crate::persist::sanitize(graph),
        graph_type: crate::protocol::GraphType::Global,
        methods: vec![crate::protocol::Method::AddNode {
            node_id: node.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"n": node}))
                .expect("fixture node properties encode"),
        }],
        placement_epoch,
        fencing_token,
    }
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

/// Start one local node the way a RESTART does: recover the durable graph image
/// into the serving projection BEFORE any Raft group opens. See
/// `harness_support::start_recovered_single_node_groups`.
pub(crate) async fn start_recovered_single_node_groups(
    dir: &str,
    backend: Backend,
    isolation: IsolationLayer,
    auth_secret: &str,
    group_ids: &[crate::raft::GroupId],
) -> (Arc<crate::raft::multi::MultiRaft>, Arc<RwLock<ServerState>>) {
    crate::raft::harness_support::start_recovered_single_node_groups(
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
