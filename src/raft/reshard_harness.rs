//! Online-resharding + tenant-hibernation gauntlet (CONCEPT:EG-KG.storage.100m-tenant).
//!
//! Spins a live one-node, two-group cluster over a shared authoritative shard (the SAME
//! machinery the cross-shard harness uses) and proves the two elastic-tenant
//! invariants:
//!
//!   * **Reshard keeps data + serves correctly.** A graph written on group A,
//!     reshareded A→B, retains EVERY pre-reshard node (readable) AND a post-reshard
//!     write routes through B and lands. No downtime, no data loss.
//!   * **Hibernate → rehydrate is intact.** A graph forced durable then hibernated
//!     (in-RAM state dropped) rehydrates from redb with every node restored.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use tokio::sync::RwLock;

use super::multi::MultiRaft;
use super::reshard::TenantManager;
use super::{AppCtx, GroupId, NodeId, RaftRequest};
use crate::channels::ChannelManager;
use crate::durability::DurabilityPolicy;
use crate::isolation::IsolationLayer;
use crate::protocol::{GraphType, Method};
use crate::registry::GraphRegistry;
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use crate::server::ServerState;

const GROUP_A: GroupId = 100;
const GROUP_B: GroupId = 200;
const GRAPH: &str = "tenant:acme";

async fn make_state(dir: &str, backend: Arc<dyn PersistenceBackend>) -> Arc<RwLock<ServerState>> {
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            crate::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: "reshard-test".to_string(),
        persist_dir: Some(dir.to_string()),
        persistence: Some(backend),
        max_in_flight: Arc::new(tokio::sync::Semaphore::new(64)),
        read_admission: Arc::new(tokio::sync::Semaphore::new(64)),
        per_graph_inflight: Arc::new(dashmap::DashMap::new()),
        per_graph_inflight_limit: 16,
        write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
        routed_write_coalescer: Arc::new(
            crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
        ),
        open_txns: Arc::new(dashmap::DashMap::new()),
        txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
        txn_ttl_secs: 300,
        txn_max_per_graph: 256,
        txn_max_per_agent: 256,
        #[cfg(feature = "blob")]
        blob: None,
        #[cfg(feature = "blob")]
        blob_cursor_ttl_secs: 300,
        raft: None,
        #[cfg(feature = "raft")]
        multi_raft: None,
        #[cfg(feature = "tsdb")]
        tsdb_store: None,
        #[cfg(feature = "streaming")]
        cdc: None,
        #[cfg(feature = "wasm-udf")]
        udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: std::sync::Arc::new(parking_lot::Mutex::new(
            crate::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: std::sync::Arc::new(dashmap::DashMap::new()),
        #[cfg(feature = "kv")]
        kv: None,
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
    }))
}

fn fresh_dir(tag: &str) -> String {
    let d = std::env::temp_dir().join(format!("eg-reshard-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d.to_string_lossy().to_string()
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

async fn wait_until<F, Fut>(timeout: Duration, mut pred: F) -> Result<(), ()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if pred().await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    Err(())
}

/// Bring up a one-node, two-group cluster with `GRAPH` initially assigned to group A.
async fn bring_up(
    dir: &str,
    backend: Arc<dyn PersistenceBackend>,
) -> (Arc<MultiRaft>, Arc<RwLock<ServerState>>) {
    let state = make_state(dir, backend.clone()).await;
    let ctx = AppCtx {
        state: state.clone(),
        router: None,
    };
    let port = free_port();
    let node_id: NodeId = 1;
    let peers: BTreeMap<NodeId, BasicNode> =
        [(node_id, BasicNode::new(format!("127.0.0.1:{port}")))].into();

    let multi = MultiRaft::start(node_id, format!("127.0.0.1:{port}"), backend.clone(), ctx)
        .await
        .expect("start multi");
    multi
        .create_group(GROUP_A, peers.clone(), true)
        .await
        .unwrap();
    multi
        .create_group(GROUP_B, peers.clone(), true)
        .await
        .unwrap();
    multi.router().assign(GRAPH, GROUP_A);

    for gid in [GROUP_A, GROUP_B] {
        let g = multi.group(gid).await.expect("group exists");
        wait_until(Duration::from_secs(15), || {
            let g = g.clone();
            async move { g.current_leader().await == Some(node_id) }
        })
        .await
        .unwrap_or_else(|_| panic!("group {gid} must elect a leader"));
    }
    (multi, state)
}

/// Write `node_id` into `GRAPH` through whichever group currently owns it.
async fn write_via_owner(multi: &Arc<MultiRaft>, node_id: &str) -> Result<(), String> {
    let group = multi
        .group_for_graph(GRAPH)
        .await
        .ok_or_else(|| "owner group not running".to_string())?;
    let req = RaftRequest {
        graph_fname: crate::persist::sanitize(GRAPH),
        graph_name: GRAPH.to_string(),
        graph_type: GraphType::Global,
        committed_at_ms: 0,
        mutation: super::RaftMutationContext::internal(
            "raft-reshard-harness",
            GRAPH,
            node_id,
            0,
            0,
        ),
        command: super::ReplicatedMutation::graph(
            Method::AddNode {
                node_id: node_id.to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"id": node_id}))
                    .unwrap(),
            },
            "reshard-test",
        )?,
    };
    group.client_write(req).await.map(|_| ())
}

async fn has_node(state: &Arc<RwLock<ServerState>>, node_id: &str) -> bool {
    let s = state.read().await;
    s.registry
        .get(GRAPH)
        .map(|e| e.core.has_node(node_id))
        .unwrap_or(false)
}

async fn node_count(state: &Arc<RwLock<ServerState>>) -> usize {
    let s = state.read().await;
    s.registry
        .get(GRAPH)
        .map(|e| e.core.node_count())
        .unwrap_or(0)
}

// ─────────────────────────────────────────────────────────────────────────
// 1. Online reshard A→B keeps all data AND serves a post-reshard write.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reshard_keeps_data_and_serves_after() {
    let dir = fresh_dir("keep");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let (multi, state) = bring_up(&dir, backend.clone()).await;
    let tenants = TenantManager::new(multi.clone(), backend.clone());

    // Write 5 nodes through group A (the initial owner).
    assert_eq!(multi.router().group_of(GRAPH), GROUP_A);
    for i in 0..5 {
        write_via_owner(&multi, &format!("a{i}")).await.unwrap();
    }
    assert_eq!(node_count(&state).await, 5, "5 nodes on A");

    // ── RESHARD A→B (online, no downtime) ──
    let report = tenants
        .reshard_graph(GRAPH, GROUP_B)
        .await
        .expect("reshard A→B");
    assert_eq!(report.from_group, GROUP_A);
    assert_eq!(report.to_group, GROUP_B);
    assert_eq!(report.nodes_transferred, 5, "5 nodes durable at barrier");
    // The router now points the graph at group B.
    assert_eq!(multi.router().group_of(GRAPH), GROUP_B);

    // (a) EVERY pre-reshard node is still present + readable — no data loss.
    for i in 0..5 {
        assert!(has_node(&state, &format!("a{i}")).await, "a{i} preserved");
    }

    // (b) A post-reshard write routes through B and lands — serves correctly.
    write_via_owner(&multi, "b0").await.expect("write via B");
    assert!(has_node(&state, "b0").await, "post-reshard write landed");
    assert_eq!(node_count(&state).await, 6, "5 old + 1 new");

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 2. Reshard data survives a process restart (durable transfer barrier).
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reshard_data_durable_across_restart() {
    // Held for the whole test: opens the backend TWICE (initial + a restart reopen
    // of the SAME dir) and both opens must resolve the same encryption-at-rest
    // cipher, or the reopen's read fails with "encrypted durable value is missing
    // sealed framing". `fresh_dir`'s `Once`-guarded key provisioning only fires the
    // FIRST time it's called process-wide, so it does not by itself protect this
    // test's two opens against an unrelated test's concurrent env mutation between
    // them. See `crate::crypto::acquire_test_env_lock`'s doc.
    #[cfg(feature = "security")]
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let dir = fresh_dir("durable");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    {
        let (multi, _state) = bring_up(&dir, backend.clone()).await;
        let tenants = TenantManager::new(multi.clone(), backend.clone());
        for i in 0..4 {
            write_via_owner(&multi, &format!("d{i}")).await.unwrap();
        }
        tenants
            .reshard_graph(GRAPH, GROUP_B)
            .await
            .expect("reshard");
        multi.stop_listener();
        multi.close_group(GROUP_A).await.unwrap();
        multi.close_group(GROUP_B).await.unwrap();
    }
    // Restart over the SAME files: every reshareded node is durable.
    backend.shutdown();
    drop(backend);
    let backend2: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("reopen"));
    let (multi2, state2) = bring_up(&dir, backend2.clone()).await;
    backend2.load_all(&state2).await.expect("load_all");
    for i in 0..4 {
        assert!(
            has_node(&state2, &format!("d{i}")).await,
            "d{i} durable across restart"
        );
    }
    multi2.stop_listener();
    backend2.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 3. Hibernate drops RAM; rehydrate restores every node intact.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hibernate_then_rehydrate_intact() {
    let dir = fresh_dir("hib");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let (multi, state) = bring_up(&dir, backend.clone()).await;
    let tenants = TenantManager::new(multi.clone(), backend.clone());

    for i in 0..7 {
        write_via_owner(&multi, &format!("h{i}")).await.unwrap();
    }
    assert_eq!(node_count(&state).await, 7, "7 nodes resident");

    // ── HIBERNATE: force durable, drop in-RAM state ──
    let freed = tenants.hibernate_graph(GRAPH).await.expect("hibernate");
    assert_eq!(freed, 7, "7 nodes evicted from RAM");
    assert_eq!(node_count(&state).await, 0, "core is now empty in RAM");

    // ── REHYDRATE on next access: every node restored from redb ──
    let restored = tenants.rehydrate_graph(GRAPH).await.expect("rehydrate");
    assert_eq!(restored, 7, "7 nodes restored");
    assert_eq!(node_count(&state).await, 7, "core repopulated");
    for i in 0..7 {
        assert!(has_node(&state, &format!("h{i}")).await, "h{i} rehydrated");
    }

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
