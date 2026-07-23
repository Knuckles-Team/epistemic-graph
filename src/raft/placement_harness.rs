//! Placement-catalog gauntlet (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-1).
//!
//! Spins a live one-node, two-group cluster (the SAME `bring_up` pattern
//! [`super::reshard_harness`] uses) and proves the placement-catalog invariants:
//!
//!   * **Persists + reloads with epochs.** An assignment survives a process restart
//!     (backend close/reopen) with its epoch intact.
//!   * **Assign → route.** `placement_assign` immediately routes the tenant to the
//!     new group at the new epoch.
//!   * **Stale epoch → redirect.** A caller presenting an epoch from BEFORE a
//!     placement change gets redirected to the current `(group, epoch)` rather than
//!     silently served.
//!   * **Online move preserves data + lands the new epoch.** `move_partition` (snapshot
//!     → catch-up → fenced cutover, reusing `reshard_graph`) keeps every pre-move node
//!     readable AND leaves the catalog on the new epoch/group.
//!   * **Split spans two groups.** One tenant's two workspaces resolve to two
//!     DIFFERENT groups after `placement_split`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openraft::BasicNode;
use tokio::sync::RwLock;

use super::multi::MultiRaft;
use super::placement::split_tenant_key;
use super::reshard::TenantManager;
use super::{AppCtx, GroupId, NodeId, RaftRequest};
use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
use crate::channels::ChannelManager;
use crate::durability::DurabilityPolicy;
use crate::isolation::IsolationLayer;
use crate::protocol::{GraphType, Method, Request};
use crate::registry::GraphRegistry;
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use crate::server::{compute_verified_envelope_token, ServerState, VerifiedEnvelopeParams};

const GROUP_A: GroupId = 300;
const GROUP_B: GroupId = 400;
const TENANT: &str = "acme";
const TEST_AGENT: &str = "unit-test-agent";
const SECRET: &str = "placement-test";
static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn current_isolation() -> IsolationLayer {
    let mut isolation = IsolationLayer::new();
    isolation.register_agent(AgentIdentity {
        agent_id: TEST_AGENT.to_string(),
        role: AgentRole::System,
        teams: Vec::new(),
        roles: Vec::new(),
    });
    isolation
}

fn current_request(id: u64, method: Method) -> Request {
    std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
    std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-shared");
    std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
    std::env::set_var(
        "EPISTEMIC_GRAPH_SECURITY_STATE_DIR",
        std::env::temp_dir().join(format!("epistemic-graph-unit-auth-{}", std::process::id())),
    );
    let context = RequestContextClaims {
        principal: TEST_AGENT.to_string(),
        tenant: "tenant-shared".to_string(),
        audience: "epistemic-graph-test".to_string(),
        agent_id: TEST_AGENT.to_string(),
        roles: Vec::new(),
        scopes: vec!["*".to_string()],
        policy_version: "policy-test".to_string(),
        delegation: Vec::new(),
        node: None,
    };
    let mut request = Request {
        id,
        graph: "__commons__".to_string(),
        auth_token: String::new(),
        agent_id: Some(TEST_AGENT.to_string()),
        method,
    };
    let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the system clock is after the Unix epoch");
    let nonce = format!(
        "placement-{}-{id}-{sequence}-{}",
        std::process::id(),
        issued_at.as_nanos()
    );
    let idempotency_key = format!("placement-request-{id}-{sequence}");
    request.auth_token = compute_verified_envelope_token(
        SECRET,
        &request,
        &VerifiedEnvelopeParams {
            context: &context,
            timestamp: issued_at.as_secs(),
            nonce: &nonce,
            idempotency_key: &idempotency_key,
        },
    );
    request
}

async fn make_state(dir: &str, backend: Arc<dyn PersistenceBackend>) -> Arc<RwLock<ServerState>> {
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            crate::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: current_isolation(),
        channels: ChannelManager::new(),
        auth_secret: "placement-test".to_string(),
        persist_dir: Some(dir.to_string()),
        persistence: Some(backend),
        max_in_flight: Arc::new(tokio::sync::Semaphore::new(64)),
        read_admission: Arc::new(tokio::sync::Semaphore::new(64)),
        per_graph_inflight: Arc::new(dashmap::DashMap::new()),
        per_graph_inflight_limit: 16,
        write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
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
    let d = std::env::temp_dir().join(format!("eg-placement-{tag}-{}", std::process::id()));
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

/// Bring up a one-node, two-group cluster. `GROUP_A`/`GROUP_B` both live on the same
/// node so a move between them is exercised without needing real multi-node
/// membership (the same simplification `reshard_harness` uses).
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
    // DEFAULT_GROUP (0) backs the placement catalog itself.
    multi.ensure_group(super::DEFAULT_GROUP).await.unwrap();

    for gid in [GROUP_A, GROUP_B, super::DEFAULT_GROUP] {
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

/// Write `node_id` into `graph` through whichever group currently owns it.
async fn write_via_owner(multi: &Arc<MultiRaft>, graph: &str, node_id: &str) -> Result<(), String> {
    let routed = multi
        .handle_for_graph(graph)
        .await
        .ok_or_else(|| "owner group not running".to_string())?;
    let req = RaftRequest {
        graph_fname: crate::persist::sanitize(graph),
        graph_name: graph.to_string(),
        graph_type: GraphType::Global,
        committed_at_ms: 0,
        mutation: super::RaftMutationContext::internal(
            "raft-placement-harness",
            graph,
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
            "placement-test",
        )?,
    };
    routed.handle.client_write(req).await.map(|_| ())
}

async fn has_node(state: &Arc<RwLock<ServerState>>, graph: &str, node_id: &str) -> bool {
    let s = state.read().await;
    s.registry
        .get(graph)
        .map(|e| e.core.has_node(node_id))
        .unwrap_or(false)
}

async fn node_count(state: &Arc<RwLock<ServerState>>, graph: &str) -> usize {
    let s = state.read().await;
    s.registry
        .get(graph)
        .map(|e| e.core.node_count())
        .unwrap_or(0)
}

// ─────────────────────────────────────────────────────────────────────────
// 1. Assign → route resolves the new group + epoch immediately.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assign_then_route_returns_new_group_and_epoch() {
    let dir = fresh_dir("assign");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let (multi, _state) = bring_up(&dir, backend.clone()).await;

    // Before assignment the engine still returns a complete authoritative route.
    let placement = multi.placement();
    let initial = placement.route(TENANT, "ws1", GROUP_A).await;
    assert_eq!(initial.group, GROUP_A);
    assert_eq!(initial.epoch, 0);
    assert!(!initial.placed);

    let epoch1 = multi
        .placement_assign(TENANT, GROUP_B)
        .await
        .expect("assign");
    assert_eq!(epoch1, 1, "first placement change is epoch 1");
    let route = placement.route(TENANT, "ws1", GROUP_A).await;
    assert_eq!(route.group, GROUP_B);
    assert_eq!(route.epoch, epoch1);
    assert!(route.placed);
    // A graph named "acme:ws1" resolves through route_graph the SAME way.
    let route = multi.route_graph(&format!("{TENANT}:ws1")).await;
    assert_eq!(route.group, GROUP_B);
    assert_eq!(route.epoch, epoch1);
    let routed = multi
        .handle_for_graph(&format!("{TENANT}:ws1"))
        .await
        .expect("routed handle");
    assert_eq!(routed.group_id, GROUP_B);
    assert_eq!(routed.epoch, epoch1);
    assert_eq!(routed.fencing_token(), GROUP_B);

    // Reassigning bumps the epoch again.
    let epoch2 = multi
        .placement_assign(TENANT, GROUP_A)
        .await
        .expect("reassign");
    assert!(epoch2 > epoch1, "epoch strictly increases on reassignment");
    let route = placement.route(TENANT, "ws1", GROUP_B).await;
    assert_eq!(route.group, GROUP_A);
    assert_eq!(route.epoch, epoch2);
    assert!(route.placed);

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 2. A stale-epoch request gets a redirect, not silent service.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_epoch_request_gets_redirected() {
    let dir = fresh_dir("stale");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let (multi, _state) = bring_up(&dir, backend.clone()).await;
    let placement = multi.placement();

    let epoch1 = multi
        .placement_assign(TENANT, GROUP_A)
        .await
        .expect("assign");
    // A client that observed epoch1 is CURRENT — no redirect.
    assert_eq!(
        placement.redirect_if_stale(TENANT, "ws1", epoch1).await,
        None,
        "a caller on the current epoch is not redirected"
    );

    let epoch2 = multi
        .placement_assign(TENANT, GROUP_B)
        .await
        .expect("move to B");
    assert!(epoch2 > epoch1);

    // A client STILL holding epoch1 (pre-move) must be redirected to the NEW
    // group + epoch rather than served (which would silently hit the wrong shard
    // in a real multi-node deployment).
    let redirect = placement
        .redirect_if_stale(TENANT, "ws1", epoch1)
        .await
        .expect("a stale-epoch caller must be redirected");
    assert_eq!(redirect.group, GROUP_B);
    assert_eq!(redirect.epoch, epoch2);

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 3. The catalog persists + reloads with its epoch intact across a restart.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_persists_and_reloads_with_epoch() {
    let dir = fresh_dir("persist");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let epoch = {
        let (multi, _state) = bring_up(&dir, backend.clone()).await;
        let epoch = multi
            .placement_assign(TENANT, GROUP_B)
            .await
            .expect("assign");
        multi.stop_listener();
        multi.close_group(GROUP_A).await.unwrap();
        multi.close_group(GROUP_B).await.unwrap();
        multi.close_group(super::DEFAULT_GROUP).await.unwrap();
        epoch
    };
    backend.shutdown();
    drop(backend);

    let backend2: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("reopen"));
    let (multi2, state2) = bring_up(&dir, backend2.clone()).await;
    backend2.load_all(&state2).await.expect("load_all");

    let route = multi2.placement().route(TENANT, "ws1", GROUP_A).await;
    assert!(route.placed, "placement must survive a restart");
    assert_eq!(route.group, GROUP_B, "placement survived restart");
    assert_eq!(route.epoch, epoch, "epoch survived restart");

    multi2.stop_listener();
    backend2.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 4. Online move: snapshot → catch-up → fenced cutover preserves data + lands
//    the new epoch.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn online_move_preserves_data_and_lands_new_epoch() {
    let dir = fresh_dir("move");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let (multi, state) = bring_up(&dir, backend.clone()).await;
    let tenants = TenantManager::new(multi.clone(), backend.clone());

    let graph = format!("{TENANT}:ws1");
    let range = (0u64, u64::MAX);

    let epoch0 = multi
        .placement_assign(TENANT, GROUP_A)
        .await
        .expect("initial assign to A");
    let route = multi.route_graph(&graph).await;
    assert_eq!((route.group, route.epoch), (GROUP_A, epoch0));

    for i in 0..6 {
        write_via_owner(&multi, &graph, &format!("m{i}"))
            .await
            .unwrap();
    }
    assert_eq!(node_count(&state, &graph).await, 6, "6 nodes on A");

    // ── ONLINE MOVE A→B (snapshot → catch-up → fenced cutover) ──
    let report = tenants
        .move_partition(TENANT, range, GROUP_B)
        .await
        .expect("move_partition A→B");
    assert!(report.epoch > epoch0, "cutover strictly bumps the epoch");
    assert_eq!(report.target, GROUP_B);
    assert_eq!(report.graphs.len(), 1);
    assert_eq!(report.graphs[0].from_group, GROUP_A);
    assert_eq!(report.graphs[0].to_group, GROUP_B);
    assert_eq!(report.graphs[0].nodes_transferred, 6);
    let journals = multi.placement().move_journals().await.unwrap();
    assert_eq!(journals.len(), 1, "one durable move journal retained");
    assert_eq!(journals[0].stage, super::placement::MoveStage::Completed);
    assert_eq!(journals[0].completed_graphs, vec![graph.clone()]);

    // (a) EVERY pre-move node is still present — no data loss.
    for i in 0..6 {
        assert!(
            has_node(&state, &graph, &format!("m{i}")).await,
            "m{i} preserved across the move"
        );
    }

    // (b) The catalog now routes to the target group at the NEW epoch.
    let route = multi.route_graph(&graph).await;
    assert_eq!((route.group, route.epoch), (GROUP_B, report.epoch));

    // (c) A post-move write lands (served correctly through the new owner).
    write_via_owner(&multi, &graph, "post0")
        .await
        .expect("write via B");
    assert!(has_node(&state, &graph, "post0").await);
    assert_eq!(node_count(&state, &graph).await, 7);

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_cutover_move_abort_restores_source_and_journals_terminal_state() {
    let dir = fresh_dir("move-abort");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let (multi, _state) = bring_up(&dir, backend.clone()).await;
    let tenants = TenantManager::new(multi.clone(), backend.clone());
    let graph = format!("{TENANT}:abort");
    let range = (0u64, u64::MAX);
    let epoch = multi
        .placement_assign(TENANT, GROUP_A)
        .await
        .expect("assign source");
    let entry = multi
        .placement()
        .tenant_entries(TENANT)
        .await
        .into_iter()
        .find(|entry| entry.key.range_start == 0 && entry.key.range_end == u64::MAX)
        .unwrap();
    let mut journal =
        super::placement::PartitionMoveJournal::new(&entry, GROUP_B, vec![graph.clone()]).unwrap();
    multi.persist_move_journal(&journal).await.unwrap();
    multi
        .placement_start_move(TENANT, range, GROUP_B)
        .await
        .unwrap();
    multi.router().assign(&graph, GROUP_B);
    journal.stage = super::placement::MoveStage::Transferring;
    multi.persist_move_journal(&journal).await.unwrap();

    tenants.abort_move(&journal.move_id).await.unwrap();
    let route = multi.route_graph(&graph).await;
    assert_eq!((route.group, route.epoch), (GROUP_A, epoch));
    assert_eq!(multi.router().group_of(&graph), GROUP_A);
    let terminal = multi
        .placement()
        .move_journal(&journal.move_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.stage, super::placement::MoveStage::Aborted);

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphaned_moving_partition_fails_recovery_closed() {
    let dir = fresh_dir("move-orphan");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let (multi, _state) = bring_up(&dir, backend.clone()).await;
    let range = (0u64, u64::MAX);
    multi
        .placement_assign(TENANT, GROUP_A)
        .await
        .expect("assign source");

    // Exercise the low-level crate-internal seam deliberately: production callers
    // use TenantManager, which always journals Planned before marking Moving.
    multi
        .placement_start_move(TENANT, range, GROUP_B)
        .await
        .expect("mark moving without journal");
    let error = multi
        .placement()
        .validate_move_recovery_state()
        .await
        .expect_err("an orphaned moving placement must not be served");
    assert!(error.contains("unique durable recovery journal"));

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abort_intent_behind_committed_cutover_reconciles_forward() {
    let dir = fresh_dir("move-abort-fence-race");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let (multi, _state) = bring_up(&dir, backend.clone()).await;
    let tenants = TenantManager::new(multi.clone(), backend.clone());
    let range = (0u64, u64::MAX);
    multi
        .placement_assign(TENANT, GROUP_A)
        .await
        .expect("assign source");
    let entry = multi
        .placement()
        .tenant_entries(TENANT)
        .await
        .into_iter()
        .next()
        .unwrap();
    let mut journal =
        super::placement::PartitionMoveJournal::new(&entry, GROUP_B, Vec::new()).unwrap();
    multi.persist_move_journal(&journal).await.unwrap();
    multi
        .placement_start_move(TENANT, range, GROUP_B)
        .await
        .unwrap();
    journal.stage = super::placement::MoveStage::Aborting;
    multi.persist_move_journal(&journal).await.unwrap();

    // Model the one unavoidable ordering race: the epoch fence committed just
    // before the abort driver observed placement. Recovery must never roll it back.
    let cutover_epoch = multi
        .placement_fence_cutover(TENANT, range, GROUP_B)
        .await
        .unwrap();
    let reports = tenants.reconcile_moves().await.unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].epoch, cutover_epoch);
    let terminal = multi
        .placement()
        .move_journal(&journal.move_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.stage, super::placement::MoveStage::Completed);

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 5. Split lets one tenant span two groups.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_lets_one_tenant_span_two_groups() {
    let dir = fresh_dir("split");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let (multi, _state) = bring_up(&dir, backend.clone()).await;

    // Pick two workspace sub-keys whose stable hashes fall on either side of a
    // split point, so after splitting they resolve to DIFFERENT groups.
    let graph_lo = format!("{TENANT}:ws-lo");
    let graph_hi = format!("{TENANT}:ws-hi");
    let (_, sub_lo) = split_tenant_key(&graph_lo);
    let (_, sub_hi) = split_tenant_key(&graph_hi);
    let h_lo = super::multi::fnv1a(sub_lo);
    let h_hi = super::multi::fnv1a(sub_hi);
    let (lo_key, lo_hash, hi_key, hi_hash) = if h_lo < h_hi {
        (sub_lo, h_lo, sub_hi, h_hi)
    } else {
        (sub_hi, h_hi, sub_lo, h_lo)
    };
    let at = lo_hash + 1;
    assert!(
        at <= hi_hash,
        "chosen split point must separate the two keys"
    );

    let epoch = multi
        .placement_split(TENANT, at, GROUP_A, GROUP_B)
        .await
        .expect("split");

    let placement = multi.placement();
    let route_lo = placement.route(TENANT, lo_key, GROUP_A).await;
    let route_hi = placement.route(TENANT, hi_key, GROUP_A).await;
    assert!(route_lo.placed && route_hi.placed);
    assert_eq!(
        route_lo.group, GROUP_A,
        "the lower sub-range routes to group A"
    );
    assert_eq!(
        route_hi.group, GROUP_B,
        "the upper sub-range routes to group B"
    );
    assert_ne!(
        route_lo.group, route_hi.group,
        "one tenant now spans two groups"
    );
    assert_eq!(route_lo.epoch, epoch);
    assert_eq!(route_hi.epoch, epoch);

    // Merging collapses the tenant back onto one group.
    let merge_epoch = multi.placement_merge(TENANT, GROUP_A).await.expect("merge");
    assert!(merge_epoch > epoch);
    for key in [lo_key, hi_key] {
        let route = placement.route(TENANT, key, GROUP_B).await;
        assert!(route.placed);
        assert_eq!(route.group, GROUP_A);
    }

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 6. The wire `Method::PlacementRoute` RPC (CONCEPT:EG-KG.sharding.placement-route-rpc, DIST-P2-4)
//    resolves through the REAL served dispatch path — not just the in-process
//    `PlacementCatalog` API the tests above exercise directly. This is the
//    external-caller seam `epistemic_graph.client`'s `placement.route(...)` (and
//    AU's `placement_catalog.py`) drives.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_placement_route_resolves_through_dispatch() {
    use crate::protocol::ResultPayload;
    use crate::server::dispatch;

    let dir = fresh_dir("wire-route");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let (multi, state) = bring_up(&dir, backend.clone()).await;
    // `bring_up`'s `ServerState` never back-references the `MultiRaft` it's shared
    // with (the other tests in this file only ever call `multi.*` directly, never
    // through the served `dispatch` seam) -- but the REAL served
    // `Method::PlacementRoute` handler (`handlers/placement.rs`) resolves the
    // catalog off `state.multi_raft`, exactly like the DIST-P2-2 cross-shard 2PC
    // path (`handlers/txn.rs`) does. Wire it here (mirrors `xshard_harness.rs`'s
    // `wire_state` helper) so this test exercises the SAME lookup a real server
    // does.
    state.write().await.multi_raft = Some(multi.clone());

    let req = current_request;
    let route_json = |resp: crate::protocol::Response| -> serde_json::Value {
        assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
        match resp.result {
            Some(ResultPayload::Raw(bytes)) => {
                let route: crate::epistemic_operations::PlacementRoute =
                    rmp_serde::from_slice(&bytes).expect("typed PlacementRoute");
                serde_json::to_value(route).expect("PlacementRoute JSON projection")
            }
            other => panic!("expected a typed route result, got {other:?}"),
        }
    };

    // Before any assignment the wire still returns the engine's complete route.
    let val = route_json(
        dispatch(
            &state,
            req(
                1,
                Method::PlacementRoute {
                    request: crate::epistemic_operations::PlacementRouteRequest {
                        schema_version:
                            crate::epistemic_operations::PlacementRouteRequestSchemaVersion::V1,
                        tenant_ref: TENANT.to_string(),
                        partition_ref: "ws1".to_string(),
                        client_epoch: 0,
                    },
                },
            ),
        )
        .await,
    );
    assert_eq!(val["authoritative"], serde_json::json!(true));
    assert_eq!(val["placed"], serde_json::json!(false));
    assert_eq!(val["group"], serde_json::json!(super::DEFAULT_GROUP));
    assert_eq!(val["epoch"], serde_json::json!(0));
    assert_eq!(
        val["fencing_token"],
        serde_json::json!(super::DEFAULT_GROUP)
    );

    // After an assignment, the SAME wire RPC resolves the new group/epoch; a caller
    // presenting client_epoch=0 (never resolved before) is flagged for redirect.
    let epoch1 = multi
        .placement_assign(TENANT, GROUP_B)
        .await
        .expect("assign");
    let val = route_json(
        dispatch(
            &state,
            req(
                2,
                Method::PlacementRoute {
                    request: crate::epistemic_operations::PlacementRouteRequest {
                        schema_version:
                            crate::epistemic_operations::PlacementRouteRequestSchemaVersion::V1,
                        tenant_ref: TENANT.to_string(),
                        partition_ref: "ws1".to_string(),
                        client_epoch: 0,
                    },
                },
            ),
        )
        .await,
    );
    assert_eq!(val["authoritative"], serde_json::json!(true));
    assert_eq!(val["placed"], serde_json::json!(true));
    assert_eq!(val["group"], serde_json::json!(GROUP_B));
    assert_eq!(val["epoch"], serde_json::json!(epoch1));
    assert_eq!(val["stale"], serde_json::json!(true));
    assert_eq!(val["fencing_token"], serde_json::json!(GROUP_B));
    assert!(val["leader_ref"].is_null());

    // A caller presenting the CURRENT epoch is not flagged for redirect.
    let val = route_json(
        dispatch(
            &state,
            req(
                3,
                Method::PlacementRoute {
                    request: crate::epistemic_operations::PlacementRouteRequest {
                        schema_version:
                            crate::epistemic_operations::PlacementRouteRequestSchemaVersion::V1,
                        tenant_ref: TENANT.to_string(),
                        partition_ref: "ws1".to_string(),
                        client_epoch: epoch1,
                    },
                },
            ),
        )
        .await,
    );
    assert_eq!(val["stale"], serde_json::json!(false));

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
