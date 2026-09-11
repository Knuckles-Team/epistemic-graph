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

use std::sync::Arc;

use tokio::sync::RwLock;

use super::harness::cluster::fixture;
use super::multi::MultiRaft;
use super::placement::split_tenant_key;
use super::reshard::TenantManager;
use super::{GroupId, RaftRequest};
use crate::protocol::{GraphType, Method};

const GROUP_A: GroupId = 300;
const GROUP_B: GroupId = 400;
const TENANT: &str = "acme";
const TEST_AGENT: &str = "unit-test-agent";
const SECRET: &str = "placement-test";

fn current_isolation() -> crate::isolation::IsolationLayer {
    super::harness_support::current_isolation(TEST_AGENT)
}

fn current_request(id: u64, method: Method) -> crate::protocol::Request {
    super::harness_support::signed_request(
        id,
        "__commons__",
        method,
        SECRET,
        super::harness_support::HarnessLabels {
            agent_id: TEST_AGENT,
            nonce: "placement",
            idempotency: "placement-request",
            security_state: "epistemic-graph-unit-auth",
        },
    )
}

/// Bring up a one-node, two-group cluster. `GROUP_A`/`GROUP_B` both live on the same
/// node so a move between them is exercised without needing real multi-node
/// membership (the same simplification `reshard_harness` uses).
async fn bring_up(
    dir: &str,
    backend: fixture::Backend,
) -> (Arc<MultiRaft>, Arc<RwLock<crate::server::ServerState>>) {
    fixture::start_single_node_groups(
        dir,
        backend,
        current_isolation(),
        "placement-test",
        &[GROUP_A, GROUP_B, super::DEFAULT_GROUP],
    )
    .await
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

fn epoch_cas_request(
    coordinator_id: &str,
    nonce: u8,
    method: Method,
) -> Result<RaftRequest, String> {
    let mut mutation = super::RaftMutationContext::internal(
        "raft-placement-contention",
        super::placement::PLACEMENT_GRAPH,
        coordinator_id,
        0,
        0,
    );
    mutation.attempt_nonce = Some(eg_types::contract::Nonce::from_bytes([nonce; 32]));
    Ok(RaftRequest {
        graph_fname: crate::persist::sanitize(super::placement::PLACEMENT_GRAPH),
        graph_name: super::placement::PLACEMENT_GRAPH.to_string(),
        graph_type: GraphType::Commons,
        committed_at_ms: 0,
        mutation,
        command: super::ReplicatedMutation::graph(method, SECRET)?,
    })
}

fn bool_response(response: Result<super::RaftResponse, String>) -> bool {
    let response = response.expect("placement CAS request should reach the state machine");
    assert!(
        response.native_error.is_none(),
        "placement CAS returned an error: {:?}",
        response.native_error
    );
    match response.native_result {
        Some(crate::protocol::ResultPayload::Bool(value)) => value,
        other => panic!("placement CAS returned an unexpected result: {other:?}"),
    }
}

// A live state-machine contention check: two distinct plan authorities issue the
// same epoch CAS concurrently. Exactly one receipt may be true. The winner's
// stored boolean must be replayable with a fresh caller nonce, while reusing the
// consumed nonce must fail before the replay shortcut can answer it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn placement_state_machine_binds_plan_identity_and_replay_nonce() {
    let dir = fixture::fresh_dir("eg-placement", "cas-replay-integrity");
    let backend = fixture::open_backend(&dir).expect("open redb");
    let (multi, state) = bring_up(&dir, backend.clone()).await;
    multi
        .placement_assign(TENANT, GROUP_A)
        .await
        .expect("seed placement epoch");

    let method = super::placement::PlacementCatalog::epoch_cas_method(1, 2);
    let request_a = epoch_cas_request("plan-a", 11, method.clone()).expect("request a");
    let request_b = epoch_cas_request("plan-b", 12, method).expect("request b");
    assert_ne!(
        request_a.mutation.batch_id, request_b.mutation.batch_id,
        "distinct plans must not share a durable replay key"
    );
    let (response_a, response_b) = tokio::join!(
        multi.client_write_group(super::DEFAULT_GROUP, request_a.clone()),
        multi.client_write_group(super::DEFAULT_GROUP, request_b.clone()),
    );
    let applied_a = bool_response(response_a);
    let applied_b = bool_response(response_b);
    assert_ne!(applied_a, applied_b, "one competing CAS must win");

    let counter = {
        let guard = state.read().await;
        let bytes = guard
            .registry
            .get(super::placement::PLACEMENT_GRAPH)
            .and_then(|entry| {
                entry
                    .core
                    .get_node_properties(super::placement::PLACEMENT_EPOCH_COUNTER_NODE)
            })
            .expect("epoch counter must remain present");
        rmp_serde::from_slice::<serde_json::Value>(&bytes).expect("counter properties decode")
    };
    assert_eq!(
        counter[super::placement::PLACEMENT_EPOCH_FIELD],
        serde_json::json!(2)
    );

    let (loser, winner) = if applied_a {
        (request_b, request_a)
    } else {
        (request_a, request_b)
    };

    let mut loser_retry = loser.clone();
    loser_retry.mutation.attempt_nonce = Some(eg_types::contract::Nonce::from_bytes([14; 32]));
    assert!(!bool_response(
        multi
            .client_write_group(super::DEFAULT_GROUP, loser_retry)
            .await
    ));

    let mut fresh = winner.clone();
    fresh.mutation.attempt_nonce = Some(eg_types::contract::Nonce::from_bytes([13; 32]));
    assert!(bool_response(
        multi.client_write_group(super::DEFAULT_GROUP, fresh).await
    ));

    let reused = multi
        .client_write_group(super::DEFAULT_GROUP, winner)
        .await
        .expect_err("the exact caller nonce must be consumed by the replay probe");
    assert!(reused.contains("REPLAY_NONCE_CONSUMED"), "{reused}");

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

async fn has_node(
    state: &Arc<RwLock<crate::server::ServerState>>,
    graph: &str,
    node_id: &str,
) -> bool {
    let s = state.read().await;
    s.registry
        .get(graph)
        .map(|e| e.core.has_node(node_id))
        .unwrap_or(false)
}

// ─────────────────────────────────────────────────────────────────────────
// 1. Assign → route resolves the new group + epoch immediately.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assign_then_route_returns_new_group_and_epoch() {
    let dir = fixture::fresh_dir("eg-placement", "assign");
    let backend = fixture::open_backend(&dir).expect("open redb");
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
    let dir = fixture::fresh_dir("eg-placement", "stale");
    let backend = fixture::open_backend(&dir).expect("open redb");
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
    // Held for the whole test: opens the backend TWICE (initial + a restart reopen
    // of the SAME dir) -- same "ambient key must stay constant across both opens"
    // requirement as `reshard_harness::reshard_data_durable_across_restart`. See
    // `crate::crypto::acquire_test_env_lock`'s doc.
    #[cfg(feature = "security")]
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let (dir, backend) = fixture::fresh_backend("eg-placement", "persist");
    let epoch = {
        let (multi, _state) = bring_up(&dir, backend.clone()).await;
        let epoch = multi
            .placement_assign(TENANT, GROUP_B)
            .await
            .expect("assign");
        // `close_group` removes a group and shuts its Raft down, but it is NOT a
        // node teardown: the heartbeat-flush and leader-balance tasks, the
        // connection tasks, and -- decisively -- the owning `Arc<MultiRaft>` that
        // `MultiRaft::start` published into `ServerState::multi_raft` all stay
        // alive. That publication plus the manager's own `ctx.state` handle form
        // a REFERENCE CYCLE (see `MultiRaft::shutdown`'s own comment about it),
        // so dropping the locals here released nothing and the backend Arc stayed
        // above one for good -- which `reopen_backend` then reported, correctly,
        // as "3 other reference(s) ... a node task outlived the group that was
        // closed". `shutdown()` is the teardown that breaks the cycle: it stops
        // the control-plane workers, drains every group, stops the persistence
        // writer, and clears its own `state.multi_raft` publication.
        multi.shutdown().await;
        epoch
    };
    let backend2 = fixture::reopen_backend(backend, &dir).expect("reopen");
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
    let (dir, backend) = fixture::fresh_backend("eg-placement", "move");
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
    assert_eq!(fixture::node_count(&state, &graph).await, 6, "6 nodes on A");

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
    assert_eq!(fixture::node_count(&state, &graph).await, 7);

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_cutover_move_abort_restores_source_and_journals_terminal_state() {
    let dir = fixture::fresh_dir("eg-placement", "move-abort");
    let backend = fixture::open_backend(&dir).expect("open redb");
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
    let dir = fixture::fresh_dir("eg-placement", "move-orphan");
    let backend = fixture::open_backend(&dir).expect("open redb");
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
    let dir = fixture::fresh_dir("eg-placement", "move-abort-fence-race");
    let backend = fixture::open_backend(&dir).expect("open redb");
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
    let dir = fixture::fresh_dir("eg-placement", "split");
    let backend = fixture::open_backend(&dir).expect("open redb");
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

    let dir = fixture::fresh_dir("eg-placement", "wire-route");
    let backend = fixture::open_backend(&dir).expect("open redb");
    let (multi, state) = bring_up(&dir, backend.clone()).await;
    // `MultiRaft::start` installs the live catalog into ServerState through the
    // shared construction seam. Re-assert that binding here so the served
    // `Method::PlacementRoute` path remains explicit in this wire regression;
    // it resolves the same state-owned catalog as the cross-shard txn path.
    let mut guard = state.write().await;
    let raft = guard.raft.clone();
    guard.install_multi_raft_placement_authority(raft, multi.clone());
    drop(guard);

    let req = current_request;
    let route_json = |resp: crate::protocol::Response| -> serde_json::Value {
        assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
        match resp.result {
            // A-W1.2-2: the served route payload is the ADR-1 wire superset
            // (extra `endpoints` key); the canonical deny_unknown_fields DTO
            // rejects it, so the wire type is the only correct reader.
            Some(ResultPayload::Raw(bytes)) => {
                let route: crate::server::handlers::placement::PlacementRouteWire =
                    rmp_serde::from_slice(&bytes).expect("typed PlacementRouteWire");
                serde_json::to_value(route).expect("PlacementRouteWire JSON projection")
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
