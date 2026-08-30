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

use std::sync::Arc;

use tokio::sync::RwLock;

use super::harness::cluster::fixture;
use super::multi::MultiRaft;
use super::reshard::TenantManager;
use super::{GroupId, RaftRequest};
use crate::protocol::{GraphType, Method};

const GROUP_A: GroupId = 100;
const GROUP_B: GroupId = 200;
const GRAPH: &str = "tenant:acme";

/// Bring up a one-node, two-group cluster with `GRAPH` initially assigned to group A.
async fn bring_up(
    dir: &str,
    backend: fixture::Backend,
) -> (Arc<MultiRaft>, Arc<RwLock<crate::server::ServerState>>) {
    let (multi, state) = fixture::start_single_node_groups(
        dir,
        backend,
        crate::isolation::IsolationLayer::new(),
        "reshard-test",
        &[GROUP_A, GROUP_B],
    )
    .await;
    multi.router().assign(GRAPH, GROUP_A);
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

async fn has_node(state: &Arc<RwLock<crate::server::ServerState>>, node_id: &str) -> bool {
    let s = state.read().await;
    s.registry
        .get(GRAPH)
        .map(|e| e.core.has_node(node_id))
        .unwrap_or(false)
}

// ─────────────────────────────────────────────────────────────────────────
// 1. Online reshard A→B keeps all data AND serves a post-reshard write.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reshard_keeps_data_and_serves_after() {
    let (dir, backend) = fixture::fresh_backend("eg-reshard", "keep");
    let (multi, state) = bring_up(&dir, backend.clone()).await;
    let tenants = TenantManager::new(multi.clone(), backend.clone());

    // Write 5 nodes through group A (the initial owner).
    assert_eq!(multi.router().group_of(GRAPH), GROUP_A);
    for i in 0..5 {
        write_via_owner(&multi, &format!("a{i}")).await.unwrap();
    }
    assert_eq!(fixture::node_count(&state, GRAPH).await, 5, "5 nodes on A");

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
    assert_eq!(fixture::node_count(&state, GRAPH).await, 6, "5 old + 1 new");

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
    let dir = fixture::fresh_dir("eg-reshard", "durable");
    let backend = fixture::open_backend(&dir).expect("open redb");
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
    let backend2 = fixture::reopen_backend(backend, &dir).expect("reopen");
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
    let dir = fixture::fresh_dir("eg-reshard", "hib");
    let backend = fixture::open_backend(&dir).expect("open redb");
    let (multi, state) = bring_up(&dir, backend.clone()).await;
    let tenants = TenantManager::new(multi.clone(), backend.clone());

    for i in 0..7 {
        write_via_owner(&multi, &format!("h{i}")).await.unwrap();
    }
    assert_eq!(
        fixture::node_count(&state, GRAPH).await,
        7,
        "7 nodes resident"
    );

    // ── HIBERNATE: force durable, drop in-RAM state ──
    let freed = tenants.hibernate_graph(GRAPH).await.expect("hibernate");
    assert_eq!(freed, 7, "7 nodes evicted from RAM");
    assert_eq!(
        fixture::node_count(&state, GRAPH).await,
        0,
        "core is now empty in RAM"
    );

    // ── REHYDRATE on next access: every node restored from redb ──
    let restored = tenants.rehydrate_graph(GRAPH).await.expect("rehydrate");
    assert_eq!(restored, 7, "7 nodes restored");
    assert_eq!(
        fixture::node_count(&state, GRAPH).await,
        7,
        "core repopulated"
    );
    for i in 0..7 {
        assert!(has_node(&state, &format!("h{i}")).await, "h{i} rehydrated");
    }

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
