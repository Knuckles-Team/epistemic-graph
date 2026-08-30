//! Cross-shard READ gauntlet (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-2).
//!
//! Spins a live one-node, two-group cluster (the SAME `bring_up` pattern
//! [`super::xshard_harness`]/[`super::placement_harness`] use) and proves:
//!
//!   * **A read spanning two groups returns the merged, correct result** — writing
//!     distinct nodes into two graphs pinned to two DIFFERENT groups, then
//!     [`super::xread::CrossShardReader::read`]ing both, yields every node
//!     from BOTH groups, unioned.
//!   * **Each leg routes via the [`super::placement::PlacementCatalog`]** — after
//!     `placement_split`ting one tenant across two groups, a read naming that tenant's
//!     two sub-key graphs resolves EACH leg to the catalog's assigned group (not the
//!     engine-owned unplaced policy), exactly like a write would.
//!   * **A single-graph / single-group read is NOT flagged cross-shard** — the
//!     `is_cross_shard` gate stays false when every leg resolves to the same group,
//!     mirroring the write-side `GroupRouter::is_cross_shard` gate.
//!   * **Completion is explicit** — require-complete fails on an unavailable group,
//!     while allow-partial returns a typed failed-leg status and continuation.

use std::sync::Arc;

use tokio::sync::RwLock;

use super::harness::cluster::fixture;
use super::multi::MultiRaft;
use super::xread::{
    CompletionPolicy, CrossGraphReadErrorCode, CrossGraphReadRequest, CrossShardReader,
    ReadLegStatus, ReadPageErrorCode,
};
use super::{GroupId, RaftRequest};
use crate::protocol::{GraphType, Method};

const GROUP_A: GroupId = 500;
const GROUP_B: GroupId = 600;
const GRAPH_A: &str = "xreadShardA";
const GRAPH_B: &str = "xreadShardB";
const TENANT: &str = "xread-acme";

/// Bring up a one-node, two-group cluster (the `xshard_harness`/`placement_harness`
/// convention): `GROUP_A`/`GROUP_B` both on this node, each elected leader before the
/// test writes through them.
async fn bring_up(
    dir: &str,
    backend: fixture::Backend,
) -> (Arc<MultiRaft>, Arc<RwLock<crate::server::ServerState>>) {
    fixture::start_single_node_groups(
        dir,
        backend,
        crate::isolation::IsolationLayer::new(),
        "xread-test",
        &[GROUP_A, GROUP_B, super::DEFAULT_GROUP],
    )
    .await
}

/// Write ONE node into `graph` through `gid`'s Raft `client_write`.
async fn put_node(multi: &Arc<MultiRaft>, gid: GroupId, graph: &str, node_id: &str) {
    let group = multi.group(gid).await.expect("group running");
    let req = RaftRequest {
        graph_fname: crate::persist::sanitize(graph),
        graph_name: graph.to_string(),
        graph_type: GraphType::Global,
        committed_at_ms: 0,
        mutation: super::RaftMutationContext::internal("raft-xread-harness", graph, node_id, 0, 0),
        command: super::ReplicatedMutation::graph(
            Method::AddNode {
                node_id: node_id.to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({ "n": node_id }))
                    .unwrap(),
            },
            "xread-test",
        )
        .unwrap(),
    };
    group.client_write(req).await.expect("client_write");
}

// ─────────────────────────────────────────────────────────────────────────
// 1. A read spanning two groups returns the merged, correct result.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_cross_shard_merges_rows_from_two_groups() {
    let dir = fixture::fresh_dir("eg-xread", "merge");
    let backend = fixture::open_backend(&dir).expect("open redb");
    let (multi, _state) = bring_up(&dir, backend.clone()).await;

    multi.router().assign(GRAPH_A, GROUP_A);
    multi.router().assign(GRAPH_B, GROUP_B);
    put_node(&multi, GROUP_A, GRAPH_A, "a1").await;
    put_node(&multi, GROUP_A, GRAPH_A, "a2").await;
    put_node(&multi, GROUP_B, GRAPH_B, "b1").await;

    let reader = CrossShardReader::new(multi.clone());
    let result = reader
        .read(CrossGraphReadRequest::first_page(
            vec![GRAPH_A.to_string(), GRAPH_B.to_string()],
            100,
        ))
        .await
        .expect("cross-shard read");

    assert_eq!(result.legs.len(), 2);
    assert_eq!(result.legs[0].route.group, GROUP_A);
    assert_eq!(result.legs[1].route.group, GROUP_B);
    assert!(result.is_cross_shard(), "two distinct groups were spanned");

    let mut ids: Vec<&str> = result.merged.iter().map(|(id, _)| id.as_str()).collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["a1", "a2", "b1"],
        "merge must union both groups' rows"
    );
}

/// A read whose graphs ALL resolve to the same group is NOT cross-shard — the
/// single-group fast-path gate mirrors the write side's `GroupRouter::is_cross_shard`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_cross_shard_single_group_is_not_flagged_cross_shard() {
    let dir = fixture::fresh_dir("eg-xread", "single-group");
    let backend = fixture::open_backend(&dir).expect("open redb");
    let (multi, _state) = bring_up(&dir, backend.clone()).await;

    multi.router().assign(GRAPH_A, GROUP_A);
    multi.router().assign("alsoA", GROUP_A);
    put_node(&multi, GROUP_A, GRAPH_A, "a1").await;
    put_node(&multi, GROUP_A, "alsoA", "a2").await;

    let reader = CrossShardReader::new(multi.clone());
    let result = reader
        .read(CrossGraphReadRequest::first_page(
            vec![GRAPH_A.to_string(), "alsoA".to_string()],
            100,
        ))
        .await
        .expect("read over one group");

    assert!(!result.is_cross_shard());
    assert_eq!(result.groups_spanned().len(), 1);
    let mut ids: Vec<&str> = result.merged.iter().map(|(id, _)| id.as_str()).collect();
    ids.sort();
    assert_eq!(ids, vec!["a1", "a2"]);
}

// ─────────────────────────────────────────────────────────────────────────
// 2. Each leg routes via the PlacementCatalog (not a caller-computed route).
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_cross_shard_routes_each_leg_via_the_placement_catalog() {
    let dir = fixture::fresh_dir("eg-xread", "catalog-routing");
    let backend = fixture::open_backend(&dir).expect("open redb");
    let (multi, _state) = bring_up(&dir, backend.clone()).await;

    // Pick two workspace sub-keys whose stable hashes fall on either side of a split
    // point (the SAME recipe `placement_harness::split_lets_one_tenant_span_two_groups`
    // uses), so after splitting they resolve to DIFFERENT groups via the catalog —
    // NOT the engine's unplaced policy.
    let (ws_x, ws_y) = ("ws-x", "ws-y");
    let (h_x, h_y) = (super::multi::fnv1a(ws_x), super::multi::fnv1a(ws_y));
    let (lo_key, lo_hash, hi_key, hi_hash) = if h_x < h_y {
        (ws_x, h_x, ws_y, h_y)
    } else {
        (ws_y, h_y, ws_x, h_x)
    };
    let at = lo_hash + 1;
    assert!(
        at <= hi_hash,
        "chosen split point must separate the two keys"
    );

    multi
        .placement_split(TENANT, at, GROUP_A, GROUP_B)
        .await
        .expect("split");
    let graph_lo = format!("{TENANT}:{lo_key}");
    let graph_hi = format!("{TENANT}:{hi_key}");

    put_node(&multi, GROUP_A, &graph_lo, "lo1").await;
    put_node(&multi, GROUP_B, &graph_hi, "hi1").await;

    let reader = CrossShardReader::new(multi.clone());
    let result = reader
        .read(CrossGraphReadRequest::first_page(
            vec![graph_lo.clone(), graph_hi.clone()],
            100,
        ))
        .await
        .expect("cross-shard read via placement catalog");

    assert_eq!(result.legs[0].graph_name, graph_lo);
    assert_eq!(
        result.legs[0].route.group, GROUP_A,
        "the lower sub-range must route via the catalog"
    );
    assert_eq!(result.legs[1].graph_name, graph_hi);
    assert_eq!(
        result.legs[1].route.group, GROUP_B,
        "the upper sub-range must route via the catalog"
    );
    assert!(
        result.legs[0].route.epoch > 0,
        "a catalog route carries a real epoch"
    );
    assert_eq!(result.legs[0].route.epoch, result.legs[1].route.epoch);

    let mut ids: Vec<&str> = result.merged.iter().map(|(id, _)| id.as_str()).collect();
    ids.sort();
    assert_eq!(ids, vec!["hi1", "lo1"]);
}

// ─────────────────────────────────────────────────────────────────────────
// 3. An unreachable leg is a loud error, never a silent partial result.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_cross_shard_errors_loudly_on_a_leg_whose_group_is_not_running_here() {
    let dir = fixture::fresh_dir("eg-xread", "unreachable");
    let backend = fixture::open_backend(&dir).expect("open redb");
    let (multi, _state) = bring_up(&dir, backend.clone()).await;

    multi.router().assign(GRAPH_A, GROUP_A);
    // Route a second graph to a group that was NEVER created on this node.
    const GHOST_GROUP: GroupId = 999_999;
    multi.router().assign("ghost", GHOST_GROUP);
    put_node(&multi, GROUP_A, GRAPH_A, "a1").await;

    let reader = CrossShardReader::new(multi.clone());
    let err = reader
        .read(CrossGraphReadRequest::first_page(
            vec![GRAPH_A.to_string(), "ghost".to_string()],
            100,
        ))
        .await
        .expect_err("a leg whose group is not running here must error, not silently degrade");
    assert_eq!(err.code, CrossGraphReadErrorCode::RequiredLegFailed);
    assert_eq!(
        err.failed_legs,
        vec![("ghost".to_string(), ReadPageErrorCode::GroupUnavailable)]
    );

    let mut partial_request =
        CrossGraphReadRequest::first_page(vec![GRAPH_A.to_string(), "ghost".to_string()], 100);
    partial_request.completion = CompletionPolicy::AllowPartial;
    let partial = reader
        .read(partial_request)
        .await
        .expect("explicit partial");
    assert!(partial.partial);
    assert!(!partial.complete);
    assert!(matches!(
        partial.legs[1].status,
        ReadLegStatus::Failed(ReadPageErrorCode::GroupUnavailable)
    ));
}
