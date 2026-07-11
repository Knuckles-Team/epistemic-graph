//! L37 (CONCEPT:EG-KG.storage.incremental-spatial) — a served `Op::SpatialScan` pushes down
//! into the MAINTAINED persistent `GraphSpatialIndex` (via `ServedSpatialIndex`) when a
//! `ServerIndexFactory` is installed, instead of rebuilding a throwaway packed Hilbert
//! R-tree per query. Mirrors `served_query_completeness.rs`'s
//! `served_ranktext_pushes_down_into_persistent_index_not_snapshot_fallback` — same
//! differential-proof shape, applied to the spatial leg.
//!
//! Everything goes through the SERVED RPC: `dispatch(state, Request{ Method::* })`.
//! Module-gated on `query` + `geo`; runs under `--features full`.
#![cfg(all(feature = "query", feature = "geo"))]

use std::sync::Arc;

use dashmap::DashMap;
use serde_json::json;
use tokio::sync::{RwLock, Semaphore};

use eg_plan::{Op, Plan};
use epistemic_graph::channels::ChannelManager;
use epistemic_graph::isolation::IsolationLayer;
use epistemic_graph::protocol::{Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{compute_auth_token, dispatch, ServerState};

const SECRET: &str = "served-spatial-completeness-secret";

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

fn state() -> Arc<RwLock<ServerState>> {
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: SECRET.to_string(),
        persist_dir: None,
        persistence: None,
        redb_authoritative: false,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(
            epistemic_graph::write_coalescer::WriteCoalescerRegistry::from_env(),
        ),
        open_txns: Arc::new(DashMap::new()),
        txn_id_gen: Arc::new(epistemic_graph::server::txn::TxnIdGen::default()),
        txn_ttl_secs: 300,
        txn_max_per_graph: 256,
        txn_max_per_agent: 256,
        #[cfg(feature = "blob")]
        blob: None,
        #[cfg(feature = "blob")]
        blob_cursor_ttl_secs: 300,
        #[cfg(feature = "raft")]
        raft: None,
        #[cfg(feature = "raft")]
        multi_raft: None,
        #[cfg(feature = "tsdb")]
        tsdb_store: None,
        #[cfg(feature = "rdf-redb")]
        rdf_quads: None,
        #[cfg(feature = "streaming")]
        cdc: Some(Arc::new(epistemic_graph::server::cdc::CdcHub::new())),
        #[cfg(feature = "wasm-udf")]
        udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: Arc::new(parking_lot::Mutex::new(
            epistemic_graph::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: Arc::new(DashMap::new()),
        #[cfg(feature = "kv")]
        kv: None,
        #[cfg(feature = "dataset-handle")]
        dataset_handles: std::sync::Arc::new(
            epistemic_graph::server::dataset_handle::DatasetHandleRegistry::new(),
        ),
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(epistemic_graph::server::lake::LakeManager::new()),
    }))
}

fn req(id: u64, method: Method) -> Request {
    Request {
        id,
        graph: "__commons__".into(),
        auth_token: compute_auth_token(SECRET, id),
        agent_id: None,
        method,
    }
}

/// Decode a served `UnifiedQuery` result (a `Raw` MessagePack `Vec<(id, score|nil)>`).
fn rows_of(resp: &Response) -> Vec<(String, Option<f32>)> {
    assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
    match &resp.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).expect("row decode"),
        other => panic!("expected Raw result, got {other:?}"),
    }
}

/// L37 — a served `SpatialScan` pushes down into the MAINTAINED persistent
/// `GraphSpatialIndex`, NOT eg-plan's ephemeral per-query fallback.
///
/// Proven DIFFERENTIALLY: the persistent index derives a node's bbox from a FIXED
/// canonical geometry key (`GraphSpatialIndex`'s `SPATIAL_GEOMETRY_KEY = "geometry"`,
/// see `server::secondary_indexes`), while the snapshot-derived fallback
/// (`geometry_from_value` in `eg-plan`) accepts EITHER `geometry` OR the `geom` alias.
/// So a node whose geometry lives under the NON-canonical `geom` key matches the
/// fallback but is INVISIBLE to the persistent index. A served `SpatialScan` returning
/// hits ONLY for the canonical-keyed node — never the `geom`-keyed one — is possible
/// ONLY if the served path is genuinely searching the persistent index; a silent
/// fallback to the snapshot-derived R-tree (the per-scan-rebuild bug L37 closes) would
/// instead match BOTH.
#[tokio::test]
async fn served_spatial_scan_pushes_down_into_persistent_index_not_snapshot_fallback() {
    let state = state();
    // Install the SAME server-layer secondary-index factory `main.rs` wires at startup —
    // no text/tsdb config needed here, only the always-on `geo` registration.
    state.write().await.registry.set_secondary_index_factory(
        epistemic_graph::server::secondary_indexes::ServerIndexFactory::new().into_arc(),
    );

    // `canonical` carries its geometry under the persistent index's own `geometry` key
    // (matches BOTH the persistent index and the ephemeral fallback). `noncanonical`
    // carries the SAME point under `geom` — the fallback's alias, which the persistent
    // index does not recognize.
    for (id, key) in [("canonical", "geometry"), ("noncanonical", "geom")] {
        let r = dispatch(
            &state,
            req(
                if id == "canonical" { 1 } else { 2 },
                Method::AddNode {
                    node_id: id.to_string(),
                    properties_msgpack: blob(json!({ "type": "City", key: "POINT (1 1)" })),
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "AddNode {id}: {:?}", r.error);
    }

    let plan = Plan::new(vec![Op::SpatialScan {
        layer: "City".into(),
        bbox: [0.0, 0.0, 10.0, 10.0],
    }]);
    let resp = dispatch(
        &state,
        req(
            3,
            Method::UnifiedQuery {
                plan,
                reorder_filter_selectivity: None,
            },
        ),
    )
    .await;
    let rows = rows_of(&resp);
    let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    assert!(
        ids.contains(&"canonical"),
        "the canonical `geometry`-keyed node must hit the persistent index: {ids:?}"
    );
    assert!(
        !ids.contains(&"noncanonical"),
        "a `geom`-keyed node hitting here would prove the served path silently fell back \
         to the snapshot-derived R-tree (which also accepts the `geom` alias) instead of the \
         persistent index (which does not — a per-scan-rebuild regression this test guards \
         against): {ids:?}"
    );
}

/// A served `SpatialScan` still returns the correct hits when NO `ServerIndexFactory` is
/// installed at all (the prior v1 ephemeral-fallback behavior, unregressed): both nodes
/// above would be found via the `geom`/`geometry`-accepting fallback in that case — this
/// test uses a FRESH graph (no factory) to lock in that the fallback path itself is
/// unaffected by L37's addition.
#[tokio::test]
async fn served_spatial_scan_without_factory_keeps_ephemeral_fallback() {
    let state = state(); // no secondary-index factory installed at all
    let r = dispatch(
        &state,
        req(
            1,
            Method::AddNode {
                node_id: "geom-keyed".to_string(),
                properties_msgpack: blob(json!({ "type": "City", "geom": "POINT (1 1)" })),
            },
        ),
    )
    .await;
    assert!(r.error.is_none(), "AddNode: {:?}", r.error);

    let plan = Plan::new(vec![Op::SpatialScan {
        layer: "City".into(),
        bbox: [0.0, 0.0, 10.0, 10.0],
    }]);
    let resp = dispatch(
        &state,
        req(
            2,
            Method::UnifiedQuery {
                plan,
                reorder_filter_selectivity: None,
            },
        ),
    )
    .await;
    let rows = rows_of(&resp);
    let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    assert!(
        ids.contains(&"geom-keyed"),
        "with no persistent index installed, the `geom` alias must still be found via the \
         ephemeral fallback: {ids:?}"
    );
}
