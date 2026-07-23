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

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use serde_json::json;
use tokio::sync::{RwLock, Semaphore};

use eg_plan::{Op, Plan};
use epistemic_graph::channels::ChannelManager;
use epistemic_graph::protocol::{GraphType, Method, Request, Response, ResultPayload};
use epistemic_graph::registry::{GraphMaterial, GraphMaterializer, GraphRegistry};
use epistemic_graph::server::{dispatch, ServerState};

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
        isolation: common::current_isolation(),
        channels: ChannelManager::new(),
        auth_secret: SECRET.to_string(),
        persist_dir: None,
        persistence: None,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
        open_txns: Arc::new(DashMap::new()),
        txn_id_gen: Arc::new(epistemic_graph::server::txn::TxnIdGen),
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
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(epistemic_graph::server::lake::LakeManager::new()),
    }))
}

fn req(id: u64, method: Method) -> Request {
    common::signed_request(SECRET, id, "__commons__", method)
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
    let resp = dispatch(&state, req(3, Method::UnifiedQuery { plan })).await;
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

/// Recovery installs the server index factory AFTER durable graph material has
/// already been replayed into each `GraphCore`. The newly registered spatial
/// index must therefore backfill those existing nodes before it reports itself
/// available; registration alone must never publish an empty index.
#[tokio::test]
async fn recovered_nodes_are_backfilled_before_spatial_index_is_available() {
    let state = state();
    for (id, point) in [("inside", "POINT (1 1)"), ("outside", "POINT (20 20)")] {
        let resp = dispatch(
            &state,
            req(
                if id == "inside" { 10 } else { 11 },
                Method::AddNode {
                    node_id: id.to_string(),
                    properties_msgpack: blob(json!({ "type": "City", "geometry": point })),
                },
            ),
        )
        .await;
        assert!(resp.error.is_none(), "AddNode {id}: {:?}", resp.error);
    }

    let plan = Plan::new(vec![Op::SpatialScan {
        layer: "City".into(),
        bbox: [0.0, 0.0, 10.0, 10.0],
    }]);
    let fallback_rows =
        rows_of(&dispatch(&state, req(12, Method::UnifiedQuery { plan: plan.clone() })).await);
    assert_eq!(fallback_rows[0].0, "inside");

    let core = state
        .read()
        .await
        .registry
        .get("__commons__")
        .expect("commons graph")
        .core
        .clone();
    let served = epistemic_graph::server::secondary_indexes::ServedSpatialIndex::new(core);
    assert!(
        !served.available(),
        "no registered index before recovery wiring"
    );

    state.write().await.registry.set_secondary_index_factory(
        epistemic_graph::server::secondary_indexes::ServerIndexFactory::new().into_arc(),
    );
    assert!(
        served.available(),
        "factory installation must backfill before publishing availability"
    );

    let indexed_rows = rows_of(&dispatch(&state, req(13, Method::UnifiedQuery { plan })).await);
    assert_eq!(indexed_rows, fallback_rows);
}

struct StaticMaterializer {
    graph: String,
    material: GraphMaterial,
}

impl GraphMaterializer for StaticMaterializer {
    fn materialize(&self, graph_name: &str) -> Option<GraphMaterial> {
        (graph_name == self.graph).then(|| self.material.clone())
    }
}

/// A paged lazy-open registers its spatial index after page one so live writes
/// can be observed, but the index must remain unavailable while its source graph
/// is partial. Consuming the final page performs one complete backfill and only
/// then enables pushdown.
#[test]
fn paged_lazy_open_advertises_spatial_only_after_final_page_backfill() {
    let name = "tenant:spatial-paged";
    let nodes = vec![
        (
            "a".to_string(),
            blob(json!({ "type": "City", "geometry": "POINT (1 1)" })),
        ),
        (
            "b".to_string(),
            blob(json!({ "type": "City", "geometry": "POINT (2 2)" })),
        ),
        (
            "c".to_string(),
            blob(json!({ "type": "City", "geometry": "POINT (30 30)" })),
        ),
    ];
    let mut registry = GraphRegistry::new();
    registry.register_catalog_only(name, GraphType::Agent, None);
    registry.set_materializer(Arc::new(StaticMaterializer {
        graph: name.to_string(),
        material: GraphMaterial {
            nodes,
            edges: Vec::new(),
            semantic: Vec::new(),
            ..GraphMaterial::default()
        },
    }));
    registry.set_secondary_index_factory(
        epistemic_graph::server::secondary_indexes::ServerIndexFactory::new().into_arc(),
    );

    let outcome = registry.open_lazy_paged(name, 1);
    let mut cursor = outcome.cursor.expect("more than one material page");
    let core = registry
        .get(name)
        .expect("resident after page one")
        .core
        .clone();
    let served = epistemic_graph::server::secondary_indexes::ServedSpatialIndex::new(core.clone());
    let partial = registry.materialization_manifest(name).unwrap();
    assert!(!partial.valid);
    assert!(core
        .indexes()
        .server_manifests()
        .iter()
        .all(|(_, manifest)| {
            manifest.validity == epistemic_graph::index::IndexValidity::Building
                && !manifest.completeness.complete
        }));
    assert!(
        !served.available(),
        "a one-page partial index must keep planner pushdown disabled"
    );

    while let Some(next) = registry.page_in(name, cursor, 1) {
        cursor = next;
    }

    assert!(
        served.available(),
        "the final page must complete backfill before publishing the index"
    );
    let complete = registry.materialization_manifest(name).unwrap();
    assert!(complete.valid);
    assert!(core
        .indexes()
        .server_manifests()
        .iter()
        .all(|(_, manifest)| {
            manifest.validity == epistemic_graph::index::IndexValidity::Valid
                && manifest.completeness.complete
                && manifest.covers(core.version())
        }));
    let mut hits = eg_plan::SpatialSource::query_bbox(&served, "City", [0.0, 0.0, 10.0, 10.0]);
    hits.sort();
    assert_eq!(hits, vec!["a".to_string(), "b".to_string()]);
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
    let resp = dispatch(&state, req(2, Method::UnifiedQuery { plan })).await;
    let rows = rows_of(&resp);
    let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    assert!(
        ids.contains(&"geom-keyed"),
        "with no persistent index installed, the `geom` alias must still be found via the \
         ephemeral fallback: {ids:?}"
    );
}
