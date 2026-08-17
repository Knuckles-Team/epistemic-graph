//! `Method::Viz { op }` served dispatch proof (D-VZ-1 lanes V4 "engine
//! integration" / V6 "graph-native marks") — exercises `VizOp::Render` end to
//! end against a LIVE in-process server (`Box::pin(dispatch(state, Request{ Method::Viz }))`,
//! the SAME served RPC surface `served_mining_tsdb_scan.rs`/
//! `served_query_completeness.rs` use), for every `VizDatasetSource` variant:
//!
//!  1. `InlineColumns` (20 rows) — a Scatter mark resolves at `LodTier::Direct`,
//!     `exact == true`, and decodes to a real PNG.
//!  2. `SyntheticScatterClusters` at a row count that forces `LodTier::Density`
//!     — `exact == false`, still a real PNG.
//!  3. `SyntheticGraph` at a small node count — `MarkKind::Graph` resolves at
//!     `LodTier::Direct`, `exact == true`.
//!  4. `SyntheticGraph` at a node count that forces `LodTier::Density` under a
//!     tight budget — `exact == false`. The bounded-draw-op-count PROPERTY
//!     itself (render-plan `ops.len()` independent of `node_count`) is proven
//!     directly against the render plan in
//!     `crates/eg-viz-export/src/render.rs`'s `graph_tests` module (this
//!     dispatch-level test proves REACHABILITY end to end through the wire
//!     protocol, not a duplicate of that lower-level proof).
//!
//! Module-gated on `viz-static-export`; runs under `--features viz-static-export`
//! (the default feature set already includes `full`, so `mining`/`tsdb`/etc. are
//! irrelevant here — a render never touches any of those).

#![cfg(feature = "viz-static-export")]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{RwLock, Semaphore};

use eg_types::viz::{VizColumnValues, VizDatasetSource, VizFormat, VizOp, VizRenderRequest};
use eg_viz_core::{EncodingSpec, Encodings, MarkKind, MarkSpec, ViewSpec};
use epistemic_graph::channels::ChannelManager;
use epistemic_graph::protocol::{Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "served-viz-dispatch-secret";
const PNG_SIGNATURE: [u8; 4] = [137, 80, 78, 71];

fn state() -> Arc<RwLock<ServerState>> {
    let (persist_dir, persistence) = common::tempdir_persistence();
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: common::current_isolation(),
        channels: ChannelManager::new(),
        #[cfg(feature = "viz-static-export")]
        viz_engine: None,
        auth_secret: SECRET.to_string(),
        persist_dir,
        persistence,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
        routed_write_coalescer: Arc::new(
            epistemic_graph::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
        ),
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

/// A local mirror of the handler's private `VizRenderResponse` shape (the SAME
/// "second unpackb over a top-level bytes result" idiom `ResultPayload::raw`'s
/// own doc describes for a real client) — `view_result` stays a generic
/// `serde_json::Value` (no raw-bytes ambiguity there: `eg_viz_core::ViewResult`
/// carries no `bin` fields), while `bytes` is decoded through the SAME
/// `serde_bytes` codec the handler encoded it with, so a MessagePack `bin` value
/// round-trips as real bytes rather than a generic array of numbers.
#[derive(serde::Deserialize)]
struct DecodedVizRenderResponse {
    view_result: serde_json::Value,
    #[allow(dead_code)]
    format: String,
    content_type: String,
    result_ref: String,
    cached: bool,
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
}

fn raw_result(resp: &Response) -> DecodedVizRenderResponse {
    assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
    match &resp.result {
        Some(ResultPayload::Raw(bytes)) => {
            rmp_serde::from_slice(bytes).expect("Raw payload must decode as a VizRenderResponse")
        }
        other => panic!("expected Raw result, got {other:?}"),
    }
}

fn scatter_spec(dataset_ref: &str) -> serde_json::Value {
    let spec = ViewSpec::new(vec![MarkSpec::new(MarkKind::Scatter, dataset_ref)
        .with_encodings(Encodings {
            x: Some(EncodingSpec::field("x")),
            y: Some(EncodingSpec::field("y")),
            ..Default::default()
        })]);
    serde_json::to_value(&spec).unwrap()
}

fn graph_spec(dataset_ref: &str) -> serde_json::Value {
    let spec = ViewSpec::new(vec![MarkSpec::new(MarkKind::Graph, dataset_ref)]);
    serde_json::to_value(&spec).unwrap()
}

fn assert_valid_png(bytes: &[u8]) {
    assert!(
        bytes.len() > 8,
        "PNG bytes too short: {} bytes",
        bytes.len()
    );
    assert_eq!(
        &bytes[0..4],
        &PNG_SIGNATURE,
        "must start with the PNG signature"
    );
}

#[tokio::test]
async fn inline_columns_scatter_resolves_at_direct_tier() {
    let state = state();

    let mut columns = std::collections::BTreeMap::new();
    let xs: Vec<f64> = (0..20).map(|i| i as f64).collect();
    let ys: Vec<f64> = (0..20).map(|i| (i * 2) as f64).collect();
    columns.insert("x".to_string(), VizColumnValues::F64(xs));
    columns.insert("y".to_string(), VizColumnValues::F64(ys));

    let render = VizRenderRequest {
        spec_json: scatter_spec("ds:inline"),
        dataset: Some(VizDatasetSource::InlineColumns { columns }),
        width_px: 400,
        height_px: 300,
        format: VizFormat::Png,
        max_primitives: 200_000,
        max_bytes: 50_000_000,
        dataset_ref: "ds:inline".to_string(),
    };

    let resp = Box::pin(dispatch(
        &state,
        req(
            1,
            Method::Viz {
                op: VizOp::Render(render),
            },
        ),
    ))
    .await;
    let payload = raw_result(&resp);

    assert_eq!(payload.view_result["lod_tier"], serde_json::json!("direct"));
    assert_eq!(payload.view_result["exact"], serde_json::json!(true));
    assert_eq!(payload.content_type, "image/png");
    assert_valid_png(&payload.bytes);
}

#[tokio::test]
async fn synthetic_scatter_clusters_at_high_row_count_resolves_at_density_tier() {
    let state = state();

    let render = VizRenderRequest {
        spec_json: scatter_spec("ds:synthetic-scatter"),
        dataset: Some(VizDatasetSource::SyntheticScatterClusters {
            row_count: 5_000_000,
            clusters: 5,
            seed: 42,
        }),
        width_px: 800,
        height_px: 600,
        format: VizFormat::Png,
        max_primitives: 2_000_000,
        max_bytes: 16 * 1024 * 1024,
        dataset_ref: "ds:synthetic-scatter".to_string(),
    };

    let resp = Box::pin(dispatch(
        &state,
        req(
            2,
            Method::Viz {
                op: VizOp::Render(render),
            },
        ),
    ))
    .await;
    let payload = raw_result(&resp);

    assert_eq!(
        payload.view_result["lod_tier"],
        serde_json::json!("density")
    );
    assert_eq!(payload.view_result["exact"], serde_json::json!(false));
    assert_valid_png(&payload.bytes);
}

#[tokio::test]
async fn synthetic_graph_small_resolves_at_direct_tier() {
    let state = state();

    let render = VizRenderRequest {
        spec_json: graph_spec("ds:synthetic-graph-small"),
        dataset: Some(VizDatasetSource::SyntheticGraph {
            node_count: 40,
            edge_count: 80,
            seed: 1,
        }),
        width_px: 400,
        height_px: 300,
        format: VizFormat::Png,
        max_primitives: 200_000,
        max_bytes: 50_000_000,
        dataset_ref: "ds:synthetic-graph-small".to_string(),
    };

    let resp = Box::pin(dispatch(
        &state,
        req(
            3,
            Method::Viz {
                op: VizOp::Render(render),
            },
        ),
    ))
    .await;
    let payload = raw_result(&resp);

    assert_eq!(payload.view_result["lod_tier"], serde_json::json!("direct"));
    assert_eq!(payload.view_result["exact"], serde_json::json!(true));
    assert_eq!(payload.view_result["row_count"], serde_json::json!(40));
    assert_valid_png(&payload.bytes);
}

/// The graph-specific bounded-draw-op-count PROPERTY itself is proven directly
/// against the render plan in `eg-viz-export`'s own
/// `render::graph_tests::large_graph_resolves_at_density_tier_with_bounded_op_count`
/// (asserting `plan.ops.len()` is independent of `node_count`, which the wire
/// response below does not expose). This test proves the SAME scenario is
/// reachable end to end through `Method::Viz`: a large graph never selects
/// `Direct`, always resolves to a real image, and the rendered PNG stays a
/// modest size regardless of the underlying node count — consistent with (if
/// not a substitute for) the crate-level bound.
#[tokio::test]
async fn synthetic_graph_large_resolves_at_density_tier() {
    let state = state();

    let render = VizRenderRequest {
        spec_json: graph_spec("ds:synthetic-graph-large"),
        dataset: Some(VizDatasetSource::SyntheticGraph {
            node_count: 300_000,
            edge_count: 600_000,
            seed: 2,
        }),
        width_px: 800,
        height_px: 600,
        format: VizFormat::Png,
        // Deliberately tight: 300_000 nodes * 3 primitives/row = 900_000, well
        // over this budget, forcing Density.
        max_primitives: 100_000,
        max_bytes: 50_000_000,
        dataset_ref: "ds:synthetic-graph-large".to_string(),
    };

    let resp = Box::pin(dispatch(
        &state,
        req(
            4,
            Method::Viz {
                op: VizOp::Render(render),
            },
        ),
    ))
    .await;
    let payload = raw_result(&resp);

    assert_eq!(
        payload.view_result["lod_tier"],
        serde_json::json!("density")
    );
    assert_eq!(payload.view_result["exact"], serde_json::json!(false));
    assert_eq!(payload.view_result["row_count"], serde_json::json!(300_000));
    assert_valid_png(&payload.bytes);
    assert!(
        payload.bytes.len() < 2_000_000,
        "a Density-tier graph render must stay a modest size regardless of node \
         count (bounded render-plan op count) — got {} bytes",
        payload.bytes.len()
    );
}

#[tokio::test]
async fn capability_matrix_is_reachable_over_the_wire() {
    let state = state();
    let resp = Box::pin(dispatch(
        &state,
        req(
            5,
            Method::Viz {
                op: VizOp::CapabilityMatrix,
            },
        ),
    ))
    .await;
    assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
    match &resp.result {
        Some(ResultPayload::Raw(bytes)) => {
            let value: serde_json::Value = rmp_serde::from_slice(bytes).unwrap();
            let entries = value["entries"].as_array().expect("entries array");
            assert!(!entries.is_empty(), "capability matrix must not be empty");
        }
        other => panic!("expected Raw result, got {other:?}"),
    }
}

#[tokio::test]
async fn invalid_spec_json_is_a_typed_error_not_a_panic() {
    let state = state();
    let render = VizRenderRequest {
        spec_json: serde_json::json!({"not": "a valid ViewSpec"}),
        dataset: Some(VizDatasetSource::SyntheticScatterClusters {
            row_count: 10,
            clusters: 1,
            seed: 0,
        }),
        width_px: 100,
        height_px: 100,
        format: VizFormat::Png,
        max_primitives: 1_000,
        max_bytes: 1_000_000,
        dataset_ref: "ds:bad-spec".to_string(),
    };
    let resp = Box::pin(dispatch(
        &state,
        req(
            6,
            Method::Viz {
                op: VizOp::Render(render),
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_some(),
        "an invalid spec_json must be a typed error"
    );
}

// ── Lane V4: persistent ColumnStore, content-addressed render cache,
// durable provenance — end to end through the SAME served RPC surface. ──

#[tokio::test]
async fn omitting_dataset_against_an_unknown_dataset_ref_is_an_explicit_unavailable_error() {
    let state = state();
    let render = VizRenderRequest {
        spec_json: scatter_spec("ds:never-ingested"),
        dataset: None,
        width_px: 200,
        height_px: 150,
        format: VizFormat::Png,
        max_primitives: 200_000,
        max_bytes: 50_000_000,
        dataset_ref: "ds:never-ingested".to_string(),
    };
    let resp = Box::pin(dispatch(
        &state,
        req(
            7,
            Method::Viz {
                op: VizOp::Render(render),
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_some(),
        "an unknown dataset_ref with no data supplied must be a clear \
         'unavailable' error, never a fabricated empty render"
    );
    let message = resp.error.unwrap();
    assert!(
        message.contains("unavailable"),
        "error message should explain the data is unavailable, got: {message}"
    );
}

#[tokio::test]
async fn a_second_request_omitting_dataset_reuses_the_persistently_ingested_data() {
    let state = state();

    let mut columns = std::collections::BTreeMap::new();
    let xs: Vec<f64> = (0..30).map(|i| i as f64).collect();
    let ys: Vec<f64> = (0..30).map(|i| (i * 3) as f64).collect();
    columns.insert("x".to_string(), VizColumnValues::F64(xs));
    columns.insert("y".to_string(), VizColumnValues::F64(ys));

    let first = VizRenderRequest {
        spec_json: scatter_spec("ds:persist"),
        dataset: Some(VizDatasetSource::InlineColumns { columns }),
        width_px: 400,
        height_px: 300,
        format: VizFormat::Png,
        max_primitives: 200_000,
        max_bytes: 50_000_000,
        dataset_ref: "ds:persist".to_string(),
    };
    let resp1 = Box::pin(dispatch(
        &state,
        req(
            8,
            Method::Viz {
                op: VizOp::Render(first),
            },
        ),
    ))
    .await;
    let payload1 = raw_result(&resp1);
    assert_valid_png(&payload1.bytes);

    // Second request: SAME dataset_ref, dataset OMITTED, and a DIFFERENT
    // format (SVG) -- proves this is a real second resolve against the
    // persistent store (not merely a cache hit of the first response), and
    // that omitting `dataset` genuinely resolves against already-ingested
    // data rather than erroring.
    let second = VizRenderRequest {
        spec_json: scatter_spec("ds:persist"),
        dataset: None,
        width_px: 400,
        height_px: 300,
        format: VizFormat::Svg,
        max_primitives: 200_000,
        max_bytes: 50_000_000,
        dataset_ref: "ds:persist".to_string(),
    };
    let resp2 = Box::pin(dispatch(
        &state,
        req(
            9,
            Method::Viz {
                op: VizOp::Render(second),
            },
        ),
    ))
    .await;
    let payload2 = raw_result(&resp2);
    assert_eq!(payload2.content_type, "image/svg+xml");
    assert!(String::from_utf8(payload2.bytes)
        .unwrap()
        .starts_with("<svg"));
    assert_eq!(payload2.view_result["row_count"], serde_json::json!(30));
}

#[tokio::test]
async fn an_identical_repeat_render_request_is_served_from_the_cache() {
    let state = state();

    let mut columns = std::collections::BTreeMap::new();
    columns.insert("x".to_string(), VizColumnValues::F64(vec![1.0, 2.0, 3.0]));
    columns.insert("y".to_string(), VizColumnValues::F64(vec![1.0, 4.0, 9.0]));

    let make_request = |dataset: Option<VizDatasetSource>| VizRenderRequest {
        spec_json: scatter_spec("ds:cache"),
        dataset,
        width_px: 300,
        height_px: 200,
        format: VizFormat::Png,
        max_primitives: 200_000,
        max_bytes: 50_000_000,
        dataset_ref: "ds:cache".to_string(),
    };

    let resp1 = Box::pin(dispatch(
        &state,
        req(
            10,
            Method::Viz {
                op: VizOp::Render(make_request(Some(VizDatasetSource::InlineColumns {
                    columns,
                }))),
            },
        ),
    ))
    .await;
    let payload1 = raw_result(&resp1);
    assert!(
        !payload1.cached,
        "the FIRST render must be a genuine cache miss"
    );

    let resp2 = Box::pin(dispatch(
        &state,
        req(
            11,
            Method::Viz {
                op: VizOp::Render(make_request(None)),
            },
        ),
    ))
    .await;
    let payload2 = raw_result(&resp2);
    assert!(
        payload2.cached,
        "an IDENTICAL repeat request (same spec/dataset/canvas/format) must hit \
         the V4 render cache"
    );
    assert_eq!(
        payload1.result_ref, payload2.result_ref,
        "identical renders must share the SAME content-addressed provenance identity"
    );
    assert_eq!(payload1.bytes, payload2.bytes);
}

#[tokio::test]
async fn render_provenance_is_queryable_after_a_render_and_absent_before() {
    let state = state();

    let before = Box::pin(dispatch(
        &state,
        req(
            12,
            Method::Viz {
                op: VizOp::RenderProvenance {
                    result_ref: "eg:viz_result:nonexistent".to_string(),
                },
            },
        ),
    ))
    .await;
    assert!(before.error.is_none());
    match &before.result {
        Some(ResultPayload::Raw(bytes)) => {
            let value: Option<serde_json::Value> = rmp_serde::from_slice(bytes).unwrap();
            assert!(
                value.is_none(),
                "an unknown result_ref must answer None, not an error"
            );
        }
        other => panic!("expected Raw result, got {other:?}"),
    }

    let mut columns = std::collections::BTreeMap::new();
    columns.insert("x".to_string(), VizColumnValues::F64(vec![1.0, 2.0]));
    columns.insert("y".to_string(), VizColumnValues::F64(vec![1.0, 2.0]));
    let render = VizRenderRequest {
        spec_json: scatter_spec("ds:provenance"),
        dataset: Some(VizDatasetSource::InlineColumns { columns }),
        width_px: 200,
        height_px: 150,
        format: VizFormat::Png,
        max_primitives: 200_000,
        max_bytes: 50_000_000,
        dataset_ref: "ds:provenance".to_string(),
    };
    let render_resp = Box::pin(dispatch(
        &state,
        req(
            13,
            Method::Viz {
                op: VizOp::Render(render),
            },
        ),
    ))
    .await;
    let payload = raw_result(&render_resp);

    let after = Box::pin(dispatch(
        &state,
        req(
            14,
            Method::Viz {
                op: VizOp::RenderProvenance {
                    result_ref: payload.result_ref.clone(),
                },
            },
        ),
    ))
    .await;
    assert!(after.error.is_none());
    match &after.result {
        Some(ResultPayload::Raw(bytes)) => {
            let value: Option<serde_json::Value> = rmp_serde::from_slice(bytes).unwrap();
            let record = value.expect("provenance record must exist after a real render");
            assert_eq!(record["dataset_ref"], serde_json::json!("ds:provenance"));
            assert_eq!(record["row_count"], serde_json::json!(2));
            assert_eq!(record["exact"], serde_json::json!(true));
        }
        other => panic!("expected Raw result, got {other:?}"),
    }
}
