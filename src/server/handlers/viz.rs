//! `Method::Viz { op }` (D-VZ-1 lanes V4 "engine integration" / V6 "graph-native
//! marks"), feature `viz-static-export`: resolve a caller-provided
//! `eg_viz_core::ViewSpec` against a dataset and render it to static PNG/SVG/PDF
//! bytes, fetch the mark x surface capability matrix, or look up a rendered
//! view's durable provenance.
//!
//! NOT graph-scoped: a render never reads a live `GraphCore` — so this module
//! self-routes in `dispatch.rs`'s top-level match, ahead of the per-graph
//! `dispatch_graph_op` chain — exactly like `handlers::jobs`/`handlers::statechart`.
//! Gated on the facade's `viz-static-export` feature (a deliberate deviation from
//! gating on bare `viz`: this handler needs a real `eg_viz_columnstore::ColumnStore`
//! and `eg_viz_export::ColumnStoreExportBackend` to do anything, which only exist
//! at that tier — see the root `Cargo.toml`'s `viz-static-export` doc comment).
//!
//! ## Scope — full V4 (upgraded from the former V4-LITE slice)
//!
//! Previously this handler built a FRESH, ephemeral, per-request `ColumnStore`
//! from `VizRenderRequest::dataset` and discarded it after the response — no
//! reuse, no cache, no provenance. It now holds a PERSISTENT engine state
//! (`crate::server::viz_engine::VizEngineState`, lazily created on first use —
//! see [`engine_state`]) across requests:
//!
//! - **Persistent ColumnStore.** `VizRenderRequest::dataset` is now `Option`:
//!   a caller who already ingested a `dataset_ref` (any prior `VizOp::Render`
//!   call that supplied data) may omit it on a later request — e.g. a
//!   different `ViewSpec`/canvas/format over the SAME data, or a pan/zoom
//!   follow-up — without resending the dataset over the wire. Omitting it
//!   against an unknown `dataset_ref` is a clear, typed "unavailable" error,
//!   never a fabricated empty render (see [`render`]'s "no usable data" path).
//! - **Content-addressed render cache**, keyed by
//!   `crate::server::viz_engine::render_cache_key` — see that function's own
//!   doc, and this handler's [`render`], for why it is keyed on the dataset's
//!   CONTENT fingerprint rather than a version counter any unrelated write
//!   would bump.
//! - **Durable render provenance** (`crate::server::viz_provenance`),
//!   queryable via `VizOp::RenderProvenance`.
//!
//! ## Scope — V6-lite graph marks, static-export only
//!
//! `MarkKind::Graph` renders through `eg_viz_export::render`'s `resolve_graph`
//! path (private to that crate; reached transitively through
//! `ColumnStoreExportBackend::resolve` — see that function's doc for the exact
//! dataset convention [`VizDatasetSource::SyntheticGraph`] ingestion below
//! follows: `dataset_ref` names a one-column node dataset,
//! `"{dataset_ref}:edges"` names the `src`/`dst` I64 edge dataset). This is
//! static PNG/SVG/PDF export only — no WebGPU pan/zoom/picking (that surface is
//! the interactive HTTP listener, `crate::server::viz_interactive`, lane V3b).
//!
//! ## Bounds
//!
//! Canvas dimensions are clamped to `[MIN_CANVAS_PX, MAX_CANVAS_PX]` px each way.
//! Synthetic/inline dataset sizes are capped (see the `MAX_*` constants below) so
//! this read-only, non-graph-ACL-scoped surface cannot be used to force unbounded
//! server-side allocation/compute from a single request.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::RwLock;

use eg_types::viz::{VizColumnValues, VizDatasetSource, VizFormat, VizOp, VizRenderRequest};
use eg_viz_columnstore::{ColumnData, ColumnInput, ColumnStore};
use eg_viz_core::{
    CapabilityMatrix, FrameBudget, StaticExportBackend, StaticExportFormat, ViewSpec,
};
use eg_viz_export::ColumnStoreExportBackend;

use crate::protocol::{Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use crate::server::state::ServerState;
use crate::server::viz_engine::{render_cache_key, CachedRender, VizEngineState};
use crate::server::viz_provenance::VizProvenanceRecord;

const MIN_CANVAS_PX: u32 = 16;
const MAX_CANVAS_PX: u32 = 8192;
const MAX_SYNTHETIC_SCATTER_ROWS: u64 = 200_000_000;
const MAX_SYNTHETIC_CLUSTERS: u32 = 10_000;
const MAX_SYNTHETIC_GRAPH_NODES: u64 = 2_000_000;
const MAX_SYNTHETIC_GRAPH_EDGES: u64 = 10_000_000;
const MAX_INLINE_COLUMN_ROWS: usize = 20_000_000;

/// Handle `Method::Viz { op }`. `authority` is currently unused for
/// render/capability/provenance (a render touches no owner-scoped durable
/// state and provenance is engine-wide, not per-actor ACL'd) — kept in the
/// signature for parity with the sibling self-routed handlers and so a future
/// per-caller rate limit has an authenticated identity ready to hand.
pub(crate) async fn handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _authority: &CarrierAuthority,
    op: VizOp,
) -> Response {
    match op {
        VizOp::CapabilityMatrix => Response::ok(
            req_id,
            ResultPayload::raw(&CapabilityMatrix::default_matrix()),
        ),
        VizOp::Render(request) => {
            let engine = engine_state(state).await;
            match render(&engine, request) {
                Ok(response) => Response::ok(req_id, ResultPayload::raw(&response)),
                Err(message) => Response::err(req_id, message),
            }
        }
        VizOp::RenderProvenance { result_ref } => {
            let engine = engine_state(state).await;
            let record: Option<VizProvenanceRecord> = engine.provenance.get(&result_ref);
            Response::ok(req_id, ResultPayload::raw(&record))
        }
    }
}

/// Fetch (lazily creating on first use) this server's persistent viz engine
/// state. Double-checked: the common case (already initialized) only ever
/// takes a READ lock on the coarse `ServerState`; the one-time init path
/// briefly escalates to a write lock. `persist_dir` (if configured) is reused
/// as-is — durable render provenance lives alongside the graph engine's own
/// durable state, under the SAME operator-configured directory, in its own
/// `viz_provenance.redb` file (see `viz_provenance::VizProvenanceStore::open`).
/// `pub(crate)` so `server::viz_interactive` (lane V3b, feature
/// `viz-interactive`) shares the SAME persistent engine (and therefore the
/// SAME render cache + dataset store) the RPC path uses — one engine, two
/// transports, never two independent ColumnStores drifting apart.
pub(crate) async fn engine_state(state: &Arc<RwLock<ServerState>>) -> Arc<VizEngineState> {
    if let Some(engine) = state.read().await.viz_engine.clone() {
        return engine;
    }
    let mut guard = state.write().await;
    if guard.viz_engine.is_none() {
        let persist_dir = guard.persist_dir.clone();
        guard.viz_engine = Some(Arc::new(VizEngineState::new(persist_dir.as_deref())));
    }
    guard
        .viz_engine
        .clone()
        .expect("just initialized on the line above")
}

/// The `ResultPayload::Raw`-encoded response shape for `VizOp::Render` — decoded
/// client-side with a second `unpackb`, per `ResultPayload::raw`'s own doc.
#[derive(Debug, Serialize)]
struct VizRenderResponse {
    view_result: eg_viz_core::ViewResult,
    format: VizFormat,
    content_type: &'static str,
    /// The durable provenance key this render was (or will be) recorded under
    /// — feed it to `VizOp::RenderProvenance` to fetch the full record.
    result_ref: String,
    /// Whether this response was served from the content-addressed render
    /// cache (lane V4) with zero recomputation, or freshly resolved+rendered.
    /// Diagnostic only — the `bytes`/`view_result` are identical either way.
    cached: bool,
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
}

/// Resolve `request` against `engine`'s persistent ColumnStore, honoring the
/// content-addressed render cache and recording durable provenance on a miss.
///
/// **Never fabricates data.** If `request.dataset` is omitted and
/// `request.dataset_ref` names no dataset this engine has ever ingested (or
/// once held but can no longer resolve a content fingerprint for — the same
/// check either way), this returns an explicit, typed "no usable data" error
/// — never a silently empty render that could be mistaken for a real
/// zero-row result.
fn render(engine: &VizEngineState, request: VizRenderRequest) -> Result<VizRenderResponse, String> {
    let spec: ViewSpec = serde_json::from_value(request.spec_json)
        .map_err(|e| format!("failed to parse spec_json as a ViewSpec: {e}"))?;
    spec.validate()
        .map_err(|errors| format!("invalid ViewSpec: {}", errors.join("; ")))?;

    let width_px = request.width_px.clamp(MIN_CANVAS_PX, MAX_CANVAS_PX);
    let height_px = request.height_px.clamp(MIN_CANVAS_PX, MAX_CANVAS_PX);
    let budget = FrameBudget::new(request.max_primitives.max(1), request.max_bytes.max(1));

    if let Some(dataset) = request.dataset {
        let mut store = engine.store.write();
        ingest_dataset(&mut store, &request.dataset_ref, dataset)?;
    }

    let fingerprint = {
        let store = engine.store.read();
        store.content_fingerprint(&request.dataset_ref)
    };
    let fingerprint = fingerprint.ok_or_else(|| {
        format!(
            "dataset `{}` is unavailable: no data was supplied on this request and no \
             matching dataset has been previously ingested — refusing to render (an empty \
             chart here would be indistinguishable from a real zero-row result)",
            request.dataset_ref
        )
    })?;

    let query_hash = eg_viz_core::query_hash(&spec, &request.dataset_ref, fingerprint)
        .map_err(|e| format!("failed to compute query hash: {e}"))?;
    let cache_key = render_cache_key(&query_hash, width_px, height_px, request.format, budget);
    let result_ref = provenance_result_ref(&cache_key);

    if let Some(cached) = engine.cache_get(&cache_key) {
        return Ok(VizRenderResponse {
            view_result: cached.view_result,
            format: request.format,
            content_type: cached.content_type,
            result_ref,
            cached: true,
            bytes: cached.bytes.to_vec(),
        });
    }

    let (format, content_type) = match request.format {
        VizFormat::Png => (StaticExportFormat::Png, "image/png"),
        VizFormat::Svg => (StaticExportFormat::Svg, "image/svg+xml"),
        VizFormat::Pdf => (StaticExportFormat::Pdf, "application/pdf"),
    };

    let (view_result, bytes) = {
        let store = engine.store.read();
        let backend = ColumnStoreExportBackend::new(&store, width_px, height_px);
        // `fingerprint` (not a graph version or wall-clock value) is passed as
        // the snapshot_version `resolve` folds into its OWN internal
        // `query_hash` computation — so the ViewResult's `query_hash` this
        // produces is BYTE-IDENTICAL to the one computed above, by
        // construction (same function, same three inputs).
        let view_result = backend
            .resolve(&spec, &request.dataset_ref, budget, fingerprint)
            .map_err(|e| format!("failed to resolve ViewSpec against dataset: {e}"))?;
        let bytes = backend
            .export(&spec, &view_result, format)
            .map_err(|e| format!("failed to export rendered plan: {e}"))?;
        (view_result, bytes)
    };

    debug_assert_eq!(
        view_result.query_hash, query_hash,
        "engine-level cache key and eg-viz-export's own query_hash must agree"
    );

    engine.cache_put(
        cache_key,
        CachedRender {
            view_result: view_result.clone(),
            content_type,
            bytes: Arc::from(bytes.clone()),
        },
    );

    let algo_name = spec
        .marks
        .first()
        .map(|m| eg_viz_core::viz_algorithm_name(m.kind))
        .unwrap_or("empty");
    let _ = engine.provenance.put_if_absent(VizProvenanceRecord {
        result_ref: result_ref.clone(),
        query_hash: query_hash.clone(),
        dataset_ref: request.dataset_ref.clone(),
        content_fingerprint: fingerprint,
        algo_family: eg_viz_core::VIZ_JOB_FAMILY.to_string(),
        algo_name: algo_name.to_string(),
        lod_tier: format!("{:?}", view_result.lod_tier),
        exact: view_result.exact,
        row_count: view_result.row_count,
        width_px,
        height_px,
        format: format!("{:?}", request.format),
        wall_time_ms: view_result.wall_time_ms,
        produced_at_unix_ms: view_result.produced_at_unix_ms,
    });

    Ok(VizRenderResponse {
        view_result,
        format: request.format,
        content_type,
        result_ref,
        cached: false,
        bytes,
    })
}

/// The durable provenance identity for a render — reuses the SAME
/// content-addressed payload [`render_cache_key`] already computed (both are
/// "the identity of this exact render"), under a distinct namespace prefix so
/// a provenance lookup key is never confused with the internal cache key by a
/// caller inspecting logs/traces.
fn provenance_result_ref(cache_key: &str) -> String {
    format!(
        "eg:viz_result:{}",
        cache_key.trim_start_matches("eg:viz_render:")
    )
}

/// Ingest `dataset` into `store` under `dataset_ref`, per [`VizDatasetSource`]'s
/// own documented shape. `SyntheticGraph` additionally ingests a SECOND dataset
/// under `"{dataset_ref}:edges"` — see the module doc's "Scope — V6-lite" section
/// for why (mirrors `eg_viz_export::render::resolve_graph`'s dataset convention).
fn ingest_dataset(
    store: &mut ColumnStore,
    dataset_ref: &str,
    dataset: VizDatasetSource,
) -> Result<(), String> {
    match dataset {
        VizDatasetSource::InlineColumns { columns } => {
            ingest_inline_columns(store, dataset_ref, columns)
        }
        VizDatasetSource::SyntheticScatterClusters {
            row_count,
            clusters,
            seed,
        } => ingest_synthetic_scatter_clusters(store, dataset_ref, row_count, clusters, seed),
        VizDatasetSource::SyntheticGraph {
            node_count,
            edge_count,
            seed,
        } => ingest_synthetic_graph(store, dataset_ref, node_count, edge_count, seed),
    }
}

fn ingest_inline_columns(
    store: &mut ColumnStore,
    dataset_ref: &str,
    columns: BTreeMap<String, VizColumnValues>,
) -> Result<(), String> {
    if columns.is_empty() {
        return Err("VizDatasetSource::InlineColumns must supply at least one column".to_string());
    }
    let mut inputs = Vec::with_capacity(columns.len());
    for (name, values) in columns {
        let data = match values {
            VizColumnValues::F64(v) => {
                if v.len() > MAX_INLINE_COLUMN_ROWS {
                    return Err(format!(
                        "column `{name}` has {} rows, exceeding the maximum inline row count ({MAX_INLINE_COLUMN_ROWS})",
                        v.len()
                    ));
                }
                ColumnData::F64(v)
            }
            VizColumnValues::Utf8(v) => {
                if v.len() > MAX_INLINE_COLUMN_ROWS {
                    return Err(format!(
                        "column `{name}` has {} rows, exceeding the maximum inline row count ({MAX_INLINE_COLUMN_ROWS})",
                        v.len()
                    ));
                }
                ColumnData::Utf8(v)
            }
        };
        inputs.push(ColumnInput::new(name, data));
    }
    store
        .ingest_columns(dataset_ref, inputs)
        .map_err(|e| e.to_string())
        .map(|_| ())
}

fn ingest_synthetic_scatter_clusters(
    store: &mut ColumnStore,
    dataset_ref: &str,
    row_count: u64,
    clusters: u32,
    seed: u64,
) -> Result<(), String> {
    if row_count > MAX_SYNTHETIC_SCATTER_ROWS {
        return Err(format!(
            "row_count {row_count} exceeds the maximum synthetic scatter row count ({MAX_SYNTHETIC_SCATTER_ROWS})"
        ));
    }
    let clusters = clusters.max(1);
    if clusters > MAX_SYNTHETIC_CLUSTERS {
        return Err(format!(
            "clusters {clusters} exceeds the maximum synthetic cluster count ({MAX_SYNTHETIC_CLUSTERS})"
        ));
    }
    let (xs, ys) = synthetic_scatter_clusters(row_count, clusters, seed);
    store
        .ingest_columns(
            dataset_ref,
            vec![
                ColumnInput::new("x", ColumnData::F64(xs)),
                ColumnInput::new("y", ColumnData::F64(ys)),
            ],
        )
        .map_err(|e| e.to_string())
        .map(|_| ())
}

fn ingest_synthetic_graph(
    store: &mut ColumnStore,
    dataset_ref: &str,
    node_count: u64,
    edge_count: u64,
    seed: u64,
) -> Result<(), String> {
    if node_count > MAX_SYNTHETIC_GRAPH_NODES {
        return Err(format!(
            "node_count {node_count} exceeds the maximum synthetic graph node count ({MAX_SYNTHETIC_GRAPH_NODES})"
        ));
    }
    if edge_count > MAX_SYNTHETIC_GRAPH_EDGES {
        return Err(format!(
            "edge_count {edge_count} exceeds the maximum synthetic graph edge count ({MAX_SYNTHETIC_GRAPH_EDGES})"
        ));
    }
    let edges = synthetic_graph_edges(node_count, edge_count, seed);

    store
        .ingest_columns(
            dataset_ref,
            vec![ColumnInput::new(
                "node_id",
                ColumnData::I64((0..node_count as i64).collect()),
            )],
        )
        .map_err(|e| e.to_string())?;

    let src: Vec<i64> = edges.iter().map(|&(s, _)| s as i64).collect();
    let dst: Vec<i64> = edges.iter().map(|&(_, d)| d as i64).collect();
    store
        .ingest_columns(
            &format!("{dataset_ref}:edges"),
            vec![
                ColumnInput::new("src", ColumnData::I64(src)),
                ColumnInput::new("dst", ColumnData::I64(dst)),
            ],
        )
        .map_err(|e| e.to_string())
        .map(|_| ())
}

/// splitmix64 — a small, dependency-free, deterministic PRNG (Vigna's
/// well-known public-domain construction), the SAME hand-rolled generator
/// `eg_viz_export::graph_layout` uses (that copy is crate-private, so this is a
/// separate small copy rather than a new shared public dependency — this repo
/// avoids adding `rand`/`rand_chacha` here unless already resolved and reachable
/// from this crate, which it is not for a `viz-static-export`-only build). Not
/// cryptographic; used purely as a deterministic bit source for synthetic demo
/// data, never for anything security-sensitive.
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A uniform `f64` in `[0, 1)`.
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Deterministic, seeded, clustered pseudo-scatter — `clusters` distinct
/// Gaussian-ish blobs across a `0..1_000_000` domain, mirroring the algorithm
/// `crates/eg-viz-export/examples/render_large_n_tier.rs` already uses for
/// exactly this kind of high-density demo data (generalized here from that
/// example's fixed 5 clusters to a caller-chosen `clusters` count).
fn synthetic_scatter_clusters(row_count: u64, clusters: u32, seed: u64) -> (Vec<f64>, Vec<f64>) {
    let clusters = clusters as u64;
    let mut center_rng = SplitMix64::new(seed ^ 0xC0FF_EE00_D15E_A5E5);
    let centers: Vec<(f64, f64)> = (0..clusters)
        .map(|_| {
            (
                center_rng.next_f64() * 1_000_000.0,
                center_rng.next_f64() * 1_000_000.0,
            )
        })
        .collect();

    let mut xs = Vec::with_capacity(row_count as usize);
    let mut ys = Vec::with_capacity(row_count as usize);
    for i in 0..row_count {
        let (cx, cy) = centers[(i % clusters) as usize];
        let mut jitter_rng = SplitMix64::new(seed ^ i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let jx = (jitter_rng.next_f64() - 0.5) * 90_000.0;
        let jy = (jitter_rng.next_f64() - 0.5) * 90_000.0;
        xs.push((cx + jx).clamp(0.0, 1_000_000.0));
        ys.push((cy + jy).clamp(0.0, 1_000_000.0));
    }
    (xs, ys)
}

/// A deterministic seeded random graph: node ids `0..node_count`, `edge_count`
/// edges among them (no self-loops; dedup not required — matches
/// `VizDatasetSource::SyntheticGraph`'s own documented shape). `node_count < 2`
/// yields zero edges (no valid non-self-loop pair exists) rather than looping.
fn synthetic_graph_edges(node_count: u64, edge_count: u64, seed: u64) -> Vec<(u32, u32)> {
    if node_count < 2 || edge_count == 0 {
        return Vec::new();
    }
    let mut rng = SplitMix64::new(seed);
    let mut edges = Vec::with_capacity(edge_count as usize);
    while (edges.len() as u64) < edge_count {
        let a = rng.next_u64() % node_count;
        let b = rng.next_u64() % node_count;
        if a != b {
            edges.push((a as u32, b as u32));
        }
    }
    edges
}
