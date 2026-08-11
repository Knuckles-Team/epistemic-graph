//! Graph-native marks demo (D-VZ-1 lane V6, "graph-native marks" — V6-lite: no
//! WebGPU pan/zoom/picking, static PNG export only). Ingests two REAL (if
//! synthetic, seeded, deterministic) graphs into a real `ColumnStore` under
//! this crate's own `MarkKind::Graph` dataset convention (see
//! `crate::render::resolve_graph`'s doc), resolves them through the real
//! `select_tier` -> `crate::graph_layout::layout` -> reduction pipeline, and
//! exports PNG. Proves two things end to end, not just at the unit-test level:
//! a small graph renders every node/edge exactly (`LodTier::Direct`), and a
//! large graph never attempts that — it lands on `LodTier::Density` with a
//! render-plan op count bounded independent of node count, exactly like the
//! existing `render_large_n_tier` scatter proof.
//!
//! Run: `cargo run -p eg-viz-export --example render_graph_demo --release`
//! (optionally set `EG_VIZ_DEMO_OUT_DIR` to control where the two PNGs land;
//! defaults to `/tmp`).

use std::time::Instant;

use eg_viz_columnstore::{ColumnData, ColumnInput, ColumnStore};
use eg_viz_core::{
    FrameBudget, LodTier, MarkKind, MarkSpec, StaticExportBackend, StaticExportFormat, ViewSpec,
};
use eg_viz_export::ColumnStoreExportBackend;

/// splitmix64 — the SAME small, dependency-free, deterministic PRNG
/// `eg_viz_export::graph_layout` uses, reused here purely to generate a
/// deterministic seeded random graph (this example's own concern; the layout
/// module's copy stays crate-private).
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
}

/// A deterministic seeded random graph: node ids `0..node_count`, `edge_count`
/// edges among them (no self-loops; dedup not required — mirrors
/// `eg_types::viz::VizDatasetSource::SyntheticGraph`'s own documented shape).
fn synthetic_graph(node_count: u64, edge_count: u64, seed: u64) -> Vec<(u32, u32)> {
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

/// Ingest a `MarkKind::Graph` dataset under `eg-viz-export`'s own convention:
/// `dataset_ref` gets a one-column node dataset, `"{dataset_ref}:edges"` gets
/// the `src`/`dst` I64 edge dataset.
fn ingest_graph(store: &mut ColumnStore, dataset_ref: &str, node_count: u64, edges: &[(u32, u32)]) {
    store
        .ingest_columns(
            dataset_ref,
            vec![ColumnInput::new(
                "node_id",
                ColumnData::I64((0..node_count as i64).collect()),
            )],
        )
        .unwrap();
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
        .unwrap();
}

fn graph_spec(dataset_ref: &str, title: &str) -> ViewSpec {
    let mut spec = ViewSpec::new(vec![MarkSpec::new(MarkKind::Graph, dataset_ref)]);
    spec.title = Some(title.to_string());
    spec.theme.palette = vec!["#4C78A8".to_string()];
    spec.theme.background = Some("#FFFFFF".to_string());
    spec
}

fn out_dir() -> String {
    std::env::var("EG_VIZ_DEMO_OUT_DIR").unwrap_or_else(|_| "/tmp".to_string())
}

fn small_graph_demo() {
    const NODE_COUNT: u64 = 300;
    const EDGE_COUNT: u64 = 600;

    let edges = synthetic_graph(NODE_COUNT, EDGE_COUNT, 1);
    let mut store = ColumnStore::new();
    ingest_graph(&mut store, "ds:graph-small", NODE_COUNT, &edges);

    let spec = graph_spec("ds:graph-small", "V6-lite: 300-node graph (Direct tier)");
    let backend = ColumnStoreExportBackend::new(&store, 900, 700);
    let budget = FrameBudget::new(2_000_000, 16 * 1024 * 1024);

    let started = Instant::now();
    let result = backend
        .resolve(&spec, "ds:graph-small", budget, 1)
        .expect("resolve the small graph");
    let elapsed = started.elapsed();

    println!(
        "small graph: {NODE_COUNT} nodes, {EDGE_COUNT} edges -> {:?} (exact={}) in {:.3}s",
        result.lod_tier,
        result.exact,
        elapsed.as_secs_f64()
    );
    assert_eq!(
        result.lod_tier,
        LodTier::Direct,
        "a 300-node graph must fit LodTier::Direct under a generous frame budget"
    );

    let bytes = backend
        .export(&spec, &result, StaticExportFormat::Png)
        .unwrap();
    assert_eq!(
        &bytes[0..4],
        &[137, 80, 78, 71],
        "must be a valid PNG (correct signature bytes)"
    );

    let path = format!("{}/eg-viz-graph-small.png", out_dir());
    std::fs::write(&path, &bytes).unwrap();
    println!("wrote {path} ({} bytes)", bytes.len());
}

fn large_graph_demo() {
    const NODE_COUNT: u64 = 100_000;
    const EDGE_COUNT: u64 = 300_000;

    println!("generating a deterministic seeded {NODE_COUNT}-node / {EDGE_COUNT}-edge graph...");
    let gen_started = Instant::now();
    let edges = synthetic_graph(NODE_COUNT, EDGE_COUNT, 2);
    println!(
        "graph generated in {:.3}s",
        gen_started.elapsed().as_secs_f64()
    );

    let mut store = ColumnStore::new();
    let ingest_started = Instant::now();
    ingest_graph(&mut store, "ds:graph-large", NODE_COUNT, &edges);
    println!(
        "ingested into ColumnStore in {:.3}s",
        ingest_started.elapsed().as_secs_f64()
    );

    let spec = graph_spec(
        "ds:graph-large",
        "V6-lite: 100,000-node graph (Density tier)",
    );
    let backend = ColumnStoreExportBackend::new(&store, 1200, 900);
    // A deliberately tighter budget than the generous 2M/16MB default the other
    // demos use, chosen so a 100,000-node graph — already large for a
    // force-directed layout to even consider — visibly exercises the Density
    // tier rather than needing millions of nodes to overflow the generous
    // default (100,000 nodes * 3 primitives/row = 300,000, comfortably under
    // 2,000,000; this budget forces the overflow at a realistic scale instead).
    let budget = FrameBudget::new(200_000, 16 * 1024 * 1024);

    let pipeline_started = Instant::now();
    let result = backend
        .resolve(&spec, "ds:graph-large", budget, 1)
        .expect("resolve the large graph");
    let resolve_elapsed = pipeline_started.elapsed();

    println!(
        "large graph: {NODE_COUNT} nodes, {EDGE_COUNT} edges -> {:?} (exact={}) resolved (ingest+layout+reduce) in {:.3}s",
        result.lod_tier,
        result.exact,
        resolve_elapsed.as_secs_f64()
    );
    assert_eq!(
        result.lod_tier,
        LodTier::Density,
        "a 100,000-node graph over this tight budget must select Density, never Direct — \
         the layout must never attempt full physics AND full per-node/edge drawing at this scale"
    );
    assert!(
        !result.exact,
        "a Density-tier result must never claim exact"
    );

    let op_count = backend
        .cached_op_count(&result.query_hash)
        .expect("resolve() must have cached a render plan");
    println!(
        "render plan has {op_count} draw ops (the density grid, independent of {NODE_COUNT} nodes)"
    );
    // The absolute bound: the density grid is a FIXED 200x150 = 30,000-cell cap
    // regardless of node_count (see `eg_viz_export::reduce::density_grid`'s
    // doc) — this alone is the "independent of node_count" property. An
    // ADDITIONAL "op_count << node_count" ratio check (as
    // `render_large_n_tier.rs` asserts for Scatter) is only meaningful once
    // node_count comfortably exceeds the grid cap by a couple orders of
    // magnitude; at this demo's deliberately realistic ~100,000-node scale
    // (chosen to match the illustrative scale the lane spec names, not to
    // clear that ratio), the grid is nearly fully populated, so that stronger
    // ratio bound is proven separately, at a much larger node_count, by
    // `eg-viz-export`'s own
    // `render::graph_tests::large_graph_resolves_at_density_tier_with_bounded_op_count`
    // unit test (10,000,000 nodes) rather than repeated here at demo-run cost.
    assert!(
        op_count <= 200 * 150,
        "graph density render must stay within the density grid's cell budget (<= 30000), got {op_count} ops"
    );

    let export_started = Instant::now();
    let bytes = backend
        .export(&spec, &result, StaticExportFormat::Png)
        .unwrap();
    let export_elapsed = export_started.elapsed();
    println!(
        "rendered a real PNG in {:.3}s ({} bytes) WITHOUT attempting to draw {NODE_COUNT} nodes / {EDGE_COUNT} edges directly",
        export_elapsed.as_secs_f64(),
        bytes.len()
    );

    let total_elapsed = gen_started.elapsed();
    println!(
        "TOTAL wall time (generate+ingest+layout+reduce+export): {:.3}s",
        total_elapsed.as_secs_f64()
    );

    let path = format!("{}/eg-viz-graph-large.png", out_dir());
    std::fs::write(&path, &bytes).unwrap();
    println!("wrote {path} ({} bytes)", bytes.len());
}

fn main() {
    small_graph_demo();
    println!();
    large_graph_demo();
}
