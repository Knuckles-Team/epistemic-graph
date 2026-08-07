//! Large-N tier-selection proof (D-VZ-1 lane V3a demo). 100,000,000 rows — the
//! SAME row count V0's own acceptance test
//! (`eg-viz-core::tier::tests::a_100_million_row_scatter_spec_selects_a_bounded_tier`)
//! uses — ingested into a real `ColumnStore` and pushed through the real
//! export pipeline. Proves two things end to end, not just at the `select_tier`
//! unit-test level: the pipeline selects `LodTier::Density` (never attempts
//! `Direct`), and the actual rendered output stays bounded by the density
//! grid's cell budget rather than by the row count.
//!
//! Run: `cargo run -p eg-viz-export --example render_large_n_tier --release`

use std::time::Instant;

use eg_viz_columnstore::{ColumnData, ColumnInput, ColumnStore};
use eg_viz_core::{
    EncodingSpec, Encodings, FrameBudget, LodTier, MarkKind, MarkSpec, StaticExportBackend,
    StaticExportFormat, ViewSpec,
};
use eg_viz_export::ColumnStoreExportBackend;

const ROW_COUNT: usize = 100_000_000;

fn main() {
    println!("ingesting {ROW_COUNT} rows into eg-viz-columnstore (real chunk/hash/zone-map path, not a shortcut)...");
    let started = Instant::now();
    // Deterministic pseudo-scatter (Knuth multiplicative hashing), not a
    // repeated constant — every one of the ~12,208 chunks per column gets
    // genuinely distinct content, so this also exercises the content-addressed
    // dedup path honestly (no free dedup from an all-identical fixture).
    let xs: Vec<f64> = (0..ROW_COUNT)
        .map(|i| (i.wrapping_mul(2_654_435_761) % 1_000_003) as f64)
        .collect();
    let ys: Vec<f64> = (0..ROW_COUNT)
        .map(|i| {
            (i.wrapping_mul(40_503).wrapping_add(7) % 998_244_353) as f64 / 998_244_353.0
                * 1_000_000.0
        })
        .collect();

    let mut store = ColumnStore::new();
    store
        .ingest_columns(
            "ds:large-n",
            vec![
                ColumnInput::new("x", ColumnData::F64(xs)),
                ColumnInput::new("y", ColumnData::F64(ys)),
            ],
        )
        .unwrap();
    let ingest_elapsed = started.elapsed();
    let stats = store.stats();
    println!(
        "ingest done in {:.2}s: {} chunk references, {} unique content-addressed chunks, {} stored bytes",
        ingest_elapsed.as_secs_f64(),
        stats.chunk_references,
        stats.unique_chunks,
        stats.stored_bytes
    );

    let spec = ViewSpec::new(vec![MarkSpec::new(MarkKind::Scatter, "ds:large-n")
        .with_encodings(Encodings {
            x: Some(EncodingSpec::field("x")),
            y: Some(EncodingSpec::field("y")),
            ..Default::default()
        })]);

    let backend = ColumnStoreExportBackend::new(&store, 1200, 900);
    // The SAME "generous single-frame interactive budget" (~2M primitives,
    // ~16MB) V0's own tier.rs test fixtures use — not a budget hand-picked here
    // to force a particular answer.
    let budget = FrameBudget::new(2_000_000, 16 * 1024 * 1024);

    let render_started = Instant::now();
    let result = backend
        .resolve(&spec, "ds:large-n", budget, 1)
        .expect("resolve the 100M-row dataset");
    let render_elapsed = render_started.elapsed();

    println!(
        "select_tier over {} rows -> {:?} (exact={})",
        result.row_count, result.lod_tier, result.exact
    );
    assert_eq!(
        result.lod_tier,
        LodTier::Density,
        "a 100M-row Scatter spec must select Density, never Direct — select_tier must not try to draw every point"
    );
    assert!(
        !result.exact,
        "a Density-tier result must never claim exact"
    );

    let op_count = backend
        .cached_op_count(&result.query_hash)
        .expect("resolve() must have cached a render plan");
    println!(
        "render plan has {op_count} draw ops (the density grid, independent of {ROW_COUNT} rows)"
    );
    assert!(
        op_count <= 200 * 150,
        "render output must stay within the density grid's cell budget (<= 30000), got {op_count} ops"
    );
    assert!(
        (op_count as u64) < result.row_count / 1000,
        "render output ({op_count} ops) must be orders of magnitude smaller than the row count ({}) \
         — this is the entire property select_tier exists to guarantee",
        result.row_count
    );

    let bytes = backend
        .export(&spec, &result, StaticExportFormat::Png)
        .unwrap();
    println!(
        "rendered a real PNG in {:.3}s ({} bytes) WITHOUT attempting to draw {ROW_COUNT} primitives",
        render_elapsed.as_secs_f64(),
        bytes.len()
    );

    let out_dir = std::env::var("EG_VIZ_DEMO_OUT_DIR").unwrap_or_else(|_| "/tmp".to_string());
    let path = format!("{out_dir}/eg-viz-large-n-density.png");
    std::fs::write(&path, &bytes).unwrap();
    println!("wrote {path}");
}
