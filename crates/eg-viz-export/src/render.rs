//! Resolution: `ViewSpec` + `ColumnStore` + `FrameBudget` -> `(ViewResult, RenderPlan)`
//! (D-VZ-1 lane V3a).
//!
//! This is the one place that calls [`eg_viz_core::select_tier`] and dispatches
//! on its answer — every backend (`png`/`svg`/`pdf`) renders whatever
//! [`resolve`] already decided, never re-deriving a tier or a budget of its own.
//! [`eg_viz_core::ViewResult::exact`]/[`eg_viz_core::ViewResult::reduced`] compute
//! the `exact` bit (`ViewResult::exact_bit_for`, defined once in `eg-viz-core`) —
//! this module never sets it directly.
//!
//! **Scale scope note.** V0's [`eg_viz_core::ScaleKind`] enumerates
//! `Linear`/`Log`/`Time`/`Category`/`Symlog`; this lane's [`crate::plan::LinearMap`]
//! implements only the linear case. A spec naming a non-linear scale still
//! renders (the domain is still resolved and mapped linearly) rather than being
//! rejected — log/time/category axis transforms are a documented gap for a later
//! lane, not a silent misrender of DATA (every value still maps monotonically
//! into the same domain range).

use std::time::{SystemTime, UNIX_EPOCH};

use eg_viz_columnstore::ColumnStore;
use eg_viz_core::{
    query_hash, select_tier, FrameBudget, LodTier, MarkKind, MarkSpec, PayloadKind, PayloadRef,
    ReductionKind, ScaleSpec, TierInput, ViewResult, ViewSpec,
};

use crate::error::ExportError;
use crate::plan::{LinearMap, RenderPlan};
use crate::reduce;

const DEFAULT_STROKE: [u8; 4] = [0x1f, 0x1f, 0x1f, 0xff];
/// Density-grid resolution — deliberately coarser than the pixel canvas
/// (bounded independent of BOTH row count and canvas size) so a huge render
/// target does not itself become a second, unbounded cost axis.
const DENSITY_GRID_COLS: u32 = 200;
const DENSITY_GRID_ROWS: u32 = 150;

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn resolve_domain(scale: Option<&ScaleSpec>, column_range: Option<(f64, f64)>) -> (f64, f64) {
    if let Some(scale) = scale {
        if let Some(domain) = scale.domain {
            return domain;
        }
    }
    column_range.unwrap_or((0.0, 1.0))
}

fn find_scale<'a>(spec: &'a ViewSpec, id: &str) -> Option<&'a ScaleSpec> {
    spec.scales.iter().find(|s| s.id == id)
}

/// Resolve `spec`'s FIRST mark against `dataset_ref` in `store`, honoring
/// whatever tier `select_tier` selects for its real row count. Returns the
/// `ViewResult` recording what happened plus the bounded `RenderPlan` every
/// static-export backend rasterizes/serializes from.
pub fn resolve(
    store: &ColumnStore,
    spec: &ViewSpec,
    dataset_ref: &str,
    width_px: u32,
    height_px: u32,
    budget: FrameBudget,
    snapshot_version: u64,
) -> Result<(ViewResult, RenderPlan), ExportError> {
    let mark: &MarkSpec = spec.marks.first().ok_or(ExportError::NoMarks)?;

    // `MarkKind::Graph` (D-VZ-1 lane V6) has no x/y COLUMN encodings at all — its
    // positions come from `crate::graph_layout::layout`, not a materialized
    // column — so it branches out before the generic x/y-encoding path below
    // even looks for one.
    if mark.kind == MarkKind::Graph {
        return resolve_graph(
            store,
            spec,
            mark,
            dataset_ref,
            width_px,
            height_px,
            budget,
            snapshot_version,
        );
    }

    let x_enc = mark
        .encodings
        .x
        .as_ref()
        .ok_or(ExportError::MissingEncoding("x"))?;
    let y_enc = mark
        .encodings
        .y
        .as_ref()
        .ok_or(ExportError::MissingEncoding("y"))?;

    let started = std::time::Instant::now();

    let xs = store.materialize_f64(dataset_ref, &x_enc.field)?;
    let ys = store.materialize_f64(dataset_ref, &y_enc.field)?;
    if xs.len() != ys.len() {
        return Err(ExportError::ColumnLengthMismatch {
            x: xs.len(),
            y: ys.len(),
        });
    }
    let row_count = xs.len() as u64;

    // ★ Zone-map-derived range: O(chunk count), not O(row count) — the payoff
    // of carrying per-chunk min/max at ingest time (see eg-viz-columnstore's
    // `Column::range`), not a full rescan of a potentially huge column.
    let column = store
        .column(dataset_ref, &x_enc.field)
        .ok_or(ExportError::MissingEncoding("x"))?;
    let x_domain = resolve_domain(
        x_enc.scale.as_deref().and_then(|id| find_scale(spec, id)),
        column.range(),
    );
    let y_column = store
        .column(dataset_ref, &y_enc.field)
        .ok_or(ExportError::MissingEncoding("y"))?;
    let y_domain = resolve_domain(
        y_enc.scale.as_deref().and_then(|id| find_scale(spec, id)),
        y_column.range(),
    );

    let x_map = LinearMap::new(x_domain.0, x_domain.1, width_px as f32, false);
    let y_map = LinearMap::new(y_domain.0, y_domain.1, height_px as f32, true);

    // ★ The load-bearing call: select_tier converts row_count through
    // primitives_per_row(mark)/bytes_per_row(encodings) before comparing to the
    // caller's budget — this function never re-derives that limit itself.
    let decision = select_tier(&TierInput {
        mark: mark.kind,
        row_count,
        encodings: mark.encodings.clone(),
        budget,
        out_of_core: false,
    });

    let color = spec
        .theme
        .palette
        .first()
        .and_then(|hex| parse_hex_color(hex))
        .unwrap_or(DEFAULT_STROKE);
    let background = spec
        .theme
        .background
        .as_deref()
        .and_then(parse_hex_color)
        .unwrap_or([0xff, 0xff, 0xff, 0xff]);

    let (reduction, ops) = match decision.tier {
        LodTier::Direct => (
            ReductionKind::None,
            reduce::direct(mark.kind, &xs, &ys, x_map, y_map, width_px, color)?,
        ),
        LodTier::Decimate => (
            ReductionKind::Decimate,
            reduce::decimate_minmax(&xs, &ys, x_map, y_map, width_px, color),
        ),
        LodTier::Density => (
            ReductionKind::Density,
            reduce::density_grid(
                &xs,
                &ys,
                x_map,
                y_map,
                DENSITY_GRID_COLS,
                DENSITY_GRID_ROWS,
                color,
            ),
        ),
        LodTier::Tiled => return Err(ExportError::TieredNotSupportedByStaticExport),
    };

    let wall_time_ms = started.elapsed().as_millis() as u64;
    let hash = query_hash(spec, dataset_ref, snapshot_version)
        .map_err(|e| ExportError::Digest(e.to_string()))?;
    let payload_kind = if decision.tier == LodTier::Density {
        PayloadKind::DensityGrid
    } else {
        PayloadKind::Geometry
    };
    let payload = PayloadRef::new(hash.clone(), payload_kind, decision.estimated_bytes);

    let result = if decision.tier == LodTier::Direct {
        ViewResult::exact(hash, row_count, vec![payload], wall_time_ms, now_unix_ms())?
    } else {
        // Deterministic binning (pixel column / grid cell), never wall-clock/RNG
        // sampling — seed 0 by convention, matching `eg-viz-core::result`'s
        // documented "pure function of (row_id, zoom_level)" reproducibility
        // contract for a deterministic (non-sampling) reduction.
        ViewResult::reduced(
            hash,
            0,
            row_count,
            decision.tier,
            reduction,
            vec![payload],
            wall_time_ms,
            now_unix_ms(),
        )?
    };

    let mut render_plan = RenderPlan::new(width_px, height_px, background);
    render_plan.ops = ops;
    Ok((result, render_plan))
}

/// Resolve a `MarkKind::Graph` mark (D-VZ-1 lane V6, "graph-native marks"):
/// read the node/edge datasets ingested under `dataset_ref`'s own graph-mark
/// convention (documented below — the SAME convention
/// `src/server/handlers/viz.rs`'s module doc documents on the ingest side),
/// compute a deterministic layout ([`crate::graph_layout::layout`]), and reduce
/// it through the SAME `select_tier`/tier-dispatch machinery every other mark
/// uses — never a separate, undocumented code path.
///
/// **Dataset convention (this lane's own choice — documented once here and
/// mirrored by the ingest side).** A Graph mark's `dataset_ref` names TWO
/// datasets in `store`:
///
/// - `dataset_ref` itself: one I64 column (any name; only its length is read),
///   one row per node — its row count IS the graph's node count, the "row"
///   concept [`eg_viz_core::tier::primitives_per_row`]'s `MarkKind::Graph` case
///   ("one node plus its average ~2 incident edges") is written against.
/// - `"{dataset_ref}:edges"`: two I64 columns named `src`/`dst`, one row per
///   edge (or ABSENT entirely for a zero-edge graph).
///
/// This sidesteps `ColumnStore::ingest_columns`'s "every column in one dataset
/// shares one row count" rule (node count and edge count are, in general,
/// different numbers) without inventing a padding/sentinel scheme.
#[allow(clippy::too_many_arguments)] // mirrors the sibling `resolve()`'s own shape/arity
fn resolve_graph(
    store: &ColumnStore,
    spec: &ViewSpec,
    mark: &MarkSpec,
    dataset_ref: &str,
    width_px: u32,
    height_px: u32,
    budget: FrameBudget,
    snapshot_version: u64,
) -> Result<(ViewResult, RenderPlan), ExportError> {
    let started = std::time::Instant::now();

    let node_count = store
        .dataset(dataset_ref)
        .ok_or_else(|| ExportError::GraphDatasetMissing(dataset_ref.to_string()))?
        .row_count as usize;

    let edges_ref = format!("{dataset_ref}:edges");
    let (src, dst) = if store.dataset(&edges_ref).is_some() {
        let src = store.materialize_f64(&edges_ref, "src")?;
        let dst = store.materialize_f64(&edges_ref, "dst")?;
        (src, dst)
    } else {
        (Vec::new(), Vec::new())
    };
    if src.len() != dst.len() {
        return Err(ExportError::ColumnLengthMismatch {
            x: src.len(),
            y: dst.len(),
        });
    }
    let edges: Vec<(u32, u32)> = src
        .iter()
        .zip(&dst)
        .map(|(&s, &d)| (s as u32, d as u32))
        .collect();

    // `row_count` for tier selection is the NODE count — `primitives_per_row`
    // already amortizes each node's average incident-edge cost into its
    // per-row estimate (see this function's own doc), so it is never
    // `node_count + edge_count`.
    let row_count = node_count as u64;
    let decision = select_tier(&TierInput {
        mark: mark.kind,
        row_count,
        encodings: mark.encodings.clone(),
        budget,
        out_of_core: false,
    });

    let color = spec
        .theme
        .palette
        .first()
        .and_then(|hex| parse_hex_color(hex))
        .unwrap_or(DEFAULT_STROKE);
    let background = spec
        .theme
        .background
        .as_deref()
        .and_then(parse_hex_color)
        .unwrap_or([0xff, 0xff, 0xff, 0xff]);

    // Layout positions live in a fixed [0,1]x[0,1] normalized space (see
    // `crate::graph_layout`), so the pixel map is a plain identity-domain
    // LinearMap — not derived from any resolved data column range.
    let x_map = LinearMap::new(0.0, 1.0, width_px as f32, false);
    let y_map = LinearMap::new(0.0, 1.0, height_px as f32, true);

    // ★ The LOD ladder bounds LAYOUT work, not just draw calls (see
    // `crate::graph_layout`'s module doc): even at Density tier we still compute
    // a real position for every node — bounded by `graph_layout::layout`'s own
    // node-count-based strategy (real FR physics below FULL_LAYOUT_NODE_CAP, a
    // fast deterministic hash spread above it) — then bin those into the
    // EXISTING `density_grid` kernel. What stays bounded independent of
    // `node_count` is the DRAW-OP count, never the layout call itself. Seed 0 by
    // convention — the SAME "deterministic, never wall-clock/RNG" convention the
    // Decimate/Density branch below uses for `ViewResult::reduced`'s seed.
    const GRAPH_LAYOUT_SEED: u64 = 0;
    let positions = crate::graph_layout::layout(node_count, &edges, GRAPH_LAYOUT_SEED);

    let (reduction, ops) = match decision.tier {
        LodTier::Direct => (
            ReductionKind::None,
            reduce::graph_direct(&positions, &edges, x_map, y_map, color),
        ),
        LodTier::Density => {
            let xs: Vec<f64> = positions.iter().map(|(x, _)| *x as f64).collect();
            let ys: Vec<f64> = positions.iter().map(|(_, y)| *y as f64).collect();
            // Edges are OMITTED at Density tier BY DESIGN: node positions alone
            // convey graph shape at this scale, and the edge count here would
            // exceed the frame budget by definition (that is WHY we are at
            // Density in the first place) — the simpler, defensible choice the
            // spec allows over a budget-capped random edge sample.
            (
                ReductionKind::Density,
                reduce::density_grid(
                    &xs,
                    &ys,
                    x_map,
                    y_map,
                    DENSITY_GRID_COLS,
                    DENSITY_GRID_ROWS,
                    color,
                ),
            )
        }
        LodTier::Decimate | LodTier::Tiled => {
            return Err(ExportError::TieredNotSupportedByStaticExport)
        }
    };

    let wall_time_ms = started.elapsed().as_millis() as u64;
    let hash = query_hash(spec, dataset_ref, snapshot_version)
        .map_err(|e| ExportError::Digest(e.to_string()))?;
    let payload_kind = if decision.tier == LodTier::Density {
        PayloadKind::DensityGrid
    } else {
        PayloadKind::Geometry
    };
    let payload = PayloadRef::new(hash.clone(), payload_kind, decision.estimated_bytes);

    let result = if decision.tier == LodTier::Direct {
        ViewResult::exact(hash, row_count, vec![payload], wall_time_ms, now_unix_ms())?
    } else {
        ViewResult::reduced(
            hash,
            GRAPH_LAYOUT_SEED,
            row_count,
            decision.tier,
            reduction,
            vec![payload],
            wall_time_ms,
            now_unix_ms(),
        )?
    };

    let mut render_plan = RenderPlan::new(width_px, height_px, background);
    render_plan.ops = ops;
    Ok((result, render_plan))
}

fn parse_hex_color(hex: &str) -> Option<[u8; 4]> {
    let hex = hex.trim_start_matches('#');
    if hex.len() != 6 {
        return None;
    }
    let r = u8::from_str_radix(&hex[0..2], 16).ok()?;
    let g = u8::from_str_radix(&hex[2..4], 16).ok()?;
    let b = u8::from_str_radix(&hex[4..6], 16).ok()?;
    Some([r, g, b, 0xff])
}

#[cfg(test)]
mod graph_tests {
    use super::*;
    use eg_viz_columnstore::{ColumnData, ColumnInput};
    use eg_viz_core::MarkSpec;

    /// Ingest a `MarkKind::Graph` dataset under this module's own convention (see
    /// `resolve_graph`'s doc): `dataset_ref` gets a one-column node dataset,
    /// `"{dataset_ref}:edges"` gets the `src`/`dst` I64 edge dataset.
    fn ingest_graph(
        store: &mut ColumnStore,
        dataset_ref: &str,
        node_count: u64,
        edges: &[(u32, u32)],
    ) {
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

    fn graph_spec(dataset_ref: &str) -> ViewSpec {
        let mut spec = ViewSpec::new(vec![MarkSpec::new(MarkKind::Graph, dataset_ref)]);
        spec.theme.palette = vec!["#4C78A8".to_string()];
        spec.theme.background = Some("#FFFFFF".to_string());
        spec
    }

    #[test]
    fn small_graph_resolves_at_direct_tier_exact() {
        let mut store = ColumnStore::new();
        let edges: Vec<(u32, u32)> = (0..30u32).map(|i| (i, (i + 1) % 30)).collect();
        ingest_graph(&mut store, "ds:graph-small", 30, &edges);

        let spec = graph_spec("ds:graph-small");
        let budget = FrameBudget::new(2_000_000, 16 * 1024 * 1024);
        let (result, plan) = resolve(&store, &spec, "ds:graph-small", 400, 300, budget, 1).unwrap();

        assert_eq!(result.lod_tier, LodTier::Direct);
        assert!(result.exact);
        // 30 points + up to 30 segments (all edges are in-range here).
        assert_eq!(plan.ops.len(), 30 + edges.len());
    }

    #[test]
    fn large_graph_resolves_at_density_tier_with_bounded_op_count() {
        let mut store = ColumnStore::new();
        // The density grid is a FIXED 200x150 = 30,000-cell cap regardless of
        // node_count (see `reduce::density_grid`'s doc), so a
        // node_count-independent "ops << node_count" ratio bound is only a
        // meaningful additional proof (beyond the absolute cap already asserted
        // below) once node_count comfortably exceeds the grid cap by a couple
        // orders of magnitude — mirrors why `render_large_n_tier.rs` uses
        // 100,000,000 rows for the SAME kind of ratio assertion on Scatter.
        // Edges are irrelevant here (Density tier omits them by design — see
        // `resolve_graph`'s doc), so this ingests zero edges to keep the test
        // fast despite the large node_count (pure O(node_count) hash-layout
        // work, no O(node_count²) physics — node_count is far above
        // `graph_layout::FULL_LAYOUT_NODE_CAP`).
        let node_count: u64 = 10_000_000;
        ingest_graph(&mut store, "ds:graph-large", node_count, &[]);

        let spec = graph_spec("ds:graph-large");
        // A deliberately tight budget (well under node_count * primitives_per_row)
        // so this graph visibly exercises the Density tier.
        let budget = FrameBudget::new(100_000, 16 * 1024 * 1024);
        let (result, plan) = resolve(&store, &spec, "ds:graph-large", 400, 300, budget, 1).unwrap();

        assert_eq!(result.lod_tier, LodTier::Density);
        assert!(!result.exact);
        assert!(
            plan.ops.len() <= 200 * 150,
            "graph density render must stay within the density grid's cell budget, got {}",
            plan.ops.len()
        );
        assert!(
            (plan.ops.len() as u64) < node_count / 100,
            "graph density render ({} ops) must be orders of magnitude smaller than \
             node_count ({node_count}) — the property select_tier exists to guarantee",
            plan.ops.len()
        );
    }

    #[test]
    fn graph_mark_with_unknown_dataset_ref_is_a_clear_error() {
        let store = ColumnStore::new();
        let spec = graph_spec("ds:missing");
        let budget = FrameBudget::new(2_000_000, 16 * 1024 * 1024);
        let err = resolve(&store, &spec, "ds:missing", 100, 100, budget, 1).unwrap_err();
        assert!(matches!(err, ExportError::GraphDatasetMissing(ref d) if d == "ds:missing"));
    }

    #[test]
    fn graph_with_no_edges_dataset_still_resolves_nodes_only() {
        let mut store = ColumnStore::new();
        store
            .ingest_columns(
                "ds:graph-no-edges",
                vec![ColumnInput::new(
                    "node_id",
                    ColumnData::I64((0..10i64).collect()),
                )],
            )
            .unwrap();
        let spec = graph_spec("ds:graph-no-edges");
        let budget = FrameBudget::new(2_000_000, 16 * 1024 * 1024);
        let (result, plan) =
            resolve(&store, &spec, "ds:graph-no-edges", 100, 100, budget, 1).unwrap();
        assert_eq!(result.lod_tier, LodTier::Direct);
        assert_eq!(plan.ops.len(), 10, "10 node points, zero edge segments");
    }
}
