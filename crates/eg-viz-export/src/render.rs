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
    query_hash, select_tier, FrameBudget, LodTier, MarkSpec, PayloadKind, PayloadRef,
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
