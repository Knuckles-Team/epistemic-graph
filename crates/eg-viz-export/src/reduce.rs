//! Minimal per-tier reduction kernels (D-VZ-1 lane V3a's own scope, NOT lane V2).
//!
//! **Scope note, read before extending this file.** The program charter names
//! V2 ("LOD/decimation kernels: M4, LTTB, min-max, density grids, SIMD-friendly,
//! tier selection rules") as its own lane, depending on V1 and blocking V3b. This
//! lane (V1 + V3a) does not implement V2 — but V3a cannot honestly demonstrate
//! "the exporter honors the tier decision" with NO reduction at all, so this
//! module ships the smallest real reduction that makes the tier boundary
//! observable end-to-end:
//!
//! - [`decimate_minmax`] — one of the three named Tier-1 algorithms (min-max;
//!   M4/LTTB are NOT implemented here), vertical-range-per-pixel-column.
//! - [`density_grid`] — a screen-bounded count grid (the Tier-2 shape the
//!   charter names), log-scaled to alpha.
//!
//! Neither is SIMD-optimized, and neither is the full algorithm family V2
//! promises. What both genuinely prove: **output size is a function of the
//! pixel budget (`width_px` / `grid_cols * grid_rows`), never the row count** —
//! the exact property `select_tier` exists to protect. `direct()` is Tier 0 —
//! full fidelity, one draw op per row, only ever reached when
//! [`eg_viz_core::select_tier`] already confirmed that many primitives fit the
//! caller's [`eg_viz_core::FrameBudget`].

use eg_viz_core::MarkKind;

use crate::error::ExportError;
use crate::plan::{DrawOp, LinearMap};

/// Tier 0 — one draw op per finite `(x, y)` row. Only called when
/// `select_tier` already returned `LodTier::Direct` for this row count, so this
/// function trusts that decision rather than re-checking a budget itself (the
/// "never re-derive your own limits" rule the lane charter states explicitly).
pub fn direct(
    mark: MarkKind,
    xs: &[f64],
    ys: &[f64],
    x_map: LinearMap,
    y_map: LinearMap,
    width_px: u32,
    color: [u8; 4],
) -> Result<Vec<DrawOp>, ExportError> {
    match mark {
        MarkKind::Scatter => Ok(xs
            .iter()
            .zip(ys)
            .filter(|(x, y)| x.is_finite() && y.is_finite())
            .map(|(&x, &y)| DrawOp::Point {
                x: x_map.map(x),
                y: y_map.map(y),
                radius: 1.6,
                color,
            })
            .collect()),
        MarkKind::Line | MarkKind::Area => {
            let mut indices: Vec<usize> = (0..xs.len())
                .filter(|&i| xs[i].is_finite() && ys[i].is_finite())
                .collect();
            indices.sort_by(|&a, &b| xs[a].partial_cmp(&xs[b]).unwrap());
            let mut ops = Vec::with_capacity(indices.len().saturating_sub(1));
            for pair in indices.windows(2) {
                let (i0, i1) = (pair[0], pair[1]);
                ops.push(DrawOp::Segment {
                    x0: x_map.map(xs[i0]),
                    y0: y_map.map(ys[i0]),
                    x1: x_map.map(xs[i1]),
                    y1: y_map.map(ys[i1]),
                    color,
                });
            }
            Ok(ops)
        }
        MarkKind::Bar => {
            let n = xs.len().max(1);
            let slot = (width_px as f32 / n as f32).max(1.0);
            let bar_w = (slot * 0.8).max(1.0);
            let baseline = y_map.extent;
            Ok(xs
                .iter()
                .zip(ys)
                .enumerate()
                .filter(|(_, (x, y))| x.is_finite() && y.is_finite())
                .map(|(i, (_x, &y))| {
                    let top = y_map.map(y);
                    let cx = i as f32 * slot + slot / 2.0;
                    DrawOp::Rect {
                        x: cx - bar_w / 2.0,
                        y: top.min(baseline),
                        w: bar_w,
                        h: (baseline - top).abs(),
                        color,
                    }
                })
                .collect())
        }
        other => Err(ExportError::UnsupportedMark(other)),
    }
}

/// Tier 1 — shape-preserving min-max decimation, one of the three algorithms
/// `lod-architecture.md` §1 names (M4/LTTB/min-max). Buckets rows by the pixel
/// column their `x` maps to and keeps only each column's `(min_y, max_y)`, drawn
/// as a vertical range segment. **Output size is `<= width_px`, independent of
/// `xs.len()`** — the property this reduction exists to prove.
pub fn decimate_minmax(
    xs: &[f64],
    ys: &[f64],
    x_map: LinearMap,
    y_map: LinearMap,
    width_px: u32,
    color: [u8; 4],
) -> Vec<DrawOp> {
    let cols = width_px.max(1) as usize;
    let mut buckets: Vec<Option<(f64, f64)>> = vec![None; cols];
    for (&x, &y) in xs.iter().zip(ys) {
        if !x.is_finite() || !y.is_finite() {
            continue;
        }
        let col = (x_map.map(x) as usize).min(cols - 1);
        buckets[col] = Some(match buckets[col] {
            None => (y, y),
            Some((lo, hi)) => (lo.min(y), hi.max(y)),
        });
    }
    buckets
        .iter()
        .enumerate()
        .filter_map(|(col, bucket)| bucket.map(|(lo, hi)| (col, lo, hi)))
        .map(|(col, lo, hi)| DrawOp::Segment {
            x0: col as f32 + 0.5,
            y0: y_map.map(lo),
            x1: col as f32 + 0.5,
            y1: y_map.map(hi),
            color,
        })
        .collect()
}

/// Tier 2 — a screen-bounded density/count grid, the shape
/// `lod-architecture.md` §1 calls "born aggregated": `grid_cols * grid_rows`
/// cells cover the whole plot area regardless of row count. Count is
/// log-scaled to alpha (`ln(1+count) / ln(1+max_count)`) so a single hot cell
/// does not wash out every other cell to invisibility — a linear scale under
/// heavy skew (typical of a scatter density) makes everything but the mode
/// look empty. **Output size is `<= grid_cols * grid_rows`, independent of
/// `xs.len()`** — proven directly by
/// `crates/eg-viz-export/examples/render_large_n_tier.rs`, which asserts this
/// bound at `xs.len() == 100_000_000`.
pub fn density_grid(
    xs: &[f64],
    ys: &[f64],
    x_map: LinearMap,
    y_map: LinearMap,
    grid_cols: u32,
    grid_rows: u32,
    color: [u8; 4],
) -> Vec<DrawOp> {
    let cols = grid_cols.max(1) as usize;
    let rows = grid_rows.max(1) as usize;
    let mut counts = vec![0u64; cols * rows];
    let cell_w = x_map.extent / cols as f32;
    let cell_h = y_map.extent / rows as f32;

    for (&x, &y) in xs.iter().zip(ys) {
        if !x.is_finite() || !y.is_finite() {
            continue;
        }
        let px = x_map.map(x);
        let py = y_map.map(y);
        let col = ((px / cell_w) as usize).min(cols - 1);
        let row = ((py / cell_h) as usize).min(rows - 1);
        counts[row * cols + col] += 1;
    }

    let max_count = counts.iter().copied().max().unwrap_or(0);
    if max_count == 0 {
        return Vec::new();
    }
    let log_max = ((max_count + 1) as f64).ln();

    let mut ops = Vec::new();
    for row in 0..rows {
        for col in 0..cols {
            let count = counts[row * cols + col];
            if count == 0 {
                continue;
            }
            let alpha = (((count + 1) as f64).ln() / log_max).clamp(0.0, 1.0);
            ops.push(DrawOp::Rect {
                x: col as f32 * cell_w,
                y: row as f32 * cell_h,
                w: cell_w,
                h: cell_h,
                color: [color[0], color[1], color[2], (alpha * 255.0) as u8],
            });
        }
    }
    ops
}

/// Tier 0 for `MarkKind::Graph` (D-VZ-1 lane V6, "graph-native marks") — one
/// `DrawOp::Point` per already-laid-out node position (`crate::graph_layout::layout`'s
/// output, in `[0,1]x[0,1]`, mapped through `x_map`/`y_map` the SAME way every
/// other Direct-tier mark's coordinates are), plus one `DrawOp::Segment` per
/// edge whose endpoints both resolve to a known position. Only called when
/// `select_tier` already returned `LodTier::Direct` for the node count (mirrors
/// [`direct`]'s "trust the decision, never re-check a budget" rule).
pub fn graph_direct(
    positions: &[(f32, f32)],
    edges: &[(u32, u32)],
    x_map: LinearMap,
    y_map: LinearMap,
    color: [u8; 4],
) -> Vec<DrawOp> {
    let mut ops = Vec::with_capacity(positions.len() + edges.len());
    for &(x, y) in positions {
        ops.push(DrawOp::Point {
            x: x_map.map(x as f64),
            y: y_map.map(y as f64),
            radius: 2.0,
            color,
        });
    }
    for &(src, dst) in edges {
        if let (Some(&(sx, sy)), Some(&(dx, dy))) =
            (positions.get(src as usize), positions.get(dst as usize))
        {
            ops.push(DrawOp::Segment {
                x0: x_map.map(sx as f64),
                y0: y_map.map(sy as f64),
                x1: x_map.map(dx as f64),
                y1: y_map.map(dy as f64),
                color,
            });
        }
    }
    ops
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity_map(extent: f32) -> LinearMap {
        LinearMap::new(0.0, extent as f64, extent, false)
    }

    #[test]
    fn decimate_output_size_is_bounded_by_width_not_row_count() {
        let n = 5_000_000;
        let xs: Vec<f64> = (0..n).map(|i| (i % 800) as f64).collect();
        let ys: Vec<f64> = (0..n).map(|i| (i % 97) as f64).collect();
        let ops = decimate_minmax(
            &xs,
            &ys,
            identity_map(800.0),
            identity_map(600.0),
            800,
            [0, 0, 0, 255],
        );
        assert!(
            ops.len() <= 800,
            "decimate output must be <= width_px, got {}",
            ops.len()
        );
        assert!(!ops.is_empty());
    }

    #[test]
    fn density_output_size_is_bounded_by_grid_not_row_count() {
        let n = 10_000_000;
        let xs: Vec<f64> = (0..n).map(|i| (i % 400) as f64).collect();
        let ys: Vec<f64> = (0..n).map(|i| (i % 300) as f64).collect();
        let ops = density_grid(
            &xs,
            &ys,
            identity_map(400.0),
            identity_map(300.0),
            160,
            120,
            [0, 0, 0, 255],
        );
        assert!(
            ops.len() <= 160 * 120,
            "density output must be <= grid_cols*grid_rows, got {}",
            ops.len()
        );
        assert!(!ops.is_empty());
    }

    #[test]
    fn density_grid_alpha_reflects_relative_count() {
        // A cell hit 1000x more often must have a strictly higher alpha than a
        // cell hit once, but the log scale keeps the once-hit cell visible
        // (alpha > 0), not washed to zero.
        let mut xs = vec![1.0]; // one point in cell (0, 0)
        let mut ys = vec![1.0];
        for _ in 0..1000 {
            xs.push(101.0); // 1000 points in a different cell
            ys.push(1.0);
        }
        let ops = density_grid(
            &xs,
            &ys,
            identity_map(200.0),
            identity_map(100.0),
            2,
            1,
            [10, 20, 30, 255],
        );
        assert_eq!(ops.len(), 2);
        let alpha_low = ops
            .iter()
            .find(|o| matches!(o, DrawOp::Rect { x, .. } if *x == 0.0))
            .unwrap();
        let alpha_high = ops
            .iter()
            .find(|o| matches!(o, DrawOp::Rect { x, .. } if *x != 0.0))
            .unwrap();
        let a = |op: &DrawOp| match op {
            DrawOp::Rect { color, .. } => color[3],
            _ => unreachable!(),
        };
        assert!(a(alpha_high) > a(alpha_low));
        assert!(
            a(alpha_low) > 0,
            "a single-hit cell must remain visible, not washed to zero"
        );
    }

    #[test]
    fn direct_scatter_emits_one_point_per_finite_row() {
        let xs = vec![1.0, 2.0, f64::NAN, 4.0];
        let ys = vec![1.0, 2.0, 3.0, 4.0];
        let ops = direct(
            MarkKind::Scatter,
            &xs,
            &ys,
            identity_map(100.0),
            identity_map(100.0),
            100,
            [0, 0, 0, 255],
        )
        .unwrap();
        assert_eq!(
            ops.len(),
            3,
            "the NaN row must be skipped, not plotted as a fabricated point"
        );
    }

    #[test]
    fn direct_line_connects_points_in_x_order_regardless_of_input_order() {
        let xs = vec![3.0, 1.0, 2.0];
        let ys = vec![30.0, 10.0, 20.0];
        let ops = direct(
            MarkKind::Line,
            &xs,
            &ys,
            identity_map(100.0),
            identity_map(100.0),
            100,
            [0, 0, 0, 255],
        )
        .unwrap();
        assert_eq!(ops.len(), 2);
        match &ops[0] {
            DrawOp::Segment { x0, x1, .. } => assert!(x0 < x1),
            _ => panic!("expected a segment"),
        }
    }

    #[test]
    fn graph_direct_emits_one_point_per_node_and_one_segment_per_valid_edge() {
        let positions = vec![(0.0_f32, 0.0_f32), (0.5, 0.5), (1.0, 1.0)];
        // One valid edge (0->1), one edge referencing an out-of-range node (must
        // be skipped, not panic).
        let edges = [(0u32, 1u32), (2, 99)];
        let ops = graph_direct(
            &positions,
            &edges,
            identity_map(100.0),
            identity_map(100.0),
            [0, 0, 0, 255],
        );
        let point_count = ops
            .iter()
            .filter(|o| matches!(o, DrawOp::Point { .. }))
            .count();
        let segment_count = ops
            .iter()
            .filter(|o| matches!(o, DrawOp::Segment { .. }))
            .count();
        assert_eq!(point_count, 3, "one point per node");
        assert_eq!(segment_count, 1, "only the in-range edge draws a segment");
    }
}
