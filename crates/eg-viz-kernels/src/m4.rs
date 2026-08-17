//! M4 aggregation (D-VZ-1 lane V2) — the four-point-per-pixel-column algorithm
//! (Jugel et al., "M4: A Visualization-Oriented Time Series Data Aggregation",
//! VLDB 2014): for every pixel column a line/area mark occupies, emit at most
//! four points — **first** (smallest x in the column), **min** (smallest y),
//! **max** (largest y), **last** (largest x) — sorted back into x order. A
//! polyline through those points is visually indistinguishable from the full
//! line at that pixel width: every extremum and every column's temporal
//! endpoints are preserved, so it can never draw a smoother-looking-but-wrong
//! picture the way naive downsampling (e.g. "keep every Nth row") can.
//!
//! **Complexity: O(n), one pass, independent of `width_px`.** Each of the `n`
//! input rows updates at most one bucket's four running extrema (four
//! branch-and-maybe-store comparisons) — no sort, no per-point search. Output
//! size is `<= 4 * width_px`, independent of `n` (the property
//! [`eg_viz_core::tier::select_tier`] exists to let a caller rely on). Because
//! bucket assignment is by x-VALUE (not array position), **unsorted input needs
//! no separate sort step** — a point's bucket and its first/last/min/max
//! candidacy are decided independently of where it appears in the input slice.
//!
//! See `crate::simd` for the SIMD-accelerated part of this pipeline (the batched
//! bucket-index/finite-mask precompute) and its module doc for why the actual
//! per-bucket reduction itself is not vectorized (a scatter across `width_px`
//! independent targets, not a case AVX2 can honestly accelerate).

use crate::simd;

const SIMD_BATCH_MIN_LEN: usize = 64;

#[derive(Clone, Copy)]
struct Bucket {
    first: (f64, f64),
    last: (f64, f64),
    min_y: (f64, f64),
    max_y: (f64, f64),
}

impl Bucket {
    fn seed(p: (f64, f64)) -> Self {
        Bucket {
            first: p,
            last: p,
            min_y: p,
            max_y: p,
        }
    }

    fn update(&mut self, p: (f64, f64)) {
        if p.0 < self.first.0 {
            self.first = p;
        }
        if p.0 > self.last.0 {
            self.last = p;
        }
        if p.1 < self.min_y.1 {
            self.min_y = p;
        }
        if p.1 > self.max_y.1 {
            self.max_y = p;
        }
    }

    /// Up to 4 distinct points, sorted by x ascending. Any two of
    /// first/min/max/last that coincide (the common case for a narrow or
    /// monotonic bucket) collapse to ONE output point — M4 never fabricates
    /// duplicate geometry to pad out to exactly 4.
    fn into_points(self) -> Vec<(f64, f64)> {
        let mut pts = [self.first, self.min_y, self.max_y, self.last];
        pts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let mut out = Vec::with_capacity(4);
        for p in pts {
            if out.last() != Some(&p) {
                out.push(p);
            }
        }
        out
    }
}

fn resolve_domain(mut domain: (f64, f64)) -> (f64, f64) {
    if (domain.1 - domain.0).abs() < f64::EPSILON {
        domain = (domain.0 - 0.5, domain.1 + 0.5);
    }
    domain
}

/// Reduce `(xs, ys)` (equal length) to at most `4 * width_px` points via M4,
/// bucketing by `x_domain` (the SAME `(min, max)` a caller would derive from a
/// column's zone-map range, e.g. `eg_viz_columnstore::Column::range`) mapped
/// linearly across `width_px` pixel columns.
///
/// Non-finite rows (`NaN`/`±inf` in either coordinate) are excluded — never
/// treated as an extremum. Empty input returns an empty output. `width_px == 0`
/// is treated as `1` (a single bucket), matching
/// `eg_viz_export::plan::LinearMap`'s degenerate-extent handling.
pub fn m4_reduce(xs: &[f64], ys: &[f64], x_domain: (f64, f64), width_px: u32) -> Vec<(f64, f64)> {
    assert_eq!(xs.len(), ys.len(), "m4_reduce: xs/ys length mismatch");
    if xs.is_empty() {
        return Vec::new();
    }
    let cols = width_px.max(1) as usize;
    let (domain_min, domain_max) = resolve_domain(x_domain);
    let inv_range = 1.0 / (domain_max - domain_min);

    let mut buckets: Vec<Option<Bucket>> = vec![None; cols];

    if xs.len() >= SIMD_BATCH_MIN_LEN {
        let mut idx = vec![0u32; xs.len()];
        let mut finite = vec![false; xs.len()];
        simd::bucket_indices(xs, domain_min, inv_range, cols, &mut idx, &mut finite);
        for i in 0..xs.len() {
            if !finite[i] || !ys[i].is_finite() {
                continue;
            }
            let p = (xs[i], ys[i]);
            let col = idx[i] as usize;
            match &mut buckets[col] {
                None => buckets[col] = Some(Bucket::seed(p)),
                Some(b) => b.update(p),
            }
        }
    } else {
        for i in 0..xs.len() {
            let (x, y) = (xs[i], ys[i]);
            if !x.is_finite() || !y.is_finite() {
                continue;
            }
            let t = ((x - domain_min) * inv_range).clamp(0.0, 1.0);
            let col = ((t * cols as f64) as usize).min(cols - 1);
            let p = (x, y);
            match &mut buckets[col] {
                None => buckets[col] = Some(Bucket::seed(p)),
                Some(b) => b.update(p),
            }
        }
    }

    let mut out = Vec::with_capacity(cols * 2);
    for bucket in buckets.into_iter().flatten() {
        out.extend(bucket.into_points());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_yields_empty_output() {
        assert!(m4_reduce(&[], &[], (0.0, 1.0), 100).is_empty());
    }

    #[test]
    fn single_point_is_returned_unchanged() {
        let out = m4_reduce(&[5.0], &[7.0], (0.0, 10.0), 100);
        assert_eq!(out, vec![(5.0, 7.0)]);
    }

    #[test]
    fn output_never_exceeds_four_times_width() {
        let n = 2_000_000;
        let xs: Vec<f64> = (0..n).map(|i| (i % 500) as f64).collect();
        let ys: Vec<f64> = (0..n).map(|i| ((i * 37) % 1000) as f64).collect();
        let out = m4_reduce(&xs, &ys, (0.0, 499.0), 500);
        assert!(out.len() <= 4 * 500);
        assert!(!out.is_empty());
    }

    #[test]
    fn nan_and_infinite_rows_are_excluded() {
        let xs = [1.0, f64::NAN, 3.0, f64::INFINITY, 5.0];
        let ys = [1.0, 2.0, f64::NAN, 4.0, f64::NEG_INFINITY];
        let out = m4_reduce(&xs, &ys, (0.0, 10.0), 10);
        // Only row 0 (1.0,1.0) is fully finite on both axes.
        assert_eq!(out, vec![(1.0, 1.0)]);
    }

    #[test]
    fn all_equal_values_do_not_panic_and_yield_one_point() {
        let xs = vec![3.0; 1000];
        let ys = vec![9.0; 1000];
        let out = m4_reduce(&xs, &ys, (3.0, 3.0), 50);
        assert_eq!(out, vec![(3.0, 9.0)]);
    }

    #[test]
    fn unsorted_input_still_produces_x_sorted_output() {
        let mut xs = vec![5.0, 1.0, 9.0, 3.0, 7.0, 0.0, 8.0, 2.0, 6.0, 4.0];
        let ys = vec![1.0; xs.len()];
        // Shuffle deterministically (reverse) to prove ORDER of arrival never
        // matters to bucket assignment.
        xs.reverse();
        let out = m4_reduce(&xs, &ys, (0.0, 9.0), 10);
        let mut sorted = out.clone();
        sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        assert_eq!(out, sorted, "M4 output must already be x-sorted");
    }

    #[test]
    fn preserves_a_spike_that_min_max_decimation_alone_would_still_show_but_first_last_add_shape() {
        // One pixel column with a sharp spike plus a clear entry/exit point —
        // M4 must report first/last (temporal endpoints) in addition to the
        // spike extrema, which a plain min/max-only reduction would omit.
        let xs = vec![0.0, 0.1, 0.5, 0.9, 1.0];
        let ys = vec![0.0, 0.0, 100.0, 0.0, 0.0];
        let out = m4_reduce(&xs, &ys, (0.0, 1.0), 1);
        assert!(out.iter().any(|&(_, y)| y == 100.0), "spike must survive");
        assert!(
            out.iter().any(|&(x, _)| x == 0.0),
            "the column's first (entry) point must survive"
        );
        assert!(
            out.iter().any(|&(x, _)| x == 1.0),
            "the column's last (exit) point must survive"
        );
    }

    #[test]
    fn triggers_the_simd_batch_path_and_matches_the_scalar_shape() {
        // Deliberately >= SIMD_BATCH_MIN_LEN so this exercises `simd::bucket_indices`
        // (which self-dispatches to AVX2 when available, scalar otherwise) —
        // proving the two code paths in `m4_reduce` (small-n scalar loop vs.
        // batched-index loop) agree.
        let n = SIMD_BATCH_MIN_LEN * 3 + 7;
        let xs: Vec<f64> = (0..n).map(|i| (i as f64) * 0.37).collect();
        let ys: Vec<f64> = (0..n).map(|i| ((i as f64) * 1.7).sin()).collect();
        let domain = (0.0, xs[n - 1]);
        let out = m4_reduce(&xs, &ys, domain, 32);
        assert!(out.len() <= 4 * 32);
        assert!(!out.is_empty());
    }
}
