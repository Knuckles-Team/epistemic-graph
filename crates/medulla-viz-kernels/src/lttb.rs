//! LTTB — Largest Triangle Three Buckets (D-VZ-1 lane V2; Sveinn Steinarsson,
//! "Downsampling Time Series for Visual Representation", 2013).
//!
//! Unlike [`crate::m4::m4_reduce`] (which synthesizes up to 4 aggregate points
//! per pixel column), LTTB selects `threshold` **real, original** data points —
//! it never fabricates a value. Each selected point is the one, within its
//! bucket, that forms the largest triangle with the previously-selected point
//! and the NEXT bucket's mean point — the point whose omission would most
//! change the rendered line's visual shape. This crate uses LTTB for the
//! interactive tile path (`crate` root doc): a client hovering/picking a
//! served point gets a REAL row's data, not a synthetic M4 extremum.
//!
//! **Complexity.** `O(n)` once the input is known to be x-sorted (a single
//! linear scan checks this — `data.windows(2).all(...)`), which is the common
//! case for a time series ColumnStore already ingests in arrival order;
//! `O(n log n)` when a sort is actually required for genuinely unsorted input
//! (each of the `n` points is visited exactly once by the bucket/triangle-area
//! passes regardless). Output size is exactly
//! `min(threshold, finite_input_len)`, never more.
//!
//! See `crate::simd::triangle_areas` for the SIMD-accelerated inner loop (the
//! per-candidate-point triangle-area evaluation within one bucket, a regular
//! contiguous-slice computation — genuinely vectorizable, unlike the serial
//! bucket-to-bucket selection chain around it).

use crate::simd;

/// Reduce `(xs, ys)` (equal length) to at most `threshold` points via LTTB.
/// Non-finite rows are excluded before bucketing (never selected as a
/// "representative" point). `threshold == 0` returns an empty output;
/// `threshold` at or above the (post-filter) input length returns every
/// finite point, x-sorted, unchanged — LTTB never pads to reach `threshold`
/// points that don't exist.
pub fn lttb_reduce(xs: &[f64], ys: &[f64], threshold: usize) -> Vec<(f64, f64)> {
    assert_eq!(xs.len(), ys.len(), "lttb_reduce: xs/ys length mismatch");

    let mut data: Vec<(f64, f64)> = xs
        .iter()
        .zip(ys)
        .filter(|(x, y)| x.is_finite() && y.is_finite())
        .map(|(&x, &y)| (x, y))
        .collect();

    if data.is_empty() || threshold == 0 {
        return Vec::new();
    }
    if data.len() == 1 {
        return data;
    }

    let already_sorted = data.windows(2).all(|w| w[0].0 <= w[1].0);
    if !already_sorted {
        data.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    }

    let n = data.len();
    if threshold >= n {
        return data;
    }
    if threshold <= 2 {
        return endpoints_only(&data, threshold);
    }

    select_bucket_representatives(&data, threshold)
}

/// `threshold` is 1 or 2 (and below the input length): the first point, or the first
/// and last.
fn endpoints_only(data: &[(f64, f64)], threshold: usize) -> Vec<(f64, f64)> {
    if threshold == 1 {
        vec![data[0]]
    } else {
        vec![data[0], data[data.len() - 1]]
    }
}

/// The LTTB selection chain for `3 <= threshold < data.len()` over x-sorted, finite
/// `data`: the first point, one representative per interior bucket, the last point.
fn select_bucket_representatives(data: &[(f64, f64)], threshold: usize) -> Vec<(f64, f64)> {
    let n = data.len();
    let buckets = LttbBuckets {
        size: (n - 2) as f64 / (threshold - 2) as f64,
        n,
    };
    let mut scratch = TriangleScratch::with_capacity((buckets.size.ceil() as usize) + 2);

    let mut sampled = Vec::with_capacity(threshold);
    sampled.push(data[0]);
    let mut a = 0usize;

    for i in 0..(threshold - 2) {
        let avg_point = buckets.next_bucket_mean(data, i);
        let (range_start, range_end) = buckets.bounds(i, i + 1);
        a = if range_start >= range_end {
            range_start.min(n - 1)
        } else {
            range_start + scratch.largest_triangle(data, a, avg_point, range_start, range_end)
        };
        sampled.push(data[a]);
    }

    sampled.push(data[n - 1]);
    sampled
}

/// The bucket geometry of one LTTB run over `n` points.
struct LttbBuckets {
    size: f64,
    n: usize,
}

impl LttbBuckets {
    /// `[start, end)` of the index range spanning bucket boundaries `from..to`,
    /// clamped to `n` and never inverted.
    fn bounds(&self, from: usize, to: usize) -> (usize, usize) {
        let start = ((from as f64 * self.size) as usize + 1).min(self.n);
        let end = ((to as f64 * self.size) as usize + 1).min(self.n);
        (start.min(end), end)
    }

    /// The mean point of the bucket after bucket `i`, or the last point when that
    /// bucket is empty.
    fn next_bucket_mean(&self, data: &[(f64, f64)], i: usize) -> (f64, f64) {
        let (avg_start, avg_end) = self.bounds(i + 1, i + 2);
        if avg_end <= avg_start {
            return data[self.n - 1];
        }
        let (mut sx, mut sy) = (0.0, 0.0);
        for p in &data[avg_start..avg_end] {
            sx += p.0;
            sy += p.1;
        }
        let len = (avg_end - avg_start) as f64;
        (sx / len, sy / len)
    }
}

/// Reused per-bucket buffers for the SIMD triangle-area pass.
struct TriangleScratch {
    cx: Vec<f64>,
    cy: Vec<f64>,
    area: Vec<f64>,
}

impl TriangleScratch {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            cx: Vec::with_capacity(capacity),
            cy: Vec::with_capacity(capacity),
            area: Vec::with_capacity(capacity),
        }
    }

    /// Offset within `data[range_start..range_end]` (non-empty) of the point forming
    /// the largest triangle with `data[a]` and `avg_point`; the first wins ties.
    fn largest_triangle(
        &mut self,
        data: &[(f64, f64)],
        a: usize,
        avg_point: (f64, f64),
        range_start: usize,
        range_end: usize,
    ) -> usize {
        self.cx.clear();
        self.cy.clear();
        for p in &data[range_start..range_end] {
            self.cx.push(p.0);
            self.cy.push(p.1);
        }
        self.area.clear();
        self.area.resize(self.cx.len(), 0.0);
        simd::triangle_areas(data[a], avg_point, &self.cx, &self.cy, &mut self.area);
        first_max_index(&self.area)
    }
}

/// Index of the first maximum of a non-empty slice (strict `>` keeps the earliest).
fn first_max_index(values: &[f64]) -> usize {
    let mut best_local = 0usize;
    let mut best_area = values[0];
    for (j, &area) in values.iter().enumerate().skip(1) {
        if area > best_area {
            best_area = area;
            best_local = j;
        }
    }
    best_local
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_yields_empty_output() {
        assert!(lttb_reduce(&[], &[], 100).is_empty());
    }

    #[test]
    fn zero_threshold_yields_empty_output() {
        assert!(lttb_reduce(&[1.0, 2.0], &[1.0, 2.0], 0).is_empty());
    }

    #[test]
    fn single_point_is_returned_unchanged() {
        assert_eq!(lttb_reduce(&[5.0], &[7.0], 100), vec![(5.0, 7.0)]);
    }

    #[test]
    fn threshold_above_input_length_returns_every_finite_point_unpadded() {
        let xs = vec![1.0, 2.0, 3.0];
        let ys = vec![1.0, 4.0, 9.0];
        let out = lttb_reduce(&xs, &ys, 1000);
        assert_eq!(out.len(), 3, "must not pad beyond the real input");
        assert_eq!(out, vec![(1.0, 1.0), (2.0, 4.0), (3.0, 9.0)]);
    }

    #[test]
    fn output_length_matches_threshold_exactly() {
        let n = 10_000;
        let xs: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let ys: Vec<f64> = (0..n).map(|i| (i as f64 * 0.01).sin()).collect();
        for threshold in [3usize, 100, 999, 5000] {
            let out = lttb_reduce(&xs, &ys, threshold);
            assert_eq!(out.len(), threshold, "threshold={threshold}");
        }
    }

    #[test]
    fn output_always_includes_first_and_last_point() {
        let xs: Vec<f64> = (0..5000).map(|i| i as f64).collect();
        let ys: Vec<f64> = (0..5000).map(|i| (i as f64).cos()).collect();
        let out = lttb_reduce(&xs, &ys, 200);
        assert_eq!(*out.first().unwrap(), (xs[0], ys[0]));
        assert_eq!(*out.last().unwrap(), (xs[4999], ys[4999]));
    }

    #[test]
    fn output_is_selected_real_points_never_synthetic_averages() {
        // Every output point must be a member of the original (x,y) set --
        // LTTB never invents a value the way an aggregate/mean reduction would.
        let xs: Vec<f64> = (0..2000).map(|i| i as f64).collect();
        let ys: Vec<f64> = (0..2000)
            .map(|i| ((i as f64) * 0.05).sin() * 100.0)
            .collect();
        let original: std::collections::HashSet<(u64, u64)> = xs
            .iter()
            .zip(&ys)
            .map(|(&x, &y)| (x.to_bits(), y.to_bits()))
            .collect();
        let out = lttb_reduce(&xs, &ys, 150);
        for p in out {
            assert!(
                original.contains(&(p.0.to_bits(), p.1.to_bits())),
                "point {p:?} was not in the original series"
            );
        }
    }

    #[test]
    fn nan_and_infinite_rows_are_excluded() {
        let xs = [1.0, f64::NAN, 3.0, f64::INFINITY, 5.0, 6.0];
        let ys = [1.0, 2.0, f64::NAN, 4.0, f64::NEG_INFINITY, 6.0];
        let out = lttb_reduce(&xs, &ys, 100);
        // Only (1,1) and (6,6) are fully finite on both axes.
        assert_eq!(out, vec![(1.0, 1.0), (6.0, 6.0)]);
    }

    #[test]
    fn all_equal_values_do_not_panic() {
        let xs = vec![3.0; 500];
        let ys = vec![9.0; 500];
        let out = lttb_reduce(&xs, &ys, 50);
        assert_eq!(out.len(), 50);
        assert!(out.iter().all(|&p| p == (3.0, 9.0)));
    }

    #[test]
    fn unsorted_input_is_sorted_before_reduction() {
        let n = 3000;
        let mut xs: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let mut ys: Vec<f64> = (0..n).map(|i| (i as f64 * 0.02).sin()).collect();
        // Reverse arrival order -- LTTB must still treat this as the SAME
        // temporal series (sorted by x), producing an x-monotonic reduction.
        xs.reverse();
        ys.reverse();
        let out = lttb_reduce(&xs, &ys, 100);
        assert!(out.windows(2).all(|w| w[0].0 <= w[1].0));
        assert_eq!(out.first().unwrap().0, 0.0);
        assert_eq!(out.last().unwrap().0, (n - 1) as f64);
    }

    fn wavy_series(n: usize) -> (Vec<f64>, Vec<f64>) {
        let xs: Vec<f64> = (0..n).map(|i| i as f64 * 0.5).collect();
        let ys: Vec<f64> = (0..n)
            .map(|i| (i as f64 * 0.37).sin() * 10.0 + (i % 7) as f64)
            .collect();
        (xs, ys)
    }

    #[test]
    fn selected_points_are_pinned_for_a_fixed_series() {
        let (xs, ys) = wavy_series(120);
        assert_eq!(format!("{:?}", lttb_reduce(&xs, &ys, 10)), "[(0.0, 0.0), (2.5, 14.612752029752999), (7.5, -5.69239857276262), (19.5, 13.574299115788811), (23.0, -5.667081186743308), (36.5, 12.534067706087917), (40.0, -6.701057337071854), (45.0, 15.513287387867827), (56.5, -7.244872570338623), (59.5, 0.4768476000354508)]");
        let (mut rxs, mut rys) = (xs.clone(), ys.clone());
        rxs.reverse();
        rys.reverse();
        assert_eq!(
            format!("{:?}", lttb_reduce(&rxs, &rys, 10)),
            "[(0.0, 0.0), (2.5, 14.612752029752999), (7.5, -5.69239857276262), (19.5, 13.574299115788811), (23.0, -5.667081186743308), (36.5, 12.534067706087917), (40.0, -6.701057337071854), (45.0, 15.513287387867827), (56.5, -7.244872570338623), (59.5, 0.4768476000354508)]"
        );
    }

    #[test]
    fn short_series_edge_thresholds_are_pinned() {
        let (xs, ys) = wavy_series(9);
        assert_eq!(format!("{:?}", lttb_reduce(&xs, &ys, 8)), "[(0.0, 0.0), (0.5, 4.61615431964962), (1.0, 8.74287911628145), (1.5, 11.956986856800476), (2.0, 13.9588084453764), (2.5, 14.612752029752999), (3.0, 13.965654722360867), (4.0, 2.805962678942329)]");
        assert_eq!(
            format!("{:?}", lttb_reduce(&xs, &ys, 3)),
            "[(0.0, 0.0), (2.5, 14.612752029752999), (4.0, 2.805962678942329)]"
        );
        assert_eq!(
            lttb_reduce(&xs, &ys, 2),
            vec![(xs[0], ys[0]), (xs[8], ys[8])]
        );
        assert_eq!(lttb_reduce(&xs, &ys, 1), vec![(xs[0], ys[0])]);
    }
}
