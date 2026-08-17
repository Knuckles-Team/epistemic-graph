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
        return if threshold == 1 {
            vec![data[0]]
        } else {
            vec![data[0], data[n - 1]]
        };
    }

    let bucket_size = (n - 2) as f64 / (threshold - 2) as f64;
    let max_bucket_cap = (bucket_size.ceil() as usize) + 2;
    let mut cx_buf: Vec<f64> = Vec::with_capacity(max_bucket_cap);
    let mut cy_buf: Vec<f64> = Vec::with_capacity(max_bucket_cap);
    let mut area_buf: Vec<f64> = Vec::with_capacity(max_bucket_cap);

    let mut sampled = Vec::with_capacity(threshold);
    sampled.push(data[0]);
    let mut a = 0usize;

    for i in 0..(threshold - 2) {
        let avg_start = (((i + 1) as f64 * bucket_size) as usize + 1).min(n);
        let avg_end = (((i + 2) as f64 * bucket_size) as usize + 1).min(n);
        let avg_start = avg_start.min(avg_end);
        let avg_point = if avg_end > avg_start {
            let (mut sx, mut sy) = (0.0, 0.0);
            for p in &data[avg_start..avg_end] {
                sx += p.0;
                sy += p.1;
            }
            let len = (avg_end - avg_start) as f64;
            (sx / len, sy / len)
        } else {
            data[n - 1]
        };

        let range_start = ((i as f64 * bucket_size) as usize + 1).min(n);
        let range_end = (((i + 1) as f64 * bucket_size) as usize + 1).min(n);
        let range_start = range_start.min(range_end);

        if range_start >= range_end {
            let idx = range_start.min(n - 1);
            sampled.push(data[idx]);
            a = idx;
            continue;
        }

        cx_buf.clear();
        cy_buf.clear();
        for p in &data[range_start..range_end] {
            cx_buf.push(p.0);
            cy_buf.push(p.1);
        }
        area_buf.clear();
        area_buf.resize(cx_buf.len(), 0.0);
        simd::triangle_areas(data[a], avg_point, &cx_buf, &cy_buf, &mut area_buf);

        let mut best_local = 0usize;
        let mut best_area = area_buf[0];
        for (j, &area) in area_buf.iter().enumerate().skip(1) {
            if area > best_area {
                best_area = area;
                best_local = j;
            }
        }
        let best_idx = range_start + best_local;
        sampled.push(data[best_idx]);
        a = best_idx;
    }

    sampled.push(data[n - 1]);
    sampled
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
}
