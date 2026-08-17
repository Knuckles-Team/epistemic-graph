//! Property-based invariants for the V2 decimation kernels (D-VZ-1).
//!
//! These assert the CORRECTNESS BAR the program explicitly requires: M4 and
//! LTTB must be provably shape-preserving/visually-lossless at the stated
//! pixel width — not just "matches a golden image", which only proves one
//! fixed input stayed the same, never that the algorithm holds in general.
//! Each property below is an invariant that must hold for EVERY input
//! `proptest` can generate (thousands of randomized cases per run, shrunk to
//! a minimal counterexample on failure), including the required edge classes:
//! NaN, +/-infinity, empty input, a single point, all-equal values, and
//! unsorted input (`proptest`'s random generation already covers all of
//! these; a few `#[test]` cases below additionally pin them down explicitly
//! so a regression names the exact failing class rather than a shrunk-but-
//! opaque proptest counterexample).

use eg_viz_kernels::{lttb_reduce, m4_reduce, simd};
use proptest::prelude::*;
use std::collections::HashSet;

fn finite_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        9 => -1.0e6f64..1.0e6f64,
        1 => Just(0.0f64),
    ]
}

/// x/y pairs, always finite (isolates the SHAPE invariants below from the
/// separately-covered NaN/Inf-filtering behavior).
fn finite_series(max_len: usize) -> impl Strategy<Value = (Vec<f64>, Vec<f64>)> {
    (0..=max_len).prop_flat_map(move |n| {
        (
            prop::collection::vec(finite_f64(), n),
            prop::collection::vec(finite_f64(), n),
        )
    })
}

/// x/y pairs that MAY contain NaN/+-inf, at any position, in any x order —
/// the "never panic, never fabricate" fuzz target.
fn noisy_series(max_len: usize) -> impl Strategy<Value = (Vec<f64>, Vec<f64>)> {
    let noisy_f64 = prop_oneof![
        6 => -1.0e6f64..1.0e6f64,
        1 => Just(f64::NAN),
        1 => Just(f64::INFINITY),
        1 => Just(f64::NEG_INFINITY),
        1 => Just(0.0f64),
    ];
    (0..=max_len).prop_flat_map(move |n| {
        (
            prop::collection::vec(noisy_f64.clone(), n),
            prop::collection::vec(noisy_f64.clone(), n),
        )
    })
}

fn global_y_range(xs: &[f64], ys: &[f64]) -> Option<(f64, f64)> {
    xs.iter()
        .zip(ys)
        .filter(|(x, y)| x.is_finite() && y.is_finite())
        .map(|(_, &y)| y)
        .fold(None, |acc, y| {
            Some(acc.map_or((y, y), |(lo, hi): (f64, f64)| (lo.min(y), hi.max(y))))
        })
}

fn x_domain(xs: &[f64]) -> (f64, f64) {
    let finite: Vec<f64> = xs.iter().copied().filter(|x| x.is_finite()).collect();
    if finite.is_empty() {
        return (0.0, 1.0);
    }
    let lo = finite.iter().cloned().fold(f64::INFINITY, f64::min);
    let hi = finite.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    (lo, hi)
}

proptest! {
    /// M4 never emits more than 4 points per pixel column, regardless of row
    /// count — the property `select_tier` relies on to bound render cost.
    #[test]
    fn m4_output_bounded_by_four_times_width((xs, ys) in finite_series(600), width_px in 1u32..64) {
        let domain = x_domain(&xs);
        let out = m4_reduce(&xs, &ys, domain, width_px);
        prop_assert!(out.len() <= 4 * width_px as usize);
    }

    /// M4 never fabricates a y-value outside the real data's range — an
    /// aggregate kernel that could report a min/max beyond the true extremes
    /// would be lying about the data's shape.
    #[test]
    fn m4_never_reports_a_y_outside_the_true_range((xs, ys) in finite_series(600), width_px in 1u32..64) {
        let domain = x_domain(&xs);
        let out = m4_reduce(&xs, &ys, domain, width_px);
        if let Some((lo, hi)) = global_y_range(&xs, &ys) {
            for &(_, y) in &out {
                prop_assert!(y >= lo - 1e-9 && y <= hi + 1e-9, "y={y} outside [{lo},{hi}]");
            }
        } else {
            prop_assert!(out.is_empty());
        }
    }

    /// M4's output is always x-sorted, independent of input arrival order —
    /// bucketing is by x-VALUE, not array position.
    #[test]
    fn m4_output_is_always_x_sorted((xs, ys) in finite_series(600), width_px in 1u32..64) {
        let domain = x_domain(&xs);
        let out = m4_reduce(&xs, &ys, domain, width_px);
        prop_assert!(out.windows(2).all(|w| w[0].0 <= w[1].0));
    }

    /// M4 never panics on adversarial input containing NaN/+-inf mixed with
    /// finite values, at any length, in any order.
    #[test]
    fn m4_never_panics_on_noisy_input((xs, ys) in noisy_series(400), width_px in 1u32..64) {
        let domain = x_domain(&xs);
        let _ = m4_reduce(&xs, &ys, domain, width_px);
    }

    /// LTTB's output length is exactly `min(threshold, finite_input_len)` —
    /// never padded, never truncated short.
    #[test]
    fn lttb_output_length_matches_threshold_or_input((xs, ys) in finite_series(600), threshold in 0usize..80) {
        let out = lttb_reduce(&xs, &ys, threshold);
        let finite_len = xs.iter().zip(&ys).filter(|(x, y)| x.is_finite() && y.is_finite()).count();
        prop_assert_eq!(out.len(), threshold.min(finite_len));
    }

    /// Every LTTB output point is a REAL row from the input — LTTB selects,
    /// it never synthesizes a value the way a mean/aggregate kernel would.
    #[test]
    fn lttb_output_points_are_all_real_input_rows((xs, ys) in finite_series(600), threshold in 3usize..80) {
        let original: HashSet<(u64, u64)> = xs
            .iter()
            .zip(&ys)
            .filter(|(x, y)| x.is_finite() && y.is_finite())
            .map(|(&x, &y)| (x.to_bits(), y.to_bits()))
            .collect();
        let out = lttb_reduce(&xs, &ys, threshold);
        for p in &out {
            prop_assert!(original.contains(&(p.0.to_bits(), p.1.to_bits())));
        }
    }

    /// LTTB's output is x-sorted regardless of input arrival order.
    #[test]
    fn lttb_output_is_always_x_sorted((xs, ys) in finite_series(600), threshold in 0usize..80) {
        let out = lttb_reduce(&xs, &ys, threshold);
        prop_assert!(out.windows(2).all(|w| w[0].0 <= w[1].0));
    }

    /// LTTB never panics on adversarial NaN/+-inf-laced input.
    #[test]
    fn lttb_never_panics_on_noisy_input((xs, ys) in noisy_series(400), threshold in 0usize..80) {
        let _ = lttb_reduce(&xs, &ys, threshold);
    }

    /// The runtime-dispatched SIMD bucket-index path (used inside `m4_reduce`
    /// once input is large enough) must be byte-identical to the scalar
    /// reference for every randomized domain/column-count combination — the
    /// load-bearing proof that "SIMD accelerates, never changes the answer".
    #[test]
    fn simd_bucket_indices_matches_scalar_reference(
        (xs, _ys) in noisy_series(300),
        domain_min in -1000.0f64..1000.0,
        span in 0.01f64..2000.0,
        cols in 1usize..500,
    ) {
        let domain_max = domain_min + span;
        let inv_range = 1.0 / (domain_max - domain_min);
        let mut idx_scalar = vec![0u32; xs.len()];
        let mut fin_scalar = vec![false; xs.len()];
        simd::bucket_indices_scalar(&xs, domain_min, inv_range, cols, &mut idx_scalar, &mut fin_scalar);

        let mut idx_dispatch = vec![0u32; xs.len()];
        let mut fin_dispatch = vec![false; xs.len()];
        simd::bucket_indices(&xs, domain_min, inv_range, cols, &mut idx_dispatch, &mut fin_dispatch);

        prop_assert_eq!(idx_scalar, idx_dispatch);
        prop_assert_eq!(fin_scalar, fin_dispatch);
    }

    /// Same cross-check for the triangle-area SIMD path LTTB uses.
    #[test]
    fn simd_triangle_areas_matches_scalar_reference(
        (cx, cy) in finite_series(300),
        prev in (-100.0f64..100.0, -100.0f64..100.0),
        avg in (-100.0f64..100.0, -100.0f64..100.0),
    ) {
        let mut scalar_out = vec![0.0; cx.len()];
        simd::triangle_areas_scalar(prev, avg, &cx, &cy, &mut scalar_out);

        let mut dispatch_out = vec![0.0; cx.len()];
        simd::triangle_areas(prev, avg, &cx, &cy, &mut dispatch_out);

        for (a, b) in scalar_out.iter().zip(&dispatch_out) {
            // Bitwise-identical is not guaranteed across differing instruction
            // sequences on some platforms (fma contraction), so this compares
            // with a tight epsilon rather than `assert_eq!` -- still proves
            // "same answer", not "same bits".
            prop_assert!((a - b).abs() <= (a.abs().max(b.abs()) * 1e-9).max(1e-12));
        }
    }
}

// ── Pinned edge-class regressions (explicit, not left to proptest shrinking) ──

#[test]
fn m4_handles_all_five_required_edge_classes_without_panicking() {
    // Empty.
    assert!(m4_reduce(&[], &[], (0.0, 1.0), 100).is_empty());
    // Single point.
    assert_eq!(m4_reduce(&[1.0], &[2.0], (0.0, 2.0), 100), vec![(1.0, 2.0)]);
    // All-equal values.
    let eq_out = m4_reduce(&vec![5.0; 200], &vec![5.0; 200], (5.0, 5.0), 20);
    assert_eq!(eq_out, vec![(5.0, 5.0)]);
    // NaN/infinity mixed in.
    let noisy_out = m4_reduce(
        &[f64::NAN, 1.0, f64::INFINITY, 2.0, f64::NEG_INFINITY],
        &[1.0, f64::NAN, 2.0, 3.0, 4.0],
        (0.0, 3.0),
        10,
    );
    assert_eq!(noisy_out, vec![(2.0, 3.0)]);
    // Unsorted input.
    let mut xs: Vec<f64> = (0..100).map(|i| i as f64).collect();
    xs.reverse();
    let ys = vec![1.0; 100];
    let unsorted_out = m4_reduce(&xs, &ys, (0.0, 99.0), 10);
    assert!(unsorted_out.windows(2).all(|w| w[0].0 <= w[1].0));
}

#[test]
fn lttb_handles_all_five_required_edge_classes_without_panicking() {
    assert!(lttb_reduce(&[], &[], 100).is_empty());
    assert_eq!(lttb_reduce(&[1.0], &[2.0], 100), vec![(1.0, 2.0)]);
    let eq_out = lttb_reduce(&vec![5.0; 200], &vec![5.0; 200], 20);
    assert_eq!(eq_out.len(), 20);
    assert!(eq_out.iter().all(|&p| p == (5.0, 5.0)));
    let noisy_out = lttb_reduce(
        &[f64::NAN, 1.0, f64::INFINITY, 2.0, f64::NEG_INFINITY],
        &[1.0, f64::NAN, 2.0, 3.0, 4.0],
        10,
    );
    assert_eq!(noisy_out, vec![(2.0, 3.0)]);
    let mut xs: Vec<f64> = (0..100).map(|i| i as f64).collect();
    xs.reverse();
    let ys = vec![1.0; 100];
    let unsorted_out = lttb_reduce(&xs, &ys, 10);
    assert!(unsorted_out.windows(2).all(|w| w[0].0 <= w[1].0));
    assert_eq!(unsorted_out.first().unwrap().0, 0.0);
    assert_eq!(unsorted_out.last().unwrap().0, 99.0);
}
