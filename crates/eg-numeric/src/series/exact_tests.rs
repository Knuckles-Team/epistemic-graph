//! EH-562 oracles: the O(1) exact rolling statistics against an exact recompute.
//!
//! Series on a dyadic grid `x = k · 2^s` (`k` a bounded integer) have exact integer sums
//! in `i128`, so the oracle is the true value correctly rounded — the kernel must match it
//! BIT FOR BIT for sum / mean / variance / weighted sum, at every step, for ordinary,
//! huge (`2^800`) and tiny (`2^-900`) scales. Arbitrary `f64` flat windows must report a
//! deviation of exactly zero, whatever came before them.

use proptest::prelude::*;

use super::super::{apply, apply_pair, PairStat, Rolling, Spec};
use super::super::wide::scale_pow2;

/// `num / den` (`num ≥ 0`, `den > 0`) correctly rounded, times `2^exp`.
fn rounded_quotient(num: u128, den: u128, exp: i32) -> f64 {
    if num == 0 {
        return 0.0;
    }
    let shift = num.leading_zeros().saturating_sub(1);
    let scaled = num << shift;
    let q = scaled / den;
    let sticky = u128::from(scaled % den != 0);
    let top = 128 - q.leading_zeros();
    let drop = top.saturating_sub(64);
    let lost = u128::from(q & ((1u128 << drop) - 1) != 0);
    let leading = ((q >> drop) | sticky | lost) as u64 as f64;
    scale_pow2(leading, exp - shift as i32 + drop as i32)
}

fn signed_quotient(num: i128, den: u128, exp: i32) -> f64 {
    let magnitude = rounded_quotient(num.unsigned_abs(), den, exp);
    if num < 0 {
        -magnitude
    } else {
        magnitude
    }
}

/// The exact window statistics of `ks · 2^s`.
struct Oracle {
    sum: f64,
    mean: f64,
    variance: f64,
}

fn oracle(ks: &[i64], s: i32) -> Oracle {
    let n = ks.len() as i128;
    let s1: i128 = ks.iter().map(|&k| i128::from(k)).sum();
    let s2: i128 = ks.iter().map(|&k| i128::from(k) * i128::from(k)).sum();
    let spread = (n * s2 - s1 * s1) as u128;
    Oracle {
        sum: signed_quotient(s1, 1, s),
        mean: signed_quotient(s1, n as u128, s),
        variance: rounded_quotient(spread, (n * n) as u128, 2 * s),
    }
}

fn grid(ks: &[i64], s: i32) -> Vec<f64> {
    ks.iter().map(|&k| scale_pow2(k as f64, s)).collect()
}

fn bits(v: Option<f64>) -> Option<u64> {
    v.map(f64::to_bits)
}

fn check_rolling(ks: &[i64], s: i32, w: usize) {
    let xs = grid(ks, s);
    let run = |op| apply(Spec::Rolling(op, w), &xs).unwrap();
    let (sum, mean, std) = (run(Rolling::Sum), run(Rolling::Mean), run(Rolling::Std));
    for end in w..=ks.len() {
        let want = oracle(&ks[end - w..end], s);
        let at = end - 1;
        assert_eq!(bits(sum[at]), bits(Some(want.sum)), "sum at {at} (scale {s})");
        assert_eq!(bits(mean[at]), bits(Some(want.mean)), "mean at {at} (scale {s})");
        let root = want.variance.sqrt();
        assert_eq!(bits(std[at]), bits(Some(root)), "std at {at} (scale {s})");
    }
}

fn check_weighted(ks: &[i64], ws: &[i64], s: i32, w: usize) {
    let (xs, ys) = (grid(ks, s), grid(ws, 3));
    let got = apply_pair(Spec::Pair(PairStat::WeightedSum, w), &xs, &ys).unwrap();
    for end in w..=ks.len() {
        let exact: i128 = (end - w..end)
            .map(|i| i128::from(ks[i]) * i128::from(ws[i]))
            .sum();
        let want = signed_quotient(exact, 1, s + 3);
        assert_eq!(bits(got[end - 1]), bits(Some(want)), "wsum at {}", end - 1);
    }
}

const SCALES: [i32; 4] = [-20, 0, 800, -900];

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn rolling_moments_equal_the_exact_recompute(
        ks in prop::collection::vec(-(1i64 << 40)..(1i64 << 40), 1..80),
        w in 1usize..12,
        scale in 0usize..4,
    ) {
        prop_assume!(w <= ks.len());
        check_rolling(&ks, SCALES[scale], w);
    }

    #[test]
    fn weighted_sums_equal_the_exact_recompute(
        pairs in prop::collection::vec((-(1i64 << 30)..(1i64 << 30), -(1i64 << 20)..(1i64 << 20)), 2..60),
        w in 2usize..10,
        scale in 0usize..3,
    ) {
        prop_assume!(w <= pairs.len());
        let (ks, ws): (Vec<i64>, Vec<i64>) = pairs.into_iter().unzip();
        check_weighted(&ks, &ws, SCALES[scale], w);
    }

    #[test]
    fn a_flat_window_has_exactly_zero_deviation(
        prefix in prop::collection::vec(-1e12f64..1e12, 0..40),
        level in prop::num::f64::NORMAL | prop::num::f64::SUBNORMAL | prop::num::f64::ZERO,
        w in 2usize..16,
    ) {
        let mut xs = prefix;
        xs.extend(std::iter::repeat_n(level, w));
        let last = xs.len() - 1;
        let std = apply(Spec::Rolling(Rolling::Std, w), &xs).unwrap();
        let z = apply(Spec::Rolling(Rolling::Zscore, w), &xs).unwrap();
        let mean = apply(Spec::Rolling(Rolling::Mean, w), &xs).unwrap();
        prop_assert_eq!(bits(std[last]), bits(Some(0.0)));
        prop_assert_eq!(bits(z[last]), bits(Some(0.0)));
        prop_assert_eq!(mean[last], Some(level));
    }
}

/// Adversarial series: huge magnitude with a tiny spread (catastrophic cancellation for
/// `E[x²] − E[x]²` and residue for add/remove Welford), alternating extremes, and a
/// return to flat after an excursion of 300 orders of magnitude.
#[test]
fn adversarial_series_stay_exact() {
    let huge: Vec<i64> = (0..60).map(|i| (1i64 << 52) + (i % 5)).collect();
    check_rolling(&huge, 800, 7);
    check_rolling(&huge, 0, 7);
    let alternating: Vec<i64> = (0..60).map(|i| if i % 2 == 0 { 1 << 45 } else { -(1 << 45) + 1 }).collect();
    check_rolling(&alternating, 0, 5);
    check_rolling(&alternating, -900, 6);

    let mut xs = vec![1e300, -1e300, 3e-300, 7.0, 1e300];
    xs.extend(std::iter::repeat_n(0.1, 4));
    let std = apply(Spec::Rolling(Rolling::Std, 4), &xs).unwrap();
    let mean = apply(Spec::Rolling(Rolling::Mean, 4), &xs).unwrap();
    assert_eq!(std[xs.len() - 1], Some(0.0));
    assert_eq!(bits(mean[xs.len() - 1]), bits(Some(0.1)));
}

/// A correlation is scale-free and exact enough to be ±1 on a perfectly linear window,
/// `None` on a flat side, and agrees with the two-pass Pearson on ordinary data.
#[test]
fn correlation_is_exact_at_the_edges() {
    let xs: Vec<f64> = (0..40).map(|i| scale_pow2(f64::from((1 << 20) + i), 600)).collect();
    let ys: Vec<f64> = (0..40).map(|i| scale_pow2(-f64::from(i), -800)).collect();
    let r = apply_pair(Spec::Pair(PairStat::Corr, 8), &xs, &ys).unwrap();
    assert!(r[7..].iter().all(|v| v.is_some_and(|c| (c + 1.0).abs() < 1e-12)), "{r:?}");
    let flat = vec![2.5; 40];
    let r = apply_pair(Spec::Pair(PairStat::Corr, 8), &xs, &flat).unwrap();
    assert!(r.iter().all(Option::is_none));

    let a: Vec<f64> = (0..50).map(|i| f64::from((i * 37) % 11) - 4.0).collect();
    let b: Vec<f64> = (0..50).map(|i| f64::from((i * 13) % 7) * 0.5).collect();
    let got = apply_pair(Spec::Pair(PairStat::Corr, 9), &a, &b).unwrap();
    for end in 9..=50 {
        let want = super::super::window::pearson(&a[end - 9..end], &b[end - 9..end]);
        match (got[end - 1], want) {
            (Some(g), Some(w)) => assert!((g - w).abs() < 1e-12, "{g} vs {w}"),
            (g, w) => assert_eq!(g.is_some(), w.is_some()),
        }
    }
}

/// A non-finite value poisons exactly the windows that hold it, then the exact path resumes.
#[test]
fn a_non_finite_value_propagates_only_while_in_the_window() {
    let xs = [1.0, 2.0, f64::NAN, 4.0, 5.0, 6.0, 7.0];
    let mean = apply(Spec::Rolling(Rolling::Mean, 3), &xs).unwrap();
    assert!(mean[2].is_some_and(f64::is_nan) && mean[4].is_some_and(f64::is_nan));
    assert_eq!(mean[5], Some(5.0));
    assert_eq!(mean[6], Some(6.0));
    let sum = apply(Spec::Rolling(Rolling::Sum, 2), &[1.0, f64::INFINITY, 2.0, 3.0]).unwrap();
    assert_eq!(sum, vec![None, Some(f64::INFINITY), Some(f64::INFINITY), Some(5.0)]);
}

/// A state restored from its checkpoint rebuilds the exact sums and continues bit for bit.
#[test]
fn a_restored_window_rebuilds_its_exact_sums() {
    let xs: Vec<f64> = (0..64).map(|i| 1e9 + f64::from(i % 9) * 1e-3).collect();
    let spec = Spec::Rolling(Rolling::Zscore, 16);
    let whole: Vec<Option<u64>> = apply(spec, &xs).unwrap().into_iter().map(bits).collect();
    let mut state = super::super::State::new(spec).unwrap();
    let mut got: Vec<Option<u64>> = xs[..30].iter().map(|&x| bits(state.step(Some(x), None))).collect();
    let bytes = rmp_serde::to_vec(&state).unwrap();
    let mut restored: super::super::State = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(restored, state);
    got.extend(xs[30..].iter().map(|&x| bits(restored.step(Some(x), None))));
    assert_eq!(got, whole);
}
