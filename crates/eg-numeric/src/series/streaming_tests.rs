//! EH-562: the O(1) rolling statistics against the exact two-pass recompute, over many
//! seeded series (drifting, flat runs, a large offset, ties) and window widths.

use super::*;
use crate::series::window::{pearson, Moments};

/// A seeded series: a random walk at `offset` with a flat run and repeated levels.
fn series(n: usize, seed: u64, offset: f64) -> Vec<f64> {
    let mut state = seed;
    let mut level = offset;
    (0..n)
        .map(|i| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let step = ((state >> 33) % 41) as f64 - 20.0;
            if !(30..30 + (seed as usize % 17) + 3).contains(&i) {
                level += step / 8.0;
            }
            level
        })
        .collect()
}

fn close(got: f64, want: f64, scale: f64) -> bool {
    (got - want).abs() <= 1e-12 * scale.max(1.0)
}

fn check_moments(xs: &[f64], window: usize) {
    let mut stats = RollingMoments::new(window);
    for (i, &x) in xs.iter().enumerate() {
        let got = stats.step(x);
        if i + 1 < window {
            assert_eq!(got, None, "warm-up at {i}");
            continue;
        }
        let win = &xs[i + 1 - window..=i];
        let want = Moments::of(win.iter().copied()).unwrap();
        let got = got.unwrap();
        let scale = win.iter().fold(0.0_f64, |m, v| m.max(v.abs()));
        assert!(close(got.mean, want.mean, scale), "mean at {i} w={window}");
        assert!(
            close(got.sum, want.sum, scale * window as f64),
            "sum at {i} w={window}"
        );
        // The two-pass reference itself carries ~eps·|x|/spread relative error at a large
        // offset, so the variance is compared relatively, plus an eps·|x| floor.
        let var_tol = 1e-6 * want.variance + 1e-12 * scale;
        assert!(
            (got.variance - want.variance).abs() <= var_tol,
            "var at {i} w={window}"
        );
        if win.iter().all(|&v| v == win[0]) {
            assert_eq!(
                (got.mean, got.variance),
                (win[0], 0.0),
                "flat window at {i}"
            );
        }
    }
}

// spec: EG-FEDERATED-QUERY-R037
#[test]
fn rolling_moments_match_the_exact_recompute_on_every_window() {
    for seed in 1..=24 {
        for offset in [0.0, 100.0, 1.0e9] {
            let xs = series(400, seed, offset);
            for window in [1, 2, 3, 7, 16, 61] {
                check_moments(&xs, window);
            }
        }
    }
}

// spec: EG-FEDERATED-QUERY-R037
#[test]
fn a_flat_window_reports_exactly_zero_spread_after_any_history() {
    let mut xs = series(500, 5, 1.0e6);
    xs.extend(std::iter::repeat_n(1.0e6 + 0.1, 40));
    let mut stats = RollingMoments::new(9);
    let last = xs.iter().map(|&x| stats.step(x)).last().flatten().unwrap();
    assert_eq!(last.variance, 0.0);
    assert_eq!(last.mean, 1.0e6 + 0.1);
}

// spec: EG-FEDERATED-QUERY-R037
#[test]
fn a_non_finite_value_falls_back_while_in_the_window_and_leaves_no_residue() {
    let mut xs = series(60, 9, 50.0);
    xs[20] = f64::INFINITY;
    xs[21] = f64::NAN;
    let window = 5;
    let mut stats = RollingMoments::new(window);
    for (i, &x) in xs.iter().enumerate() {
        let got = stats.step(x);
        if i + 1 < window {
            continue;
        }
        let want = Moments::of(xs[i + 1 - window..=i].iter().copied()).unwrap();
        let got = got.unwrap();
        if (20..21 + window).contains(&i) {
            assert!(
                got.mean.is_nan() || got.mean.is_infinite(),
                "poisoned at {i}"
            );
        } else {
            assert!(close(got.mean, want.mean, 100.0), "clean again at {i}");
            assert!(
                close(got.variance, want.variance, 1.0e4),
                "clean again at {i}"
            );
        }
    }
}

#[test]
fn rolling_pairs_match_the_two_pass_correlation_and_weighted_sum() {
    for seed in 1..=16 {
        let (xs, ys) = (series(300, seed, 1.0e3), series(300, seed + 40, 5.0));
        for window in [2, 5, 12, 40] {
            assert_rolling_pair_window(&xs, &ys, window);
        }
    }
}

fn assert_rolling_pair_window(xs: &[f64], ys: &[f64], window: usize) {
    let mut pairs = RollingPairs::new(window);
    for i in 0..xs.len() {
        if !pairs.push(xs[i], ys[i]) {
            continue;
        }
        let (wx, wy) = (&xs[i + 1 - window..=i], &ys[i + 1 - window..=i]);
        let sums = pairs.sums().unwrap();
        let want = pearson(wx, wy);
        let got = if pairs.has_flat_side() {
            None
        } else {
            sums.correlation(window)
        };
        match (got, want) {
            (Some(g), Some(w)) => assert!((g - w).abs() < 1e-9, "corr at {i}"),
            (g, w) => assert_eq!(g.is_some(), w.is_some(), "corr defined at {i}"),
        }
        let dot = wx.iter().zip(wy).fold(0.0, |acc, (x, y)| acc + x * y);
        assert!(close(sums.product_sum(), dot, dot.abs()), "wsum at {i}");
    }
}

#[test]
fn a_restored_checkpoint_continues_bit_identically() {
    let xs = series(300, 3, 1.0e6);
    let mut whole = RollingMoments::new(13);
    let expected: Vec<_> = xs.iter().map(|&x| whole.step(x)).collect();
    let mut first = RollingMoments::new(13);
    let mut got: Vec<_> = xs[..140].iter().map(|&x| first.step(x)).collect();
    let bytes = rmp_serde::to_vec(&first).unwrap();
    let mut restored: RollingMoments = rmp_serde::from_slice(&bytes).unwrap();
    got.extend(xs[140..].iter().map(|&x| restored.step(x)));
    assert_eq!(got, expected);
}
