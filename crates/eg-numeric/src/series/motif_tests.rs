//! EH-529 oracles: MASS against naive distances, the SCRIMP++ matrix profile against a
//! brute-force O(n²m) profile, planted motif and discord recovery, the anytime cut, and
//! seed-independence of a complete profile.

use super::distance::exclusion_zone;
use super::mass::{mass, Shape};
use super::matrix_profile::{discords, matrix_profile, motifs, MatrixProfile, ProfileOptions};
use crate::detkernel::math;

/// Deterministic noise in `[-0.5, 0.5)`.
fn noise(n: usize, seed: u64) -> Vec<f64> {
    let mut state = seed;
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            ((state >> 11) as f64 / (1u64 << 53) as f64) - 0.5
        })
        .collect()
}

/// A smooth bump followed by a ramp — the planted shape.
fn pattern(m: usize) -> Vec<f64> {
    (0..m)
        .map(|i| {
            let t = i as f64 / m as f64;
            5.0 * math::exp(-((t - 0.3) * (t - 0.3)) / 0.01) + 3.0 * t
        })
        .collect()
}

/// The explicitly z-normalised Euclidean distance (two-pass statistics).
fn naive_distance(a: &[f64], b: &[f64]) -> f64 {
    let z = |w: &[f64]| {
        let mean = w.iter().sum::<f64>() / w.len() as f64;
        let sd = (w.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / w.len() as f64).sqrt();
        w.iter().map(|x| (x - mean) / sd).collect::<Vec<f64>>()
    };
    let (za, zb) = (z(a), z(b));
    za.iter().zip(&zb).map(|(x, y)| (x - y) * (x - y)).sum::<f64>().sqrt()
}

fn brute_profile(xs: &[f64], m: usize) -> Vec<(f64, usize)> {
    let l = xs.len() + 1 - m;
    (0..l)
        .map(|i| {
            (0..l)
                .filter(|&j| i.abs_diff(j) > exclusion_zone(m))
                .map(|j| (naive_distance(&xs[i..i + m], &xs[j..j + m]), j))
                .min_by(|a, b| a.0.total_cmp(&b.0))
                .unwrap_or((f64::INFINITY, 0))
        })
        .collect()
}

fn full(xs: &[f64], m: usize, seed: u64) -> MatrixProfile {
    let options = ProfileOptions {
        m,
        max_work: u64::MAX,
        seed,
    };
    matrix_profile(xs, options).unwrap()
}

#[test]
fn mass_equals_the_naive_distance_profile() {
    let series = noise(300, 1);
    let query = pattern(24);
    let profile = mass(&query, &series).unwrap();
    let shape = Shape::new(&query, &series).unwrap();
    assert_eq!(profile.len(), 300 - 24 + 1);
    for (i, &d) in profile.iter().enumerate() {
        let want = naive_distance(&query, &series[i..i + 24]);
        assert!((d - want).abs() < 1e-8, "at {i}: {d} vs {want}");
        assert!((shape.distance_at(i) - want).abs() < 1e-9, "direct at {i}");
    }
}

#[test]
fn the_matrix_profile_equals_brute_force() {
    for (n, m, seed) in [(160, 8, 3), (220, 17, 5), (90, 4, 9)] {
        let xs = noise(n, seed);
        let got = full(&xs, m, 11);
        assert!(!got.approximate);
        for (i, (want, j)) in brute_profile(&xs, m).into_iter().enumerate() {
            assert!((got.distance[i] - want).abs() < 1e-8, "n={n} m={m} at {i}");
            assert_eq!(got.neighbor[i], Some(j), "n={n} m={m} neighbour of {i}");
        }
    }
}

#[test]
fn a_complete_profile_does_not_depend_on_the_seed() {
    let xs = noise(400, 17);
    let (a, b) = (full(&xs, 12, 1), full(&xs, 12, 99));
    let bits = |p: &MatrixProfile| p.distance.iter().map(|d| d.to_bits()).collect::<Vec<_>>();
    assert_eq!(bits(&a), bits(&b));
    assert_eq!(a.neighbor, b.neighbor);
}

#[test]
fn a_planted_motif_is_the_top_motif_and_query_match() {
    let m = 40;
    let mut xs: Vec<f64> = noise(600, 23).iter().map(|v| v * 0.2).collect();
    for start in [100, 400] {
        for (k, p) in pattern(m).iter().enumerate() {
            xs[start + k] += p;
        }
    }
    let top = motifs(&full(&xs, m, 0), 1)[0];
    let pair = (top.start.min(top.neighbor.unwrap()), top.start.max(top.neighbor.unwrap()));
    assert!(pair.0.abs_diff(100) <= 2 && pair.1.abs_diff(400) <= 2, "{top:?}");

    let profile = mass(&pattern(m), &xs).unwrap();
    let mut best = super::matrix_profile::select(
        &profile,
        2,
        m,
        super::matrix_profile::Extreme::Nearest,
        &|_| None,
    );
    best.sort_unstable();
    assert!(best[0].abs_diff(100) <= 2 && best[1].abs_diff(400) <= 2, "{best:?}");
}

#[test]
fn a_planted_discord_is_the_top_discord() {
    let (m, period) = (25, 25);
    let mut xs: Vec<f64> = noise(500, 31)
        .iter()
        .enumerate()
        .map(|(i, v)| (i % period) as f64 + v * 0.05)
        .collect();
    for x in &mut xs[300..325] {
        *x = 12.0 - *x / 2.0;
    }
    let top = discords(&full(&xs, m, 0), 1)[0];
    assert!((300 - m..=325).contains(&top.start), "{top:?}");
}

#[test]
fn a_cut_profile_is_an_upper_bound_flagged_approximate() {
    let xs = noise(500, 41);
    let exact = full(&xs, 16, 0);
    let options = ProfileOptions {
        m: 16,
        max_work: 30_000,
        seed: 5,
    };
    let cut = matrix_profile(&xs, options).unwrap();
    assert!(cut.approximate);
    assert!(cut.work <= 30_000);
    for (c, e) in cut.distance.iter().zip(&exact.distance) {
        assert!(*c >= *e - 1e-9, "a cut value is never below the exact one");
    }
    let finite = cut.distance.iter().filter(|d| d.is_finite()).count();
    assert!(finite * 10 >= cut.distance.len() * 9, "PreSCRIMP reaches most subsequences");
}

#[test]
fn out_of_domain_inputs_are_refused() {
    assert!(mass(&[1.0], &[1.0, 2.0]).is_err());
    assert!(mass(&[1.0, 2.0, 3.0], &[1.0, 2.0]).is_err());
    assert!(mass(&[1.0, 2.0], &[1.0, f64::NAN, 2.0]).is_err());
    let options = ProfileOptions {
        m: 50,
        max_work: 10,
        seed: 0,
    };
    assert!(matrix_profile(&[0.0; 10], options).is_err());
}
