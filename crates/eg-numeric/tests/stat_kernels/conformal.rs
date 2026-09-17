//! Conformal prediction: exact ranks, closed-form scores, and empirical
//! coverage on synthetic data with known coverage over seeded repeats.

use crate::common::{
    assert_all_close, assert_at_least_within, assert_close, categorical, level, rng, uniform,
    uniform_in,
};
use eg_numeric::calibration::ProbabilityMatrix;
use eg_numeric::conformal::{
    acceptability_score, aps_label_scores, aps_scores, binary_conformal, lac_scores,
    mondrian_conformal, realised_coverage_interval, split_conformal, weighted_split_conformal,
    AdaptiveConformal, BinarySet, ClassThreshold, PredictionSet, RapsPenalty, Threshold,
};
use eg_numeric::detkernel::kernels::softmax;
use eg_numeric::detkernel::Level;
use eg_numeric::risk::sample_gate::coverage_standard_deviation;
use eg_numeric::risk::SampleGate;
use rand_chacha::ChaCha8Rng;

#[test]
fn split_conformal_uses_the_exact_order_statistic() {
    let scores: Vec<f64> = (1..=9).rev().map(f64::from).collect();
    let quantile = split_conformal(&scores, level(1, 10)).unwrap();
    assert_eq!(quantile.threshold(), Threshold::Finite(9.0));
    assert_eq!(
        (quantile.n_calibration(), quantile.alpha()),
        (9, level(1, 10))
    );
    assert_eq!(quantile.interval(1.0), (-8.0, 10.0));
    let trivial = split_conformal(&scores[..8], level(1, 10)).unwrap();
    assert_eq!(trivial.threshold(), Threshold::Infinite);
    assert_eq!(trivial.interval(0.0), (f64::NEG_INFINITY, f64::INFINITY));
    assert_eq!(
        split_conformal(&scores, level(1, 2)).unwrap().threshold(),
        Threshold::Finite(5.0)
    );
    assert!(
        split_conformal(&[], level(1, 10)).is_err()
            && split_conformal(&[f64::NAN], level(1, 10)).is_err()
    );
    let (lo, hi) = realised_coverage_interval(level(1, 10), 99, level(1, 20))
        .unwrap()
        .unwrap();
    assert_close(lo, 0.8344310242579897, 1e-10, "coverage interval lower");
    assert_close(hi, 0.9504891277913371, 1e-10, "coverage interval upper");
    assert_eq!(
        realised_coverage_interval(level(1, 10), 5, level(1, 20)).unwrap(),
        None
    );
    assert!(!Threshold::Empty.admits(f64::NEG_INFINITY) && Threshold::Infinite.admits(1e300));
}

fn heteroscedastic_scores(generator: &mut ChaCha8Rng, n: usize) -> Vec<f64> {
    (0..n)
        .map(|_| {
            let x = uniform(generator);
            (uniform_in(generator, -1.0, 1.0) * (1.0 + 3.0 * x)).abs()
        })
        .collect()
}

#[test]
fn split_conformal_regression_coverage_is_nominal_over_seeded_repeats() {
    let alpha = level(1, 10);
    let repeats = 100;
    let mut total = 0.0;
    for seed in 0..repeats {
        let mut generator = rng(2000 + seed as u64);
        let quantile =
            split_conformal(&heteroscedastic_scores(&mut generator, 199), alpha).unwrap();
        let test = heteroscedastic_scores(&mut generator, 1000);
        let covered = test
            .iter()
            .filter(|&&s| quantile.threshold().admits(s))
            .count();
        total += covered as f64 / 1000.0;
    }
    let mean = total / repeats as f64;
    let sd = coverage_standard_deviation(alpha, 199) + 0.01;
    assert_at_least_within(mean, 0.9, sd, repeats, "split conformal coverage");
    assert!(
        mean <= 0.9 + 1.0 / 200.0 + 4.0 * sd / 10.0,
        "coverage {mean} is not tight"
    );
}

#[test]
fn aps_raps_and_lac_scores_match_closed_forms() {
    let aps = aps_scores(&[0.5, 0.3, 0.2], 0.5, RapsPenalty::none()).unwrap();
    assert_all_close(&aps, &[0.25, 0.65, 0.9], 1e-15, "randomised aps");
    let raps = aps_scores(&[0.5, 0.3, 0.2], 1.0, RapsPenalty::new(0.1, 1).unwrap()).unwrap();
    assert_all_close(&raps, &[0.5, 0.9, 1.2], 1e-15, "raps");
    let tied = aps_scores(&[0.4, 0.4, 0.2], 1.0, RapsPenalty::none()).unwrap();
    assert_all_close(
        &tied,
        &[0.4, 0.8, 1.0],
        1e-15,
        "ties rank the lower class first",
    );
    assert_eq!(lac_scores(&[0.75, 0.25]).unwrap(), vec![0.25, 0.75]);
    let set = PredictionSet::from_scores(&[0.5, 0.9, 1.2], Threshold::Finite(0.9));
    assert_eq!(set.options(), &[0, 1]);
    assert!(set.contains(1) && !set.contains(2) && set.meets(&[2, 1]) && !set.meets(&[2]));
    assert_eq!((set.len(), set.is_empty()), (2, false));
    assert_eq!(acceptability_score(&[0.7, 0.2, 0.4], &[0, 2]).unwrap(), 0.4);
    assert!(
        acceptability_score(&[0.7], &[]).is_err() && acceptability_score(&[0.7], &[1]).is_err()
    );
    assert!(
        aps_scores(&[0.5, 0.5], 1.5, RapsPenalty::none()).is_err()
            && RapsPenalty::new(-1.0, 0).is_err()
    );
}

struct Classified {
    probabilities: Vec<Vec<f64>>,
    labels: Vec<usize>,
    u: Vec<f64>,
}

/// Calibrated planted classifier: `p = softmax(z)`, labels drawn from `p`.
fn classified(generator: &mut ChaCha8Rng, n: usize, classes: usize) -> Classified {
    let mut data = Classified {
        probabilities: Vec::new(),
        labels: Vec::new(),
        u: Vec::new(),
    };
    for _ in 0..n {
        let z: Vec<f64> = (0..classes)
            .map(|_| uniform_in(generator, -3.0, 3.0))
            .collect();
        let p = softmax(&z).unwrap();
        data.labels.push(categorical(generator, &p));
        data.u.push(uniform(generator));
        data.probabilities.push(p);
    }
    data
}

fn aps_coverage(seed: u64, penalty: RapsPenalty, randomised: bool) -> f64 {
    let mut generator = rng(seed);
    let calibration = classified(&mut generator, 500, 5);
    let u = if randomised {
        calibration.u.clone()
    } else {
        vec![1.0; 500]
    };
    let matrix = ProbabilityMatrix::from_rows(&calibration.probabilities).unwrap();
    let scores = aps_label_scores(&matrix, &calibration.labels, &u, penalty).unwrap();
    let threshold = split_conformal(&scores, level(1, 10)).unwrap().threshold();
    let test = classified(&mut generator, 500, 5);
    let covered = (0..500)
        .filter(|&i| {
            let ui = if randomised { test.u[i] } else { 1.0 };
            let row = aps_scores(&test.probabilities[i], ui, penalty).unwrap();
            PredictionSet::from_scores(&row, threshold).contains(test.labels[i])
        })
        .count();
    covered as f64 / 500.0
}

#[test]
fn aps_and_raps_sets_cover_planted_labels() {
    let repeats = 50;
    let sd = coverage_standard_deviation(level(1, 10), 500) + 0.014;
    let cases = [
        (RapsPenalty::none(), true),
        (RapsPenalty::none(), false),
        (RapsPenalty::new(0.05, 2).unwrap(), true),
    ];
    for (penalty, randomised) in cases {
        let mean = (0..repeats)
            .map(|seed| aps_coverage(3000 + seed, penalty, randomised))
            .sum::<f64>()
            / repeats as f64;
        assert_at_least_within(mean, 0.9, sd, repeats as usize, "APS coverage");
    }
}

fn acceptability_coverage(seed: u64) -> f64 {
    let mut generator = rng(seed);
    let mut draw = |n: usize| -> Vec<(Vec<f64>, Vec<usize>)> {
        classified(&mut generator, n, 6)
            .probabilities
            .into_iter()
            .map(|p| {
                let mut acceptable = vec![categorical(&mut generator, &p)];
                if uniform(&mut generator) < 0.5 {
                    acceptable.push(categorical(&mut generator, &p));
                }
                (lac_scores(&p).unwrap(), acceptable)
            })
            .collect()
    };
    let calibration: Vec<f64> = draw(300)
        .iter()
        .map(|(s, a)| acceptability_score(s, a).unwrap())
        .collect();
    let threshold = split_conformal(&calibration, level(1, 5))
        .unwrap()
        .threshold();
    let test = draw(500);
    let met = test
        .iter()
        .filter(|(s, a)| PredictionSet::from_scores(s, threshold).meets(a))
        .count();
    met as f64 / 500.0
}

#[test]
fn acceptability_sets_meet_the_acceptable_options_at_nominal_rate() {
    let repeats = 50;
    let mean = (0..repeats)
        .map(|seed| acceptability_coverage(4000 + seed))
        .sum::<f64>()
        / repeats as f64;
    let sd = coverage_standard_deviation(level(1, 5), 300) + 0.018;
    assert_at_least_within(mean, 0.8, sd, repeats as usize, "acceptability coverage");
}

#[test]
fn mondrian_covers_every_class_including_rare_ones() {
    let alpha = level(1, 10);
    let gate = SampleGate::new(20).unwrap();
    let (mut hits, mut totals) = ([0usize; 3], [0usize; 3]);
    for seed in 0..40u64 {
        let mut generator = rng(5000 + seed);
        let mut draw = |n: usize| -> (Vec<Vec<f64>>, Vec<usize>) {
            (0..n)
                .map(|_| {
                    let label = categorical(&mut generator, &[0.7, 0.2, 0.1]);
                    let row: Vec<f64> = (0..3)
                        .map(|k| uniform(&mut generator) + if k == label { 0.0 } else { 0.3 })
                        .collect();
                    (row, label)
                })
                .unzip()
        };
        let (rows, labels) = draw(600);
        let label_scores: Vec<f64> = rows.iter().zip(&labels).map(|(r, &y)| r[y]).collect();
        let mondrian = mondrian_conformal(&label_scores, &labels, 3, alpha, gate).unwrap();
        assert!(mondrian.fully_calibrated());
        let (test_rows, test_labels) = draw(600);
        for (row, &label) in test_rows.iter().zip(&test_labels) {
            totals[label] += 1;
            hits[label] += usize::from(mondrian.prediction_set(row).unwrap().contains(label));
        }
    }
    for class in 0..3 {
        let rate = hits[class] as f64 / totals[class] as f64;
        let sd = coverage_standard_deviation(alpha, 60) + 0.02;
        assert_at_least_within(rate, 0.9, sd, 40, "per-class coverage");
    }
}

#[test]
fn mondrian_below_minimum_admits_the_class_without_a_claim() {
    let scores = [0.1, 0.2, 0.3, 0.9];
    let labels = [0, 0, 0, 1];
    let mondrian = mondrian_conformal(
        &scores,
        &labels,
        2,
        level(1, 2),
        SampleGate::new(2).unwrap(),
    )
    .unwrap();
    assert!(!mondrian.fully_calibrated());
    assert_eq!(
        mondrian.thresholds()[1],
        ClassThreshold::BelowMinimum { n: 1, n_min: 2 }
    );
    assert_eq!(
        mondrian.prediction_set(&[0.15, 100.0]).unwrap().options(),
        &[0, 1]
    );
    assert_eq!(
        mondrian.prediction_set(&[0.35, 100.0]).unwrap().options(),
        &[1]
    );
    assert!(mondrian.prediction_set(&[0.1]).is_err());
    assert!(mondrian_conformal(
        &scores,
        &[0, 0, 0, 2],
        2,
        level(1, 2),
        SampleGate::new(1).unwrap()
    )
    .is_err());
}

#[test]
fn binary_conformal_sets_follow_per_label_thresholds() {
    let scores = [0.9, 0.8, 0.7, 0.6, 0.1, 0.2, 0.3, 0.4];
    let outcomes = [true, true, true, true, false, false, false, false];
    let binary =
        binary_conformal(&scores, &outcomes, level(1, 5), SampleGate::new(4).unwrap()).unwrap();
    let cases = [
        (0.95, BinarySet::Positive),
        (0.5, BinarySet::Empty),
        (0.6, BinarySet::Positive),
        (0.4, BinarySet::Negative),
    ];
    for (p, expected) in cases {
        assert_eq!(binary.predict(p).unwrap(), expected, "p = {p}");
    }
    let wide = binary_conformal(
        &scores,
        &outcomes,
        level(1, 10),
        SampleGate::new(4).unwrap(),
    )
    .unwrap();
    assert_eq!(wide.predict(0.5).unwrap(), BinarySet::Both);
    let gated =
        binary_conformal(&scores, &outcomes, level(1, 5), SampleGate::new(5).unwrap()).unwrap();
    assert!(!gated.mondrian().fully_calibrated());
    assert_eq!(gated.predict(0.5).unwrap(), BinarySet::Both);
    assert!(binary.predict(1.5).is_err());
}

#[test]
fn weighted_conformal_reduces_to_split_and_follows_the_weights() {
    let mut generator = rng(6000);
    let scores: Vec<f64> = (0..137).map(|_| uniform(&mut generator)).collect();
    let alpha = level(3, 20);
    let equal = weighted_split_conformal(&scores, &vec![1.0; 137], 1.0, alpha).unwrap();
    assert_eq!(
        equal.threshold(),
        split_conformal(&scores, alpha).unwrap().threshold()
    );
    let heavy_tail: Vec<f64> = scores
        .iter()
        .map(|&s| if s > 0.5 { 10.0 } else { 0.1 })
        .collect();
    let shifted = weighted_split_conformal(&scores, &heavy_tail, 1.0, alpha).unwrap();
    let (Threshold::Finite(plain), Threshold::Finite(weighted)) =
        (equal.threshold(), shifted.threshold())
    else {
        panic!("finite thresholds expected");
    };
    assert!(
        weighted > plain,
        "weights on large scores raise the threshold"
    );
    assert_eq!(
        weighted_split_conformal(&[0.3], &[1.0], 100.0, alpha)
            .unwrap()
            .threshold(),
        Threshold::Infinite
    );
    assert!(weighted_split_conformal(&[0.3], &[-1.0], 1.0, alpha).is_err());
    assert!(weighted_split_conformal(&[0.3], &[0.0], 0.0, alpha).is_err());
}

fn drifting_score(generator: &mut ChaCha8Rng, t: usize) -> f64 {
    uniform(generator) * (1.0 + t as f64 / 400.0)
}

#[test]
fn adaptive_conformal_meets_its_long_run_bound_under_drift() {
    let alpha: Level = level(1, 10);
    let gamma = 0.05;
    let mut generator = rng(7000);
    let mut window: Vec<f64> = (0..200)
        .map(|_| drifting_score(&mut generator, 0))
        .collect();
    let static_threshold = split_conformal(&window, alpha).unwrap().threshold();
    let mut adaptive = AdaptiveConformal::new(alpha, gamma).unwrap();
    let mut static_misses = 0u64;
    for t in 0..3000 {
        let score = drifting_score(&mut generator, t);
        adaptive.observe(adaptive.threshold(&window).unwrap().admits(score));
        static_misses += u64::from(!static_threshold.admits(score));
        let report = adaptive.report();
        let gap = (report.empirical_miscoverage - 0.1).abs();
        assert!(
            gap <= report.miscoverage_bound + 1e-12,
            "t = {t}: gap {gap} above bound"
        );
        window.remove(0);
        window.push(score);
    }
    let report = adaptive.report();
    assert_eq!(report.steps, 3000);
    assert!(report.miscoverage_bound < 0.01);
    assert!(
        static_misses as f64 / 3000.0 > 0.5,
        "the static threshold should fail under drift"
    );
    assert!(AdaptiveConformal::new(alpha, 0.0).is_err());
    assert_eq!(
        AdaptiveConformal::new(alpha, 1.0)
            .unwrap()
            .report()
            .miscoverage_bound,
        f64::INFINITY
    );
}
