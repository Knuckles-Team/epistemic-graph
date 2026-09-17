//! Risk control against published reference values, exact coverage and planted
//! (synthetic) error curves.

use crate::common::{assert_close, level, rng, uniform};
use eg_numeric::detkernel::math;
use eg_numeric::risk::beta::{beta_interval, ln_beta};
use eg_numeric::risk::pooling::fit_concentration;
use eg_numeric::risk::sample_gate::coverage_standard_deviation;
use eg_numeric::risk::selective::ThresholdTest;
use eg_numeric::risk::{
    beta_quantile, binomial_cdf, calibrate_selective_risk, clopper_pearson, pool_hierarchy,
    regularized_incomplete_beta, BetaDistribution, BinomialCounts, ConcentrationBounds,
    GroupCounts, IntervalSide, PoolTree, RiskTarget, SampleAssessment, SampleGate, TestOutcome,
};
use std::collections::BTreeMap;

fn two_sided(k: u64, n: u64) -> (f64, f64) {
    let interval = clopper_pearson(
        BinomialCounts::new(k, n).unwrap(),
        level(1, 20),
        IntervalSide::TwoSided,
    )
    .unwrap();
    (interval.lower, interval.upper)
}

/// Clopper–Pearson 95% intervals; the first three rows are the published R
/// `binom.test` values, all rows agree with SciPy `beta.ppf`.
const CLOPPER_PEARSON_95: [(u64, u64, f64, f64); 8] = [
    (5, 10, 0.18708602844739855, 0.8129139715526015),
    (1, 10, 0.0025285785444617848, 0.4450161170281954),
    (50, 100, 0.39832112950330095, 0.601678870496699),
    (0, 10, 0.0, 0.3084971078187607),
    (10, 10, 0.6915028921812392, 1.0),
    (3, 1000, 0.0006190999316495713, 0.008742023238478303),
    (999, 1000, 0.9944410757201734, 0.9999746825125088),
    (17, 40, 0.27042903126886286, 0.5910994195730367),
];

#[test]
fn clopper_pearson_matches_reference_tables_and_closed_forms() {
    for (k, n, lower, upper) in CLOPPER_PEARSON_95 {
        let (lo, hi) = two_sided(k, n);
        assert_close(lo, lower, 1e-10, "CP lower");
        assert_close(hi, upper, 1e-10, "CP upper");
    }
    for n in [1u64, 7, 30, 500] {
        let closed_upper = 1.0 - math::pow(0.025, 1.0 / n as f64);
        assert_close(two_sided(0, n).1, closed_upper, 1e-12, "k = 0 closed form");
        assert_close(
            two_sided(n, n).0,
            1.0 - closed_upper,
            1e-12,
            "k = n closed form",
        );
    }
    for (k, n, upper) in [
        (0u64, 30u64, 0.09503385285530411),
        (2, 50, 0.12061415542204412),
        (10, 200, 0.08333515106637196),
    ] {
        let one_sided = clopper_pearson(
            BinomialCounts::new(k, n).unwrap(),
            level(1, 20),
            IntervalSide::Upper,
        )
        .unwrap();
        assert_eq!(one_sided.lower, 0.0);
        assert_close(one_sided.upper, upper, 1e-10, "CP one-sided upper");
    }
    // One-sided bounds use the FULL delta as the tail (no halving), so this
    // must be `level(1, 40)` (tail 0.025) to match `two_sided`'s lower tail
    // (`level(1, 20)` halved to 0.025), not `level(1, 10)` (tail 0.1).
    let lower_only = clopper_pearson(
        BinomialCounts::new(5, 10).unwrap(),
        level(1, 40),
        IntervalSide::Lower,
    )
    .unwrap();
    assert_eq!(
        (lower_only.lower, lower_only.upper),
        (two_sided(5, 10).0, 1.0)
    );
    assert!(BinomialCounts::new(3, 2).is_err() && BinomialCounts::new(0, 0).is_err());
}

#[test]
fn incomplete_beta_quantile_and_binomial_cdf_match_references() {
    let incomplete = [
        (2.0, 3.0, 0.4, 0.5248),
        (0.5, 0.5, 0.25, 1.0 / 3.0),
        (10.0, 20.0, 0.3, 0.3640040810719437),
        (500.0, 700.0, 0.42, 0.5937664454826134),
        (1.0, 1.0, 0.73, 0.73),
        (3.5, 1.25, 0.999, 0.9992177663912517),
        (0.1, 10.0, 1e-5, 0.41655822619209415),
    ];
    for (a, b, x, expected) in incomplete {
        assert_close(
            regularized_incomplete_beta(x, a, b).unwrap(),
            expected,
            1e-12,
            "I_x(a, b)",
        );
    }
    let quantiles = [
        (2.0, 3.0, 0.5, 0.3857275681323895),
        (10.0, 20.0, 0.025, 0.17938364923511183),
        (0.5, 0.5, 0.9, 0.9755282581475768),
        (500.0, 700.0, 0.975, 0.4446754406462241),
        (1.0, 50.0, 1e-6, 2.000000980000647e-8),
    ];
    for (a, b, p, expected) in quantiles {
        assert_close(
            beta_quantile(p, a, b).unwrap(),
            expected,
            1e-10 * expected,
            "beta quantile",
        );
    }
    for (k, n, p, expected) in [
        (3u64, 20u64, 0.1, 0.8670466765656649),
        (50, 1000, 0.07, 0.005929633320516831),
        (0, 5, 0.3, 0.16807),
        (12, 40, 0.5, 0.008294501687487355),
    ] {
        assert_close(
            binomial_cdf(k, n, p).unwrap(),
            expected,
            1e-12,
            "binomial cdf",
        );
    }
    assert_eq!(binomial_cdf(5, 5, 0.3).unwrap(), 1.0);
    assert_close(ln_beta(2.0, 3.0), math::ln(1.0 / 12.0), 1e-14, "ln B(2, 3)");
    assert!(
        regularized_incomplete_beta(0.5, 0.0, 1.0).is_err()
            && beta_quantile(1.5, 1.0, 1.0).is_err()
    );
    let (lo, hi) = beta_interval(90.0, 10.0, level(1, 20)).unwrap();
    assert!(lo < 0.9 && 0.9 < hi);
}

fn exact_coverage(n: u64, p: f64) -> f64 {
    let mut coverage = 0.0;
    let mut below = 0.0;
    for k in 0..=n {
        let cdf = binomial_cdf(k, n, p).unwrap();
        let (lo, hi) = two_sided(k, n);
        if lo <= p && p <= hi {
            coverage += cdf - below;
        }
        below = cdf;
    }
    coverage
}

#[test]
fn clopper_pearson_exact_coverage_is_at_least_nominal() {
    for (n, p) in [(10u64, 0.05), (25, 0.3), (40, 0.5), (60, 0.9), (120, 0.02)] {
        let coverage = exact_coverage(n, p);
        assert!(
            coverage >= 0.95 - 1e-12,
            "n = {n}, p = {p}: exact coverage {coverage}"
        );
    }
}

fn target(epsilon: (u64, u64), delta: (u64, u64), n_min: u64) -> RiskTarget {
    RiskTarget {
        epsilon: level(epsilon.0, epsilon.1),
        delta: level(delta.0, delta.1),
        gate: SampleGate::new(n_min).unwrap(),
    }
}

fn outcomes(tests: &[ThresholdTest]) -> Vec<TestOutcome> {
    tests.iter().map(|t| t.outcome).collect()
}

#[test]
fn selective_risk_follows_the_fixed_sequence() {
    let scores: Vec<f64> = (0..100).map(|i| f64::from(i) / 100.0).collect();
    let wrong: Vec<bool> = (0..100).map(|i| i < 50 && i % 2 == 0).collect();
    let thresholds = [0.995, 0.7, 0.5, 0.2, 0.0];
    let certificate =
        calibrate_selective_risk(&scores, &wrong, &thresholds, target((1, 10), (1, 10), 5))
            .unwrap();
    use TestOutcome::{Certified, NotReached, NotRejected, SkippedBelowMinimum};
    assert_eq!(
        outcomes(&certificate.tests),
        vec![
            SkippedBelowMinimum,
            Certified,
            Certified,
            NotRejected,
            NotReached
        ]
    );
    let certified = certificate.certified.unwrap();
    assert_eq!(
        (certified.threshold, certified.acted, certified.wrong),
        (0.5, 50, 0)
    );
    assert_close(
        certified.risk_upper_bound,
        1.0 - math::pow(0.1, 1.0 / 50.0),
        1e-12,
        "CP bound at k = 0",
    );
    assert_eq!(
        (certificate.tests[3].acted, certificate.tests[3].wrong),
        (80, 15)
    );
    assert!(
        calibrate_selective_risk(&scores, &wrong, &[0.5, 0.5], target((1, 10), (1, 10), 5))
            .is_err()
    );
    let none = calibrate_selective_risk(&scores, &[true; 100], &[0.5], target((1, 10), (1, 10), 5))
        .unwrap();
    assert!(none.certified.is_none());
}

fn planted_selective_run(seed: u64) -> Option<f64> {
    let mut generator = rng(seed);
    let (scores, wrong): (Vec<f64>, Vec<bool>) = (0..2000)
        .map(|_| {
            let s = uniform(&mut generator);
            (s, uniform(&mut generator) < 1.0 - s)
        })
        .unzip();
    // Fixed-sequence testing stops at the first threshold that fails to
    // reject, so every threshold ABOVE the one of interest is one more
    // chance for a spurious (Type II) non-rejection to halt the sequence
    // early. Ten thresholds 0.05 apart give the procedure only two
    // conservative "gatekeepers" (0.95, 0.90) to clear before reaching the
    // useful zone, instead of ~19 finer-grained ones (a 0.01 step from 0.99
    // reproducibly certified a useful threshold in only 148/200 runs).
    let thresholds: Vec<f64> = (0..10).map(|i| 0.95 - f64::from(i) * 0.05).collect();
    let certificate =
        calibrate_selective_risk(&scores, &wrong, &thresholds, target((1, 10), (1, 10), 30))
            .unwrap();
    certificate.certified.map(|c| c.threshold)
}

#[test]
fn selective_risk_controls_planted_risk_with_power() {
    // P(wrong | s) = 1 - s with s ~ U(0, 1): the true risk of acting at lambda
    // is (1 - lambda) / 2, so epsilon = 0.1 is met exactly for lambda >= 0.8.
    let repeats = 200u64;
    let mut violations = 0u64;
    let mut useful = 0u64;
    for seed in 0..repeats {
        let certified = planted_selective_run(1000 + seed);
        violations += u64::from(certified.is_some_and(|l| (1.0 - l) / 2.0 > 0.1 + 1e-12));
        useful += u64::from(certified.is_some_and(|l| l <= 0.9));
    }
    let allowed = 0.1 * repeats as f64 + 4.0 * (0.09 * repeats as f64).sqrt();
    assert!(
        (violations as f64) <= allowed,
        "{violations} violations of epsilon in {repeats} runs"
    );
    // A useful (<= 0.9) threshold needs both gatekeepers (0.95 at true risk
    // 0.025, 0.90 at true risk 0.05) to reject the "risk >= 0.1" null; each
    // has ample power at n ~ 100-200 acted items, so this is a wide,
    // 4-standard-deviation margin below a conservatively assumed 75% true
    // rate, not the actual expected rate.
    let useful_target = 0.75_f64;
    let useful_min = useful_target * repeats as f64
        - 4.0 * (useful_target * (1.0 - useful_target) * repeats as f64).sqrt();
    assert!(
        useful as f64 >= useful_min,
        "certified a useful threshold in only {useful} of {repeats} runs (need >= {useful_min})"
    );
}

#[test]
fn sample_gates_assess_and_refuse() {
    let gate = SampleGate::new(30).unwrap();
    assert_eq!(
        gate.assess(29),
        SampleAssessment::Insufficient { n: 29, n_min: 30 }
    );
    assert_eq!(gate.assess(30), SampleAssessment::Sufficient { n: 30 });
    assert!(gate.require(12, "class a").is_err() && gate.require(31, "class a").is_ok());
    assert_eq!(
        SampleGate::for_conformal(level(1, 10), 5).unwrap().n_min(),
        9
    );
    assert_eq!(
        SampleGate::for_conformal(level(1, 10), 50).unwrap().n_min(),
        50
    );
    assert!(SampleGate::new(0).is_err());
    let counts: BTreeMap<&str, u64> = [("b", 40), ("a", 3)].into_iter().collect();
    let assessed: Vec<_> = gate.assess_classes(&counts).into_iter().collect();
    assert_eq!(
        assessed[0],
        ("a", SampleAssessment::Insufficient { n: 3, n_min: 30 })
    );
    assert_close(
        coverage_standard_deviation(level(1, 10), 100),
        (0.09f64 / 102.0).sqrt(),
        1e-15,
        "coverage sd",
    );
}

fn counts(successes: u64, trials: u64) -> GroupCounts {
    GroupCounts::new(successes, trials).unwrap()
}

fn leaf_tree(groups: &[(&str, u64, u64)]) -> PoolTree {
    PoolTree::Branch(
        groups
            .iter()
            .map(|&(name, s, t)| (name.to_string(), PoolTree::Leaf(counts(s, t))))
            .collect(),
    )
}

#[test]
fn pooling_closed_forms_and_structure() {
    let prior = BetaDistribution::new(2.0, 3.0).unwrap();
    let posterior = prior.update(counts(3, 5));
    assert_eq!(
        (posterior.alpha(), posterior.beta(), posterior.mean()),
        (5.0, 5.0, 0.5)
    );
    assert_eq!(posterior.concentration(), 10.0);
    let bounds = ConcentrationBounds::new(1.0, 1000.0).unwrap();
    assert_eq!(
        fit_concentration(&[counts(5, 10), counts(50, 100)], 0.5, bounds),
        1000.0,
        "no excess spread"
    );
    assert_eq!(
        fit_concentration(&[counts(5, 10)], 0.5, bounds),
        1000.0,
        "one sibling"
    );
    assert_eq!(
        fit_concentration(&[counts(0, 50), counts(50, 50)], 0.5, bounds),
        1.0,
        "maximal spread"
    );
    let tree = PoolTree::Branch(
        [("kind".to_string(), leaf_tree(&[("z", 9, 10), ("a", 1, 10)]))]
            .into_iter()
            .collect(),
    );
    let pooled = pool_hierarchy(&tree, BetaDistribution::new(1.0, 1.0).unwrap(), bounds).unwrap();
    assert_eq!(pooled.counts, counts(10, 20));
    let kind = &pooled.children["kind"];
    assert_eq!(kind.children.keys().collect::<Vec<_>>(), vec!["a", "z"]);
    assert!(kind.children["a"].posterior.mean() > 0.1 && kind.children["z"].posterior.mean() < 0.9);
    assert!(BetaDistribution::new(0.0, 1.0).is_err() && GroupCounts::new(2, 1).is_err());
    assert!(ConcentrationBounds::new(5.0, 1.0).is_err());
}

/// Squared errors (pooled, raw) of option rates for one planted hierarchy:
/// three classes with means 0.2 / 0.5 / 0.8, eight options each with true rate
/// `Beta(20 m, 20 (1 - m))` and 4..=15 trials.
fn planted_hierarchy_errors(seed: u64) -> (f64, f64) {
    let mut generator = rng(seed);
    let mut classes = BTreeMap::new();
    let mut truth = Vec::new();
    for (class, class_mean) in [("c0", 0.2), ("c1", 0.5), ("c2", 0.8)] {
        let mut options = BTreeMap::new();
        for option in 0..8 {
            let rate = beta_quantile(
                uniform(&mut generator),
                20.0 * class_mean,
                20.0 * (1.0 - class_mean),
            )
            .unwrap();
            let trials = 4 + (uniform(&mut generator) * 12.0) as u64;
            let successes = (0..trials)
                .filter(|_| uniform(&mut generator) < rate)
                .count() as u64;
            let name = format!("o{option}");
            truth.push((class, name.clone(), rate, successes as f64 / trials as f64));
            options.insert(name, PoolTree::Leaf(counts(successes, trials)));
        }
        classes.insert(class.to_string(), PoolTree::Branch(options));
    }
    let bounds = ConcentrationBounds::new(0.5, 500.0).unwrap();
    let pooled = pool_hierarchy(
        &PoolTree::Branch(classes),
        BetaDistribution::new(1.0, 1.0).unwrap(),
        bounds,
    )
    .unwrap();
    truth.iter().fold(
        (0.0, 0.0),
        |(pooled_error, raw_error), (class, option, rate, raw)| {
            let estimate = pooled.children[*class].children[option].posterior.mean();
            (
                pooled_error + (estimate - rate) * (estimate - rate),
                raw_error + (raw - rate) * (raw - rate),
            )
        },
    )
}

#[test]
fn pooling_beats_raw_rates_on_planted_hierarchies() {
    let (pooled, raw) = (0..20)
        .map(|seed| planted_hierarchy_errors(500 + seed))
        .fold((0.0, 0.0), |acc, e| (acc.0 + e.0, acc.1 + e.1));
    assert!(pooled < 0.7 * raw, "pooled {pooled} vs raw {raw}");
}
