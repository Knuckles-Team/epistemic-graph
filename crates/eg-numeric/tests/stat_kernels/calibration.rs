//! Calibration against closed forms and planted (synthetic) calibration curves.

use crate::common::{assert_close, mean_max_abs_difference, planted_binary, planted_logits};
use eg_numeric::calibration::metrics::{brier_score, classwise_ece, log_loss, reliability, top_label_reliability};
use eg_numeric::calibration::scaling::{calibrate_rows, negative_log_likelihood};
use eg_numeric::calibration::{
    fit_isotonic, fit_isotonic_weighted, fit_scaling, BinCount, LabelledScores, ProbabilityFloor, ProbabilityMatrix,
    ScalingFamily, ScalingOptions, ScoreMatrix,
};
use eg_numeric::detkernel::math;

fn labelled(rows: &[Vec<f64>], labels: &[usize]) -> LabelledScores {
    LabelledScores::new(ScoreMatrix::from_rows(rows).unwrap(), labels.to_vec()).unwrap()
}

fn probability_rows(rows: &[Vec<f64>]) -> ProbabilityMatrix {
    ProbabilityMatrix::from_rows(rows).unwrap()
}

#[test]
fn temperature_scaling_recovers_a_planted_temperature() {
    let (rows, labels, _) = planted_logits(11, 4000, 4, 3.0, &[0.0; 4]);
    let fitted = fit_scaling(&labelled(&rows, &labels), ScalingFamily::Temperature, ScalingOptions::default()).unwrap();
    assert_close(fitted.temperature().unwrap(), 3.0, 0.2, "planted temperature 3");
    assert!(fitted.nll_after() < fitted.nll_before());
    assert_eq!(fitted.n_calibration(), 4000);
    let (calibrated_rows, calibrated_labels, _) = planted_logits(12, 4000, 4, 1.0, &[0.0; 4]);
    let identity = fit_scaling(
        &labelled(&calibrated_rows, &calibrated_labels),
        ScalingFamily::Temperature,
        ScalingOptions::default(),
    )
    .unwrap();
    assert_close(identity.temperature().unwrap(), 1.0, 0.08, "calibrated data keeps T = 1");
}

#[test]
fn vector_and_matrix_scaling_recover_planted_probabilities() {
    let shift = [1.0, -0.5, 0.0];
    let (rows, labels, _) = planted_logits(21, 6000, 3, 2.0, &shift);
    let data = labelled(&rows, &labels);
    let (test_rows, _, truth) = planted_logits(22, 1000, 3, 2.0, &shift);
    let test = ScoreMatrix::from_rows(&test_rows).unwrap();
    let options = ScalingOptions::new(20, 100, 0.0).unwrap();
    for family in [ScalingFamily::Vector, ScalingFamily::Matrix] {
        let fitted = fit_scaling(&data, family, options).unwrap();
        assert!(fitted.temperature().is_none());
        assert!(fitted.nll_after() < fitted.nll_before());
        let calibrated = calibrate_rows(&fitted, &test).unwrap();
        let error = mean_max_abs_difference(&calibrated, &truth);
        assert!(error < 0.03, "{family:?}: mean max probability error {error}");
    }
    assert_close(negative_log_likelihood(&data), fit_scaling(&data, ScalingFamily::Vector, options).unwrap().nll_before(), 0.0, "identity NLL");
    assert!(ScalingOptions::new(0, 10, 0.0).is_err() && ScalingOptions::new(1, 10, -1.0).is_err());
}

#[test]
fn isotonic_regression_pools_violators_and_ties() {
    let fit = fit_isotonic(&[1.0, 2.0, 3.0, 4.0, 5.0], &[true, false, true, false, true]).unwrap();
    assert_eq!(fit.blocks(), vec![(1.0, 2.0, 0.5), (3.0, 4.0, 0.5), (5.0, 5.0, 1.0)]);
    assert_eq!(fit.predict(0.0).unwrap(), 0.5);
    assert_eq!(fit.predict(4.5).unwrap(), 0.5);
    assert_eq!(fit.predict(9.0).unwrap(), 1.0);
    let tied = fit_isotonic(&[2.0, 1.0, 1.0], &[true, true, false]).unwrap();
    assert_eq!(tied.blocks(), vec![(1.0, 1.0, 0.5), (2.0, 2.0, 1.0)]);
    let weighted = fit_isotonic_weighted(&[1.0, 2.0], &[1.0, 0.0], &[3.0, 1.0]).unwrap();
    assert_eq!(weighted.blocks(), vec![(1.0, 2.0, 0.75)]);
    assert!(fit_isotonic_weighted(&[1.0], &[0.5], &[0.0]).is_err());
    assert!(fit.predict(f64::NAN).is_err());
}

fn square(x: f64) -> f64 {
    x * x
}

#[test]
fn isotonic_recovers_a_planted_curve_and_removes_calibration_error() {
    let (scores, outcomes) = planted_binary(31, 40_000, square);
    let fit = fit_isotonic(&scores, &outcomes).unwrap();
    for grid in [0.2, 0.4, 0.6, 0.8] {
        assert_close(fit.predict(grid).unwrap(), square(grid), 0.05, "planted s^2 curve");
    }
    let (fresh_scores, fresh_outcomes) = planted_binary(32, 40_000, square);
    let bins = BinCount::new(20).unwrap();
    let raw = reliability(&fresh_scores, &fresh_outcomes, bins).unwrap();
    assert_close(raw.expected_calibration_error, 1.0 / 6.0, 0.02, "planted ECE E|s - s^2|");
    let mapped: Vec<f64> = fresh_scores.iter().map(|&s| fit.predict(s).unwrap()).collect();
    let after = reliability(&mapped, &fresh_outcomes, bins).unwrap();
    assert!(after.expected_calibration_error < 0.02, "isotonic ECE {}", after.expected_calibration_error);
}

#[test]
fn reliability_table_matches_a_hand_computed_example() {
    let report = reliability(&[0.1, 0.1, 0.9, 0.9, 1.0], &[false, true, true, true, true], BinCount::new(10).unwrap()).unwrap();
    assert_eq!(report.n, 5);
    assert_eq!(report.bins[1].count, 2);
    assert_eq!(report.bins[9].count, 3, "p = 1 lands in the last bin");
    assert_close(report.bins[9].mean_confidence, 2.8 / 3.0, 1e-15, "last bin confidence");
    let expected_ece = 0.4 * 2.0 / 5.0 + (1.0 - 2.8 / 3.0) * 3.0 / 5.0;
    assert_close(report.expected_calibration_error, expected_ece, 1e-15, "hand ECE");
    assert_close(report.maximum_calibration_error, 0.4, 1e-15, "hand MCE");
    assert!(BinCount::new(0).is_err());
    assert!(reliability(&[1.5], &[true], BinCount::new(10).unwrap()).is_err());
}

#[test]
fn brier_log_loss_top_label_and_classwise_metrics() {
    let probabilities = probability_rows(&[vec![0.7, 0.2, 0.1], vec![0.2, 0.2, 0.6]]);
    assert_close(brier_score(&probabilities, &[0, 0]).unwrap(), (0.14 + 1.04) / 2.0, 1e-15, "brier");
    let floor = ProbabilityFloor::new(1e-12).unwrap();
    let expected_loss = -(math::ln(0.7) + math::ln(0.2)) / 2.0;
    assert_close(log_loss(&probabilities, &[0, 0], floor).unwrap(), expected_loss, 1e-15, "log loss");
    let bins = BinCount::new(10).unwrap();
    let top = top_label_reliability(&probabilities, &[0, 0], bins).unwrap();
    assert_eq!((top.bins[7].count, top.bins[6].count), (1, 1));
    assert_close(top.expected_calibration_error, (0.3 + 0.6) / 2.0, 1e-15, "top-label ECE");
    assert!(classwise_ece(&probabilities, &[0, 3], bins).is_err());
    let perfect = probability_rows(&[vec![1.0, 0.0], vec![0.0, 1.0]]);
    assert_eq!(classwise_ece(&perfect, &[0, 1], bins).unwrap(), 0.0);
    assert!(ProbabilityMatrix::from_rows(&[vec![0.7, 0.7]]).is_err());
    assert!(ProbabilityFloor::new(0.0).is_err());
}
