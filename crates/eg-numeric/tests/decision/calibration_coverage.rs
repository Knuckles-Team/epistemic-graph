//! Variable-option calibration acceptance; native execution is still pending.
//! These synthetic fixtures exercise the real fitted resident scorer and evaluator.

use eg_numeric::calibration::metrics::{reliability, BinCount};
use eg_numeric::decision::admission::{admit, Regime};
use eg_numeric::decision::evaluate::{evaluate, EvalSpec, ECE_BINS};
use eg_numeric::decision::features::FeatureMatrix;
use eg_numeric::decision::head_eval::{
    act_rule, calibration_statement, read_head, top_index, HeadReading,
};
use eg_numeric::decision::scorer::fixed::{to_f64, ONE};
use eg_types::decision::statistical::dataset::{ItemLabel, LabelSource, LabelledDataset};
use eg_types::decision::statistical::head::HeadKind;
use eg_types::decision::statistical::CalibrationMethod;

use super::common::{dataset, gold_item_for, statistical, FEATURES};
use super::fit_eval::{fitted, rules};

const SIZES: [usize; 4] = [2, 3, 5, 8];
const PER_SIZE: usize = 200;

fn variable_dataset(sizes: &[usize], seed: usize) -> LabelledDataset {
    dataset(
        sizes
            .iter()
            .flat_map(|&size| {
                (0..PER_SIZE).map(move |i| {
                    let mut item = gold_item_for(i, seed, LabelSource::SyntheticConstruction, size);
                    item.item_id = format!("size-{size}-{}", item.item_id);
                    item
                })
            })
            .collect(),
    )
}

// spec: EG-DECISION-ENGINE-R084.1
#[test]
fn measures_ece_and_empirical_coverage_across_option_sets_of_varying_size() {
    let training = variable_dataset(&SIZES, 0);
    // One fit, one pooled conformal threshold, one temperature for every size.
    let head = fitted(HeadKind::OptionAttention, &training);
    let calibration = head.calibration.as_ref().expect("calibrated scorer");
    assert!(calibration.synthetic);
    assert_eq!(
        calibration_statement(calibration).method,
        CalibrationMethod::Conformal
    );
    let stat = statistical();
    let spec = EvalSpec {
        regime: Regime::FullLabel,
        statistical: &stat,
        estimators: &[],
        head_digest: "sha256:variable-option-scorer",
        policy_digest: "sha256:policy",
    };
    for size in SIZES {
        let holdout = variable_dataset(&[size], 10_001);
        assert!(holdout.items.iter().all(|item| {
            item.candidate_ids.len() == size && item.features.len() == size * FEATURES.len()
        }));
        assert!(holdout.items.iter().all(|item| {
            training
                .items
                .iter()
                .all(|trained| trained.item_id != item.item_id)
        }));
        let admitted = admit(&holdout, &rules(Regime::FullLabel, &[]));
        assert_eq!(admitted.items.len(), PER_SIZE);
        let receipt = evaluate(&head, &holdout, &admitted.items, admitted.exclusions, &spec)
            .expect("evaluates variable-size options");
        assert_eq!(
            receipt.calibration.as_ref().expect("calibration").method,
            CalibrationMethod::Conformal
        );
        let metrics = receipt.metrics.expect("ECE and coverage metrics");
        assert_eq!(metrics.n_items, PER_SIZE as u64);

        // Independently tally the public serving path, then compare its actual
        // probabilities and prediction sets to the evaluator's reported metrics.
        let mut confidence = Vec::new();
        let mut correct = Vec::new();
        let mut covered = 0_u64;
        for item in &holdout.items {
            let matrix = FeatureMatrix {
                candidate_ids: item.candidate_ids.as_slice().to_vec(),
                feature_names: FEATURES.iter().map(|s| s.to_string()).collect(),
                values: item.features.as_slice().to_vec(),
            };
            let HeadReading::InDistribution(reading) = read_head(&head, &matrix).expect("served")
            else {
                panic!("size {size} unexpectedly out of distribution");
            };
            let probabilities = reading.probabilities.expect("resident probabilities");
            assert_eq!(probabilities.len(), size);
            let ItemLabel::Gold { acceptable, .. } = &item.label else {
                unreachable!()
            };
            let top = top_index(&probabilities).expect("nonempty options");
            confidence.push(probabilities[top]);
            correct.push(
                acceptable
                    .as_slice()
                    .contains(&item.candidate_ids.as_slice()[top]),
            );
            let rule = act_rule(&probabilities, calibration);
            covered += u64::from(rule.prediction_set.iter().any(|&i| {
                acceptable
                    .as_slice()
                    .contains(&item.candidate_ids.as_slice()[i])
            }));
        }
        let served_ece = reliability(&confidence, &correct, BinCount::new(ECE_BINS).unwrap())
            .expect("served reliability")
            .expected_calibration_error;
        assert!(
            (to_f64(metrics.expected_calibration_error.value) - served_ece).abs()
                <= 1.0 / ONE as f64
        );
        assert_eq!(metrics.covered, covered, "size {size}: served coverage");
        let (num, den) = (
            calibration.alpha.numerator(),
            calibration.alpha.denominator(),
        );
        assert!(
            covered * den >= metrics.n_items * (den - num),
            "size {size}: coverage {covered}/{}",
            metrics.n_items
        );
        assert!(!receipt.failed_gates.iter().any(|gate| gate == "coverage"));
        eprintln!(
            "options={size} ece={served_ece} empirical_coverage={covered}/{}",
            metrics.n_items
        );

        // A broken conformal set must be detected, even with identical scoring.
        let mut broken = head.clone();
        broken.calibration.as_mut().unwrap().set_threshold.value = -ONE;
        let admitted = admit(&holdout, &rules(Regime::FullLabel, &[]));
        let failed = evaluate(
            &broken,
            &holdout,
            &admitted.items,
            admitted.exclusions,
            &spec,
        )
        .unwrap();
        assert!(!failed.passed);
        assert!(failed.failed_gates.iter().any(|gate| gate == "coverage"));
        let failed_metrics = failed.metrics.unwrap();
        assert_eq!(failed_metrics.covered, 0);
        assert_eq!(
            failed_metrics.expected_calibration_error,
            metrics.expected_calibration_error
        );
    }
}
