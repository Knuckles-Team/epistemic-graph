//! EH-293 / EH-295 / EH-302: fitting the resident scorer, its conformal
//! calibration, the act rule's coverage guard, and the promotion protocol.

use eg_numeric::decision::admission::{admit, Regime};
use eg_numeric::decision::evaluate::{evaluate, EvalSpec};
use eg_numeric::decision::exploration::ExplorationPermit;
use eg_numeric::decision::features::FeatureMatrix;
use eg_numeric::decision::head_eval::{calibration_statement, read_head, HeadReading};
use eg_numeric::decision::ladder::{decide, LadderInputs};
use eg_types::contract::BoundedVec;
use eg_types::decision::jobs::DecisionEvalReceipt;
use eg_types::decision::statistical::dataset::{ItemLabel, LabelSource, LabelledDataset};
use eg_types::decision::statistical::head::{DecisionHeadBody, HeadKind};
use eg_types::decision::statistical::{CalibrationMethod, StatisticalOutcome};
use eg_types::decision::{ColdStart, QuantScaleTag, QuantisedValue};

use super::common::{
    dataset, gold_dataset, gold_item, option_ids, policy, rows, statistical, FEATURES, OPTIONS,
};
use super::fit_eval::{fitted, rules};

fn scorer() -> DecisionHeadBody {
    fitted(HeadKind::OptionAttention, &gold_dataset(200, 0))
}

fn receipt(head: &DecisionHeadBody, data: &LabelledDataset) -> DecisionEvalReceipt {
    let stat = statistical();
    let admitted = admit(data, &rules(Regime::FullLabel, &[]));
    let spec = EvalSpec {
        regime: Regime::FullLabel,
        statistical: &stat,
        estimators: &[],
        head_digest: "sha256:scorer",
        policy_digest: "sha256:policy",
    };
    evaluate(head, data, &admitted.items, admitted.exclusions, &spec).expect("evaluates")
}

fn matrix(item: usize) -> FeatureMatrix {
    FeatureMatrix {
        candidate_ids: option_ids(),
        feature_names: FEATURES.iter().map(|s| s.to_string()).collect(),
        values: rows(item, item % OPTIONS),
    }
}

fn outcome(head: &DecisionHeadBody, item: usize) -> StatisticalOutcome {
    let HeadReading::InDistribution(reading) = read_head(head, &matrix(item)).expect("reads")
    else {
        panic!("item {item} is in distribution")
    };
    let (ids, strict, stat, seed) = (
        option_ids(),
        policy(ColdStart::DeterministicOnly),
        statistical(),
        [7_u8; 32],
    );
    decide(&LadderInputs {
        candidate_ids: &ids,
        head: Some(head),
        reading: Some(&reading),
        policy: &strict,
        statistical: &stat,
        permit: ExplorationPermit::Off,
        seed: &seed,
    })
    .expect("decides")
    .outcome
}

#[test]
fn a_scorer_fit_is_reproducible_and_conformally_calibrated() {
    let head = scorer();
    assert_eq!(head, scorer(), "same items, same seed, same bits");
    let params = head
        .scorer
        .as_deref()
        .expect("an OptionAttention head carries its scorer");
    assert!(
        params.self_weight.iter().any(|w| *w != 0),
        "training moved off the linear head"
    );
    let calibration = head
        .calibration
        .as_ref()
        .expect("full-label heads calibrate");
    assert!(
        calibration.act_threshold.is_some(),
        "a separable gold set certifies acting"
    );
    assert_eq!(
        calibration_statement(calibration).method,
        CalibrationMethod::Conformal,
        "conformal is the calibration rung"
    );
    assert!(head.clone().checked().is_ok());
}

#[test]
fn an_abstention_is_returned_when_the_calibrated_coverage_bound_fails() {
    let head = scorer();
    let acting = (0..40)
        .find(|&item| matches!(outcome(&head, item), StatisticalOutcome::Acted { .. }))
        .expect("the calibrated scorer acts on some state");

    // The conformal quantile came out empty: no coverage claim at alpha.
    let mut empty = head.clone();
    let calibration = empty.calibration.as_mut().expect("calibrated");
    calibration.set_threshold = QuantisedValue {
        scale: QuantScaleTag::Q32,
        value: -(1 << 32),
    };
    assert_eq!(
        calibration_statement(calibration).method,
        CalibrationMethod::Temperature,
        "without a conformal claim temperature is only the fallback"
    );
    assert!(matches!(
        outcome(&empty, acting),
        StatisticalOutcome::Abstained { .. }
    ));

    // A finite set that misses the top option: the bound fails for this state.
    let mut missing = head.clone();
    let calibration = missing.calibration.as_mut().expect("calibrated");
    calibration.set_threshold = QuantisedValue {
        scale: QuantScaleTag::Q32,
        value: 0,
    };
    assert!(matches!(
        outcome(&missing, acting),
        StatisticalOutcome::Abstained { .. }
    ));
}

#[test]
fn the_promotion_protocol_measures_every_metric_and_passes_a_sound_scorer() {
    let head = scorer();
    let good = receipt(&head, &gold_dataset(200, 1));
    assert!(
        good.passed,
        "failed gates: {:?}",
        good.failed_gates.as_slice()
    );
    let promotion = good
        .promotion
        .as_deref()
        .expect("full-label receipts carry the protocol");
    assert_eq!(
        promotion.stable_items, 200,
        "every decision replays bit for bit"
    );
    assert!(promotion.macs_total > 0);
    assert!(promotion.answered > 0);
    assert!(promotion.early_coverage.is_some() && promotion.late_coverage.is_some());
    assert_eq!(promotion.per_class.len(), 1);
}

/// Items whose gold label moves to another option half-way through time.
fn drifted() -> LabelledDataset {
    let items = (0..200)
        .map(|i| {
            let mut item = gold_item(i, 1, LabelSource::SyntheticConstruction);
            if i >= 100 {
                let wrong = (i + 1 + 1) % OPTIONS;
                item.label = ItemLabel::Gold {
                    acceptable: BoundedVec::new(vec![format!("option-{wrong}")]).unwrap(),
                    source: LabelSource::SyntheticConstruction,
                };
            }
            item
        })
        .collect();
    dataset(items)
}

#[test]
fn a_scorer_that_fails_the_protocol_is_not_promoted() {
    let head = scorer();
    let failed = receipt(&head, &drifted());
    assert!(!failed.passed, "a failed protocol never passes");
    let gates = failed.failed_gates.as_slice();
    assert!(
        gates.iter().any(|g| g == "calibration_drift"),
        "late coverage collapsed: {gates:?}"
    );
    let promotion = failed.promotion.as_deref().expect("protocol");
    let (early, late) = (
        promotion.early_coverage.expect("timed"),
        promotion.late_coverage.expect("timed"),
    );
    assert!(
        u128::from(late.numerator()) * u128::from(early.denominator())
            < u128::from(early.numerator()) * u128::from(late.denominator())
    );
}
