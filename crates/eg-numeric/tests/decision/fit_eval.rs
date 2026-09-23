//! EH-027 / EH-062 / EH-005 / EH-006: a deterministic fit, its calibration,
//! and the evaluation receipt that gates promotion.

use eg_numeric::decision::admission::{admit, AdmissionRules, Regime};
use eg_numeric::decision::evaluate::{evaluate, EvalSpec};
use eg_numeric::decision::fit::{fit, FitSpec};
use eg_types::decision::jobs::{OpeEstimatorKind, OptimiserSpec};
use eg_types::decision::statistical::dataset::LabelledDataset;
use eg_types::decision::statistical::head::{DecisionHeadBody, HeadKind};
use eg_types::decision::{QuantScaleTag, QuantisedValue, TraceFidelityLevel};

use super::common::{
    approved, dataset, gold_dataset, logged_item, statistical, window, SCHEMA_DIGEST,
};

fn optimiser() -> OptimiserSpec {
    OptimiserSpec {
        max_iterations: 60,
        tolerance: QuantisedValue {
            scale: QuantScaleTag::Q32,
            value: 1 << 12,
        },
        seed: 0,
    }
}

fn rules(regime: Regime, approved: &[String]) -> AdmissionRules<'_> {
    AdmissionRules {
        regime,
        window: window(),
        fidelity_floor: TraceFidelityLevel::ToolCalls,
        approved_principals: approved,
    }
}

fn fitted(kind: HeadKind, data: &LabelledDataset) -> DecisionHeadBody {
    let stat = statistical();
    let admitted = admit(data, &rules(Regime::FullLabel, &[]));
    let spec = FitSpec {
        head_kind: kind,
        regime: Regime::FullLabel,
        optimiser: optimiser(),
        feature_schema_digest: SCHEMA_DIGEST,
        statistical: &stat,
    };
    fit(data, &admitted.items, &spec).expect("fits")
}

#[test]
fn a_fit_is_bit_for_bit_reproducible_and_certifies_an_act_threshold() {
    let data = gold_dataset(200, 0);
    let head = fitted(HeadKind::ListwiseLogistic, &data);
    assert_eq!(
        head,
        fitted(HeadKind::ListwiseLogistic, &data),
        "same items, same bits"
    );
    assert!(eg_types::decision::statistical::head::HeadKind::ListwiseLogistic == head.kind);
    let calibration = head
        .calibration
        .as_ref()
        .expect("full-label listwise heads calibrate");
    assert!(
        calibration.act_threshold.is_some(),
        "a separable gold set certifies acting"
    );
    assert!(calibration.synthetic, "synthetic evidence is labelled");
    assert!(
        head.weights.as_slice()[0].value > 0,
        "quality pushes the logit up"
    );
}

#[test]
fn a_weighted_features_head_is_advisory_and_never_calibrated() {
    let head = fitted(HeadKind::WeightedFeatures, &gold_dataset(60, 0));
    assert!(head.calibration.is_none());
}

#[test]
fn the_receipt_passes_on_held_out_gold_and_names_failed_gates_otherwise() {
    let head = fitted(HeadKind::ListwiseLogistic, &gold_dataset(200, 0));
    let holdout = gold_dataset(200, 1);
    let stat = statistical();
    let admitted = admit(&holdout, &rules(Regime::FullLabel, &[]));
    let spec = EvalSpec {
        regime: Regime::FullLabel,
        statistical: &stat,
        estimators: &[],
        head_digest: "sha256:head",
        policy_digest: "sha256:policy",
    };
    let receipt =
        evaluate(&head, &holdout, &admitted.items, admitted.exclusions, &spec).expect("evaluates");
    let metrics = receipt.metrics.expect("full-label metrics");
    assert!(
        receipt.passed,
        "failed gates: {:?}",
        receipt.failed_gates.as_slice()
    );
    assert!(metrics.top1_hits * 10 >= metrics.n_items * 9);
    assert!(receipt.receipt_digest.starts_with("sha256:"));

    let tiny = gold_dataset(5, 1);
    let admitted = admit(&tiny, &rules(Regime::FullLabel, &[]));
    let small =
        evaluate(&head, &tiny, &admitted.items, admitted.exclusions, &spec).expect("evaluates");
    assert!(!small.passed);
    assert!(
        small.failed_gates.iter().any(|g| g == "n_min"),
        "no claim below n_min"
    );
}

#[test]
fn off_policy_evaluation_reports_support_and_ess() {
    let head = fitted(HeadKind::ListwiseLogistic, &gold_dataset(200, 0));
    let logs = dataset((0..300).map(logged_item).collect());
    let who = approved();
    let admitted = admit(&logs, &rules(Regime::BanditLabel, &who));
    assert_eq!(admitted.items.len(), 300);
    let stat = statistical();
    let estimators = [
        OpeEstimatorKind::Ips,
        OpeEstimatorKind::ClippedIps,
        OpeEstimatorKind::Snips,
        OpeEstimatorKind::Switch,
        OpeEstimatorKind::DoublyRobust,
    ];
    let spec = EvalSpec {
        regime: Regime::BanditLabel,
        statistical: &stat,
        estimators: &estimators,
        head_digest: "sha256:head",
        policy_digest: "sha256:policy",
    };
    let receipt =
        evaluate(&head, &logs, &admitted.items, admitted.exclusions, &spec).expect("evaluates");
    assert_eq!(receipt.estimates.len(), estimators.len());
    assert!(receipt
        .estimates
        .iter()
        .all(|e| e.unsupported_mass.numerator() == 0));
    assert!(
        receipt.passed,
        "failed gates: {:?}",
        receipt.failed_gates.as_slice()
    );
    assert!(
        !receipt.pooled.is_empty(),
        "options above min_support are pooled"
    );
}

#[test]
fn an_off_policy_estimate_on_unsupported_actions_blocks_promotion() {
    let head = fitted(HeadKind::ListwiseLogistic, &gold_dataset(200, 0));
    let items: Vec<_> = (0..60)
        .map(|i| {
            let mut item = logged_item(i);
            if let eg_types::decision::statistical::dataset::ItemLabel::Logged(l) = &mut item.label
            {
                let executed = (i / 3) % 3;
                let mut p = vec![super::common::rational(0, 1); 3];
                p[executed] = super::common::rational(1, 1);
                l.logging_propensities = super::common::bounded(p);
            }
            item
        })
        .collect();
    let logs = dataset(items);
    let who = approved();
    let admitted = admit(&logs, &rules(Regime::BanditLabel, &who));
    let stat = statistical();
    let spec = EvalSpec {
        regime: Regime::BanditLabel,
        statistical: &stat,
        estimators: &[OpeEstimatorKind::Ips],
        head_digest: "sha256:head",
        policy_digest: "sha256:policy",
    };
    let receipt =
        evaluate(&head, &logs, &admitted.items, admitted.exclusions, &spec).expect("evaluates");
    assert!(!receipt.passed);
    assert!(receipt.failed_gates.iter().any(|g| g == "unsupported_mass"));
}
