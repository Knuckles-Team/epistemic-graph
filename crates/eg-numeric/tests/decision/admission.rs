//! EH-016 / EH-022: only independently evaluated outcomes train, and the two
//! label regimes never mix. Each §11.2 label defect is planted once.

use eg_numeric::decision::admission::{admit, exclusion, AdmissionRules, Exclusion, Regime};
use eg_types::decision::statistical::dataset::{
    ItemLabel, LabelSource, LabelledItem, LoggedOutcome, OutcomeFidelity, PropensitySource,
};
use eg_types::decision::{EvidenceClass, RecordWindow, TraceFidelityLevel};

use super::common::{gold_item, logged_item, rational};

fn rules(regime: Regime, approved: &[String]) -> AdmissionRules<'_> {
    AdmissionRules {
        regime,
        window: RecordWindow {
            from_ms: 0,
            to_ms: u64::MAX,
        },
        fidelity_floor: TraceFidelityLevel::ToolCalls,
        approved_principals: approved,
    }
}

fn with_logged(edit: impl FnOnce(&mut LoggedOutcome)) -> LabelledItem {
    let mut item = logged_item(0);
    if let ItemLabel::Logged(logged) = &mut item.label {
        edit(logged);
    }
    item
}

#[test]
fn every_planted_label_defect_is_refused_by_name() {
    let approved = vec!["approved".to_string()];
    let bandit = rules(Regime::BanditLabel, &approved);
    let cases: Vec<(LabelledItem, Exclusion)> = vec![
        (
            with_logged(|l| l.evaluation.producer = l.evaluation.selected_agent.clone()),
            Exclusion::SelfReported,
        ),
        (
            with_logged(|l| l.evaluation.producer = l.evaluation.lease_holder.clone()),
            Exclusion::SelfReported,
        ),
        (
            with_logged(|l| l.evaluation.class = EvidenceClass::Claim),
            Exclusion::NotObservation,
        ),
        (
            with_logged(|l| l.evaluation.success = None),
            Exclusion::Censored,
        ),
        (
            with_logged(|l| l.evaluation.fidelity = OutcomeFidelity::Cancelled),
            Exclusion::Censored,
        ),
        (
            with_logged(|l| l.evaluation.fidelity = OutcomeFidelity::FinalOutput),
            Exclusion::BelowFidelityFloor,
        ),
        (
            with_logged(|l| l.propensity_source = PropensitySource::HeadMass),
            Exclusion::PropensityNotExecutedPolicy,
        ),
        (with_logged(|l| l.pinned = true), Exclusion::Pinned),
        (
            with_logged(|l| l.commit_principal = "stranger".to_string()),
            Exclusion::UnapprovedPrincipal,
        ),
        (
            with_logged(|l| {
                l.logging_propensities =
                    super::common::bounded(vec![rational(0, 1), rational(1, 2), rational(1, 2)])
            }),
            Exclusion::ZeroExecutedPropensity,
        ),
        (gold_item(0, 0, LabelSource::Human), Exclusion::WrongRegime),
    ];
    for (item, expected) in cases {
        assert_eq!(exclusion(&item, &bandit), Some(expected));
    }
    assert_eq!(exclusion(&logged_item(0), &bandit), None);
}

#[test]
fn an_llm_resolved_abstention_never_enters_the_calibration_set() {
    let full = rules(Regime::FullLabel, &[]);
    assert_eq!(
        exclusion(&gold_item(0, 0, LabelSource::LlmResolved), &full),
        Some(Exclusion::LlmResolved)
    );
    assert_eq!(exclusion(&gold_item(0, 0, LabelSource::Human), &full), None);
    assert_eq!(
        exclusion(&logged_item(0), &full),
        Some(Exclusion::WrongRegime)
    );
}

#[test]
fn admission_counts_every_refusal() {
    let items = vec![
        gold_item(0, 0, LabelSource::Human),
        gold_item(1, 0, LabelSource::LlmResolved),
        logged_item(2),
    ];
    let data = super::common::dataset(items);
    let admitted = admit(&data, &rules(Regime::FullLabel, &[]));
    assert_eq!(admitted.items.len(), 1);
    assert_eq!(admitted.exclusions.llm_resolved, 1);
    assert_eq!(admitted.exclusions.wrong_regime, 1);
}
