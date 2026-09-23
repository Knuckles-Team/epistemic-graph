//! Fixtures shared by the decision tests.

use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::dataset::{
    ItemLabel, LabelSource, LabelledDataset, LabelledItem, LoggedOutcome, OutcomeEvaluation,
    OutcomeFidelity, PropensitySource, LABELLED_DATASET_SCHEMA_VERSION,
};
use eg_types::decision::{
    ColdStart, DecisionPolicy, EvidenceClass, ObjectiveLevelKind, ObjectiveOrder, QuantScaleTag,
    QuantisedValue, RecordWindow, StatisticalPolicy, TraceFidelityLevel, UnitRationalWire,
    UnknownCostRule, DECISION_POLICY_SCHEMA_VERSION,
};
use eg_types::solve::Scalar;

pub const OPTIONS: usize = 3;
pub const FEATURES: [&str; 2] = ["quality", "noise"];
pub const SCHEMA_DIGEST: &str = "sha256:feature-schema";

pub fn rational(n: u64, d: u64) -> UnitRationalWire {
    UnitRationalWire::new(n, d).expect("fixture rational")
}

pub fn bounded<T, const N: usize>(values: Vec<T>) -> BoundedVec<T, N> {
    BoundedVec::new(values).expect("fixture inside bound")
}

pub fn statistical() -> StatisticalPolicy {
    StatisticalPolicy {
        alpha: rational(1, 10),
        epsilon: rational(1, 10),
        delta: rational(1, 10),
        n_min: 20,
        min_support: 5,
        min_ess: QuantisedValue {
            scale: QuantScaleTag::Q32,
            value: 10 << 32,
        },
        min_outcome_fidelity: TraceFidelityLevel::ToolCalls,
        tenant_public_features: false,
        audit_sample: rational(1, 20),
    }
}

pub fn policy(cold_start: ColdStart) -> DecisionPolicy {
    DecisionPolicy {
        schema_version: DECISION_POLICY_SCHEMA_VERSION,
        objective: ObjectiveOrder::Lexicographic {
            levels: bounded(vec![ObjectiveLevelKind::Uncovered]),
        },
        unknown_cost: UnknownCostRule::ExcludeWhenStrict,
        accepted_gap: Scalar::new(0),
        node_budget: 100_000,
        max_templates: 8,
        max_slots: 6,
        max_nogood_rounds: 8,
        max_why_not_per_slot: 3,
        a2a_requires_observation: true,
        cold_start,
        statistical: Some(statistical()),
    }
}

pub fn window() -> RecordWindow {
    RecordWindow {
        from_ms: 0,
        to_ms: u64::MAX,
    }
}

pub fn option_ids() -> Vec<String> {
    (0..OPTIONS).map(|j| format!("option-{j}")).collect()
}

/// Deterministic rows: the gold option's `quality` is higher by a margin that
/// the `noise` column occasionally erodes.
pub fn rows(item: usize, gold: usize) -> Vec<i64> {
    let mut out = Vec::new();
    for j in 0..OPTIONS {
        let quality = if j == gold { 2.0 } else { 0.0 } + ((item * 7 + j * 13) % 10) as f64 / 5.0;
        let noise = ((item * 3 + j * 5) % 7) as f64 / 7.0;
        out.push((quality * 4_294_967_296.0) as i64);
        out.push((noise * 4_294_967_296.0) as i64);
    }
    out
}

pub fn gold_item(item: usize, seed: usize, source: LabelSource) -> LabelledItem {
    let gold = (item + seed) % OPTIONS;
    LabelledItem {
        item_id: format!("gold-{seed}-{item:04}"),
        recorded_at_ms: 1_000 + item as u64,
        class_key: "eg:task/research".to_string(),
        candidate_ids: bounded(option_ids()),
        features: bounded(rows(item + seed, gold)),
        label: ItemLabel::Gold {
            acceptable: bounded(vec![format!("option-{gold}")]),
            source,
        },
        audit_inclusion: None,
    }
}

pub fn evaluation(success: Option<bool>) -> OutcomeEvaluation {
    OutcomeEvaluation {
        evaluation_id: "evaluation".to_string(),
        class: EvidenceClass::Observation,
        producer: "independent-evaluator".to_string(),
        selected_agent: "agent".to_string(),
        lease_holder: "worker".to_string(),
        fidelity: OutcomeFidelity::ToolCalls,
        success,
    }
}

pub fn logged(executed: usize) -> LoggedOutcome {
    LoggedOutcome {
        executed: format!("option-{executed}"),
        logging_propensities: bounded(vec![rational(1, 3); OPTIONS]),
        propensity_source: PropensitySource::ExecutedPolicy,
        pinned: false,
        commit_principal: "approved".to_string(),
        evaluation: evaluation(Some(true)),
    }
}

pub fn logged_item(item: usize) -> LabelledItem {
    let gold = item % OPTIONS;
    let executed = (item / OPTIONS) % OPTIONS;
    let mut outcome = logged(executed);
    outcome.evaluation.success = Some(executed == gold);
    LabelledItem {
        item_id: format!("logged-{item:04}"),
        label: ItemLabel::Logged(Box::new(outcome)),
        ..gold_item(item, 0, LabelSource::Human)
    }
}

pub fn dataset(items: Vec<LabelledItem>) -> LabelledDataset {
    LabelledDataset {
        schema_version: LABELLED_DATASET_SCHEMA_VERSION,
        feature_schema_digest: SCHEMA_DIGEST.to_string(),
        feature_names: bounded(FEATURES.iter().map(|s| s.to_string()).collect()),
        scale: QuantScaleTag::Q32,
        items: bounded(items),
        synthetic: true,
    }
}

pub fn gold_dataset(n: usize, seed: usize) -> LabelledDataset {
    dataset(
        (0..n)
            .map(|i| gold_item(i, seed, LabelSource::SyntheticConstruction))
            .collect(),
    )
}

pub fn approved() -> Vec<String> {
    vec!["approved".to_string()]
}
