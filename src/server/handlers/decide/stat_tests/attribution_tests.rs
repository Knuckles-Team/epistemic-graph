//! EH-523: the per-component split of an assembled slate's outcome, served on the
//! outcome aggregate. The logged game is the evaluated slates; a split is computed only
//! from sub-slates that were themselves assembled and evaluated at `min_support`.

use super::consumer_tests::evaluate_assembly;
use super::*;
use crate::server::persistence::decision_record::tests as v1;
use eg_types::agent_component::ComponentDependency;
use eg_types::decision::record::SlotAssignment;
use eg_types::decision::statistical::log::{
    SlateAttribution, SlateAttributionMethod, SlateAttributionRequest,
};
use eg_types::decision::{DecisionOutcome, DecisionRecord};

/// `record` re-slotted to `components` under graph digest `graph`, committed.
fn commit_slate(
    h: &Harness,
    template: &DecisionRecord,
    graph: &str,
    components: &[&str],
    nonce: u8,
) {
    let mut record = template.clone();
    record.record_id = format!("decision:slate-{graph}");
    let DecisionOutcome::Solved {
        certificate, slots, ..
    } = &template.outcome
    else {
        panic!("the fixture is solved")
    };
    let pin = slots.iter().next().expect("one slot").component.clone();
    let slots = components
        .iter()
        .enumerate()
        .map(|(i, component)| SlotAssignment {
            slot: format!("slot-{i}"),
            component: ComponentDependency {
                component_id: (*component).to_string(),
                ..pin.clone()
            },
        })
        .collect();
    record.outcome = DecisionOutcome::Solved {
        graph_digest: graph.to_string(),
        slots: BoundedVec::new(slots).unwrap(),
        certificate: certificate.clone(),
        topology: None,
    };
    let digest = record.inputs.catalog_digest.clone();
    h.store
        .commit_decision_record(
            v1::commit_context(&h.store, &format!("slate-{graph}"), nonce),
            &record,
            &digest,
        )
        .unwrap();
}

/// `successes` of 10 independent evaluations of slate `graph`.
async fn evaluate_slate(h: &Harness, graph: &str, successes: usize) {
    let outcomes: Vec<bool> = (0..10).map(|n| n < successes).collect();
    evaluate_assembly(h, &format!("decision:slate-{graph}"), &outcomes).await;
}

async fn split(h: &Harness, method: SlateAttributionMethod) -> Result<SlateAttribution, String> {
    let evaluator = VerifiedRequestContext::verified_for_test_in_tenant("evaluator", v1::TENANT);
    let op = DecisionLogOp::Aggregate {
        request: OutcomeAggregateRequest {
            tenant_id: v1::TENANT.to_string(),
            question_id: Some("assembly".to_string()),
            window: window(),
            attribution: Some(SlateAttributionRequest {
                graph_digest: "g-ab".to_string(),
                method,
            }),
        },
    };
    let aggregate: OutcomeAggregate =
        decode(super::super::log::handle_decision_log(&h.state, 41, &evaluator, op).await)?;
    Ok(aggregate.attribution.expect("the split was asked for"))
}

fn value(q: QuantisedValue) -> f64 {
    q.value as f64 / (1u64 << 32) as f64
}

/// Slates {a} (6/10), {b} (2/10), {a, b} (9/10), plus a template to copy.
async fn slates(h: &Harness) {
    v1::seed_library(&h.store);
    let template = v1::decided(&h.store).record;
    commit_slate(h, &template, "g-a", &["comp-a"], 1);
    commit_slate(h, &template, "g-b", &["comp-b"], 2);
    commit_slate(h, &template, "g-ab", &["comp-a", "comp-b"], 3);
    evaluate_slate(h, "g-a", 6).await;
    evaluate_slate(h, "g-b", 2).await;
    evaluate_slate(h, "g-ab", 9).await;
}

#[tokio::test]
async fn a_fully_logged_slate_splits_by_exact_shapley() {
    let h = Harness::new().await;
    slates(&h).await;
    let report = split(&h, SlateAttributionMethod::Shapley).await.unwrap();
    assert_eq!(report.evidence_class, EvidenceClass::Claim);
    assert_eq!(report.observed_coalitions, 3);
    let phi: Vec<f64> = report
        .components
        .iter()
        .map(|c| value(c.contribution))
        .collect();
    // φ_a = (v(a) + v(ab) - v(b)) / 2 = 0.65; φ_b = (v(b) + v(ab) - v(a)) / 2 = 0.25.
    assert!(
        (phi[0] - 0.65).abs() < 1e-6 && (phi[1] - 0.25).abs() < 1e-6,
        "{phi:?}"
    );
    assert!((value(report.grand) - 0.9).abs() < 1e-6);
    assert!(report.digest.starts_with("sha256:"));
    assert_eq!(report.components.as_slice()[0].component_id, "comp-a");
}

#[tokio::test]
async fn an_unlogged_sub_slate_is_refused_but_the_declared_additive_split_answers() {
    let h = Harness::new().await;
    v1::seed_library(&h.store);
    let template = v1::decided(&h.store).record;
    commit_slate(&h, &template, "g-a", &["comp-a"], 1);
    commit_slate(&h, &template, "g-ab", &["comp-a", "comp-b"], 3);
    evaluate_slate(&h, "g-a", 6).await;
    evaluate_slate(&h, "g-ab", 9).await;
    let error = split(&h, SlateAttributionMethod::Shapley)
        .await
        .unwrap_err();
    assert!(error.contains("UNSUPPORTED_COALITION"), "{error}");
    let additive = split(&h, SlateAttributionMethod::Additive).await.unwrap();
    let phi: Vec<f64> = additive
        .components
        .iter()
        .map(|c| value(c.contribution))
        .collect();
    assert!(
        (phi[0] - 0.6).abs() < 1e-6 && (phi[1] - 0.3).abs() < 1e-6,
        "{phi:?}"
    );
}
