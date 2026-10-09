//! EG-DECISION-ENGINE-R038: advisory pre-tool risk scoring. `Decide` is
//! evaluate-only for every question kind (this crate's own module doc:
//! "it reads candidates ... it commits nothing"); `PreToolRisk` inherits
//! that guarantee rather than being a special case. This test proves it: a
//! `PreToolRisk` question served through the real `Decide` handler returns a
//! record whose outcome is never `Acted` under the default
//! (deterministic-only, no licensed calibration) policy, so the risk score
//! it carries can only ever inform a caller's own authorization step --
//! `Decide` offers no further "apply this score" mechanism at all.

use eg_types::decision::record::ResolutionKind;

use super::*;

fn pretool_risk_request(schema: &ComponentDependency) -> DecideRequest {
    let mut asked = request(
        schema,
        None,
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    asked.question.kind = QuestionKind::PreToolRisk;
    asked
}

#[tokio::test]
async fn a_pretool_risk_question_is_served_as_advisory_only_never_as_an_act() {
    let h = Harness::new().await;
    h.publish(
        "tool-risky",
        AgentComponentKind::Tool,
        "a tool whose invocation is risk-scored before the call",
        None::<&()>,
        None,
    )
    .unwrap();
    let schema_pin = h
        .publish(
            "schema-pretool-risk",
            AgentComponentKind::FeatureSchema,
            "pre-tool risk features",
            Some(&schema()),
            None,
        )
        .unwrap();

    let batch = decide(&h, pretool_risk_request(&schema_pin)).await.unwrap();
    let record = &batch.records.as_slice()[0];
    assert_eq!(record.question.kind, QuestionKind::PreToolRisk);
    assert!(
        !matches!(record.outcome, StatisticalOutcome::Acted { .. }),
        "a pre-tool risk score must never itself be an Act: {:?}",
        record.outcome
    );
    // The record's resolution kind is derived generically from the outcome
    // (never from the question kind: see `stat_decide::resolution`), so it is
    // populated here exactly as for any other advisory/abstained decision.
    match record.outcome {
        StatisticalOutcome::Abstained { .. } => {
            assert_eq!(record.resolution_kind, ResolutionKind::Abstention);
        }
        StatisticalOutcome::Advisory { .. } | StatisticalOutcome::Explored { .. } => {
            assert_eq!(record.resolution_kind, ResolutionKind::Statistical);
        }
        StatisticalOutcome::Acted { .. } => unreachable!("checked above"),
    }
}
