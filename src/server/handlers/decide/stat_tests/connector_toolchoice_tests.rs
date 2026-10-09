//! EG-DECISION-ENGINE-R042: connector tool-choice advisory via `Decide`.
//! `Decide` evaluates which internal tool a connector should use for a task
//! and returns a proposal with evidence only; the connector's own
//! deterministic authorization path, never `Decide`, governs whether any
//! resulting write-back actually occurs. This test exercises tool-choice
//! evaluation for a connector task and confirms the proposal alone cannot
//! trigger a write-back: `StatisticalOutcome` is a closed, four-variant enum
//! (checked here exhaustively) with no "write" variant at all, and under the
//! default policy with no licensed head the served outcome is `Abstained`.

use super::*;

fn connector_tool_choice_request(schema: &ComponentDependency) -> DecideRequest {
    let mut asked = request(
        schema,
        None,
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    asked.question.kind = QuestionKind::ConnectorToolChoice;
    asked
}

// spec: EG-DECISION-ENGINE-R042
#[tokio::test]
async fn connector_tool_choice_is_a_proposal_that_cannot_itself_trigger_a_write_back() {
    let h = Harness::new().await;
    h.publish(
        "tool-connector-candidate",
        AgentComponentKind::Tool,
        "an internal tool a connector might use for its task",
        None::<&()>,
        None,
    )
    .unwrap();
    let schema_pin = h
        .publish(
            "schema-connector-toolchoice",
            AgentComponentKind::FeatureSchema,
            "connector tool-choice features",
            Some(&schema()),
            None,
        )
        .unwrap();

    let batch = decide(&h, connector_tool_choice_request(&schema_pin))
        .await
        .unwrap();
    let record = &batch.records.as_slice()[0];
    assert_eq!(record.question.kind, QuestionKind::ConnectorToolChoice);

    // Exhaustive by construction: `StatisticalOutcome` names no "write" or
    // "write-back" variant at all, so no proposal from this question kind
    // (or any other) can itself trigger one -- the only four things `Decide`
    // can ever answer are these.
    match &record.outcome {
        StatisticalOutcome::Abstained { .. }
        | StatisticalOutcome::Advisory { .. }
        | StatisticalOutcome::Explored { .. }
        | StatisticalOutcome::Acted { .. } => {}
    }
    assert!(
        matches!(record.outcome, StatisticalOutcome::Abstained { .. }),
        "no licensed head: the proposal alone takes no action: {:?}",
        record.outcome
    );
}
