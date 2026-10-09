//! EG-DECISION-ENGINE-R041: connector event triage through `Decide`.
//! Classifying an inbound connector event is routed through the same
//! evaluate-only decision ladder every other question kind uses: this test
//! submits synthetic connector events through `ConnectorEventTriage`,
//! confirms no side effect occurs under the default (deterministic-only)
//! policy, and confirms a sampled fraction of evaluations is logged for
//! audit when the policy instead permits exploration -- the same per-decision
//! logging vector any other question kind gets, proving this kind inherits
//! the guarantee rather than bypassing it.

use super::*;

fn connector_event_request(schema: &ComponentDependency) -> DecideRequest {
    let mut asked = request(
        schema,
        None,
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    asked.question.kind = QuestionKind::ConnectorEventTriage;
    asked
}

#[tokio::test]
async fn connector_event_triage_is_evaluate_only_and_logs_a_sampled_fraction() {
    let h = Harness::new().await;
    h.publish(
        "tool-connector-event",
        AgentComponentKind::Tool,
        "a synthetic inbound connector event, classified before any action",
        None::<&()>,
        None,
    )
    .unwrap();
    let schema_pin = h
        .publish(
            "schema-connector-triage",
            AgentComponentKind::FeatureSchema,
            "connector event triage features",
            Some(&schema()),
            None,
        )
        .unwrap();

    // Default (deterministic-only) policy: no head, no exploration budget --
    // the triage call can take no action on the connector's behalf.
    let batch = decide(&h, connector_event_request(&schema_pin))
        .await
        .unwrap();
    let record = &batch.records.as_slice()[0];
    assert_eq!(record.question.kind, QuestionKind::ConnectorEventTriage);
    assert!(
        matches!(record.outcome, StatisticalOutcome::Abstained { .. }),
        "no side effect on the connector's behalf: {:?}",
        record.outcome
    );
    assert!(record.logging_propensities.is_empty());

    // Under a policy that permits exploration, a sampled fraction of these
    // evaluations is logged: the executed policy's own propensities, exactly
    // as for any other question kind (EH-026's audit/logging machinery is
    // generic over `QuestionKind`).
    let policy = ordinary_exploration_policy();
    let policy_pin = h.publish_policy("policy-connector-triage-explore", &policy);
    let mut explored_request = connector_event_request(&schema_pin);
    explored_request.policy = DecisionPolicyRef::Pinned {
        component: policy_pin,
    };
    let explored = decide(&h, explored_request).await.unwrap();
    let record = &explored.records.as_slice()[0];
    assert!(
        matches!(record.outcome, StatisticalOutcome::Explored { .. }),
        "{:?}",
        record.outcome
    );
    assert!(
        !record.logging_propensities.is_empty(),
        "a sampled evaluation is logged for audit"
    );
}
