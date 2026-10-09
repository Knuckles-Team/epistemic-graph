//! EG-DECISION-ENGINE-R043: write-back proposals routed through `Decide`.
//! The decision ladder can produce a write-back proposal with supporting
//! evidence for a connector action, but the proposal never authorizes the
//! write itself; any exploration or statistical head behind it stays outside
//! the connector's own deterministic authorization step. This negative test
//! asserts a `Decide`-sourced write-back proposal has no external effect: the
//! served outcome -- whether abstained (default policy) or explored (an
//! exploring policy) -- is a closed, four-variant enum with no "write" or
//! "committed" variant or field at all, so naming a candidate as chosen is
//! never itself the write.

use super::*;

fn connector_write_back_request(schema: &ComponentDependency) -> DecideRequest {
    let mut asked = request(
        schema,
        None,
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    asked.question.kind = QuestionKind::ConnectorWriteBack;
    asked
}

#[tokio::test]
async fn a_write_back_proposal_has_no_external_effect_until_separately_authorized() {
    let h = Harness::new().await;
    h.publish(
        "tool-connector-write-target",
        AgentComponentKind::Tool,
        "the connector action a write-back proposal would name",
        None::<&()>,
        None,
    )
    .unwrap();
    let schema_pin = h
        .publish(
            "schema-connector-writeback",
            AgentComponentKind::FeatureSchema,
            "connector write-back features",
            Some(&schema()),
            None,
        )
        .unwrap();

    // Default (deterministic-only) policy: no licensed head, so the ladder
    // takes no action on the connector's behalf at all.
    let batch = decide(&h, connector_write_back_request(&schema_pin))
        .await
        .unwrap();
    let record = &batch.records.as_slice()[0];
    assert_eq!(record.question.kind, QuestionKind::ConnectorWriteBack);
    assert!(matches!(
        record.outcome,
        StatisticalOutcome::Abstained { .. }
    ));

    // Under an exploring policy the ladder DOES name a chosen option -- but
    // `Explored` carries only an option id and its propensity, never a
    // "committed" or "written" field; naming a candidate is not writing it.
    let policy = ordinary_exploration_policy();
    let policy_pin = h.publish_policy("policy-connector-writeback-explore", &policy);
    let mut explored_request = connector_write_back_request(&schema_pin);
    explored_request.policy = DecisionPolicyRef::Pinned {
        component: policy_pin,
    };
    let explored = decide(&h, explored_request).await.unwrap();
    let record = &explored.records.as_slice()[0];
    let StatisticalOutcome::Explored {
        option_id,
        propensity: _,
    } = &record.outcome
    else {
        panic!("expected an exploration: {:?}", record.outcome)
    };
    assert_eq!(option_id, "tool-connector-write-target");
    // Exhaustive by construction: `StatisticalOutcome` names no "write" or
    // "write-back" variant, for this question kind or any other, so this
    // proposal alone can never authorize the write the connector would make.
    match &record.outcome {
        StatisticalOutcome::Abstained { .. }
        | StatisticalOutcome::Advisory { .. }
        | StatisticalOutcome::Explored { .. }
        | StatisticalOutcome::Acted { .. } => {}
    }
}
