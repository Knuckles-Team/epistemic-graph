//! MEDULLA-DECISION-ENGINE-R129.3/.4 through the served `AgentComponent` publish:
//! a guardrail rule that contradicts a stored invariant is never promoted,
//! and the full agreement an explicit publish carries cannot override it.

use super::{context, persistence_id, Served, ADMIN, TENANT};
use crate::protocol::{Method, Response};
use medulla_types::agent_component::{
    AgentComponentKind, AgentComponentOp, AgentComponentPublishRequest,
};
use medulla_types::decision::guardrail::{GuardrailRule, GUARDRAIL_FORBIDS_RULE_ATTRIBUTE};

async fn publish(
    served: &Served,
    key: &str,
    id: &str,
    class: &str,
    forbids: Option<&str>,
) -> Response {
    let mut draft = crate::server::persistence::agent_component::test_component_draft(TENANT, id);
    draft.kind = AgentComponentKind::GuardrailRule;
    draft.attributes = GuardrailRule {
        rule_id: id.into(),
        applies_to_class: class.into(),
        source_generation: "generation-7".into(),
    }
    .to_attributes();
    if let Some(rule) = forbids {
        draft.attributes.insert(
            GUARDRAIL_FORBIDS_RULE_ATTRIBUTE.to_string(),
            rule.to_string(),
        );
    }
    let mut publish_context = context(key);
    publish_context.principal = persistence_id(ADMIN);
    publish_context.caller_principal = persistence_id(ADMIN);
    publish_context.expected_revision = Some(0);
    let op = AgentComponentOp::Publish {
        request: Box::new(AgentComponentPublishRequest {
            context: publish_context,
            component: draft,
            evaluation_receipt_digest: None,
        }),
    };
    served.call(key, Method::AgentComponent { op }).await
}

// spec: MEDULLA-DECISION-ENGINE-R129.3
// spec: MEDULLA-DECISION-ENGINE-R129.4
#[tokio::test]
async fn served_publish_refuses_a_rule_that_contradicts_a_stored_invariant() {
    let served = Served::new();
    for (key, iri) in [
        ("inv-first", "eg:invariant/first"),
        ("inv-second", "eg:invariant/second"),
    ] {
        let stored = publish(
            &served,
            key,
            iri,
            "eg:task/Deploy",
            Some("eg:rule/skip-review"),
        )
        .await;
        assert!(stored.error.is_none(), "{stored:?}");
    }

    // An explicit publish is the full agreement of its principal; the
    // invariant gate still runs first and names the first contradicted one.
    let refused = publish(
        &served,
        "rule-bad",
        "eg:rule/skip-review",
        "eg:task/Deploy",
        None,
    )
    .await;
    assert_eq!(
        refused.error.as_deref(),
        Some("RULE_CONTRADICTS_INVARIANT"),
        "{refused:?}"
    );
    assert!(
        refused
            .error_detail
            .as_deref()
            .is_some_and(|d| d.contains("'eg:invariant/first'") && !d.contains("second")),
        "{refused:?}"
    );
    assert!(refused.result.is_none());
    let store = served.state.write().await.ensure_agent_library().unwrap();
    assert!(store
        .current_component(TENANT, "eg:rule/skip-review")
        .unwrap()
        .is_none());

    // Exact class match only: the same rule on another class, and another
    // rule on the protected class, are both promoted.
    for (key, id, class) in [
        ("rule-other-class", "eg:rule/skip-review", "eg:task/Read"),
        ("rule-other-id", "eg:rule/add-check", "eg:task/Deploy"),
    ] {
        let ok = publish(&served, key, id, class, None).await;
        assert!(ok.error.is_none(), "{key}: {ok:?}");
    }
    assert_eq!(
        store
            .published_rule_invariants(TENANT)
            .unwrap()
            .into_iter()
            .map(|i| i.invariant_iri)
            .collect::<Vec<_>>(),
        vec!["eg:invariant/first", "eg:invariant/second"]
    );
}
