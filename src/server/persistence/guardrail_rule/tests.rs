//! `GuardrailRule` publish + read-back (EG-DECISION-ENGINE-R127.2.1).

use eg_types::agent_component::{
    AgentComponentDraft, AgentComponentKind, AgentComponentPublishRequest,
};
use eg_types::decision::guardrail::{GuardrailRule, GUARDRAIL_APPLIES_TO_CLASS_ATTRIBUTE};

use super::super::agent_component::test_component_draft;
use super::super::agent_fixtures::{mutation_context, open_agent_store};
use super::*;

const TENANT: &str = "tenant-a";

fn rule(id: &str, applies_to: &str) -> GuardrailRule {
    GuardrailRule {
        rule_id: id.to_string(),
        applies_to_class: applies_to.to_string(),
        source_generation: "gen-7".to_string(),
    }
}

fn rule_draft(tenant: &str, rule: &GuardrailRule) -> AgentComponentDraft {
    AgentComponentDraft {
        kind: AgentComponentKind::GuardrailRule,
        attributes: rule.to_attributes(),
        ..test_component_draft(tenant, &rule.rule_id)
    }
}

fn publish(
    store: &AgentLibraryStore,
    tenant: &str,
    nonce: u8,
    draft: AgentComponentDraft,
) -> Result<(), String> {
    let key = format!("guardrail:{}", draft.component_id);
    store
        .publish_component(AgentComponentPublishRequest {
            context: mutation_context(store, tenant, &key, nonce, 0, "agent-component:publish"),
            evaluation_receipt_digest: None,
            component: draft,
        })
        .map(|_| ())
}

// spec: EG-DECISION-ENGINE-R127.2.1
#[test]
fn a_published_guardrail_rule_reads_back_unchanged_after_a_restart() {
    let (dir, store) = open_agent_store();
    let audit = rule("governance:audit-trail", "eg:task");
    let review = rule("standard:code-review", "eg:task/engineering");
    publish(&store, TENANT, 1, rule_draft(TENANT, &review)).expect("publishes");
    publish(&store, TENANT, 2, rule_draft(TENANT, &audit)).expect("publishes");
    // Another tenant's rule and a non-rule component are not in the set.
    publish(
        &store,
        "tenant-b",
        3,
        rule_draft("tenant-b", &rule("foreign:rule", "eg:task")),
    )
    .expect("publishes");
    publish(&store, TENANT, 4, test_component_draft(TENANT, "tool-a")).expect("publishes");

    let expected = vec![audit, review];
    assert_eq!(store.published_guardrail_rules(TENANT).unwrap(), expected);

    drop(store);
    let reopened = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
    assert_eq!(
        reopened.published_guardrail_rules(TENANT).unwrap(),
        expected
    );
    assert!(reopened
        .published_guardrail_rules("tenant-c")
        .unwrap()
        .is_empty());
}

// spec: EG-DECISION-ENGINE-R127.2.1
#[test]
fn a_malformed_guardrail_rule_is_refused_at_publish() {
    let (_dir, store) = open_agent_store();
    let mut missing = rule_draft(TENANT, &rule("rule:missing", "eg:task"));
    missing
        .attributes
        .remove(GUARDRAIL_APPLIES_TO_CLASS_ATTRIBUTE);
    let error = publish(&store, TENANT, 1, missing).unwrap_err();
    assert!(error.contains("MALFORMED_GUARDRAIL_RULE"), "{error}");

    let not_iri = rule_draft(TENANT, &rule("rule:not-iri", "not an iri"));
    let error = publish(&store, TENANT, 2, not_iri).unwrap_err();
    assert!(error.contains("MALFORMED_GUARDRAIL_RULE"), "{error}");

    assert!(store.published_guardrail_rules(TENANT).unwrap().is_empty());
}
