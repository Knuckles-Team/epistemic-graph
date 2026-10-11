//! Native DTO, Method and pure-query checks. Served dispatcher tests live in
//! the facade and remain a separate acceptance gate.

#![cfg(feature = "contract")]

use eg_types::decision::guardrail::{
    guardrail_entailment_query, EntailmentStep, GuardrailEntailmentRequest, GuardrailRule,
};
use eg_types::protocol::Method;

#[test]
fn guardrail_wire_roundtrip_retains_tenant_and_task() {
    let method = Method::GuardrailEntailment {
        request: Box::new(GuardrailEntailmentRequest {
            tenant_id: "tenant-a".into(),
            task_iri: "eg:task/research".into(),
        }),
    };
    let value = serde_json::to_value(&method).unwrap();
    let decoded: Method = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    let policy = eg_capabilities::policy(&method);
    assert!(!policy.mutates);
    assert_eq!(policy.authz_action, "agent:assemble-read");
    assert_eq!(
        policy.txn_participation,
        eg_capabilities::TxnParticipation::Snapshot
    );
}

#[test]
fn guardrail_request_validation_refuses_malformed_task_iris() {
    for task in ["", " ", "research", "eg:task research", ":task", "eg:"] {
        let request = GuardrailEntailmentRequest {
            tenant_id: "tenant-a".into(),
            task_iri: task.into(),
        };
        assert!(request
            .validate()
            .unwrap_err()
            .starts_with("INVALID_ARGUMENT:"));
    }
}

#[test]
fn native_guardrail_query_proves_exact_and_subclass_rules_only() {
    let rules = [
        GuardrailRule {
            rule_id: "rule:parent".into(),
            applies_to_class: "eg:task".into(),
            source_generation: "generation-7".into(),
        },
        GuardrailRule {
            rule_id: "rule:exact".into(),
            applies_to_class: "eg:task/research".into(),
            source_generation: "generation-8".into(),
        },
        GuardrailRule {
            rule_id: "rule:unrelated".into(),
            applies_to_class: "eg:capability".into(),
            source_generation: "generation-9".into(),
        },
    ];
    let result = guardrail_entailment_query("eg:task/research", &rules).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(
        result[0].entailment_path,
        vec![EntailmentStep {
            narrower: "eg:task/research".into(),
            broader: "eg:task".into()
        }]
    );
    assert_eq!(result[0].source_generation, "generation-7");
    assert_eq!(result[1].rule_id, "rule:exact");
    assert!(result[1].entailment_path.is_empty());
    assert!(guardrail_entailment_query("urn:unrelated:task", &rules)
        .unwrap()
        .is_empty());
    assert!(guardrail_entailment_query("", &rules).is_err());
    for code in ["GUARDRAIL_SCOPE_TOO_LARGE", "MALFORMED_GUARDRAIL_RULE"] {
        assert!(eg_capabilities::error_routing::method_allows_error(
            "GuardrailEntailment",
            code
        ));
    }
}
