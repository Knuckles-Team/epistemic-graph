//! EH-200: rule-derived capability classes feed library candidates as claims,
//! filter `classification_under`, and pin the rule-set identity.

use super::*;

fn model_profile(h: &Harness, id: &str, supports_tools: bool) {
    let mut draft = test_component_draft(TENANT, id);
    draft.kind = AgentComponentKind::ModelProfile;
    draft.summary = format!("model {id}");
    draft.facts = eg_types::agent_component::AgentComponentFacts::ModelProfile {
        provider: "provider".to_string(),
        model_identity: format!("model:{id}"),
        context_window_tokens: 8_192,
        max_output_tokens: 1_024,
        supports_tools,
        supports_structured_output: false,
        supports_vision: false,
        modalities: Default::default(),
        cost: None,
        latency_declared: None,
        latency_observed_ref: None,
    };
    h.commit(draft, None).unwrap();
}

#[cfg(feature = "owl")]
#[tokio::test]
async fn derived_classes_select_candidates_as_claims_under_a_pinned_rule_set() {
    let h = Harness::new().await;
    model_profile(&h, "model-a-tools", true);
    model_profile(&h, "model-b-plain", false);
    let schema_pin = h
        .publish(
            "schema-models",
            AgentComponentKind::FeatureSchema,
            "model features",
            Some(&schema()),
            None,
        )
        .unwrap();
    let mut request = request(
        &schema_pin,
        None,
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    request.candidates = CandidateSource::AgentLibrary {
        scope: LibraryCandidateScope {
            kinds: BoundedVec::new(vec![AgentComponentKind::ModelProfile]).unwrap(),
            classification_under: Some("eg:profile/tool-calling".to_string()),
        },
    };
    let batch = decide(&h, request).await.unwrap();
    let record = &batch.records.as_slice()[0];
    let FeatureMatrixRef::Inline { candidate_ids, .. } = &record.inputs.feature_matrix else {
        panic!("inline matrix")
    };
    assert_eq!(
        candidate_ids.as_slice(),
        ["model-a-tools".to_string()],
        "only the derived class matches the root"
    );
    assert!(
        record.inputs.classification_rules.is_some(),
        "the rule-set identity is recorded"
    );
    let derived = record
        .premises
        .iter()
        .find(|p| p.fact.starts_with("derived:eg:profile/tool-calling"))
        .expect("the derived class is a named premise");
    assert_eq!(derived.class, eg_types::decision::PremiseClass::Claim);
    assert_eq!(
        record.evidence_class,
        eg_types::decision::EvidenceClass::Claim
    );
}
