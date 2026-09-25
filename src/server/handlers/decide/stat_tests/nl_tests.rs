//! EH-064, served: an utterance is routed over published `NlTemplate`
//! components; with no calibrated head the engine abstains, and an LLM's
//! template proposal is bound only as a CLAIM premise with its producer --
//! never as the engine's choice.

use super::*;
use eg_types::decision::statistical::nl::{
    NlChoiceSource, NlSlot, NlSlotType, NlTarget, NlTemplateBody, NL_TEMPLATE_SCHEMA_VERSION,
};

fn template(utterance: &str) -> NlTemplateBody {
    NlTemplateBody {
        schema_version: NL_TEMPLATE_SCHEMA_VERSION,
        utterances: BoundedVec::new(vec![utterance.to_string()]).unwrap(),
        labels: BoundedVec::default(),
        slots: BoundedVec::new(vec![NlSlot {
            name: "topic".to_string(),
            slot_type: NlSlotType::Text {
                after: "about".to_string(),
            },
            required: true,
        }])
        .unwrap(),
        target: NlTarget::AgentAssemble,
    }
}

fn text(name: &str, value: &str) -> TypedParam {
    TypedParam {
        name: name.to_string(),
        value: TypedValue::Text(value.to_string()),
    }
}

fn nl_request(schema: &ComponentDependency, params: Vec<TypedParam>) -> DecideRequest {
    DecideRequest {
        tenant_id: TENANT.to_string(),
        question: StatisticalQuestion {
            question_id: "nl.route".to_string(),
            kind: QuestionKind::TemplateChoice,
            safety: QuestionSafety::Ordinary,
        },
        candidates: CandidateSource::AgentLibrary {
            scope: LibraryCandidateScope {
                kinds: BoundedVec::new(vec![AgentComponentKind::NlTemplate]).unwrap(),
                classification_under: None,
            },
        },
        feature_schema: schema.clone(),
        head: None,
        policy: DecisionPolicyRef::Default,
        params: BoundedVec::new(params).unwrap(),
        max_records: None,
        belief_as_of: BoundedVec::default(),
    }
}

#[tokio::test]
async fn an_utterance_routes_over_templates_and_an_llm_proposal_is_only_a_claim() {
    let h = Harness::new().await;
    for (id, utterance) in [
        ("nl-build-agent", "build an agent about {topic}"),
        ("nl-find-owner", "who owns the service about {topic}"),
    ] {
        h.publish(
            id,
            AgentComponentKind::NlTemplate,
            utterance,
            Some(&template(utterance)),
            None,
        )
        .unwrap();
    }
    let schema = FeatureSchemaBody {
        schema_version: FEATURE_SCHEMA_VERSION,
        features: BoundedVec::new(vec![FeatureSpec {
            name: "utterance_match".to_string(),
            kind: FeatureKind::TextBm25 {
                key: "nl.utterances".to_string(),
                param: "utterance".to_string(),
            },
            missing: MissingValue::Abstain,
        }])
        .unwrap(),
    };
    let schema_pin = h
        .publish(
            "schema-nl",
            AgentComponentKind::FeatureSchema,
            "template features",
            Some(&schema),
            None,
        )
        .unwrap();
    let utterance = text("utterance", "build an agent about billing");

    let plain = decide(&h, nl_request(&schema_pin, vec![utterance.clone()]))
        .await
        .unwrap();
    let record = &plain.records.as_slice()[0];
    assert!(matches!(
        record.outcome,
        StatisticalOutcome::Abstained { .. }
    ));
    assert!(
        record.nl_binding.is_none(),
        "no head, no proposal: nothing bound"
    );

    let params = vec![
        text("llm_producer", "au-nl-planner"),
        text("llm_prompt_digest", "sha256:prompt"),
        text("llm_proposal_template", "nl-build-agent"),
        utterance,
    ];
    let proposed = decide(&h, nl_request(&schema_pin, params)).await.unwrap();
    let record = &proposed.records.as_slice()[0];
    let binding = record.nl_binding.as_ref().expect("the proposal is bound");
    assert_eq!(binding.template.component_id, "nl-build-agent");
    assert!(matches!(binding.source, NlChoiceSource::LlmProposal { .. }));
    assert_eq!(binding.params.as_slice()[0].name, "topic");
    assert!(record
        .premises
        .iter()
        .any(|p| p.fact == "llm_template_proposal"
            && p.class == eg_types::decision::PremiseClass::Claim));
    assert_eq!(record.evidence_class, EvidenceClass::Claim);
    assert!(matches!(
        record.outcome,
        StatisticalOutcome::Abstained { .. }
    ));
}
