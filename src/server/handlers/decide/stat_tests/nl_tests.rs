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

// spec: EG-DECISION-ENGINE-R098, EG-DECISION-ENGINE-R062
// spec: EG-DECISION-ENGINE-R028
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

/// EG-DECISION-ENGINE-R098: the Decide ladder excludes generative free-text
/// models by design. A bound `NlBinding`'s typed slot values are never
/// free-form generated text: a `Text` slot's value is always a verbatim
/// substring of the caller's own utterance, never a string invented by the
/// engine, and an LLM proposal may only pick among already-published, fixed
/// templates -- it can never supply its own response text, and an id outside
/// the published set is refused rather than bound.
// spec: EG-DECISION-ENGINE-R098
#[tokio::test]
async fn nl_binding_text_slots_are_verbatim_substrings_never_generated_text() {
    let h = Harness::new().await;
    h.publish(
        "nl-build-agent",
        AgentComponentKind::NlTemplate,
        "build an agent about {topic}",
        Some(&template("build an agent about {topic}")),
        None,
    )
    .unwrap();
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
            "schema-nl-verbatim",
            AgentComponentKind::FeatureSchema,
            "template features",
            Some(&schema),
            None,
        )
        .unwrap();

    // The LLM proposal path names a template id and nothing else -- it has
    // no field through which to supply response text of its own.
    let utterance_text = "build an agent about quarterly billing reconciliation";
    let params = vec![
        text("llm_producer", "au-nl-planner"),
        text("llm_prompt_digest", "sha256:prompt"),
        text("llm_proposal_template", "nl-build-agent"),
        text("utterance", utterance_text),
    ];
    let proposed = decide(&h, nl_request(&schema_pin, params)).await.unwrap();
    let record = &proposed.records.as_slice()[0];
    let binding = record.nl_binding.as_ref().expect("the proposal is bound");
    let topic = binding
        .params
        .iter()
        .find(|p| p.name == "topic")
        .expect("topic slot filled");
    let TypedValue::Text(bound_text) = &topic.value else {
        panic!("topic is a Text-typed slot value");
    };
    // The bound value is a byte-identical substring of the caller's own
    // utterance: it was extracted, never generated.
    assert!(
        utterance_text.contains(bound_text.as_str()),
        "bound text {bound_text:?} must be a verbatim substring of the utterance"
    );

    // An id outside the published, fixed template set is refused, not bound
    // to engine-invented content: the LLM can only select among what was
    // already published, never author the response itself.
    let bad_params = vec![
        text("llm_producer", "au-nl-planner"),
        text("llm_prompt_digest", "sha256:prompt"),
        text("llm_proposal_template", "nl-does-not-exist"),
        text("utterance", utterance_text),
    ];
    let refused = decide(&h, nl_request(&schema_pin, bad_params)).await;
    assert!(
        refused.is_err(),
        "a proposal naming an unpublished template id must be refused"
    );
}
