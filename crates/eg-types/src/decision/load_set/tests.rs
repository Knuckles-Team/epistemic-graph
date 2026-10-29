use super::*;
use crate::decision::{PremiseClass, PremiseProvenance};

// spec: MEDULLA-DECISION-ENGINE-R176.1
#[test]
fn load_set_round_trips() {
    let question: LoadSetQuestion =
        serde_json::from_value(serde_json::json!({ "job_text": "triage github issues" })).unwrap();
    assert_eq!((question.max_tools, question.max_agents), (32, 4));
    let question = LoadSetQuestion {
        job_embedding: vec![0.25, -0.5],
        required_capabilities: vec!["urn:cap:issues.read".into()],
        max_tools: 128,
        ..question
    };
    let value = serde_json::to_value(&question).unwrap();
    assert_eq!(
        serde_json::from_value::<LoadSetQuestion>(value.clone()).unwrap(),
        question
    );
    let mut over = value;
    over["max_tools"] = serde_json::json!(129);
    assert!(serde_json::from_value::<LoadSetQuestion>(over).is_err());
    let answer = LoadSetAnswer {
        tools: vec!["github.list_issues".into()],
        skills: vec!["skill:issue-triage".into()],
        agents: vec![AgentStart {
            agent_iri: "urn:agent:triager".into(),
            prompt_iri: "urn:prompt:triage".into(),
            model_profile: "profile:small".into(),
        }],
        graph_revision: 42,
        premises: vec![PremiseRef {
            subject: "github.list_issues".into(),
            fact: "designated_for=urn:cap:issues.read".into(),
            class: PremiseClass::Claim,
            provenance: PremiseProvenance::Request {
                field: "required_capabilities".into(),
            },
        }],
    };
    let value = serde_json::to_value(&answer).unwrap();
    assert_eq!(
        serde_json::from_value::<LoadSetAnswer>(value).unwrap(),
        answer
    );
    assert_eq!(
        serde_json::to_value(crate::decision::statistical::QuestionKind::LoadSet).unwrap(),
        "load_set"
    );
}
