use super::*;

// spec: MEDULLA-DECISION-ENGINE-R177.1
#[test]
fn guidance_round_trips() {
    let guidance = UsageGuidance {
        connector: "github".into(),
        job_class: "urn:job:issue-triage".into(),
        conditions: vec!["token has issues:read".into()],
        steps: vec![
            UsageStep {
                tool: "list_issues".into(),
                inputs_from: vec![],
                precedes: vec!["label_issue".into()],
            },
            UsageStep {
                tool: "label_issue".into(),
                inputs_from: vec!["list_issues".into()],
                precedes: vec![],
            },
        ],
        prompts: vec!["urn:prompt:triage".into()],
        cautions: vec!["labels are visible to the reporter".into()],
        graph_revision: 9,
    };
    let value = serde_json::to_value(&guidance).unwrap();
    assert_eq!(
        serde_json::from_value::<UsageGuidance>(value.clone()).unwrap(),
        guidance
    );
    let mut missing = value.clone();
    missing["steps"][0].as_object_mut().unwrap().remove("tool");
    assert!(serde_json::from_value::<UsageGuidance>(missing).is_err());
    let mut blank = value;
    blank["steps"][1]["tool"] = serde_json::json!(" ");
    assert!(serde_json::from_value::<UsageGuidance>(blank).is_err());
    assert_eq!(
        serde_json::to_value(crate::decision::statistical::QuestionKind::UsageGuidance).unwrap(),
        "usage_guidance"
    );
}
