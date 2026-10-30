use super::*;
use crate::decision::statistical::QuestionKind;

fn step(state: &str) -> ProgramStep {
    ProgramStep {
        state: state.into(),
        action: format!("act:{state}"),
        next: format!("next:{state}"),
    }
}

fn program() -> CompiledProgram {
    CompiledProgram {
        task_signature: "sig:tenant:sha256:abcd".into(),
        steps: BoundedVec::new(vec![step("load"), step("query"), step("store")]).unwrap(),
        parameters: BoundedVec::new(vec!["source".into(), "limit".into()]).unwrap(),
        source_run_id: "run:tenant:a".into(),
    }
}

// spec: EG-DECISION-ENGINE-R135.1
#[test]
fn program_round_trips() {
    let program = program();
    let wire = serde_json::to_string(&program).unwrap();
    assert_eq!(
        serde_json::from_str::<CompiledProgram>(&wire).unwrap(),
        program
    );

    // The question kind that looks programs up, on its wire name.
    let wire = serde_json::to_string(&QuestionKind::ProgramLookup).unwrap();
    assert_eq!(wire, "\"program_lookup\"");
    assert_eq!(
        serde_json::from_str::<QuestionKind>(&wire).unwrap(),
        QuestionKind::ProgramLookup
    );

    // Unknown fields are refused.
    let mut value: serde_json::Value = serde_json::to_value(program).unwrap();
    value["max_steps"] = serde_json::json!(40);
    assert!(serde_json::from_value::<CompiledProgram>(value).is_err());
    let mut step_value: serde_json::Value = serde_json::to_value(step("load")).unwrap();
    step_value["guard"] = serde_json::json!("always");
    assert!(serde_json::from_value::<ProgramStep>(step_value).is_err());

    // A program with more than 40 steps is refused on the way in.
    assert_eq!(MAX_PROGRAM_STEPS, 40);
    let too_many: Vec<ProgramStep> = (0..=MAX_PROGRAM_STEPS)
        .map(|i| step(&format!("s{i}")))
        .collect();
    assert!(BoundedVec::<ProgramStep, MAX_PROGRAM_STEPS>::new(too_many).is_err());
    let mut value: serde_json::Value = serde_json::to_value(program).unwrap();
    value["steps"] = serde_json::to_value(&too_many).unwrap();
    assert!(serde_json::from_value::<CompiledProgram>(value).is_err());
}
