use super::*;

fn task(id: &str, deps: &[&str]) -> TaskPlanTask {
    TaskPlanTask {
        task_id: id.into(),
        prompt: format!("Analyze {id} with cited evidence"),
        depends_on: BoundedVec::new(deps.iter().map(|s| s.to_string()).collect()).unwrap(),
    }
}

fn proposal() -> TaskPlanProposal {
    TaskPlanProposal {
        tasks: BoundedVec::new(vec![
            task("join", &["c", "a"]),
            task("c", &["b"]),
            task("b", &[]),
            task("a", &[]),
        ])
        .unwrap(),
        output_task_id: "join".into(),
        max_concurrency: 2,
        max_steps: 4,
    }
}

// spec: EG-DECISION-ENGINE-R134.111.1
#[test]
fn task_plan_round_trips() {
    let plan = plan_explicit_tasks(&proposal()).unwrap();
    let value = serde_json::to_value(&plan).unwrap();
    assert_eq!(
        serde_json::from_value::<TaskPlan>(value.clone()).unwrap(),
        plan
    );
    let mut unknown = value;
    unknown["authorize"] = serde_json::json!(true);
    assert!(serde_json::from_value::<TaskPlan>(unknown).is_err());
    let mut value = serde_json::to_value(proposal()).unwrap();
    value["tasks"][0]["model"] = serde_json::json!("untrusted");
    assert!(serde_json::from_value::<TaskPlanProposal>(value).is_err());
}

// spec: EG-DECISION-ENGINE-R134.111.2
#[test]
fn plan_respects_dependencies() {
    let original = proposal();
    let plan = plan_explicit_tasks(&original).unwrap();
    assert_eq!(
        plan.tasks
            .iter()
            .map(|t| t.task_id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c", "join"]
    );
    assert_eq!(plan.tasks.as_slice()[2].depends_on.as_slice(), ["b"]);
    assert_eq!(plan.tasks.as_slice()[3].depends_on.as_slice(), ["a", "c"]);
    assert_eq!(
        (plan.total_tasks, plan.max_steps, plan.max_concurrency),
        (4, 4, 2)
    );
    let mut reordered = original.clone();
    reordered.tasks = BoundedVec::new(original.tasks.iter().cloned().rev().collect()).unwrap();
    let other = plan_explicit_tasks(&reordered).unwrap();
    assert_eq!(plan.plan_digest, other.plan_digest);
    assert_ne!(plan.input_digest, other.input_digest);
    let mut changed = original.clone();
    let mut tasks = changed.tasks.as_slice().to_vec();
    tasks[0].prompt.push('!');
    changed.tasks = BoundedVec::new(tasks).unwrap();
    assert_ne!(
        plan.plan_digest,
        plan_explicit_tasks(&changed).unwrap().plan_digest
    );
}

fn refuses(mutator: impl FnOnce(&mut TaskPlanProposal), reason: &str) {
    let mut input = proposal();
    mutator(&mut input);
    assert!(plan_explicit_tasks(&input).unwrap_err().starts_with(reason));
}

// spec: EG-DECISION-ENGINE-R134.111.4
#[test]
fn invalid_graphs_refuse() {
    refuses(|p| p.tasks = BoundedVec::default(), "empty_plan");
    refuses(
        |p| p.tasks = BoundedVec::new(vec![task("a", &[]), task("a", &[])]).unwrap(),
        "duplicate_task_id",
    );
    refuses(
        |p| p.tasks = BoundedVec::new(vec![task("bad id", &[])]).unwrap(),
        "invalid_task_id",
    );
    refuses(
        |p| p.tasks = BoundedVec::new(vec![task("a", &["missing"])]).unwrap(),
        "missing_dependency",
    );
    refuses(
        |p| p.tasks = BoundedVec::new(vec![task("a", &["a"])]).unwrap(),
        "plan_cycle",
    );
    refuses(
        |p| p.tasks = BoundedVec::new(vec![task("a", &["b"]), task("b", &["a"])]).unwrap(),
        "plan_cycle",
    );
    refuses(
        |p| p.tasks = BoundedVec::new(vec![task("a", &["b", "b"]), task("b", &[])]).unwrap(),
        "duplicate_dependency",
    );
    refuses(
        |p| p.output_task_id = "missing".into(),
        "invalid_output_task",
    );
    refuses(|p| p.output_task_id = "c".into(), "disconnected_output");
}

// spec: EG-DECISION-ENGINE-R134.111.4
#[test]
fn work_and_text_bounds_refuse() {
    for value in [0, 3, 65] {
        refuses(|p| p.max_steps = value, "plan_bounds");
    }
    for value in [0, 5, 9] {
        refuses(|p| p.max_concurrency = value, "plan_bounds");
    }
    for prompt in [" ".into(), "x".repeat(MAX_PROMPT_BYTES + 1)] {
        refuses(
            |p| {
                p.tasks = BoundedVec::new(vec![TaskPlanTask {
                    prompt,
                    ..task("join", &[])
                }])
                .unwrap()
            },
            "plan_text_bounds",
        );
    }
    let mut input = proposal();
    input.tasks = BoundedVec::new(
        (0..5)
            .map(|i| TaskPlanTask {
                prompt: "x".repeat(MAX_PROMPT_BYTES),
                ..task(&format!("t{i}"), &[])
            })
            .collect(),
    )
    .unwrap();
    input.max_steps = 5;
    assert!(plan_explicit_tasks(&input)
        .unwrap_err()
        .starts_with("plan_text_bounds"));
    let input = TaskPlanProposal {
        tasks: BoundedVec::new(
            (0..64)
                .map(|i| task(&format!("t{i:02}"), if i == 0 { &[] } else { &["t00"] }))
                .collect(),
        )
        .unwrap(),
        output_task_id: "t63".into(),
        max_concurrency: 8,
        max_steps: 64,
    };
    // All tasks are bounded, but most do not feed the output sink.
    assert!(plan_explicit_tasks(&input)
        .unwrap_err()
        .starts_with("disconnected_output"));
}

// spec: EG-DECISION-ENGINE-R134.111.2
#[test]
fn maximum_bounded_chain_is_valid() {
    let tasks = (0..64)
        .map(|i| TaskPlanTask {
            task_id: format!("t{i:02}"),
            prompt: "Bounded work".to_string(),
            depends_on: BoundedVec::new(if i == 0 {
                vec![]
            } else {
                vec![format!("t{:02}", i - 1)]
            })
            .unwrap(),
        })
        .collect();
    let input = TaskPlanProposal {
        tasks: BoundedVec::new(tasks).unwrap(),
        output_task_id: "t63".into(),
        max_concurrency: 8,
        max_steps: 64,
    };
    let plan = plan_explicit_tasks(&input).unwrap();
    assert_eq!(plan.total_tasks, 64);
    assert_eq!(plan.tasks.as_slice()[63].task_id, "t63");
}

// spec: EG-DECISION-ENGINE-R134.111.4
#[test]
fn deserialization_enforces_collection_bounds() {
    let mut value = serde_json::to_value(proposal()).unwrap();
    value["tasks"] = serde_json::json!((0..65)
        .map(|i| task(&format!("t{i}"), &[]))
        .collect::<Vec<_>>());
    assert!(serde_json::from_value::<TaskPlanProposal>(value).is_err());
}

// spec: EG-DECISION-ENGINE-R134.111.4
#[test]
fn index_refusal_precedence_is_stable() {
    let mut input = proposal();
    input.max_steps = 0;
    input.tasks = BoundedVec::default();
    assert_eq!(
        index(&input).unwrap_err(),
        "empty_plan: at least one task is required"
    );
    input.tasks = BoundedVec::new(vec![task("bad id", &[])]).unwrap();
    assert_eq!(
        index(&input).unwrap_err(),
        "plan_bounds: invalid task or concurrency ceiling"
    );
    input.max_steps = 4;
    let mut invalid = task("bad id", &["missing", "missing"]);
    invalid.prompt = " ".into();
    input.tasks = BoundedVec::new(vec![invalid.clone()]).unwrap();
    assert_eq!(index(&input).unwrap_err(), "invalid_task_id: bad id");
    invalid.task_id = "a".into();
    input.tasks = BoundedVec::new(vec![invalid.clone()]).unwrap();
    assert_eq!(index(&input).unwrap_err(), "plan_text_bounds: a");
    invalid.prompt = "Valid prompt".into();
    input.tasks = BoundedVec::new(vec![task("a", &[]), invalid]).unwrap();
    assert_eq!(index(&input).unwrap_err(), "duplicate_dependency: a");
}

// spec: EG-DECISION-ENGINE-R134.111.4
#[test]
fn index_accepts_exact_total_prompt_byte_limit() {
    let mut input = proposal();
    let mut tasks = input.tasks.as_slice().to_vec();
    for task in &mut tasks {
        task.prompt = "é".repeat(MAX_PROMPT_BYTES / 2);
    }
    input.tasks = BoundedVec::new(tasks).unwrap();
    assert!(index(&input).is_ok());
    let mut tasks = input.tasks.as_slice().to_vec();
    tasks.push(task("extra", &[]));
    input.tasks = BoundedVec::new(tasks).unwrap();
    input.max_steps = 5;
    assert_eq!(index(&input).unwrap_err(), "plan_text_bounds: extra");
}

// spec: EG-DECISION-ENGINE-R134.111.7
#[test]
fn the_canonical_task_plan_feature_schema_is_valid_and_stable() {
    let body = task_plan_feature_schema().checked().unwrap();
    assert_eq!(body.names(), ["task_position"]);
    let digest = crate::decision::statistical::body::encode_body(&body)
        .unwrap()
        .content_digest;
    assert_eq!(
        digest,
        crate::decision::statistical::body::encode_body(&task_plan_feature_schema())
            .unwrap()
            .content_digest
    );
}
