use super::*;

fn task(id: &str, deps: &[&str]) -> TaskPlanTask {
    TaskPlanTask {
        task_id: id.into(),
        prompt: format!("Analyze {id} with cited evidence"),
        depends_on: BoundedVec::new(deps.iter().map(|s| s.to_string()).collect()).unwrap(),
        call: None,
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

// spec: MEDULLA-DECISION-ENGINE-R134
// spec: MEDULLA-DECISION-ENGINE-R134.111.1
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

// spec: MEDULLA-DECISION-ENGINE-R134.111.2
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

// spec: MEDULLA-DECISION-ENGINE-R134.111.4
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

// spec: MEDULLA-DECISION-ENGINE-R134.111.4
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

// spec: MEDULLA-DECISION-ENGINE-R134.111.2
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
            call: None,
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

// spec: MEDULLA-DECISION-ENGINE-R134.111.4
#[test]
fn deserialization_enforces_collection_bounds() {
    let mut value = serde_json::to_value(proposal()).unwrap();
    value["tasks"] = serde_json::json!((0..65)
        .map(|i| task(&format!("t{i}"), &[]))
        .collect::<Vec<_>>());
    assert!(serde_json::from_value::<TaskPlanProposal>(value).is_err());
}

// spec: MEDULLA-DECISION-ENGINE-R134.111.4
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

// spec: MEDULLA-DECISION-ENGINE-R134.111.4
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

// spec: MEDULLA-DECISION-ENGINE-R134.111.7
#[test]
fn the_canonical_task_plan_feature_schema_is_valid_and_stable() {
    let body = task_plan_feature_schema().checked().unwrap();
    assert_eq!(
        body.names(),
        ["task_position", "task_call", "task_call_bindings"]
    );
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

fn tool(id: &str, deps: &[&str], arguments: serde_json::Value) -> TaskPlanTask {
    TaskPlanTask {
        call: Some(TaskPlanCall {
            server: "github".into(),
            tool: "get_issue".into(),
            arguments,
        }),
        ..task(id, deps)
    }
}

fn mixed(arguments: serde_json::Value) -> TaskPlanProposal {
    TaskPlanProposal {
        tasks: BoundedVec::new(vec![task("a", &[]), tool("b", &["a"], arguments)]).unwrap(),
        output_task_id: "b".into(),
        max_concurrency: 1,
        max_steps: 2,
    }
}

fn reference(text: &str) -> serde_json::Value {
    serde_json::json!({ "$ref": text })
}

// spec: MEDULLA-DECISION-ENGINE-R134.111.8
#[test]
fn tool_step_round_trips_with_literals_and_bindings() {
    let arguments = serde_json::json!({
        "repo": "medulla",
        "limit": 3,
        "issue": reference("steps.a.output.items.0.number"),
        "labels": ["bug", reference("steps.a.output.label")],
    });
    let proposal = mixed(arguments.clone());
    let wire = serde_json::to_value(&proposal).unwrap();
    assert!(
        wire["tasks"][0].get("call").is_none(),
        "prompt step keeps its form"
    );
    assert_eq!(
        serde_json::from_value::<TaskPlanProposal>(wire).unwrap(),
        proposal
    );
    let plan = plan_explicit_tasks(&proposal).unwrap();
    let call = plan.tasks.as_slice()[1].call.as_ref().unwrap();
    assert_eq!(call.arguments, arguments);
    assert_eq!(
        serde_json::from_value::<TaskPlan>(serde_json::to_value(&plan).unwrap()).unwrap(),
        plan
    );
    assert!(plan.tasks.as_slice()[0].call.is_none());
    let bindings = task_bindings(&plan.tasks.as_slice()[1]);
    assert_eq!(
        bindings
            .iter()
            .map(|b| (b.pointer.as_str(), b.source_task.as_str()))
            .collect::<Vec<_>>(),
        [("/issue", "a"), ("/labels/1", "a")]
    );
    let mut unknown = serde_json::to_value(mixed(arguments)).unwrap();
    unknown["tasks"][1]["call"]["credential"] = serde_json::json!("secret");
    assert!(serde_json::from_value::<TaskPlanProposal>(unknown).is_err());
    let extra = serde_json::json!({ "x": { "$ref": "steps.a.output", "default": 1 } });
    assert!(plan_explicit_tasks(&mixed(extra))
        .unwrap_err()
        .starts_with("binding_bad_path"));
}

// spec: MEDULLA-DECISION-ENGINE-R134.111.9
#[test]
fn binding_canonicalization_is_order_independent() {
    let first: serde_json::Value = serde_json::from_str(
        r#"{"b": 1, "a": {"y": [1, 2], "x": {"$ref": " steps.a.output.items.007 "}}}"#,
    )
    .unwrap();
    let second: serde_json::Value = serde_json::from_str(
        r#"{ "a" : { "x" : { "$ref" : "steps.a.output.items.7" }, "y" : [1,2] }, "b" : 1 }"#,
    )
    .unwrap();
    let one = plan_explicit_tasks(&mixed(first)).unwrap();
    let two = plan_explicit_tasks(&mixed(second)).unwrap();
    assert_eq!(one.plan_digest, two.plan_digest);
    assert_eq!(
        one.tasks.as_slice()[1].call.as_ref().unwrap().arguments["a"]["x"],
        reference("steps.a.output.items.7")
    );
    // A binding never adds an edge: the referenced task is still only `a`.
    assert_eq!(one.tasks.as_slice()[1].depends_on.as_slice(), ["a"]);
}

fn bound_arguments(bindings: usize, padding: usize) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for i in 0..bindings {
        map.insert(format!("k{i:02}"), reference("steps.a.output"));
    }
    map.insert("pad".into(), serde_json::Value::String("x".repeat(padding)));
    serde_json::Value::Object(map)
}

fn argument_bytes(arguments: &serde_json::Value) -> usize {
    serde_json::to_vec(arguments).unwrap().len()
}

// spec: MEDULLA-DECISION-ENGINE-R134.111.9
#[test]
fn binding_bounds() {
    assert!(plan_explicit_tasks(&mixed(bound_arguments(MAX_BINDINGS_PER_CALL, 0))).is_ok());
    assert!(
        plan_explicit_tasks(&mixed(bound_arguments(MAX_BINDINGS_PER_CALL + 1, 0)))
            .unwrap_err()
            .starts_with("call_bounds")
    );
    let base = argument_bytes(&bound_arguments(0, 0));
    let exact = bound_arguments(0, MAX_CALL_ARGUMENT_BYTES - base);
    assert_eq!(argument_bytes(&exact), MAX_CALL_ARGUMENT_BYTES);
    assert!(plan_explicit_tasks(&mixed(exact)).is_ok());
    let over = bound_arguments(0, MAX_CALL_ARGUMENT_BYTES - base + 1);
    assert!(plan_explicit_tasks(&mixed(over))
        .unwrap_err()
        .starts_with("call_bounds"));
    let path = |n: usize| {
        let segments = vec!["k"; n].join(".");
        serde_json::json!({ "x": reference(&format!("steps.a.output.{segments}")) })
    };
    assert!(plan_explicit_tasks(&mixed(path(MAX_BINDING_PATH_SEGMENTS))).is_ok());
    assert!(
        plan_explicit_tasks(&mixed(path(MAX_BINDING_PATH_SEGMENTS + 1)))
            .unwrap_err()
            .starts_with("binding_bad_path")
    );
    let long = |len: usize| {
        let key = "k".repeat(len - "steps.a.output.".len());
        serde_json::json!({ "x": reference(&format!("steps.a.output.{key}")) })
    };
    assert!(plan_explicit_tasks(&mixed(long(MAX_BINDING_PATH_BYTES))).is_ok());
    assert!(
        plan_explicit_tasks(&mixed(long(MAX_BINDING_PATH_BYTES + 1)))
            .unwrap_err()
            .starts_with("binding_bad_path")
    );
    let tools = TaskPlanProposal {
        tasks: BoundedVec::new(
            (0..MAX_TOOL_STEPS)
                .map(|i| {
                    let deps: Vec<String> = (i > 0)
                        .then(|| format!("t{:02}", i - 1))
                        .into_iter()
                        .collect();
                    let deps: Vec<&str> = deps.iter().map(String::as_str).collect();
                    tool(&format!("t{i:02}"), &deps, serde_json::json!({}))
                })
                .collect(),
        )
        .unwrap(),
        output_task_id: "t63".into(),
        max_concurrency: 8,
        max_steps: 64,
    };
    assert_eq!(plan_explicit_tasks(&tools).unwrap().total_tasks, 64);
    let mut over_total = mixed(bound_arguments(0, MAX_CALL_ARGUMENT_BYTES - base));
    let mut tasks = over_total.tasks.as_slice().to_vec();
    for task in &mut tasks {
        task.prompt = "x".repeat(MAX_PROMPT_BYTES);
    }
    for i in 0..2 {
        tasks.push(TaskPlanTask {
            prompt: "x".repeat(MAX_PROMPT_BYTES),
            ..task(&format!("p{i}"), &[])
        });
    }
    tasks[1].depends_on = BoundedVec::new(vec!["a".into(), "p0".into(), "p1".into()]).unwrap();
    over_total.tasks = BoundedVec::new(tasks).unwrap();
    over_total.max_steps = 4;
    // Four full prompts fit; the call text pushes the plan past 64 KiB.
    assert!(plan_explicit_tasks(&over_total)
        .unwrap_err()
        .starts_with("plan_text_bounds"));
}

fn binding_refusal(proposal: TaskPlanProposal) -> String {
    plan_explicit_tasks(&proposal).unwrap_err()
}

// spec: MEDULLA-DECISION-ENGINE-R134.111.10
#[test]
fn refuses_each_binding_defect() {
    let cases = [
        (
            serde_json::json!({ "x": reference("steps.zz.output") }),
            "binding_unknown_task: task=b pointer=/x",
        ),
        (
            serde_json::json!({ "n": { "x": reference("steps.a.output.a b") } }),
            "binding_bad_path: task=b pointer=/n/x",
        ),
        (
            serde_json::json!({ "x": [reference("steps.b.output")] }),
            "binding_cycle: task=b pointer=/x/0",
        ),
    ];
    for (arguments, expected) in cases {
        assert_eq!(binding_refusal(mixed(arguments)), expected);
    }
    let mut not_dependency = mixed(serde_json::json!({ "x": reference("steps.c.output") }));
    let mut tasks = not_dependency.tasks.as_slice().to_vec();
    tasks.push(task("c", &[]));
    tasks[1].depends_on = BoundedVec::new(vec!["a".into()]).unwrap();
    tasks[0].depends_on = BoundedVec::new(vec!["c".into()]).unwrap();
    not_dependency.tasks = BoundedVec::new(tasks).unwrap();
    not_dependency.max_steps = 3;
    assert_eq!(
        binding_refusal(not_dependency),
        "binding_not_dependency: task=b pointer=/x"
    );
    // A reference to a downstream task closes a cycle through `depends_on`.
    let mut downstream = mixed(serde_json::json!({}));
    let mut tasks = downstream.tasks.as_slice().to_vec();
    tasks[0] = tool(
        "a",
        &[],
        serde_json::json!({ "y": reference("steps.b.output") }),
    );
    downstream.tasks = BoundedVec::new(tasks).unwrap();
    assert_eq!(
        binding_refusal(downstream),
        "binding_cycle: task=a pointer=/y"
    );
    for call in [
        TaskPlanCall {
            server: String::new(),
            tool: "t".into(),
            arguments: serde_json::json!({}),
        },
        TaskPlanCall {
            server: "s".into(),
            tool: "bad tool".into(),
            arguments: serde_json::json!({}),
        },
        TaskPlanCall {
            server: "s".into(),
            tool: "t".into(),
            arguments: serde_json::json!([1]),
        },
        TaskPlanCall {
            server: "s".into(),
            tool: "t".into(),
            arguments: reference("steps.a.output"),
        },
    ] {
        let mut input = mixed(serde_json::json!({}));
        let mut tasks = input.tasks.as_slice().to_vec();
        tasks[1].call = Some(call);
        input.tasks = BoundedVec::new(tasks).unwrap();
        assert_eq!(binding_refusal(input), "call_invalid: task=b pointer=");
    }
    assert_eq!(
        binding_refusal(mixed(bound_arguments(MAX_BINDINGS_PER_CALL + 1, 0))),
        "call_bounds: task=b pointer=/k00"
    );
    // A plain dependency cycle still refuses as a plan cycle.
    let mut cyclic = mixed(serde_json::json!({ "x": reference("steps.a.output") }));
    let mut tasks = cyclic.tasks.as_slice().to_vec();
    tasks[0].depends_on = BoundedVec::new(vec!["b".into()]).unwrap();
    cyclic.tasks = BoundedVec::new(tasks).unwrap();
    assert!(binding_refusal(cyclic).starts_with("plan_cycle"));
}

// spec: MEDULLA-DECISION-ENGINE-R134.111.11
#[test]
fn digest_covers_calls_and_bindings() {
    let arguments = serde_json::json!({ "repo": "medulla", "x": reference("steps.a.output.id") });
    let base = plan_explicit_tasks(&mixed(arguments.clone())).unwrap();
    let digest = |edit: &dyn Fn(&mut TaskPlanCall)| {
        let mut input = mixed(arguments.clone());
        let mut tasks = input.tasks.as_slice().to_vec();
        edit(tasks[1].call.as_mut().unwrap());
        input.tasks = BoundedVec::new(tasks).unwrap();
        plan_explicit_tasks(&input).unwrap().plan_digest
    };
    assert_eq!(digest(&|_| {}), base.plan_digest);
    assert_ne!(digest(&|c| c.tool = "list_issues".into()), base.plan_digest);
    assert_ne!(digest(&|c| c.server = "gitlab".into()), base.plan_digest);
    assert_ne!(
        digest(&|c| c.arguments["repo"] = serde_json::json!("other")),
        base.plan_digest
    );
    assert_ne!(
        digest(&|c| c.arguments["x"] = reference("steps.a.output.name")),
        base.plan_digest
    );
    let prompt_only = plan_explicit_tasks(&proposal()).unwrap();
    assert_ne!(prompt_only.policy_digest, "");
    assert_eq!(base.policy_digest, task_plan_policy_digest());
}
