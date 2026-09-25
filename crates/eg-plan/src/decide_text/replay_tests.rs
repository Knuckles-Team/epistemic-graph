use std::collections::BTreeMap;

use eg_types::decision::jobs::DatasetSource;
use eg_types::decision::replay::{EvalMode, ReplayEnvironment, TrialLog};
use eg_types::decision::statistical::TypedValue;
use eg_types::decision::{DecisionPolicyRef, EvalCandidate};

use super::{parse_replay, DecideTextErrorKind, ReplayBindings};

const TEXT: &str = "CANDIDATES $c |> DECIDE USING $policy |> REPLAY WALK FORWARD TRAIN 252 TEST 21 STEP 21 PURGE 5 EMBARGO 5 FROM $t0 TO $t1 BUDGET 1.0";

fn bindings<'a>(
    candidates: &'a BTreeMap<String, EvalCandidate>,
    policies: &'a BTreeMap<String, DecisionPolicyRef>,
    values: &'a BTreeMap<String, TypedValue>,
) -> ReplayBindings<'a> {
    ReplayBindings {
        candidates,
        policies,
        values,
        graph: "tenant-a/replay".into(),
        source: DatasetSource::Logged {
            question_id: "q".into(),
        },
        gold_set_digest: None,
        trials: TrialLog {
            declared: 4,
            searched: 4,
        },
        idempotency_key: "replay-1".into(),
    }
}

fn maps() -> (
    BTreeMap<String, EvalCandidate>,
    BTreeMap<String, DecisionPolicyRef>,
    BTreeMap<String, TypedValue>,
) {
    (
        BTreeMap::from([(
            "c".into(),
            EvalCandidate::DraftArtifact {
                sha256: "digest".into(),
                length: 1,
            },
        )]),
        BTreeMap::from([("policy".into(), DecisionPolicyRef::Default)]),
        BTreeMap::from([
            ("t0".into(), TypedValue::Int(1000)),
            ("t1".into(), TypedValue::Int(2000)),
        ]),
    )
}

#[test]
fn approved_spelling_binds_to_decision_eval_replay() {
    let (candidates, policies, values) = maps();
    let request = parse_replay(TEXT, "tenant-a", bindings(&candidates, &policies, &values))
        .expect("typed replay request");
    assert_eq!(request.tenant_id, "tenant-a");
    assert_eq!(request.window.from_ms, 1000);
    assert_eq!(request.window.to_ms, 2000);
    let EvalMode::Replay { spec } = request.mode else {
        panic!("replay mode")
    };
    assert_eq!(
        (spec.folds.train, spec.folds.test, spec.folds.step),
        (252, 21, 21)
    );
    assert_eq!((spec.folds.purge, spec.folds.embargo), (5, 5));
    assert_eq!(spec.budget.cap.value, 1_000_000_000_000);
    assert_eq!(spec.env, ReplayEnvironment::PolicyIndependent);
    assert_eq!(spec.graph, "tenant-a/replay");
    assert_eq!(spec.trials.searched, 4);
}

#[test]
fn typed_bindings_and_bounds_fail_before_job_submission() {
    let (candidates, policies, mut values) = maps();
    values.insert("t0".into(), TypedValue::Text("1000".into()));
    let error = parse_replay(TEXT, "t", bindings(&candidates, &policies, &values)).unwrap_err();
    assert_eq!(error.kind, DecideTextErrorKind::ParameterType);
    values.insert("t0".into(), TypedValue::Int(1000));
    for text in [
        TEXT.replace("STEP 21", "STEP 20"),
        TEXT.replace("BUDGET 1.0", "BUDGET 0"),
        TEXT.replace("TRAIN 252", "TRAIN 0"),
        TEXT.replace("BUDGET 1.0", "BUDGET 1.1234567890123"),
    ] {
        assert_eq!(
            parse_replay(&text, "t", bindings(&candidates, &policies, &values))
                .unwrap_err()
                .kind,
            DecideTextErrorKind::Syntax,
            "{text}"
        );
    }
}

#[test]
fn unbound_names_and_trailing_clauses_are_refused() {
    let (candidates, policies, values) = maps();
    for text in [
        TEXT.replace("$c", "$unknown"),
        TEXT.replace("$policy", "$unknown"),
        TEXT.replace("$t0", "$unknown"),
    ] {
        assert_eq!(
            parse_replay(&text, "t", bindings(&candidates, &policies, &values))
                .unwrap_err()
                .kind,
            DecideTextErrorKind::UnboundParameter
        );
    }
    let error = parse_replay(
        &format!("{TEXT} |> ASSEMBLE"),
        "t",
        bindings(&candidates, &policies, &values),
    )
    .unwrap_err();
    assert_eq!(error.kind, DecideTextErrorKind::Syntax);
}

#[test]
fn replay_requires_a_bounded_graph_anchor() {
    let (candidates, policies, values) = maps();
    for graph in [String::new(), "g".repeat(257)] {
        let mut bound = bindings(&candidates, &policies, &values);
        bound.graph = graph;
        assert_eq!(
            parse_replay(TEXT, "t", bound).unwrap_err().kind,
            DecideTextErrorKind::ParameterType
        );
    }
}
