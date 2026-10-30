//! Run-trace fixtures against the orchestration shapes (EG-ORCH-VOCAB-R009.16).

use std::collections::BTreeSet;

use eg_shacl::validate_turtle;
use serde_json::Value;

const SHAPES: &str = include_str!("../ontology/orchestration-v1.shapes.ttl");
const FIXTURES: &str = include_str!("fixtures/run_traces.json");

const COUNTERS: [&str; 6] = [
    "peakContextTokens",
    "contextBudgetTokens",
    "elisionCount",
    "summarizationCount",
    "turnCount",
    "toolCallCount",
];

fn traces() -> Vec<Value> {
    let document: Value = serde_json::from_str(FIXTURES).expect("the fixtures parse");
    document["traces"].as_array().expect("a trace list").clone()
}

/// One trace as Turtle: the harness properties and the three links.
fn turtle(trace: &Value) -> String {
    let text = |key: &str| {
        trace[key]
            .as_str()
            .unwrap_or_else(|| panic!("{key} is text"))
    };
    let mut out = String::from(
        "@prefix : <http://knuckles.team/kg#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n",
    );
    out.push_str(&format!(
        ":{} a :RunTrace ;\n  :status \"{}\" ;\n  :terminationReason \"{}\" ;\n",
        text("id"),
        text("status"),
        text("terminationReason")
    ));
    for counter in COUNTERS {
        out.push_str(&format!("  :{counter} {} ;\n", trace[counter]));
    }
    out.push_str(&format!(
        "  :costUsd \"{}\"^^xsd:decimal ;\n  :runModel :{} ;\n  :runTaskClass :{} ;\n  :appliedHarnessConfig :{} .\n",
        text("costUsd"),
        text("runModel"),
        text("runTaskClass"),
        text("appliedHarnessConfig")
    ));
    out
}

// spec: EG-ORCH-VOCAB-R009.16
#[test]
fn fixtures_pass_all_run_trace_shapes() {
    let traces = traces();
    assert_eq!(
        traces.len(),
        6,
        "five reasons plus one over-budget overflow"
    );
    let reasons: BTreeSet<&str> = traces
        .iter()
        .map(|t| t["terminationReason"].as_str().unwrap())
        .collect();
    assert_eq!(
        reasons,
        BTreeSet::from([
            "final_answer",
            "context_overflow",
            "stuck",
            "step_budget",
            "error"
        ]),
        "every termination reason has a trace"
    );
    let over_budget: Vec<&Value> = traces
        .iter()
        .filter(|t| t["peakContextTokens"].as_i64() > t["contextBudgetTokens"].as_i64())
        .collect();
    assert_eq!(over_budget.len(), 1, "exactly one trace exceeds its budget");
    assert_eq!(over_budget[0]["terminationReason"], "context_overflow");
    for trace in &traces {
        let report = validate_turtle(SHAPES, &turtle(trace)).expect("the trace validates");
        assert!(
            report.conforms,
            "{} conforms to the run-trace shapes: {:?}",
            trace["id"], report.results
        );
    }
    // The shapes are exercised: the over-budget trace with a non-overflow
    // reason, and a trace missing a counter, both fail.
    let mut wrong_reason = over_budget[0].clone();
    wrong_reason["terminationReason"] = Value::from("error");
    assert!(
        !validate_turtle(SHAPES, &turtle(&wrong_reason))
            .unwrap()
            .conforms,
        "an over-budget trace that did not overflow is refused"
    );
    let mut negative = traces[0].clone();
    negative["turnCount"] = Value::from(-1);
    assert!(
        !validate_turtle(SHAPES, &turtle(&negative))
            .unwrap()
            .conforms,
        "a negative counter is refused"
    );
}
