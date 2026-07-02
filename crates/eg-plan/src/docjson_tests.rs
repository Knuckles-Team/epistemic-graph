//! Document/JSON modality executor proofs (CONCEPT:EG-084).
//!
//! A small `Doc` layer of deep JSON documents drives the `Pred::JsonPath` FILTER leg
//! end-to-end through the fused executor:
//!  * `JsonPathOp::Exists`   — deep path existence (`jsonb_path_query` / `@?`);
//!  * `JsonPathOp::Eq`       — deep `->>`-style equality (with numeric→text coercion);
//!  * `JsonPathOp::Contains` — Postgres `@>` JSON containment.
//! Plus a compose proof: a relational `Scan` then a JSONPath `Filter` in ONE plan.

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_types::wire::JsonPathOp;
use serde_json::json;

use crate::algebra::{Op, Plan, Pred};
use crate::exec::PlanCtx;
use crate::PlanExt;

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// A layer of `Doc` nodes with nested `meta` objects + `tags` arrays.
fn docs() -> GraphView {
    let core = GraphCore::new();
    core.add_node(
        "d1".into(),
        blob(json!({"type": "Doc", "meta": {"lang": "rust", "year": 2024}, "tags": ["a", "b"]})),
    );
    core.add_node(
        "d2".into(),
        blob(json!({"type": "Doc", "meta": {"lang": "go", "year": 2024}, "tags": ["b", "c"]})),
    );
    core.add_node(
        "d3".into(),
        blob(json!({"type": "Doc", "meta": {"lang": "rust", "year": 2025}})),
    );
    core.analysis_snapshot()
}

fn run(plan: &Plan, view: &GraphView) -> Vec<String> {
    let sem = SemanticStore::new();
    let c = PlanCtx::new(view, &sem);
    let mut ids = plan.execute(&c).unwrap().ids();
    ids.sort();
    ids
}

/// CONCEPT:EG-084 — deep `->>`-style equality on a nested path (`$.meta.lang = 'rust'`).
#[test]
fn eg084_jsonpath_eq_deep_filter() {
    let view = docs();
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: vec![Pred::JsonPath {
                path: "$.meta.lang".into(),
                op: JsonPathOp::Eq {
                    value: json!("rust"),
                },
            }],
        },
    ]);
    assert_eq!(run(&plan, &view), vec!["d1", "d3"]);
}

/// CONCEPT:EG-084 — a JSON-string literal coerces to text for a numeric leaf
/// (`$.meta.year ->> = '2024'` matches the numeric `2024`).
#[test]
fn eg084_jsonpath_eq_numeric_text_coercion() {
    let view = docs();
    let plan = Plan::new(vec![Op::Filter {
        preds: vec![Pred::JsonPath {
            path: "$.meta.year".into(),
            op: JsonPathOp::Eq {
                value: json!("2024"),
            },
        }],
    }]);
    assert_eq!(run(&plan, &view), vec!["d1", "d2"]);
}

/// CONCEPT:EG-084 — deep existence: only d1/d2 carry a `tags` array.
#[test]
fn eg084_jsonpath_exists_filter() {
    let view = docs();
    let plan = Plan::new(vec![Op::Filter {
        preds: vec![Pred::JsonPath {
            path: "$.tags[*]".into(),
            op: JsonPathOp::Exists,
        }],
    }]);
    assert_eq!(run(&plan, &view), vec!["d1", "d2"]);
}

/// CONCEPT:EG-084 — `@>` containment at the root (`props @> '{"meta":{"lang":"go"}}'`).
#[test]
fn eg084_jsonpath_contains_filter() {
    let view = docs();
    let plan = Plan::new(vec![Op::Filter {
        preds: vec![Pred::JsonPath {
            path: "$".into(),
            op: JsonPathOp::Contains {
                value: json!({"meta": {"lang": "go"}}),
            },
        }],
    }]);
    assert_eq!(run(&plan, &view), vec!["d2"]);
}

/// CONCEPT:EG-084 — array `@>` containment (`$.tags @> '["b"]'`) keeps d1 and d2.
#[test]
fn eg084_jsonpath_array_contains_filter() {
    let view = docs();
    let plan = Plan::new(vec![Op::Filter {
        preds: vec![Pred::JsonPath {
            path: "$.tags".into(),
            op: JsonPathOp::Contains {
                value: json!(["b"]),
            },
        }],
    }]);
    assert_eq!(run(&plan, &view), vec!["d1", "d2"]);
}

/// CONCEPT:EG-084 — a relational pred + a JSONPath pred compose in ONE Filter: the
/// relational leg (DataFusion) AND the per-row JSON leg both apply.
#[test]
fn eg084_jsonpath_composes_with_relational() {
    let view = docs();
    let plan = Plan::new(vec![Op::Filter {
        preds: vec![
            Pred::Eq {
                prop: "type".into(),
                value: "Doc".into(),
            },
            Pred::JsonPath {
                path: "$.meta.lang".into(),
                op: JsonPathOp::Eq {
                    value: json!("rust"),
                },
            },
        ],
    }]);
    assert_eq!(run(&plan, &view), vec!["d1", "d3"]);
}
