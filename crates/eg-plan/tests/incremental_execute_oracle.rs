//! Differential oracle: the incremental circuit MUST equal the REAL `eg_plan::execute`
//! recompute (CONCEPT:EG-KG.storage.incremental-matview).
//!
//! This is the acceptance-grade proof the design (`dbsp-incremental-matview-design.md`
//! §5.1) specifies: `materialize(&plan, …)` — i.e. `eg_plan::execute` — on the recompute
//! side, NOT the circuit's own `recompute()`. An `Incremental`-mode matview and its
//! `Recompute` fallback must be interchangeable (a view flips modes on a reseed or a
//! CDC-ring-lag fallback), so `Circuit::current()` (fed the mutation as a CDC delta) must
//! be row-equal to `execute` over the SAME live graph after EVERY mutation. This is the
//! oracle that catches a divergence between the circuit's per-row predicate and the real
//! plan semantics (e.g. `Scan` matching `node_type` when `exec::scan_label` matches only
//! `type`) — invisible to the circuit-vs-circuit oracle, which shares `row_passes`.
//!
//! Comparison convention: `GraphView::node_properties` is a `HashMap`, so `exec`'s
//! membership order is nondeterministic — an unranked result is a SET. So both sides are
//! normalized (sorted by `(id, score)`) before comparison. The WINDOW result IS ordered
//! (ascending bucket start on both sides), and a trailing `Limit` truncates the SAME
//! deterministic order on both sides, so the surviving sets still match under the
//! normalized compare. Membership plans carry NO `Limit` here (an unranked `Limit` picks a
//! nondeterministic `k` in `exec` — its determinism is proved separately in
//! `incremental_oracle.rs`); window plans may.

#![cfg(feature = "query")]

use std::collections::BTreeMap;

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphCore;
use eg_plan::{execute, Circuit, Delta, Op, Plan, PlanCtx, Pred, RowSet, ZRow};
use eg_types::wire::TimeAxis;
use proptest::prelude::*;
use serde_json::{json, Map, Value};

type Node = Map<String, Value>;

fn blob(v: &Node) -> Vec<u8> {
    rmp_serde::to_vec_named(v).unwrap()
}

/// Normalize a RowSet to a stable, order-insensitive `(id, score-bits)` multiset.
fn norm(rs: &RowSet) -> Vec<(String, Option<u32>)> {
    let mut v: Vec<(String, Option<u32>)> = rs
        .rows()
        .iter()
        .map(|r| (r.id.clone(), r.score.map(f32::to_bits)))
        .collect();
    v.sort();
    v
}

/// A generated mutation against the graph.
#[derive(Clone, Debug)]
enum Mutation {
    Upsert { id: String, node: NodeSpec },
    Remove { id: String },
}

#[derive(Clone, Debug)]
struct NodeSpec {
    kind: String,
    year: i64,
    value: i64,
    valid_from: i64,
    valid_until: i64,
}

impl NodeSpec {
    fn to_props(&self) -> Node {
        json!({
            "type": self.kind,
            "year": self.year,
            "value": self.value,
            "valid_from": self.valid_from,
            "valid_until": self.valid_until,
        })
        .as_object()
        .unwrap()
        .clone()
    }
}

/// Apply a mutation to the model + real graph, returning the CDC-shaped delta the circuit
/// consumes. Drives BOTH the circuit's model AND a real `GraphCore` execute runs over.
fn apply(model: &mut BTreeMap<String, Node>, core: &GraphCore, m: &Mutation) -> Delta {
    match m {
        Mutation::Upsert { id, node } => {
            let after = node.to_props();
            let mut rows = Vec::new();
            if let Some(before) = model.get(id) {
                rows.push(ZRow::retract(id.clone(), before.clone()));
            }
            rows.push(ZRow::insert(id.clone(), after.clone()));
            core.add_node(id.clone(), blob(&after));
            model.insert(id.clone(), after);
            Delta::from(rows)
        }
        Mutation::Remove { id } => {
            if let Some(before) = model.remove(id) {
                core.remove_node(id.clone());
                Delta::from(vec![ZRow::retract(id.clone(), before)])
            } else {
                Delta::new()
            }
        }
    }
}

fn arb_node() -> impl Strategy<Value = NodeSpec> {
    (
        prop::sample::select(vec!["Doc", "Note", "Other"]),
        1990i64..2010,
        -20i64..20,
        0i64..30,
        20i64..60,
    )
        .prop_map(|(kind, year, value, vf, span)| NodeSpec {
            kind: kind.to_string(),
            year,
            value,
            valid_from: vf,
            valid_until: vf + span,
        })
}

fn arb_mutation() -> impl Strategy<Value = Mutation> {
    let ids = || prop::sample::select(vec!["n0", "n1", "n2", "n3", "n4", "n5"]);
    prop_oneof![
        3 => (ids(), arb_node()).prop_map(|(id, node)| Mutation::Upsert { id: id.to_string(), node }),
        1 => ids().prop_map(|id| Mutation::Remove { id: id.to_string() }),
    ]
}

/// Membership plans (Scan/Filter/AsOf) — NO trailing Limit (see the module docs).
fn arb_membership_plan() -> impl Strategy<Value = Plan> {
    let shapes: Vec<Vec<Op>> = vec![
        vec![Op::Scan {
            label: "Doc".into(),
        }],
        vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2000.0,
                }],
            },
        ],
        vec![
            Op::Scan {
                label: "Note".into(),
            },
            Op::Filter {
                preds: vec![Pred::LtNum {
                    prop: "year".into(),
                    n: 2003.0,
                }],
            },
        ],
        vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::AsOf {
                ts: 20.0,
                axis: TimeAxis::Valid,
            },
        ],
        vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 1995.0,
                }],
            },
            Op::AsOf {
                ts: 25.0,
                axis: TimeAxis::Valid,
            },
        ],
    ];
    prop::sample::select(shapes).prop_map(Plan::new)
}

/// A persistent node that is NEVER a Scan target (type "Schema") but carries every filtered
/// field, so `exec`'s DataFusion schema-on-read always has the `year` column (an EMPTY
/// `nodes` table infers no columns and errors "No field named year"). It is added to BOTH
/// sides identically, so it only affects a `Filter`/`AsOf` SOURCE-on-empty result equally —
/// parity is preserved, and the Scan set can still empty (exercising the quirk).
fn schema_keeper() -> (String, Node) {
    let props = json!({
        "type": "Schema",
        "year": 2000,
        "value": 0,
        "valid_from": 0,
        "valid_until": 1_000_000,
    })
    .as_object()
    .unwrap()
    .clone();
    ("__schema_keeper__".to_string(), props)
}

/// Run one plan against a churn stream, asserting `Circuit::current()` equals the real
/// `execute` recompute after every mutation.
fn churn_matches_execute(plan: &Plan, mutations: &[Mutation]) -> Result<(), TestCaseError> {
    let mut circuit = Circuit::compile(plan).expect("plan is in the supported set");
    let mut model: BTreeMap<String, Node> = BTreeMap::new();
    let core = GraphCore::new();
    let semantic = SemanticStore::new();

    // Keep the SQL schema stable (see `schema_keeper`).
    let (keeper_id, keeper_props) = schema_keeper();
    core.add_node(keeper_id.clone(), blob(&keeper_props));
    circuit.apply(&Delta::from(vec![ZRow::insert(
        keeper_id.clone(),
        keeper_props.clone(),
    )]));
    model.insert(keeper_id, keeper_props);

    for m in mutations {
        let delta = apply(&mut model, &core, m);
        circuit.apply(&delta);

        let view = core.analysis_snapshot();
        let ctx = PlanCtx::new(&view, &semantic);
        let recompute = execute(plan, &ctx).expect("execute recompute");
        let incremental = circuit.current();
        prop_assert_eq!(
            norm(&incremental),
            norm(&recompute),
            "incremental diverged from execute after {:?}\n  plan {:?}",
            m,
            plan.ops
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Membership (Scan/Filter/AsOf) incremental == real `execute` recompute, every step.
    #[test]
    fn membership_matches_execute(
        plan in arb_membership_plan(),
        mutations in prop::collection::vec(arb_mutation(), 1..24),
    ) {
        churn_matches_execute(&plan, &mutations)?;
    }
}

/// A concrete regression for the Scan-over-matches bug: an incremental `Scan{Doc}` must
/// select the SAME nodes as `execute` — only those with `type == "Doc"`, never a
/// `node_type: Doc` node. (Deterministic, so it pins the exact fix without proptest.)
#[test]
fn scan_type_only_matches_execute() {
    let plan = Plan::new(vec![Op::Scan {
        label: "Doc".into(),
    }]);
    let mut circuit = Circuit::compile(&plan).unwrap();
    let core = GraphCore::new();
    let semantic = SemanticStore::new();

    for (id, v) in [
        ("a", json!({"type": "Doc"})),
        ("b", json!({"node_type": "Doc"})), // must NOT be selected
        ("c", json!({"label": "Doc"})),     // must NOT be selected
        ("d", json!({"type": "Other"})),
    ] {
        let props = v.as_object().unwrap().clone();
        core.add_node(id.into(), blob(&props));
        circuit.apply(&Delta::from(vec![ZRow::insert(id, props)]));
    }

    let view = core.analysis_snapshot();
    let ctx = PlanCtx::new(&view, &semantic);
    let recompute = execute(&plan, &ctx).unwrap();
    assert_eq!(norm(&circuit.current()), norm(&recompute));
    assert_eq!(circuit.current().ids(), vec!["a"]);
}

// ── window plans: only under `timeseries` (exec aggregates; otherwise it passes
//    WindowAgg through and the circuit falls it back — proven by compile in the
//    circuit-vs-circuit oracle). ──────────────────────────────────────────────

#[cfg(feature = "timeseries")]
mod window {
    use super::*;

    /// Window plans (Scan[/Filter]→WindowAgg[→Limit]) — the aggregate + an optional Limit
    /// (both sides truncate the SAME ascending-start order).
    fn arb_window_plan() -> impl Strategy<Value = Plan> {
        let shapes: Vec<Vec<Op>> = vec![
            vec![
                Op::Scan {
                    label: "Doc".into(),
                },
                Op::WindowAgg {
                    secs: 10.0,
                    agg: "sum".into(),
                },
            ],
            vec![
                Op::Scan {
                    label: "Doc".into(),
                },
                Op::WindowAgg {
                    secs: 8.0,
                    agg: "mean".into(),
                },
            ],
            vec![
                Op::Scan {
                    label: "Doc".into(),
                },
                Op::WindowAgg {
                    secs: 15.0,
                    agg: "count".into(),
                },
            ],
            // Scan → WindowAgg → Limit (both sides truncate the SAME ascending-start order).
            vec![
                Op::Scan {
                    label: "Doc".into(),
                },
                Op::WindowAgg {
                    secs: 10.0,
                    agg: "sum".into(),
                },
                Op::Limit { k: 2 },
            ],
        ];
        prop::sample::select(shapes).prop_map(Plan::new)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        /// WindowAgg incremental == real `exec::window_aggregate` recompute, every step.
        #[test]
        fn window_matches_execute(
            plan in arb_window_plan(),
            mutations in prop::collection::vec(arb_mutation(), 1..24),
        ) {
            churn_matches_execute(&plan, &mutations)?;
        }
    }
}
