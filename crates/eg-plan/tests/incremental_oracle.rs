//! Differential oracle for the incremental materialized-view circuit
//! (CONCEPT:EG-KG.storage.incremental-matview).
//!
//! The correctness bar for a DBSP-style engine: the INCREMENTALLY-maintained view result
//! MUST equal a full recompute after an ARBITRARY mutation sequence — a wrong circuit
//! silently mis-counts. This is the same discipline the repo already applies twice
//! (`differential_oracle.rs`, `dag_oracle.rs`) and once at the unit level in
//! `src/server/cdc.rs`'s `continuous_count_query_matches_full_rerun`, generalized here to
//! an arbitrary SUPPORTED `Plan`.
//!
//! The strengthening over the two `cdc.rs` unit tests (which assert only the FINAL value):
//! this checks equality AFTER EVERY mutation, so a circuit that diverges then "self-heals"
//! on a later delta still fails.
//!
//! `Circuit::apply` folds signed deltas into maintained state; `Circuit::recompute` does a
//! from-scratch full scan with NO maintained state. They must agree — the maintained
//! membership map / bucket accumulators are the tested surface.

use std::collections::BTreeMap;

use eg_plan::{Circuit, Delta, ZRow};
use eg_types::wire::{Op, Plan, Pred, TimeAxis};
use proptest::prelude::*;
use serde_json::{json, Map, Value};

type Node = Map<String, Value>;
type Model = BTreeMap<String, Node>;

/// A generated mutation against the graph model.
#[derive(Clone, Debug)]
enum Mutation {
    /// Add/replace a node with a fully-specified prop set.
    Upsert { id: String, node: NodeSpec },
    /// Remove a node (no-op if absent).
    Remove { id: String },
}

/// A generated node's property values (the fields the supported ops read).
#[derive(Clone, Debug)]
struct NodeSpec {
    kind: String, // -> "type"
    year: i64,    // -> "year"  (Filter GtNum/LtNum/Eq)
    ts: i64,      // -> "ts"    (WindowAgg bucketing)
    value: i64,   // -> "value" (WindowAgg sum/mean); integers keep sums EXACT
    valid_from: i64,
    valid_until: i64,
}

impl NodeSpec {
    fn to_props(&self) -> Node {
        json!({
            "type": self.kind,
            "year": self.year,
            "ts": self.ts,
            "value": self.value,
            "valid_from": self.valid_from,
            "valid_until": self.valid_until,
        })
        .as_object()
        .unwrap()
        .clone()
    }
}

/// Apply a mutation to the model AND produce the CDC-shaped `Delta` the circuit consumes
/// (`Upsert` of an existing id = the update retract(old)+insert(new) idiom).
fn apply_to_model(model: &mut Model, m: &Mutation) -> Delta {
    match m {
        Mutation::Upsert { id, node } => {
            let after = node.to_props();
            let mut rows = Vec::new();
            if let Some(before) = model.get(id) {
                rows.push(ZRow::retract(id.clone(), before.clone()));
            }
            rows.push(ZRow::insert(id.clone(), after.clone()));
            model.insert(id.clone(), after);
            Delta::from(rows)
        }
        Mutation::Remove { id } => {
            if let Some(before) = model.remove(id) {
                Delta::from(vec![ZRow::retract(id.clone(), before)])
            } else {
                Delta::new()
            }
        }
    }
}

// ── generators ──────────────────────────────────────────────────────────────

fn arb_node() -> impl Strategy<Value = NodeSpec> {
    (
        prop::sample::select(vec!["Doc", "Note", "Other"]),
        1990i64..2010,
        0i64..40,
        -20i64..20,
        0i64..30,
        20i64..60,
    )
        .prop_map(|(kind, year, ts, value, vf, span)| NodeSpec {
            kind: kind.to_string(),
            year,
            ts,
            value,
            valid_from: vf,
            valid_until: vf + span,
        })
}

fn arb_mutation() -> impl Strategy<Value = Mutation> {
    // A small id space so upserts frequently hit existing ids (exercising the
    // retract+insert update path and bucket moves), and removes frequently hit.
    let ids = || prop::sample::select(vec!["n0", "n1", "n2", "n3", "n4", "n5"]);
    prop_oneof![
        3 => (ids(), arb_node()).prop_map(|(id, node)| Mutation::Upsert {
            id: id.to_string(),
            node,
        }),
        1 => ids().prop_map(|id| Mutation::Remove { id: id.to_string() }),
    ]
}

/// One of the supported plan shapes (all compile to a `Circuit`).
fn arb_supported_plan() -> impl Strategy<Value = Plan> {
    prop_oneof![
        // Scan
        Just(vec![Op::Scan {
            label: "Doc".into()
        }]),
        // Scan + Filter(GtNum)
        Just(vec![
            Op::Scan {
                label: "Doc".into()
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2000.0
                }]
            },
        ]),
        // Scan + Filter(Eq + LtNum) + Limit
        Just(vec![
            Op::Scan {
                label: "Note".into()
            },
            Op::Filter {
                preds: vec![
                    Pred::Eq {
                        prop: "type".into(),
                        value: "Note".into()
                    },
                    Pred::LtNum {
                        prop: "year".into(),
                        n: 2005.0
                    },
                ],
            },
            Op::Limit { k: 3 },
        ]),
        // Scan + AsOf + Limit
        Just(vec![
            Op::Scan {
                label: "Doc".into()
            },
            Op::AsOf {
                ts: 40.0,
                axis: TimeAxis::Valid
            },
            Op::Limit { k: 4 },
        ]),
        // Scan + WindowAgg(sum)
        Just(vec![
            Op::Scan {
                label: "Doc".into()
            },
            Op::WindowAgg {
                secs: 10.0,
                agg: "sum".into()
            },
        ]),
        // Scan + Filter + WindowAgg(count) + Limit
        Just(vec![
            Op::Scan {
                label: "Doc".into()
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 1995.0
                }]
            },
            Op::WindowAgg {
                secs: 15.0,
                agg: "count".into()
            },
            Op::Limit { k: 2 },
        ]),
        // Scan + WindowAgg(mean)
        Just(vec![
            Op::Scan {
                label: "Doc".into()
            },
            Op::WindowAgg {
                secs: 8.0,
                agg: "mean".into()
            },
        ]),
    ]
    .prop_map(Plan::new)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(400))]

    /// The mandatory verification: for an arbitrary supported plan and an arbitrary
    /// mutation sequence, the incrementally-maintained result equals a full recompute
    /// after EVERY mutation.
    #[test]
    fn incremental_matches_full_recompute_after_arbitrary_mutations(
        plan in arb_supported_plan(),
        mutations in prop::collection::vec(arb_mutation(), 1..60),
    ) {
        let mut circuit = Circuit::compile(&plan)
            .expect("arb_supported_plan only emits compilable plans");
        let mut model: Model = BTreeMap::new();

        for m in &mutations {
            let delta = apply_to_model(&mut model, m);
            circuit.apply(&delta);

            let incremental = circuit.current();
            let recomputed = circuit.recompute(&model);
            prop_assert_eq!(
                incremental,
                recomputed,
                "diverged after mutation {:?} (plan {:?})",
                m,
                plan.ops
            );
        }
    }
}
