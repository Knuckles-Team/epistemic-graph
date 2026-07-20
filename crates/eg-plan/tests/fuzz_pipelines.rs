//! Query-pipeline fuzzing (CONCEPT:EG-KG.query.pipeline-fuzz + EG-KG.query.concurrency-chaos-fuzz) — the "Fuzzing / Chaos for Queries"
//! track from the external review. It EXTENDS the `plan_proptest` determinism harness
//! with LONGER, denser random pipelines over MANY more cases, and adds a concurrency
//! CHAOS variant.
//!
//! Two properties, one crate:
//!
//!  * **fuzz (EG-KG.query.pipeline-fuzz)** — a proptest generator emits RANDOM but VALID UQL pipelines up
//!    to 16 stages (vs the determinism test's ≤6) over 512 cases against a denser graph,
//!    asserting the invariants a unified query engine must never violate on ANY valid
//!    input: no panic / no error, a well-formed `RowSet` (UNIQUE ids — the closed-algebra
//!    consistent-state invariant), a trailing `LIMIT k` truncates to ≤ k, and DETERMINISM
//!    (the same plan over the same immutable snapshot yields the byte-identical `RowSet` —
//!    the read side of snapshot isolation).
//!
//!  * **chaos (EG-KG.query.concurrency-chaos-fuzz)** — a concurrency variant: a batch of random pipelines is executed
//!    both SERIALLY (the reference) and CONCURRENTLY across many threads sharing ONE
//!    immutable `GraphView` snapshot + `SemanticStore`. Every concurrent result must equal
//!    its serial reference — proving reads are ISOLATED (a concurrent workload, including
//!    the lazy HNSW-index build races on first search, never perturbs another reader's
//!    result). This is the query-workload analog of a nemesis run: mixed pipelines under
//!    contention, asserting snapshot-isolation holds.
//!
//! No `cargo-fuzz`/libFuzzer target is added: proptest gives structured generation on
//! STABLE (so it rides the normal `cargo test` gate with no nightly toolchain), which is
//! what the CI perf/fuzz gate needs. A libFuzzer `fuzz/` target is the documented
//! follow-up (CONCEPT:EG-KG.query.libfuzzer-parser-deferred) for unstructured byte-level UQL-parser fuzzing.

mod common;

use common::*;
use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_plan::{execute, PlanCtx};
use proptest::prelude::*;
use serde_json::json;
use std::collections::HashSet;

// ── a denser fixture than `build_docs` (7 nodes) so long pipelines have real fan-out ──

/// An `n`-node `Doc` graph: a CITES chain + a +3 skip fan (branching for multi-hop
/// TRAVERSE) + a deterministic 3-d embedding per node (so RANK imposes a real order). A
/// couple of `Tool` distractors exercise the relational FILTER label leg.
fn build_dense(n: usize) -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    for k in 0..n {
        core.add_node(
            format!("d{k}"),
            blob(json!({ "type": "Doc", "year": 2000 + (k as i64 % 30) })),
        );
    }
    core.add_node("t1".into(), blob(json!({ "type": "Tool", "year": 2025 })));
    core.add_node("t2".into(), blob(json!({ "type": "Tool", "year": 2010 })));
    for k in 0..n.saturating_sub(1) {
        core.add_edge(
            format!("d{k}"),
            format!("d{}", k + 1),
            blob(json!({ "relationship": "CITES" })),
        )
        .unwrap();
    }
    for k in 0..n.saturating_sub(3) {
        core.add_edge(
            format!("d{k}"),
            format!("d{}", k + 3),
            blob(json!({ "relationship": "MENTIONS" })),
        )
        .unwrap();
    }
    let mut semantic = SemanticStore::new();
    for k in 0..n {
        let a = (k as f32 * 0.13).sin();
        let b = (k as f32 * 0.13).cos();
        let c = (k as f32 * 0.07).sin();
        semantic.add_embedding(format!("d{k}"), vec![a, b, c]);
    }
    (core.analysis_snapshot(), semantic)
}

// ── the random VALID-UQL pipeline generator (a superset of plan_proptest's, longer) ──

fn label_strategy() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("Doc"), Just("Tool"), Just("NoSuchLabel")]
}

/// One grammatically-legal pipeline stage rendered to UQL text. Every arm is a
/// base-`query` op (no owl/text/timeseries-gated surface), so any concatenation with `|>`
/// is valid UQL under EVERY feature build — the fuzz test is feature-agnostic.
fn stage_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        (0usize..4, 0usize..4).prop_map(|(min, d)| format!(
            "TRAVERSE -[:CITES]->{{{},{}}}",
            min,
            min + d
        )),
        (0usize..4, 0usize..4).prop_map(|(min, d)| format!(
            "TRAVERSE -[:MENTIONS]->{{{},{}}}",
            min,
            min + d
        )),
        (0.0f64..2.0, 0.0f64..2.0, 0.0f64..2.0)
            .prop_map(|(a, b, c)| format!("RANK BY ~[{a:.4},{b:.4},{c:.4}]")),
        Just("RERANK MENTIONS".to_string()),
        Just("RERANK NODE_DISTANCE FROM d0".to_string()),
        (0.0f64..1.0, 0usize..8).prop_map(|(l, k)| format!("RERANK MMR {l:.3} {k}")),
        (2000i64..2030).prop_map(|y| format!("WHERE year > {y}")),
        (2000i64..2030).prop_map(|y| format!("WHERE year < {y}")),
        (0u64..2_000_000_000).prop_map(|ts| format!("AS OF @{ts}")),
        (1u64..7200).prop_map(|s| format!("WINDOW {s}")),
        Just("FOREIGN \"peer\"".to_string()),
        (0usize..30).prop_map(|k| format!("LIMIT {k}")),
    ]
}

/// A pipeline of up to 16 stages (vs the determinism test's ≤6) — the "larger/longer"
/// the review asks for. Returns the UQL text.
fn pipeline_strategy() -> impl Strategy<Value = String> {
    (
        label_strategy(),
        prop::collection::vec(stage_strategy(), 0..16),
    )
        .prop_map(|(label, stages)| {
            let mut q = format!("MATCH (:{label})");
            for s in stages {
                q.push_str(" |> ");
                q.push_str(&s);
            }
            q
        })
}

/// The trailing `LIMIT k` bound (if the pipeline's LAST stage is a LIMIT), parsed back
/// from the text so we can assert the executor honored it.
fn trailing_limit(uql: &str) -> Option<usize> {
    let last = uql.rsplit("|>").next()?.trim();
    last.strip_prefix("LIMIT ")
        .and_then(|k| k.trim().parse().ok())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Fuzz (EG-KG.query.pipeline-fuzz): any generated VALID UQL up to 16 stages parses, executes without
    /// panic/error, yields a well-formed RowSet (unique ids), respects a trailing LIMIT,
    /// and is deterministic across repeated execution over the same snapshot.
    #[test]
    fn long_pipelines_never_panic_and_are_wellformed(q in pipeline_strategy()) {
        let (view, semantic) = build_dense(40);
        let plan = eg_plan::uql::parse(&q).expect("generated UQL must be valid");
        let ctx = PlanCtx::new(&view, &semantic);

        let a = execute(&plan, &ctx).expect("a valid plan must execute without error");

        // Unique ids — the closed-algebra dedup / consistent-state invariant.
        let ids = a.ids();
        let uniq: HashSet<&String> = ids.iter().collect();
        prop_assert_eq!(uniq.len(), ids.len(), "RowSet ids must be unique: {}", q);

        // A trailing LIMIT k truncates to ≤ k.
        if let Some(k) = trailing_limit(&q) {
            prop_assert!(ids.len() <= k, "trailing LIMIT {} not honored ({} rows): {}", k, ids.len(), q);
        }

        // Determinism / snapshot isolation — same plan, same snapshot, byte-identical.
        let b = execute(&plan, &ctx).unwrap();
        prop_assert_eq!(stable_rows(&a), stable_rows(&b), "non-deterministic execution: {}", q);
    }
}

proptest! {
    // Fewer cases: each case spawns threads, so keep the outer count modest.
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Chaos (EG-KG.query.concurrency-chaos-fuzz): a batch of random pipelines executed CONCURRENTLY over ONE shared
    /// immutable snapshot must produce, for every pipeline, the byte-identical RowSet its
    /// SERIAL execution produced — reads are isolated under contention (incl. the lazy
    /// HNSW build race on first search). This is the query-workload nemesis: mixed
    /// pipelines under thread contention, asserting snapshot-isolation holds.
    #[test]
    fn concurrent_mixed_workload_is_snapshot_isolated(
        pipelines in prop::collection::vec(pipeline_strategy(), 8..24)
    ) {
        let (view, semantic) = build_dense(40);

        // Serial reference: each pipeline's canonical RowSet over the snapshot.
        let plans: Vec<_> = pipelines
            .iter()
            .map(|q| eg_plan::uql::parse(q).expect("valid UQL"))
            .collect();
        let reference: Vec<Vec<(String, Option<u32>)>> = {
            let ctx = PlanCtx::new(&view, &semantic);
            plans.iter().map(|p| stable_rows(&execute(p, &ctx).unwrap())).collect()
        };

        // Concurrent: 8 threads, each replays the WHOLE batch against the SHARED snapshot
        // (std::thread::scope lets the closures borrow &view/&semantic — no Arc needed).
        // Every thread's every result must equal the serial reference.
        let plans_ref = &plans;
        let reference_ref = &reference;
        let view_ref = &view;
        let semantic_ref = &semantic;
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(move || {
                        let ctx = PlanCtx::new(view_ref, semantic_ref);
                        for (i, plan) in plans_ref.iter().enumerate() {
                            let got = stable_rows(&execute(plan, &ctx).unwrap());
                            assert_eq!(
                                got, reference_ref[i],
                                "concurrent read diverged from the serial reference (plan #{i})"
                            );
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("a worker thread panicked — concurrent execution is not isolated");
            }
        });
    }
}
