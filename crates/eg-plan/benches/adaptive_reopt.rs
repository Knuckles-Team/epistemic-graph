//! Adaptive re-optimization skew bench (CONCEPT:EG-KG.query.adaptive-reoptimization, W4.12
//! phase 1 — "planner feedback + learned costs").
//!
//! `crate::optimizer::reoptimize_remaining` (its own unit tests, `optimizer.rs`) and its
//! auto-wiring into the executor's per-op loop (`tests.rs`'s
//! `adaptive_reopt_auto_wires_into_ordinary_execution`, `runtime.rs`'s
//! `parallel_driver_runs_adaptive_reopt`) already PROVE the mechanism fires on a
//! cardinality miss and CAN change the remaining plan. This bench answers the
//! complementary question those correctness proofs don't: does it actually pay off in
//! wall-clock time on data whose skew a plan-time-only estimate cannot see?
//!
//! ## The fixture
//!
//! One HUB node fans out to `REACHED` candidate nodes via a `LINK` edge; a large pool
//! of unrelated `FILLER` nodes (no edges at all) keeps the graph's GLOBAL average
//! out-degree tiny, so [`crate::cost::ModalityCardinality`]'s degree-average
//! `Traverse` estimator badly UNDER-shoots the hub's real fan-out — the exact,
//! already-proven trigger shape `tests.rs`'s hub/reached/filler fixture uses, scaled up
//! here for a measurable wall-clock delta. Every reached node carries a real embedding
//! (full coverage, so a vector `Rank` never narrows the set on its own) and a `keep`
//! flag true for only a SMALL, selective fraction of them.
//!
//! ## The plan, written the way a caller naturally would
//!
//! `Scan(Hub) -> Traverse(LINK) -> Rank(query) -> Filter(keep=yes) -> Limit(5)` — rank
//! the reached candidates by relevance, THEN apply a business-rule filter. Written this
//! way, an un-corrected execution pays a full vector distance for EVERY reached node
//! before the selective filter ever narrows anything; correctly reordering the
//! selective filter ahead of the brute-force rank (which
//! `crate::optimizer::plan_optimize`/the runtime feedback loop both do, using the SAME
//! cost machinery whether the seed is the pre-Traverse estimate or the post-Traverse
//! actual) is exactly the win a cost-based cross-modal planner exists to deliver.
//!
//! ## The two arms
//!
//! Both arms run the IDENTICAL plan through the ordinary, public `execute()` entry
//! point — no white-box hook, the same call a caller makes:
//!  * `skewed_reopt_on`  — `EPISTEMIC_GRAPH_COST_OPT` unset (the shipped default):
//!    plan-time `optimize()` AND the auto-wired runtime feedback loop are both active.
//!  * `skewed_reopt_off` — `EPISTEMIC_GRAPH_COST_OPT=0`, the documented kill-switch
//!    ([`crate::optimizer::enabled`]) that disables BOTH halves together (by design —
//!    see `exec.rs`'s `SerialDriver` docs): a plain left fold over the plan exactly as
//!    written, "no per-op cardinality bookkeeping" — the codebase's own definition of
//!    "no-reopt".
//!
//! Run: `cargo bench -p eg-plan --features query --bench adaptive_reopt`.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_plan::{execute, Op, Plan, PlanCtx, Pred};
use serde_json::json;

/// Reached candidates under the hub. Large enough that scoring every one of them
/// (the un-corrected order) is measurably slower than scoring only the selective
/// survivors, small enough that the bench itself stays fast.
const REACHED: usize = 2_000;
/// Unrelated filler nodes with NO edges — keeps the graph's global average out-degree
/// tiny (`edge_count / node_count ≈ REACHED / (1 + REACHED + FILLER)`), so the
/// `Traverse` degree-average estimator badly under-shoots the hub's real fan-out.
const FILLER: usize = 2_000;
/// Only every 33rd reached node passes the `keep` filter (~3%) — the selective
/// narrower a cost-based reorder should always run before the brute-force rank.
const KEEP_STRIDE: usize = 33;
const DIM: usize = 16;

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// Deterministic LCG (no `rand` dep, matching `hybrid_queries.rs`'s convention) so the
/// embedding set — and therefore the measured timings — is byte-reproducible run to run.
struct Lcg(u64);
impl Lcg {
    fn next_signed(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let bits = (self.0 >> 40) as f32 / (1u64 << 24) as f32;
        bits * 2.0 - 1.0
    }
}

/// Build the hub/reached/filler skew fixture (CONCEPT:EG-KG.query.adaptive-reoptimization).
fn build_skew_fixture() -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    core.add_node("hub".into(), blob(json!({"type": "Hub"})));
    for i in 0..REACHED {
        let id = format!("r{i}");
        let keep = if i % KEEP_STRIDE == 0 { "yes" } else { "no" };
        core.add_node(id.clone(), blob(json!({"type": "Reached", "keep": keep})));
        core.add_edge("hub".into(), id, blob(json!({"relationship": "LINK"})))
            .unwrap();
    }
    for i in 0..FILLER {
        core.add_node(format!("filler{i}"), blob(json!({"type": "Filler"})));
    }

    let mut semantic = SemanticStore::new();
    let mut rng = Lcg(0xA5A5_1234_9E37_79B9);
    for i in 0..REACHED {
        let v: Vec<f32> = (0..DIM).map(|_| rng.next_signed()).collect();
        semantic.add_embedding(format!("r{i}"), v);
    }
    (core.analysis_snapshot(), semantic)
}

/// `Scan -> Traverse -> Rank -> Filter -> Limit`, written the way a caller naturally
/// would (rank first, then apply a business-rule filter) — see the module doc.
fn skewed_plan() -> Plan {
    Plan::new(vec![
        Op::Scan {
            label: "Hub".into(),
        },
        Op::Traverse {
            rel: "LINK".into(),
            min: 1,
            max: 1,
        },
        Op::Rank {
            query: (0..DIM).map(|i| if i == 0 { 1.0 } else { 0.0 }).collect(),
        },
        Op::Filter {
            preds: vec![Pred::Eq {
                prop: "keep".into(),
                value: "yes".into(),
            }],
        },
        Op::Limit { k: 5 },
    ])
}

/// Sets (or clears) the process-wide cost-opt kill-switch. Criterion runs
/// `bench_function`s sequentially within one process by default, and this is set ONCE
/// per function (not inside the timed `b.iter` closure), so there is no cross-arm
/// interference — mirrors `federation_tests.rs`'s existing `std::env::set_var` use in
/// this same crate (no `unsafe` needed on the pinned toolchain).
fn set_cost_opt(on: bool) {
    if on {
        std::env::remove_var("EPISTEMIC_GRAPH_COST_OPT");
    } else {
        std::env::set_var("EPISTEMIC_GRAPH_COST_OPT", "0");
    }
}

fn bench_skewed_reopt_on(c: &mut Criterion) {
    let (view, semantic) = build_skew_fixture();
    let ctx = PlanCtx::new(&view, &semantic);
    let plan = skewed_plan();
    set_cost_opt(true);
    c.bench_function("skewed_reopt_on", |b| {
        b.iter(|| black_box(execute(&plan, &ctx).unwrap()))
    });
}

fn bench_skewed_reopt_off(c: &mut Criterion) {
    let (view, semantic) = build_skew_fixture();
    let ctx = PlanCtx::new(&view, &semantic);
    let plan = skewed_plan();
    set_cost_opt(false);
    c.bench_function("skewed_reopt_off", |b| {
        b.iter(|| black_box(execute(&plan, &ctx).unwrap()))
    });
    // Restore the default for any bench that runs after this one in the same process.
    set_cost_opt(true);
}

fn all_benches(c: &mut Criterion) {
    bench_skewed_reopt_on(c);
    bench_skewed_reopt_off(c);
}

criterion_group! {
    name = benches;
    // Bounded, non-flaky: a short warm-up + a modest sample floor keep wall-clock low,
    // mirroring `hybrid_queries.rs`'s CI-gate config.
    config = Criterion::default()
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(30);
    targets = all_benches
}
criterion_main!(benches);
