//! Property-based multi-branch JOIN proof (CONCEPT:EG-KG.query.dag-multi-branch-join, E5
//! phase 3 — "the genuinely new capability"). A constrained proptest generator emits TWO
//! independent branch pipelines (each a `Scan` source followed by 0..3 random narrowing
//! ops), wires them into a [`PlanDag`] joining at a shared `Limit` node, and asserts
//! [`eg_plan::execute_dag`]'s result is EXACTLY the manually-computed oracle: each branch
//! executed independently via the UN-optimized [`SerialDriver`] (the same raw, in-structural-
//! order apply loop `execute_dag` runs its nodes through — `execute_dag` has no optimizer
//! pass of its own, exactly like `SerialDriver` vs. `execute`'s `plan_optimize` step),
//! intersected on id (preserving branch-A order — the same [`RowSet::intersect_keep_order`]
//! idiom every existing narrower already uses) via the PUBLIC `RowSet` API, then `Limit`-ed —
//! i.e. the exact join `dag_exec` performs, reproduced from outside the crate with no access
//! to its internals. This is the correctness proof for the multi-branch join CAPABILITY a
//! linear `Plan` cannot express at all (there is no `Plan` this test could even ask the
//! linear executor to run).

mod common;

use common::*;
use eg_plan::{Driver, Op, PlanCtx, PlanDag, PlanNode, Pred, RowSet, SerialDriver};
use proptest::prelude::*;

/// One legal branch-stage op, biased toward small/cheap shapes so the fixture (a handful of
/// `Doc`/`Tool` nodes) exercises real narrowing without every branch collapsing to empty.
fn stage_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (2000i64..2027).prop_map(|y| Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: y as f64,
            }],
        }),
        (0usize..2, 0usize..2).prop_map(|(min, d)| Op::Traverse {
            rel: "CITES".into(),
            min,
            max: min + d,
        }),
        Just(Op::Rank { query: query_vec() }),
        Just(Op::RankMentions {}),
        (0u64..2_000_000_000).prop_map(|ts| Op::AsOf {
            ts: ts as f64,
            axis: Default::default(),
        }),
    ]
}

fn branch_strategy() -> impl Strategy<Value = Vec<Op>> {
    (
        prop_oneof![Just("Doc"), Just("Tool")],
        prop::collection::vec(stage_strategy(), 0..3),
    )
        .prop_map(|(label, stages)| {
            let mut ops = vec![Op::Scan {
                label: label.to_string(),
            }];
            ops.extend(stages);
            ops
        })
}

/// Append a linear chain of `ops` to `nodes`, returning the id of the chain's LAST node
/// (the id a join/consumer wires to). Mirrors `PlanDag::from(Plan)`'s degenerate-chain
/// shape, built directly since this constructs a genuine multi-branch dag no `Plan` can
/// express.
fn push_chain(nodes: &mut Vec<PlanNode>, ops: Vec<Op>) -> usize {
    let mut last = None;
    for op in ops {
        let inputs = last.map_or_else(Vec::new, |p| vec![p]);
        nodes.push(PlanNode::new(op, inputs));
        last = Some(nodes.len() - 1);
    }
    last.expect("a branch always has at least a Scan source")
}

proptest! {
    /// `execute_dag` over a two-branch join equals the manually-computed oracle for ANY
    /// pair of randomly generated branch pipelines and join `k` — the multi-branch join
    /// capability, proved from OUTSIDE the crate via the public `RowSet` API only.
    #[test]
    fn multi_branch_join_matches_manual_oracle(
        branch_a in branch_strategy(),
        branch_b in branch_strategy(),
        join_k in 0usize..8,
    ) {
        let (view, semantic) = build_docs();
        let ctx = PlanCtx::new(&view, &semantic);

        // The oracle: each branch executed independently through the UN-optimized
        // SerialDriver (dag_exec applies no optimizer pass — same raw structural-order
        // apply loop), then joined via the PUBLIC RowSet API exactly as dag_exec's
        // `join_inputs` does for a 2-input node (first branch's order preserved).
        let ra = SerialDriver.run(&branch_a, &ctx).expect("branch A must execute");
        let rb = SerialDriver.run(&branch_b, &ctx).expect("branch B must execute");
        let keep = rb.id_set();
        let oracle: RowSet = ra.intersect_keep_order(&keep).limit(join_k);

        // The SAME two branches wired into a genuine multi-branch PlanDag, joined at a
        // shared Limit node — a shape no `Plan` (a `Vec<Op>`) can express at all.
        let mut nodes = Vec::new();
        let last_a = push_chain(&mut nodes, branch_a);
        let last_b = push_chain(&mut nodes, branch_b);
        nodes.push(PlanNode::new(Op::Limit { k: join_k }, vec![last_a, last_b]));
        let dag = PlanDag::new(nodes);

        let via_dag = eg_plan::execute_dag(&dag, &ctx).expect("dag exec must succeed");
        prop_assert_eq!(stable_rows(&via_dag), stable_rows(&oracle));
    }
}
