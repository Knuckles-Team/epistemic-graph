//! Property-based multi-branch JOIN proof (CONCEPT:EG-KG.query.dag-multi-branch-join, E5
//! phase 3 — "the genuinely new capability"). A constrained proptest generator emits TWO
//! arbitrary, ordered branch outputs. [`eg_plan::execute_dag_with`] injects them at separate
//! source nodes and joins them at a shared `Limit` node. The expected result is computed
//! independently through the PUBLIC [`RowSet`] algebra: intersect on id while preserving
//! branch-A order, then apply the limit. Keeping branch execution out of this property is
//! deliberate: the implementation under test cannot produce both its actual and expected
//! branch results and thereby self-confirm a scheduling defect.
//!
//! A separate deterministic proof below covers raw single-parent structural order using the
//! exact branch that exposed the former adaptive-`SerialDriver` oracle as invalid.

mod common;

use common::*;
use eg_plan::{Op, Plan, PlanCtx, PlanDag, PlanNode, Pred, RowSet};
use proptest::prelude::*;

/// Arbitrary ordered/deduplicated branch materializations, including ranked and unranked
/// rows. Small integer scores are exactly representable as `f32`, keeping the equality
/// oracle bit-stable while still proving score preservation through the join.
fn branch_output_strategy() -> impl Strategy<Value = RowSet> {
    prop::collection::vec((0u8..16, prop::option::of(0u8..16)), 0..16).prop_map(|rows| {
        RowSet::from_rows(
            rows.into_iter()
                .map(|(id, score)| (format!("n{id}"), score.map(f32::from))),
        )
    })
}

proptest! {
    /// A two-parent DAG join equals the independently computed public-`RowSet` oracle for
    /// arbitrary ordered branch outputs and `k`.
    #[test]
    fn multi_branch_join_matches_manual_oracle(
        branch_a in branch_output_strategy(),
        branch_b in branch_output_strategy(),
        join_k in 0usize..8,
    ) {
        let (view, semantic) = build_docs();
        let ctx = PlanCtx::new(&view, &semantic);

        let keep = branch_b.id_set();
        let oracle = branch_a.intersect_keep_order(&keep).limit(join_k);

        // The two source ops are placeholders: the public override seam supplies each
        // independently generated branch output before local op dispatch. Only the join
        // node falls through to its real Limit op.
        let dag = PlanDag::new(vec![
            PlanNode::new(Op::Limit { k: usize::MAX }, vec![]),
            PlanNode::new(Op::Limit { k: usize::MAX }, vec![]),
            PlanNode::new(Op::Limit { k: join_k }, vec![0, 1]),
        ]);
        let via_dag = eg_plan::execute_dag_with(&dag, &ctx, |id, _node, _input| {
            Ok(match id {
                0 => Some(branch_a.clone()),
                1 => Some(branch_b.clone()),
                _ => None,
            })
        }).expect("dag exec must succeed");
        prop_assert_eq!(stable_rows(&via_dag), stable_rows(&oracle));
    }
}

/// A single-parent DAG applies ops in structural order. This is the exact minimized branch
/// that invalidated the former `SerialDriver` oracle: adaptive execution may move `Rank`
/// before `Filter`, while raw DAG execution must keep `Scan → Filter → Rank`. `t1` passes
/// the filter but has no embedding, so the independently known correct result is empty.
#[test]
fn single_branch_preserves_structural_order() {
    let (view, semantic) = build_docs();
    let ctx = PlanCtx::new(&view, &semantic);
    let branch = PlanDag::from(Plan::new(vec![
        Op::Scan {
            label: "Tool".into(),
        },
        Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 2000.0,
            }],
        },
        Op::Rank { query: query_vec() },
    ]));

    let result = eg_plan::execute_dag(&branch, &ctx).expect("raw branch must execute");
    assert!(
        result.is_empty(),
        "structural Scan → Filter → Rank must drop the unembedded Tool, got {:?}",
        stable_rows(&result)
    );
}
