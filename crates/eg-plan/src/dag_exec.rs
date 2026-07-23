//! Physical execution of a [`PlanDag`] (CONCEPT:EG-KG.query.plan-dag-exec, E5 phase 2) —
//! runs ALONGSIDE the untouched [`crate::exec::execute`] / [`crate::exec::apply`]: this
//! module adds NO new per-op semantics, it only adds a new SCHEDULING shape around the
//! SAME `apply` dispatch every op already runs through.
//!
//! ## Execution model
//!
//! [`execute_dag`] runs every [`PlanNode`] in [`PlanDag::topo_order`] order. A node's
//! input `RowSet` is resolved from its `inputs`:
//!
//!  * **zero inputs** — the node is a SOURCE; it runs over an empty seed, exactly like
//!    index 0 of a linear [`Plan`].
//!  * **one input** — the node runs over that single parent's finished output,
//!    byte-for-byte the linear left-fold [`crate::exec::SerialDriver`] already does.
//!  * **two or more inputs (CONCEPT:EG-KG.query.dag-multi-branch-join, the new
//!    capability)** — a MULTI-BRANCH JOIN: the parents' outputs are combined via
//!    [`RowSet::intersect_keep_order`], chained left-to-right in `inputs` order, so the
//!    FIRST-listed parent's order is preserved (mirroring the intersect idiom every
//!    existing narrower — `AsOf`, `Reason`, epistemic `EvidenceFor`/`Contradicts` — already
//!    uses to combine a candidate set with a second constraint). The joined `RowSet` then
//!    runs through this node's own `op` via the SAME [`crate::exec::apply`] every op
//!    already dispatches through.
//!
//! For a DEGENERATE chain dag (every [`From<Plan>`] conversion produces one), every node
//! has exactly 0 or 1 inputs, so the join step never fires and [`execute_dag`] is
//! BYTE-IDENTICAL to [`crate::exec::execute`] — the differential oracle
//! (`tests/dag_oracle.rs`) proves this over the whole existing plan-fixture suite.

use crate::dag::{NodeId, PlanDag, PlanNode};
use crate::exec::{apply, PlanCtx};
use crate::rowset::RowSet;

/// Execute `dag` over `ctx`, returning the `RowSet` of its single sink node
/// (CONCEPT:EG-KG.query.plan-dag-exec). An empty dag yields an empty `RowSet` (the
/// vacuous plan, matching `Plan::new(vec![])` under `exec::execute`). `Err` on a
/// malformed dag (a cycle, a dangling input reference, or no single sink) or when any
/// node's `op` errors (propagated exactly like `exec::apply`).
///
/// The special case of [`execute_dag_with`] that never overrides a node.
pub fn execute_dag(dag: &PlanDag, ctx: &PlanCtx) -> Result<RowSet, String> {
    execute_dag_with(dag, ctx, |_id, _node, _joined_input| Ok(None))
}

/// [`execute_dag`], generalized with a per-node OVERRIDE hook (CONCEPT:EG-KG.query.dag-distributed-exchange,
/// X4) — the seam the cluster-feature-gated cross-shard EXCHANGE operator
/// (`src/raft/exchange.rs`, outside this dep-free crate) needs, added here rather than
/// duplicated there.
///
/// For every node in [`PlanDag::topo_order`], `override_node(id, node, &joined_input)`
/// runs FIRST:
///  * `Ok(Some(rows))` supplies the node's OUTPUT directly and SKIPS the local
///    `apply(&node.op, ..)` dispatch entirely — e.g. a branch root whose real work ran
///    on a REMOTE Raft group; `rows` is whatever came back over the wire.
///  * `Ok(None)` falls through to the normal local `apply(&node.op, joined_input, ctx)`
///    — byte-for-byte what [`execute_dag`] always does.
///  * `Err(e)` aborts the whole execution, exactly like an `apply` error.
///
/// Crucially, [`join_inputs`] (the multi-branch JOIN) runs IDENTICALLY either way: it
/// only reads `outputs[..]` by node id, oblivious to whether a given entry was computed
/// locally or supplied by the override — so a distributed EXCHANGE's merge is the SAME
/// `RowSet::intersect_keep_order` chain a single-node multi-branch dag already uses,
/// reused rather than reimplemented.
pub fn execute_dag_with<F>(
    dag: &PlanDag,
    ctx: &PlanCtx,
    mut override_node: F,
) -> Result<RowSet, String>
where
    F: FnMut(NodeId, &PlanNode, &RowSet) -> Result<Option<RowSet>, String>,
{
    if dag.is_empty() {
        return Ok(RowSet::new());
    }
    let sink = dag.sink()?;
    let order = dag.topo_order()?;

    let mut outputs: Vec<Option<RowSet>> = vec![None; dag.len()];
    for id in order {
        let node = &dag.nodes[id];
        let input = join_inputs(node, &outputs)?;
        let out = match override_node(id, node, &input)? {
            Some(rows) => rows,
            None => apply(&node.op, input, ctx)?,
        };
        outputs[id] = Some(out);
    }
    outputs[sink]
        .take()
        .ok_or_else(|| format!("dag_exec: sink node {sink} produced no output"))
}

/// Resolve one node's input `RowSet` from its already-computed parents
/// (CONCEPT:EG-KG.query.dag-multi-branch-join): empty ⇒ a fresh seed (a source op),
/// one ⇒ that parent's output verbatim, many ⇒ the id-intersection of every parent's
/// output, preserving the FIRST parent's order.
pub(crate) fn join_inputs(node: &PlanNode, outputs: &[Option<RowSet>]) -> Result<RowSet, String> {
    let parent = |id: NodeId| -> Result<&RowSet, String> {
        outputs[id]
            .as_ref()
            .ok_or_else(|| format!("dag_exec: node {id} consumed before it was computed"))
    };
    match node.inputs.as_slice() {
        [] => Ok(RowSet::new()),
        [only] => Ok(parent(*only)?.clone()),
        [first, rest @ ..] => {
            let mut acc = parent(*first)?.clone();
            for &id in rest {
                let keep = parent(id)?.id_set();
                acc = acc.intersect_keep_order(&keep);
            }
            Ok(acc)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::{Op, Plan};
    use crate::dag::PlanNode;
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    fn fixture() -> (eg_core::graph::GraphView, SemanticStore) {
        let core = GraphCore::new();
        for (id, ty) in [("a", "Doc"), ("b", "Doc"), ("c", "Tool")] {
            core.add_node(id.into(), blob(json!({ "type": ty })));
        }
        (core.analysis_snapshot(), SemanticStore::new())
    }

    /// A degenerate (linear) dag executes IDENTICALLY to `exec::execute` over the same
    /// `Plan` — the zero-behavior-change proof at unit-test grain (the integration
    /// `tests/dag_oracle.rs` proves this broadly over the fixture suite).
    #[test]
    fn linear_dag_matches_exec_execute() {
        let (view, semantic) = fixture();
        let ctx = PlanCtx::new(&view, &semantic);
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Limit { k: 1 },
        ]);
        let via_exec = crate::exec::execute(&plan, &ctx).unwrap();
        let dag = PlanDag::from(plan);
        let via_dag = execute_dag(&dag, &ctx).unwrap();
        assert_eq!(via_exec, via_dag);
    }

    /// A genuine multi-branch join: two independent label scans intersected — only the
    /// id present in BOTH parents' output survives. `a`/`b` are `Doc`, `c` is `Tool`, so
    /// scanning `Doc` and scanning "every node" (an unfiltered `AsOf` source) and
    /// joining them keeps exactly `{a,b}`.
    #[test]
    fn multi_branch_join_intersects_parent_outputs() {
        let (view, semantic) = fixture();
        let ctx = PlanCtx::new(&view, &semantic);
        let dag = PlanDag::new(vec![
            PlanNode::new(
                Op::Scan {
                    label: "Doc".into(),
                },
                vec![],
            ),
            PlanNode::new(
                Op::Scan {
                    label: "Tool".into(),
                },
                vec![],
            ),
            // Join node 0 (Doc) with node 1 (Tool) — disjoint sets, so the join is empty.
            PlanNode::new(Op::Limit { k: 10 }, vec![0, 1]),
        ]);
        let out = execute_dag(&dag, &ctx).unwrap();
        assert!(out.is_empty(), "Doc ∩ Tool must be empty");
    }

    /// [`execute_dag_with`]'s override hook: substituting a branch's output (as if it
    /// had been fetched from a REMOTE Raft group, X4's shape) still lets the
    /// UNMODIFIED `join_inputs` merge do the right thing — the join sees only
    /// `outputs[..]`, not where they came from. Node 1 (`Tool`) is overridden to
    /// return `{b}` instead of running its real `Scan` locally (which would yield
    /// `{c}`), so joining it with node 0's real `Doc` scan (`{a,b}`) keeps `{b}` — the
    /// value only reachable through the override, proving it actually took effect.
    #[test]
    fn execute_dag_with_override_feeds_the_same_join_inputs_merge() {
        let (view, semantic) = fixture();
        let ctx = PlanCtx::new(&view, &semantic);
        let dag = PlanDag::new(vec![
            PlanNode::new(
                Op::Scan {
                    label: "Doc".into(),
                },
                vec![],
            ),
            PlanNode::new(
                Op::Scan {
                    label: "Tool".into(),
                },
                vec![],
            ),
            PlanNode::new(Op::Limit { k: 10 }, vec![0, 1]),
        ]);
        let out = execute_dag_with(&dag, &ctx, |id, _node, _joined| {
            if id == 1 {
                Ok(Some(RowSet::from_ids(["b".to_string()])))
            } else {
                Ok(None)
            }
        })
        .unwrap();
        assert_eq!(out.ids(), vec!["b".to_string()]);
    }

    #[test]
    fn empty_dag_yields_empty_rowset() {
        let (view, semantic) = fixture();
        let ctx = PlanCtx::new(&view, &semantic);
        let dag = PlanDag::new(Vec::new());
        assert!(execute_dag(&dag, &ctx).unwrap().is_empty());
    }

    #[test]
    fn malformed_dag_errors_cleanly() {
        let (view, semantic) = fixture();
        let ctx = PlanCtx::new(&view, &semantic);
        // Two candidate sinks — a plan must have exactly one output.
        let dag = PlanDag::new(vec![
            PlanNode::new(
                Op::Scan {
                    label: "Doc".into(),
                },
                vec![],
            ),
            PlanNode::new(
                Op::Scan {
                    label: "Tool".into(),
                },
                vec![],
            ),
        ]);
        assert!(execute_dag(&dag, &ctx).is_err());
    }
}
