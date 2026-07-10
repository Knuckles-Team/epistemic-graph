//! `PlanDag` — the typed DAG plan representation (CONCEPT:EG-KG.query.plan-dag, E5 phase 1).
//!
//! [`crate::algebra::Plan`] is a linear `Vec<Op>` — a pipeline where op *i* consumes
//! op *(i-1)*'s [`crate::RowSet`]. That already hides a latent DAG: [`Op::FuseRrf`]
//! (`branches: Vec<Vec<Op>>`) runs N independent sub-plans over the SAME seed and
//! fuses their outputs — a fan-out/fan-in shape a flat `Vec<Op>` can only express by
//! nesting an entire sub-algebra inside one variant. `PlanDag` makes that shape
//! FIRST-CLASS: a node's `inputs` name the OTHER nodes it consumes from, so a plan is
//! a real graph of operators, not just a chain with one embedded escape hatch.
//!
//! ## What this phase is (and is not)
//!
//!  * [`Op`] is COMPLETELY UNCHANGED — a [`PlanNode`] wraps the SAME wire `Op` a
//!    [`Plan`] carries; nothing here touches the algebra or the wire format.
//!  * A linear [`Plan`] converts losslessly to a DEGENERATE chain `PlanDag` — node
//!    *i*'s only input is node *(i-1)*, node 0 has none (see [`From<Plan>`]) — so
//!    every plan the engine already runs is representable with ZERO behavior
//!    change. [`PlanDag::as_linear_plan`] is the exact inverse for such a chain,
//!    proved round-trip-lossless by `plan_dag_round_trips_linear_plan` below.
//!  * A genuine multi-parent node (two or more `inputs`) is the NEW capability later
//!    phases build on (`dag_exec`'s multi-branch join, the DAG-aware optimizer) —
//!    this phase only defines the shape + the lossless conversion, execution is
//!    untouched (`exec::execute` keeps running the linear `Plan` exactly as before).
//!
//! ## X4 — distributed cross-shard exchange (SCOPED TODO, cluster-gated, NOT built here)
//!
//! `PlanDag`'s multi-input node is deliberately the seam a **cross-shard EXCHANGE**
//! operator would hang off (CONCEPT:EG-KG.query.dag-distributed-exchange): a
//! multi-branch plan whose branches execute on DIFFERENT Raft groups, each producing a
//! partial `RowSet`, merged at a native exchange node — the distributed generalization
//! of `dag_exec`'s single-node `join_inputs`. This is **not implemented in this
//! workstream** and is a clearly-scoped follow-up, on purpose:
//!
//!  * It MUST be `raft`/`cluster`-feature-gated ONLY — a `default`/`full` build (which
//!    is single-node by construction; `full` links no `raft`) must link NONE of it, so
//!    the exchange operator + its shard-routing live behind `#[cfg(feature = "cluster")]`
//!    beside the existing 2PC/federation/`GroupRouter` machinery (`src/raft/`), NOT in
//!    this dep-free `eg-plan` crate.
//!  * The concrete shape: a new cluster-gated exchange node kind that, given a branch's
//!    `PlanNode` subtree and a target `GroupId` (via `raft::multi::GroupRouter`), ships
//!    the sub-plan to that group (over the SAME length-prefixed-MessagePack transport
//!    federation's `Op::ForeignScan` already uses), collects the partial `RowSet`, and
//!    feeds it into a local `dag_exec` multi-branch join — reusing
//!    `RowSet::intersect_keep_order` for the merge so the distributed result is identical
//!    to the single-node one when all branches happen to route to one group.
//!
//! Landing X4 was descoped to keep phases 1–5 (the single-node typed DAG, its
//! differential-oracle-proven executor, the DAG-aware optimizer, the EXPLAIN surfaces
//! and the D7 planner-writeback ACID seam) a complete, mergeable E5. See the
//! feat/eg-planner-dag report + docs/north_star.md for the tracked row.

use crate::algebra::{Op, Plan};

/// A node's index into [`PlanDag::nodes`] — the DAG's edge currency (an edge from
/// `PlanDag::nodes[a]` to `PlanDag::nodes[b]` is recorded as `a` appearing in
/// `nodes[b].inputs`).
pub type NodeId = usize;

/// One operator in a [`PlanDag`]: the SAME [`Op`] a linear [`Plan`] carries, plus the
/// DAG structure (`inputs`) and the per-node metadata later phases hang off it.
#[derive(Clone, Debug, PartialEq)]
pub struct PlanNode {
    /// The operator this node runs — unchanged from the wire algebra.
    pub op: Op,
    /// The nodes whose OUTPUT this node consumes, in order. Empty ⇒ this node is a
    /// SOURCE (it runs over an empty seed `RowSet`, exactly like index 0 of a linear
    /// `Plan`). One entry ⇒ the ordinary linear-chain case. Two or more ⇒ a
    /// multi-branch JOIN (the DAG executor's new capability — see `crate::dag_exec`).
    pub inputs: Vec<NodeId>,
    /// Optional declared input projection/schema (CONCEPT:EG-KG.query.knowledge-set) —
    /// EXPLAIN PLAN surfaces it when set; execution does not require it. `None` by
    /// default (every existing plan carries no schema annotation).
    pub input_schema: Option<crate::knowledge::ProjectionSchema>,
    /// Optional declared output projection/schema, mirroring `input_schema`.
    pub output_schema: Option<crate::knowledge::ProjectionSchema>,
    /// Whether provenance (source/evidence refs, CONCEPT:EG-KG.epistemic.epistemic-substrate)
    /// should be pushed down and resolved at this node when the plan is EXPLAINed
    /// (`EXPLAIN PROVENANCE`, E5 phase 4). `false` by default — a bare conversion from
    /// a linear `Plan` never opts in.
    pub provenance_pushdown: bool,
    /// Whether the RLS/policy filter (`eg_core::isolation::IsolationLayer`) should be
    /// pushed down and evaluated at this node when the plan is EXPLAINed (`EXPLAIN
    /// POLICY`). `false` by default.
    pub policy_pushdown: bool,
}

impl PlanNode {
    /// A node with no schema/pushdown annotations — the shape every node converted
    /// from a linear [`Plan`] has.
    pub fn new(op: Op, inputs: Vec<NodeId>) -> Self {
        Self {
            op,
            inputs,
            input_schema: None,
            output_schema: None,
            provenance_pushdown: false,
            policy_pushdown: false,
        }
    }
}

/// A typed DAG plan: an ordered list of [`PlanNode`]s whose `inputs` encode the
/// dependency edges (CONCEPT:EG-KG.query.plan-dag). Node ids are stable indices into
/// `nodes`, so an edge is just `NodeId < nodes.len()`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlanDag {
    pub nodes: Vec<PlanNode>,
}

impl PlanDag {
    pub fn new(nodes: Vec<PlanNode>) -> Self {
        Self { nodes }
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// The SINGLE node no other node consumes (the plan's terminal output) — the node
    /// [`crate::dag_exec::execute_dag`] returns the `RowSet` of. `Err` when the dag is
    /// empty, when no such node exists (every node is consumed — a cycle, since a
    /// well-formed DAG always has a sink), or when MORE than one exists (an
    /// ill-formed multi-output dag this phase does not support — a plan has ONE
    /// result, exactly like a linear `Plan`'s last op).
    pub fn sink(&self) -> Result<NodeId, String> {
        if self.nodes.is_empty() {
            return Err("PlanDag has no nodes".to_string());
        }
        let mut consumed = vec![false; self.nodes.len()];
        for node in &self.nodes {
            for &input in &node.inputs {
                if input >= self.nodes.len() {
                    return Err(format!(
                        "PlanDag node references out-of-range input {input}"
                    ));
                }
                consumed[input] = true;
            }
        }
        let sinks: Vec<NodeId> = (0..self.nodes.len()).filter(|&i| !consumed[i]).collect();
        match sinks.as_slice() {
            [only] => Ok(*only),
            [] => Err("PlanDag has no sink (every node is consumed — a cycle?)".to_string()),
            many => Err(format!(
                "PlanDag has {} candidate sinks {many:?} — a plan must have exactly one output",
                many.len()
            )),
        }
    }

    /// A topological order over the nodes (Kahn's algorithm), so a caller can execute
    /// each node only after every node it depends on has already run. Ties (multiple
    /// ready nodes) break by ascending `NodeId` — deterministic, and exactly the
    /// left-to-right order a degenerate linear chain already runs in. `Err` on a
    /// dangling input reference or a cycle (fewer nodes emitted than exist).
    pub fn topo_order(&self) -> Result<Vec<NodeId>, String> {
        let n = self.nodes.len();
        let mut in_degree = vec![0usize; n];
        let mut children: Vec<Vec<NodeId>> = vec![Vec::new(); n];
        for (id, node) in self.nodes.iter().enumerate() {
            in_degree[id] = node.inputs.len();
            for &input in &node.inputs {
                if input >= n {
                    return Err(format!(
                        "PlanDag node {id} references out-of-range input {input}"
                    ));
                }
                children[input].push(id);
            }
        }

        // A small sorted ready-set (ascending NodeId) kept via a Vec + binary search —
        // n is a plan's op count (small), so this is simpler than pulling in a heap dep.
        let mut ready: Vec<NodeId> = (0..n).filter(|&i| in_degree[i] == 0).collect();
        let mut order = Vec::with_capacity(n);
        while let Some(&next) = ready.first() {
            ready.remove(0);
            order.push(next);
            for &child in &children[next] {
                in_degree[child] -= 1;
                if in_degree[child] == 0 {
                    let pos = ready.partition_point(|&x| x < child);
                    ready.insert(pos, child);
                }
            }
        }
        if order.len() != n {
            return Err("PlanDag has a cycle".to_string());
        }
        Ok(order)
    }

    /// When this dag is a LINEAR chain — node 0 has no inputs, and node *i* (`i>0`)'s
    /// only input is node *(i-1)* — return the equivalent [`Plan`]. `None` for a
    /// genuine multi-branch dag (any node with 0 or 2+ inputs past the first, or an
    /// out-of-sequence input). The exact inverse of [`From<Plan>`] for the chain shape
    /// it produces — the round-trip proof.
    pub fn as_linear_plan(&self) -> Option<Plan> {
        for (i, node) in self.nodes.iter().enumerate() {
            let expected: &[NodeId] = if i == 0 { &[] } else { &[i - 1] };
            if node.inputs != expected {
                return None;
            }
        }
        Some(Plan::new(self.nodes.iter().map(|n| n.op.clone()).collect()))
    }
}

/// A linear [`Plan`] is a DEGENERATE chain `PlanDag`: node *i* depends on node
/// *(i-1)* alone, node 0 is a source. Lossless (see [`PlanDag::as_linear_plan`]) and
/// a ZERO-behavior-change conversion — [`crate::dag_exec::execute_dag`] over the
/// result executes identically to [`crate::exec::execute`] over `plan` (the
/// differential oracle, E5 phase 2, proves this over the whole fixture suite).
impl From<Plan> for PlanDag {
    fn from(plan: Plan) -> Self {
        let mut nodes = Vec::with_capacity(plan.ops.len());
        for (i, op) in plan.ops.into_iter().enumerate() {
            let inputs = if i == 0 { Vec::new() } else { vec![i - 1] };
            nodes.push(PlanNode::new(op, inputs));
        }
        PlanDag { nodes }
    }
}

impl From<&Plan> for PlanDag {
    fn from(plan: &Plan) -> Self {
        PlanDag::from(plan.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::Pred;

    fn sample_plan() -> Plan {
        Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2024.0,
                }],
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 2,
            },
            Op::Rank {
                query: vec![1.0, 0.0],
            },
            Op::Limit { k: 10 },
        ])
    }

    /// ZERO behavior change (phase 1's non-negotiable proof): converting a linear
    /// `Plan` to a `PlanDag` and back yields the IDENTICAL op sequence — a lossless
    /// round trip, so every existing plan is representable with no change.
    #[test]
    fn plan_dag_round_trips_linear_plan() {
        let plan = sample_plan();
        let dag = PlanDag::from(plan.clone());

        // Structural shape: node 0 is a source, every later node chains to its
        // immediate predecessor only.
        assert_eq!(dag.len(), 5);
        assert_eq!(dag.nodes[0].inputs, Vec::<NodeId>::new());
        for i in 1..dag.len() {
            assert_eq!(dag.nodes[i].inputs, vec![i - 1]);
        }
        // Fresh nodes carry no schema/pushdown annotations.
        for node in &dag.nodes {
            assert_eq!(node.input_schema, None);
            assert_eq!(node.output_schema, None);
            assert!(!node.provenance_pushdown);
            assert!(!node.policy_pushdown);
        }

        assert_eq!(dag.as_linear_plan(), Some(plan));
    }

    #[test]
    fn single_op_plan_round_trips() {
        let plan = Plan::new(vec![Op::Scan {
            label: "Doc".into(),
        }]);
        let dag = PlanDag::from(plan.clone());
        assert_eq!(dag.sink(), Ok(0));
        assert_eq!(dag.as_linear_plan(), Some(plan));
    }

    #[test]
    fn empty_dag_has_no_sink() {
        let dag = PlanDag::new(Vec::new());
        assert!(dag.sink().is_err());
        assert_eq!(dag.as_linear_plan(), Some(Plan::new(Vec::new())));
    }

    /// A genuine multi-branch dag (two sources joining into one node) is NOT a linear
    /// chain — `as_linear_plan` must reject it (the shape a later phase's join
    /// executor consumes, not a `Plan`).
    #[test]
    fn multi_input_node_is_not_a_linear_plan() {
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
            PlanNode::new(Op::Limit { k: 5 }, vec![0, 1]),
        ]);
        assert_eq!(dag.sink(), Ok(2));
        assert_eq!(dag.as_linear_plan(), None);
        assert_eq!(dag.topo_order().unwrap(), vec![0, 1, 2]);
    }

    #[test]
    fn topo_order_rejects_cycle_and_dangling_input() {
        let cyclic = PlanDag::new(vec![
            PlanNode::new(Op::Limit { k: 1 }, vec![1]),
            PlanNode::new(Op::Limit { k: 1 }, vec![0]),
        ]);
        assert!(cyclic.topo_order().is_err());

        let dangling = PlanDag::new(vec![PlanNode::new(Op::Limit { k: 1 }, vec![5])]);
        assert!(dangling.topo_order().is_err());
        assert!(dangling.sink().is_err());
    }

    #[test]
    fn sink_rejects_multiple_candidates() {
        // Two nodes, neither consumed by the other — two candidate sinks.
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
        assert!(dag.sink().is_err());
    }
}
