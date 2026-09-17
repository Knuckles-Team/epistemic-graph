//! Walk a pre-order proof tree, checking every node against the model.
//!
//! The walker keeps its own path and an explicit stack of open branches. A
//! tree is accepted only when it is exactly one complete pre-order tree:
//! every branch has both subtrees, no variable is fixed twice on a path, and
//! no node follows the last leaf. Such a tree partitions `{0,1}^n`.

use super::dual::{integer_bound, scaled_lagrangian, Target};
use super::error::VerifyError;
use crate::solve::certificate::{LagrangeDual, LeafProof, ProofNode};
use crate::solve::model::{Model, RowId, VarId};

/// What the verified leaves establish.
#[derive(Debug, Default)]
pub(super) struct Evidence {
    /// Smallest bound over bound leaves.
    pub(super) min_bound: Option<i128>,
    /// Rows used by forced nodes and infeasibility leaves, sorted, unique.
    pub(super) infeasibility_rows: Vec<RowId>,
    pub(super) bound_leaves: usize,
}

impl Evidence {
    fn add_bound(&mut self, bound: i128) {
        self.bound_leaves += 1;
        self.min_bound = Some(self.min_bound.map_or(bound, |low| low.min(bound)));
    }
}

/// An open branch: its variable, the value of its second subtree, whether that
/// subtree has started, and the path depth before the branch.
struct Open {
    var: usize,
    second: bool,
    started_second: bool,
    depth: usize,
}

struct Walker<'m> {
    model: &'m Model,
    path: Vec<Option<bool>>,
    fixed: Vec<usize>,
    open: Vec<Open>,
    finished: bool,
    evidence: Evidence,
}

/// Check a whole pre-order tree.
pub(super) fn walk(model: &Model, nodes: &[ProofNode]) -> Result<Evidence, VerifyError> {
    let mut walker = Walker {
        model,
        path: vec![None; model.variable_count()],
        fixed: Vec::new(),
        open: Vec::new(),
        finished: false,
        evidence: Evidence::default(),
    };
    for (index, node) in nodes.iter().enumerate() {
        if walker.finished {
            return Err(VerifyError::TrailingNodes { node: index });
        }
        walker.visit(index, node)?;
    }
    if !walker.finished {
        return Err(VerifyError::TreeIncomplete);
    }
    let mut evidence = walker.evidence;
    evidence.infeasibility_rows.sort_unstable();
    evidence.infeasibility_rows.dedup();
    Ok(evidence)
}

/// Check the root dual alone, at the empty assignment.
pub(super) fn root(model: &Model, dual: &LagrangeDual) -> Result<Evidence, VerifyError> {
    let path = vec![None; model.variable_count()];
    let scaled = scaled_lagrangian(model, dual, &path, (Target::Objective, 0))?;
    let mut evidence = Evidence::default();
    evidence.add_bound(integer_bound(scaled, dual.denominator));
    Ok(evidence)
}

impl Walker<'_> {
    fn visit(&mut self, index: usize, node: &ProofNode) -> Result<(), VerifyError> {
        match node {
            ProofNode::Branch { var, first } => {
                let depth = self.fixed.len();
                self.assign(index, *var, *first)?;
                let open = Open {
                    var: var.index(),
                    second: !*first,
                    started_second: false,
                    depth,
                };
                self.open.push(open);
                Ok(())
            }
            ProofNode::Forced { var, value, row } => self.forced(index, *var, *value, *row),
            ProofNode::Leaf { proof } => {
                self.leaf(index, proof)?;
                self.close();
                Ok(())
            }
        }
    }

    fn assign(&mut self, index: usize, var: VarId, value: bool) -> Result<(), VerifyError> {
        let slot = self
            .path
            .get_mut(var.index())
            .ok_or(VerifyError::VariableOutOfRange { node: index, var })?;
        if slot.is_some() {
            return Err(VerifyError::VariableAlreadyFixed { node: index, var });
        }
        *slot = Some(value);
        self.fixed.push(var.index());
        Ok(())
    }

    /// `var = value` is justified when `row` is unsatisfiable with `var = !value`.
    fn forced(
        &mut self,
        index: usize,
        var: VarId,
        value: bool,
        row: RowId,
    ) -> Result<(), VerifyError> {
        let model = self.model;
        let spec = model
            .rows()
            .get(row.index())
            .ok_or(VerifyError::RowOutOfRange { node: index, row })?;
        self.assign(index, var, !value)?;
        let justified =
            spec.coefficient_of(var).is_some() && super::rows::unsatisfiable(spec, &self.path);
        self.path[var.index()] = Some(value);
        if !justified {
            return Err(VerifyError::ForcedUnjustified { node: index });
        }
        self.evidence.infeasibility_rows.push(row);
        Ok(())
    }

    fn leaf(&mut self, index: usize, proof: &LeafProof) -> Result<(), VerifyError> {
        match proof {
            LeafProof::Bound { dual } => {
                let scaled =
                    scaled_lagrangian(self.model, dual, &self.path, (Target::Objective, index))?;
                self.evidence
                    .add_bound(integer_bound(scaled, dual.denominator));
            }
            LeafProof::Infeasible { dual } => {
                let scaled =
                    scaled_lagrangian(self.model, dual, &self.path, (Target::Feasibility, index))?;
                if scaled <= 0 {
                    return Err(VerifyError::LeafNotInfeasible { node: index });
                }
                let rows = dual.entries.iter().map(|entry| entry.row);
                self.evidence.infeasibility_rows.extend(rows);
            }
        }
        Ok(())
    }

    /// After a leaf: start the innermost unstarted second subtree, or finish.
    fn close(&mut self) {
        while let Some(open) = self.open.last_mut() {
            let (depth, var, second) = (open.depth, open.var, open.second);
            let start = !open.started_second;
            open.started_second = true;
            self.unwind(depth);
            if start {
                self.path[var] = Some(second);
                self.fixed.push(var);
                return;
            }
            self.open.pop();
        }
        self.finished = true;
    }

    fn unwind(&mut self, depth: usize) {
        for var in self.fixed.drain(depth..) {
            self.path[var] = None;
        }
    }
}
