//! Pre-order recording of the search tree as a certificate proof.

use super::state::{row_id, var_id};
use crate::solve::certificate::{DualEntry, LagrangeDual, LeafProof, ProofNode};
use crate::solve::model::{Relation, RowId};
use crate::solve::scalar::Scalar;

/// Records proof nodes until the leaf limit is passed, then only tracks the
/// minimum bound-leaf value.
pub(super) struct Recorder {
    nodes: Vec<ProofNode>,
    leaves: u32,
    max_leaves: u32,
    overflowed: bool,
    min_bound: Option<i128>,
}

impl Recorder {
    pub(super) fn new(max_leaves: u32) -> Self {
        Self {
            nodes: Vec::new(),
            leaves: 0,
            max_leaves,
            overflowed: false,
            min_bound: None,
        }
    }

    pub(super) fn branch(&mut self, var: usize, first: bool) {
        self.push(ProofNode::Branch {
            var: var_id(var),
            first,
        });
    }

    pub(super) fn forced(&mut self, var: usize, value: bool, row: usize) {
        self.push(ProofNode::Forced {
            var: var_id(var),
            value,
            row: row_id(row),
        });
    }

    /// Close a path with a bound dual whose value at this path is at least `value`.
    pub(super) fn bound_leaf(&mut self, dual: LagrangeDual, value: i128) {
        self.min_bound = Some(self.min_bound.map_or(value, |current| current.min(value)));
        self.leaf(LeafProof::Bound { dual });
    }

    /// Close a path on a row whose activity interval excludes its right-hand side.
    pub(super) fn infeasible_leaf(&mut self, relation: Relation, row: usize, exceeds: bool) {
        let numerator = match (relation, exceeds) {
            (Relation::LessEqual, _) | (Relation::Equal, true) => -1,
            (Relation::GreaterEqual, _) | (Relation::Equal, false) => 1,
        };
        let entry = DualEntry {
            row: row_id(row),
            numerator: Scalar::new(numerator),
        };
        let dual = LagrangeDual {
            denominator: 1,
            entries: vec![entry],
        };
        self.leaf(LeafProof::Infeasible { dual });
    }

    fn leaf(&mut self, proof: LeafProof) {
        self.leaves = self.leaves.saturating_add(1);
        if self.leaves > self.max_leaves {
            self.overflowed = true;
            self.nodes = Vec::new();
        }
        self.push(ProofNode::Leaf { proof });
    }

    fn push(&mut self, node: ProofNode) {
        if !self.overflowed {
            self.nodes.push(node);
        }
    }

    pub(super) fn min_bound(&self) -> Option<i128> {
        self.min_bound
    }

    /// The recorded tree, or `None` when it passed the leaf limit.
    pub(super) fn into_tree(self) -> Option<Vec<ProofNode>> {
        (!self.overflowed).then_some(self.nodes)
    }
}

/// Rows an infeasibility tree relies on: forced rows and infeasible-leaf rows.
pub(super) fn core_rows(nodes: &[ProofNode]) -> Vec<RowId> {
    let mut rows: Vec<RowId> = nodes
        .iter()
        .flat_map(|node| match node {
            ProofNode::Forced { row, .. } => vec![*row],
            ProofNode::Leaf {
                proof: LeafProof::Infeasible { dual },
            } => dual.entries.iter().map(|entry| entry.row).collect(),
            ProofNode::Leaf {
                proof: LeafProof::Bound { .. },
            }
            | ProofNode::Branch { .. } => Vec::new(),
        })
        .collect();
    rows.sort_unstable();
    rows.dedup();
    rows
}
