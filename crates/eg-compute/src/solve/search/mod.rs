//! Deterministic depth-first branch-and-bound.
//!
//! The search reads no clock and no hash order: every choice is a function of
//! integer data with ties broken by index, and work is limited by a node
//! budget. Its output is a [`Certificate`]; see `certificate.rs` for the proof
//! semantics and `verify` for the independent checker.

mod bound;
mod branch;
mod incumbent;
mod propagate;
mod record;
mod state;
mod status;

use std::rc::Rc;

use bound::{dual_ascent, NodeBound};
use branch::{choose, completion, Choice};
use propagate::{fix_and_queue, propagate, RowQueue};
use record::Recorder;
use state::SearchState;

use super::certificate::{Certificate, LagrangeDual, SolverConfig};
use super::model::Model;

/// Solve `model` to proven optimality, proven infeasibility or the node budget.
pub fn solve(model: &Model, config: &SolverConfig) -> Certificate {
    let mut search = Search::new(model, config);
    search.run();
    search.finish()
}

/// A node waiting to be expanded: undo to `trail_len`, then apply `fix`.
struct Pending {
    trail_len: usize,
    fix: Option<(usize, bool)>,
    parent: Option<Rc<NodeBound>>,
}

struct Search<'m> {
    config: SolverConfig,
    state: SearchState<'m>,
    queue: RowQueue,
    recorder: Recorder,
    incumbent: Option<(Vec<bool>, i128)>,
    stack: Vec<Pending>,
    root: Rc<NodeBound>,
    nodes_expanded: u64,
    exhausted: bool,
}

impl<'m> Search<'m> {
    fn new(model: &'m Model, config: &SolverConfig) -> Self {
        let state = SearchState::new(model);
        let root = Rc::new(dual_ascent(&state, config.bound_denominator()));
        Self {
            config: *config,
            state,
            queue: RowQueue::new(model.rows().len()),
            recorder: Recorder::new(config.max_certificate_leaves()),
            incumbent: incumbent::greedy(model),
            stack: vec![Pending {
                trail_len: 0,
                fix: None,
                parent: None,
            }],
            root,
            nodes_expanded: 0,
            exhausted: false,
        }
    }

    fn run(&mut self) {
        while let Some(node) = self.stack.pop() {
            if self.nodes_expanded >= self.config.node_budget() {
                self.close_open(node);
                continue;
            }
            self.nodes_expanded += 1;
            self.expand(node);
        }
    }

    /// Budget exhausted: close a pending node with its parent's dual, whose
    /// value can only rise under the node's extra fix.
    fn close_open(&mut self, node: Pending) {
        self.exhausted = true;
        let parent = node.parent.unwrap_or_else(|| Rc::clone(&self.root));
        self.recorder.bound_leaf(parent.dual.clone(), parent.value);
    }

    fn expand(&mut self, node: Pending) {
        self.state.undo_to(node.trail_len);
        match node.fix {
            Some((var, value)) => fix_and_queue(&mut self.state, &mut self.queue, var, value),
            None => self.queue.push_all(),
        }
        let recorder = &mut self.recorder;
        let mut forced = |var: usize, value: bool, row: usize| recorder.forced(var, value, row);
        if let Err(row) = propagate(&mut self.state, &mut self.queue, &mut forced) {
            self.close_infeasible(row);
            return;
        }
        match choose(&self.state) {
            Choice::Complete => self.close_complete(),
            Choice::Branch { var, first } => self.bound_or_branch(var, first),
        }
    }

    fn close_infeasible(&mut self, row: usize) {
        let spec = &self.state.model().rows()[row];
        let (low, _) = self.state.activity(row);
        let exceeds = low > i128::from(spec.rhs());
        self.recorder.infeasible_leaf(spec.relation(), row, exceeds);
    }

    /// Every row holds: the best completion is this subtree's optimum.
    fn close_complete(&mut self) {
        let (selected, value) = completion(&self.state);
        let improves = self
            .incumbent
            .as_ref()
            .is_none_or(|(_, best)| value < *best);
        if improves {
            self.incumbent = Some((selected, value));
        }
        let dual = LagrangeDual {
            denominator: 1,
            entries: Vec::new(),
        };
        self.recorder.bound_leaf(dual, value);
    }

    fn bound_or_branch(&mut self, var: usize, first: bool) {
        let bound = dual_ascent(&self.state, self.config.bound_denominator());
        let pruned = self
            .incumbent
            .as_ref()
            .is_some_and(|(_, best)| bound.value >= *best);
        if pruned {
            self.recorder.bound_leaf(bound.dual, bound.value);
            return;
        }
        self.recorder.branch(var, first);
        let parent = Rc::new(bound);
        let trail_len = self.state.trail_len();
        for value in [!first, first] {
            let parent = Some(Rc::clone(&parent));
            self.stack.push(Pending {
                trail_len,
                fix: Some((var, value)),
                parent,
            });
        }
    }
}
