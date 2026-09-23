//! The tableau's search: deterministic saturation, non-deterministic choice points and
//! chronological backtracking, over an explicit stack (EH-355).
//!
//! Three things keep one choice cheap on a large ABox component, where the TBox's
//! internalized GCIs put a `⊔` into every node label and a search makes thousands of
//! choices:
//!
//! * **Local saturation.** Before a choice the graph is saturated; a `⊔`/choose branch
//!   then adds one concept to one node. Only that node and, through the `∀`-rule, its
//!   successors can gain consequences, so they are saturated from a worklist instead of
//!   re-running every rule over every node. Anything the worklist cannot bound locally —
//!   a nominal (which can merge nodes), a node merge, or a qualified `≤n r.C` (whose
//!   clash can be caused by a neighbour's label) — falls back to the full saturation.
//! * **A resumable disjunction scan.** Which unresolved `⊔` is chosen next does not
//!   affect soundness or completeness, so the scan resumes where the last one stopped
//!   instead of restarting at node 0.
//! * **Copy-on-write snapshots** ([`super::store`]).
//!
//! And one thing keeps a clash from costing the whole search: **dependency-directed
//! backjumping** (Horrocks, *Optimising Tableaux Decision Procedures for Description
//! Logics*, 1997). Every node records the open choice points its label and edges can
//! depend on (a superset: a branch adds its own, and the ∀-rule, generation and merges
//! carry the source's), and a clash reports the union over the nodes it involves. A
//! choice point outside that set cannot have caused the clash, so its untried
//! alternatives would clash the same way and are skipped. Chronological backtracking
//! instead retried every unrelated `⊔` choice made after the culprit — exponential on
//! an ABox where every individual carries the internalized GCIs.

use std::collections::BTreeSet;

use super::{Branch, Completion, Dl, NODE_CAP};

/// Open choice points (positions on the search stack) a fact can depend on.
pub(super) type Deps = BTreeSet<usize>;

/// Where [`Completion::run_to_choice`] stopped.
enum Progress {
    Clash(Deps),
    Exhausted,
    Complete,
    Choice(Vec<Branch>, Trigger),
}

/// What made a choice point necessary; its failure depends on it too.
enum Trigger {
    /// A `⊔` in this node's label.
    Node(usize),
    /// A choose or `≤` rule, which involves a node and its neighbours: conservatively,
    /// every earlier choice point.
    Any,
}

/// The state after a branch was applied and locally saturated.
enum Local {
    /// Saturated and clash-free: the full saturation can be skipped.
    Settled,
    Clash(Deps),
    /// Not decidable locally: run the full saturation.
    Unsettled,
}

/// A choice point on the search stack: the graph it was taken on, its untried
/// alternatives, and the dependencies of the clashes its tried alternatives hit.
struct OpenChoice {
    graph: Completion,
    alternatives: std::vec::IntoIter<Branch>,
    failed: Deps,
}

/// `≤n r.C` with a qualifying filler `C ≠ ⊤`.
pub(super) fn is_qualified_max(concept: &Dl) -> bool {
    matches!(concept, Dl::Max(_, _, filler) if **filler != Dl::Top)
}

impl Completion {
    /// Is there a clash-free complete completion reachable from this graph? The tableau
    /// decision procedure: saturate deterministically, branch on a non-determinism, and
    /// backjump on a clash. `true` ⇒ satisfiable.
    ///
    /// The search is depth-first over an explicit stack of open choice points, each
    /// holding the graph it branched from and its untried alternatives, so its depth —
    /// one level per choice on the current path, which grows with the ABox — costs heap,
    /// not thread stack. On a spent budget it returns `false` and the caller reads the
    /// verdict from the budget.
    pub(super) fn expand(&mut self) -> bool {
        let mut open: Vec<OpenChoice> = Vec::new();
        let mut local = Local::Unsettled;
        loop {
            if !self.budget.charge() {
                return false;
            }
            let progress = match std::mem::replace(&mut local, Local::Unsettled) {
                Local::Clash(deps) => Progress::Clash(deps),
                Local::Settled => self.run_to_choice(true),
                Local::Unsettled => self.run_to_choice(false),
            };
            let deps = match progress {
                Progress::Complete => return true,
                Progress::Exhausted => return false,
                Progress::Clash(deps) => deps,
                Progress::Choice(branches, trigger) => {
                    let position = open.len();
                    let failed = match trigger {
                        Trigger::Node(i) => self.nodes[self.find(i)].deps.clone(),
                        Trigger::Any => (0..position).collect(),
                    };
                    let mut alternatives = branches.into_iter();
                    match alternatives.next() {
                        Some(first) => {
                            open.push(OpenChoice {
                                graph: self.clone(),
                                alternatives,
                                failed,
                            });
                            local = self.apply_branch(first, position);
                            continue;
                        }
                        // No alternative at all: fails on what made the choice necessary.
                        None => failed,
                    }
                }
            };
            if !self.resume_after(backjump(&mut open, deps), &mut local) {
                return false;
            }
        }
    }

    /// Continue the search on the alternative [`backjump`] chose; `false` (and
    /// nothing changed) when none is left.
    fn resume_after(
        &mut self,
        next: Option<(Completion, Branch, usize)>,
        local: &mut Local,
    ) -> bool {
        let Some((graph, branch, position)) = next else {
            *local = Local::Clash(Deps::new());
            return false;
        };
        *self = graph;
        *local = self.apply_branch(branch, position);
        true
    }

    /// Run the deterministic rules until the graph clashes, is complete, or reaches a
    /// non-deterministic choice point. `settled`: the graph is already saturated.
    fn run_to_choice(&mut self, mut settled: bool) -> Progress {
        loop {
            if !self.budget.charge() {
                return Progress::Exhausted;
            }
            // (1) Saturate the non-generating deterministic rules to fixpoint.
            if !std::mem::take(&mut settled) {
                if let Some(deps) = self.saturate_nongenerating() {
                    return Progress::Clash(deps);
                }
            }
            // (2) Stop at a non-deterministic choice point (⊔, then choose, then ≤).
            if let Some((branches, trigger)) = self.next_nondet() {
                return Progress::Choice(branches, trigger);
            }
            // (3) Generating rules last (∃ / ≥) so blocking is checked on stable labels.
            if self.nodes.len() >= NODE_CAP {
                return Progress::Complete; // safety valve (see NODE_CAP)
            }
            if !self.step_generating() {
                // (4) No rule applies and no clash ⇒ a clash-free complete model exists.
                return Progress::Complete;
            }
        }
    }

    fn next_nondet(&mut self) -> Option<(Vec<Branch>, Trigger)> {
        if let Some(found) = self.find_or_rule_branch() {
            return Some(found);
        }
        self.find_choose_rule_branch()
            .or_else(|| self.find_max_rule_branch())
            .map(|branches| (branches, Trigger::Any))
    }

    /// The ⊔-rule: a node with an unresolved `Or`, scanning from where the previous scan
    /// stopped and wrapping around.
    fn find_or_rule_branch(&mut self) -> Option<(Vec<Branch>, Trigger)> {
        let count = self.nodes.len();
        for offset in 0..count {
            let i = (self.cursor + offset) % count;
            if self.find(i) != i {
                continue;
            }
            if let Some(branches) = self.unresolved_or(i) {
                self.cursor = i;
                return Some((branches, Trigger::Node(i)));
            }
        }
        None
    }

    fn unresolved_or(&self, i: usize) -> Option<Vec<Branch>> {
        let label = &self.nodes[i].label;
        label.iter().find_map(|c| match c {
            Dl::Or(ds) if !ds.iter().any(|d| label.contains(d)) => Some(
                ds.iter()
                    .map(|d| Branch::AddConcept(i, d.clone()))
                    .collect(),
            ),
            _ => None,
        })
    }

    /// Apply one alternative of the choice point at stack `position`; the changed node
    /// now depends on that choice point.
    fn apply_branch(&mut self, branch: Branch, position: usize) -> Local {
        match branch {
            Branch::AddConcept(i, c) => {
                let r = self.find(i);
                self.add_label(r, c);
                self.nodes.get_mut(r).deps.insert(position);
                self.saturate_local(r)
            }
            Branch::Merge(a, b) => {
                self.union(a, b);
                let r = self.find(a);
                self.nodes.get_mut(r).deps.insert(position);
                Local::Unsettled
            }
        }
    }

    /// `to` now carries a consequence of `from`'s label, so it depends on whatever
    /// `from` depends on.
    pub(super) fn inherit_deps(&mut self, to: usize, from: usize) {
        let from = self.find(from);
        let to = self.find(to);
        if self.nodes[from].deps.is_subset(&self.nodes[to].deps) {
            return;
        }
        let deps = self.nodes[from].deps.clone();
        self.nodes.get_mut(to).deps.extend(deps);
    }

    /// Saturate the non-generating deterministic rules to fixpoint; the dependencies of
    /// a clash if one arises.
    pub(super) fn saturate_nongenerating(&mut self) -> Option<Deps> {
        loop {
            if let Some(deps) = self.find_clash() {
                return Some(deps);
            }
            if !self.step_nongenerating() {
                return None;
            }
        }
    }

    /// A clash in the current graph — `⊥`, `{A,¬A}`, `{ {a},¬{a} }`, a self-inequality,
    /// or a `≤n r.C` with `n+1` pairwise-distinct `C`-witnesses — with its dependencies.
    pub(super) fn find_clash(&self) -> Option<Deps> {
        for (a, b) in &self.neq {
            let (a, b) = (self.find(*a), self.find(*b));
            if a == b {
                return Some(self.nodes[a].deps.clone());
            }
        }
        (0..self.nodes.len())
            .filter(|&i| self.find(i) == i)
            .find_map(|i| self.clash_at(i))
    }

    /// Node `i`'s clash, if any, depending on `i` and — for a cardinality clash, which
    /// counts them — its neighbours.
    fn clash_at(&self, i: usize) -> Option<Deps> {
        if !self.node_has_clash(i) {
            return None;
        }
        let mut deps = self.nodes[i].deps.clone();
        if self.nodes[i].label.iter().any(|c| matches!(c, Dl::Max(..))) {
            for (_, t) in self.out_edges(i) {
                deps.extend(self.nodes[t].deps.iter().copied());
            }
            for (a, b) in &self.neq {
                deps.extend(self.nodes[self.find(*a)].deps.iter().copied());
                deps.extend(self.nodes[self.find(*b)].deps.iter().copied());
            }
        }
        Some(deps)
    }

    /// Saturate the consequences of a label change at `start`: the ⊓-rule and lazy
    /// unfolding at each changed node, the ∀-rule along its outgoing edges.
    fn saturate_local(&mut self, start: usize) -> Local {
        let mut work = vec![start];
        let mut touched = Vec::new();
        while let Some(i) = work.pop() {
            let i = self.find(i);
            while self.unfold_node(i) {}
            if self.nodes[i]
                .label
                .iter()
                .any(|c| matches!(c, Dl::Nominal(_)))
            {
                return Local::Unsettled;
            }
            touched.push(i);
            work.extend(self.apply_all_rule_at(i));
        }
        let clash = if self.qualified_max {
            self.find_clash()
        } else {
            touched.iter().find_map(|&i| self.clash_at(i))
        };
        match clash {
            Some(deps) => Local::Clash(deps),
            None => Local::Settled,
        }
    }
}

/// Backjump from a clash that depends on `deps`: close every choice point the clash
/// does not depend on (its other alternatives would clash the same way), and return the
/// next untried alternative of the innermost one it does depend on, with a copy of the
/// graph that choice was taken on and its stack position. A choice point whose
/// alternatives are all exhausted fails on the union of their clashes' dependencies.
/// `None` when no alternative is left anywhere.
fn backjump(open: &mut Vec<OpenChoice>, mut deps: Deps) -> Option<(Completion, Branch, usize)> {
    while let Some(position) = open.len().checked_sub(1) {
        if !deps.remove(&position) {
            open.pop();
            continue;
        }
        let top = &mut open[position];
        top.failed.extend(std::mem::take(&mut deps));
        if let Some(branch) = top.alternatives.next() {
            return Some((top.graph.clone(), branch, position));
        }
        deps = std::mem::take(&mut top.failed);
        open.pop();
    }
    None
}
