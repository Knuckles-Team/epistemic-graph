//! The search frontier and path reconstruction shared by this module's shortest-path
//! searches (CONCEPT:EG-KG.domains.geo-routing).
//!
//! [`std::collections::BinaryHeap`] is a MAX-heap, so both frontier entries invert their
//! comparison to pop the smallest priority first, and both treat an incomparable (NaN)
//! priority as equal so it sorts last instead of panicking. The two reconstruction walks
//! mirror the two search-state shapes: a plain node predecessor array, and the
//! `(prev, node)` state chain a turn-aware search needs so a node can be re-entered from a
//! different predecessor.

use std::cmp::Ordering;
use std::collections::HashMap;

use super::Path;

/// Walk a `prev` predecessor array back from `target` to `source`, returning the node ids
/// in travel order. `None` when the chain breaks before reaching the source, which means
/// no path was found.
pub(super) fn reconstruct_prev(source: usize, target: usize, prev: &[usize]) -> Option<Vec<usize>> {
    let mut nodes = vec![target];
    let mut cur = target;
    while cur != source {
        let p = prev[cur];
        if p == usize::MAX {
            return None;
        }
        nodes.push(p);
        cur = p;
    }
    nodes.reverse();
    Some(nodes)
}

/// Rebuild the node sequence from a `prev` predecessor array; `None` if `target` was never
/// reached.
pub(super) fn reconstruct(
    source: usize,
    target: usize,
    dist: &[f64],
    prev: &[usize],
) -> Option<Path> {
    if !dist[target].is_finite() {
        return None;
    }
    let mut nodes = vec![target];
    let mut cur = target;
    while cur != source {
        let p = prev[cur];
        if p == usize::MAX {
            return None;
        }
        nodes.push(p);
        cur = p;
    }
    nodes.reverse();
    Some(Path {
        nodes,
        cost: dist[target],
    })
}

/// The inverted priority order both frontier entries use: `BinaryHeap` is a max-heap, so
/// comparing the OTHER entry's priority against ours makes the smallest priority pop first.
/// An incomparable (NaN) priority compares equal, so it sorts last instead of panicking.
fn smallest_priority_first(ours: f64, theirs: f64) -> Ordering {
    theirs.partial_cmp(&ours).unwrap_or(Ordering::Equal)
}

/// A priority-queue frontier entry ordered so [`std::collections::BinaryHeap`] (a
/// max-heap) pops the **smallest** `priority` first. NaN sorts last.
pub(super) struct Frontier {
    pub(super) priority: f64,
    pub(super) cost: f64,
    pub(super) node: usize,
}
impl PartialEq for Frontier {
    fn eq(&self, o: &Self) -> bool {
        self.priority == o.priority
    }
}
impl Eq for Frontier {}
impl PartialOrd for Frontier {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Frontier {
    fn cmp(&self, o: &Self) -> Ordering {
        smallest_priority_first(self.priority, o.priority)
    }
}

/// A turn-aware priority-queue entry (CONCEPT:EG-KG.domains.geo-partitioning): like [`Frontier`] but the search state
/// is the directed edge `(prev, node)` just travelled, so a node can be re-entered from a
/// different predecessor. Ordered so the smallest `priority` pops first.
pub(super) struct TurnFrontier {
    pub(super) priority: f64,
    pub(super) cost: f64,
    pub(super) prev: usize,
    pub(super) node: usize,
}
impl PartialEq for TurnFrontier {
    fn eq(&self, o: &Self) -> bool {
        self.priority == o.priority
    }
}
impl Eq for TurnFrontier {}
impl PartialOrd for TurnFrontier {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for TurnFrontier {
    fn cmp(&self, o: &Self) -> Ordering {
        smallest_priority_first(self.priority, o.priority)
    }
}

/// Rebuild the node sequence for a turn-aware search (CONCEPT:EG-KG.domains.geo-partitioning) by walking the
/// `(prev, node)` predecessor-state chain back to the start state.
pub(super) fn reconstruct_turns(
    winning: (usize, usize),
    cost: f64,
    prev_state: &HashMap<(usize, usize), (usize, usize)>,
) -> Option<Path> {
    let mut nodes = Vec::new();
    let mut st = winning;
    loop {
        nodes.push(st.1);
        match prev_state.get(&st) {
            Some(&p) => st = p,
            None => break, // start state (prev == usize::MAX): its node is the source
        }
    }
    nodes.reverse();
    Some(Path { nodes, cost })
}
