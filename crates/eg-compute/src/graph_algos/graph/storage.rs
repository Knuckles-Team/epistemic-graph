use std::collections::HashMap;

/// The compact storage shared by the graph's public capability views.
#[derive(Debug, Clone)]
pub(super) struct GraphStorage<N> {
    /// Compact index → node id. Sorted, unique — the determinism anchor.
    pub(super) nodes: Vec<N>,
    /// Node id → compact index.
    pub(super) index: HashMap<N, usize>,
    /// Outgoing edges per node, each sorted by target index, weights merged.
    pub(super) out: Vec<Vec<(usize, f64)>>,
    /// Incoming edges per node, each sorted by source index, weights merged.
    pub(super) inc: Vec<Vec<(usize, f64)>>,
}

/// Turn an index→weight map into a sorted `(idx, weight)` edge list.
pub(super) fn sorted_edges(m: HashMap<usize, f64>) -> Vec<(usize, f64)> {
    let mut v: Vec<(usize, f64)> = m.into_iter().collect();
    v.sort_unstable_by_key(|(i, _)| *i);
    v
}
