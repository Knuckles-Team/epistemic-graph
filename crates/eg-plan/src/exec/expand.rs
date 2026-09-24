//! `MATCH ()` and the general traversal (UQL-03/UQL-05).
//!
//! [`scan_all`] seeds every node of the (already RLS/lease-filtered) snapshot in id
//! order. [`expand_op`] is `Op::Expand`: a breadth-first walk from the current rows along
//! edges in the requested direction, optionally restricted to one relationship and to
//! edges whose property blob satisfies the edge predicates (SQL three-valued logic via
//! `crate::pred_eval`). Like `Traverse` it returns the nodes reached at depth
//! `min..=max`, in discovery order, and it is bounded by the plan's traversal budget.

use std::collections::HashSet;

use eg_core::graph::GraphView;
use eg_types::wire::{EdgeDir, Pred};
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::EdgeRef;
use serde_json::{Map, Value};

use crate::budget::Budget;
use crate::rowset::RowSet;

/// Every node id of the snapshot, sorted (deterministic).
pub(crate) fn scan_all(view: &GraphView) -> RowSet {
    let mut ids: Vec<String> = view.node_properties.keys().cloned().collect();
    ids.sort_unstable();
    RowSet::from_ids(ids)
}

/// What an edge must satisfy to be followed.
pub(crate) struct EdgeFilter<'p> {
    pub rel: Option<&'p str>,
    pub preds: &'p [Pred],
}

impl EdgeFilter<'_> {
    /// The properties of the first stored edge blob between `from → to` that passes
    /// the filter (empty when an unfiltered edge carries no blob); `None` when none does.
    pub(crate) fn admitted(
        &self,
        view: &GraphView,
        from: &str,
        to: &str,
    ) -> Option<Map<String, Value>> {
        let Some(blobs) = view
            .edge_properties
            .get(&(from.to_string(), to.to_string()))
        else {
            return (self.rel.is_none() && self.preds.is_empty()).then(Map::new);
        };
        blobs.iter().map(|blob| decode(blob)).find(|props| {
            let rel_ok = self
                .rel
                .is_none_or(|rel| props.get("relationship").and_then(Value::as_str) == Some(rel));
            rel_ok && crate::pred_eval::all_hold(props, self.preds)
        })
    }
}

fn decode(blob: &[u8]) -> Map<String, Value> {
    eg_types::msgpack::decode_property_value(blob)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default()
}

/// The directions a [`EdgeDir`] walks.
pub(crate) fn directions(dir: EdgeDir) -> &'static [petgraph::Direction] {
    match dir {
        EdgeDir::Out => &[petgraph::Direction::Outgoing],
        EdgeDir::In => &[petgraph::Direction::Incoming],
        EdgeDir::Both => &[petgraph::Direction::Outgoing, petgraph::Direction::Incoming],
    }
}

/// The admitted edges of `node` in `dir`: `(neighbour, admitted edge properties)`.
pub(crate) fn admitted_edges(
    view: &GraphView,
    node: NodeIndex,
    dir: EdgeDir,
    filter: &EdgeFilter<'_>,
) -> Vec<(NodeIndex, Map<String, Value>)> {
    let mut out = Vec::new();
    for &d in directions(dir) {
        for edge in view.graph.edges_directed(node, d) {
            let (from, to) = (&view.graph[edge.source()], &view.graph[edge.target()]);
            let other = if d == petgraph::Direction::Outgoing {
                edge.target()
            } else {
                edge.source()
            };
            if let Some(props) = filter.admitted(view, from, to) {
                out.push((other, props));
            }
        }
    }
    out
}

/// The (edge-filtered) neighbours of `node` in `dir`.
fn neighbours(
    view: &GraphView,
    node: NodeIndex,
    dir: EdgeDir,
    filter: &EdgeFilter<'_>,
) -> Vec<NodeIndex> {
    admitted_edges(view, node, dir, filter)
        .into_iter()
        .map(|(other, _)| other)
        .collect()
}

/// `Op::Expand`: nodes reached from `input` at depth `min..=max` along admitted edges.
pub(crate) fn expand_op(
    view: &GraphView,
    input: &RowSet,
    dir: EdgeDir,
    hops: (usize, usize),
    filter: &EdgeFilter<'_>,
    budget: &Budget,
) -> Result<RowSet, String> {
    let (min, max) = hops;
    let mut frontier: Vec<NodeIndex> = input
        .ids()
        .iter()
        .filter_map(|id| view.node_map.get(id).copied())
        .collect();
    let mut visited: HashSet<NodeIndex> = frontier.iter().copied().collect();
    let mut reached: Vec<String> = Vec::new();
    if min == 0 {
        reached.extend(frontier.iter().map(|&n| view.graph[n].clone()));
    }
    let mut depth = 0;
    while depth < max && !frontier.is_empty() {
        depth += 1;
        let mut next = Vec::new();
        for &node in &frontier {
            for nbr in neighbours(view, node, dir, filter) {
                if visited.insert(nbr) {
                    next.push(nbr);
                }
            }
        }
        budget.check_traversal(visited.len())?;
        if depth >= min {
            reached.extend(next.iter().map(|&n| view.graph[n].clone()));
        }
        frontier = next;
    }
    Ok(RowSet::from_ids(reached))
}
