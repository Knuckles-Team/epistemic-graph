//! [`BeliefGraph`] — the compact support/contradiction/attack projection the
//! propagation walk runs over, plus the one adapter that builds it from an engine
//! [`GraphView`] snapshot.
//!
//! Isolating the blob-decode here keeps the propagation algorithm ([`crate::propagate`])
//! pure and unit-testable against hand-built fixtures ([`BeliefGraph::from_parts`]),
//! while the `GraphView` decode reuses the exact `rmp_serde -> serde_json::Value`
//! pattern the planner's FILTER leg already uses.

use std::collections::HashMap;

use eg_core::graph::GraphView;

use crate::model::{classify_relationship, EdgeKind, TimeAxis};

/// A read-only projection of a graph's epistemic topology: each node's stored
/// confidence prior, and the support/contradiction/attack edges INCOMING to each node.
#[derive(Clone, Debug, Default)]
pub struct BeliefGraph {
    /// node id → stored `NodeData.confidence` prior.
    pub priors: HashMap<String, f64>,
    /// target id → [(source id, kind)] — the evidence bearing on the target's belief.
    pub in_edges: HashMap<String, Vec<(String, EdgeKind)>>,
    /// The bitemporal instant this projection was pinned at (set when the caller
    /// composed an `AS OF` filter before building it); `None` = as of now.
    pub as_of: Option<(TimeAxis, u64)>,
    /// node id → its RLS [`RowVisibility`](eg_core::isolation::RowVisibility)
    /// (CONCEPT:EG-KG.sharding.row-level-security), decoded from the SAME property blob
    /// `filter_view` reads on every other read path. Consulted ONLY by
    /// [`crate::redact::explain_belief_redacted`] (EPI-P3-4) to decide, per proof-tree
    /// node, whether the requesting actor may see that node's identity. A node absent
    /// from this map (no property blob at all) is treated as unowned/visible — the
    /// same default `filter_view` applies. Behind `epistemic-redaction` so a plain
    /// build carries no extra dependency or per-node decode cost.
    #[cfg(feature = "epistemic-redaction")]
    pub node_visibility: HashMap<String, eg_core::isolation::RowVisibility>,
}

impl BeliefGraph {
    /// Build from a `GraphView` snapshot. Decodes each node's `confidence` and each
    /// epistemic edge's `relationship_type` from the msgpack property blobs; neutral
    /// (non-support/contradict/attack) edges are ignored.
    pub fn from_graph_view(view: &GraphView) -> Self {
        let mut priors = HashMap::with_capacity(view.node_properties.len());
        for (id, blob) in &view.node_properties {
            let confidence = rmp_serde::from_slice::<serde_json::Value>(blob)
                .ok()
                .and_then(|v| v.get("confidence").and_then(|c| c.as_f64()))
                .unwrap_or(1.0);
            priors.insert(id.clone(), confidence);
        }

        let mut in_edges: HashMap<String, Vec<(String, EdgeKind)>> = HashMap::new();
        for ((source, target), blobs) in &view.edge_properties {
            for blob in blobs {
                let Some(kind) = rmp_serde::from_slice::<serde_json::Value>(blob)
                    .ok()
                    .and_then(|v| {
                        v.get("relationship_type")
                            .and_then(|r| r.as_str())
                            .and_then(classify_relationship)
                    })
                else {
                    continue;
                };
                in_edges
                    .entry(target.clone())
                    .or_default()
                    .push((source.clone(), kind));
            }
        }

        #[cfg(feature = "epistemic-redaction")]
        let node_visibility = view
            .node_properties
            .iter()
            .map(|(id, blob)| (id.clone(), eg_core::isolation::row_visibility(blob)))
            .collect();

        BeliefGraph {
            priors,
            in_edges,
            as_of: None,
            #[cfg(feature = "epistemic-redaction")]
            node_visibility,
        }
    }

    /// Pin this projection to a bitemporal instant (the caller applied an `AS OF`
    /// filter upstream). Recorded on the resulting [`crate::BeliefState`].
    pub fn pinned_at(mut self, axis: TimeAxis, ts: u64) -> Self {
        self.as_of = Some((axis, ts));
        self
    }

    /// Test/utility constructor from explicit `(id, prior)` nodes and
    /// `(source, target, kind)` edges — no `GraphView` needed.
    pub fn from_parts<'a, N, E>(nodes: N, edges: E) -> Self
    where
        N: IntoIterator<Item = (&'a str, f64)>,
        E: IntoIterator<Item = (&'a str, &'a str, EdgeKind)>,
    {
        let priors = nodes
            .into_iter()
            .map(|(id, c)| (id.to_string(), c))
            .collect();
        let mut in_edges: HashMap<String, Vec<(String, EdgeKind)>> = HashMap::new();
        for (source, target, kind) in edges {
            in_edges
                .entry(target.to_string())
                .or_default()
                .push((source.to_string(), kind));
        }
        BeliefGraph {
            priors,
            in_edges,
            as_of: None,
            #[cfg(feature = "epistemic-redaction")]
            node_visibility: HashMap::new(),
        }
    }

    /// Attach explicit per-node [`RowVisibility`](eg_core::isolation::RowVisibility)
    /// (EPI-P3-4 test/utility constructor — a real caller builds this from a
    /// `GraphView` via [`Self::from_graph_view`]). Any id not given here defaults to
    /// unowned/visible, exactly like a node with no property blob at all.
    #[cfg(feature = "epistemic-redaction")]
    pub fn with_visibility<'a, V>(mut self, visibility: V) -> Self
    where
        V: IntoIterator<Item = (&'a str, eg_core::isolation::RowVisibility)>,
    {
        for (id, vis) in visibility {
            self.node_visibility.insert(id.to_string(), vis);
        }
        self
    }
}
