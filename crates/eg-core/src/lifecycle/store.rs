//! The in-memory lifecycle edge set the derivation pass writes (MEDULLA-SDLC-R001).
//! Derived edges enter only through [`LifecycleStore::accept_changeset`]; a
//! direct write of a derived kind refuses.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::lifecycle::{LifecycleEdgeKind, LifecycleKind, LifecycleRefusal};
use eg_types::source_changeset::Changeset;

use super::derive::{derive_lifecycle_edges, CodeGraphView, DerivedEdge};

/// Identity of an edge: kind, source and target.
pub type EdgeKey = (LifecycleEdgeKind, String, String);

/// Nodes and edges of one lifecycle graph.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LifecycleStore {
    nodes: BTreeMap<String, LifecycleKind>,
    edges: BTreeMap<EdgeKey, DerivedEdge>,
    adjacency: BTreeMap<String, BTreeSet<EdgeKey>>,
}

fn key_of(edge: &DerivedEdge) -> EdgeKey {
    (edge.kind, edge.from.clone(), edge.to.clone())
}

impl LifecycleStore {
    pub fn add_node(&mut self, id: &str, kind: LifecycleKind) {
        self.nodes.insert(id.to_owned(), kind);
    }

    pub fn node_kind(&self, id: &str) -> Option<LifecycleKind> {
        self.nodes.get(id).copied()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Every edge, in key order.
    pub fn edges(&self) -> impl Iterator<Item = &DerivedEdge> {
        self.edges.values()
    }

    /// Edges touching a node, in key order.
    pub fn edges_of(&self, node: &str) -> impl Iterator<Item = &DerivedEdge> + '_ {
        self.adjacency
            .get(node)
            .into_iter()
            .flatten()
            .filter_map(|key| self.edges.get(key))
    }

    /// The edge set as canonical bytes, for byte-for-byte comparison.
    pub fn edge_set_bytes(&self) -> Vec<u8> {
        let edges: Vec<&DerivedEdge> = self.edges.values().collect();
        serde_json::to_vec(&edges).unwrap_or_default()
    }

    fn check_endpoints(
        &self,
        edges: &[DerivedEdge],
        extra: Option<&str>,
    ) -> Result<(), LifecycleRefusal> {
        let known = |id: &str| self.nodes.contains_key(id) || extra == Some(id);
        for edge in edges {
            for end in [&edge.from, &edge.to] {
                if !known(end) {
                    return Err(LifecycleRefusal::EndpointUnknown(end.clone()));
                }
            }
        }
        Ok(())
    }

    fn insert_edges(&mut self, edges: Vec<DerivedEdge>) {
        for edge in edges {
            let key = key_of(&edge);
            for end in [&edge.from, &edge.to] {
                self.adjacency
                    .entry(end.clone())
                    .or_default()
                    .insert(key.clone());
            }
            self.edges.insert(key, edge);
        }
    }

    /// Insert derived edges atomically: an absent endpoint refuses the whole
    /// batch and writes nothing.
    pub fn apply_derived(&mut self, edges: Vec<DerivedEdge>) -> Result<(), LifecycleRefusal> {
        self.check_endpoints(&edges, None)?;
        self.insert_edges(edges);
        Ok(())
    }

    /// Accept a changeset: add its node, derive its edges and write them. An
    /// absent endpoint refuses and leaves the store unchanged.
    pub fn accept_changeset(
        &mut self,
        changeset: &Changeset,
        view: &CodeGraphView,
    ) -> Result<usize, LifecycleRefusal> {
        let edges = derive_lifecycle_edges(changeset, view);
        self.check_endpoints(&edges, Some(&changeset.id))?;
        self.add_node(&changeset.id, LifecycleKind::Changeset);
        let count = edges.len();
        self.insert_edges(edges);
        Ok(count)
    }

    /// A direct edge write. Derived kinds refuse: only a rule writes them.
    pub fn write_edge(
        &mut self,
        kind: LifecycleEdgeKind,
        from: &str,
        to: &str,
    ) -> Result<(), LifecycleRefusal> {
        if kind.is_derived() {
            return Err(LifecycleRefusal::EdgeDerivedOnly(kind.name()));
        }
        let edge = DerivedEdge {
            kind,
            from: from.to_owned(),
            to: to.to_owned(),
            rule: "manual".to_owned(),
            rule_version: 0,
            input_digests: Vec::new(),
        };
        self.check_endpoints(std::slice::from_ref(&edge), None)?;
        self.insert_edges(vec![edge]);
        Ok(())
    }

    /// Rebuild from the changeset log: replay every rule over the same nodes.
    pub fn rebuild(
        seed_nodes: &BTreeMap<String, LifecycleKind>,
        log: &[(Changeset, CodeGraphView)],
    ) -> Result<Self, LifecycleRefusal> {
        let mut store = Self {
            nodes: seed_nodes.clone(),
            ..Self::default()
        };
        for (changeset, view) in log {
            store.accept_changeset(changeset, view)?;
        }
        Ok(store)
    }

    pub fn nodes(&self) -> &BTreeMap<String, LifecycleKind> {
        &self.nodes
    }
}
