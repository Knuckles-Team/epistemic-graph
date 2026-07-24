//! The alignment graph (CONCEPT:EG-P1-3) — a typed structure connecting document
//! spans, image regions, audio intervals, video frames/shots, extracted entities,
//! claims, and source blobs.
//!
//! Every "located evidence" node ([`AlignmentNode::Evidence`]) wraps an
//! `eg_modality::EvidenceLocus`, so this crate never needs a parallel located-
//! evidence type. Entities, claims, and source blobs are referenced by
//! bare id (they live in the KG one layer up — this crate does not know or care
//! whether an `entity_id`/`claim_id` resolves to a real KG node; it only records
//! that an alignment edge points at one).
//!
//! This is a plain in-memory graph (`Vec` of nodes + `Vec` of typed edges), not a
//! full query engine — it is a shared WIRE/STORAGE shape a caller builds once
//! (e.g. per extraction run, or per document) and later reads back (`node`,
//! `edges_from`, `path_exists`) or resolves through an [`crate::resolver::EvidenceResolver`].

use eg_modality::EvidenceLocus;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// A stable handle to a node inside one [`AlignmentGraph`] — an index into its
/// node vec. Only meaningful relative to the graph that produced it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AlignmentNodeId(pub usize);

/// One node of the alignment graph. `Evidence` covers every located-artifact
/// kind (document span, image region, audio interval, video frame/shot, table
/// cell, code symbol, trace span — see `eg_modality::EvidenceLocus`); `Entity`/
/// `Claim`/`Blob` are bare KG-id references for the non-artifact-located
/// endpoints an alignment can also connect.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AlignmentNode {
    /// A located reference into a source artifact (document span, image region,
    /// audio interval, video frame/shot, …).
    Evidence(EvidenceLocus),
    /// An extracted entity, by its KG id.
    Entity { entity_id: String },
    /// A claim, by its KG id.
    Claim { claim_id: String },
    /// A source blob, by its content-addressed blob ref.
    Blob { blob_ref: String },
}

/// The typed relation an [`AlignmentLink`] carries between two [`AlignmentNode`]s.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlignmentRelation {
    /// The `from` evidence was extracted/derived from the `to` source blob.
    DerivedFrom,
    /// The `from` and `to` evidence co-occur (same page/timestamp window/frame,
    /// a cross-modal correlation with no derivation direction implied).
    CoOccursWith,
    /// The `from` evidence mentions the `to` entity.
    MentionsEntity,
    /// The `from` node (evidence or entity) supports the `to` claim.
    SupportsClaim,
    /// The `from` node (evidence or entity) contradicts the `to` claim.
    ContradictsClaim,
}

/// A directed, typed edge between two nodes (by [`AlignmentNodeId`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlignmentLink {
    pub from: AlignmentNodeId,
    pub to: AlignmentNodeId,
    pub relation: AlignmentRelation,
}

/// The alignment graph itself: a node list + a typed edge list. Construction is
/// purely additive (`add_node`/`add_edge`) — this crate never infers an edge on
/// its own; every alignment is caller-asserted.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AlignmentGraph {
    nodes: Vec<AlignmentNode>,
    edges: Vec<AlignmentLink>,
}

impl AlignmentGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a node, returning the id it can be referenced by (`add_edge`, `node`).
    pub fn add_node(&mut self, node: AlignmentNode) -> AlignmentNodeId {
        let id = AlignmentNodeId(self.nodes.len());
        self.nodes.push(node);
        id
    }

    /// Add a directed, typed edge between two already-added nodes. Panics if
    /// either id is out of range (a caller-programming error — ids only ever
    /// come from this same graph's own `add_node`).
    pub fn add_edge(
        &mut self,
        from: AlignmentNodeId,
        to: AlignmentNodeId,
        relation: AlignmentRelation,
    ) {
        assert!(
            from.0 < self.nodes.len(),
            "add_edge: `from` id out of range"
        );
        assert!(to.0 < self.nodes.len(), "add_edge: `to` id out of range");
        self.edges.push(AlignmentLink { from, to, relation });
    }

    pub fn node(&self, id: AlignmentNodeId) -> Option<&AlignmentNode> {
        self.nodes.get(id.0)
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Every outgoing edge from `id`, in insertion order.
    pub fn edges_from(&self, id: AlignmentNodeId) -> impl Iterator<Item = &AlignmentLink> {
        self.edges.iter().filter(move |e| e.from == id)
    }

    /// Every distinct node reachable in ONE outgoing hop from `id`.
    pub fn neighbors(&self, id: AlignmentNodeId) -> Vec<AlignmentNodeId> {
        self.edges_from(id).map(|e| e.to).collect()
    }

    /// Whether `to` is reachable from `from` by following outgoing edges
    /// (breadth-first, direction-respecting) — the connectivity check "this doc
    /// span aligns (transitively) to this claim" reduces to.
    pub fn path_exists(&self, from: AlignmentNodeId, to: AlignmentNodeId) -> bool {
        if from == to {
            return true;
        }
        let mut seen = vec![false; self.nodes.len()];
        let mut queue = VecDeque::new();
        seen[from.0] = true;
        queue.push_back(from);
        while let Some(cur) = queue.pop_front() {
            for next in self.neighbors(cur) {
                if next == to {
                    return true;
                }
                if !seen[next.0] {
                    seen[next.0] = true;
                    queue.push_back(next);
                }
            }
        }
        false
    }

    /// Resolve the [`AlignmentNode::Evidence`] at `id` through an
    /// [`crate::resolver::EvidenceResolver`]. `None` when `id` is out of range,
    /// names a non-`Evidence` node (an `Entity`/`Claim`/`Blob` has no located
    /// span to resolve), or the resolver has nothing for that artifact.
    pub fn resolve_evidence<R: crate::resolver::EvidenceResolver>(
        &self,
        id: AlignmentNodeId,
        resolver: &R,
    ) -> Option<crate::resolver::ResolvedArtifact> {
        match self.node(id)? {
            AlignmentNode::Evidence(span) => resolver.resolve(span),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_modality::{
        ArtifactId, DerivationId, EvidenceAddress, EvidenceLocusId, OpaqueRef, ResourceId,
    };
    use std::collections::HashMap;

    /// Test-only `EvidenceResolver` fixture — see the identical helper in
    /// `resolver.rs`'s own test module for why this crate keeps no shipped
    /// resolver implementation.
    struct FixtureResolver(HashMap<OpaqueRef, crate::resolver::ResolvedArtifact>);

    impl FixtureResolver {
        fn new() -> Self {
            Self(HashMap::new())
        }

        fn with_text(mut self, subject: OpaqueRef, excerpt: impl Into<String>) -> Self {
            self.0.insert(
                subject.clone(),
                crate::resolver::ResolvedArtifact::Text {
                    subject_ref: subject.to_string(),
                    excerpt: excerpt.into(),
                },
            );
            self
        }
    }

    impl crate::resolver::EvidenceResolver for FixtureResolver {
        fn resolve(&self, locus: &EvidenceLocus) -> Option<crate::resolver::ResolvedArtifact> {
            self.0.get(crate::resolver::subject_ref(locus)).cloned()
        }
    }

    fn r(namespace: &str, suffix: u8) -> OpaqueRef {
        OpaqueRef::scoped(namespace, &format!("00000000000000{suffix:02x}")).unwrap()
    }

    fn locus(address: EvidenceAddress, suffix: u8) -> EvidenceLocus {
        EvidenceLocus {
            id: EvidenceLocusId::from_token(&format!("00000000000000{suffix:02x}")).unwrap(),
            subject: ResourceId::Artifact(
                ArtifactId::from_token(&format!("00000000000001{suffix:02x}")).unwrap(),
            ),
            address,
            policy_ref: r("policy", suffix),
            derivation_ref: DerivationId::from_token(&format!("00000000000002{suffix:02x}"))
                .unwrap(),
        }
    }

    #[test]
    fn add_node_returns_sequential_ids() {
        let mut g = AlignmentGraph::new();
        let a = g.add_node(AlignmentNode::Blob {
            blob_ref: "b1".to_string(),
        });
        let b = g.add_node(AlignmentNode::Entity {
            entity_id: "e1".to_string(),
        });
        assert_eq!(a, AlignmentNodeId(0));
        assert_eq!(b, AlignmentNodeId(1));
        assert_eq!(g.node_count(), 2);
    }

    #[test]
    fn edges_from_only_returns_outgoing_edges() {
        let mut g = AlignmentGraph::new();
        let a = g.add_node(AlignmentNode::Entity {
            entity_id: "a".to_string(),
        });
        let b = g.add_node(AlignmentNode::Entity {
            entity_id: "b".to_string(),
        });
        let c = g.add_node(AlignmentNode::Entity {
            entity_id: "c".to_string(),
        });
        g.add_edge(a, b, AlignmentRelation::CoOccursWith);
        g.add_edge(c, a, AlignmentRelation::CoOccursWith);
        let from_a: Vec<_> = g.edges_from(a).collect();
        assert_eq!(from_a.len(), 1);
        assert_eq!(from_a[0].to, b);
    }

    #[test]
    fn path_exists_follows_a_transitive_chain() {
        let mut g = AlignmentGraph::new();
        let doc_span = g.add_node(AlignmentNode::Evidence(locus(
            EvidenceAddress::CharacterRange { start: 0, end: 10 },
            1,
        )));
        let image_region = g.add_node(AlignmentNode::Evidence(locus(
            EvidenceAddress::ImageRegion {
                x: 0.0,
                y: 0.0,
                width: 10.0,
                height: 10.0,
            },
            2,
        )));
        let claim = g.add_node(AlignmentNode::Claim {
            claim_id: "claim-1".to_string(),
        });
        g.add_edge(doc_span, image_region, AlignmentRelation::CoOccursWith);
        g.add_edge(image_region, claim, AlignmentRelation::SupportsClaim);
        assert!(g.path_exists(doc_span, claim));
        assert!(!g.path_exists(claim, doc_span), "edges are directed");
    }

    #[test]
    fn path_exists_is_true_for_a_node_and_itself() {
        let mut g = AlignmentGraph::new();
        let a = g.add_node(AlignmentNode::Blob {
            blob_ref: "b".to_string(),
        });
        assert!(g.path_exists(a, a));
    }

    #[test]
    fn path_exists_is_false_when_unconnected() {
        let mut g = AlignmentGraph::new();
        let a = g.add_node(AlignmentNode::Blob {
            blob_ref: "a".to_string(),
        });
        let b = g.add_node(AlignmentNode::Blob {
            blob_ref: "b".to_string(),
        });
        assert!(!g.path_exists(a, b));
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn add_edge_panics_on_an_id_from_another_graph() {
        let mut g = AlignmentGraph::new();
        g.add_edge(
            AlignmentNodeId(0),
            AlignmentNodeId(1),
            AlignmentRelation::CoOccursWith,
        );
    }

    #[test]
    fn resolve_evidence_returns_none_for_a_non_evidence_node() {
        let mut g = AlignmentGraph::new();
        let claim = g.add_node(AlignmentNode::Claim {
            claim_id: "c1".to_string(),
        });
        let resolver = FixtureResolver::new();
        assert_eq!(g.resolve_evidence(claim, &resolver), None);
    }

    #[test]
    fn resolve_evidence_resolves_a_registered_evidence_node() {
        let mut g = AlignmentGraph::new();
        let evidence = locus(EvidenceAddress::CharacterRange { start: 0, end: 5 }, 1);
        let subject = evidence.subject.opaque().clone();
        let doc_span = g.add_node(AlignmentNode::Evidence(evidence));
        let resolver = FixtureResolver::new().with_text(subject, "hello");
        assert!(g.resolve_evidence(doc_span, &resolver).is_some());
    }

    #[test]
    fn serde_round_trips() {
        let mut g = AlignmentGraph::new();
        let a = g.add_node(AlignmentNode::Entity {
            entity_id: "a".to_string(),
        });
        let b = g.add_node(AlignmentNode::Claim {
            claim_id: "c".to_string(),
        });
        g.add_edge(a, b, AlignmentRelation::SupportsClaim);
        let json = serde_json::to_string(&g).unwrap();
        let back: AlignmentGraph = serde_json::from_str(&json).unwrap();
        assert_eq!(g, back);
    }
}
