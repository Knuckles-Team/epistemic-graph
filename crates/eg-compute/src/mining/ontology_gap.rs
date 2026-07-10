// CONCEPT:EG-KG.mining.ontology-gap — Ontology completeness gap detection.
//
// Pure-Rust, dependency-light, GRAPH-NATIVE (no `rdf`/OWL-reasoner dependency —
// this operates over the same `type`/`relation`-tagged resident-graph shape
// `subgraph::build_host_graph` already projects elsewhere in this crate, so it
// runs in every `mining` build, including a plain graph with no OWL ingestion at
// all). Given a set of ontology CLASS nodes, each described by:
//
//   * `property_count` — how many properties/attributes are declared FOR this
//     class (an OWL `rdfs:domain` fact, or just "this class node has outgoing
//     `HAS_PROPERTY` edges" in a lighter graph-native ontology);
//   * `declares_parent` / `parent_resolves` — whether the class declares a
//     `subClassOf`/`SUBCLASS_OF` parent, and whether that parent resolves to
//     another class IN THE SAME SET (a dangling reference is an "orphan
//     subclass" — it names a parent that isn't actually a resident class);
//   * `edge_count` — the class node's total incident edge count in the WHOLE
//     graph (of any kind) — `0` means the class is completely disconnected.
//
// find_gaps flags three gap kinds:
//
//   * `NoProperties`   — the class declares zero properties (severity 0.5: a
//     mild completeness gap, possibly intentional for a leaf marker class).
//   * `OrphanSubclass` — the class names a parent that doesn't resolve
//     (severity 0.8: a structural break — the class hierarchy itself is broken).
//   * `Disconnected`   — the class has NO edges anywhere in the graph (severity
//     1.0: the worst gap — an orphaned definition nothing else references).
//
// Severities are fixed, documented constants (NOT a fabricated ML score) —
// candidate signal strength for the downstream `:OntologyGap` claim, which
// still carries `validation_state = "unvalidated"` pending a human/reasoner check.

/// One ontology class's structural facts, as read off the resident graph.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClassNode {
    pub property_count: usize,
    pub declares_parent: bool,
    pub parent_resolves: bool,
    pub edge_count: usize,
}

/// The kind of completeness gap flagged for one class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapKind {
    NoProperties,
    OrphanSubclass,
    Disconnected,
}

impl GapKind {
    /// Fixed, documented severity in `[0,1]` (see module docs).
    pub fn severity(self) -> f64 {
        match self {
            GapKind::NoProperties => 0.5,
            GapKind::OrphanSubclass => 0.8,
            GapKind::Disconnected => 1.0,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            GapKind::NoProperties => "no_properties",
            GapKind::OrphanSubclass => "orphan_subclass",
            GapKind::Disconnected => "disconnected",
        }
    }
}

/// One flagged gap for one class index.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OntologyGap {
    pub class_index: usize,
    pub kind: GapKind,
}

/// Scan `classes` for completeness gaps (CONCEPT:EG-KG.mining.ontology-gap). A
/// single class may be flagged for MULTIPLE gap kinds (e.g. disconnected AND
/// property-less) — each is reported separately so the claim writeback can
/// materialize one `:OntologyGap` per (class, kind) pair.
pub fn find_gaps(classes: &[ClassNode]) -> Vec<OntologyGap> {
    let mut out = Vec::new();
    for (i, c) in classes.iter().enumerate() {
        if c.edge_count == 0 {
            out.push(OntologyGap {
                class_index: i,
                kind: GapKind::Disconnected,
            });
        }
        if c.declares_parent && !c.parent_resolves {
            out.push(OntologyGap {
                class_index: i,
                kind: GapKind::OrphanSubclass,
            });
        }
        if c.property_count == 0 {
            out.push(OntologyGap {
                class_index: i,
                kind: GapKind::NoProperties,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disconnected_class_is_flagged() {
        let classes = vec![ClassNode {
            property_count: 2,
            declares_parent: false,
            parent_resolves: false,
            edge_count: 0,
        }];
        let gaps = find_gaps(&classes);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].kind, GapKind::Disconnected);
    }

    #[test]
    fn orphan_subclass_is_flagged_only_when_parent_declared_and_unresolved() {
        let classes = vec![
            ClassNode {
                property_count: 1,
                declares_parent: true,
                parent_resolves: false,
                edge_count: 3,
            },
            ClassNode {
                property_count: 1,
                declares_parent: true,
                parent_resolves: true,
                edge_count: 3,
            },
            ClassNode {
                property_count: 1,
                declares_parent: false,
                parent_resolves: false,
                edge_count: 3,
            },
        ];
        let gaps = find_gaps(&classes);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].class_index, 0);
        assert_eq!(gaps[0].kind, GapKind::OrphanSubclass);
    }

    #[test]
    fn no_properties_is_flagged_independently() {
        let classes = vec![ClassNode {
            property_count: 0,
            declares_parent: true,
            parent_resolves: true,
            edge_count: 5,
        }];
        let gaps = find_gaps(&classes);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].kind, GapKind::NoProperties);
    }

    #[test]
    fn a_class_can_be_flagged_for_multiple_gap_kinds() {
        let classes = vec![ClassNode {
            property_count: 0,
            declares_parent: true,
            parent_resolves: false,
            edge_count: 0,
        }];
        let gaps = find_gaps(&classes);
        assert_eq!(gaps.len(), 3);
    }

    #[test]
    fn well_formed_class_has_no_gaps() {
        let classes = vec![ClassNode {
            property_count: 3,
            declares_parent: true,
            parent_resolves: true,
            edge_count: 10,
        }];
        assert!(find_gaps(&classes).is_empty());
    }

    #[test]
    fn severities_are_ordered_by_structural_severity() {
        assert!(GapKind::Disconnected.severity() > GapKind::OrphanSubclass.severity());
        assert!(GapKind::OrphanSubclass.severity() > GapKind::NoProperties.severity());
    }
}
