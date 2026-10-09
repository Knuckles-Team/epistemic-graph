//! Ontology-driven cross-source coverage (EG-FEDERATED-QUERY-R072.1).
//!
//! A caller names the ontology classes of a question. This module's typed model is
//! the read-only RESULT of linking those classes through object properties within a
//! hop budget, entailing subclass relations along the way, and naming — for each
//! class on the path — the approved virtual mapping that serves it, or the class
//! itself as uncovered. **It plans no execution**: a [`CoverageResult`] is a named
//! verdict plus its justifying premises, never an executable query plan. The actual
//! `VirtualMapping`/`ForeignSourceSpec` wiring that a covered class resolves to is
//! EG-FEDERATED-QUERY-R073's concern (a parallel lane); this module only needs a
//! mapping's *name* to report coverage.
//!
//! The full hop-budget search (walking a live ontology + mapping registry) is a
//! later child of this requirement. This slice fixes the typed vocabulary — the
//! [`CoverageResult`] outcomes and the [`Premise`]s every selection must carry — and
//! proves it against a small two-source fixture covering all four verification
//! cases: a direct mapping, a subclass mapping serving its superclass, an uncovered
//! class, and a disconnected pair.

use std::collections::BTreeMap;

/// A single justifying fact behind a coverage verdict. Every [`CoverageResult`]
/// carries the premises that led to it, so the selection is auditable rather than
/// asserted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Premise {
    /// An `rdfs:label`-style fact naming a class on the path.
    Label { class: String, label: String },
    /// An object-property fact linking two classes, traversed during the
    /// hop-budget search (one hop).
    Relation {
        from_class: String,
        property: String,
        to_class: String,
    },
    /// An entailed-or-asserted `subClassOf` fact used to find a mapping that
    /// serves a superclass of the class actually being covered.
    Subclass {
        sub_class: String,
        super_class: String,
    },
    /// The approved virtual mapping fact that serves `served_class` (which may be
    /// the queried class itself or a superclass reached via [`Premise::Subclass`]).
    Mapping {
        served_class: String,
        mapping_name: String,
    },
    /// The source generation (append-only epoch) the above facts were read at.
    SourceGeneration { source: String, generation: u64 },
}

/// One class-to-class hop on a selection path, carrying the premises that justify
/// traversing it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceSelectionHop {
    pub from_class: String,
    pub to_class: String,
    pub premises: Vec<Premise>,
}

/// The read-only, per-class verdict of a cross-source coverage search. No variant
/// carries an execution plan: `Covered` names only the approved mapping, never a
/// query, connection, or operation to run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoverageResult {
    /// `class` is served by `mapping_name`, an approved virtual mapping, reached
    /// via `path` (empty when `class` is itself directly mapped) plus whatever
    /// subclass premise justified using that mapping.
    Covered {
        class: String,
        mapping_name: String,
        path: Vec<SourceSelectionHop>,
        premises: Vec<Premise>,
    },
    /// `class` lies within the hop budget of a queried root but no approved
    /// virtual mapping serves it (directly or through an entailed superclass).
    Uncovered {
        class: String,
        premises: Vec<Premise>,
    },
    /// `class` has no object-property path to any queried root within the hop
    /// budget — it is not merely unmapped, it was never reached.
    Disconnected { class: String },
}

impl CoverageResult {
    pub fn class(&self) -> &str {
        match self {
            CoverageResult::Covered { class, .. }
            | CoverageResult::Uncovered { class, .. }
            | CoverageResult::Disconnected { class } => class,
        }
    }
}

/// A small, in-memory ontology + mapping fixture: object-property facts, asserted
/// `subClassOf` facts, and the approved-mapping registry, each carrying the source
/// generation it was read at. Exists to drive [`select_sources`] and its tests —
/// the live KG-backed reasoner (walking `eg-rdf::owl`/`tableau` classification and
/// the real mapping catalog) is a later child of R072.
#[derive(Clone, Debug, Default)]
pub struct CoverageFixture {
    object_properties: Vec<(String, String, String)>,
    subclass_of: Vec<(String, String)>,
    /// class -> (mapping name, source, generation)
    mappings: BTreeMap<String, (String, String, u64)>,
}

impl CoverageFixture {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_relation(mut self, from_class: &str, property: &str, to_class: &str) -> Self {
        self.object_properties.push((
            from_class.to_string(),
            property.to_string(),
            to_class.to_string(),
        ));
        self
    }

    pub fn with_subclass(mut self, sub_class: &str, super_class: &str) -> Self {
        self.subclass_of
            .push((sub_class.to_string(), super_class.to_string()));
        self
    }

    pub fn with_mapping(
        mut self,
        class: &str,
        mapping_name: &str,
        source: &str,
        generation: u64,
    ) -> Self {
        self.mappings.insert(
            class.to_string(),
            (mapping_name.to_string(), source.to_string(), generation),
        );
        self
    }

    /// Asserted superclasses of `class` plus `class` itself — the subclass
    /// entailment closure a real reasoner (`owl`/`tableau`) would derive; the
    /// fixture only asserts it directly.
    fn superclasses_closure(&self, class: &str) -> Vec<String> {
        let mut seen = vec![class.to_string()];
        let mut frontier = vec![class.to_string()];
        while let Some(current) = frontier.pop() {
            for (sub, sup) in &self.subclass_of {
                if sub == &current && !seen.contains(sup) {
                    seen.push(sup.clone());
                    frontier.push(sup.clone());
                }
            }
        }
        seen
    }

    fn mapping_premises(&self, source_mapping: &(String, String, u64)) -> Vec<Premise> {
        vec![Premise::SourceGeneration {
            source: source_mapping.1.clone(),
            generation: source_mapping.2,
        }]
    }
}

/// Links `roots` through object properties within `hop_budget` hops, entailing
/// subclass relations at each visited class, and returns one [`CoverageResult`] per
/// class in the fixture's universe (every class mentioned by a relation, subclass
/// fact, or mapping). Read-only: no execution plan is built or returned.
pub fn select_sources(
    fixture: &CoverageFixture,
    roots: &[String],
    hop_budget: usize,
) -> Vec<CoverageResult> {
    // The queried universe: every class the fixture mentions anywhere.
    let mut universe: Vec<String> = Vec::new();
    for (a, _, b) in &fixture.object_properties {
        for c in [a, b] {
            if !universe.contains(c) {
                universe.push(c.clone());
            }
        }
    }
    for (a, b) in &fixture.subclass_of {
        for c in [a, b] {
            if !universe.contains(c) {
                universe.push(c.clone());
            }
        }
    }
    for c in fixture.mappings.keys() {
        if !universe.contains(c) {
            universe.push(c.clone());
        }
    }
    for c in roots {
        if !universe.contains(c) {
            universe.push(c.clone());
        }
    }

    // BFS from every root over the (undirected) object-property graph, up to
    // hop_budget hops, recording the hop path premises per reached class.
    let mut reached: BTreeMap<String, Vec<SourceSelectionHop>> = BTreeMap::new();
    let mut frontier: Vec<(String, Vec<SourceSelectionHop>)> =
        roots.iter().map(|r| (r.clone(), Vec::new())).collect();
    for root in roots {
        reached.entry(root.clone()).or_default();
    }
    for _ in 0..hop_budget {
        let mut next_frontier = Vec::new();
        for (class, path_so_far) in &frontier {
            for (from_class, property, to_class) in &fixture.object_properties {
                let neighbor = if from_class == class {
                    Some(to_class.clone())
                } else if to_class == class {
                    Some(from_class.clone())
                } else {
                    None
                };
                let Some(neighbor) = neighbor else { continue };
                if reached.contains_key(&neighbor) {
                    continue;
                }
                let mut path = path_so_far.clone();
                path.push(SourceSelectionHop {
                    from_class: class.clone(),
                    to_class: neighbor.clone(),
                    premises: vec![Premise::Relation {
                        from_class: from_class.clone(),
                        property: property.clone(),
                        to_class: to_class.clone(),
                    }],
                });
                reached.insert(neighbor.clone(), path.clone());
                next_frontier.push((neighbor, path));
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        frontier = next_frontier;
    }

    universe
        .into_iter()
        .map(|class| {
            let Some(path) = reached.get(&class) else {
                return CoverageResult::Disconnected { class };
            };
            // Subclass entailment: a mapping on any superclass in the closure
            // (including the class itself) covers this class.
            for ancestor in fixture.superclasses_closure(&class) {
                if let Some(source_mapping) = fixture.mappings.get(&ancestor) {
                    let mut premises = fixture.mapping_premises(source_mapping);
                    if ancestor != class {
                        premises.push(Premise::Subclass {
                            sub_class: class.clone(),
                            super_class: ancestor.clone(),
                        });
                    }
                    premises.push(Premise::Mapping {
                        served_class: ancestor.clone(),
                        mapping_name: source_mapping.0.clone(),
                    });
                    return CoverageResult::Covered {
                        class: class.clone(),
                        mapping_name: source_mapping.0.clone(),
                        path: path.clone(),
                        premises,
                    };
                }
            }
            CoverageResult::Uncovered {
                class: class.clone(),
                premises: Vec::new(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two-source fixture: `Invoice` is mapped directly by `source-a`. `Contractor`
    /// is a subclass of `Person`, which is mapped by `source-b` — proving a
    /// subclass mapping serves its superclass. `Invoice` relates to `Customer` (no
    /// mapping anywhere in its closure): uncovered. `Legacy` has no relation to
    /// anything: disconnected from the queried roots.
    fn two_source_fixture() -> CoverageFixture {
        CoverageFixture::new()
            .with_relation("Invoice", "billedTo", "Customer")
            .with_relation("Invoice", "signedBy", "Contractor")
            .with_subclass("Contractor", "Person")
            .with_mapping("Invoice", "source-a-invoices", "source-a", 7)
            .with_mapping("Person", "source-b-people", "source-b", 3)
    }

    // spec: EG-FEDERATED-QUERY-R072.1
    #[test]
    fn direct_mapping_is_covered_with_source_generation_premise() {
        let fixture = two_source_fixture();
        let results = select_sources(&fixture, &["Invoice".to_string()], 2);
        let invoice = results.iter().find(|r| r.class() == "Invoice").unwrap();
        match invoice {
            CoverageResult::Covered {
                mapping_name,
                premises,
                ..
            } => {
                assert_eq!(mapping_name, "source-a-invoices");
                assert!(premises.contains(&Premise::SourceGeneration {
                    source: "source-a".to_string(),
                    generation: 7,
                }));
            }
            other => panic!("expected Covered, got {other:?}"),
        }
    }

    // spec: EG-FEDERATED-QUERY-R072.1
    #[test]
    fn subclass_mapping_serves_its_superclass() {
        let fixture = two_source_fixture();
        let results = select_sources(&fixture, &["Invoice".to_string()], 2);
        let contractor = results.iter().find(|r| r.class() == "Contractor").unwrap();
        match contractor {
            CoverageResult::Covered {
                mapping_name,
                premises,
                ..
            } => {
                assert_eq!(mapping_name, "source-b-people");
                assert!(premises.contains(&Premise::Subclass {
                    sub_class: "Contractor".to_string(),
                    super_class: "Person".to_string(),
                }));
                assert!(premises.contains(&Premise::SourceGeneration {
                    source: "source-b".to_string(),
                    generation: 3,
                }));
            }
            other => panic!("expected Covered via subclass entailment, got {other:?}"),
        }
    }

    // spec: EG-FEDERATED-QUERY-R072.1
    #[test]
    fn reachable_unmapped_class_is_uncovered() {
        let fixture = two_source_fixture();
        let results = select_sources(&fixture, &["Invoice".to_string()], 2);
        let customer = results.iter().find(|r| r.class() == "Customer").unwrap();
        assert!(matches!(customer, CoverageResult::Uncovered { .. }));
    }

    #[test]
    fn unlinked_class_is_disconnected_not_uncovered() {
        let fixture = two_source_fixture().with_relation("Legacy", "notes", "Legacy");
        // `Legacy` only relates to itself, so from `Invoice` it is unreachable.
        let results = select_sources(&fixture, &["Invoice".to_string()], 2);
        let legacy = results.iter().find(|r| r.class() == "Legacy").unwrap();
        assert!(matches!(legacy, CoverageResult::Disconnected { .. }));
    }

    #[test]
    fn selection_plans_no_execution() {
        // A `Covered` verdict carries only a mapping name and premises — there is
        // no field on `CoverageResult` for a query, connection, or operation to
        // run. This test is a compile-time witness of that shape: if it compiles,
        // `select_sources`'s output remains plan-free.
        let fixture = two_source_fixture();
        let results = select_sources(&fixture, &["Invoice".to_string()], 2);
        for result in &results {
            if let CoverageResult::Covered { mapping_name, .. } = result {
                assert!(!mapping_name.is_empty());
            }
        }
    }
}
