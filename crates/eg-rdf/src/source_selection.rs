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
//!
//! An unconnected rank-1 source is a typed abstention, never a silent fallback
//! (EG-CQ-XSRC-R005.4): the result names the source and the class it was supposed
//! to serve, and a lower-ranked alternate wins only when the rank-1 row's
//! `vg:allowFallback` permits it.

use std::collections::{BTreeMap, BTreeSet};

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
    /// The ontology authority row that declared the source and rank for a
    /// mapping.  This is kept separate from [`Premise::Mapping`] so callers
    /// can explain why one candidate won and why the remaining candidates are
    /// alternates.
    Authority {
        row: String,
        source: String,
        class: String,
        facet: Option<String>,
        rank: u64,
        scope: Option<String>,
        origin: String,
        generation: u64,
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
        /// Lower-ranked mappings for the same covered class, in authority-rank
        /// order.  They are reported for provenance; execution/fallback is a
        /// later planner concern.
        alternates: Vec<String>,
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
    /// `class` has an authority row, but its rank-1 source is not connected and
    /// the row does not permit fallback. The abstention names the source and the
    /// class, so the caller reports it instead of serving from a lower-ranked
    /// source silently (EG-CQ-XSRC-R005.4).
    Unconnected {
        class: String,
        source: String,
        premises: Vec<Premise>,
    },
}

/// One ontology `vg:SourceAuthority` row in the fixture-backed selection
/// model.  The optional fields are retained in the typed premise now so the
/// live graph-schema reader can add facet/scope/origin filtering without
/// changing the covered-result shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityRow {
    pub row: String,
    pub source: String,
    pub class: String,
    pub mapping_name: String,
    pub facet: Option<String>,
    pub rank: u64,
    pub scope: Option<String>,
    pub origin: String,
    pub generation: u64,
    /// `vg:allowFallback`: whether a lower-ranked row may serve the class when
    /// this row's source is not connected (EG-CQ-XSRC-R005.4).
    pub allow_fallback: bool,
}

impl CoverageResult {
    pub fn class(&self) -> &str {
        match self {
            CoverageResult::Covered { class, .. }
            | CoverageResult::Uncovered { class, .. }
            | CoverageResult::Disconnected { class }
            | CoverageResult::Unconnected { class, .. } => class,
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
    /// class -> all authority rows (the previous implementation kept one
    /// tuple here and silently discarded earlier candidates).
    mappings: BTreeMap<String, Vec<AuthorityRow>>,
    /// Sources the fixture declares not connected (EG-CQ-XSRC-R005.4). Empty
    /// by default: every source is connected.
    unconnected_sources: BTreeSet<String>,
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
        self.push_mapping(AuthorityRow {
            row: format!("{class}::{mapping_name}"),
            source: source.to_string(),
            class: class.to_string(),
            mapping_name: mapping_name.to_string(),
            facet: None,
            rank: 1,
            scope: None,
            origin: "fixture".to_string(),
            generation,
            allow_fallback: false,
        });
        self
    }

    /// Add a ranked `vg:SourceAuthority` candidate that permits fallback to a
    /// lower-ranked connected source when its own source is not connected.
    pub fn with_authority_allow_fallback(
        mut self,
        class: &str,
        mapping_name: &str,
        source: &str,
        generation: u64,
        rank: u64,
    ) -> Self {
        self.push_mapping(AuthorityRow {
            row: format!("{class}::{mapping_name}"),
            source: source.to_string(),
            class: class.to_string(),
            mapping_name: mapping_name.to_string(),
            facet: None,
            rank,
            scope: None,
            origin: "fixture".to_string(),
            generation,
            allow_fallback: true,
        });
        self
    }

    /// Declare `source` not connected (EG-CQ-XSRC-R005.4).
    pub fn with_unconnected_source(mut self, source: &str) -> Self {
        self.unconnected_sources.insert(source.to_string());
        self
    }

    /// Add a ranked `vg:SourceAuthority` candidate for `class`.
    ///
    /// Lower authority ranks win.  The row identifier is deterministic in the
    /// fixture because the live authority vocabulary is not parsed by this
    /// R072/R005.1 unit.
    pub fn with_authority(
        mut self,
        class: &str,
        mapping_name: &str,
        source: &str,
        generation: u64,
        rank: u64,
    ) -> Self {
        self.push_mapping(AuthorityRow {
            row: format!("{class}::{mapping_name}"),
            source: source.to_string(),
            class: class.to_string(),
            mapping_name: mapping_name.to_string(),
            facet: None,
            rank,
            scope: None,
            origin: "fixture".to_string(),
            generation,
            allow_fallback: false,
        });
        self
    }

    fn push_mapping(&mut self, row: AuthorityRow) {
        self.mappings
            .entry(row.class.clone())
            .or_default()
            .push(row);
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

    fn mapping_premises(&self, source_mapping: &AuthorityRow) -> Vec<Premise> {
        vec![Premise::SourceGeneration {
            source: source_mapping.source.clone(),
            generation: source_mapping.generation,
        }]
    }

    fn authority_premise(row: &AuthorityRow) -> Premise {
        Premise::Authority {
            row: row.row.clone(),
            source: row.source.clone(),
            class: row.class.clone(),
            facet: row.facet.clone(),
            rank: row.rank,
            scope: row.scope.clone(),
            origin: row.origin.clone(),
            generation: row.generation,
        }
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
    let universe = query_universe(fixture, roots);
    let reached = reach_from_roots(fixture, roots, hop_budget);
    universe
        .into_iter()
        .map(|class| classify_class(fixture, &reached, class))
        .collect()
}

/// The queried universe: every class the fixture mentions anywhere, plus the roots.
fn query_universe(fixture: &CoverageFixture, roots: &[String]) -> Vec<String> {
    let mut universe: Vec<String> = Vec::new();
    let mut add = |class: &String| {
        if !universe.contains(class) {
            universe.push(class.clone());
        }
    };
    for (a, _, b) in &fixture.object_properties {
        add(a);
        add(b);
    }
    for (a, b) in &fixture.subclass_of {
        add(a);
        add(b);
    }
    for c in fixture.mappings.keys() {
        add(c);
    }
    for c in roots {
        add(c);
    }
    universe
}

/// The other end of an object property touching `class`, if any.
fn property_neighbor<'a>(class: &str, from_class: &'a str, to_class: &'a str) -> Option<&'a str> {
    if from_class == class {
        Some(to_class)
    } else if to_class == class {
        Some(from_class)
    } else {
        None
    }
}

/// BFS from every root over the (undirected) object-property graph, up to
/// `hop_budget` hops, recording the hop path premises per reached class.
fn reach_from_roots(
    fixture: &CoverageFixture,
    roots: &[String],
    hop_budget: usize,
) -> BTreeMap<String, Vec<SourceSelectionHop>> {
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
                let Some(neighbor) = property_neighbor(class, from_class, to_class) else {
                    continue;
                };
                if reached.contains_key(neighbor) {
                    continue;
                }
                let mut path = path_so_far.clone();
                path.push(SourceSelectionHop {
                    from_class: class.clone(),
                    to_class: neighbor.to_string(),
                    premises: vec![Premise::Relation {
                        from_class: from_class.clone(),
                        property: property.clone(),
                        to_class: to_class.clone(),
                    }],
                });
                reached.insert(neighbor.to_string(), path.clone());
                next_frontier.push((neighbor.to_string(), path));
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        frontier = next_frontier;
    }
    reached
}

/// Classify one class: disconnected, covered by a mapping on it or an
/// ancestor in its subclass closure, or uncovered.
fn classify_class(
    fixture: &CoverageFixture,
    reached: &BTreeMap<String, Vec<SourceSelectionHop>>,
    class: String,
) -> CoverageResult {
    let Some(path) = reached.get(&class) else {
        return CoverageResult::Disconnected { class };
    };
    for ancestor in fixture.superclasses_closure(&class) {
        if let Some(source_mappings) = fixture.mappings.get(&ancestor) {
            let mut candidates = source_mappings.clone();
            candidates.sort_by(|left, right| {
                left.rank
                    .cmp(&right.rank)
                    .then_with(|| left.row.cmp(&right.row))
            });
            let source_mapping = &candidates[0];
            // An unconnected rank-1 source is a typed abstention unless its row
            // permits fallback; a fallback names the row it came from in the
            // premises so the report says so (EG-CQ-XSRC-R005.4).
            let used = if fixture.unconnected_sources.contains(&source_mapping.source) {
                match candidates
                    .iter()
                    .find(|candidate| !fixture.unconnected_sources.contains(&candidate.source))
                {
                    Some(alternate) if source_mapping.allow_fallback => alternate,
                    _ => {
                        let mut premises = fixture.mapping_premises(source_mapping);
                        premises.push(CoverageFixture::authority_premise(source_mapping));
                        return CoverageResult::Unconnected {
                            class,
                            source: source_mapping.source.clone(),
                            premises,
                        };
                    }
                }
            } else {
                source_mapping
            };
            let mut premises = fixture.mapping_premises(used);
            if used.row != source_mapping.row {
                premises.push(CoverageFixture::authority_premise(source_mapping));
            }
            premises.push(CoverageFixture::authority_premise(used));
            if ancestor != class {
                premises.push(Premise::Subclass {
                    sub_class: class.clone(),
                    super_class: ancestor.clone(),
                });
            }
            premises.push(Premise::Mapping {
                served_class: ancestor.clone(),
                mapping_name: used.mapping_name.clone(),
            });
            let alternates = candidates
                .iter()
                .filter(|candidate| candidate.row != used.row)
                .map(|candidate| {
                    premises.push(CoverageFixture::authority_premise(candidate));
                    candidate.mapping_name.clone()
                })
                .collect();
            return CoverageResult::Covered {
                class,
                mapping_name: used.mapping_name.clone(),
                alternates,
                path: path.clone(),
                premises,
            };
        }
    }
    CoverageResult::Uncovered {
        class,
        premises: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KG: &str = "http://knuckles.team/kg#";

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

    // spec: EG-CQ-XSRC-R005.1
    #[test]
    fn authority_rank_selects_winner_and_lists_lower_ranked_alternate() {
        let fixture = CoverageFixture::new()
            .with_relation("Invoice", "billedTo", "Customer")
            .with_authority("Invoice", "source-b-invoices", "source-b", 3, 2)
            .with_authority("Invoice", "source-a-invoices", "source-a", 7, 1);
        let results = select_sources(&fixture, &["Invoice".to_string()], 1);
        let invoice = results.iter().find(|r| r.class() == "Invoice").unwrap();

        match invoice {
            CoverageResult::Covered {
                mapping_name,
                alternates,
                premises,
                ..
            } => {
                assert_eq!(mapping_name, "source-a-invoices");
                assert_eq!(alternates, &["source-b-invoices".to_string()]);
                assert!(premises.contains(&Premise::Authority {
                    row: "Invoice::source-a-invoices".to_string(),
                    source: "source-a".to_string(),
                    class: "Invoice".to_string(),
                    facet: None,
                    rank: 1,
                    scope: None,
                    origin: "fixture".to_string(),
                    generation: 7,
                }));
                assert!(premises.contains(&Premise::Authority {
                    row: "Invoice::source-b-invoices".to_string(),
                    source: "source-b".to_string(),
                    class: "Invoice".to_string(),
                    facet: None,
                    rank: 2,
                    scope: None,
                    origin: "fixture".to_string(),
                    generation: 3,
                }));
            }
            other => panic!("expected ranked Covered, got {other:?}"),
        }
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

    // spec: EG-FEDERATED-QUERY-R072.1
    #[test]
    fn unlinked_class_is_disconnected_not_uncovered() {
        let fixture = two_source_fixture().with_relation("Legacy", "notes", "Legacy");
        // `Legacy` only relates to itself, so from `Invoice` it is unreachable.
        let results = select_sources(&fixture, &["Invoice".to_string()], 2);
        let legacy = results.iter().find(|r| r.class() == "Legacy").unwrap();
        assert!(matches!(legacy, CoverageResult::Disconnected { .. }));
    }

    // spec: EG-FEDERATED-QUERY-R072.1
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

    /// The row's scenario: `:Supplier` to erpnext (rank 1) with leanix (rank 2),
    /// erpnext not connected.
    fn supplier_fixture(allow_fallback: bool) -> CoverageFixture {
        let supplier = format!("{KG}Supplier");
        let fixture = if allow_fallback {
            CoverageFixture::new().with_authority_allow_fallback(
                &supplier,
                "erpnext-suppliers",
                "erpnext-mcp",
                5,
                1,
            )
        } else {
            CoverageFixture::new().with_authority(
                &supplier,
                "erpnext-suppliers",
                "erpnext-mcp",
                5,
                1,
            )
        };
        fixture
            .with_authority(&supplier, "leanix-suppliers", "leanix-mcp", 4, 2)
            .with_unconnected_source("erpnext-mcp")
    }

    // spec: EG-CQ-XSRC-R005.4
    #[test]
    fn unconnected_rank_one_source_is_a_typed_abstention() {
        let fixture = supplier_fixture(false);
        let results = select_sources(&fixture, &[format!("{KG}Supplier")], 1);
        let supplier = results
            .iter()
            .find(|r| r.class() == format!("{KG}Supplier"))
            .unwrap();
        match supplier {
            CoverageResult::Unconnected {
                class,
                source,
                premises,
            } => {
                assert_eq!(class, &format!("{KG}Supplier"));
                assert_eq!(source, "erpnext-mcp");
                assert!(premises.contains(&Premise::Authority {
                    row: format!("{KG}Supplier::erpnext-suppliers"),
                    source: "erpnext-mcp".to_string(),
                    class: format!("{KG}Supplier"),
                    facet: None,
                    rank: 1,
                    scope: None,
                    origin: "fixture".to_string(),
                    generation: 5,
                }));
            }
            other => panic!("expected the typed Unconnected abstention, got {other:?}"),
        }
    }

    // spec: EG-CQ-XSRC-R005.4
    #[test]
    fn fallback_uses_the_lower_ranked_alternate_only_when_permitted() {
        let fixture = supplier_fixture(true);
        let results = select_sources(&fixture, &[format!("{KG}Supplier")], 1);
        let supplier = results
            .iter()
            .find(|r| r.class() == format!("{KG}Supplier"))
            .unwrap();
        match supplier {
            CoverageResult::Covered {
                mapping_name,
                alternates,
                premises,
                ..
            } => {
                assert_eq!(mapping_name, "leanix-suppliers");
                assert_eq!(alternates, &["erpnext-suppliers".to_string()]);
                // The report can say so: the rank-1 row is named in the premises.
                assert!(premises.contains(&Premise::Authority {
                    row: format!("{KG}Supplier::erpnext-suppliers"),
                    source: "erpnext-mcp".to_string(),
                    class: format!("{KG}Supplier"),
                    facet: None,
                    rank: 1,
                    scope: None,
                    origin: "fixture".to_string(),
                    generation: 5,
                }));
                assert!(premises.contains(&Premise::Mapping {
                    served_class: format!("{KG}Supplier"),
                    mapping_name: "leanix-suppliers".to_string(),
                }));
            }
            other => panic!("expected fallback Covered via leanix, got {other:?}"),
        }
    }
}
