//! W5 — the OWL-DL **tableau** reasoner (CONCEPT:EG-KG.ontology.concept-2).
//!
//! A pure-Rust description-logic tableau over the parsed ontology's TBox+ABox. It
//! DECIDES exactly the constructs the monotone EL⁺/RL completion in [`crate::owl`]
//! cannot (documented as DEFERRED there): full cardinality restrictions (`≥n`/`≤n`,
//! qualified `onClass`), `owl:complementOf`/negation (via negation-normal-form),
//! `owl:oneOf`/`owl:hasValue` **nominals**, and reasoning-**by-cases** over a
//! `owl:unionOf` SUPERCLASS (`A ⊑ C₁ ⊔ C₂`). It is the DL completion of the OWL story:
//! `owl.rs` is the fast, tractable path; this is the complete-but-worst-case-harder
//! path, chosen only when a non-EL/RL construct is present ([`reason_dl`]).
//!
//! ## The algorithm (ALCQO + role hierarchy + transitive roles)
//!
//! A standard completion-graph tableau (Baader/Calvanese/McGuinness/Nardi/Patel-Schneider,
//! *The Description Logic Handbook*, ch. 2–3; Horrocks/Sattler tableau for `SHOQ`):
//!
//!   * **NNF** — every concept is first pushed to negation-normal-form ([`Dl::nnf`]),
//!     so negation sits only on atoms/nominals and every construct has a matching rule.
//!   * **TBox internalization** — every GCI `C ⊑ D` becomes the meta-constraint
//!     `¬C ⊔ D` (in NNF), and that set (`ct`) is stamped into EVERY node label on
//!     creation, so all nodes are forced to satisfy the TBox.
//!   * **Expansion rules** — `⊓` (add both), `⊔` (branch), `∃` (create a witness),
//!     `∀` (propagate to r-successors, incl. transitive-role folding), `≥n` (create n
//!     pairwise-distinct witnesses), `≤n` (merge two mergeable witnesses),
//!     `choose` (guess `C` / `¬C` on a witness for qualified number restrictions), and
//!     the nominal rule `{a}` (all nodes carrying the same nominal are one).
//!   * **Clash** — `⊥` in a label, `{A, ¬A}`, `{ {a}, ¬{a} }`, a forced self-inequality,
//!     or `≤n r.C` with `n+1` pairwise-distinct `C`-witnesses.
//!   * **Blocking** — equality (label-set) blocking against a non-nominal ancestor
//!     guarantees termination (labels are drawn from the finite sub-concept set), which
//!     is sound for the `SHQ`-shaped fragment we admit (no inverse roles in the tableau).
//!
//! ## Entry points
//!
//!   * [`is_consistent`]   — ontology (TBox+ABox) satisfiability.
//!   * [`is_subsumed`]     — concept subsumption via unsatisfiability of `sub ⊓ ¬sup`.
//!   * [`is_instance`]     — instance checking via inconsistency of `ont ∪ {a : ¬C}`.
//!   * [`classify_dl`]     — the full named-class hierarchy (pairwise subsumption).
//!   * [`reason_dl`]       — the engine PICKER: EL⁺/RL fast path when the ontology is
//!     inside the tractable envelope, the tableau ONLY when a DL construct forces it.
//!
//! ## Reuse (surgical)
//!
//! The DL concept language here is strictly richer than the EL/RL [`crate::owl::Concept`]
//! (it adds `⊔`/`¬`/`≥`/`≤`/`{a}`), so it needs its own model. But it does NOT re-parse
//! RDF from scratch: it reuses owl.rs's `pub(crate)` triple index + node-id helpers
//! (`TripleIndex`, `term_key`, `iri`, `parse_rdf_list`), so both engines speak the SAME
//! `<iri>` ids and read the SAME RDF the `rdf`/oxttl layer produced. The EL/RL path in
//! `owl.rs` is untouched.
//!
//! ## Pi contract
//!
//! Behind `owl-dl` (implies `owl`). Pure Rust — no new dep. Deliberately OUT of `pi`
//! (a Pi node runs only the tractable EL⁺/RL fast path); folded into node/full.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use oxrdf::{Term, Triple};

use crate::owl::{iri, parse_rdf_list, term_key, TripleIndex};

// ── OWL / RDF(S) vocabulary IRIs used by the DL parser ───────────────────────

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const RDFS_SUBPROPERTY_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";

const OWL_THING: &str = "http://www.w3.org/2002/07/owl#Thing";
const OWL_NOTHING: &str = "http://www.w3.org/2002/07/owl#Nothing";
const OWL_EQUIVALENT_CLASS: &str = "http://www.w3.org/2002/07/owl#equivalentClass";
const OWL_DISJOINT_WITH: &str = "http://www.w3.org/2002/07/owl#disjointWith";
const OWL_INTERSECTION_OF: &str = "http://www.w3.org/2002/07/owl#intersectionOf";
const OWL_UNION_OF: &str = "http://www.w3.org/2002/07/owl#unionOf";
const OWL_COMPLEMENT_OF: &str = "http://www.w3.org/2002/07/owl#complementOf";
const OWL_ONE_OF: &str = "http://www.w3.org/2002/07/owl#oneOf";
const OWL_ON_PROPERTY: &str = "http://www.w3.org/2002/07/owl#onProperty";
const OWL_SOME_VALUES_FROM: &str = "http://www.w3.org/2002/07/owl#someValuesFrom";
const OWL_ALL_VALUES_FROM: &str = "http://www.w3.org/2002/07/owl#allValuesFrom";
const OWL_HAS_VALUE: &str = "http://www.w3.org/2002/07/owl#hasValue";
const OWL_MIN_CARDINALITY: &str = "http://www.w3.org/2002/07/owl#minCardinality";
const OWL_MAX_CARDINALITY: &str = "http://www.w3.org/2002/07/owl#maxCardinality";
const OWL_CARDINALITY: &str = "http://www.w3.org/2002/07/owl#cardinality";
const OWL_MIN_QUALIFIED_CARDINALITY: &str = "http://www.w3.org/2002/07/owl#minQualifiedCardinality";
const OWL_MAX_QUALIFIED_CARDINALITY: &str = "http://www.w3.org/2002/07/owl#maxQualifiedCardinality";
const OWL_QUALIFIED_CARDINALITY: &str = "http://www.w3.org/2002/07/owl#qualifiedCardinality";
const OWL_ON_CLASS: &str = "http://www.w3.org/2002/07/owl#onClass";
const OWL_TRANSITIVE_PROPERTY: &str = "http://www.w3.org/2002/07/owl#TransitiveProperty";
const OWL_FUNCTIONAL_PROPERTY: &str = "http://www.w3.org/2002/07/owl#FunctionalProperty";
const OWL_SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";
const OWL_DIFFERENT_FROM: &str = "http://www.w3.org/2002/07/owl#differentFrom";

/// The set of predicates the DL parser interprets STRUCTURALLY (i.e. NOT as an ABox
/// role assertion). A triple whose predicate is outside this set (and whose object is
/// an IRI) is treated as a role edge between two individuals.
const STRUCTURAL_PREDICATES: &[&str] = &[
    RDF_TYPE,
    RDFS_SUBCLASS_OF,
    RDFS_SUBPROPERTY_OF,
    OWL_EQUIVALENT_CLASS,
    OWL_DISJOINT_WITH,
    OWL_INTERSECTION_OF,
    OWL_UNION_OF,
    OWL_COMPLEMENT_OF,
    OWL_ONE_OF,
    OWL_ON_PROPERTY,
    OWL_SOME_VALUES_FROM,
    OWL_ALL_VALUES_FROM,
    OWL_HAS_VALUE,
    OWL_MIN_CARDINALITY,
    OWL_MAX_CARDINALITY,
    OWL_CARDINALITY,
    OWL_MIN_QUALIFIED_CARDINALITY,
    OWL_MAX_QUALIFIED_CARDINALITY,
    OWL_QUALIFIED_CARDINALITY,
    OWL_ON_CLASS,
    OWL_INVERSE_OF,
    OWL_SAME_AS,
    OWL_DIFFERENT_FROM,
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#first",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",
    "http://epistemic-graph/owl#confidence",
];
const OWL_INVERSE_OF: &str = "http://www.w3.org/2002/07/owl#inverseOf";

/// Safety cap on total completion-graph nodes. Blocking guarantees termination for the
/// admitted fragment; this is a defensive valve only. If a completion ever exceeds it we
/// treat the branch as satisfiable (an OVER-approximation of consistency — so a spurious
/// subsumption is never reported), which keeps the reasoner sound-by-omission rather than
/// looping. Tiny for real ontologies; never hit by the test suite.
const NODE_CAP: usize = 50_000;

// ── The DL concept language (in NNF after [`Dl::nnf`]) ───────────────────────

/// A description-logic concept expression. Strictly richer than [`crate::owl::Concept`]
/// (which is only `Named` / `∃r.C`): this adds disjunction, negation, universal and
/// qualified-number restrictions, and nominals — the constructs that need a tableau.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Dl {
    /// `owl:Thing`.
    Top,
    /// `owl:Nothing`.
    Bottom,
    /// A named class IRI (canonical `<iri>` form).
    Atom(String),
    /// `¬C`. After [`Dl::nnf`] this only ever wraps an `Atom` or a `Nominal`.
    Not(Box<Dl>),
    /// `C₁ ⊓ … ⊓ Cₙ`.
    And(Vec<Dl>),
    /// `C₁ ⊔ … ⊔ Cₙ`.
    Or(Vec<Dl>),
    /// `∃ r . C`.
    Some(String, Box<Dl>),
    /// `∀ r . C`.
    All(String, Box<Dl>),
    /// `≥ n r . C` (qualified; `C = Top` is the unqualified `≥ n r.⊤`).
    Min(usize, String, Box<Dl>),
    /// `≤ n r . C` (qualified; `C = Top` is the unqualified `≤ n r.⊤`).
    Max(usize, String, Box<Dl>),
    /// A nominal `{a}` — the singleton class containing exactly the individual `a`.
    Nominal(String),
}

impl Dl {
    /// Push the concept to **negation-normal form**: negation is driven inward until it
    /// sits only on atoms/nominals, using the DL De Morgan / quantifier / cardinality
    /// dualities. Every rule in the tableau matches an NNF shape.
    pub fn nnf(self) -> Dl {
        match self {
            Dl::Not(inner) => inner.negate(),
            Dl::And(v) => Dl::And(v.into_iter().map(Dl::nnf).collect()),
            Dl::Or(v) => Dl::Or(v.into_iter().map(Dl::nnf).collect()),
            Dl::Some(r, c) => Dl::Some(r, Box::new(c.nnf())),
            Dl::All(r, c) => Dl::All(r, Box::new(c.nnf())),
            Dl::Min(n, r, c) => Dl::Min(n, r, Box::new(c.nnf())),
            Dl::Max(n, r, c) => Dl::Max(n, r, Box::new(c.nnf())),
            other => other, // Top / Bottom / Atom / Nominal
        }
    }

    /// The NNF of `¬self` (the dual). Used by [`Dl::nnf`] and the `choose` rule.
    ///
    /// `¬⊤=⊥`, `¬⊥=⊤`, `¬(C⊓D)=¬C⊔¬D`, `¬(C⊔D)=¬C⊓¬D`, `¬∃r.C=∀r.¬C`, `¬∀r.C=∃r.¬C`,
    /// `¬(≥n r.C)=≤(n-1) r.C` (and `¬(≥0 …)=⊥`), `¬(≤n r.C)=≥(n+1) r.C`.
    pub fn negate(self) -> Dl {
        match self {
            Dl::Top => Dl::Bottom,
            Dl::Bottom => Dl::Top,
            Dl::Atom(a) => Dl::Not(Box::new(Dl::Atom(a))),
            Dl::Nominal(a) => Dl::Not(Box::new(Dl::Nominal(a))),
            Dl::Not(inner) => inner.nnf(),
            Dl::And(v) => Dl::Or(v.into_iter().map(Dl::negate).collect()),
            Dl::Or(v) => Dl::And(v.into_iter().map(Dl::negate).collect()),
            Dl::Some(r, c) => Dl::All(r, Box::new(c.negate())),
            Dl::All(r, c) => Dl::Some(r, Box::new(c.negate())),
            Dl::Min(n, r, c) => {
                if n == 0 {
                    Dl::Bottom // ≥0 r.C is a tautology, so ¬ is ⊥.
                } else {
                    Dl::Max(n - 1, r, Box::new(c.nnf()))
                }
            }
            Dl::Max(n, r, c) => Dl::Min(n + 1, r, Box::new(c.nnf())),
        }
    }
}

/// Map a named-class IRI to its concept, folding the two builtins.
fn named_concept(id: &str) -> Dl {
    if id == iri(OWL_THING) {
        Dl::Top
    } else if id == iri(OWL_NOTHING) {
        Dl::Bottom
    } else {
        Dl::Atom(id.to_string())
    }
}

// ── The parsed DL ontology (TBox GCIs + role box + ABox) ─────────────────────

/// A parsed OWL-DL ontology: general concept inclusions over the full [`Dl`] language,
/// a role hierarchy, and an ABox of individual assertions.
#[derive(Clone, Debug, Default)]
pub struct DlOntology {
    /// General concept inclusions `C ⊑ D` (both sides arbitrary [`Dl`], in NNF).
    pub gcis: Vec<(Dl, Dl)>,
    /// `r ⊑ s` role inclusions.
    pub sub_roles: Vec<(String, String)>,
    /// Transitive roles (`r ∘ r ⊑ r`).
    pub transitive: BTreeSet<String>,
    /// ABox class assertions `a : C` (C in NNF).
    pub abox_types: Vec<(String, Dl)>,
    /// ABox role assertions `r(a, b)`.
    pub abox_roles: Vec<(String, String, String)>,
    /// Asserted `owl:sameAs` individual pairs.
    pub same_as: Vec<(String, String)>,
    /// Asserted `owl:differentFrom` individual pairs.
    pub different_from: Vec<(String, String)>,
    /// Every named class IRI mentioned (so [`classify_dl`] can iterate the signature).
    pub classes: BTreeSet<String>,
    /// Every named individual mentioned in the ABox.
    pub individuals: BTreeSet<String>,
}

impl DlOntology {
    /// Parse a DL ontology from an RDF triple stream (oxttl-parsed by the `rdf` layer).
    pub fn from_triples(triples: &[Triple]) -> Self {
        parse_dl_ontology(triples)
    }
}

/// Collect every named class atom appearing in a concept into the signature.
fn collect_classes(c: &Dl, out: &mut BTreeSet<String>) {
    match c {
        Dl::Atom(a) => {
            out.insert(a.clone());
        }
        Dl::Not(inner) => collect_classes(inner, out),
        Dl::And(v) | Dl::Or(v) => v.iter().for_each(|x| collect_classes(x, out)),
        Dl::Some(_, f) | Dl::All(_, f) | Dl::Min(_, _, f) | Dl::Max(_, _, f) => {
            collect_classes(f, out)
        }
        Dl::Top | Dl::Bottom | Dl::Nominal(_) => {}
    }
}

/// Parse a DL ontology (TBox + role box + ABox) from a triple stream.
/// `s rdfs:subClassOf ok` — apply one subclass GCI. Split out of
/// `parse_dl_ontology` (extract-method, cx/wD8) — same terms, same order as
/// before.
fn apply_subclass_of_triple(idx: &TripleIndex, ont: &mut DlOntology, s: &str, ok: &str) {
    if let (std::option::Option::Some(sub), std::option::Option::Some(sup)) =
        (parse_dl(idx, s), parse_dl(idx, ok))
    {
        ont.gcis.push((sub.nnf(), sup.nnf()));
    }
}

/// `s owl:equivalentClass ok` — apply both directions of the equivalence.
/// Split out of `parse_dl_ontology` (extract-method, cx/wD8) — same terms,
/// same order as before.
fn apply_equivalent_class_triple(idx: &TripleIndex, ont: &mut DlOntology, s: &str, ok: &str) {
    if let (std::option::Option::Some(a), std::option::Option::Some(b)) =
        (parse_dl(idx, s), parse_dl(idx, ok))
    {
        let (a, b) = (a.nnf(), b.nnf());
        ont.gcis.push((a.clone(), b.clone()));
        ont.gcis.push((b, a));
    }
}

/// `s owl:disjointWith ok` — A disjoint B ≡ A ⊑ ¬B. Split out of
/// `parse_dl_ontology` (extract-method, cx/wD8) — same terms, same order as
/// before.
fn apply_disjoint_with_triple(idx: &TripleIndex, ont: &mut DlOntology, s: &str, ok: &str) {
    if let (std::option::Option::Some(a), std::option::Option::Some(b)) =
        (parse_dl(idx, s), parse_dl(idx, ok))
    {
        // A disjoint B  ≡  A ⊑ ¬B.
        ont.gcis.push((a.nnf(), b.negate()));
    }
}

/// `s rdf:type o` — either a vocabulary declaration, a transitive/functional
/// role marker, an ABox class assertion (named or anonymous class), split
/// out of `parse_dl_ontology` (extract-method, cx/wD8) — same terms, same
/// order as before.
fn apply_rdf_type_triple(idx: &TripleIndex, ont: &mut DlOntology, s: &str, ok: &str, o: &Term) {
    if let Term::NamedNode(ty) = o {
        match ty.as_str() {
            OWL_TRANSITIVE_PROPERTY => {
                ont.transitive.insert(s.to_string());
            }
            OWL_FUNCTIONAL_PROPERTY => {
                // A functional role is the global axiom ⊤ ⊑ ≤1 r.⊤.
                ont.gcis
                    .push((Dl::Top, Dl::Max(1, s.to_string(), Box::new(Dl::Top))));
            }
            // Vocabulary declarations carry no ABox content.
            "http://www.w3.org/2002/07/owl#Class"
            | "http://www.w3.org/2002/07/owl#ObjectProperty"
            | "http://www.w3.org/2002/07/owl#DatatypeProperty"
            | "http://www.w3.org/2002/07/owl#NamedIndividual"
            | "http://www.w3.org/2002/07/owl#Ontology"
            | "http://www.w3.org/2002/07/owl#Restriction"
            | OWL_SYMMETRIC_MARKER => {}
            // Otherwise `a rdf:type C` is an ABox class assertion.
            _ => {
                let class = iri(ty.as_str());
                ont.abox_types.push((s.to_string(), named_concept(&class)));
                ont.individuals.insert(s.to_string());
                ont.classes.insert(class);
            }
        }
    } else if let std::option::Option::Some(c) = parse_dl(idx, ok) {
        // `a rdf:type [ complex class expr ]` — ABox assertion of an anon class.
        ont.abox_types.push((s.to_string(), c.nnf()));
        ont.individuals.insert(s.to_string());
    }
}

/// `s owl:sameAs o` — register both individuals and the link. Split out of
/// `parse_dl_ontology` (extract-method, cx/wD8) — same terms, same order as
/// before.
fn apply_same_as_triple(ont: &mut DlOntology, s: &str, o: &Term) {
    if let Term::NamedNode(b) = o {
        let b = iri(b.as_str());
        ont.same_as.push((s.to_string(), b.clone()));
        ont.individuals.insert(s.to_string());
        ont.individuals.insert(b);
    }
}

/// `s owl:differentFrom o` — register both individuals and the link. Split
/// out of `parse_dl_ontology` (extract-method, cx/wD8) — same terms, same
/// order as before.
fn apply_different_from_triple(ont: &mut DlOntology, s: &str, o: &Term) {
    if let Term::NamedNode(b) = o {
        let b = iri(b.as_str());
        ont.different_from.push((s.to_string(), b.clone()));
        ont.individuals.insert(s.to_string());
        ont.individuals.insert(b);
    }
}

/// Any other `s p o` with an IRI object between two individuals — a role
/// edge. Split out of `parse_dl_ontology` (extract-method, cx/wD8) — same
/// terms, same order as before.
fn apply_role_edge_triple(ont: &mut DlOntology, s: &str, p: &str, o: &Term) {
    if let Term::NamedNode(b) = o {
        let b = iri(b.as_str());
        ont.abox_roles.push((s.to_string(), iri(p), b.clone()));
        ont.individuals.insert(s.to_string());
        ont.individuals.insert(b);
    }
}

pub fn parse_dl_ontology(triples: &[Triple]) -> DlOntology {
    let idx = TripleIndex::build(triples);
    let mut ont = DlOntology::default();

    for t in triples {
        let s = term_key(&t.subject.clone().into());
        let p = t.predicate.as_str();
        let o = &t.object;
        let ok = term_key(o);
        match p {
            RDFS_SUBCLASS_OF => apply_subclass_of_triple(&idx, &mut ont, &s, &ok),
            OWL_EQUIVALENT_CLASS => apply_equivalent_class_triple(&idx, &mut ont, &s, &ok),
            OWL_DISJOINT_WITH => apply_disjoint_with_triple(&idx, &mut ont, &s, &ok),
            RDFS_SUBPROPERTY_OF => {
                if let Term::NamedNode(sup) = o {
                    ont.sub_roles.push((s.clone(), iri(sup.as_str())));
                }
            }
            OWL_SAME_AS => apply_same_as_triple(&mut ont, &s, o),
            OWL_DIFFERENT_FROM => apply_different_from_triple(&mut ont, &s, o),
            RDF_TYPE => apply_rdf_type_triple(&idx, &mut ont, &s, &ok, o),
            // Structurally-consumed predicates carry no direct axiom here.
            _ if STRUCTURAL_PREDICATES.contains(&p) => {}
            // Anything else with an IRI object between two individuals is a role edge.
            _ => apply_role_edge_triple(&mut ont, &s, p, o),
        }
    }

    // Build the class signature from every concept mentioned.
    let gcis = ont.gcis.clone();
    for (a, b) in &gcis {
        collect_classes(a, &mut ont.classes);
        collect_classes(b, &mut ont.classes);
    }
    let abox = ont.abox_types.clone();
    for (_, c) in &abox {
        collect_classes(c, &mut ont.classes);
    }
    ont
}
const OWL_SYMMETRIC_MARKER: &str = "http://www.w3.org/2002/07/owl#SymmetricProperty";

/// Parse a class expression rooted at node `id` into a [`Dl`] (NOT yet NNF; callers
/// apply [`Dl::nnf`]). Handles named classes, `owl:Thing`/`owl:Nothing`, intersections,
/// unions, complements, `oneOf` nominals, and every `owl:Restriction` shape (`some`/
/// `all`/`hasValue` + un/qualified `min`/`max`/exact cardinality).
/// Try each boolean-combinator shape (`intersectionOf`/`unionOf`/
/// `complementOf`/`oneOf`) for `id`. Split out of `parse_dl`
/// (extract-method, cx/wD8) — same terms, same order as before. `Some(_)`
/// means `id` matched one of these shapes (the inner `Option<Dl>` may still
/// be `None` on a malformed sub-expression, exactly as the original early
/// `return`s propagated a `?` failure); `None` means none matched and
/// parsing should continue to the next category.
fn try_parse_dl_boolean_combinator(idx: &TripleIndex, id: &str) -> Option<Option<Dl>> {
    if let Some(head) = idx.first_object(id, OWL_INTERSECTION_OF) {
        let cs = parse_list_concepts(idx, head);
        return Some(cs.map(Dl::And));
    }
    if let Some(head) = idx.first_object(id, OWL_UNION_OF) {
        let cs = parse_list_concepts(idx, head);
        return Some(cs.map(Dl::Or));
    }
    if let Some(inner) = idx.first_object(id, OWL_COMPLEMENT_OF) {
        return Some(parse_dl(idx, &term_key(inner)).map(|c| Dl::Not(Box::new(c))));
    }
    if let Some(head) = idx.first_object(id, OWL_ONE_OF) {
        // {a₁, …, aₙ}  ≡  {a₁} ⊔ … ⊔ {aₙ}.
        let members: Vec<Dl> = parse_rdf_list(idx, head)
            .iter()
            .map(|t| Dl::Nominal(term_key(t)))
            .collect();
        if members.len() == 1 {
            return Some(Some(members.into_iter().next().unwrap()));
        }
        return Some(Some(Dl::Or(members)));
    }
    None
}

/// Try the `owl:Restriction` shapes (`some`/`all`/`hasValue` + un/qualified
/// `min`/`max`/exact cardinality) for `id`. Split out of `parse_dl`
/// (extract-method, cx/wD8) — same terms, same order as before. Same
/// `Some(_)`/`None` convention as [`try_parse_dl_boolean_combinator`]: `None`
/// means `id` has no `owl:onProperty` (not a restriction at all), `Some(_)`
/// means it does — and once it does, the original always returned from
/// inside this block (down to the explicit `return None` at the end for an
/// unrecognized restriction shape).
fn try_parse_dl_restriction(idx: &TripleIndex, id: &str) -> Option<Option<Dl>> {
    let on_prop = idx.first_object(id, OWL_ON_PROPERTY)?;
    let role = term_key(on_prop);
    if let Some(filler) = idx.first_object(id, OWL_SOME_VALUES_FROM) {
        return Some(parse_dl(idx, &term_key(filler)).map(|c| Dl::Some(role, Box::new(c))));
    }
    if let Some(filler) = idx.first_object(id, OWL_ALL_VALUES_FROM) {
        return Some(parse_dl(idx, &term_key(filler)).map(|c| Dl::All(role, Box::new(c))));
    }
    if let Some(val) = idx.first_object(id, OWL_HAS_VALUE) {
        // ∃r.{a}: the value is a nominal filler.
        return Some(Some(Dl::Some(role, Box::new(Dl::Nominal(term_key(val))))));
    }
    // Qualified cardinalities (need an onClass filler).
    let on_class = idx
        .first_object(id, OWL_ON_CLASS)
        .and_then(|c| parse_dl(idx, &term_key(c)))
        .unwrap_or(Dl::Top);
    if let Some(n) = idx
        .first_object(id, OWL_MIN_QUALIFIED_CARDINALITY)
        .and_then(literal_usize)
    {
        return Some(Some(Dl::Min(n, role, Box::new(on_class))));
    }
    if let Some(n) = idx
        .first_object(id, OWL_MAX_QUALIFIED_CARDINALITY)
        .and_then(literal_usize)
    {
        return Some(Some(Dl::Max(n, role, Box::new(on_class))));
    }
    if let Some(n) = idx
        .first_object(id, OWL_QUALIFIED_CARDINALITY)
        .and_then(literal_usize)
    {
        return Some(Some(Dl::And(vec![
            Dl::Min(n, role.clone(), Box::new(on_class.clone())),
            Dl::Max(n, role, Box::new(on_class)),
        ])));
    }
    // Unqualified cardinalities (filler ⊤).
    if let Some(n) = idx
        .first_object(id, OWL_MIN_CARDINALITY)
        .and_then(literal_usize)
    {
        return Some(Some(Dl::Min(n, role, Box::new(Dl::Top))));
    }
    if let Some(n) = idx
        .first_object(id, OWL_MAX_CARDINALITY)
        .and_then(literal_usize)
    {
        return Some(Some(Dl::Max(n, role, Box::new(Dl::Top))));
    }
    if let Some(n) = idx
        .first_object(id, OWL_CARDINALITY)
        .and_then(literal_usize)
    {
        return Some(Some(Dl::And(vec![
            Dl::Min(n, role.clone(), Box::new(Dl::Top)),
            Dl::Max(n, role, Box::new(Dl::Top)),
        ])));
    }
    Some(None)
}

fn parse_dl(idx: &TripleIndex, id: &str) -> Option<Dl> {
    // Builtins.
    if id == iri(OWL_THING) {
        return Some(Dl::Top);
    }
    if id == iri(OWL_NOTHING) {
        return Some(Dl::Bottom);
    }
    if let Some(result) = try_parse_dl_boolean_combinator(idx, id) {
        return result;
    }
    if let Some(result) = try_parse_dl_restriction(idx, id) {
        return result;
    }
    // A named class IRI.
    if id.starts_with('<') {
        return Some(Dl::Atom(id.to_string()));
    }
    None
}

fn parse_list_concepts(idx: &TripleIndex, head: &Term) -> Option<Vec<Dl>> {
    let mut out = Vec::new();
    for item in parse_rdf_list(idx, head) {
        out.push(parse_dl(idx, &term_key(&item))?);
    }
    (!out.is_empty()).then_some(out)
}

fn literal_usize(t: &Term) -> Option<usize> {
    match t {
        Term::Literal(l) => l.value().parse::<usize>().ok(),
        _ => None,
    }
}

// ── The completion graph ─────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Node {
    label: BTreeSet<Dl>,
    /// The named individual(s) this node stands for. A set (not a single id) because a
    /// `≤`/nominal merge can identify several individuals into one node — every identity
    /// must be retained so a later `{a}` claim on a DIFFERENT node still identifies with
    /// this one (the bug that otherwise lets `x ∈ {mon,tue}` escape when `tue` merged).
    nominal: BTreeSet<String>,
    /// The tree parent (used for equality blocking); `None` for root nodes.
    parent: Option<usize>,
}

/// The role hierarchy + transitivity, precomputed once per reasoning call.
#[derive(Clone, Debug, Default)]
struct RoleInfo {
    /// `super_roles[r] = { s : r ⊑* s }` (reflexive-transitive closure, incl. `r`).
    super_roles: HashMap<String, BTreeSet<String>>,
    transitive: BTreeSet<String>,
}

impl RoleInfo {
    fn build(ont: &DlOntology) -> Self {
        let mut supers: HashMap<String, BTreeSet<String>> = HashMap::new();
        // Seed reflexive.
        let mut roles: BTreeSet<String> = BTreeSet::new();
        for (r, s) in &ont.sub_roles {
            roles.insert(r.clone());
            roles.insert(s.clone());
        }
        roles.extend(ont.transitive.iter().cloned());
        for r in &roles {
            supers.entry(r.clone()).or_default().insert(r.clone());
        }
        for (r, s) in &ont.sub_roles {
            supers.entry(r.clone()).or_default().insert(s.clone());
        }
        // Transitive closure of ⊑.
        let mut changed = true;
        while changed {
            changed = false;
            let snapshot = supers.clone();
            for sups in supers.values_mut() {
                let extra: Vec<String> = sups
                    .iter()
                    .filter_map(|s| snapshot.get(s))
                    .flatten()
                    .cloned()
                    .collect();
                for e in extra {
                    if sups.insert(e) {
                        changed = true;
                    }
                }
            }
        }
        Self {
            super_roles: supers,
            transitive: ont.transitive.clone(),
        }
    }

    /// Is `sup` a super-role of `edge` (i.e. `edge ⊑* sup`, incl. `edge == sup`)?
    fn is_super(&self, sup: &str, edge: &str) -> bool {
        if sup == edge {
            return true;
        }
        self.super_roles
            .get(edge)
            .map(|s| s.contains(sup))
            .unwrap_or(false)
    }
}

/// A completion graph (one non-deterministic branch). Cloned to explore `⊔`/`≤`/choose
/// alternatives. Node identity is a union-find representative (nominal / `≤`-merges).
#[derive(Clone)]
struct Completion {
    nodes: Vec<Node>,
    /// Union-find parent per node id (`≤`-merge and nominal identification).
    uf: Vec<usize>,
    /// Raw role edges `(from, role, to)` — endpoints interpreted through [`find`].
    edges: Vec<(usize, String, usize)>,
    /// Raw inequalities `(a, b)` — a clash iff `find(a) == find(b)`.
    neq: Vec<(usize, usize)>,
    /// The internalized TBox: each entry (NNF `¬C ⊔ D`) is stamped into every node.
    ct: Rc<Vec<Dl>>,
    roles: Rc<RoleInfo>,
}

/// One alternative of a non-deterministic choice point.
#[derive(Clone, Debug)]
enum Branch {
    /// Add a concept to a node's label (`⊔` / `choose`).
    AddConcept(usize, Dl),
    /// Merge two nodes (`≤`).
    Merge(usize, usize),
}

impl Completion {
    fn new(ct: Rc<Vec<Dl>>, roles: Rc<RoleInfo>) -> Self {
        Self {
            nodes: Vec::new(),
            uf: Vec::new(),
            edges: Vec::new(),
            neq: Vec::new(),
            ct,
            roles,
        }
    }

    fn add_node(
        &mut self,
        mut label: BTreeSet<Dl>,
        nominal: BTreeSet<String>,
        parent: Option<usize>,
    ) -> usize {
        for c in self.ct.iter() {
            label.insert(c.clone());
        }
        let id = self.nodes.len();
        self.nodes.push(Node {
            label,
            nominal,
            parent,
        });
        self.uf.push(id);
        id
    }

    fn find(&self, mut i: usize) -> usize {
        while self.uf[i] != i {
            i = self.uf[i];
        }
        i
    }

    fn reps(&self) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|&i| self.find(i) == i)
            .collect()
    }

    fn union(&mut self, a: usize, b: usize) {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return;
        }
        let (keep, drop) = if ra < rb { (ra, rb) } else { (rb, ra) };
        self.uf[drop] = keep;
        let dropped = std::mem::take(&mut self.nodes[drop].label);
        self.nodes[keep].label.extend(dropped);
        let dropped_noms = std::mem::take(&mut self.nodes[drop].nominal);
        self.nodes[keep].nominal.extend(dropped_noms);
    }

    /// Representative r-neighbors of `x` for role `r` (via any sub-role edge), each
    /// paired with whether it qualifies for filler `c` (`c == Top` ⇒ always).
    fn role_neighbors(&self, x: usize, r: &str) -> Vec<usize> {
        let x = self.find(x);
        let mut out = Vec::new();
        for (f, e, t) in &self.edges {
            if self.find(*f) == x && self.roles.is_super(r, e) {
                let t = self.find(*t);
                if !out.contains(&t) {
                    out.push(t);
                }
            }
        }
        out
    }

    fn qualifies(&self, node: usize, c: &Dl) -> bool {
        matches!(c, Dl::Top) || self.nodes[self.find(node)].label.contains(c)
    }

    /// Equality blocking: a non-nominal, non-root node is blocked by a non-nominal
    /// ancestor with an identical label (bounds the tree — termination).
    fn is_blocked(&self, x: usize) -> bool {
        let x = self.find(x);
        if !self.nodes[x].nominal.is_empty() {
            return false;
        }
        let mut cur = self.nodes[x].parent;
        while let Some(p) = cur {
            let p = self.find(p);
            if p == x {
                break;
            }
            if self.nodes[p].nominal.is_empty() && self.nodes[p].label == self.nodes[x].label {
                return true;
            }
            cur = self.nodes[p].parent;
        }
        false
    }

    /// A clash in the current graph: `⊥`, `{A,¬A}`, `{ {a},¬{a} }`, a self-inequality,
    /// or a `≤n r.C` with `n+1` pairwise-distinct `C`-witnesses.
    /// Whether node `i`'s own label clashes (contains `⊥`, a directly
    /// negated atom/nominal, or a violated `≤n r.f` cardinality). Split out
    /// of `has_clash` (extract-method, cx/wD8) — same terms, same order as
    /// before.
    fn node_has_clash(&self, i: usize) -> bool {
        let label = &self.nodes[i].label;
        if label.contains(&Dl::Bottom) {
            return true;
        }
        for c in label {
            match c {
                Dl::Not(inner)
                    if matches!(**inner, Dl::Atom(_) | Dl::Nominal(_)) && label.contains(inner) =>
                {
                    return true;
                }
                Dl::Max(n, r, f) => {
                    let neigh: Vec<usize> = self
                        .role_neighbors(i, r)
                        .into_iter()
                        .filter(|&y| self.qualifies(y, f))
                        .collect();
                    if neigh.len() > *n && self.all_pairwise_distinct(&neigh) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    fn has_clash(&self) -> bool {
        // Forced self-inequality (from a merge of two ≠ nodes).
        for (a, b) in &self.neq {
            if self.find(*a) == self.find(*b) {
                return true;
            }
        }
        for i in self.reps() {
            if self.node_has_clash(i) {
                return true;
            }
        }
        false
    }

    fn distinct(&self, a: usize, b: usize) -> bool {
        let (a, b) = (self.find(a), self.find(b));
        self.neq.iter().any(|(x, y)| {
            (self.find(*x) == a && self.find(*y) == b) || (self.find(*x) == b && self.find(*y) == a)
        })
    }

    fn all_pairwise_distinct(&self, ns: &[usize]) -> bool {
        for i in 0..ns.len() {
            for j in (i + 1)..ns.len() {
                if !self.distinct(ns[i], ns[j]) {
                    return false;
                }
            }
        }
        true
    }

    /// Apply the deterministic NON-GENERATING rules once across the graph (nominal
    /// identification, `⊓`, `∀` + transitive folding); return whether anything changed.
    /// These have priority OVER the generating rules ([`step_generating`]), so a node's
    /// label is stable before it is used to block or to spawn successors.
    /// Phase (0a) of `step_nongenerating`: union nodes that already share the
    /// SAME nominal (from a prior merge). Split out (extract-method,
    /// cx/wD8) — same terms, same order as before. Returns the
    /// nominal->representative map built along the way (fed into phase
    /// (0b)) and whether anything changed.
    fn merge_nodes_sharing_a_nominal(&mut self) -> (HashMap<String, usize>, bool) {
        let mut changed = false;
        let mut nom_rep: HashMap<String, usize> = HashMap::new();
        for i in self.reps() {
            for a in self.nodes[i].nominal.clone() {
                match nom_rep.entry(a) {
                    Entry::Occupied(e) => {
                        let j = *e.get();
                        if self.find(j) != self.find(i) {
                            self.union(i, j);
                            changed = true;
                        }
                    }
                    Entry::Vacant(v) => {
                        v.insert(self.find(i));
                    }
                }
            }
        }
        (nom_rep, changed)
    }

    /// Phase (0b) of `step_nongenerating`: a `{a}` nominal CONCEPT identifies
    /// its node with the individual `a`. Split out (extract-method,
    /// cx/wD8) — same terms, same order as before.
    fn identify_nominal_concepts(&mut self, mut nom_rep: HashMap<String, usize>) -> bool {
        let mut changed = false;
        let nominal_concepts: Vec<(usize, String)> = self
            .reps()
            .into_iter()
            .flat_map(|i| {
                self.nodes[i]
                    .label
                    .iter()
                    .filter_map(|c| match c {
                        Dl::Nominal(a) => std::option::Option::Some((i, a.clone())),
                        _ => std::option::Option::None,
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        for (i, a) in nominal_concepts {
            match nom_rep.get(&a).copied() {
                std::option::Option::Some(j) => {
                    if self.find(i) != self.find(j) {
                        self.union(i, j);
                        changed = true;
                    }
                }
                std::option::Option::None => {
                    // No node yet represents individual `a`: this node becomes it.
                    let r = self.find(i);
                    if self.nodes[r].nominal.insert(a.clone()) {
                        nom_rep.insert(a, r);
                        changed = true;
                    }
                }
            }
        }
        changed
    }

    /// Phase (1) of `step_nongenerating`: the ⊓-rule. Split out
    /// (extract-method, cx/wD8) — same terms, same order as before.
    fn step_and_rule(&mut self) -> bool {
        let mut changed = false;
        for i in self.reps() {
            let ands: Vec<Vec<Dl>> = self.nodes[i]
                .label
                .iter()
                .filter_map(|c| match c {
                    Dl::And(v) => std::option::Option::Some(v.clone()),
                    _ => std::option::Option::None,
                })
                .collect();
            for v in ands {
                for x in v {
                    if self.nodes[i].label.insert(x) {
                        changed = true;
                    }
                }
            }
        }
        changed
    }

    /// Phase (2) of `step_nongenerating`: the ∀-rule (+ transitive-role
    /// folding). Split out (extract-method, cx/wD8) — same terms, same
    /// order as before.
    /// Apply the ∀-rule for one `(role, filler)` pair over one `(e, t)` edge
    /// (+ transitive-role folding). Split out of `apply_all_rule_to_edges`
    /// (extract-method, cx/wD8) — same terms, same order as before.
    fn apply_all_rule_to_edge(&mut self, role: &str, filler: &Dl, e: &str, t: usize) -> bool {
        if !self.roles.is_super(role, e) {
            return false;
        }
        let mut changed = self.nodes[t].label.insert(filler.clone());
        // Transitive folding: ∀r.C over a transitive sub-role e ⇒ ∀e.C
        // on the successor, so C reaches the whole e-chain.
        if self.roles.transitive.contains(e) {
            let prop = Dl::All(e.to_string(), Box::new(filler.clone()));
            if self.nodes[t].label.insert(prop) {
                changed = true;
            }
        }
        changed
    }

    /// Apply the ∀-rule's known `(role, filler)` pairs across `edges`
    /// (+ transitive-role folding). Split out of `step_all_rule`
    /// (extract-method, cx/wD8) — same terms, same order as before.
    fn apply_all_rule_to_edges(&mut self, alls: &[(String, Dl)], edges: &[(String, usize)]) -> bool {
        let mut changed = false;
        for (role, filler) in alls {
            for (e, t) in edges {
                if self.apply_all_rule_to_edge(role, filler, e, *t) {
                    changed = true;
                }
            }
        }
        changed
    }

    fn step_all_rule(&mut self) -> bool {
        let mut changed = false;
        for i in self.reps() {
            let alls: Vec<(String, Dl)> = self.nodes[i]
                .label
                .iter()
                .filter_map(|c| match c {
                    Dl::All(r, f) => std::option::Option::Some((r.clone(), (**f).clone())),
                    _ => std::option::Option::None,
                })
                .collect();
            if alls.is_empty() {
                continue;
            }
            let edges: Vec<(String, usize)> = self
                .edges
                .iter()
                .filter(|(f, _, _)| self.find(*f) == i)
                .map(|(_, e, t)| (e.clone(), self.find(*t)))
                .collect();
            if self.apply_all_rule_to_edges(&alls, &edges) {
                changed = true;
            }
        }
        changed
    }

    fn step_nongenerating(&mut self) -> bool {
        // (0) Nominal identification: nodes sharing a nominal are one; a `{a}` concept
        // identifies its node with the individual `a`.
        let (nom_rep, changed0) = self.merge_nodes_sharing_a_nominal();
        let changed1 = self.identify_nominal_concepts(nom_rep);
        // (1) ⊓-rule.
        let changed2 = self.step_and_rule();
        // (2) ∀-rule (+ transitive-role folding).
        let changed3 = self.step_all_rule();
        changed0 || changed1 || changed2 || changed3
    }

    /// Apply the deterministic GENERATING rules once (`∃`, `≥`) — lowest priority, and
    /// only on non-blocked nodes, so equality blocking terminates the tree.
    /// Phase (3) of `step_generating`: the ∃-rule. Split out
    /// (extract-method, cx/wD8) — same terms, same order as before,
    /// including the `NODE_CAP` safety-valve early return: the original
    /// `return changed;` mid-loop exited `step_generating` ENTIRELY
    /// (skipping the ≥-rule too), so this returns `(changed, true)` to let
    /// the caller reproduce that exact short-circuit.
    fn step_exists_rule(&mut self) -> (bool, bool) {
        let mut changed = false;
        for i in self.reps() {
            if self.is_blocked(i) {
                continue;
            }
            let exs: Vec<(String, Dl)> = self.nodes[i]
                .label
                .iter()
                .filter_map(|c| match c {
                    Dl::Some(r, f) => std::option::Option::Some((r.clone(), (**f).clone())),
                    _ => std::option::Option::None,
                })
                .collect();
            for (role, filler) in exs {
                let neighbors = self.role_neighbors(i, &role);
                let satisfied = neighbors
                    .iter()
                    .any(|&y| self.nodes[y].label.contains(&filler));
                if !satisfied {
                    if self.nodes.len() >= NODE_CAP {
                        return (changed, true);
                    }
                    let mut lab = BTreeSet::new();
                    lab.insert(filler.clone());
                    let z = self.add_node(lab, BTreeSet::new(), std::option::Option::Some(i));
                    self.edges.push((i, role.clone(), z));
                    changed = true;
                }
            }
        }
        (changed, false)
    }

    /// Apply the ≥-rule's known `(n, role, filler)` obligations for one node
    /// `i`. Split out of `step_min_rule` (extract-method, cx/wD8) — same
    /// terms, same order as before, same `NODE_CAP` short-circuit
    /// convention as [`Self::step_exists_rule`].
    fn apply_min_rule_to_node(&mut self, i: usize) -> (bool, bool) {
        let mut changed = false;
        let mins: Vec<(usize, String, Dl)> = self.nodes[i]
            .label
            .iter()
            .filter_map(|c| match c {
                Dl::Min(n, r, f) => std::option::Option::Some((*n, r.clone(), (**f).clone())),
                _ => std::option::Option::None,
            })
            .collect();
        for (n, role, filler) in mins {
            if n == 0 {
                continue;
            }
            let witnesses: Vec<usize> = self
                .role_neighbors(i, &role)
                .into_iter()
                .filter(|&y| self.qualifies(y, &filler))
                .collect();
            if witnesses.len() < n {
                if self.nodes.len() >= NODE_CAP {
                    return (changed, true);
                }
                let mut lab = BTreeSet::new();
                if !matches!(filler, Dl::Top) {
                    lab.insert(filler.clone());
                }
                let z = self.add_node(lab, BTreeSet::new(), std::option::Option::Some(i));
                self.edges.push((i, role.clone(), z));
                // Distinct from every existing qualifying witness (pairwise ≠).
                for &y in &witnesses {
                    self.neq.push((z, y));
                }
                changed = true;
            }
        }
        (changed, false)
    }

    /// Phase (4) of `step_generating`: the ≥-rule. Split out
    /// (extract-method, cx/wD8) — same terms, same order as before,
    /// same `NODE_CAP` short-circuit convention as [`Self::step_exists_rule`].
    fn step_min_rule(&mut self) -> (bool, bool) {
        let mut changed = false;
        for i in self.reps() {
            if self.is_blocked(i) {
                continue;
            }
            let (c, capped) = self.apply_min_rule_to_node(i);
            if c {
                changed = true;
            }
            if capped {
                return (changed, true);
            }
        }
        (changed, false)
    }

    fn step_generating(&mut self) -> bool {
        // (3) ∃-rule.
        let (changed3, capped3) = self.step_exists_rule();
        if capped3 {
            return changed3;
        }
        // (4) ≥-rule.
        let (changed4, _capped4) = self.step_min_rule();
        changed3 || changed4
    }

    /// Find the first non-deterministic choice point and return its alternatives, or
    /// `None` when the graph is complete (no rule applies): `⊔`, then `choose`, then `≤`.
    fn next_nondet(&self) -> Option<Vec<Branch>> {
        // ⊔-rule.
        for i in self.reps() {
            for c in &self.nodes[i].label {
                if let Dl::Or(ds) = c {
                    if !ds.iter().any(|d| self.nodes[i].label.contains(d)) {
                        return Some(
                            ds.iter()
                                .map(|d| Branch::AddConcept(i, d.clone()))
                                .collect(),
                        );
                    }
                }
            }
        }
        // choose-rule (qualified number restrictions): a witness must commit to C / ¬C.
        for i in self.reps() {
            let restr: Vec<(String, Dl)> = self.nodes[i]
                .label
                .iter()
                .filter_map(|c| match c {
                    Dl::Max(_, r, f) | Dl::Min(_, r, f) if !matches!(**f, Dl::Top) => {
                        std::option::Option::Some((r.clone(), (**f).clone()))
                    }
                    _ => std::option::Option::None,
                })
                .collect();
            for (role, filler) in &restr {
                let neg = filler.clone().negate();
                for y in self.role_neighbors(i, role) {
                    let has = self.nodes[y].label.contains(filler);
                    let has_neg = self.nodes[y].label.contains(&neg);
                    if !has && !has_neg {
                        return Some(vec![
                            Branch::AddConcept(y, filler.clone()),
                            Branch::AddConcept(y, neg.clone()),
                        ]);
                    }
                }
            }
        }
        // ≤-rule: too many witnesses ⇒ merge a mergeable (not-yet-≠) pair.
        for i in self.reps() {
            let maxes: Vec<(usize, Dl)> = self.nodes[i]
                .label
                .iter()
                .filter_map(|c| match c {
                    Dl::Max(n, _r, f) => std::option::Option::Some((*n, (**f).clone())),
                    _ => std::option::Option::None,
                })
                .collect();
            for (n, filler) in maxes {
                // gather qualifying witnesses per the same role via re-scan
                let roles_here: Vec<String> = self.nodes[i]
                    .label
                    .iter()
                    .filter_map(|c| match c {
                        Dl::Max(m, r, f) if *m == n && **f == filler => {
                            std::option::Option::Some(r.clone())
                        }
                        _ => std::option::Option::None,
                    })
                    .collect();
                for role in roles_here {
                    let ws: Vec<usize> = self
                        .role_neighbors(i, &role)
                        .into_iter()
                        .filter(|&y| self.qualifies(y, &filler))
                        .collect();
                    if ws.len() > n {
                        let mut branches = Vec::new();
                        for a in 0..ws.len() {
                            for b in (a + 1)..ws.len() {
                                if !self.distinct(ws[a], ws[b]) {
                                    branches.push(Branch::Merge(ws[a], ws[b]));
                                }
                            }
                        }
                        if !branches.is_empty() {
                            return Some(branches);
                        }
                    }
                }
            }
        }
        None
    }

    fn apply_branch(&mut self, b: Branch) {
        match b {
            Branch::AddConcept(i, c) => {
                let r = self.find(i);
                self.nodes[r].label.insert(c);
            }
            Branch::Merge(a, b) => self.union(a, b),
        }
    }

    /// Is there a clash-free complete completion reachable from this graph? The tableau
    /// decision procedure: saturate deterministically, branch on the first
    /// non-determinism, and recurse. `true` ⇒ satisfiable.
    /// Saturate the non-generating deterministic rules to fixpoint, checking
    /// for a clash before and after. Split out of `expand`'s step (1)
    /// (extract-method, cx/wD8) — same terms, same order as before. Returns
    /// whether a clash was found.
    fn saturate_nongenerating(&mut self) -> bool {
        let mut changed = true;
        while changed {
            if self.has_clash() {
                return true;
            }
            changed = self.step_nongenerating();
        }
        self.has_clash()
    }

    /// Try every branch of a non-deterministic choice point, recursing into
    /// each. Split out of `expand`'s step (2) (extract-method, cx/wD8) —
    /// same terms, same order as before.
    fn try_nondet_branches(&mut self, branches: Vec<Branch>) -> bool {
        for b in branches {
            let mut child = self.clone();
            child.apply_branch(b);
            if child.expand() {
                return true;
            }
        }
        false
    }

    fn expand(&mut self) -> bool {
        loop {
            // (1) Saturate the non-generating deterministic rules to fixpoint.
            if self.saturate_nongenerating() {
                return false;
            }
            // (2) Resolve the first non-deterministic choice point (⊔ / choose / ≤).
            if let std::option::Option::Some(branches) = self.next_nondet() {
                return self.try_nondet_branches(branches);
            }
            // (3) Generating rules last (∃ / ≥) so blocking is checked on stable labels.
            if self.nodes.len() >= NODE_CAP {
                return true; // safety valve (see NODE_CAP)
            }
            if self.step_generating() {
                continue;
            }
            // (4) No rule applies and no clash ⇒ a clash-free complete model exists.
            return true;
        }
    }
}

// ── Building blocks shared by the entry points ───────────────────────────────

/// The internalized TBox: NNF `¬C ⊔ D` per GCI `C ⊑ D`, stamped into every node.
fn build_ct(ont: &DlOntology) -> Vec<Dl> {
    ont.gcis
        .iter()
        .map(|(c, d)| Dl::Or(vec![c.clone().negate(), d.clone().nnf()]).nnf())
        .collect()
}

/// Test whether a single fresh (anonymous) root labeled `extra` (∪ the TBox) has a
/// clash-free model — i.e. the conjunction of `extra` is satisfiable w.r.t. the TBox.
fn concept_sat(ont: &DlOntology, extra: Vec<Dl>) -> bool {
    let ct = Rc::new(build_ct(ont));
    let roles = Rc::new(RoleInfo::build(ont));
    let mut comp = Completion::new(ct, roles);
    let mut lab = BTreeSet::new();
    for c in extra {
        lab.insert(c.nnf());
    }
    comp.add_node(lab, BTreeSet::new(), None);
    comp.expand()
}

// ── Public entry points ──────────────────────────────────────────────────────

/// **Ontology consistency** (CONCEPT:EG-KG.ontology.concept-2): does the ontology (TBox + ABox) have a
/// model? Builds a completion with one nominal root per named individual (carrying its
/// asserted types + role edges + same/different constraints) and runs the tableau. With
/// an empty ABox it probes a single anonymous `⊤` node (detects a globally-unsatisfiable
/// TBox). `true` ⇒ consistent.
pub fn is_consistent(ont: &DlOntology) -> bool {
    let ct = Rc::new(build_ct(ont));
    let roles = Rc::new(RoleInfo::build(ont));
    let mut comp = Completion::new(ct, roles);

    // Gather every individual mentioned anywhere in the ABox.
    let mut inds: BTreeSet<String> = ont.individuals.clone();
    for (a, _) in &ont.abox_types {
        inds.insert(a.clone());
    }
    for (a, _, b) in &ont.abox_roles {
        inds.insert(a.clone());
        inds.insert(b.clone());
    }
    for (a, b) in ont.same_as.iter().chain(ont.different_from.iter()) {
        inds.insert(a.clone());
        inds.insert(b.clone());
    }

    if inds.is_empty() {
        // Empty ABox: probe ⊤-satisfiability of the TBox.
        let mut lab = BTreeSet::new();
        lab.insert(Dl::Top);
        comp.add_node(lab, BTreeSet::new(), None);
        return comp.expand();
    }

    let mut id_of: HashMap<String, usize> = HashMap::new();
    for ind in &inds {
        let mut noms = BTreeSet::new();
        noms.insert(ind.clone());
        let node = comp.add_node(BTreeSet::new(), noms, None);
        id_of.insert(ind.clone(), node);
    }
    for (a, c) in &ont.abox_types {
        let n = id_of[a];
        comp.nodes[n].label.insert(c.clone().nnf());
    }
    for (a, r, b) in &ont.abox_roles {
        comp.edges.push((id_of[a], r.clone(), id_of[b]));
    }
    for (a, b) in &ont.same_as {
        comp.union(id_of[a], id_of[b]);
    }
    for (a, b) in &ont.different_from {
        comp.neq.push((id_of[a], id_of[b]));
    }
    comp.expand()
}

/// **Concept subsumption** (CONCEPT:EG-KG.ontology.concept-2): does `sub ⊑ sup` hold w.r.t. the TBox?
/// Decided by the unsatisfiability of `sub ⊓ ¬sup`. `sub`/`sup` are named-class IRIs in
/// canonical `<iri>` form (`owl:Thing`/`owl:Nothing` fold to `⊤`/`⊥`, so
/// `is_subsumed(_, A, owl:Nothing)` is the unsatisfiability test for `A`).
pub fn is_subsumed(ont: &DlOntology, sub: &str, sup: &str) -> bool {
    let sub_c = named_concept(sub);
    let neg_sup = named_concept(sup).negate();
    !concept_sat(ont, vec![sub_c, neg_sup])
}

/// **Instance checking** (CONCEPT:EG-KG.ontology.concept-2): is individual `ind` a member of `class`?
/// Decided by the inconsistency of `ont ∪ { ind : ¬class }`. `ind`/`class` are canonical
/// `<iri>` ids.
pub fn is_instance(ont: &DlOntology, ind: &str, class: &str) -> bool {
    let mut ont2 = ont.clone();
    ont2.individuals.insert(ind.to_string());
    ont2.abox_types
        .push((ind.to_string(), named_concept(class).negate()));
    !is_consistent(&ont2)
}

/// **Full classification** (CONCEPT:EG-KG.ontology.concept-2): the complete subsumer set `S(A) = { B :
/// A ⊑ B }` for every named class `A` (reflexive, includes `owl:Thing`, and
/// `owl:Nothing` for an unsatisfiable class). O(n²) subsumption tests over the signature.
pub fn classify_dl(ont: &DlOntology) -> BTreeMap<String, BTreeSet<String>> {
    let classes: Vec<String> = ont.classes.iter().cloned().collect();
    let thing = iri(OWL_THING);
    let nothing = iri(OWL_NOTHING);
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for a in &classes {
        let mut subs = BTreeSet::new();
        subs.insert(a.clone());
        subs.insert(thing.clone());
        // Unsatisfiable class ⊑ everything (report ⊥ membership explicitly).
        if is_subsumed(ont, a, &nothing) {
            subs.insert(nothing.clone());
            for b in &classes {
                subs.insert(b.clone());
            }
        } else {
            for b in &classes {
                if a != b && is_subsumed(ont, a, b) {
                    subs.insert(b.clone());
                }
            }
        }
        out.insert(a.clone(), subs);
    }
    out
}

// ── The engine picker: EL⁺/RL fast path vs. tableau ──────────────────────────

/// Which reasoning engine [`reason_dl`] chose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DlEngine {
    /// The tractable monotone EL⁺/RL completion ([`crate::owl`]).
    ElRl,
    /// The OWL-DL tableau (this module).
    Tableau,
}

/// The result of [`reason_dl`]: which engine ran, the named-class subsumer hierarchy,
/// and whether the ontology is consistent.
#[derive(Clone, Debug)]
pub struct DlReasoningResult {
    pub engine: DlEngine,
    pub subsumers: BTreeMap<String, BTreeSet<String>>,
    pub consistent: bool,
}

/// Does the triple set use any construct OUTSIDE the tractable EL⁺/RL envelope (so the
/// tableau is REQUIRED for completeness)? True on `complementOf`, `oneOf`, any
/// cardinality restriction, or a `unionOf` used as a SUPERCLASS (`_ ⊑ [unionOf …]` /
/// `_ ≡ [unionOf …]`) — the reasoning-by-cases direction the EL path deliberately drops.
fn needs_tableau(triples: &[Triple]) -> bool {
    let idx = TripleIndex::build(triples);
    for t in triples {
        match t.predicate.as_str() {
            OWL_COMPLEMENT_OF
            | OWL_ONE_OF
            | OWL_MIN_CARDINALITY
            | OWL_MAX_CARDINALITY
            | OWL_CARDINALITY
            | OWL_MIN_QUALIFIED_CARDINALITY
            | OWL_MAX_QUALIFIED_CARDINALITY
            | OWL_QUALIFIED_CARDINALITY => return true,
            RDFS_SUBCLASS_OF | OWL_EQUIVALENT_CLASS => {
                // A unionOf on the SUPERCLASS side needs reasoning-by-cases.
                let o = term_key(&t.object);
                if idx.first_object(&o, OWL_UNION_OF).is_some() {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

/// The engine picker (CONCEPT:EG-KG.ontology.concept-2): route through the tableau ONLY when a DL
/// construct forces it ([`needs_tableau`]); otherwise run the fast EL⁺/RL completion in
/// [`crate::owl`]. Correctness-preserving: both engines agree on the EL/RL fragment, and
/// the tableau is complete on the rest.
pub fn reason_dl(triples: &[Triple]) -> DlReasoningResult {
    if needs_tableau(triples) {
        let ont = parse_dl_ontology(triples);
        let subsumers = classify_dl(&ont);
        let consistent = is_consistent(&ont);
        DlReasoningResult {
            engine: DlEngine::Tableau,
            subsumers,
            consistent,
        }
    } else {
        let mut r = crate::owl::Reasoner::from_triples(triples);
        let cls = r.classify();
        DlReasoningResult {
            engine: DlEngine::ElRl,
            subsumers: cls.subsumers,
            consistent: cls.consistent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::parse_turtle;

    const PRE: &str = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
"#;

    fn ex(local: &str) -> String {
        iri(&format!("http://example.org/{local}"))
    }
    fn ont(body: &str) -> DlOntology {
        let triples = parse_turtle(&format!("{PRE}{body}")).unwrap();
        parse_dl_ontology(&triples)
    }
    const NOTHING: &str = "http://www.w3.org/2002/07/owl#Nothing";

    // ── consistency ─────────────────────────────────────────────────────────

    /// `A ⊑ B ⊓ ¬B` with an instance `a : A` is INCONSISTENT (a forces the clash).
    #[test]
    fn consistency_a_sub_b_and_not_b_is_inconsistent() {
        let o = ont(r#"
ex:A rdfs:subClassOf [ owl:intersectionOf ( ex:B [ owl:complementOf ex:B ] ) ] .
ex:a rdf:type ex:A .
"#);
        assert!(
            !is_consistent(&o),
            "A ⊑ B ⊓ ¬B with a:A must be inconsistent"
        );
    }

    /// A plainly-satisfiable ontology is consistent.
    #[test]
    fn consistency_satisfiable_ontology_is_consistent() {
        let o = ont(r#"
ex:A rdfs:subClassOf ex:B .
ex:B rdfs:subClassOf [ owl:complementOf ex:C ] .
ex:a rdf:type ex:A .
"#);
        assert!(is_consistent(&o), "A⊑B, B⊑¬C, a:A has a model");
    }

    // ── cardinality ───────────────────────────────────────────────────────────

    /// `X ≡ (≥2 r.⊤) ⊓ (≤1 r.⊤)` is unsatisfiable (X ⊑ ⊥).
    #[test]
    fn cardinality_min2_max1_unsatisfiable() {
        let o = ont(r#"
ex:X owl:equivalentClass [ owl:intersectionOf (
    [ owl:onProperty ex:r ; owl:minCardinality "2"^^<http://www.w3.org/2001/XMLSchema#nonNegativeInteger> ]
    [ owl:onProperty ex:r ; owl:maxCardinality "1"^^<http://www.w3.org/2001/XMLSchema#nonNegativeInteger> ]
) ] .
"#);
        assert!(
            is_subsumed(&o, &ex("X"), NOTHING),
            "≥2 r.⊤ ⊓ ≤1 r.⊤ is unsatisfiable"
        );
    }

    /// Functional-property clash: `r` functional, `X ⊑ ∃r.A ⊓ ∃r.B`, `A ⊓ B ⊑ ⊥` ⇒
    /// the two r-successors must merge (≤1) but then are both A and B — a clash.
    #[test]
    fn cardinality_functional_property_clash() {
        let o = ont(r#"
ex:r rdf:type owl:FunctionalProperty .
ex:A owl:disjointWith ex:B .
ex:X rdfs:subClassOf [ owl:onProperty ex:r ; owl:someValuesFrom ex:A ] .
ex:X rdfs:subClassOf [ owl:onProperty ex:r ; owl:someValuesFrom ex:B ] .
"#);
        assert!(
            is_subsumed(&o, &ex("X"), NOTHING),
            "functional r forces the A- and B-successors to merge into a disjoint clash"
        );
    }

    // ── disjunction / reasoning-by-cases (the headline) ───────────────────────

    /// **HEADLINE (DL beyond EL/RL):** reasoning-by-cases over a `unionOf` superclass.
    /// `C ⊑ A ⊔ B`, `A ⊑ D`, `B ⊑ D` ⇒ `C ⊑ D`. The EL⁺/RL path (`owl.rs`) drops the
    /// union-as-superclass axiom entirely (documented DEFERRED), so it CANNOT derive it;
    /// the tableau proves `C ⊓ ¬D` unsatisfiable by splitting on `A ⊔ B`.
    #[test]
    fn subsumption_by_cases_beyond_el() {
        let body = r#"
ex:C rdfs:subClassOf [ owl:unionOf ( ex:A ex:B ) ] .
ex:A rdfs:subClassOf ex:D .
ex:B rdfs:subClassOf ex:D .
"#;
        let o = ont(body);
        assert!(
            is_subsumed(&o, &ex("C"), &ex("D")),
            "tableau derives C ⊑ D by cases over A ⊔ B"
        );

        // Contrast: the EL⁺/RL completion CANNOT reach it.
        let triples = parse_turtle(&format!("{PRE}{body}")).unwrap();
        let mut el = crate::owl::Reasoner::from_triples(&triples);
        let cls = el.classify();
        assert!(
            !cls.entails_subclass(&ex("C"), &ex("D")),
            "EL/RL must NOT derive C ⊑ D (union-superclass reasoning-by-cases is deferred)"
        );

        // And reason_dl picks the tableau for this ontology.
        assert_eq!(reason_dl(&triples).engine, DlEngine::Tableau);
    }

    // ── nominals (oneOf / hasValue) ───────────────────────────────────────────

    /// `oneOf` reasoning-by-cases: `Weekday ≡ {mon, tue}`, `x : Weekday`,
    /// `x ≠ mon`, `x ≠ tue` ⇒ inconsistent (x must be one of two nominals it differs from).
    #[test]
    fn nominals_oneof_case_split_clash() {
        let o = ont(r#"
ex:Weekday owl:equivalentClass [ owl:oneOf ( ex:mon ex:tue ) ] .
ex:x rdf:type ex:Weekday .
ex:x owl:differentFrom ex:mon .
ex:x owl:differentFrom ex:tue .
"#);
        assert!(
            !is_consistent(&o),
            "x ∈ {{mon,tue}} but x≠mon and x≠tue is a nominal clash"
        );
    }

    /// `hasValue` nominal is consistent and grounds the value: `Italian ≡ ∃nation.{italy}`,
    /// `mario : Italian` has a model (mario is related to italy).
    #[test]
    fn nominals_hasvalue_consistent() {
        let o = ont(r#"
ex:Italian owl:equivalentClass [ owl:onProperty ex:nation ; owl:hasValue ex:italy ] .
ex:mario rdf:type ex:Italian .
"#);
        assert!(
            is_consistent(&o),
            "hasValue nominal ontology is satisfiable"
        );
        // mario is an instance of Italian by construction.
        assert!(is_instance(&o, &ex("mario"), &ex("Italian")));
    }

    // ── ∀ interacting with ∃ producing a clash ────────────────────────────────

    /// `∀`/`∃` interaction: `A ⊑ ∃r.⊤`, `A ⊑ ∀r.C`, `A ⊑ ∀r.¬C` ⇒ the r-successor is
    /// both C and ¬C — so `A` is unsatisfiable. EL's `cls-avf` alone cannot see the
    /// ¬C clash (no negation), the tableau does.
    #[test]
    fn all_values_meets_some_values_clash() {
        let o = ont(r#"
ex:A rdfs:subClassOf [ owl:onProperty ex:r ; owl:someValuesFrom owl:Thing ] .
ex:A rdfs:subClassOf [ owl:onProperty ex:r ; owl:allValuesFrom ex:C ] .
ex:A rdfs:subClassOf [ owl:onProperty ex:r ; owl:allValuesFrom [ owl:complementOf ex:C ] ] .
"#);
        assert!(
            is_subsumed(&o, &ex("A"), NOTHING),
            "∃r.⊤ with ∀r.C and ∀r.¬C forces a C/¬C clash on the successor"
        );
    }

    // ── is_instance ───────────────────────────────────────────────────────────

    /// Instance checking through subsumption + an existential: `a : A`, `A ⊑ B` ⇒ `a : B`.
    #[test]
    fn instance_checking_through_subclass() {
        let o = ont(r#"
ex:A rdfs:subClassOf ex:B .
ex:a rdf:type ex:A .
"#);
        assert!(is_instance(&o, &ex("a"), &ex("B")));
        assert!(!is_instance(&o, &ex("a"), &ex("C")));
    }

    // ── classify_dl + engine picker ───────────────────────────────────────────

    /// `classify_dl` produces the full hierarchy incl. the by-cases entailment.
    #[test]
    fn classify_dl_includes_by_cases() {
        let o = ont(r#"
ex:C rdfs:subClassOf [ owl:unionOf ( ex:A ex:B ) ] .
ex:A rdfs:subClassOf ex:D .
ex:B rdfs:subClassOf ex:D .
"#);
        let h = classify_dl(&o);
        assert!(h[&ex("C")].contains(&ex("D")), "C ⊑ D in the hierarchy");
    }

    /// The engine picker uses the fast EL/RL path when no DL construct is present.
    #[test]
    fn reason_dl_picks_el_for_tractable_ontology() {
        let triples = parse_turtle(&format!(
            "{PRE}ex:A rdfs:subClassOf ex:B . ex:B rdfs:subClassOf ex:E ."
        ))
        .unwrap();
        let res = reason_dl(&triples);
        assert_eq!(res.engine, DlEngine::ElRl);
        assert!(res.subsumers[&ex("A")].contains(&ex("E")));
    }
}
