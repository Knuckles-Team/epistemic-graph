//! W3 — a NATIVE OWL 2 reasoner (CONCEPT:KG-2.219).
//!
//! A pure-Rust OWL 2 **EL⁺ completion** reasoner (Baader/Brandt/Lutz "Pushing the EL
//! Envelope" — the algorithmic core of ELK/CEL) UNIONED with the full **OWL 2 RL**
//! property rule set. It parses OWL axioms directly from the RDF triples eg-rdf
//! already produces (oxttl/oxrdf — NO horned-owl, NO whelk-rs, so the OWL tier carries
//! ZERO native deps and stays Pi-clean), and provides:
//!
//!   * **classification** — the complete subclass hierarchy `S(A) = { B : A ⊑ B }`
//!     over named classes, derived through existential restrictions and role chains;
//!   * **consistency checking** — a `⊥`-derivation (a class forced to be `owl:Nothing`,
//!     i.e. unsatisfiable) makes the ontology inconsistent;
//!   * **incremental / differential** materialization — adding axioms only ADDS
//!     subsumers (EL completion is monotone), so a delta re-runs the fixpoint seeded
//!     from the prior closure instead of from scratch;
//!   * **justifications** — every inferred subsumption records which axiom(s) +
//!     premise(s) derived it (the completion rule + its antecedents).
//!
//! ## Why this reaches what `eg-compute::reasoning` (OWL 2 RL) cannot
//!
//! `eg-compute/src/reasoning.rs` is RL-flavored Datalog over MATERIALIZED node/edge
//! facts: subClassOf/subPropertyOf inheritance, transitive/symmetric/inverse closure,
//! domain/range, property chains. It can only propagate types/edges that exist on
//! concrete individuals.
//!
//! EL completion reasons at the **TBox / concept** level. Given
//! ```text
//!   Heart        ⊑ ∃partOf.Body
//!   ∃partOf.Body ⊑ HumanComponent     (existential restriction on the LHS)
//!   HumanHeart   ⊑ Heart
//!   partOf ∘ partOf ⊑ partOf
//! ```
//! it derives `HumanHeart ⊑ HumanComponent` — an entailment RL CANNOT reach, because
//! the consequent flows through an existential restriction `∃partOf.Body` on the LEFT
//! of a subclass axiom: there is no `partOf` edge to a concrete `Body` individual to
//! chain over; the inference is purely at the concept level. (See the `el_*` tests.)
//!
//! ## DAG home (the acyclic justification)
//!
//! This lives in `eg-rdf` (not `eg-compute`) because it CONSUMES the RDF/ontology the
//! `mapping`/oxttl layer parses, and eg-rdf is the crate that owns the oxrdf term
//! model. `eg-rdf` depends only on `eg-core` (it is parallel to `eg-query`), so adding
//! the reasoner here introduces no cycle. Putting it in `eg-compute` (left of eg-rdf
//! in the DAG) would force eg-compute to depend on oxrdf/oxttl — pulling the RDF stack
//! into every compute build — OR re-parse OWL by hand. The RL property reasoner stays
//! in eg-compute (it works over the property-graph); the EL concept reasoner lives
//! here, beside the RDF surface it reasons over. The native EL completion shares the
//! same monotone-fixpoint discipline `run_datalog_reasoning` uses.
//!
//! ## Pi contract
//!
//! Behind the `owl` feature (which implies `rdf`). Pure Rust — oxrdf/oxttl only, no
//! new dep — so `--features pi` (which carries `rdf`+`sparql`) can fold it in with no
//! native crate entering the tree.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use oxrdf::{Term, Triple};

// ── OWL / RDFS / RDF vocabulary IRIs ─────────────────────────────────────────

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const RDFS_SUBPROPERTY_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";

const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
const OWL_THING: &str = "http://www.w3.org/2002/07/owl#Thing";
const OWL_NOTHING: &str = "http://www.w3.org/2002/07/owl#Nothing";
const OWL_EQUIVALENT_CLASS: &str = "http://www.w3.org/2002/07/owl#equivalentClass";
const OWL_INTERSECTION_OF: &str = "http://www.w3.org/2002/07/owl#intersectionOf";
const OWL_SOME_VALUES_FROM: &str = "http://www.w3.org/2002/07/owl#someValuesFrom";
const OWL_ON_PROPERTY: &str = "http://www.w3.org/2002/07/owl#onProperty";
const OWL_PROPERTY_CHAIN_AXIOM: &str = "http://www.w3.org/2002/07/owl#propertyChainAxiom";
const OWL_TRANSITIVE_PROPERTY: &str = "http://www.w3.org/2002/07/owl#TransitiveProperty";
const OWL_SYMMETRIC_PROPERTY: &str = "http://www.w3.org/2002/07/owl#SymmetricProperty";
const OWL_INVERSE_OF: &str = "http://www.w3.org/2002/07/owl#inverseOf";
const OWL_DISJOINT_WITH: &str = "http://www.w3.org/2002/07/owl#disjointWith";

// ── OWL 2 axioms added in EG-021 (broader EL⁺/RL coverage toward DL-lite) ─────
/// `owl:equivalentProperty` — `r ≡ s` ⇒ `r ⊑ s` AND `s ⊑ r` (both role inclusions).
const OWL_EQUIVALENT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#equivalentProperty";
/// `owl:allValuesFrom` — universal restriction `∀r.C`. Handled by the RL `cls-avf`
/// propagation rule over the completion's R relation (sound, tractable; NOT full DL).
const OWL_ALL_VALUES_FROM: &str = "http://www.w3.org/2002/07/owl#allValuesFrom";
/// `owl:hasValue` — value restriction `∃r.{a}`. Modelled as an existential to the
/// VALUE token (a nominal treated as a Named filler) so it composes through CR-some.
const OWL_HAS_VALUE: &str = "http://www.w3.org/2002/07/owl#hasValue";
/// `owl:unionOf` — only the SOUND EL direction `Cᵢ ⊑ (C₁ ⊔ … ⊔ Cₙ)` (each disjunct is
/// subsumed by the union) is materialised; the case-split direction `A ⊑ C₁ ⊔ C₂` needs
/// a DL tableau and is DEFERRED.
const OWL_UNION_OF: &str = "http://www.w3.org/2002/07/owl#unionOf";
/// `owl:FunctionalProperty` — `r(x,y) ∧ r(x,z) → y owl:sameAs z` (equality / merge).
/// Instance-level; consumed by the [`crate::rules`] Datalog/equality engine.
const OWL_FUNCTIONAL_PROPERTY: &str = "http://www.w3.org/2002/07/owl#FunctionalProperty";
/// `owl:InverseFunctionalProperty` — `r(y,x) ∧ r(z,x) → y owl:sameAs z` (a key).
const OWL_INVERSE_FUNCTIONAL_PROPERTY: &str =
    "http://www.w3.org/2002/07/owl#InverseFunctionalProperty";
/// `owl:sameAs` — individual equality (instance-level; [`crate::rules`] equality closure).
const OWL_SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";
/// `owl:differentFrom` — individual inequality; a derived `sameAs` over a `differentFrom`
/// pair is a clash (instance-level inconsistency, reported by [`crate::rules`]).
const OWL_DIFFERENT_FROM: &str = "http://www.w3.org/2002/07/owl#differentFrom";

/// Annotation property carrying an axiom's confidence in `[0, 1]` (CONCEPT:KG-2.236):
/// `ex:Heart eg:confidence "0.8"` attaches `0.8` to the axiom(s) whose SUBJECT is
/// `ex:Heart` (e.g. `Heart ⊑ …`). Absent ⇒ `1.0` (a hard axiom). This is the epistemic
/// engine's native "uncertain axiom" hook — the EL/RL closure then PROPAGATES it.
const EG_CONFIDENCE: &str = "http://epistemic-graph/owl#confidence";

/// `<iri>` canonical form (matches the node-id convention of [`crate::mapping`]).
fn iri(s: &str) -> String {
    format!("<{s}>")
}

/// Confidence-fixpoint convergence epsilon (CONCEPT:KG-2.236): a derivation only
/// re-triggers the closure when it raises a recorded confidence by more than this, so
/// the max-confidence-per-pair fixpoint terminates (monotone-bounded above by 1.0).
const CONF_EPS: f64 = 1e-9;

// ── Class expressions in the EL fragment ─────────────────────────────────────

/// An EL concept expression. The TBox is normalised so that every LHS/RHS of a GCI
/// is one of these. Anonymous expressions (intersections, existentials) are interned
/// to stable string keys so the completion can index by them.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Concept {
    /// A named class IRI (canonical `<iri>` form), or `owl:Thing` / `owl:Nothing`.
    Named(String),
    /// `∃ r . C` — an existential restriction.
    Some(String, Box<Concept>),
}

impl Concept {
    /// A stable key for indexing (also the form a justification cites).
    pub fn key(&self) -> String {
        match self {
            Concept::Named(n) => n.clone(),
            Concept::Some(r, c) => format!("(∃ {r} . {})", c.key()),
        }
    }
    pub fn thing() -> Concept {
        Concept::Named(iri(OWL_THING))
    }
    pub fn nothing() -> Concept {
        Concept::Named(iri(OWL_NOTHING))
    }
}

// ── The parsed TBox (normalised general concept inclusions) ──────────────────

/// A normalised general concept inclusion `lhs ⊑ rhs`, where `lhs` is a conjunction
/// of EL concepts and `rhs` is a single EL concept. (EL normal form: every axiom is
/// `A ⊓ B ⊑ C`, `A ⊑ ∃r.B`, or `∃r.A ⊑ B`.)
#[derive(Clone, Debug)]
pub struct Gci {
    /// Conjuncts on the LHS (their conjunction is the subclass).
    pub lhs: Vec<Concept>,
    pub rhs: Concept,
    /// A human-readable axiom label for justifications.
    pub label: String,
    /// Axiom confidence in `[0, 1]` (CONCEPT:KG-2.236) — `1.0` for a hard/asserted
    /// axiom. A derived subsumption's confidence MULTIPLIES this in (the conjunctive
    /// rule: a chain is only as confident as its weakest axiom × its premises).
    pub conf: f64,
}

/// A role chain `r1 ∘ r2 ∘ … ⊑ s` (covers `TransitiveProperty` as `r ∘ r ⊑ r`).
#[derive(Clone, Debug)]
pub struct RoleChain {
    pub chain: Vec<String>,
    pub sup: String,
    pub label: String,
    /// Axiom confidence in `[0, 1]` (CONCEPT:KG-2.236).
    pub conf: f64,
}

/// The parsed OWL TBox: the EL concept axioms + the RL property axioms.
#[derive(Clone, Debug, Default)]
pub struct Ontology {
    /// EL general concept inclusions (normalised).
    pub gcis: Vec<Gci>,
    /// `r ⊑ s` role inclusions (incl. those produced by `equivalentProperty`):
    /// `(sub_role, sup_role, label, conf)`. `conf` is the axiom confidence in `[0,1]`.
    pub sub_roles: Vec<(String, String, String, f64)>,
    /// Role chains `r1∘…∘rn ⊑ s`.
    pub chains: Vec<RoleChain>,
    /// `owl:inverseOf` pairs (RL).
    pub inverses: Vec<(String, String)>,
    /// Symmetric roles (RL).
    pub symmetric: BTreeSet<String>,
    /// Domain rules `(role, class)` — RL, lift to EL via `∃role.⊤ ⊑ class`.
    pub domains: Vec<(String, String)>,
    /// Range rules `(role, class)` — RL.
    pub ranges: Vec<(String, String)>,
    /// `A ⊓ B ⊑ ⊥` disjointness (EL — derives ⊥ on a shared instance/subclass).
    pub disjoint: Vec<(String, String, String)>,
    /// `owl:allValuesFrom` universal restrictions (EG-021): `(sub_class, role, filler,
    /// label, conf)` meaning `sub_class ⊑ ∀role.filler`. Applied by the RL `cls-avf`
    /// completion rule (CR-allValues): a role witness of a `sub_class` is forced into
    /// `filler`. Sound + tractable; the only universal-restriction shape we admit.
    pub all_values: Vec<(String, String, String, String, f64)>,
    /// `owl:FunctionalProperty` roles (EG-021) — instance equality generators.
    pub functional: BTreeSet<String>,
    /// `owl:InverseFunctionalProperty` roles (EG-021).
    pub inverse_functional: BTreeSet<String>,
    /// Asserted `owl:sameAs` individual pairs (EG-021).
    pub same_as: Vec<(String, String)>,
    /// Asserted `owl:differentFrom` individual pairs (EG-021).
    pub different_from: Vec<(String, String)>,
    /// Every named class IRI mentioned (so classification can iterate the signature).
    pub classes: BTreeSet<String>,
}

// ── Parsing OWL axioms out of an RDF triple stream ───────────────────────────

/// Index the triples for axiom extraction: `(s, p) -> [o]` and `s -> [(p,o)]`.
struct TripleIndex {
    spo: HashMap<(String, String), Vec<Term>>,
}

impl TripleIndex {
    fn build(triples: &[Triple]) -> Self {
        let mut spo: HashMap<(String, String), Vec<Term>> = HashMap::new();
        for t in triples {
            let s = term_key(&t.subject.clone().into());
            let p = t.predicate.as_str().to_string();
            spo.entry((s, p)).or_default().push(t.object.clone());
        }
        Self { spo }
    }
    fn objects(&self, s: &str, p: &str) -> &[Term] {
        self.spo
            .get(&(s.to_string(), p.to_string()))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
    fn first_object(&self, s: &str, p: &str) -> Option<&Term> {
        self.objects(s, p).first()
    }
}

/// Canonical key for a subject/object term (IRI or blank node) — matches the
/// node-id convention so the EL closure speaks the SAME ids the property-graph uses.
fn term_key(t: &Term) -> String {
    match t {
        Term::NamedNode(n) => format!("<{}>", n.as_str()),
        Term::BlankNode(b) => format!("_:{}", b.as_str()),
        Term::Literal(l) => l.value().to_string(),
        #[allow(unreachable_patterns)]
        _ => String::new(),
    }
}

/// Parse an OWL ontology (the EL + RL axioms we support) from a triple stream.
///
/// Recognised: `rdfs:subClassOf` (incl. an `owl:Restriction`/`someValuesFrom`,
/// `owl:hasValue`, or `owl:allValuesFrom`, or an `owl:intersectionOf`/`owl:unionOf` on
/// either side), `owl:equivalentClass`, `rdfs:subPropertyOf`, `owl:equivalentProperty`,
/// `owl:propertyChainAxiom`, `owl:TransitiveProperty`, `owl:SymmetricProperty`,
/// `owl:inverseOf`, `owl:FunctionalProperty`, `owl:InverseFunctionalProperty`,
/// `rdfs:domain`/`rdfs:range`, `owl:disjointWith`, `owl:sameAs`, `owl:differentFrom`.
///
/// ## OWL-2 coverage (EG-021) and the DEFERRED DL constructs
///
/// The goal is **DL-lite / EL++**, NOT full OWL-2 DL. What IS covered (soundly, in the
/// monotone EL⁺/RL completion + the instance-level [`crate::rules`] engine):
/// `equivalentClass`/`equivalentProperty` (both directions), `someValuesFrom` &
/// `hasValue` existential/value restrictions, `allValuesFrom` via the RL `cls-avf`
/// rule, `intersectionOf` (LHS conjunction), `unionOf` (the sound `Cᵢ ⊑ union` /
/// `union ⊑ D` direction only), property chains + `TransitiveProperty`,
/// `Symmetric`/`inverseOf`, `Functional`/`InverseFunctionalProperty` (→ `owl:sameAs`
/// merges), `sameAs`/`differentFrom` equality with clash detection, and `disjointWith`.
///
/// **DEFERRED** (need a full DL TABLEAU — out of the tractable envelope, intentionally
/// NOT implemented): general negation / `complementOf`, cardinality restrictions beyond
/// the functional case (`min`/`max`/`exactCardinality`, `owl:qualifiedCardinality`),
/// reasoning-by-cases over a `unionOf` SUPERCLASS (`A ⊑ C₁ ⊔ C₂`), `oneOf` enumerated
/// classes as full nominals, and `hasKey` beyond inverse-functional. Anything else is
/// ignored — the engine stays sound by construction.
pub fn parse_ontology(triples: &[Triple]) -> Ontology {
    let idx = TripleIndex::build(triples);
    let mut ont = Ontology::default();

    // Pre-index per-subject axiom confidences (CONCEPT:KG-2.236): `S eg:confidence "c"`.
    let mut conf_of: HashMap<String, f64> = HashMap::new();
    for t in triples {
        if t.predicate.as_str() == EG_CONFIDENCE {
            if let Term::Literal(l) = &t.object {
                if let Ok(c) = l.value().parse::<f64>() {
                    let s = term_key(&t.subject.clone().into());
                    conf_of.insert(s, c.clamp(0.0, 1.0));
                }
            }
        }
    }
    let conf_for = |s: &str| -> f64 { conf_of.get(s).copied().unwrap_or(1.0) };

    for t in triples {
        let s = term_key(&t.subject.clone().into());
        let p = t.predicate.as_str();
        let o = &t.object;
        match p {
            RDFS_SUBCLASS_OF => {
                let ok = term_key(o);
                let c = conf_for(&s);
                // C ⊑ ∀r.D (owl:allValuesFrom superclass) — RL cls-avf restriction.
                if let Some((role, filler)) = parse_all_values(&idx, &ok) {
                    if let Some(lhs) = parse_class_expr(&idx, &s) {
                        for sub in lhs {
                            if let Concept::Named(sub) = sub {
                                ont.all_values.push((
                                    sub.clone(),
                                    role.clone(),
                                    filler.clone(),
                                    format!(
                                        "{} ⊑ ∀{}.{}",
                                        short(&sub),
                                        short(&role),
                                        short(&filler)
                                    ),
                                    c,
                                ));
                                register_class(&mut ont, &sub);
                                register_class(&mut ont, &filler);
                            }
                        }
                    }
                }
                // (C₁ ⊔ … ⊔ Cₙ) ⊑ D (owl:unionOf subclass) ≡ each Cᵢ ⊑ D — sound EL.
                else if let Some(disjuncts) = parse_union(&idx, &s) {
                    if let Some(rhs) = parse_class_expr(&idx, &ok) {
                        for d in disjuncts {
                            push_subclass(
                                &mut ont,
                                vec![d.clone()],
                                rhs.clone(),
                                format!("{} ⊑ {} (∪-elim)", d.key(), short(&ok)),
                                c,
                            );
                        }
                    }
                } else if let (Some(lhs), Some(rhs)) =
                    (parse_class_expr(&idx, &s), parse_class_expr(&idx, &ok))
                {
                    push_subclass(
                        &mut ont,
                        lhs,
                        rhs,
                        format!("{} ⊑ {}", short(&s), short(&ok)),
                        c,
                    );
                }
            }
            OWL_EQUIVALENT_CLASS => {
                let sk = term_key(o);
                let c = conf_for(&s);
                // A ≡ (C₁ ⊔ …): only the sound `Cᵢ ⊑ A` direction (union ⊑ A); the
                // `A ⊑ union` case-split is DL-tableau territory and is deferred.
                let mut handled_union = false;
                for (named, union_node) in [(&s, &sk), (&sk, &s)] {
                    if let (Some(Concept::Named(a)), Some(disjuncts)) = (
                        parse_class_expr(&idx, named)
                            .and_then(|mut v| (v.len() == 1).then(|| v.pop().unwrap())),
                        parse_union(&idx, union_node),
                    ) {
                        for d in disjuncts {
                            push_subclass(
                                &mut ont,
                                vec![d.clone()],
                                vec![Concept::Named(a.clone())],
                                format!("{} ⊑ {} (≡∪-elim)", d.key(), short(&a)),
                                c,
                            );
                        }
                        handled_union = true;
                    }
                }
                if !handled_union {
                    if let (Some(a), Some(b)) =
                        (parse_class_expr(&idx, &s), parse_class_expr(&idx, &sk))
                    {
                        // A ≡ B  ⇒  A ⊑ B  AND  B ⊑ A.
                        push_subclass(
                            &mut ont,
                            a.clone(),
                            b.clone(),
                            format!("{} ≡ {} (→⊑)", short(&s), short(&sk)),
                            c,
                        );
                        push_subclass(
                            &mut ont,
                            b,
                            a,
                            format!("{} ≡ {} (←⊑)", short(&s), short(&sk)),
                            c,
                        );
                    }
                }
            }
            OWL_EQUIVALENT_PROPERTY => {
                if let Term::NamedNode(sup) = o {
                    let sup = iri(sup.as_str());
                    let c = conf_for(&s);
                    // r ≡ s ⇒ r ⊑ s AND s ⊑ r.
                    ont.sub_roles.push((
                        s.clone(),
                        sup.clone(),
                        format!("{} ≡ {} (→⊑)", short(&s), short(&sup)),
                        c,
                    ));
                    ont.sub_roles.push((
                        sup.clone(),
                        s.clone(),
                        format!("{} ≡ {} (←⊑)", short(&s), short(&sup)),
                        c,
                    ));
                }
            }
            OWL_SAME_AS => {
                if let Term::NamedNode(b) = o {
                    ont.same_as.push((s.clone(), iri(b.as_str())));
                }
            }
            OWL_DIFFERENT_FROM => {
                if let Term::NamedNode(b) = o {
                    ont.different_from.push((s.clone(), iri(b.as_str())));
                }
            }
            RDFS_SUBPROPERTY_OF => {
                if let Term::NamedNode(sup) = o {
                    let sup = iri(sup.as_str());
                    ont.sub_roles.push((
                        s.clone(),
                        sup.clone(),
                        format!("{} ⊑ {}", short(&s), short(&sup)),
                        conf_for(&s),
                    ));
                }
            }
            OWL_PROPERTY_CHAIN_AXIOM => {
                let chain = parse_rdf_list(&idx, o)
                    .into_iter()
                    .map(|t| term_key(&t))
                    .collect::<Vec<_>>();
                if !chain.is_empty() {
                    let label = format!(
                        "{} ⊑ {}",
                        chain
                            .iter()
                            .map(|c| short(c))
                            .collect::<Vec<_>>()
                            .join(" ∘ "),
                        short(&s)
                    );
                    ont.chains.push(RoleChain {
                        chain,
                        sup: s.clone(),
                        label,
                        conf: conf_for(&s),
                    });
                }
            }
            OWL_INVERSE_OF => {
                if let Term::NamedNode(inv) = o {
                    ont.inverses.push((s.clone(), iri(inv.as_str())));
                }
            }
            RDFS_DOMAIN => {
                if let Term::NamedNode(d) = o {
                    ont.domains.push((s.clone(), iri(d.as_str())));
                }
            }
            RDFS_RANGE => {
                if let Term::NamedNode(r) = o {
                    ont.ranges.push((s.clone(), iri(r.as_str())));
                }
            }
            OWL_DISJOINT_WITH => {
                if let Term::NamedNode(b) = o {
                    let b = iri(b.as_str());
                    ont.disjoint.push((
                        s.clone(),
                        b.clone(),
                        format!("{} ⊓ {} ⊑ ⊥", short(&s), short(&b)),
                    ));
                    register_class(&mut ont, &s);
                    register_class(&mut ont, &b);
                }
            }
            RDF_TYPE => {
                if let Term::NamedNode(ty) = o {
                    match ty.as_str() {
                        OWL_TRANSITIVE_PROPERTY => {
                            // r∘r ⊑ r (the EL role-chain form of transitivity).
                            ont.chains.push(RoleChain {
                                chain: vec![s.clone(), s.clone()],
                                sup: s.clone(),
                                label: format!("{0} ∘ {0} ⊑ {0} (transitive)", short(&s)),
                                conf: conf_for(&s),
                            });
                        }
                        OWL_SYMMETRIC_PROPERTY => {
                            ont.symmetric.insert(s.clone());
                        }
                        OWL_FUNCTIONAL_PROPERTY => {
                            ont.functional.insert(s.clone());
                        }
                        OWL_INVERSE_FUNCTIONAL_PROPERTY => {
                            ont.inverse_functional.insert(s.clone());
                        }
                        OWL_CLASS => register_class(&mut ont, &s),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    // Lift domain rules into EL so they classify through existentials too:
    // domain(r, D)  ≈  ∃r.⊤ ⊑ D.
    for (r, d) in ont.domains.clone() {
        push_gci(
            &mut ont,
            vec![Concept::Some(r.clone(), Box::new(Concept::thing()))],
            Concept::Named(d.clone()),
            format!("dom({}) = {}", short(&r), short(&d)),
            1.0,
        );
    }

    ont
}

/// Parse a class expression rooted at node `id`: a named class, an
/// `owl:Restriction`/`someValuesFrom` existential, or an `owl:intersectionOf` (whose
/// conjuncts each recurse). `None` if `id` is not a recognised class expression.
fn parse_class_expr(idx: &TripleIndex, id: &str) -> Option<Vec<Concept>> {
    // Intersection: a conjunction is its list of conjuncts.
    if let Some(list_head) = idx.first_object(id, OWL_INTERSECTION_OF) {
        let mut conjs = Vec::new();
        for item in parse_rdf_list(idx, list_head) {
            conjs.extend(parse_class_expr(idx, &term_key(&item))?);
        }
        if !conjs.is_empty() {
            return Some(conjs);
        }
    }
    // Restriction ∃r.C.
    if let (Some(on_prop), Some(filler)) = (
        idx.first_object(id, OWL_ON_PROPERTY),
        idx.first_object(id, OWL_SOME_VALUES_FROM),
    ) {
        let role = term_key(on_prop);
        let filler_concept = parse_class_expr(idx, &term_key(filler))?;
        // someValuesFrom takes a single class expression.
        let f = conjunction_to_concept(filler_concept);
        return Some(vec![Concept::Some(role, Box::new(f))]);
    }
    // Value restriction ∃r.{a} (owl:hasValue) — the value `a` is a NOMINAL, modelled as
    // a Named filler token so `C ⊑ ∃r.{a}` and `∃r.{a} ⊑ D` compose to `C ⊑ D` through
    // the existing CR-some rules (a sound class-level approximation of the nominal).
    if let (Some(on_prop), Some(val)) = (
        idx.first_object(id, OWL_ON_PROPERTY),
        idx.first_object(id, OWL_HAS_VALUE),
    ) {
        let role = term_key(on_prop);
        let value_token = format!("{{{}}}", term_key(val)); // {<...>} nominal marker
        return Some(vec![Concept::Some(
            role,
            Box::new(Concept::Named(value_token)),
        )]);
    }
    // A named class / Thing / Nothing.
    if id.starts_with('<') || id == iri(OWL_THING) || id == iri(OWL_NOTHING) {
        return Some(vec![Concept::Named(id.to_string())]);
    }
    // A bare blank node with no restriction/intersection structure — unsupported.
    None
}

/// Parse an `owl:allValuesFrom` restriction rooted at `id`: `∀role.filler`. Returns
/// `(role, filler)` (both canonical ids) when `id` is `[onProperty role; allValuesFrom
/// C]` with a NAMED filler `C`; `None` otherwise (a non-named filler is not RL-tractable
/// here and is skipped). Used to record an `all_values` (cls-avf) axiom (EG-021).
fn parse_all_values(idx: &TripleIndex, id: &str) -> Option<(String, String)> {
    let on_prop = idx.first_object(id, OWL_ON_PROPERTY)?;
    let filler = idx.first_object(id, OWL_ALL_VALUES_FROM)?;
    let role = term_key(on_prop);
    let filler = term_key(filler);
    // Only a named class filler is admitted (the RL cls-avf consequent is a class).
    filler.starts_with('<').then_some((role, filler))
}

/// Parse an `owl:unionOf` expression rooted at `id` into its disjuncts (EG-021). Only
/// the SOUND direction is consumed by callers (each disjunct ⊑ the union); the union as
/// a SUPERCLASS (`A ⊑ C₁ ⊔ C₂`, reasoning-by-cases) is DEFERRED. `None` when `id` is not
/// a union node.
fn parse_union(idx: &TripleIndex, id: &str) -> Option<Vec<Concept>> {
    let head = idx.first_object(id, OWL_UNION_OF)?;
    let mut disjuncts = Vec::new();
    for item in parse_rdf_list(idx, head) {
        disjuncts.extend(parse_class_expr(idx, &term_key(&item))?);
    }
    (!disjuncts.is_empty()).then_some(disjuncts)
}

/// Fold a conjunction list to a single concept: a single conjunct passes through; a
/// multi-conjunct filler is approximated by its first conjunct (EL fillers are class
/// expressions; full conjunctive fillers are normalised away upstream). For our
/// supported shapes a someValuesFrom filler is a single named/existential concept.
fn conjunction_to_concept(mut cs: Vec<Concept>) -> Concept {
    if cs.len() == 1 {
        cs.pop().unwrap()
    } else if cs.is_empty() {
        Concept::thing()
    } else {
        // Multiple conjuncts in a filler are rare in EL normal form; keep the first.
        cs.into_iter().next().unwrap()
    }
}

/// Walk an `rdf:first`/`rdf:rest`/`rdf:nil` collection into a vector of object terms.
fn parse_rdf_list(idx: &TripleIndex, head: &Term) -> Vec<Term> {
    let mut out = Vec::new();
    let mut cur = term_key(head);
    let mut guard = 0;
    while cur != iri(RDF_NIL) && guard < 10_000 {
        guard += 1;
        let Some(first) = idx.first_object(&cur, RDF_FIRST) else {
            break;
        };
        out.push(first.clone());
        match idx.first_object(&cur, RDF_REST) {
            Some(rest) => cur = term_key(rest),
            None => break,
        }
    }
    out
}

fn push_gci(ont: &mut Ontology, lhs: Vec<Concept>, rhs: Concept, label: String, conf: f64) {
    for c in lhs.iter().chain(std::iter::once(&rhs)) {
        register_concept_classes(ont, c);
    }
    ont.gcis.push(Gci {
        lhs,
        rhs,
        label,
        conf,
    });
}

/// Push a subclass axiom whose RHS may be a conjunction: `A ⊑ B ⊓ C` is EL-equivalent
/// to `A ⊑ B` AND `A ⊑ C`, so a multi-conjunct RHS splits into one GCI per conjunct
/// (keeping every axiom in EL normal form: a single concept on the RHS).
fn push_subclass(
    ont: &mut Ontology,
    lhs: Vec<Concept>,
    rhs: Vec<Concept>,
    label: String,
    conf: f64,
) {
    for r in rhs {
        push_gci(ont, lhs.clone(), r, label.clone(), conf);
    }
}

fn register_concept_classes(ont: &mut Ontology, c: &Concept) {
    match c {
        Concept::Named(n) => register_class(ont, n),
        Concept::Some(_, f) => register_concept_classes(ont, f),
    }
}

fn register_class(ont: &mut Ontology, c: &str) {
    // Named IRIs and hasValue NOMINAL tokens (`{<value>}`) both seed into the signature
    // so the completion's S/R relations index them (a nominal filler must subsume
    // itself for CR-some⁻ to compose `∃r.{a}` axioms — EG-021).
    if c.starts_with('<') || c.starts_with('{') {
        ont.classes.insert(c.to_string());
    }
}

/// A short, human-readable rendering of an IRI node-id (local name) for labels.
fn short(id: &str) -> String {
    let inner = id
        .strip_prefix('<')
        .and_then(|s| s.strip_suffix('>'))
        .unwrap_or(id);
    let local = inner
        .rsplit(['#', '/'])
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or(inner);
    local.to_string()
}

// ── EL⁺ completion (classification) ──────────────────────────────────────────

/// A justification for one inferred subsumption: the rule and the axiom/premises that
/// produced it. `axioms` are the human-readable axiom labels; `premises` are prior
/// subsumptions `(sub, sup)` the rule consumed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Justification {
    pub rule: &'static str,
    pub axioms: Vec<String>,
    pub premises: Vec<(String, String)>,
}

/// The materialised classification: for each named class `A`, its set of subsumers
/// `S(A)`; plus the role relation `R(r)` (pairs `(A,B)` with `A ⊑ ∃r.B`), the
/// derived-subsumption justifications, and whether the ontology is consistent.
#[derive(Clone, Debug)]
pub struct Classification {
    /// `subsumers[A] = { B : A ⊑ B }` (reflexive — `A ∈ S(A)` — and includes ⊤).
    pub subsumers: BTreeMap<String, BTreeSet<String>>,
    /// `roles[(r)] = { (A, B) : A ⊑ ∃r.B }` — the EL completion's R relation.
    pub roles: BTreeMap<String, BTreeSet<(String, String)>>,
    /// Justification for each DERIVED (non-reflexive, non-asserted) subsumption.
    pub justifications: HashMap<(String, String), Justification>,
    /// Confidence in `[0, 1]` for each subsumption `(sub, sup)` (CONCEPT:KG-2.236).
    /// A reflexive/seed subsumption is `1.0`; a DERIVED one is the MAX over its
    /// alternative derivations of `axiom_conf × ∏ premise_conf` — high-confidence
    /// chains stay high, a single weak axiom drags its consequence down. Empty when
    /// the classification was run without confidence (`classify`); populated by
    /// [`Reasoner::classify_weighted`].
    pub confidence: BTreeMap<(String, String), f64>,
    /// `false` when some satisfiable class was forced to subsume `owl:Nothing`
    /// (a ⊥-derivation) — i.e. the ontology is inconsistent / has an unsatisfiable
    /// class. [`unsatisfiable`] lists the offending classes.
    pub consistent: bool,
    /// Named classes derived to be unsatisfiable (`A ⊑ ⊥`).
    pub unsatisfiable: BTreeSet<String>,
}

impl Classification {
    /// Does `sub ⊑ sup` hold under the classification (named-class subsumption)?
    pub fn entails_subclass(&self, sub: &str, sup: &str) -> bool {
        self.subsumers
            .get(sub)
            .map(|s| s.contains(sup))
            .unwrap_or(false)
    }

    /// The confidence in `[0, 1]` that `sub ⊑ sup` (CONCEPT:KG-2.236). `1.0` for a
    /// reflexive/asserted-hard subsumption; the MAX-over-derivations propagated value
    /// for a derived one. Returns `0.0` when `sub ⊑ sup` does not hold. A confidence
    /// is only recorded when the classification was run with
    /// [`Reasoner::classify_weighted`]; a plain [`Reasoner::classify`] leaves the map
    /// empty, so this returns `1.0` for any holding subsumption (every axiom hard).
    pub fn subclass_confidence(&self, sub: &str, sup: &str) -> f64 {
        if !self.entails_subclass(sub, sup) {
            return 0.0;
        }
        self.confidence
            .get(&(sub.to_string(), sup.to_string()))
            .copied()
            .unwrap_or(1.0)
    }
}

/// A subsumption-RHS axiom indexed by its trigger conjunct: `(consequent, label, conf)`
/// — used for the CR-sub (`B ⊑ C`) and CR-some⁻ (`∃r.D ⊑ E`) rules.
type RhsAxiom = (Concept, String, f64);
/// An existential-RHS axiom `B ⊑ ∃r.D` indexed by `B`: `(role, filler, label, conf)`.
type SomeRhsAxiom = (String, Concept, String, f64);

/// A reusable completion state so a DELTA can re-run incrementally (CONCEPT:KG-2.219).
/// Holds the normalised axioms + the current `S`/`R` closure; [`Reasoner::classify`]
/// runs the fixpoint, [`Reasoner::add_axioms`] adds axioms and re-runs only the new
/// consequences (EL completion is monotone — adding axioms only ADDS subsumers).
#[derive(Clone, Debug, Default)]
pub struct Reasoner {
    ont: Ontology,
    /// `S(A)` — subsumers per class (the completion's mutable state).
    s: BTreeMap<String, BTreeSet<String>>,
    /// `R(r)` — (A,B) with A ⊑ ∃r.B.
    r: BTreeMap<String, BTreeSet<(String, String)>>,
    just: HashMap<(String, String), Justification>,
    /// Confidence per subsumption `(A,B)` — MAX over derivations of
    /// `axiom_conf × ∏ premise_conf` (CONCEPT:KG-2.236). Only maintained when
    /// `weighted` is set; an unweighted run leaves it empty.
    conf: BTreeMap<(String, String), f64>,
    /// Confidence per role pair `(r, (A,B))` — analogous to `conf` for the R relation,
    /// so an existential derivation can multiply in the confidence of the role edge.
    rconf: BTreeMap<(String, String, String), f64>,
    /// When true, the saturation tracks + propagates confidences (CONCEPT:KG-2.236).
    weighted: bool,
}

impl Reasoner {
    /// Build a reasoner from a parsed ontology.
    pub fn new(ont: Ontology) -> Self {
        Self {
            ont,
            ..Default::default()
        }
    }

    /// Convenience: parse + build from a triple stream.
    pub fn from_triples(triples: &[Triple]) -> Self {
        Self::new(parse_ontology(triples))
    }

    /// The signature: every named class plus ⊤ and ⊥.
    fn signature(&self) -> BTreeSet<String> {
        let mut sig = self.ont.classes.clone();
        sig.insert(iri(OWL_THING));
        sig.insert(iri(OWL_NOTHING));
        // Fillers / existential targets that appear only inside restrictions.
        for g in &self.ont.gcis {
            for c in g.lhs.iter().chain(std::iter::once(&g.rhs)) {
                collect_named(c, &mut sig);
            }
        }
        sig
    }

    /// Seed S(A) = {A, ⊤} for every class in the signature.
    fn seed(&mut self) {
        let thing = iri(OWL_THING);
        for a in self.signature() {
            let entry = self.s.entry(a.clone()).or_default();
            entry.insert(a.clone());
            entry.insert(thing.clone());
            if self.weighted {
                // Reflexive + ⊤ subsumptions are certain.
                self.conf.insert((a.clone(), a.clone()), 1.0);
                self.conf.insert((a.clone(), thing.clone()), 1.0);
            }
        }
    }

    /// Run EL⁺ completion to fixpoint and return the classification. Idempotent —
    /// re-running over the same axioms yields the same closure.
    pub fn classify(&mut self) -> Classification {
        self.weighted = false;
        self.seed();
        self.saturate();
        self.snapshot()
    }

    /// Run EL⁺ completion AND propagate per-subsumption confidence (CONCEPT:KG-2.236).
    /// Same closure as [`classify`] (membership is identical — confidence weighting
    /// never changes WHICH subsumptions hold, only their `[0,1]` confidence), with the
    /// `confidence` map populated: a derived `A ⊑ B` carries the MAX over its
    /// alternative derivations of `axiom_conf × ∏ premise_conf`. A hard ontology (all
    /// axioms `conf = 1.0`) yields confidence `1.0` everywhere, so the weighted run is
    /// a strict superset of the plain one.
    pub fn classify_weighted(&mut self) -> Classification {
        self.weighted = true;
        self.seed();
        self.saturate();
        self.snapshot()
    }

    /// Add axioms (a delta) and re-saturate INCREMENTALLY: the prior `S`/`R` closure
    /// is kept (EL completion is monotone, so nothing is retracted) and the fixpoint
    /// resumes from it — only the NEW consequences are derived, not the whole closure
    /// from scratch (CONCEPT:KG-2.219 incremental materialization). Returns the new
    /// classification.
    pub fn add_axioms(&mut self, delta: Ontology) -> Classification {
        merge_ontology(&mut self.ont, delta);
        // Seed any newly-introduced classes; keep existing S/R.
        self.seed();
        self.saturate();
        self.snapshot()
    }

    /// The completion loop. Applies the EL⁺ rules until no `S`/`R` membership changes:
    ///
    /// * **CR-sub** — if `B ∈ S(A)` and `B ⊑ C` is an axiom (single-conjunct LHS), add
    ///   `C` to `S(A)`.
    /// * **CR-conj⁻** — if `{B1,…,Bn} ∈ S(A)` (a conjunctive LHS all subsume A) and
    ///   `B1 ⊓ … ⊓ Bn ⊑ C`, add `C` to `S(A)`.
    /// * **CR-some⁺** — if `B ∈ S(A)` and `B ⊑ ∃r.D` is an axiom, add `(A,D)` to `R(r)`.
    /// * **CR-some⁻** — if `(A,B) ∈ R(r)` and `D ∈ S(B)` and `∃r.D ⊑ E` is an axiom, add
    ///   `E` to `S(A)`.
    /// * **CR-chain** — if `(A,B) ∈ R(r1)` and `(B,C) ∈ R(r2)` and `r1∘r2 ⊑ s`, add
    ///   `(A,C)` to `R(s)` (covers transitivity via `r∘r ⊑ r`).
    /// * **CR-subrole** — if `(A,B) ∈ R(r)` and `r ⊑ s`, add `(A,B)` to `R(s)`.
    /// * **CR-bot** — if `⊥ ∈ S(B)` and `(A,B) ∈ R(r)`, add `⊥` to `S(A)` (the empty
    ///   filler propagates unsatisfiability up an existential).
    /// * **CR-disjoint** — if `D1, D2 ∈ S(A)` and `D1 ⊓ D2 ⊑ ⊥`, add `⊥` to `S(A)`.
    fn saturate(&mut self) {
        // Pre-index axioms by their trigger for O(1) rule lookup. Each entry carries
        // the axiom's confidence as the last tuple element (CONCEPT:KG-2.236).
        // single-conjunct sub: B -> [(C, axiom_label, conf)]
        let mut sub_index: HashMap<String, Vec<RhsAxiom>> = HashMap::new();
        // conjunctive sub: list of (conjuncts, C, label, conf)
        let mut conj_axioms: Vec<(Vec<String>, Concept, String, f64)> = Vec::new();
        // existential RHS: B -> [(r, D, label, conf)]  (B ⊑ ∃r.D)
        let mut some_rhs: HashMap<String, Vec<SomeRhsAxiom>> = HashMap::new();
        // existential LHS: (r, D) -> [(E, label, conf)]  (∃r.D ⊑ E)
        let mut some_lhs: HashMap<(String, String), Vec<RhsAxiom>> = HashMap::new();

        for g in &self.ont.gcis {
            let rhs = g.rhs.clone();
            if g.lhs.len() == 1 {
                match &g.lhs[0] {
                    Concept::Named(b) => match &rhs {
                        Concept::Named(_) => {
                            sub_index.entry(b.clone()).or_default().push((
                                rhs,
                                g.label.clone(),
                                g.conf,
                            ));
                        }
                        Concept::Some(r, d) => {
                            some_rhs.entry(b.clone()).or_default().push((
                                r.clone(),
                                (**d).clone(),
                                g.label.clone(),
                                g.conf,
                            ));
                        }
                    },
                    Concept::Some(r, d) => {
                        // ∃r.D ⊑ E
                        some_lhs.entry((r.clone(), d.key())).or_default().push((
                            rhs,
                            g.label.clone(),
                            g.conf,
                        ));
                    }
                }
            } else {
                // Conjunctive LHS — all conjuncts must be named in EL normal form.
                let names: Vec<String> = g.lhs.iter().map(|c| c.key()).collect();
                conj_axioms.push((names, rhs, g.label.clone(), g.conf));
            }
        }

        // disjoint pairs by class for CR-disjoint.
        let disjoint: Vec<(String, String, String)> = self.ont.disjoint.clone();
        // allValuesFrom axioms (cls-avf): (sub_class, role, filler, label, conf).
        let all_values = self.ont.all_values.clone();
        let chains = self.ont.chains.clone();
        let sub_roles = self.ont.sub_roles.clone();
        let symmetric: Vec<String> = self.ont.symmetric.iter().cloned().collect();
        let inverses = self.ont.inverses.clone();
        let nothing = iri(OWL_NOTHING);

        let mut changed = true;
        let mut guard = 0;
        while changed && guard < 100_000 {
            guard += 1;
            changed = false;

            // Snapshot the class set we iterate (S keys can grow; iterate a copy).
            let classes: Vec<String> = self.s.keys().cloned().collect();

            for a in &classes {
                let s_a: Vec<String> = self
                    .s
                    .get(a)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .collect();

                // CR-sub + CR-some⁺
                for b in &s_a {
                    if let Some(rules) = sub_index.get(b) {
                        for (c, label, axiom_conf) in rules {
                            if let Concept::Named(cn) = c {
                                // conjunctive rule: conf(a⊑c) = conf(a⊑b) × conf(b⊑c).
                                let conf = self.cur_conf(a, b) * axiom_conf;
                                if self.add_sub(
                                    a,
                                    cn,
                                    "CR-sub",
                                    vec![label.clone()],
                                    vec![(a.clone(), b.clone())],
                                    conf,
                                ) {
                                    changed = true;
                                }
                            }
                        }
                    }
                    if let Some(rules) = some_rhs.get(b) {
                        for (r, d, label, axiom_conf) in rules {
                            if let Concept::Named(dn) = d {
                                let conf = self.cur_conf(a, b) * axiom_conf;
                                if self.add_role_weighted(r, a, dn, conf) {
                                    self.just
                                        .entry((format!("R:{r}"), format!("{a}->{dn}")))
                                        .or_insert(Justification {
                                            rule: "CR-some⁺",
                                            axioms: vec![label.clone()],
                                            premises: vec![(a.clone(), b.clone())],
                                        });
                                    changed = true;
                                }
                            }
                        }
                    }
                }

                // CR-conj⁻
                for (conjuncts, c, label, axiom_conf) in &conj_axioms {
                    if conjuncts
                        .iter()
                        .all(|b| self.s.get(a).map(|s| s.contains(b)).unwrap_or(false))
                    {
                        if let Concept::Named(cn) = c {
                            let premises: Vec<(String, String)> =
                                conjuncts.iter().map(|b| (a.clone(), b.clone())).collect();
                            // conjunctive rule: ∏ over every conjunct premise × axiom.
                            let conf = conjuncts
                                .iter()
                                .fold(*axiom_conf, |acc, b| acc * self.cur_conf(a, b));
                            if self.add_sub(a, cn, "CR-conj⁻", vec![label.clone()], premises, conf)
                            {
                                changed = true;
                            }
                        }
                    }
                }

                // CR-disjoint
                for (d1, d2, label) in &disjoint {
                    let has = self
                        .s
                        .get(a)
                        .map(|s| s.contains(d1) && s.contains(d2))
                        .unwrap_or(false);
                    if has {
                        let conf = self.cur_conf(a, d1) * self.cur_conf(a, d2);
                        if self.add_sub(
                            a,
                            &nothing,
                            "CR-disjoint",
                            vec![label.clone()],
                            vec![(a.clone(), d1.clone()), (a.clone(), d2.clone())],
                            conf,
                        ) {
                            changed = true;
                        }
                    }
                }
            }

            // CR-some⁻ + CR-bot over R.
            let roles_snapshot: Vec<(String, Vec<(String, String)>)> = self
                .r
                .iter()
                .map(|(r, set)| (r.clone(), set.iter().cloned().collect()))
                .collect();
            for (r, pairs) in &roles_snapshot {
                for (a, b) in pairs {
                    // CR-some⁻: D ∈ S(B), ∃r.D ⊑ E ⇒ E ∈ S(A).
                    let s_b: Vec<String> = self
                        .s
                        .get(b)
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .collect();
                    for d in &s_b {
                        if let Some(rules) = some_lhs.get(&(r.clone(), d.clone())) {
                            for (e, label, axiom_conf) in rules {
                                if let Concept::Named(en) = e {
                                    let conf =
                                        self.cur_rconf(r, a, b) * self.cur_conf(b, d) * axiom_conf;
                                    if self.add_sub(
                                        a,
                                        en,
                                        "CR-some⁻",
                                        vec![label.clone()],
                                        vec![(b.clone(), d.clone())],
                                        conf,
                                    ) {
                                        changed = true;
                                    }
                                }
                            }
                        }
                    }
                    // CR-bot: ⊥ ∈ S(B) ⇒ ⊥ ∈ S(A).
                    if self.s.get(b).map(|s| s.contains(&nothing)).unwrap_or(false) {
                        let conf = self.cur_rconf(r, a, b) * self.cur_conf(b, &nothing);
                        if self.add_sub(
                            a,
                            &nothing,
                            "CR-bot",
                            vec![],
                            vec![(b.clone(), nothing.clone())],
                            conf,
                        ) {
                            changed = true;
                        }
                    }
                }
            }

            // CR-subrole: (A,B) ∈ R(r), r ⊑ s ⇒ (A,B) ∈ R(s). And symmetric.
            for (r, sup, _label, axiom_conf) in &sub_roles {
                if let Some(pairs) = self.r.get(r).cloned() {
                    for (a, b) in pairs {
                        let conf = self.cur_rconf(r, &a, &b) * axiom_conf;
                        if self.add_role_weighted(sup, &a, &b, conf) {
                            changed = true;
                        }
                    }
                }
            }
            for r in &symmetric {
                if let Some(pairs) = self.r.get(r).cloned() {
                    for (a, b) in pairs {
                        let conf = self.cur_rconf(r, &a, &b);
                        if self.add_role_weighted(r, &b, &a, conf) {
                            changed = true;
                        }
                    }
                }
            }
            for (p1, p2) in &inverses {
                if let Some(pairs) = self.r.get(p1).cloned() {
                    for (a, b) in pairs {
                        let conf = self.cur_rconf(p1, &a, &b);
                        if self.add_role_weighted(p2, &b, &a, conf) {
                            changed = true;
                        }
                    }
                }
                if let Some(pairs) = self.r.get(p2).cloned() {
                    for (a, b) in pairs {
                        let conf = self.cur_rconf(p2, &a, &b);
                        if self.add_role_weighted(p1, &b, &a, conf) {
                            changed = true;
                        }
                    }
                }
            }

            // CR-allValues (RL cls-avf): sub ⊑ ∀r.filler, sub ∈ S(A), (A,B) ∈ R(r) ⇒
            // filler ∈ S(B). The universal restriction forces every r-witness of a
            // `sub` member into `filler`. Sound + tractable; the one ∀-shape we admit.
            for (sub, role, filler, label, axiom_conf) in &all_values {
                if let Some(pairs) = self.r.get(role).cloned() {
                    for (a, b) in &pairs {
                        if self.s.get(a).map(|s| s.contains(sub)).unwrap_or(false) {
                            let conf =
                                self.cur_conf(a, sub) * self.cur_rconf(role, a, b) * axiom_conf;
                            if self.add_sub(
                                b,
                                filler,
                                "CR-allValues",
                                vec![label.clone()],
                                vec![(a.clone(), sub.clone())],
                                conf,
                            ) {
                                changed = true;
                            }
                        }
                    }
                }
            }

            // CR-chain: (A,B) ∈ R(r1), (B,C) ∈ R(r2), r1∘r2 ⊑ s ⇒ (A,C) ∈ R(s).
            for ch in &chains {
                if ch.chain.len() == 2 {
                    let (r1, r2) = (&ch.chain[0], &ch.chain[1]);
                    let left = self.r.get(r1).cloned().unwrap_or_default();
                    let right = self.r.get(r2).cloned().unwrap_or_default();
                    // index right by its source
                    let mut by_src: HashMap<String, Vec<String>> = HashMap::new();
                    for (b, c) in &right {
                        by_src.entry(b.clone()).or_default().push(c.clone());
                    }
                    for (a, b) in &left {
                        if let Some(cs) = by_src.get(b) {
                            for c in cs {
                                let conf =
                                    self.cur_rconf(r1, a, b) * self.cur_rconf(r2, b, c) * ch.conf;
                                if self.add_role_weighted(&ch.sup, a, c, conf) {
                                    changed = true;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Current confidence of a subsumption `(a,b)` — `1.0` when unweighted or unseen
    /// (an unrecorded pair is treated as certain, e.g. the reflexive seed).
    fn cur_conf(&self, a: &str, b: &str) -> f64 {
        if !self.weighted {
            return 1.0;
        }
        self.conf
            .get(&(a.to_string(), b.to_string()))
            .copied()
            .unwrap_or(1.0)
    }

    /// Current confidence of a role pair `(r,(a,b))`.
    fn cur_rconf(&self, r: &str, a: &str, b: &str) -> f64 {
        if !self.weighted {
            return 1.0;
        }
        self.rconf
            .get(&(r.to_string(), a.to_string(), b.to_string()))
            .copied()
            .unwrap_or(1.0)
    }

    /// Add (or raise the confidence of) a subsumption. Returns `true` when membership
    /// was newly added OR (weighted) the derived `conf` raised the recorded confidence
    /// beyond `CONF_EPS` — either is a change that re-triggers the fixpoint, so the
    /// closure converges on BOTH the membership AND the max-confidence per pair.
    fn add_sub(
        &mut self,
        a: &str,
        b: &str,
        rule: &'static str,
        axioms: Vec<String>,
        premises: Vec<(String, String)>,
        conf: f64,
    ) -> bool {
        let added = self
            .s
            .entry(a.to_string())
            .or_default()
            .insert(b.to_string());
        if added && a != b {
            self.just
                .entry((a.to_string(), b.to_string()))
                .or_insert(Justification {
                    rule,
                    axioms,
                    premises,
                });
        }
        let mut changed = added;
        if self.weighted {
            let key = (a.to_string(), b.to_string());
            let prev = self.conf.get(&key).copied().unwrap_or(0.0);
            // noisy-OR / MAX across alternative derivations: keep the strongest.
            let combined = prev.max(conf.clamp(0.0, 1.0));
            if combined > prev + CONF_EPS {
                self.conf.insert(key, combined);
                changed = true;
            } else if added {
                self.conf.entry(key).or_insert(combined);
            }
        }
        changed
    }

    /// Add (or raise the confidence of) a role pair `(r,(a,b))`.
    fn add_role_weighted(&mut self, r: &str, a: &str, b: &str, conf: f64) -> bool {
        let added = self
            .r
            .entry(r.to_string())
            .or_default()
            .insert((a.to_string(), b.to_string()));
        let mut changed = added;
        if self.weighted {
            let key = (r.to_string(), a.to_string(), b.to_string());
            let prev = self.rconf.get(&key).copied().unwrap_or(0.0);
            let combined = prev.max(conf.clamp(0.0, 1.0));
            if combined > prev + CONF_EPS {
                self.rconf.insert(key, combined);
                changed = true;
            } else if added {
                self.rconf.entry(key).or_insert(combined);
            }
        }
        changed
    }

    /// Project the current closure into an immutable [`Classification`], computing
    /// consistency: the ontology is inconsistent iff some class OTHER than ⊥ itself
    /// is forced to subsume ⊥ (i.e. is unsatisfiable). ⊥ ⊑ ⊥ is not a defect.
    fn snapshot(&self) -> Classification {
        let nothing = iri(OWL_NOTHING);
        let mut unsat = BTreeSet::new();
        for (a, subs) in &self.s {
            if a != &nothing && subs.contains(&nothing) {
                unsat.insert(a.clone());
            }
        }
        Classification {
            subsumers: self.s.clone(),
            roles: self.r.clone(),
            justifications: self.just.clone(),
            confidence: if self.weighted {
                self.conf.clone()
            } else {
                BTreeMap::new()
            },
            consistent: unsat.is_empty(),
            unsatisfiable: unsat,
        }
    }
}

fn collect_named(c: &Concept, out: &mut BTreeSet<String>) {
    match c {
        Concept::Named(n) => {
            // IRIs and hasValue nominal tokens (`{<value>}`) — both must be seeded.
            if n.starts_with('<') || n.starts_with('{') {
                out.insert(n.clone());
            }
        }
        Concept::Some(_, f) => collect_named(f, out),
    }
}

fn merge_ontology(into: &mut Ontology, delta: Ontology) {
    into.gcis.extend(delta.gcis);
    into.sub_roles.extend(delta.sub_roles);
    into.chains.extend(delta.chains);
    into.inverses.extend(delta.inverses);
    into.symmetric.extend(delta.symmetric);
    into.domains.extend(delta.domains);
    into.ranges.extend(delta.ranges);
    into.disjoint.extend(delta.disjoint);
    into.all_values.extend(delta.all_values);
    into.functional.extend(delta.functional);
    into.inverse_functional.extend(delta.inverse_functional);
    into.same_as.extend(delta.same_as);
    into.different_from.extend(delta.different_from);
    into.classes.extend(delta.classes);
}

// ── Instance materialization (the bridge to the property-graph / RowSet) ─────

/// Given the classification + the concrete instance→type assignments already in the
/// graph, return every `(instance, inferred_class)` pair: for each individual `x` of
/// asserted type `A`, `x` is an instance of every `B ∈ S(A)`. This is what seeds a
/// `Reason` Op's RowSet (the EL-inferred members of a target class) — including
/// members the property-graph stored NO explicit type edge for.
///
/// `asserted` maps an instance id → its asserted class IRIs (canonical `<iri>`).
pub fn materialize_instances(
    cls: &Classification,
    asserted: &HashMap<String, HashSet<String>>,
) -> HashMap<String, BTreeSet<String>> {
    let mut out: HashMap<String, BTreeSet<String>> = HashMap::new();
    for (inst, types) in asserted {
        let entry = out.entry(inst.clone()).or_default();
        for ty in types {
            if let Some(subs) = cls.subsumers.get(ty) {
                for sup in subs {
                    entry.insert(sup.clone());
                }
            } else {
                entry.insert(ty.clone());
            }
        }
    }
    out
}

/// Every instance id that is (inferred to be) a member of `target_class` under the
/// classification — the id set a `Reason{target}` Op projects into a RowSet.
pub fn instances_of(
    cls: &Classification,
    asserted: &HashMap<String, HashSet<String>>,
    target_class: &str,
) -> Vec<String> {
    let mat = materialize_instances(cls, asserted);
    let mut out: Vec<String> = mat
        .into_iter()
        .filter_map(|(inst, classes)| classes.contains(target_class).then_some(inst))
        .collect();
    out.sort();
    out
}

/// The confidence of a single TYPE FACT (CONCEPT:KG-2.236): the engine's per-node
/// `confidence` in `[0,1]` (default `1.0`) MULTIPLIED by its Ebbinghaus recency weight
/// `exp(-ln2·age/half_life)` — so an OLD fact contributes LESS even when its stored
/// confidence is high. `age`/`half_life` share a unit. This is the epistemic
/// differentiator: OWL inference is confidence-weighted AND time-aware over the SAME
/// forgetting curve the memory/series tier uses ([`eg_core::decay::ebbinghaus_weight`]).
#[inline]
pub fn fact_confidence(node_confidence: f64, age: f64, half_life: f64) -> f64 {
    (node_confidence.clamp(0.0, 1.0) * eg_core::decay::ebbinghaus_weight(age, half_life))
        .clamp(0.0, 1.0)
}

/// Confidence-weighted instance membership (CONCEPT:KG-2.236). For each individual `x`
/// with asserted type facts `{(A, fact_conf)}`, `x` is a member of every `B ∈ S(A)`
/// with confidence `fact_conf × subclass_confidence(A, B)` (the conjunctive chain: the
/// fact AND the subsumption must both hold). When several asserted types reach the same
/// `B`, the strongest derivation wins (MAX / noisy-OR). Returns
/// `instance -> {class -> confidence}`. `asserted_conf` maps an instance id → its
/// asserted `(class, fact_confidence)` pairs (the fact confidence is typically
/// [`fact_confidence`] of the node's stored `confidence` + age).
pub fn materialize_instances_weighted(
    cls: &Classification,
    asserted_conf: &HashMap<String, Vec<(String, f64)>>,
) -> HashMap<String, HashMap<String, f64>> {
    let mut out: HashMap<String, HashMap<String, f64>> = HashMap::new();
    for (inst, type_facts) in asserted_conf {
        let entry = out.entry(inst.clone()).or_default();
        for (ty, fact_conf) in type_facts {
            let fact_conf = fact_conf.clamp(0.0, 1.0);
            if let Some(subs) = cls.subsumers.get(ty) {
                for sup in subs {
                    let c = fact_conf * cls.subclass_confidence(ty, sup);
                    let slot = entry.entry(sup.clone()).or_insert(0.0);
                    if c > *slot {
                        *slot = c;
                    }
                }
            } else {
                // No classification entry → the asserted type itself, at fact conf.
                let slot = entry.entry(ty.clone()).or_insert(0.0);
                if fact_conf > *slot {
                    *slot = fact_conf;
                }
            }
        }
    }
    out
}

/// Confidence-weighted members of `target_class` (CONCEPT:KG-2.236): every individual
/// inferred to be a `target_class` with membership confidence `≥ min_confidence`,
/// returned as `(instance, confidence)` sorted by DESCENDING confidence (then id). A
/// `min_confidence ≤ 0.0` keeps every member; a high `min_confidence` THRESHOLDS out
/// the weakly-supported (low-confidence axiom chain, or a decayed/old fact) entailments.
pub fn instances_of_weighted(
    cls: &Classification,
    asserted_conf: &HashMap<String, Vec<(String, f64)>>,
    target_class: &str,
    min_confidence: f64,
) -> Vec<(String, f64)> {
    let mat = materialize_instances_weighted(cls, asserted_conf);
    let mut out: Vec<(String, f64)> = mat
        .into_iter()
        .filter_map(|(inst, classes)| {
            classes
                .get(target_class)
                .and_then(|&c| (c >= min_confidence).then_some((inst, c)))
        })
        .collect();
    out.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    out
}

/// Read the asserted `instance -> {class}` assignments out of a GraphView's node
/// blobs (the folded `type` property + any explicit `rdf:type` edges) so a `Reason`
/// Op can classify the live graph. The class ids are canonical `<iri>` form to match
/// the ontology signature.
pub fn asserted_types_from_view(
    view: &eg_core::graph::GraphView,
) -> HashMap<String, HashSet<String>> {
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();
    for (id, blob) in &view.node_properties {
        if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) {
            if let Some(t) = v.get("type").and_then(|x| x.as_str()) {
                // A folded `type` may be a bare IRI string or a local label; store as
                // canonical <iri> when it looks like an IRI, else as-is.
                let class = if t.starts_with('<') || t.starts_with("http") {
                    iri(t.trim_start_matches('<').trim_end_matches('>'))
                } else {
                    t.to_string()
                };
                out.entry(id.clone()).or_default().insert(class);
            }
        }
    }
    // Explicit rdf:type edges (multi-typed resources): an edge whose predicate is
    // `rdf:type` assigns the subject `s` the class node `o`.
    let rdf_type_edge = iri(RDF_TYPE);
    for ((s, o), blobs) in &view.edge_properties {
        for blob in blobs {
            if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) {
                let pred = v.get("type").and_then(|x| x.as_str());
                if pred == Some(RDF_TYPE) || pred == Some(rdf_type_edge.as_str()) {
                    out.entry(s.clone()).or_default().insert(o.clone());
                }
            }
        }
    }
    out
}

/// Like [`asserted_types_from_view`] but ALSO reads each fact's confidence
/// (CONCEPT:KG-2.236): the per-node `confidence` (default `1.0`) decayed by its age
/// `now - last_access` (→ `updated_at` → `created_at`) on the Ebbinghaus curve with
/// `default_half_life` (or a per-node `half_life`), via [`fact_confidence`]. So an OLD
/// type fact (one not touched in a long time) contributes a LOWER confidence to the
/// inferred membership than a fresh one. `now`/`half_life` share a unit (seconds, like
/// the engine's stored epoch timestamps). The result is what
/// [`materialize_instances_weighted`] / [`instances_of_weighted`] consume. A node with
/// no timestamp (age `0`) keeps its stored confidence unchanged.
pub fn asserted_types_with_confidence_from_view(
    view: &eg_core::graph::GraphView,
    now: u64,
    default_half_life: f64,
) -> HashMap<String, Vec<(String, f64)>> {
    fn fact_conf_of(v: &serde_json::Value, now: u64, default_half_life: f64) -> f64 {
        let confidence = v.get("confidence").and_then(|x| x.as_f64()).unwrap_or(1.0);
        let last_access = v
            .get("last_access")
            .and_then(|x| x.as_u64())
            .or_else(|| v.get("updated_at").and_then(|x| x.as_u64()))
            .or_else(|| v.get("created_at").and_then(|x| x.as_u64()));
        let half_life = v
            .get("half_life")
            .and_then(|x| x.as_f64())
            .filter(|h| *h > 0.0)
            .unwrap_or(default_half_life);
        let age = match last_access {
            Some(la) if now > la => (now - la) as f64,
            _ => 0.0,
        };
        fact_confidence(confidence, age, half_life)
    }

    let mut out: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    for (id, blob) in &view.node_properties {
        if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) {
            if let Some(t) = v.get("type").and_then(|x| x.as_str()) {
                let class = if t.starts_with('<') || t.starts_with("http") {
                    iri(t.trim_start_matches('<').trim_end_matches('>'))
                } else {
                    t.to_string()
                };
                let c = fact_conf_of(&v, now, default_half_life);
                out.entry(id.clone()).or_default().push((class, c));
            }
        }
    }
    // Explicit rdf:type edges carry their own edge-blob confidence.
    let rdf_type_edge = iri(RDF_TYPE);
    for ((s, o), blobs) in &view.edge_properties {
        for blob in blobs {
            if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) {
                let pred = v.get("type").and_then(|x| x.as_str());
                if pred == Some(RDF_TYPE) || pred == Some(rdf_type_edge.as_str()) {
                    let c = fact_conf_of(&v, now, default_half_life);
                    out.entry(s.clone()).or_default().push((o.clone(), c));
                }
            }
        }
    }
    out
}

/// Extract the OWL/RDF triples (the TBox axioms + folded `rdf:type` facts) directly
/// from a live `GraphView`, WITHOUT the lossless-literal quad table (TBox axioms are
/// resource triples — `subClassOf`/`onProperty`/`someValuesFrom`/etc. — so the quad
/// table, which only holds multi-valued LITERALS, is irrelevant). Each edge becomes
/// `(s, edge-type, o)`; each node `type` cell becomes `(node, rdf:type, type)`. This
/// is what a `Reason` Op classifies when no explicit ontology document is supplied —
/// it reasons over the axioms already loaded into the graph via `AddTriples`.
pub fn tbox_triples_from_view(view: &eg_core::graph::GraphView) -> Vec<Triple> {
    use oxrdf::{BlankNode, NamedNode, Subject};

    fn subj(id: &str) -> Option<Subject> {
        if let Some(i) = id.strip_prefix('<').and_then(|s| s.strip_suffix('>')) {
            NamedNode::new(i).ok().map(Subject::NamedNode)
        } else if let Some(b) = id.strip_prefix("_:") {
            BlankNode::new(b).ok().map(Subject::BlankNode)
        } else {
            None
        }
    }
    fn obj(id: &str) -> Option<Term> {
        if let Some(i) = id.strip_prefix('<').and_then(|s| s.strip_suffix('>')) {
            NamedNode::new(i).ok().map(Term::NamedNode)
        } else if let Some(b) = id.strip_prefix("_:") {
            BlankNode::new(b).ok().map(Term::BlankNode)
        } else {
            // A bare IRI (folded `type` may be stored without angle brackets).
            NamedNode::new(id).ok().map(Term::NamedNode)
        }
    }

    let mut out = Vec::new();
    // Edges → object triples (predicate is the edge `type`).
    for ((s, o), blobs) in &view.edge_properties {
        for blob in blobs {
            if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) {
                if let Some(pred) = v.get("type").and_then(|x| x.as_str()) {
                    if let (Some(su), Some(pr), Some(ob)) =
                        (subj(s), NamedNode::new(pred).ok(), obj(o))
                    {
                        out.push(Triple::new(su, pr, ob));
                    }
                }
            }
        }
    }
    // Node `type` cells → folded rdf:type triples.
    for (id, blob) in &view.node_properties {
        if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) {
            if let Some(t) = v.get("type").and_then(|x| x.as_str()) {
                if let (Some(su), Some(ob)) = (subj(id), obj(t)) {
                    if let Ok(rt) = NamedNode::new(RDF_TYPE) {
                        out.push(Triple::new(su, rt, ob));
                    }
                }
            }
        }
    }
    out
}

// ── Distributed (cross-shard) reasoning (CONCEPT:KG-2.236) ────────────────────

/// The materialized result of a confidence-weighted reasoning run over one or more
/// graph views (CONCEPT:KG-2.236). `subclasses` carries the derived subsumptions WITH
/// confidence; `instances` the inferred memberships WITH confidence, already filtered
/// by `min_confidence`. This is the shape both the single-graph and the cross-shard
/// paths produce — so the distributed run is provably identical to the single-graph
/// run on the union.
#[derive(Clone, Debug, PartialEq)]
pub struct WeightedReasonResult {
    /// `(sub, sup, confidence)` — the classification hierarchy with per-edge confidence.
    pub subclasses: Vec<(String, String, f64)>,
    /// `(instance, class, confidence)` — inferred memberships with `confidence ≥ τ`.
    pub instances: Vec<(String, String, f64)>,
    pub consistent: bool,
    pub unsatisfiable: Vec<String>,
}

/// Run confidence-weighted EL⁺/RL reasoning over the UNION of `views` (each a graph /
/// shard snapshot) plus optional extra `ontology` triples (CONCEPT:KG-2.236). Gathers
/// every view's TBox axioms AND its asserted type facts (each with its decayed
/// confidence, via [`asserted_types_with_confidence_from_view`]), unions them, and runs
/// ONE weighted closure — so facts that span MULTIPLE shards classify together exactly
/// as if they lived in one graph. When `target_class` is non-empty the instance result
/// is restricted to that class's (inferred) members. `now`/`half_life` drive the fact
/// decay; `min_confidence` thresholds the returned instances. A single-element `views`
/// slice is the ordinary single-graph path (the fast path stays a one-graph gather).
pub fn reason_distributed_weighted(
    views: &[&eg_core::graph::GraphView],
    extra_ontology_triples: &[Triple],
    now: u64,
    half_life: f64,
    target_class: &str,
    min_confidence: f64,
) -> WeightedReasonResult {
    // 1. Gather + UNION the TBox axioms across every shard (+ the explicit ontology).
    let mut triples: Vec<Triple> = Vec::new();
    for v in views {
        triples.extend(tbox_triples_from_view(v));
    }
    triples.extend_from_slice(extra_ontology_triples);

    // 2. Classify the unioned TBox once, with confidence propagation.
    let mut reasoner = Reasoner::from_triples(&triples);
    let cls = reasoner.classify_weighted();

    // 3. Gather + UNION the asserted (decayed-confidence) facts across every shard.
    //    A fact for the same instance asserted on two shards keeps the STRONGER.
    let mut asserted: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    for v in views {
        for (inst, facts) in asserted_types_with_confidence_from_view(v, now, half_life) {
            asserted.entry(inst).or_default().extend(facts);
        }
    }

    // 4. Project the weighted subsumptions + the thresholded instance memberships.
    let mut subclasses: Vec<(String, String, f64)> = Vec::new();
    for (sub, sups) in &cls.subsumers {
        for sup in sups {
            subclasses.push((sub.clone(), sup.clone(), cls.subclass_confidence(sub, sup)));
        }
    }
    subclasses.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let instances: Vec<(String, String, f64)> = if target_class.trim().is_empty() {
        let mat = materialize_instances_weighted(&cls, &asserted);
        let mut out = Vec::new();
        for (inst, classes) in mat {
            for (c, conf) in classes {
                if conf >= min_confidence {
                    out.push((inst.clone(), c, conf));
                }
            }
        }
        out.sort_by(|a, b| {
            b.2.partial_cmp(&a.2)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)))
        });
        out
    } else {
        let target = if target_class.starts_with('<') {
            target_class.to_string()
        } else {
            format!(
                "<{}>",
                target_class.trim_start_matches('<').trim_end_matches('>')
            )
        };
        instances_of_weighted(&cls, &asserted, &target, min_confidence)
            .into_iter()
            .map(|(inst, conf)| (inst, target.clone(), conf))
            .collect()
    };

    WeightedReasonResult {
        subclasses,
        instances,
        consistent: cls.consistent,
        unsatisfiable: cls.unsatisfiable.into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::parse_turtle;

    /// The headline proof: EL derives `HumanHeart ⊑ HumanComponent` through an
    /// existential restriction on the LHS of a subclass axiom + a role chain — an
    /// entailment the RL `reasoning.rs` cannot reach (no concrete partOf edge exists).
    #[test]
    fn el_derives_existential_restriction_subsumption() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .

ex:Heart      rdfs:subClassOf [ a owl:Restriction ;
                                owl:onProperty ex:partOf ;
                                owl:someValuesFrom ex:Body ] .
[ a owl:Restriction ; owl:onProperty ex:partOf ; owl:someValuesFrom ex:Body ]
              rdfs:subClassOf ex:HumanComponent .
ex:HumanHeart rdfs:subClassOf ex:Heart .
ex:partOf     owl:propertyChainAxiom ( ex:partOf ex:partOf ) .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let mut reasoner = Reasoner::from_triples(&triples);
        let cls = reasoner.classify();
        assert!(cls.consistent, "ontology is consistent");
        assert!(
            cls.entails_subclass(
                "<http://example.org/HumanHeart>",
                "<http://example.org/HumanComponent>"
            ),
            "EL must derive HumanHeart ⊑ HumanComponent through ∃partOf.Body on the LHS;\n S(HumanHeart) = {:?}",
            cls.subsumers.get("<http://example.org/HumanHeart>")
        );
        // And a justification cites the existential-restriction axiom.
        let j = cls.justifications.get(&(
            "<http://example.org/HumanHeart>".into(),
            "<http://example.org/HumanComponent>".into(),
        ));
        assert!(
            j.is_some(),
            "the derived subsumption must carry a justification"
        );
    }

    /// equivalentClass is a two-way subclass — `Person ≡ HumanBeing` propagates
    /// `Mortal` both ways (a class-level RL-reachable case EL also handles).
    #[test]
    fn el_handles_equivalent_class() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Person     owl:equivalentClass ex:HumanBeing .
ex:HumanBeing rdfs:subClassOf      ex:Mortal .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let mut reasoner = Reasoner::from_triples(&triples);
        let cls = reasoner.classify();
        assert!(cls.entails_subclass("<http://example.org/Person>", "<http://example.org/Mortal>"));
        // Equivalence is symmetric: HumanBeing ⊑ Person too.
        assert!(cls.entails_subclass(
            "<http://example.org/HumanBeing>",
            "<http://example.org/Person>"
        ));
    }

    /// Conjunctive LHS (EL ⊓): `Parent ⊓ Male ⊑ Father`; a class subsumed by both
    /// conjuncts gets `Father`.
    #[test]
    fn el_conjunction_left_hand_side() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
[ owl:intersectionOf ( ex:Parent ex:Male ) ] rdfs:subClassOf ex:Father .
ex:Dad rdfs:subClassOf ex:Parent .
ex:Dad rdfs:subClassOf ex:Male .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let mut reasoner = Reasoner::from_triples(&triples);
        let cls = reasoner.classify();
        assert!(
            cls.entails_subclass("<http://example.org/Dad>", "<http://example.org/Father>"),
            "Dad ⊑ Parent ⊓ Male ⇒ Dad ⊑ Father; S(Dad) = {:?}",
            cls.subsumers.get("<http://example.org/Dad>")
        );
    }

    /// Consistency checking: `owl:disjointWith` + a class subsumed by BOTH disjoint
    /// classes derives ⊥ ⇒ the ontology is inconsistent and the class unsatisfiable.
    #[test]
    fn consistency_detects_bottom_derivation() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Alive owl:disjointWith ex:Dead .
ex:Zombie rdfs:subClassOf ex:Alive .
ex:Zombie rdfs:subClassOf ex:Dead .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let mut reasoner = Reasoner::from_triples(&triples);
        let cls = reasoner.classify();
        assert!(
            !cls.consistent,
            "Zombie ⊑ Alive ⊓ Dead (disjoint) ⇒ inconsistent"
        );
        assert!(cls.unsatisfiable.contains("<http://example.org/Zombie>"));
    }

    /// A consistent ontology with no disjointness reports consistent + no unsat.
    #[test]
    fn consistent_ontology_is_consistent() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Dog rdfs:subClassOf ex:Animal .
ex:Animal rdfs:subClassOf ex:LivingThing .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let mut reasoner = Reasoner::from_triples(&triples);
        let cls = reasoner.classify();
        assert!(cls.consistent && cls.unsatisfiable.is_empty());
        assert!(cls.entails_subclass(
            "<http://example.org/Dog>",
            "<http://example.org/LivingThing>"
        ));
    }

    /// Incremental materialization: classify, then ADD an axiom and re-saturate —
    /// only the new consequence appears, and the result equals a from-scratch run.
    #[test]
    fn incremental_add_axiom_only_adds() {
        let base = r#"
@prefix ex:  <http://example.org/> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Dog rdfs:subClassOf ex:Mammal .
"#;
        let mut reasoner = Reasoner::from_triples(&parse_turtle(base).unwrap());
        let before = reasoner.classify();
        assert!(before.entails_subclass("<http://example.org/Dog>", "<http://example.org/Mammal>"));
        assert!(!before.entails_subclass("<http://example.org/Dog>", "<http://example.org/Animal>"));

        // Delta: Mammal ⊑ Animal. Incrementally derive Dog ⊑ Animal.
        let delta_ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Mammal rdfs:subClassOf ex:Animal .
"#;
        let delta = parse_ontology(&parse_turtle(delta_ttl).unwrap());
        let after = reasoner.add_axioms(delta);
        assert!(after.entails_subclass("<http://example.org/Dog>", "<http://example.org/Animal>"));
        // Monotone: every prior subsumption still holds.
        for (a, subs) in &before.subsumers {
            for b in subs {
                assert!(
                    after.entails_subclass(a, b),
                    "incremental dropped {a} ⊑ {b}"
                );
            }
        }

        // Equivalence to a from-scratch run over the union.
        let full_ttl = format!("{base}\n{delta_ttl}");
        let mut scratch = Reasoner::from_triples(&parse_turtle(&full_ttl).unwrap());
        let scratch_cls = scratch.classify();
        assert_eq!(
            after.subsumers.get("<http://example.org/Dog>"),
            scratch_cls.subsumers.get("<http://example.org/Dog>"),
            "incremental S(Dog) must equal the from-scratch S(Dog)"
        );
    }

    // ── Probabilistic / confidence-weighted reasoning (CONCEPT:KG-2.236) ─────────

    /// PROOF 1 — a chain of HIGH-confidence axioms yields a HIGH-confidence entailment.
    /// `Dog ⊑ Mammal (0.9)`, `Mammal ⊑ Animal (0.9)` ⇒ `Dog ⊑ Animal` at `0.81`
    /// (the conjunctive product 0.9·0.9), still high.
    #[test]
    fn prob_high_confidence_chain_stays_high() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
@prefix eg:  <http://epistemic-graph/owl#> .
ex:Dog    rdfs:subClassOf ex:Mammal .
ex:Dog    eg:confidence "0.9" .
ex:Mammal rdfs:subClassOf ex:Animal .
ex:Mammal eg:confidence "0.9" .
"#;
        let mut r = Reasoner::from_triples(&parse_turtle(ttl).unwrap());
        let cls = r.classify_weighted();
        let c = cls.subclass_confidence("<http://example.org/Dog>", "<http://example.org/Animal>");
        assert!(
            (c - 0.81).abs() < 1e-9,
            "Dog ⊑ Animal confidence = 0.9·0.9 = 0.81, got {c}"
        );
        // The direct asserted edge is the axiom confidence itself.
        assert!(
            (cls.subclass_confidence("<http://example.org/Dog>", "<http://example.org/Mammal>")
                - 0.9)
                .abs()
                < 1e-9
        );
        // A hard ⊤ subsumption stays certain.
        assert!(
            (cls.subclass_confidence(
                "<http://example.org/Dog>",
                "<http://www.w3.org/2002/07/owl#Thing>"
            ) - 1.0)
                .abs()
                < 1e-9
        );
    }

    /// PROOF 2 — a LOW-confidence premise LOWERS the derived confidence. Same chain,
    /// but `Mammal ⊑ Animal` drops to `0.2` ⇒ `Dog ⊑ Animal` falls to `0.9·0.2 = 0.18`.
    #[test]
    fn prob_low_premise_lowers_derived() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
@prefix eg:  <http://epistemic-graph/owl#> .
ex:Dog    rdfs:subClassOf ex:Mammal .
ex:Dog    eg:confidence "0.9" .
ex:Mammal rdfs:subClassOf ex:Animal .
ex:Mammal eg:confidence "0.2" .
"#;
        let mut r = Reasoner::from_triples(&parse_turtle(ttl).unwrap());
        let cls = r.classify_weighted();
        let c = cls.subclass_confidence("<http://example.org/Dog>", "<http://example.org/Animal>");
        assert!(
            (c - 0.18).abs() < 1e-9,
            "a weak premise drags Dog ⊑ Animal to 0.9·0.2 = 0.18, got {c}"
        );
        // Membership is unchanged — confidence weighting never alters WHICH hold.
        assert!(cls.entails_subclass("<http://example.org/Dog>", "<http://example.org/Animal>"));
    }

    /// PROOF 3 — the EXISTENTIAL-restriction headline path propagates confidence
    /// through `∃partOf.Body` on the LHS + the role chain: every axiom 0.9 ⇒ the
    /// multi-step `HumanHeart ⊑ HumanComponent` confidence is the product of the path.
    #[test]
    fn prob_existential_path_propagates_confidence() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
@prefix eg:  <http://epistemic-graph/owl#> .
ex:Heart      rdfs:subClassOf [ a owl:Restriction ;
                                owl:onProperty ex:partOf ;
                                owl:someValuesFrom ex:Body ] .
ex:Heart      eg:confidence "0.9" .
[ a owl:Restriction ; owl:onProperty ex:partOf ; owl:someValuesFrom ex:Body ]
              rdfs:subClassOf ex:HumanComponent .
ex:HumanHeart rdfs:subClassOf ex:Heart .
ex:HumanHeart eg:confidence "0.9" .
"#;
        let mut r = Reasoner::from_triples(&parse_turtle(ttl).unwrap());
        let cls = r.classify_weighted();
        assert!(cls.entails_subclass(
            "<http://example.org/HumanHeart>",
            "<http://example.org/HumanComponent>"
        ));
        let c = cls.subclass_confidence(
            "<http://example.org/HumanHeart>",
            "<http://example.org/HumanComponent>",
        );
        // HumanHeart ⊑ Heart (0.9) → ∃partOf.Body via Heart's restriction (0.9) →
        // HumanComponent (the ∃-LHS axiom is hard 1.0). conf = 0.9·0.9 = 0.81.
        assert!(
            (c - 0.81).abs() < 1e-9,
            "existential-path confidence = 0.9·0.9 = 0.81, got {c}"
        );
        assert!(
            c > 0.0 && c < 1.0,
            "a soft path is neither certain nor zero"
        );
    }

    /// PROOF 4 — MAX across ALTERNATIVE derivations (noisy-OR). A diamond: `X ⊑ A`,
    /// `X ⊑ B` (both at X's confidence 1.0), `A ⊑ Goal (0.5)`, `B ⊑ Goal (0.9)`. The
    /// two derivations of `X ⊑ Goal` give `1.0·0.5 = 0.5` and `1.0·0.9 = 0.9`; the
    /// closure keeps the STRONGER, `0.9`.
    #[test]
    fn prob_alternative_derivations_take_max() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
@prefix eg:  <http://epistemic-graph/owl#> .
ex:X rdfs:subClassOf ex:A .
ex:X rdfs:subClassOf ex:B .
ex:A rdfs:subClassOf ex:Goal .
ex:A eg:confidence "0.5" .
ex:B rdfs:subClassOf ex:Goal .
ex:B eg:confidence "0.9" .
"#;
        let mut r = Reasoner::from_triples(&parse_turtle(ttl).unwrap());
        let cls = r.classify_weighted();
        let c = cls.subclass_confidence("<http://example.org/X>", "<http://example.org/Goal>");
        assert!(
            (c - 0.9).abs() < 1e-9,
            "two derivations 0.5 and 0.9 ⇒ MAX 0.9, got {c}"
        );
        // The weaker path is still present as the A-route confidence.
        assert!(
            (cls.subclass_confidence("<http://example.org/A>", "<http://example.org/Goal>") - 0.5)
                .abs()
                < 1e-9
        );
    }

    /// PROOF 5 — a DECAYED (old) fact contributes LESS confidence to an inferred
    /// membership than a fresh one. Two individuals of the SAME inferred class: a fresh
    /// fact keeps the subsumption confidence; a fact aged one half-life halves it.
    #[test]
    fn prob_decayed_fact_contributes_less() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
@prefix eg:  <http://epistemic-graph/owl#> .
ex:Mammal rdfs:subClassOf ex:Animal .
ex:Mammal eg:confidence "0.9" .
"#;
        let mut r = Reasoner::from_triples(&parse_turtle(ttl).unwrap());
        let cls = r.classify_weighted();
        let target = "<http://example.org/Animal>";
        let half_life = 30.0;

        // fresh Mammal fact (age 0, stored confidence 1.0) → membership = 1.0·0.9 = 0.9.
        let mut asserted: HashMap<String, Vec<(String, f64)>> = HashMap::new();
        asserted.insert(
            "<http://example.org/freshOne>".into(),
            vec![(
                "<http://example.org/Mammal>".into(),
                fact_confidence(1.0, 0.0, half_life),
            )],
        );
        // old Mammal fact aged exactly one half-life → fact conf 0.5 → membership 0.45.
        asserted.insert(
            "<http://example.org/oldOne>".into(),
            vec![(
                "<http://example.org/Mammal>".into(),
                fact_confidence(1.0, half_life, half_life),
            )],
        );

        let members = instances_of_weighted(&cls, &asserted, target, 0.0);
        let conf_of = |id: &str| members.iter().find(|(i, _)| i == id).map(|(_, c)| *c);
        let fresh = conf_of("<http://example.org/freshOne>").unwrap();
        let old = conf_of("<http://example.org/oldOne>").unwrap();
        assert!(
            (fresh - 0.9).abs() < 1e-9,
            "fresh membership = 1.0·0.9 = 0.9, got {fresh}"
        );
        assert!(
            (old - 0.45).abs() < 1e-9,
            "decayed (one half-life) membership = 0.5·0.9 = 0.45, got {old}"
        );
        assert!(
            old < fresh,
            "a decayed fact contributes LESS than a fresh one"
        );

        // PROOF 6 — thresholding EXCLUDES sub-τ entailments. τ = 0.5 keeps fresh (0.9),
        // drops the decayed (0.45).
        let kept = instances_of_weighted(&cls, &asserted, target, 0.5);
        let ids: Vec<&str> = kept.iter().map(|(i, _)| i.as_str()).collect();
        assert!(ids.contains(&"<http://example.org/freshOne>"));
        assert!(
            !ids.contains(&"<http://example.org/oldOne>"),
            "τ=0.5 must exclude the 0.45 decayed membership"
        );
        // Sorted by descending confidence.
        assert_eq!(
            kept.first().map(|(i, _)| i.as_str()),
            Some("<http://example.org/freshOne>")
        );
    }

    // ── Distributed (2-shard) reasoning == single-graph (CONCEPT:KG-2.236) ───────

    /// Build a `GraphView` holding the given individuals as typed nodes, each with a
    /// `confidence` + `last_access` so the decay path is exercised. (TBox is supplied
    /// separately as an ontology document, like a shared schema over sharded ABox.)
    fn view_with_individuals(individuals: &[(&str, &str, f64, u64)]) -> eg_core::graph::GraphView {
        let core = eg_core::graph::GraphCore::new();
        for (id, ty, conf, last_access) in individuals {
            let blob = rmp_serde::to_vec_named(&serde_json::json!({
                "type": ty,
                "confidence": conf,
                "last_access": last_access,
            }))
            .unwrap();
            core.add_node(id.to_string(), blob);
        }
        core.analysis_snapshot()
    }

    /// PROOF — a distributed run over a 2-SHARD ontology derives the SAME entailments
    /// AND the SAME confidences as the identical ontology in ONE graph. The TBox is
    /// shared; the ABox (the individuals) is SPLIT across two shards.
    #[test]
    fn distributed_two_shard_equals_single_graph() {
        let tbox = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
@prefix eg:  <http://epistemic-graph/owl#> .
ex:Paper rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] .
ex:Paper eg:confidence "0.9" .
[ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] rdfs:subClassOf ex:ScholarlyWork .
ex:Article rdfs:subClassOf ex:Paper .
ex:Article eg:confidence "0.8" .
"#;
        let onto = parse_turtle(tbox).unwrap();
        let now = 1_000_000u64;
        let hl = 30.0;

        // The full ABox in ONE graph.
        let single = view_with_individuals(&[
            (
                "<http://example.org/p1>",
                "<http://example.org/Paper>",
                1.0,
                now,
            ),
            (
                "<http://example.org/p2>",
                "<http://example.org/Article>",
                1.0,
                now,
            ),
            (
                "<http://example.org/p3>",
                "<http://example.org/Topic>",
                1.0,
                now,
            ),
        ]);
        let single_res = reason_distributed_weighted(&[&single], &onto, now, hl, "", 0.0);

        // The SAME ABox SPLIT across two shards: p1 on shard A, p2+p3 on shard B.
        let shard_a = view_with_individuals(&[(
            "<http://example.org/p1>",
            "<http://example.org/Paper>",
            1.0,
            now,
        )]);
        let shard_b = view_with_individuals(&[
            (
                "<http://example.org/p2>",
                "<http://example.org/Article>",
                1.0,
                now,
            ),
            (
                "<http://example.org/p3>",
                "<http://example.org/Topic>",
                1.0,
                now,
            ),
        ]);
        let dist_res = reason_distributed_weighted(&[&shard_a, &shard_b], &onto, now, hl, "", 0.0);

        // Identical entailments + confidences.
        assert_eq!(
            single_res.subclasses, dist_res.subclasses,
            "cross-shard subsumptions+confidences must equal the single-graph closure"
        );
        let mut a = single_res.instances.clone();
        let mut b = dist_res.instances.clone();
        a.sort_by(|x, y| x.0.cmp(&y.0).then(x.1.cmp(&y.1)));
        b.sort_by(|x, y| x.0.cmp(&y.0).then(x.1.cmp(&y.1)));
        assert_eq!(
            a, b,
            "cross-shard instance memberships+confidences must equal single-graph"
        );
        assert_eq!(single_res.consistent, dist_res.consistent);

        // And the actual epistemic content is right: p2 (Article) is an inferred
        // ScholarlyWork with confidence 0.8·0.9 = 0.72 (Article⊑Paper 0.8, Paper⊑∃ 0.9).
        let sw = "<http://example.org/ScholarlyWork>";
        let p2 = dist_res
            .instances
            .iter()
            .find(|(i, c, _)| i == "<http://example.org/p2>" && c == sw)
            .expect("p2 is an inferred ScholarlyWork across shards");
        assert!(
            (p2.2 - 0.72).abs() < 1e-9,
            "p2 ScholarlyWork conf 0.72, got {}",
            p2.2
        );
        // p3 (Topic) is NOT a ScholarlyWork.
        assert!(!dist_res
            .instances
            .iter()
            .any(|(i, c, _)| i == "<http://example.org/p3>" && c == sw));
    }

    // ── EG-021: broader OWL-2 axioms toward DL-lite ─────────────────────────────

    /// `owl:equivalentProperty` is two-way subPropertyOf: a role pair under `r` also
    /// holds under its equivalent `s` (and vice-versa). Proven via the R relation a
    /// someValuesFrom witness creates.
    #[test]
    fn eg021_equivalent_property_both_ways() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:partOf owl:equivalentProperty ex:componentOf .
ex:Heart rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:partOf ; owl:someValuesFrom ex:Body ] .
[ a owl:Restriction ; owl:onProperty ex:componentOf ; owl:someValuesFrom ex:Body ] rdfs:subClassOf ex:Embedded .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let mut r = Reasoner::from_triples(&triples);
        let cls = r.classify();
        // Heart ⊑ ∃partOf.Body ; partOf ≡ componentOf ⇒ Heart ⊑ ∃componentOf.Body ;
        // ∃componentOf.Body ⊑ Embedded ⇒ Heart ⊑ Embedded.
        assert!(
            cls.entails_subclass(
                "<http://example.org/Heart>",
                "<http://example.org/Embedded>"
            ),
            "equivalentProperty must carry the role witness; S(Heart)={:?}",
            cls.subsumers.get("<http://example.org/Heart>")
        );
    }

    /// `owl:allValuesFrom` (RL cls-avf): `Parent ⊑ ∀hasChild.Happy`, and a class with a
    /// hasChild witness into some B forces `Happy ∈ S(B)`.
    #[test]
    fn eg021_all_values_from_propagates() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:HappyParent rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:hasChild ; owl:allValuesFrom ex:Happy ] .
ex:HappyParent rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:hasChild ; owl:someValuesFrom ex:Kid ] .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let mut r = Reasoner::from_triples(&triples);
        let cls = r.classify();
        // HappyParent has a hasChild witness (the someValuesFrom Kid creates R(hasChild)
        // with filler Kid); ∀hasChild.Happy then forces Kid ⊑ Happy.
        assert!(
            cls.entails_subclass("<http://example.org/Kid>", "<http://example.org/Happy>"),
            "cls-avf must force the witness into Happy; S(Kid)={:?}",
            cls.subsumers.get("<http://example.org/Kid>")
        );
    }

    /// `owl:unionOf` (sound direction): each disjunct of a union is subsumed by a class
    /// the union is a subclass of. `(Cat ⊔ Dog) ⊑ Pet` ⇒ `Cat ⊑ Pet` and `Dog ⊑ Pet`.
    #[test]
    fn eg021_union_of_subclass() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
[ owl:unionOf ( ex:Cat ex:Dog ) ] rdfs:subClassOf ex:Pet .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let mut r = Reasoner::from_triples(&triples);
        let cls = r.classify();
        assert!(cls.entails_subclass("<http://example.org/Cat>", "<http://example.org/Pet>"));
        assert!(cls.entails_subclass("<http://example.org/Dog>", "<http://example.org/Pet>"));
    }

    /// `owl:hasValue` value restriction composes as a nominal existential: `Italian ⊑
    /// ∃nationality.{Italy}` and `∃nationality.{Italy} ⊑ EUCitizen` ⇒ `Italian ⊑
    /// EUCitizen`.
    #[test]
    fn eg021_has_value_restriction() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Italian rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:nationality ; owl:hasValue ex:Italy ] .
[ a owl:Restriction ; owl:onProperty ex:nationality ; owl:hasValue ex:Italy ] rdfs:subClassOf ex:EUCitizen .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let mut r = Reasoner::from_triples(&triples);
        let cls = r.classify();
        assert!(
            cls.entails_subclass(
                "<http://example.org/Italian>",
                "<http://example.org/EUCitizen>"
            ),
            "hasValue nominal must compose; S(Italian)={:?}",
            cls.subsumers.get("<http://example.org/Italian>")
        );
    }

    /// `instances_of` materializes inferred class members from the live graph types —
    /// an individual asserted `HumanHeart` is an inferred `HumanComponent`.
    #[test]
    fn materialize_instances_through_el() {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Heart rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:partOf ; owl:someValuesFrom ex:Body ] .
[ a owl:Restriction ; owl:onProperty ex:partOf ; owl:someValuesFrom ex:Body ] rdfs:subClassOf ex:HumanComponent .
ex:HumanHeart rdfs:subClassOf ex:Heart .
"#;
        let mut reasoner = Reasoner::from_triples(&parse_turtle(ttl).unwrap());
        let cls = reasoner.classify();
        let mut asserted: HashMap<String, HashSet<String>> = HashMap::new();
        asserted.insert(
            "<http://example.org/myHeart>".into(),
            HashSet::from(["<http://example.org/HumanHeart>".into()]),
        );
        let members = instances_of(&cls, &asserted, "<http://example.org/HumanComponent>");
        assert_eq!(members, vec!["<http://example.org/myHeart>".to_string()]);
    }
}
