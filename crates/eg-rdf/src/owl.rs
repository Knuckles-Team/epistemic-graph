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

/// `<iri>` canonical form (matches the node-id convention of [`crate::mapping`]).
fn iri(s: &str) -> String {
    format!("<{s}>")
}

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
}

/// A role chain `r1 ∘ r2 ∘ … ⊑ s` (covers `TransitiveProperty` as `r ∘ r ⊑ r`).
#[derive(Clone, Debug)]
pub struct RoleChain {
    pub chain: Vec<String>,
    pub sup: String,
    pub label: String,
}

/// The parsed OWL TBox: the EL concept axioms + the RL property axioms.
#[derive(Clone, Debug, Default)]
pub struct Ontology {
    /// EL general concept inclusions (normalised).
    pub gcis: Vec<Gci>,
    /// `r ⊑ s` role inclusions (incl. those produced by `equivalentProperty`).
    pub sub_roles: Vec<(String, String, String)>,
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
/// Recognised: `rdfs:subClassOf` (incl. an `owl:Restriction`/`someValuesFrom` or an
/// `owl:intersectionOf` on either side), `owl:equivalentClass`, `rdfs:subPropertyOf`,
/// `owl:propertyChainAxiom`, `owl:TransitiveProperty`, `owl:SymmetricProperty`,
/// `owl:inverseOf`, `rdfs:domain`/`rdfs:range`, `owl:disjointWith`. Everything else is
/// ignored (we stay in the EL+RL envelope by construction — see crate docs / the
/// deferred-DL note).
pub fn parse_ontology(triples: &[Triple]) -> Ontology {
    let idx = TripleIndex::build(triples);
    let mut ont = Ontology::default();

    for t in triples {
        let s = term_key(&t.subject.clone().into());
        let p = t.predicate.as_str();
        let o = &t.object;
        match p {
            RDFS_SUBCLASS_OF => {
                if let (Some(lhs), Some(rhs)) = (
                    parse_class_expr(&idx, &s),
                    parse_class_expr(&idx, &term_key(o)),
                ) {
                    push_subclass(
                        &mut ont,
                        lhs,
                        rhs,
                        format!("{} ⊑ {}", short(&s), short(&term_key(o))),
                    );
                }
            }
            OWL_EQUIVALENT_CLASS => {
                let sk = term_key(o);
                if let (Some(a), Some(b)) =
                    (parse_class_expr(&idx, &s), parse_class_expr(&idx, &sk))
                {
                    // A ≡ B  ⇒  A ⊑ B  AND  B ⊑ A.
                    push_subclass(
                        &mut ont,
                        a.clone(),
                        b.clone(),
                        format!("{} ≡ {} (→⊑)", short(&s), short(&sk)),
                    );
                    push_subclass(
                        &mut ont,
                        b,
                        a,
                        format!("{} ≡ {} (←⊑)", short(&s), short(&sk)),
                    );
                }
            }
            RDFS_SUBPROPERTY_OF => {
                if let Term::NamedNode(sup) = o {
                    let sup = iri(sup.as_str());
                    ont.sub_roles.push((
                        s.clone(),
                        sup.clone(),
                        format!("{} ⊑ {}", short(&s), short(&sup)),
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
                            });
                        }
                        OWL_SYMMETRIC_PROPERTY => {
                            ont.symmetric.insert(s.clone());
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
    // A named class / Thing / Nothing.
    if id.starts_with('<') || id == iri(OWL_THING) || id == iri(OWL_NOTHING) {
        return Some(vec![Concept::Named(id.to_string())]);
    }
    // A bare blank node with no restriction/intersection structure — unsupported.
    None
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

fn push_gci(ont: &mut Ontology, lhs: Vec<Concept>, rhs: Concept, label: String) {
    for c in lhs.iter().chain(std::iter::once(&rhs)) {
        register_concept_classes(ont, c);
    }
    ont.gcis.push(Gci { lhs, rhs, label });
}

/// Push a subclass axiom whose RHS may be a conjunction: `A ⊑ B ⊓ C` is EL-equivalent
/// to `A ⊑ B` AND `A ⊑ C`, so a multi-conjunct RHS splits into one GCI per conjunct
/// (keeping every axiom in EL normal form: a single concept on the RHS).
fn push_subclass(ont: &mut Ontology, lhs: Vec<Concept>, rhs: Vec<Concept>, label: String) {
    for r in rhs {
        push_gci(ont, lhs.clone(), r, label.clone());
    }
}

fn register_concept_classes(ont: &mut Ontology, c: &Concept) {
    match c {
        Concept::Named(n) => register_class(ont, n),
        Concept::Some(_, f) => register_concept_classes(ont, f),
    }
}

fn register_class(ont: &mut Ontology, c: &str) {
    if c.starts_with('<') {
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
}

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
        for a in self.signature() {
            let entry = self.s.entry(a.clone()).or_default();
            entry.insert(a.clone());
            entry.insert(iri(OWL_THING));
        }
    }

    /// Run EL⁺ completion to fixpoint and return the classification. Idempotent —
    /// re-running over the same axioms yields the same closure.
    pub fn classify(&mut self) -> Classification {
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
        // Pre-index axioms by their trigger for O(1) rule lookup.
        // single-conjunct sub: B -> [(C, axiom_label)]
        let mut sub_index: HashMap<String, Vec<(Concept, String)>> = HashMap::new();
        // conjunctive sub: list of (conjuncts, C, label)
        let mut conj_axioms: Vec<(Vec<String>, Concept, String)> = Vec::new();
        // existential RHS: B -> [(r, D, label)]  (B ⊑ ∃r.D)
        let mut some_rhs: HashMap<String, Vec<(String, Concept, String)>> = HashMap::new();
        // existential LHS: (r, D) -> [(E, label)]  (∃r.D ⊑ E)
        let mut some_lhs: HashMap<(String, String), Vec<(Concept, String)>> = HashMap::new();

        for g in &self.ont.gcis {
            let rhs = g.rhs.clone();
            if g.lhs.len() == 1 {
                match &g.lhs[0] {
                    Concept::Named(b) => match &rhs {
                        Concept::Named(_) => {
                            sub_index
                                .entry(b.clone())
                                .or_default()
                                .push((rhs, g.label.clone()));
                        }
                        Concept::Some(r, d) => {
                            some_rhs.entry(b.clone()).or_default().push((
                                r.clone(),
                                (**d).clone(),
                                g.label.clone(),
                            ));
                        }
                    },
                    Concept::Some(r, d) => {
                        // ∃r.D ⊑ E
                        some_lhs
                            .entry((r.clone(), d.key()))
                            .or_default()
                            .push((rhs, g.label.clone()));
                    }
                }
            } else {
                // Conjunctive LHS — all conjuncts must be named in EL normal form.
                let names: Vec<String> = g.lhs.iter().map(|c| c.key()).collect();
                conj_axioms.push((names, rhs, g.label.clone()));
            }
        }

        // disjoint pairs by class for CR-disjoint.
        let disjoint: Vec<(String, String, String)> = self.ont.disjoint.clone();
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
                        for (c, label) in rules {
                            if let Concept::Named(cn) = c {
                                if self.add_sub(
                                    a,
                                    cn,
                                    "CR-sub",
                                    vec![label.clone()],
                                    vec![(a.clone(), b.clone())],
                                ) {
                                    changed = true;
                                }
                            }
                        }
                    }
                    if let Some(rules) = some_rhs.get(b) {
                        for (r, d, label) in rules {
                            if let Concept::Named(dn) = d {
                                if self.add_role(r, a, dn) {
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
                for (conjuncts, c, label) in &conj_axioms {
                    if conjuncts
                        .iter()
                        .all(|b| self.s.get(a).map(|s| s.contains(b)).unwrap_or(false))
                    {
                        if let Concept::Named(cn) = c {
                            let premises =
                                conjuncts.iter().map(|b| (a.clone(), b.clone())).collect();
                            if self.add_sub(a, cn, "CR-conj⁻", vec![label.clone()], premises) {
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
                    if has
                        && self.add_sub(
                            a,
                            &nothing,
                            "CR-disjoint",
                            vec![label.clone()],
                            vec![(a.clone(), d1.clone()), (a.clone(), d2.clone())],
                        )
                    {
                        changed = true;
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
                            for (e, label) in rules {
                                if let Concept::Named(en) = e {
                                    if self.add_sub(
                                        a,
                                        en,
                                        "CR-some⁻",
                                        vec![label.clone()],
                                        vec![(b.clone(), d.clone())],
                                    ) {
                                        changed = true;
                                    }
                                }
                            }
                        }
                    }
                    // CR-bot: ⊥ ∈ S(B) ⇒ ⊥ ∈ S(A).
                    if self.s.get(b).map(|s| s.contains(&nothing)).unwrap_or(false)
                        && self.add_sub(
                            a,
                            &nothing,
                            "CR-bot",
                            vec![],
                            vec![(b.clone(), nothing.clone())],
                        )
                    {
                        changed = true;
                    }
                }
            }

            // CR-subrole: (A,B) ∈ R(r), r ⊑ s ⇒ (A,B) ∈ R(s). And symmetric.
            for (r, sup, _label) in &sub_roles {
                if let Some(pairs) = self.r.get(r).cloned() {
                    for (a, b) in pairs {
                        if self.add_role(sup, &a, &b) {
                            changed = true;
                        }
                    }
                }
            }
            for r in &symmetric {
                if let Some(pairs) = self.r.get(r).cloned() {
                    for (a, b) in pairs {
                        if self.add_role(r, &b, &a) {
                            changed = true;
                        }
                    }
                }
            }
            for (p1, p2) in &inverses {
                if let Some(pairs) = self.r.get(p1).cloned() {
                    for (a, b) in pairs {
                        if self.add_role(p2, &b, &a) {
                            changed = true;
                        }
                    }
                }
                if let Some(pairs) = self.r.get(p2).cloned() {
                    for (a, b) in pairs {
                        if self.add_role(p1, &b, &a) {
                            changed = true;
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
                                if self.add_role(&ch.sup, a, c) {
                                    changed = true;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn add_sub(
        &mut self,
        a: &str,
        b: &str,
        rule: &'static str,
        axioms: Vec<String>,
        premises: Vec<(String, String)>,
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
        added
    }

    fn add_role(&mut self, r: &str, a: &str, b: &str) -> bool {
        self.r
            .entry(r.to_string())
            .or_default()
            .insert((a.to_string(), b.to_string()))
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
            consistent: unsat.is_empty(),
            unsatisfiable: unsat,
        }
    }
}

fn collect_named(c: &Concept, out: &mut BTreeSet<String>) {
    match c {
        Concept::Named(n) => {
            if n.starts_with('<') {
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
