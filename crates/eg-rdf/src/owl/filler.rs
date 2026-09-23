//! Sound handling of role-successor constraints in the EL⁺ completion (CR-filler).
//!
//! The completion's role relation `R(r) ∋ (A, B)` means `A ⊑ ∃r.B`: every `A` has an
//! `r`-successor in `B`. A constraint on `r`-successors — a range `range(r, D)`, or a
//! universal `C ⊑ ∀r.D` with `A ⊑ C` — tells us that THIS successor is also a `D`,
//! i.e. `A ⊑ ∃r.(B ⊓ D)`. It does NOT tell us `B ⊑ D`: the `B`s that are not an
//! `r`-successor of some `A` are unconstrained. Deriving `B ⊑ D` (or, equivalently,
//! inverting `R` to `B ⊑ ∃r⁻.A` and lifting the range as a domain of `r⁻`) is unsound:
//! on the shipped core ontology it derived `BFO:Entity ⊑ kg:Code` from
//! `Module ⊑ ∃partOf.Entity`, `partOf ⊑ dependsOn` and `range(dependsOn, Code)`, and
//! the root class collapsed to `owl:Nothing` (EH-356).
//!
//! The sound rule is the range normalisation of Baader, Brandt & Lutz, *Pushing the EL
//! Envelope Further* (OWLED 2008): the constrained successor is a REFINED filler
//! `B ⊓ D₁ ⊓ … ⊓ Dₙ`, a completion-internal class whose subsumers are those of `B`
//! plus every `Dᵢ`. `(A, B ⊓ D)` joins `R(r)` beside `(A, B)`, so CR-some⁻ and CR-bot
//! see the refined successor while `B` keeps exactly the subsumers the axioms entail.
//! Refined fillers are keyed by their base class and constraint set, so there are
//! finitely many and the completion terminates.
//!
//! Inverse and symmetric declarations are handled the same way: they only transfer
//! domains and ranges (`domain(q) = range(q⁻)`, and a symmetric role's domain is also
//! its range), never by inverting `R`.

use std::collections::{BTreeMap, BTreeSet};

use super::{iri, short, Justification, Ontology, Reasoner, OWL_THING};

/// Marks a completion-internal refined filler. Canonical class ids start with `<`
/// (IRIs) or `{` (nominal tokens), so `[` cannot collide with an ontology class.
const REFINED_PREFIX: char = '[';

/// A constraint on the successors of a role: the class they must belong to, the
/// axiom label that says so, and, for a universal, the class the SOURCE must have.
#[derive(Clone, Debug)]
struct SuccessorConstraint {
    class: String,
    label: String,
    /// `None` for a range; `Some(C)` for `C ⊑ ∀r.class`.
    when_source_is: Option<String>,
    conf: f64,
}

/// Every successor constraint, indexed by the role it is declared on, plus the role
/// hierarchy needed to apply a super-role's constraints to its sub-roles.
#[derive(Debug, Default)]
pub(super) struct FillerRules {
    /// `super_roles[r]` — every `s` with `r ⊑* s`, including `r` itself.
    super_roles: BTreeMap<String, BTreeSet<String>>,
    by_role: BTreeMap<String, Vec<SuccessorConstraint>>,
}

impl FillerRules {
    pub(super) fn build(ont: &Ontology) -> Self {
        let mut rules = Self {
            super_roles: super_role_closure(ont),
            by_role: BTreeMap::new(),
        };
        for (role, class, label) in effective_ranges(ont) {
            rules.push(role, class, label, None, 1.0);
        }
        for (sub, role, class, label, conf) in &ont.all_values {
            rules.push(
                role.clone(),
                class.clone(),
                label.clone(),
                Some(sub.clone()),
                *conf,
            );
        }
        rules
    }

    fn push(
        &mut self,
        role: String,
        class: String,
        label: String,
        when_source_is: Option<String>,
        conf: f64,
    ) {
        self.by_role
            .entry(role)
            .or_default()
            .push(SuccessorConstraint {
                class,
                label,
                when_source_is,
                conf,
            });
    }

    /// The constraints that apply to an `role`-successor of a member of `a`: those
    /// declared on `role` or any super-role, whose source condition `a` satisfies.
    fn applicable<'a>(
        &'a self,
        role: &'a str,
        subsumers_of_a: &'a BTreeSet<String>,
    ) -> impl Iterator<Item = &'a SuccessorConstraint> + 'a {
        let own = std::iter::once(role.to_string());
        let supers = self.super_roles.get(role).into_iter().flatten().cloned();
        own.chain(supers)
            .collect::<BTreeSet<String>>()
            .into_iter()
            .filter_map(move |r| self.by_role.get(&r))
            .flatten()
            .filter(move |c| {
                c.when_source_is
                    .as_ref()
                    .is_none_or(|sub| subsumers_of_a.contains(sub))
            })
    }
}

/// `r ⊑* s` for every declared role inclusion (and both directions of an
/// equivalence, which the parser already records as two inclusions).
fn super_role_closure(ont: &Ontology) -> BTreeMap<String, BTreeSet<String>> {
    let mut direct: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (sub, sup, _, _) in &ont.sub_roles {
        direct.entry(sub.clone()).or_default().insert(sup.clone());
    }
    let mut closure = BTreeMap::new();
    for role in direct.keys() {
        let mut seen = BTreeSet::new();
        let mut stack = vec![role.clone()];
        while let Some(next) = stack.pop() {
            for sup in direct.get(&next).into_iter().flatten() {
                if seen.insert(sup.clone()) {
                    stack.push(sup.clone());
                }
            }
        }
        closure.insert(role.clone(), seen);
    }
    closure
}

/// The roles a role is inverse to: declared `owl:inverseOf` pairs in both directions,
/// and the role itself when it is symmetric.
fn inverse_partners(ont: &Ontology) -> Vec<(String, String)> {
    let declared = ont
        .inverses
        .iter()
        .flat_map(|(p, q)| [(p.clone(), q.clone()), (q.clone(), p.clone())]);
    let symmetric = ont.symmetric.iter().map(|r| (r.clone(), r.clone()));
    declared.chain(symmetric).collect()
}

/// Declarations of a role `q`, transferred to each inverse partner `p` of `q`: every
/// `(q, D)` in `declared` becomes `(p, D, label)`.
fn transfer_to_partners(
    ont: &Ontology,
    declared: &[(String, String)],
    describe: fn(&str, &str, &str) -> String,
) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for (p, q) in inverse_partners(ont) {
        for (_, d) in declared.iter().filter(|(r, _)| *r == q) {
            out.push((p.clone(), d.clone(), describe(&p, d, &q)));
        }
    }
    out
}

/// Asserted ranges plus the ranges transferred from the domain of an inverse partner:
/// `domain(q, D)` with `q = p⁻` means every `p`-successor is a `D`.
fn effective_ranges(ont: &Ontology) -> Vec<(String, String, String)> {
    let mut out: Vec<_> = ont
        .ranges
        .iter()
        .map(|(r, d)| {
            (
                r.clone(),
                d.clone(),
                format!("range({}) = {}", short(r), short(d)),
            )
        })
        .collect();
    out.extend(transfer_to_partners(ont, &ont.domains, |p, d, q| {
        format!(
            "range({}) = {} (from dom({}))",
            short(p),
            short(d),
            short(q)
        )
    }));
    out
}

/// The domains transferred from the range of an inverse partner: `range(q, D)` with
/// `q = p⁻` means every `p`-SOURCE is a `D`. Lifted as `∃p.⊤ ⊑ D` beside the asserted
/// domains.
pub(super) fn transferred_domains(ont: &Ontology) -> Vec<(String, String, String)> {
    transfer_to_partners(ont, &ont.ranges, |p, d, q| {
        format!(
            "dom({}) = {} (from range({}))",
            short(p),
            short(d),
            short(q)
        )
    })
}

/// The refined fillers created so far: name → (base class, constraint classes).
#[derive(Clone, Debug, Default)]
pub(super) struct RefinedFillers {
    by_name: BTreeMap<String, (String, BTreeSet<String>)>,
}

impl RefinedFillers {
    /// The base class and constraint set of `filler` (`(filler, ∅)` when it is an
    /// ordinary class).
    fn origin(&self, filler: &str) -> (String, BTreeSet<String>) {
        self.by_name
            .get(filler)
            .cloned()
            .unwrap_or_else(|| (filler.to_string(), BTreeSet::new()))
    }
}

fn is_refined(class: &str) -> bool {
    class.starts_with(REFINED_PREFIX)
}

fn refined_name(base: &str, constraints: &BTreeSet<String>) -> String {
    let parts: Vec<&str> = std::iter::once(base)
        .chain(constraints.iter().map(String::as_str))
        .collect();
    format!("{REFINED_PREFIX}{}]", parts.join(" ⊓ "))
}

/// CR-filler over the whole role relation. Returns whether anything was added.
pub(super) fn apply_filler_refinement(reasoner: &mut Reasoner, rules: &FillerRules) -> bool {
    if rules.by_role.is_empty() {
        return false;
    }
    let pairs: Vec<(String, String, String)> = reasoner
        .r
        .iter()
        .flat_map(|(role, pairs)| {
            pairs
                .iter()
                .map(move |(a, b)| (role.clone(), a.clone(), b.clone()))
        })
        .collect();
    let mut changed = false;
    for (role, a, b) in &pairs {
        changed |= refine_pair(reasoner, rules, role, a, b);
    }
    changed
}

/// The constraints on `(a, b) ∈ R(role)` that `S(b)` does not already satisfy.
fn missing_constraints(
    reasoner: &Reasoner,
    rules: &FillerRules,
    role: &str,
    a: &str,
    b: &str,
) -> Vec<SuccessorConstraint> {
    let empty = BTreeSet::new();
    let s_a = reasoner.s.get(a).unwrap_or(&empty);
    let s_b = reasoner.s.get(b).unwrap_or(&empty);
    rules
        .applicable(role, s_a)
        .filter(|c| !s_b.contains(&c.class))
        .cloned()
        .collect()
}

fn refine_pair(reasoner: &mut Reasoner, rules: &FillerRules, role: &str, a: &str, b: &str) -> bool {
    let missing = missing_constraints(reasoner, rules, role, a, b);
    if missing.is_empty() {
        return false;
    }
    let classes: BTreeSet<String> = missing.iter().map(|c| c.class.clone()).collect();
    let Some(filler) = refined_filler(reasoner, b, &classes) else {
        return false;
    };
    let conf = missing
        .iter()
        .fold(reasoner.cur_rconf(role, a, b), |acc, c| {
            let source = c
                .when_source_is
                .as_deref()
                .map_or(1.0, |sub| reasoner.cur_conf(a, sub));
            acc * source * c.conf
        });
    if !reasoner.add_role_weighted(role, a, &filler, conf) {
        return false;
    }
    reasoner
        .just
        .entry((format!("R:{role}"), format!("{a}->{filler}")))
        .or_insert(Justification {
            rule: "CR-filler",
            axioms: missing.into_iter().map(|c| c.label).collect(),
            premises: vec![(a.to_string(), b.to_string())],
        });
    true
}

/// The refined filler `base(b) ⊓ constraints(b) ⊓ classes`, created (and charged as
/// one derivation step) on first use. Its subsumers start as `S(b)` plus `classes`, so
/// every constraint that led to it is already satisfied. `None` when the derivation
/// budget refuses a new filler.
fn refined_filler(reasoner: &mut Reasoner, b: &str, classes: &BTreeSet<String>) -> Option<String> {
    let (base, mut constraints) = reasoner.refined.origin(b);
    constraints.extend(classes.iter().cloned());
    let name = refined_name(&base, &constraints);
    if reasoner.s.contains_key(&name) {
        return Some(name);
    }
    if !reasoner.meter.charge() {
        return None;
    }
    let inherited: BTreeSet<String> = reasoner.s.get(b).cloned().unwrap_or_default();
    let mut subsumers = constraints.clone();
    subsumers.extend([name.clone(), iri(OWL_THING)]);
    if reasoner.weighted {
        for class in &subsumers {
            reasoner.conf.insert((name.clone(), class.clone()), 1.0);
        }
        for class in &inherited {
            let conf = reasoner.cur_conf(b, class);
            reasoner.conf.insert((name.clone(), class.clone()), conf);
        }
    }
    subsumers.extend(inherited);
    reasoner.s.insert(name.clone(), subsumers);
    reasoner
        .refined
        .by_name
        .insert(name.clone(), (base, constraints));
    Some(name)
}

/// The classification as the rest of the system sees it: named classes only. A
/// refined filler is unsatisfiable only if some named class reaches it through `R`,
/// and CR-bot has then already made that class unsatisfiable, so hiding it loses no
/// verdict.
pub(super) struct NamedView {
    pub(super) subsumers: BTreeMap<String, BTreeSet<String>>,
    pub(super) roles: BTreeMap<String, BTreeSet<(String, String)>>,
    pub(super) confidence: BTreeMap<(String, String), f64>,
}

impl NamedView {
    pub(super) fn of(reasoner: &Reasoner) -> Self {
        let subsumers = reasoner
            .s
            .iter()
            .filter(|(class, _)| !is_refined(class))
            .map(|(class, supers)| (class.clone(), supers.clone()))
            .collect();
        let roles = reasoner
            .r
            .iter()
            .map(|(role, pairs)| {
                let named = pairs
                    .iter()
                    .filter(|(a, b)| !is_refined(a) && !is_refined(b))
                    .cloned()
                    .collect();
                (role.clone(), named)
            })
            .collect();
        let confidence = if reasoner.weighted {
            reasoner
                .conf
                .iter()
                .filter(|((a, _), _)| !is_refined(a))
                .map(|(key, conf)| (key.clone(), *conf))
                .collect()
        } else {
            BTreeMap::new()
        };
        Self {
            subsumers,
            roles,
            confidence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::Reasoner;
    use crate::mapping::parse_turtle;

    const PREFIXES: &str = "@prefix ex: <http://example.org/> .\n\
        @prefix owl: <http://www.w3.org/2002/07/owl#> .\n\
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n";

    fn ex(local: &str) -> String {
        format!("<http://example.org/{local}>")
    }

    fn classify(body: &str) -> super::super::Classification {
        let triples = parse_turtle(&format!("{PREFIXES}{body}")).unwrap();
        Reasoner::from_triples(&triples).classify()
    }

    /// EH-356, reduced from the shipped core ontology: a range on a SUPER-role of the
    /// restricted role refines the restriction's filler; it never pushes the range onto
    /// the filler class. Deriving `Entity ⊑ Code` here made the root unsatisfiable.
    #[test]
    fn a_range_refines_the_successor_not_the_filler_class() {
        let cls = classify(
            "ex:Module rdfs:subClassOf [ owl:onProperty ex:partOf ; owl:someValuesFrom ex:Entity ] .\n\
             ex:partOf rdfs:subPropertyOf ex:dependsOn .\n\
             ex:dependsOn rdfs:range ex:Code .\n\
             ex:Person owl:disjointWith ex:Code .\n\
             ex:Person rdfs:subClassOf ex:Entity .\n\
             [ owl:onProperty ex:partOf ; owl:someValuesFrom ex:Code ] rdfs:subClassOf ex:PartOfCode .\n",
        );
        assert!(cls.consistent, "unsatisfiable: {:?}", cls.unsatisfiable);
        assert!(!cls.entails_subclass(&ex("Entity"), &ex("Code")));
        assert!(!cls.entails_subclass(&ex("Person"), &ex("Code")));
        assert!(
            cls.entails_subclass(&ex("Module"), &ex("PartOfCode")),
            "the partOf-successor of a Module is an Entity ⊓ Code"
        );
        assert!(cls.subsumers.keys().all(|class| !class.starts_with('[')));
        assert!(cls
            .roles
            .values()
            .flatten()
            .all(|(a, b)| !a.starts_with('[') && !b.starts_with('[')));
    }

    /// An inverse declaration transfers the partner's domain as a range (and its range
    /// as a domain); it never inverts the existential.
    #[test]
    fn inverse_roles_transfer_domain_and_range_only() {
        let cls = classify(
            "ex:hasPart owl:inverseOf ex:partOf .\n\
             ex:partOf rdfs:domain ex:Component .\n\
             ex:partOf rdfs:range ex:Assembly .\n\
             ex:Car rdfs:subClassOf [ owl:onProperty ex:hasPart ; owl:someValuesFrom ex:Wheel ] .\n\
             [ owl:onProperty ex:hasPart ; owl:someValuesFrom ex:Component ] rdfs:subClassOf ex:HasComponent .\n",
        );
        assert!(cls.consistent);
        assert!(cls.entails_subclass(&ex("Car"), &ex("HasComponent")));
        assert!(cls.entails_subclass(&ex("Car"), &ex("Assembly")));
        assert!(!cls.entails_subclass(&ex("Wheel"), &ex("Component")));
        let has_part = cls.roles.get(&ex("hasPart")).cloned().unwrap_or_default();
        assert!(has_part.contains(&(ex("Car"), ex("Wheel"))));
        assert!(cls
            .roles
            .get(&ex("partOf"))
            .is_none_or(|pairs| !pairs.contains(&(ex("Wheel"), ex("Car")))));
    }

    /// A symmetric role's domain is also its range.
    #[test]
    fn a_symmetric_domain_constrains_the_successor() {
        let cls = classify(
            "ex:marriedTo rdf:type owl:SymmetricProperty .\n\
             ex:marriedTo rdfs:domain ex:Adult .\n\
             ex:Spouse rdfs:subClassOf [ owl:onProperty ex:marriedTo ; owl:someValuesFrom ex:Person ] .\n\
             [ owl:onProperty ex:marriedTo ; owl:someValuesFrom ex:Adult ] rdfs:subClassOf ex:MarriedToAdult .\n",
        );
        assert!(cls.entails_subclass(&ex("Spouse"), &ex("Adult")));
        assert!(cls.entails_subclass(&ex("Spouse"), &ex("MarriedToAdult")));
        assert!(!cls.entails_subclass(&ex("Person"), &ex("Adult")));
    }

    /// A contradiction between a range and the filler class is an unsatisfiable
    /// restricted class — found through the refined filler by CR-bot.
    #[test]
    fn a_range_disjoint_from_the_filler_makes_the_restricted_class_unsatisfiable() {
        let cls = classify(
            "ex:Review rdfs:subClassOf [ owl:onProperty ex:derivedFrom ; owl:someValuesFrom ex:Decision ] .\n\
             ex:derivedFrom rdfs:range ex:Specification .\n\
             ex:Decision owl:disjointWith ex:Specification .\n",
        );
        assert!(!cls.consistent);
        assert_eq!(
            cls.unsatisfiable,
            std::collections::BTreeSet::from([ex("Review")])
        );
    }
}
