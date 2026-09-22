//! ABox reasoning for the tableau: independent components and a search budget.
//!
//! **Components.** Individuals that no role assertion, `sameAs` or `differentFrom`
//! connects cannot constrain each other unless the TBox names individuals (a nominal).
//! Without nominals, the ABox is consistent iff each connected component is (a model is
//! the disjoint union of the components' models), and an individual's instance
//! memberships depend only on its own component. Deciding one completion graph per
//! component instead of one graph holding every individual is the standard ABox
//! partitioning optimisation; it is what makes the core corpus's 1 379 individuals
//! decidable at all. With a nominal anywhere, everything is one component.
//!
//! **Budget.** A [`SearchBudget`] is charged once per deterministic rule round and once
//! per explored branch, across every branch and component of one decision, so a
//! schema-entry check fails closed with [`BudgetExhausted`] instead of running for an
//! unbounded time (the tableau is exponential in the worst case).

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use oxrdf::Triple;

use super::{build_tbox, parse_dl_ontology, Completion, Dl, DlOntology, RoleInfo};
use crate::owl::{BudgetExhausted, DerivationBudget};

/// Search steps charged across every branch of one decision (shared by the clones of a
/// completion graph).
#[derive(Debug, Default)]
pub(super) struct SearchBudget {
    used: Cell<u64>,
    limit: Option<u64>,
    exhausted: Cell<bool>,
}

impl SearchBudget {
    fn limited(budget: DerivationBudget) -> Self {
        Self {
            limit: Some(budget.max_steps()),
            ..Self::default()
        }
    }

    /// Charge one search step; `false` once the budget is spent.
    pub(super) fn charge(&self) -> bool {
        if self.limit.is_some_and(|limit| self.used.get() >= limit) {
            self.exhausted.set(true);
            return false;
        }
        self.used.set(self.used.get() + 1);
        true
    }

    fn verdict(&self, max_steps: u64, consistent: bool) -> Result<bool, BudgetExhausted> {
        if self.exhausted.get() {
            return Err(BudgetExhausted { max_steps });
        }
        Ok(consistent)
    }
}

/// **Ontology consistency within a search budget** (TBox + ABox). `Err` when the
/// tableau needed more than `budget` search steps; no verdict is implied then.
pub fn is_consistent_within(
    ont: &DlOntology,
    budget: DerivationBudget,
) -> Result<bool, BudgetExhausted> {
    let meter = Rc::new(SearchBudget::limited(budget));
    let consistent = consistent_with(ont, &meter);
    meter.verdict(budget.max_steps(), consistent)
}

/// [`is_consistent_within`] over a parsed triple set: the full-ABox check run where
/// schema enters a graph (pack import, schema attach).
pub fn abox_consistency_within(
    triples: &[Triple],
    budget: DerivationBudget,
) -> Result<bool, BudgetExhausted> {
    is_consistent_within(&parse_dl_ontology(triples), budget)
}

/// Every component must be consistent; stops at the first that is not (or when the
/// budget is spent).
pub(super) fn consistent_with(ont: &DlOntology, meter: &Rc<SearchBudget>) -> bool {
    let parts = components(ont);
    if parts.is_empty() {
        return decide(ont, &BTreeSet::new(), meter).consistent;
    }
    parts
        .iter()
        .all(|part| decide(&part.ontology, &part.individuals, meter).consistent)
}

/// One independent part of the ABox: its individuals and the ontology restricted to
/// their assertions (the TBox is shared).
pub(super) struct Component {
    pub(super) individuals: BTreeSet<String>,
    pub(super) ontology: DlOntology,
}

/// The ABox's connected components, ordered by their least individual. Empty when the
/// ABox is. One component holding every individual when the ontology uses a nominal.
pub(super) fn components(ont: &DlOntology) -> Vec<Component> {
    let individuals = mentioned_individuals(ont);
    if individuals.is_empty() {
        return Vec::new();
    }
    if uses_nominals(ont) {
        return vec![restrict(ont, individuals)];
    }
    let mut parent: BTreeMap<String, String> =
        individuals.iter().map(|i| (i.clone(), i.clone())).collect();
    let links = ont
        .abox_roles
        .iter()
        .map(|(a, _, b)| (a, b))
        .chain(ont.same_as.iter().map(|(a, b)| (a, b)))
        .chain(ont.different_from.iter().map(|(a, b)| (a, b)));
    for (a, b) in links {
        let (ra, rb) = (root(&parent, a), root(&parent, b));
        let (keep, drop) = if ra <= rb { (ra, rb) } else { (rb, ra) };
        parent.insert(drop, keep);
    }
    let mut groups: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for individual in &individuals {
        groups
            .entry(root(&parent, individual))
            .or_default()
            .insert(individual.clone());
    }
    groups
        .into_values()
        .map(|members| restrict(ont, members))
        .collect()
}

fn root(parent: &BTreeMap<String, String>, individual: &str) -> String {
    let mut current = individual.to_string();
    while let Some(next) = parent.get(&current).filter(|next| **next != current) {
        current = next.clone();
    }
    current
}

fn mentioned_individuals(ont: &DlOntology) -> BTreeSet<String> {
    let mut inds = ont.individuals.clone();
    inds.extend(ont.abox_types.iter().map(|(a, _)| a.clone()));
    for (a, _, b) in &ont.abox_roles {
        inds.extend([a.clone(), b.clone()]);
    }
    for (a, b) in ont.same_as.iter().chain(&ont.different_from) {
        inds.extend([a.clone(), b.clone()]);
    }
    inds
}

fn uses_nominals(ont: &DlOntology) -> bool {
    ont.gcis
        .iter()
        .flat_map(|(c, d)| [c, d])
        .chain(ont.abox_types.iter().map(|(_, c)| c))
        .any(contains_nominal)
}

fn contains_nominal(concept: &Dl) -> bool {
    match concept {
        Dl::Nominal(_) => true,
        Dl::Not(inner)
        | Dl::Some(_, inner)
        | Dl::All(_, inner)
        | Dl::Min(_, _, inner)
        | Dl::Max(_, _, inner) => contains_nominal(inner),
        Dl::And(parts) | Dl::Or(parts) => parts.iter().any(contains_nominal),
        Dl::Top | Dl::Bottom | Dl::Atom(_) => false,
    }
}

/// The ontology with its ABox restricted to `members`.
fn restrict(ont: &DlOntology, members: BTreeSet<String>) -> Component {
    let keep = |a: &String| members.contains(a);
    let ontology = DlOntology {
        gcis: ont.gcis.clone(),
        sub_roles: ont.sub_roles.clone(),
        transitive: ont.transitive.clone(),
        classes: ont.classes.clone(),
        abox_types: ont
            .abox_types
            .iter()
            .filter(|(a, _)| keep(a))
            .cloned()
            .collect(),
        abox_roles: ont
            .abox_roles
            .iter()
            .filter(|(a, _, _)| keep(a))
            .cloned()
            .collect(),
        same_as: ont
            .same_as
            .iter()
            .filter(|(a, _)| keep(a))
            .cloned()
            .collect(),
        different_from: ont
            .different_from
            .iter()
            .filter(|(a, _)| keep(a))
            .cloned()
            .collect(),
        individuals: members.clone(),
    };
    Component {
        individuals: members,
        ontology,
    }
}

/// One decided component: its verdict and, when consistent, the clash-free complete
/// completion graph the search ended in, with each individual's node.
struct Decided {
    consistent: bool,
    model: Completion,
    node_of: HashMap<String, usize>,
}

impl Decided {
    /// Can `individual : class` be entailed? Not when the model is a complete
    /// clash-free completion whose node for `individual` lacks `class`: its canonical
    /// interpretation (an atom holds exactly where it is in the label) is a model of the
    /// component in which the individual is not a `class`. A graph cut off by the node
    /// cap is not complete and prunes nothing.
    fn may_entail(&self, individual: &str, class: &str) -> bool {
        let concept = super::named_concept(class);
        if concept == Dl::Top || self.model.nodes.len() >= super::NODE_CAP {
            return true;
        }
        let node = self.model.find(self.node_of[individual]);
        self.model.nodes[node].label.contains(&concept)
    }
}

/// Decide one component: one nominal root per individual (its asserted types, role
/// edges and same/different constraints), or — for an empty ABox — a single anonymous
/// `⊤` node, which detects a globally unsatisfiable TBox.
fn decide(ont: &DlOntology, individuals: &BTreeSet<String>, meter: &Rc<SearchBudget>) -> Decided {
    let mut comp = Completion::new(Rc::new(build_tbox(ont)), Rc::new(RoleInfo::build(ont)));
    comp.budget = Rc::clone(meter);
    let mut node_of: HashMap<String, usize> = HashMap::new();
    if individuals.is_empty() {
        comp.add_node(BTreeSet::from([Dl::Top]), BTreeSet::new(), None);
    }
    for ind in individuals {
        let node = comp.add_node(BTreeSet::new(), BTreeSet::from([ind.clone()]), None);
        node_of.insert(ind.clone(), node);
    }
    for (a, c) in &ont.abox_types {
        comp.add_label(node_of[a], c.clone().nnf());
    }
    for (a, r, b) in &ont.abox_roles {
        comp.add_edge(node_of[a], r.clone(), node_of[b]);
    }
    for (a, b) in &ont.same_as {
        comp.union(node_of[a], node_of[b]);
    }
    for (a, b) in &ont.different_from {
        comp.neq.push((node_of[a], node_of[b]));
    }
    let consistent = comp.expand();
    Decided {
        consistent,
        model: comp,
        node_of,
    }
}

/// Instance classification against `target_classes`, one component at a time: an
/// individual's memberships follow from its own component (the others are consistent
/// and unconnected to it). An inconsistent ontology entails every membership.
pub(super) fn classify_instances_for(
    ont: &DlOntology,
    target_classes: &BTreeSet<String>,
) -> BTreeMap<String, BTreeSet<String>> {
    let meter = Rc::default();
    let decided: Vec<(Component, Decided)> = components(ont)
        .into_iter()
        .map(|part| {
            let verdict = decide(&part.ontology, &part.individuals, &meter);
            (part, verdict)
        })
        .collect();
    let entails_everything = decided.iter().any(|(_, verdict)| !verdict.consistent);
    let mut out = BTreeMap::new();
    for (part, verdict) in &decided {
        for individual in part.individuals.intersection(&ont.individuals) {
            let classes: BTreeSet<String> = target_classes
                .iter()
                .filter(|class| {
                    entails_everything
                        || (verdict.may_entail(individual, class)
                            && super::is_instance(&part.ontology, individual, class))
                })
                .cloned()
                .collect();
            if !classes.is_empty() {
                out.insert(individual.clone(), classes);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn atom(local: &str) -> Dl {
        Dl::Atom(format!("<http://example.org/{local}>"))
    }

    fn ind(local: &str) -> String {
        format!("<http://example.org/{local}>")
    }

    /// Two unconnected individuals are two components; a role edge joins them.
    #[test]
    fn unconnected_individuals_are_decided_separately() {
        let mut ont = DlOntology::default();
        ont.abox_types.push((ind("a"), atom("A")));
        ont.abox_types.push((ind("b"), atom("B")));
        ont.abox_types.push((ind("c"), atom("C")));
        ont.abox_roles
            .push((ind("a"), "<http://example.org/r>".to_string(), ind("b")));
        let parts = components(&ont);
        let members: Vec<Vec<String>> = parts
            .iter()
            .map(|p| p.individuals.iter().cloned().collect())
            .collect();
        assert_eq!(members, vec![vec![ind("a"), ind("b")], vec![ind("c")]]);
        assert_eq!(parts[1].ontology.abox_types, vec![(ind("c"), atom("C"))]);
    }

    /// A clash inside one component makes the whole ABox inconsistent.
    #[test]
    fn one_inconsistent_component_makes_the_abox_inconsistent() {
        let mut ont = DlOntology::default();
        ont.gcis.push((atom("A"), atom("B").negate()));
        ont.abox_types.push((ind("x"), atom("C")));
        ont.abox_types.push((ind("y"), atom("A")));
        ont.abox_types.push((ind("y"), atom("B")));
        assert!(!super::super::is_consistent(&ont));
        ont.abox_types.pop();
        assert!(super::super::is_consistent(&ont));
    }

    /// A nominal in the TBox can identify individuals across components, so the ABox
    /// stays one component.
    #[test]
    fn a_nominal_keeps_the_abox_in_one_component() {
        let mut ont = DlOntology::default();
        ont.gcis.push((atom("A"), Dl::Nominal(ind("only"))));
        ont.abox_types.push((ind("x"), atom("A")));
        ont.abox_types.push((ind("y"), atom("A")));
        ont.different_from.push((ind("x"), ind("y")));
        assert_eq!(components(&ont).len(), 1);
        assert!(!super::super::is_consistent(&ont));
    }

    /// The budget fails closed with a typed outcome; enough budget decides.
    #[test]
    fn the_search_budget_fails_closed() {
        let mut ont = DlOntology::default();
        for i in 0..20 {
            ont.gcis.push((
                Dl::Or(vec![atom(&format!("P{i}")), atom(&format!("Q{i}"))]),
                atom(&format!("R{i}")),
            ));
            ont.abox_types
                .push((ind(&format!("i{i}")), atom(&format!("P{i}"))));
        }
        let exhausted = is_consistent_within(&ont, DerivationBudget::new(3)).unwrap_err();
        assert_eq!(exhausted, BudgetExhausted { max_steps: 3 });
        assert_eq!(
            is_consistent_within(&ont, DerivationBudget::new(1_000_000)),
            Ok(true)
        );
    }
}
