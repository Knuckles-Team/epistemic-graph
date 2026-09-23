//! TBox absorption for the tableau (Horrocks/Tobies, *Reasoning with Axioms: Theory
//! and Practice*, KR 2000).
//!
//! A GCI `A ⊑ D` whose left side is a named class is ABSORBED: the deterministic
//! lazy-unfolding rule adds `D` to a label only once `A` is in it. `⊤ ⊑ D` adds `D`
//! to every node directly. Only the remaining GCIs `C ⊑ D` are internalized as the
//! NNF of `¬C ⊔ D` and stamped into every node label on creation. Positive-only
//! unfolding of named-left axioms, combined with internalization of the rest, is
//! sound and complete.
//!
//! Absorption is required, not an optimization. Internalizing a named-left GCI puts
//! one `⊔` choice point per axiom into every node, so the depth of the search (and
//! of the `Completion::expand` recursion) grows with the size of the TBox, and
//! chronological backtracking over those irrelevant choices is exponential. On the
//! served core ontology that exhausted a 2 MiB thread stack during schema
//! composition, and with a larger stack the search did not finish.

use std::collections::{BTreeSet, HashMap};

use super::{Completion, Dl, DlOntology};

/// The TBox as the tableau consumes it, split by [`build_tbox`].
#[derive(Debug, Default)]
pub(super) struct Tbox {
    /// Constraints stamped into every node label on creation: `D` for `⊤ ⊑ D`, and the
    /// NNF of `¬C ⊔ D` for every GCI `C ⊑ D` whose left side is not a named class.
    pub(super) global: Vec<Dl>,
    /// Absorbed GCIs: `unfold[A]` holds every `D` with `A ⊑ D`, added to a label by
    /// the deterministic unfolding rule once `A` is in it.
    unfold: HashMap<String, Vec<Dl>>,
    /// Role-absorbed `rdfs:domain` (`∃p.⊤ ⊑ D`): `domain[p]` is added to the source of
    /// every `p`-edge (or sub-role edge).
    domain: HashMap<String, Vec<Dl>>,
    /// Role-absorbed `rdfs:range` (`⊤ ⊑ ∀p.R`): `range[p]` is added to the target of
    /// every `p`-edge (or sub-role edge).
    range: HashMap<String, Vec<Dl>>,
    /// `owl:AllDisjointClasses` groups of named classes (see `DlOntology::disjoint_groups`).
    groups: Vec<BTreeSet<Dl>>,
}

/// The concepts a label member deterministically adds to its own node: the conjuncts
/// of `C₁ ⊓ … ⊓ Cₙ`, and the absorbed right sides `D` of every `A ⊑ D` for a named `A`.
pub(super) fn deterministic_consequences<'a>(tbox: &'a Tbox, concept: &'a Dl) -> &'a [Dl] {
    match concept {
        Dl::And(conjuncts) => conjuncts,
        Dl::Atom(class) => tbox.unfold.get(class).map_or(&[], Vec::as_slice),
        Dl::Top
        | Dl::Bottom
        | Dl::Not(_)
        | Dl::Or(_)
        | Dl::Some(_, _)
        | Dl::All(_, _)
        | Dl::Min(_, _, _)
        | Dl::Max(_, _, _)
        | Dl::Nominal(_) => &[],
    }
}

/// Split the TBox for the tableau: absorb every GCI with a named-class left side into
/// the lazy-unfolding table, add `⊤ ⊑ D` directly, and internalize the rest as NNF
/// `¬C ⊔ D`.
pub(super) fn build_tbox(ont: &DlOntology) -> Tbox {
    let mut tbox = Tbox::default();
    for (role, class) in &ont.domains {
        tbox.domain
            .entry(role.clone())
            .or_default()
            .push(class.clone());
    }
    for (role, class) in &ont.ranges {
        tbox.range
            .entry(role.clone())
            .or_default()
            .push(class.clone());
    }
    tbox.groups = ont.disjoint_groups.clone();
    for (c, d) in &ont.gcis {
        match c {
            Dl::Atom(class) => tbox
                .unfold
                .entry(class.clone())
                .or_default()
                .push(d.clone().nnf()),
            Dl::Top => tbox.global.push(d.clone().nnf()),
            Dl::Bottom
            | Dl::Not(_)
            | Dl::And(_)
            | Dl::Or(_)
            | Dl::Some(_, _)
            | Dl::All(_, _)
            | Dl::Min(_, _, _)
            | Dl::Max(_, _, _)
            | Dl::Nominal(_) => tbox
                .global
                .push(Dl::Or(vec![c.clone().negate(), d.clone().nnf()]).nnf()),
        }
    }
    tbox
}

impl Tbox {
    /// Does `label` hold two members of one `owl:AllDisjointClasses` group?
    pub(super) fn violates_a_disjoint_group(&self, label: &BTreeSet<Dl>) -> bool {
        self.groups
            .iter()
            .any(|group| group.iter().filter(|c| label.contains(*c)).nth(1).is_some())
    }
}

/// Role absorption (EH-363). `∃p.⊤ ⊑ D` and `⊤ ⊑ ∀p.R` are deterministic in the
/// completion graph: an edge `x –e→ y` with `e ⊑* p` puts `D` into `L(x)` and `R` into
/// `L(y)`. Internalizing `∃p.⊤ ⊑ D` instead would put one `⊔` per domain axiom into
/// every label — the choice-point explosion absorption exists to avoid.
impl Completion {
    pub(super) fn step_role_absorption(&mut self) -> bool {
        if self.tbox.domain.is_empty() && self.tbox.range.is_empty() {
            return false;
        }
        let mut changed = false;
        for i in self.reps() {
            for (edge, target) in self.out_edges(i) {
                let (domains, ranges) = self.role_consequences(&edge);
                for class in domains {
                    changed |= self.add_label(i, class);
                }
                for class in ranges {
                    if self.add_label(target, class) {
                        self.inherit_deps(target, i);
                        changed = true;
                    }
                }
            }
        }
        changed
    }

    /// The domain and range classes an `edge`-role edge implies, over `edge` and all its
    /// super-roles.
    fn role_consequences(&self, edge: &str) -> (Vec<Dl>, Vec<Dl>) {
        let supers = self.roles.super_roles.get(edge).into_iter().flatten();
        let roles: Vec<&str> = std::iter::once(edge)
            .chain(supers.map(String::as_str))
            .collect();
        let collect = |table: &HashMap<String, Vec<Dl>>| -> Vec<Dl> {
            roles
                .iter()
                .filter_map(|role| table.get(*role))
                .flatten()
                .cloned()
                .collect()
        };
        (collect(&self.tbox.domain), collect(&self.tbox.range))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{is_consistent, is_subsumed};
    use super::*;

    /// A named-class axiom `A ⊑ B` is decided by lazy unfolding, never by a `¬A ⊔ B`
    /// choice point in every node. Internalizing this chain made the search recurse
    /// once per axiom: thousands of nested `expand` frames overflowed the 2 MiB test
    /// thread.
    #[test]
    fn a_long_named_subclass_chain_is_decided_by_lazy_unfolding() {
        const LENGTH: usize = 4_000;
        let class = |i: usize| format!("<http://example.org/C{i:05}>");
        let mut o = DlOntology::default();
        for i in 0..LENGTH {
            o.gcis.push((Dl::Atom(class(i)), Dl::Atom(class(i + 1))));
        }
        assert!(is_subsumed(&o, &class(0), &class(LENGTH)));
        assert!(!is_subsumed(&o, &class(LENGTH), &class(0)));
        assert!(is_consistent(&o));
    }

    /// Only named-left GCIs are absorbed; `⊤ ⊑ D` is global as `D` itself, and a
    /// complex left side stays internalized as `¬C ⊔ D`.
    #[test]
    fn only_named_left_axioms_are_absorbed() {
        let a = Dl::Atom("<http://example.org/A>".to_string());
        let b = Dl::Atom("<http://example.org/B>".to_string());
        let c = Dl::Atom("<http://example.org/C>".to_string());
        let mut o = DlOntology::default();
        o.gcis.push((a.clone(), b.clone()));
        o.gcis.push((Dl::Top, c.clone()));
        o.gcis.push((Dl::Or(vec![b.clone(), c.clone()]), a.clone()));
        let tbox = build_tbox(&o);
        assert_eq!(
            deterministic_consequences(&tbox, &a),
            std::slice::from_ref(&b)
        );
        assert!(deterministic_consequences(&tbox, &b).is_empty());
        let negated = Dl::And(vec![b.negate(), c.clone().negate()]);
        assert_eq!(tbox.global, vec![c, Dl::Or(vec![negated, a])]);
    }
}
