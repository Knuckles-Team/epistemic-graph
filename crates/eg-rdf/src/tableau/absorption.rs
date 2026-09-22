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

use std::collections::HashMap;

use super::{Dl, DlOntology};

/// The TBox as the tableau consumes it, split by [`build_tbox`].
#[derive(Debug, Default)]
pub(super) struct Tbox {
    /// Constraints stamped into every node label on creation: `D` for `⊤ ⊑ D`, and the
    /// NNF of `¬C ⊔ D` for every GCI `C ⊑ D` whose left side is not a named class.
    pub(super) global: Vec<Dl>,
    /// Absorbed GCIs: `unfold[A]` holds every `D` with `A ⊑ D`, added to a label by
    /// the deterministic unfolding rule once `A` is in it.
    unfold: HashMap<String, Vec<Dl>>,
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
