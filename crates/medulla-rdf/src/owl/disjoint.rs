//! `owl:AllDisjointClasses` — one reader for both engines (EH-363).
//!
//! `_:x a owl:AllDisjointClasses ; owl:members (C₁ … Cₙ)` says the `Cᵢ` are pairwise
//! disjoint. The EL⁺/RL completion and the tableau both expand it pairwise, from the same
//! member lists, so they cannot disagree about which classes are disjoint. The tableau
//! used to ignore the axiom entirely (and read its node as an individual typed
//! `owl:AllDisjointClasses`), so the full-ABox check where schema enters a graph admitted
//! an individual in two members of the core's BFO and agent partitions.
//!
//! Pairwise expansion is quadratic in `n`, so the supported profile bounds `n`.

use oxrdf::{Term, Triple};

use super::{
    parse_rdf_list, term_key, TripleIndex, OWL_ALL_DISJOINT_CLASSES, OWL_MEMBERS, RDF_TYPE,
};

/// The most members one `owl:AllDisjointClasses` axiom may list (2 016 pairs).
pub(crate) const MAX_ALL_DISJOINT_MEMBERS: usize = 64;

/// The member lists of the `owl:AllDisjointClasses` axiom at `subject`.
pub(crate) fn member_lists(idx: &TripleIndex, subject: &str) -> Vec<Vec<Term>> {
    idx.objects(subject, OWL_MEMBERS)
        .iter()
        .map(|head| parse_rdf_list(idx, head))
        .collect()
}

/// Every unordered pair `(items[i], items[j])`, `i < j`, in order.
pub(crate) fn unordered_pairs<T: Clone>(items: &[T]) -> Vec<(T, T)> {
    let mut pairs = Vec::new();
    for (i, left) in items.iter().enumerate() {
        for right in &items[i + 1..] {
            pairs.push((left.clone(), right.clone()));
        }
    }
    pairs
}

/// Refuse an `owl:AllDisjointClasses` axiom with more than [`MAX_ALL_DISJOINT_MEMBERS`]
/// members, whose pairwise expansion would be unbounded.
pub(super) fn validate_member_bounds(triples: &[Triple]) -> Result<(), String> {
    let axioms: Vec<String> = triples
        .iter()
        .filter(|t| {
            t.predicate.as_str() == RDF_TYPE
                && matches!(&t.object, Term::NamedNode(n) if n.as_str() == OWL_ALL_DISJOINT_CLASSES)
        })
        .map(|t| term_key(&t.subject.clone().into()))
        .collect();
    if axioms.is_empty() {
        return Ok(());
    }
    let idx = TripleIndex::build(triples);
    for subject in &axioms {
        let largest = member_lists(&idx, subject)
            .iter()
            .map(Vec::len)
            .max()
            .unwrap_or(0);
        if largest > MAX_ALL_DISJOINT_MEMBERS {
            return Err(format!(
                "OWL_UNSUPPORTED_CONSTRUCT: owl:AllDisjointClasses with {largest} members exceeds {MAX_ALL_DISJOINT_MEMBERS}"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::parse_turtle;

    #[test]
    fn pairs_are_every_unordered_pair_once() {
        assert_eq!(unordered_pairs(&[1, 2, 3]), vec![(1, 2), (1, 3), (2, 3)]);
        assert!(unordered_pairs::<u8>(&[7]).is_empty());
    }

    #[test]
    fn an_oversized_all_disjoint_axiom_is_refused() {
        let members = |n: usize| {
            (0..n)
                .map(|i| format!("ex:C{i}"))
                .collect::<Vec<_>>()
                .join(" ")
        };
        let doc = |n: usize| {
            format!(
                "@prefix ex: <http://example.org/> .\n\
                 @prefix owl: <http://www.w3.org/2002/07/owl#> .\n\
                 [] a owl:AllDisjointClasses ; owl:members ( {} ) .",
                members(n)
            )
        };
        let within = parse_turtle(&doc(MAX_ALL_DISJOINT_MEMBERS)).unwrap();
        assert_eq!(validate_member_bounds(&within), Ok(()));
        let over = parse_turtle(&doc(MAX_ALL_DISJOINT_MEMBERS + 1)).unwrap();
        assert!(validate_member_bounds(&over)
            .unwrap_err()
            .starts_with("OWL_UNSUPPORTED_CONSTRUCT"));
    }
}
