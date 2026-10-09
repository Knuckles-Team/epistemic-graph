//! The typed legal-option-set filter (EG-DECISION-ENGINE-R085.1).
//!
//! A caller-asserted candidate option is never trusted as-is: it reaches the
//! derived set handed to the scorer only if an admissibility predicate
//! accepts it. Here that predicate is a plain closure fixture,
//! `is_admissible`; the real predicate -- wired to a SHACL shape check plus a
//! bounded SPARQL shortlist query over the ontology -- is
//! EG-DECISION-ENGINE-R085.2. This module proves the contract at the type
//! level ahead of that wiring: `derive_legal_options` is the only path from a
//! candidate list to the derived set, and it can only narrow, never widen,
//! what the caller supplied.

/// One candidate option considered for derivation into the legal set.
///
/// Stands in for a caller-asserted option. The real derivation (`.2`) will
/// carry whatever fields a SHACL-shape / SPARQL-shortlist predicate needs;
/// here an `id` is enough to prove the filter contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateOption {
    pub id: String,
}

impl CandidateOption {
    /// A candidate option named `id`.
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

/// Derive the legal option set from `candidates`, keeping only those
/// `is_admissible` accepts, in input order.
///
/// `is_admissible` stands in for "passes SHACL shape + policy constraint".
/// A candidate the predicate rejects is dropped here and never appears in
/// the returned set -- so it never reaches the scorer -- regardless of
/// whether the caller asserted it.
pub fn derive_legal_options(
    candidates: &[CandidateOption],
    is_admissible: impl Fn(&CandidateOption) -> bool,
) -> Vec<CandidateOption> {
    candidates
        .iter()
        .filter(|candidate| is_admissible(candidate))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixture standing in for "passes SHACL shape + policy constraint".
    fn fixture_is_admissible(candidate: &CandidateOption) -> bool {
        candidate.id != "illegal-option"
    }

    #[test]
    fn a_caller_supplied_option_that_fails_the_admissibility_predicate_is_excluded() {
        let candidates = vec![
            CandidateOption::new("legal-option"),
            CandidateOption::new("illegal-option"),
        ];

        let derived = derive_legal_options(&candidates, fixture_is_admissible);

        assert_eq!(derived, vec![CandidateOption::new("legal-option")]);
        assert!(!derived.iter().any(|option| option.id == "illegal-option"));
    }

    #[test]
    fn an_admissible_option_passes_through_to_the_derived_set() {
        let candidates = vec![CandidateOption::new("legal-option")];

        let derived = derive_legal_options(&candidates, fixture_is_admissible);

        assert_eq!(derived, candidates);
    }
}
