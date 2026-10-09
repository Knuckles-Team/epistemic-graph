//! EG-DECISION-ENGINE-R075 (`.1` slice): component-id uniqueness within an
//! import scope.
//!
//! Every component id must be unique within its scope; a pack import that
//! carries two entries declaring the same id is rejected rather than letting
//! the second silently shadow the first. The other half of the requirement
//! -- refusing a decision-owned kind such as `DecisionRecord`, `DecisionHead`
//! or `DecisionPolicy` -- already holds structurally: [`super::index::PackEntryKind`]
//! has no variant for any of them, so no pack entry can even be constructed
//! with one. This module adds the missing, previously untested half: the
//! duplicate-id check itself, reusing the existing
//! [`super::result::PackViolationCode::DuplicateComponentId`] code. Wiring
//! this into the real import entry point is a later `.2` slice.

use super::result::PackViolationCode;

/// Reject an import whose component ids are not pairwise unique within the
/// scope they share, naming the existing [`PackViolationCode`] the import
/// result already carries for this case.
pub fn check_unique_component_ids<'a, I>(ids: I) -> Result<(), PackViolationCode>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut seen = std::collections::BTreeSet::new();
    for id in ids {
        if !seen.insert(id) {
            return Err(PackViolationCode::DuplicateComponentId);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_pairwise_unique_ids() {
        let ids = ["comp-a", "comp-b", "comp-c"];
        assert_eq!(check_unique_component_ids(ids), Ok(()));
    }

    #[test]
    fn refuses_a_duplicate_component_id() {
        let ids = ["comp-a", "comp-b", "comp-a"];
        assert_eq!(
            check_unique_component_ids(ids),
            Err(PackViolationCode::DuplicateComponentId)
        );
    }
}
