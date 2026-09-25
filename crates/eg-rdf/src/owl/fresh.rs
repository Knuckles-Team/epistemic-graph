//! The saturation frontier: which `S(A)` members CR-sub and CR-some⁺ still have to
//! run over.
//!
//! Both rules read only the member `B` and the fixed axioms indexed for this
//! saturation, so re-running them over a member they already ran over re-derives
//! nothing. Every member is fresh at the start of a saturation (new axioms may
//! apply to any of them); afterwards a member is fresh again only when it is newly
//! added to `S(A)` or its confidence rose. A pass then costs the facts it derives,
//! not the whole closure, so the work of a completion is proportional to its
//! derivation steps -- which is what makes a step budget bound its time.

use super::Reasoner;

impl Reasoner {
    /// Record `b ∈ S(a)` as fresh when `changed`; returns `changed`.
    pub(super) fn note_fresh(&mut self, a: &str, b: &str, changed: bool) -> bool {
        if changed {
            let members = self.fresh.entry(a.to_string()).or_default();
            members.insert(b.to_string());
        }
        changed
    }
}
