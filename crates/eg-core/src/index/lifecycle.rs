//! The user-managed index lifecycle (EH-352): one vocabulary every managed
//! index family reports in, whichever store maintains it.
//!
//! A managed index is one a user created and can drop — a maintained ANN index
//! over a SQL table column (`eg-query`'s maintained ANN authority) or an
//! edge-native vector/text index over a graph's edges (`eg-query`'s edge index,
//! registered here as a server index). Its lifecycle is the state machine
//! `requested -> backfilling -> active | blocked`:
//!
//! * `requested`: registered, no build has started yet;
//! * `backfilling`: the first generation is being built; queries take the
//!   family's bounded exact path meanwhile and never see a partial index;
//! * `active`: a generation is live (it may lag the source; `lag` says by how
//!   much, and the family scores every change since its build exactly);
//! * `blocked`: no generation serves and the last attempt failed, with a
//!   bounded, typed diagnostic.
//!
//! [`IndexManager`] answers the status of every managed index of a graph and
//! drops one by name, fenced, so a build in flight never activates after its
//! drop.

use super::IndexManager;

pub use eg_types::managed_index::{
    IndexBlock, IndexBlockReason, ManagedIndexFamily, ManagedIndexState, ManagedIndexStatus,
    ManagedIndexTarget, MAX_BLOCK_DETAIL_BYTES,
};

impl IndexManager {
    /// The status of every managed index registered on this graph, by name.
    pub fn managed_statuses(&self) -> Vec<ManagedIndexStatus> {
        let mut statuses: Vec<ManagedIndexStatus> = self
            .server_indexes
            .read()
            .iter()
            .filter_map(|index| index.managed_status())
            .collect();
        statuses.sort_by(|a, b| a.name.cmp(&b.name));
        statuses
    }

    /// Drop every managed index named `name`: each is retired first (a build in
    /// flight never activates), then unregistered. Returns how many were
    /// dropped.
    pub fn drop_managed(&self, name: &str) -> usize {
        let mut indexes = self.server_indexes.write();
        let before = indexes.len();
        indexes.retain(|index| {
            let named = index
                .managed_status()
                .is_some_and(|status| status.name == name);
            if named {
                index.retire();
            }
            !named
        });
        before - indexes.len()
    }

    /// Run `f` against the managed server index named `name`, holding the
    /// registry's read lock; `None` when no such index is registered.
    pub fn with_managed_index<F, R>(&self, name: &str, f: F) -> Option<R>
    where
        F: FnOnce(&dyn super::SecondaryIndex) -> R,
    {
        self.server_indexes
            .read()
            .iter()
            .find(|index| {
                index
                    .managed_status()
                    .is_some_and(|status| status.name == name)
            })
            .map(|index| f(index.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lifecycle_rule_is_requested_backfilling_active_or_blocked() {
        let block = IndexBlock::new(IndexBlockReason::BuildFailed, "boom");
        assert_eq!(
            ManagedIndexState::of(false, false, None),
            ManagedIndexState::Requested
        );
        assert_eq!(
            ManagedIndexState::of(false, true, None),
            ManagedIndexState::Backfilling
        );
        assert_eq!(
            ManagedIndexState::of(false, true, Some(&block)),
            ManagedIndexState::Blocked
        );
        assert_eq!(
            ManagedIndexState::of(true, true, Some(&block)),
            ManagedIndexState::Active,
            "a live generation serves whatever the last attempt did"
        );
    }

    #[test]
    fn a_diagnostic_is_bounded_on_a_character_boundary() {
        let long = "é".repeat(MAX_BLOCK_DETAIL_BYTES);
        let block = IndexBlock::new(IndexBlockReason::BuildBound, &long);
        assert!(block.detail.len() <= MAX_BLOCK_DETAIL_BYTES);
        assert!(long.starts_with(&block.detail));
    }
}
