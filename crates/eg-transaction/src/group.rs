//! Scope-group admission: N scoped mutations plus the store's control rows in
//! ONE physical write transaction (RF-RULING-008).
//!
//! Every other layout admits exactly one scope per `begin_write`, and that
//! bound is the tenant confinement the kernel reviews closed. The graph shard
//! is the one owner whose authoritative writer folds many scopes into one
//! commit by design: the adaptive linger coalesces N graph-scoped batches into
//! a single fsync ("one fsync, N notified"), and the Raft log and meta rows —
//! keyed by Raft group, not by graph — ride that same fsync so graph state and
//! consensus state become durable together, as do the cross-shard 2PC records
//! and the cross-modal series rows.
//!
//! A group does not weaken the per-scope bound; it repeats it. Each member is
//! an ordinary [`AdmittedMutation`] built on a capability that binds exactly
//! one serving scope, so each member gets the same `ScopedTableMut` row ACL,
//! the same version, fence, class row, idempotency check and receipt as a sole
//! writer, through the same code paths. What the group adds is that the
//! transaction underneath them is one, that no member can end it, and that a
//! member admitted for a graph scope and the control member admitted for the
//! file's own scope reach disjoint owner tables.

use crate::admitted::AdmittedMutation;
use crate::Begin;
use eg_storage::{MutationClass, OwnedStoreHandle, OwnerDomain};
use eg_types::{MutationBatch, MutationScopeIdentity};

/// One member of a scope group: the bound scope it is admitted for, the batch
/// it applies, and the class that batch is admitted under.
pub struct ScopedIntent<'i, D: OwnerDomain> {
    pub(crate) owner: &'i OwnedStoreHandle<D>,
    pub(crate) batch: &'i MutationBatch,
    pub(crate) class: MutationClass,
}

impl<'i, D: OwnerDomain> ScopedIntent<'i, D> {
    /// A caller-originated batch on this scope.
    pub fn operation(owner: &'i OwnedStoreHandle<D>, batch: &'i MutationBatch) -> Self {
        Self {
            owner,
            batch,
            class: MutationClass::Operation,
        }
    }

    /// An owner-maintenance batch on this scope (RF-RULING-005).
    pub fn maintenance(owner: &'i OwnedStoreHandle<D>, batch: &'i MutationBatch) -> Self {
        Self {
            owner,
            batch,
            class: MutationClass::Maintenance,
        }
    }

    pub fn scope(&self) -> &MutationScopeIdentity {
        self.owner.identity()
    }
}

/// N scoped admitted mutations over one physical write transaction.
///
/// Member 0 is always the **control** member — the store's own file-wide scope,
/// which owns the Raft log and meta rows, the cross-shard records and the other
/// key spaces that belong to the file rather than to any one graph. Members 1..
/// are the scoped batches, in the order they were admitted.
///
/// The group is minted only by [`crate::MutationKernel::admit_group`] and
/// hands out members by shared reference only, so no member outlives it and no
/// raw transaction is reachable through it.
pub struct AdmittedGroup<'a, D: OwnerDomain> {
    members: Vec<AdmittedMutation<'a, D>>,
    begins: Vec<Begin>,
}

impl<'a, D: OwnerDomain> AdmittedGroup<'a, D> {
    pub(crate) fn new(members: Vec<AdmittedMutation<'a, D>>, begins: Vec<Begin>) -> Self {
        Self { members, begins }
    }

    /// Number of admitted members, control member included.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// The store's own control member.
    pub fn control(&self) -> &AdmittedMutation<'a, D> {
        &self.members[0]
    }

    /// One admitted member by index; `0` is the control member.
    pub fn member(&self, index: usize) -> Result<&AdmittedMutation<'a, D>, String> {
        self.members
            .get(index)
            .ok_or_else(|| "admitted scope group has no such member".to_string())
    }

    /// What admission decided for one member: apply, or replay a terminal
    /// receipt whose rows are already durable.
    ///
    /// A group containing a replayed member IS committed: the other members'
    /// rows are real, and one retry among N is the coalescer's ordinary case.
    /// The replayed member is marked terminal at admission, so it writes no
    /// rows — `owner_rows` and `finish` both refuse it — and its scope's
    /// version, fence and receipt are left exactly as its first apply left
    /// them.
    pub fn begun(&self, index: usize) -> Result<&Begin, String> {
        self.begins
            .get(index)
            .ok_or_else(|| "admitted scope group has no such member".to_string())
    }

    /// The exact serving scope of one member.
    pub fn scope(&self, index: usize) -> Result<&MutationScopeIdentity, String> {
        self.member(index).map(AdmittedMutation::scope)
    }

    /// End the shared transaction once every member has been dropped.
    ///
    /// The last member is the one that ends it, and it can only do so after the
    /// rest are gone: the shared handle unwraps for exactly one holder. So a
    /// group cannot commit while any member is still able to write into it.
    pub(crate) fn end(self, commit: bool) -> Result<(), String> {
        let mut members = self.members;
        let last = members
            .pop()
            .ok_or_else(|| "admitted scope group is empty".to_string())?;
        drop(members);
        last.end_group_transaction(commit)
    }
}
