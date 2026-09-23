//! Once-per-transaction write authority (EH-390).
//!
//! A write transaction proves the store's authority (persisted root, owner
//! manifest, declared table census) ONCE, at [`PhysicalStore::begin_write`],
//! and each capability proves its scope binding once and keeps the proof for
//! the life of that transaction. Before EH-390 every `verify_scope` re-ran the
//! whole store census inside the already-held transaction, about nine times
//! per graph-shard commit.
//!
//! Why one check at the boundary is the same check:
//! * redb admits one writer, so nothing outside this transaction can change the
//!   identity tables while it is open;
//! * a capability can never open an identity table (`mutation_store_root`,
//!   `mutation_scope_bindings`, `mutation_owner_manifest`) nor an undeclared
//!   table, so nothing inside it can change the root, the manifest or the
//!   census either (`capability::permit_table`);
//! * the ONE in-transaction binding write a capability can make is
//!   `retire_scope_binding`, and it advances the transaction's
//!   [`TxnAuthority::binding_epoch`]. Every cached binding proof is keyed by
//!   that epoch, so the next `verify_scope` on any member re-reads its binding
//!   and a retired scope is refused.
//!
//! The proofs live on the capability and on the transaction's shared
//! [`TxnAuthority`], never on the store, so none can outlive its transaction.
//!
//! [`PhysicalStore::begin_write`]: crate::physical::root::PhysicalStore::begin_write

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// How many times each write-authority check has run on one open store.
/// Diagnostic, and the counting verifier EH-390's tests assert on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WriteValidationCounts {
    /// Full store-authority validations (root, manifest, census): one per
    /// write transaction.
    pub store_authority: u64,
    /// Scope-binding row reads: one per capability per binding epoch.
    pub scope_bindings: u64,
}

/// The live counters behind [`WriteValidationCounts`], one set per store.
#[derive(Debug, Default)]
pub(crate) struct WriteValidationCounters {
    store_authority: AtomicU64,
    scope_bindings: AtomicU64,
}

impl WriteValidationCounters {
    pub(crate) fn store_authority_checked(&self) {
        self.store_authority.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn scope_binding_checked(&self) {
        self.scope_bindings.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> WriteValidationCounts {
        WriteValidationCounts {
            store_authority: self.store_authority.load(Ordering::Relaxed),
            scope_bindings: self.scope_bindings.load(Ordering::Relaxed),
        }
    }
}

/// State one physical write transaction shares across every capability on it.
#[derive(Debug, Default)]
pub(crate) struct TxnAuthority {
    /// Set by any capability on this transaction whose operation failed; the
    /// transaction cannot commit while it is set.
    poisoned: AtomicBool,
    /// Advanced by every in-transaction scope-binding write.
    binding_epoch: AtomicU64,
}

impl TxnAuthority {
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::SeqCst);
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::SeqCst)
    }

    pub(crate) fn binding_epoch(&self) -> u64 {
        self.binding_epoch.load(Ordering::SeqCst)
    }

    /// Invalidate every binding proof cached on this transaction.
    pub(crate) fn binding_changed(&self) {
        self.binding_epoch.fetch_add(1, Ordering::SeqCst);
    }
}

/// One capability's cached binding proof: the transaction binding epoch at
/// which its scope binding was last read and validated.
#[derive(Debug)]
pub(crate) struct BindingProof(AtomicU64);

impl BindingProof {
    /// Proven at `epoch`, by the bind that minted the capability.
    pub(crate) fn proven_at(epoch: u64) -> Self {
        Self(AtomicU64::new(epoch))
    }

    pub(crate) fn holds_at(&self, epoch: u64) -> bool {
        self.0.load(Ordering::SeqCst) == epoch
    }

    pub(crate) fn reproven_at(&self, epoch: u64) {
        self.0.store(epoch, Ordering::SeqCst);
    }
}
