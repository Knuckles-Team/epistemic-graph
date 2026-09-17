//! `MutationKernel`'s consumer-reject and operator-surface methods (PX10a):
//! `outbox_reject`, `outbox_reject_in`, `outbox_dead_letters` and
//! `outbox_rewind`.
//!
//! A second `impl MutationKernel` block, in this file rather than
//! `kernel.rs`, purely to keep that file's own aggregates (already near
//! `kiss`'s per-file caps) from growing further -- inherent impls may be
//! split across files freely in Rust, and the move-once write authority
//! field being `pub(crate)` (not exposed outside this crate either way)
//! is what makes it possible.

use crate::admitted::AdmittedMutation;
use crate::kernel::MutationKernel;
use crate::outbox::{
    OutboxDeadLetterPage, OutboxPosition, OutboxRejectReason, OutboxRewindOutcome,
    OutboxRewindTarget,
};
use eg_storage::{OwnedStoreHandle, OwnerDomain, ScopedRead};
use eg_types::MutationOutboxLease;

impl MutationKernel {
    /// Resolve one held lease as a consumer-declared terminal failure, fenced
    /// like an acknowledgement but never advancing the watermark (X10-R2).
    pub fn outbox_reject<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        lease: &MutationOutboxLease,
        reason: OutboxRejectReason,
        now_ms: u64,
    ) -> Result<(), String> {
        crate::outbox::reject(&self.authority, owner, lease, reason, now_ms)
    }

    /// Reject inside the caller's already-admitted owner transaction, so a
    /// domain's own terminal evidence and this resolution commit atomically.
    pub fn outbox_reject_in<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        owner: &OwnedStoreHandle<D>,
        lease: &MutationOutboxLease,
        reason: OutboxRejectReason,
        now_ms: u64,
    ) -> Result<(), String> {
        crate::kernel::ensure_admitted_owner(write, owner)?;
        crate::outbox::reject_in(write, owner.identity(), lease, reason, now_ms)
    }

    /// List this consumer's dead-lettered rows, oldest first (X10-R5).
    pub fn outbox_dead_letters<D: OwnerDomain>(
        &self,
        read: &ScopedRead<'_, D>,
        consumer: &str,
        after: Option<&OutboxPosition>,
        limit: u32,
    ) -> Result<OutboxDeadLetterPage, String> {
        crate::outbox::dead_letters(read, consumer, after, limit)
    }

    /// Re-deliver `consumer`'s stream in order from `target`, one bounded
    /// transaction per call; repeat until `complete` (X10-R5).
    pub fn outbox_rewind<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        consumer: &str,
        target: OutboxRewindTarget,
        now_ms: u64,
    ) -> Result<OutboxRewindOutcome, String> {
        crate::outbox::rewind(&self.authority, owner, consumer, target, now_ms)
    }
}
