//! Delivery-side operations on the mutation authority.

use super::{ensure_admitted_owner, MutationKernel};
use crate::admitted::AdmittedMutation;
use crate::outbox::{OutboxBackfillOutcome, OutboxClaimBudget, OutboxClaimOutcome};
use eg_storage::{OwnedStoreHandle, OwnerDomain};
use eg_types::{MutationOutboxLease, MutationProjectionCursor};

impl MutationKernel {
    /// A subscription is the consumer's liveness and the boundary of its
    /// ordered stream. It is idempotent for the same topic and refused for a
    /// different one: changing it would move every position the consumer's
    /// cursor already names.
    pub fn outbox_subscribe<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        consumer: &str,
        topic: &str,
    ) -> Result<(), String> {
        crate::outbox::subscribe(&self.authority, owner, consumer, topic)
    }

    /// Claim pending outbox rows of `owner`'s scope for one durable consumer.
    ///
    /// Rows come in commit order from a durable scan position, each under a
    /// lease keyed `(scope, consumer, batch id, ordinal)` with a monotonic
    /// epoch. Selection and lease installation share one transaction, so two
    /// workers of one consumer can never both hold a row, and a crash between
    /// claim and ack leaves the lease to expire -- delivery is at-least-once.
    ///
    /// `budget` carries the caller-owned sweep-local 25% consecutive-claim cap
    /// across the scopes the caller visits. The composition scheduler chooses
    /// tenant order and weights and must reuse one budget for that sweep; this
    /// kernel does not persist cross-scope scheduler debt. A full in-flight
    /// queue or an exhausted sweep cap claims nothing and leaves the
    /// durable intention pending; it never drops work.
    pub fn outbox_claim<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        consumer: &str,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, String> {
        crate::outbox::claim(&self.authority, owner, consumer, budget)
    }

    /// Index every outbox row of `owner`'s scope that has no index row yet.
    ///
    /// The commit-ordered index exists only because `commit::write_outbox`
    /// writes it, so every row committed before this protocol landed is
    /// invisible to a claim -- never delivered and reported as no lag at all.
    /// The returned completion bit is durable state: an already-indexed page
    /// may report zero inserts while later primary rows still need repair.
    pub fn outbox_backfill_index<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<OutboxBackfillOutcome, String> {
        crate::outbox::backfill(&self.authority, owner)
    }

    /// Acknowledge one held lease and advance the consumer's projection cursor
    /// in the SAME admitted transaction.
    ///
    /// A crash between marking the row delivered and moving the watermark is
    /// therefore not representable: both land or neither does.
    pub fn outbox_ack<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        lease: &MutationOutboxLease,
        now_ms: u64,
    ) -> Result<MutationProjectionCursor, String> {
        crate::outbox::ack(&self.authority, owner, lease, now_ms)
    }

    /// Validate one held lease inside an already-admitted owner transaction.
    ///
    /// This performs the exact checks [`Self::outbox_ack_in`] will repeat, but
    /// writes nothing. Domain consumers use it before opening their owner-row
    /// gate so a stale, expired, released or out-of-order lease cannot stage an
    /// effect that will only be rejected after the effect ran.
    pub fn outbox_validate_in<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        owner: &OwnedStoreHandle<D>,
        lease: &MutationOutboxLease,
        now_ms: u64,
    ) -> Result<(), String> {
        ensure_admitted_owner(write, owner)?;
        crate::outbox::validate_in(write, owner.identity(), lease, now_ms)
    }

    /// Acknowledge one held lease inside an already-admitted owner transaction.
    ///
    /// This method does not commit `write`. The caller first writes its owner
    /// rows and terminal receipt, then adds this delivery transition and commits
    /// the admitted batch once, making the effect, receipt and acknowledgement
    /// indivisible. Requiring an admitted, scope-matching write prevents this
    /// entry point from becoming a second standalone acknowledgement authority.
    pub fn outbox_ack_in<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        owner: &OwnedStoreHandle<D>,
        lease: &MutationOutboxLease,
        now_ms: u64,
    ) -> Result<MutationProjectionCursor, String> {
        ensure_admitted_owner(write, owner)?;
        crate::outbox::ack_in_transaction(write, owner.identity(), lease, now_ms)
    }

    /// Give one held lease back without delivering it, so the row is
    /// immediately re-claimable. The released lease can never be acknowledged.
    pub fn outbox_release<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        lease: &MutationOutboxLease,
    ) -> Result<(), String> {
        crate::outbox::release(&self.authority, owner, lease)
    }

    /// Retire every expired lease of one consumer, returning how many. An
    /// expired lease is already re-claimable; this makes the queue's reported
    /// in-flight count agree with that.
    pub fn outbox_expire<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        consumer: &str,
        now_ms: u64,
    ) -> Result<u32, String> {
        crate::outbox::expire(&self.authority, owner, consumer, now_ms)
    }
}
