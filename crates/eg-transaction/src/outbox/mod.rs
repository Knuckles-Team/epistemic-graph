//! The outbox claim/deliver/acknowledge protocol (RF-RULING-007).
//!
//! Six ledger tables -- `mutation_outbox_topic_index`, `..._consumers`,
//! `..._deliveries`, `..._cursors`, `..._claim_cursors`, `..._fairness` -- were
//! declared by the storage kernel, censused, bootstrapped, purge-swept and
//! written by nothing. RF-RULING-007 ruled that the protocol is implemented
//! once, here, in the kernel's delivery ledger. This module provides the
//! durable at-least-once stream, bounded queue and caller-carried sweep cap;
//! the composition scheduler owns tenant selection, weights, order and any
//! restart debt across its roster.
//!
//! # Shape
//!
//! * A consumer **subscribes** to one topic on one scope
//!   ([`crate::MutationKernel::outbox_subscribe`]). The subscription is durable,
//!   so the ordered stream a cursor names cannot change under it.
//! * A **claim** takes rows in commit order from a durable scan position and
//!   installs a lease keyed `(scope, consumer, batch id, ordinal)` with a
//!   monotonic epoch. Selection and installation share one transaction, so two
//!   workers of one consumer can never hold the same row, and a crash between
//!   claim and ack leaves the lease to expire -- delivery is at-least-once.
//! * An **ack** marks the lease delivered and advances the consumer's watermark
//!   in the SAME transaction. A crash between them is not representable.
//! * **Sweep capping** has a 25% consecutive-claim limit in the caller-owned
//!   budget ([`OutboxClaimBudget`]). The composition scheduler chooses tenant
//!   order and weights; one scope cannot exceed the cap once that sweep is
//!   contended.
//! * The queue is **bounded**. When a consumer's in-flight set is full the claim
//!   returns nothing and the durable intention stays pending: no drop, no
//!   unbounded growth, no rollback of the canonical commit.
//!
//! # What this is not
//!
//! It is not a mutation of the scope. A claim and an ack write only delivery-side
//! rows; they never bump the authoritative version, write a receipt, consume an
//! idempotency key or touch replay evidence. A projection poll that moved every
//! reader's OCC expectation would make polling and writing indistinguishable.
//!
//! It is also not part of a batch's identity. The producer side -- the
//! `MutationOutboxIntent` list and the `ledger_outbox` rows `commit::finish`
//! writes from it -- is folded into the batch's canonical payload digest and is
//! immutable after compile. Nothing here appends or edits an intent.
//!
//! # Retirement
//!
//! `commit::purge_scope` sweeps all six tables with every other scoped ledger
//! table, so retiring a scope drops its undelivered rows along with the events
//! themselves. That is correct: the events describe a generation of a logical
//! owner that no longer exists, and a projection cursor pointing into it names
//! nothing a reader could ever resolve.

use crate::admitted::AdmittedMutation;
use crate::tables::FENCES;
use eg_storage::{decode_ledger_record, ledger_scope_key};
use eg_types::MutationScopeIdentity;

mod claim;
mod cursor;
mod index;
mod rows;
mod status;
mod stream;

#[cfg(test)]
pub(crate) use rows::{decode_row, encode_row};
pub use rows::{OutboxClaimCursor, OutboxConsumerState, OutboxDelivery, OutboxPosition};
pub use status::OutboxStatus;

pub(crate) use claim::{claim, expire, release};
pub(crate) use cursor::{ack, ack_in_transaction, validate_in};
pub use index::OutboxBackfillOutcome;
pub(crate) use index::{backfill, index_outbox_row, mark_index_ready_in_write};
pub(crate) use stream::subscribe;

use eg_storage::{OwnerDomain, ScopedRead};
use eg_types::{MutationOutboxLease, MutationProjectionCursor};

/// How many claimed-but-unresolved rows one consumer may hold on one scope.
///
/// The bound is the contract: a full queue claims nothing and leaves the
/// durable intention pending, rather than growing without limit or dropping
/// maintenance work.
pub fn queue_capacity() -> u32 {
    rows::OUTBOX_QUEUE_CAPACITY
}

/// Refuse delivery-side writes while a scope is in the fenced half of a graft.
///
/// `AdmittedMutation::open` proves that the scope is still bound, but it does
/// not admit a batch and therefore does not run the ordinary route-fence gate.
/// Claims, acknowledgements, subscriptions, expiry and index backfill still
/// need that gate: otherwise phase C could purge a delivery row committed
/// after the phase-B source snapshot. The graft marker uses the maximum route
/// fence, so this check is deliberately narrower than a caller's normal route
/// comparison and does not reject an ordinary live placement fence.
pub(crate) fn ensure_not_graft_fenced<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
) -> Result<(), String> {
    write.verify_scope(identity)?;
    let scope = ledger_scope_key(identity);
    let table = write.scoped_table(FENCES)?;
    let fence = table
        .get(scope.as_str())?
        .map(|value| decode_ledger_record::<eg_storage::ScopeFence>(value.value()))
        .transpose()?;
    if let Some(fence) = fence {
        if fence.identity != *identity {
            return Err("mutation fence row is not stamped with this scope identity".to_string());
        }
        if fence.placement_epoch == crate::graft::GRAFT_FENCE
            && fence.fencing_token == crate::graft::GRAFT_FENCE
        {
            return Err("STALE_FENCE: scope is under graft".to_string());
        }
    }
    Ok(())
}

/// How many times one row is leased before it is dead-lettered.
pub fn max_delivery_attempts() -> u32 {
    rows::MAX_DELIVERY_ATTEMPTS
}

/// One consumer's durable projection watermark on the read's bound scope.
///
/// The read side of the cursor `outbox_ack` advances. It is a *read*, not a
/// kernel method, for the same reason [`crate::read::version`] is: a watermark
/// is answered from a snapshot, and reaching it through the one mutation
/// authority would mean opening a write transaction to answer a question.
pub fn outbox_cursor<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    consumer: &str,
) -> Result<Option<MutationProjectionCursor>, String> {
    cursor::read_cursor(read, consumer)
}

/// One consumer's observable queue state on the read's bound scope: liveness,
/// capacity, saturation, in-flight count, pending rows (and whether that figure
/// is a lower bound), oldest pending age, dead-letter count, and lag in both
/// rows and versions.
pub fn outbox_status<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    consumer: &str,
    now_ms: u64,
) -> Result<OutboxStatus, String> {
    status::status(read, consumer, now_ms)
}

/// Why one claim handed back nothing.
///
/// DESIGN.md requires queue-full to return "an explicit `deferred/backpressured`
/// outcome"; an empty vector said the same thing as an idle queue and as a
/// fairness cap, so a caller could not tell backpressure from having nothing to
/// do without a second, separately-transacted status read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxDeferral {
    /// The consumer already holds `capacity` unresolved rows on this scope.
    QueueFull { inflight: u32, capacity: u32 },
    /// This tenant has taken its whole consecutive run for this sweep.
    FairnessCapped { consecutive: u32, cap: u32 },
    /// The sweep's total budget is spent.
    BudgetSpent,
    /// A bounded legacy index backfill has not yet reached EOF. Claims remain
    /// closed until its durable cursor completes, so a later producer cannot
    /// be delivered ahead of an unindexed historical row.
    IndexBackfillPending,
}

/// What one claim decided.
///
/// `claims` empty with `deferred: None` is an idle queue -- there was nothing
/// pending. `claims` empty with `deferred: Some(_)` is backpressure, and the
/// durable intention is still pending.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxClaimOutcome {
    pub claims: Vec<MutationOutboxLease>,
    pub deferred: Option<OutboxDeferral>,
    /// Whether the selection scan stopped at its page bound, so more rows may
    /// be claimable immediately.
    pub more_available: bool,
}

impl OutboxClaimOutcome {
    pub(crate) fn claimed(claims: Vec<MutationOutboxLease>, more_available: bool) -> Self {
        Self {
            claims,
            deferred: None,
            more_available,
        }
    }

    pub(crate) fn deferred(reason: OutboxDeferral) -> Self {
        Self {
            claims: Vec::new(),
            deferred: Some(reason),
            more_available: true,
        }
    }

    pub fn is_deferred(&self) -> bool {
        self.deferred.is_some()
    }
}

/// The caller-carried cap state of one claim sweep across N bound scopes.
///
/// A claim is bound to one serving scope by construction (the capability it
/// writes through carries exactly one), so this value carries only the
/// sweep-local consecutive-claim bound from one scope call to the next. The
/// composition scheduler owns the tenant roster, weights and call order; this
/// type does not select tenants or implement weighted round robin.
///
/// Every claim takes at most the cap, so one call cannot monopolise the
/// caller's budget. This primitive observes contention only after the caller
/// has visited another tenant; it cannot discover an unvisited eligible tenant
/// or select a weighted order. The composition scheduler must establish that
/// roster and policy when it needs a first-call contention guarantee.
/// What contention changes is whether the run *accumulates*: while only one
/// tenant has claimed, each call starts a fresh run and the tenant is never
/// starved of its own queue; from the moment a second tenant claims, a tenant's
/// consecutive run is spent until someone else takes a turn. The caller must
/// reuse one budget for the sweep; constructing a fresh budget starts a new
/// sweep by design.
///
/// The durable row remains accounting and observability for one scope. A
/// sweep's fairness decision is deliberately local: an independent
/// `(scope, consumer)` row cannot prove whether another tenant had a turn,
/// so it must never seed a cross-tenant gate after restart.
#[derive(Debug, Clone)]
pub struct OutboxClaimBudget {
    limit: u32,
    lease_ms: u64,
    now_ms: u64,
    claimed: u32,
    last_tenant: Option<String>,
    consecutive: u32,
    tenants_seen: u32,
}

impl OutboxClaimBudget {
    /// A sweep that may claim `limit` rows in total, leasing each for
    /// `lease_ms`, as of `now_ms`.
    pub fn new(limit: u32, lease_ms: u64, now_ms: u64) -> Result<Self, String> {
        if limit == 0 || lease_ms == 0 {
            return Err("outbox claim budget requires a non-zero limit and lease".to_string());
        }
        Ok(Self {
            limit,
            lease_ms,
            now_ms,
            claimed: 0,
            last_tenant: None,
            consecutive: 0,
            tenants_seen: 0,
        })
    }

    pub fn limit(&self) -> u32 {
        self.limit
    }

    pub fn lease_ms(&self) -> u64 {
        self.lease_ms
    }

    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }

    /// Rows still available to this sweep.
    pub fn remaining(&self) -> u32 {
        self.limit.saturating_sub(self.claimed)
    }

    /// The current consecutive-claim run.
    pub fn consecutive(&self) -> u32 {
        self.consecutive
    }

    /// The 25% consecutive-claim cap, never below one row: a cap that rounded
    /// down to zero would starve every tenant rather than balance them.
    pub fn consecutive_cap(&self) -> u32 {
        (self.limit / 4).max(1)
    }

    /// Whether more than one tenant has claimed in this caller-owned sweep.
    pub fn contended(&self) -> bool {
        self.tenants_seen > 1
    }

    /// Whether `tenant` is the tenant that claimed last in this sweep.
    pub(crate) fn is_consecutive(&self, tenant: &str) -> bool {
        self.last_tenant.as_deref() == Some(tenant)
    }

    /// How many rows `tenant` may take on its next claim in this sweep.
    pub(crate) fn allowance(&self, tenant: &str) -> u32 {
        let cap = self.consecutive_cap();
        // Only this sweep can prove contention. A durable per-scope run is not
        // enough: it cannot encode whether another tenant took a turn while
        // the process was down, and using it here permanently starves a lone
        // tenant after restart.
        let run = if self.contended() && self.is_consecutive(tenant) {
            cap.saturating_sub(self.consecutive.min(cap))
        } else {
            cap
        };
        self.remaining().min(run)
    }

    /// Account for what one claim actually took.
    pub(crate) fn record(&mut self, tenant: &str, claimed: u32) {
        if claimed == 0 {
            return;
        }
        self.claimed = self.claimed.saturating_add(claimed);
        if self.is_consecutive(tenant) && self.contended() {
            self.consecutive = self.consecutive.saturating_add(claimed);
        } else if self.is_consecutive(tenant) {
            self.consecutive = claimed;
            self.tenants_seen = self.tenants_seen.max(1);
        } else {
            self.last_tenant = Some(tenant.to_string());
            self.consecutive = claimed;
            self.tenants_seen = self.tenants_seen.saturating_add(1).max(1);
        }
    }
}
