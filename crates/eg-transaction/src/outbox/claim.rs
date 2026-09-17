//! Claiming, releasing, expiring and dead-lettering outbox leases.
//!
//! A claim is an admitted, scope-bound write that touches only the delivery
//! side of the ledger. It never bumps the scope's authoritative version and
//! never writes a receipt: a projection poll is not a mutation of the scope,
//! and making it one would move every reader's OCC expectation on every poll.
//! What it does share with a mutation is everything that makes a mutation
//! safe -- one storage-issued capability, one physical write transaction,
//! `Immediate` durability, and row keys taken from the capability's own scope
//! rather than from an argument.
//!
//! # Everything a claim does is bounded
//!
//! The in-flight count is a durable counter maintained in the same transaction
//! as the delivery rows it counts, not a scan of every row ever delivered; the
//! index scan is capped at a fast-class page; and each claim reclaims a bounded
//! number of resolved delivery rows behind the resolved prefix, so the delivery
//! table does not grow for the life of the scope.

use crate::admitted::AdmittedMutation;
use crate::outbox::rows::{
    decode_row, encode_row, validate_consumer, validate_delivery_event, validate_delivery_key,
    validate_delivery_state, validate_stamp, OutboxClaimCursor, OutboxConsumerState,
    OutboxDelivery, OutboxPosition, MAX_CLAIM_SCAN_ROWS, MAX_DELIVERY_ATTEMPTS,
    MAX_PRUNE_ROWS_PER_CLAIM, OUTBOX_QUEUE_CAPACITY,
};
use crate::outbox::stream::{read_outbox_row_in_write, subscribed_topic};
use crate::outbox::{
    ensure_not_graft_fenced, index, OutboxClaimBudget, OutboxClaimOutcome, OutboxDeferral,
};
use crate::tables::{OUTBOX_CLAIM_CURSORS, OUTBOX_DELIVERIES, OUTBOX_FAIRNESS};
use eg_storage::{
    ledger_scope_key, MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain, ScopedRead,
    ScopedTableMut,
};
use eg_types::{MutationOutboxLease, MutationScopeIdentity, MUTATION_BATCH_VERSION};

mod support;
use support::{mark_resolved_if_prefix_intact, validate_existing_delivery};

const EXPIRY_CURSOR_PREFIX: &str = "\u{1}kernel-outbox-expiry/";
const PRUNE_CURSOR_PREFIX: &str = "\u{1}kernel-outbox-prune/";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpiryCursor {
    schema_version: u16,
    identity: MutationScopeIdentity,
    consumer: String,
    position: Option<OutboxPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PruneCursor {
    schema_version: u16,
    identity: MutationScopeIdentity,
    consumer: String,
    /// The last commit-ordered index position examined by pruning. Keeping
    /// this cursor past retained dead-letter rows prevents one poison prefix
    /// from pinning every bounded prune page forever.
    position: Option<OutboxPosition>,
    /// The resolved prefix through which the current cursor has completed.
    /// A new, larger prefix resumes from `position` rather than rescanning the
    /// retained history.
    boundary: Option<OutboxPosition>,
    complete: bool,
}

/// Claim up to `budget`'s allowance of one topic's pending rows for `consumer`.
///
/// Selection and lease installation share one transaction, so queue pressure
/// can delay a claim but can never lose one, and two workers of one consumer
/// can never both hold the same row: the second sees the first's lease.
pub(crate) fn claim<D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    owner: &OwnedStoreHandle<D>,
    consumer: &str,
    budget: &mut OutboxClaimBudget,
) -> Result<OutboxClaimOutcome, String> {
    validate_consumer(consumer)?;
    let tenant = owner.identity().tenant().as_str().to_string();
    let write = AdmittedMutation::open(authority, owner)?;
    match claim_in(&write, owner.identity(), consumer, budget) {
        Ok(outcome) => {
            write.commit()?;
            budget.record(&tenant, outcome.claims.len() as u32);
            Ok(outcome)
        }
        Err(error) => {
            write.abort()?;
            Err(error)
        }
    }
}

fn claim_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    consumer: &str,
    budget: &mut OutboxClaimBudget,
) -> Result<OutboxClaimOutcome, String> {
    ensure_not_graft_fenced(write, identity)?;
    let scope = ledger_scope_key(identity);
    let topic = subscribed_topic(write, &scope, consumer)?;
    crate::outbox::cursor::read_cursor_in_write(write, &scope, consumer, identity)?;
    if index::backfill_pending_in_write(write, identity)? {
        return Ok(OutboxClaimOutcome::deferred(
            OutboxDeferral::IndexBackfillPending,
        ));
    }
    if crate::outbox::rewind::read_rewind_cursor(write, &scope, consumer, identity)?.is_some() {
        return Ok(OutboxClaimOutcome::deferred(OutboxDeferral::RewindPending));
    }
    let mut state = read_consumer_state(write, &scope, consumer, identity)?;
    let allowance = budget.allowance(identity.tenant().as_str());
    if budget.remaining() == 0 {
        return Ok(OutboxClaimOutcome::deferred(OutboxDeferral::BudgetSpent));
    }
    let cursor = read_claim_cursor(write, &scope, consumer, identity)?;
    validate_claim_cursor(write, &scope, &topic, identity, &cursor)?;
    let page = scan_validated_page(
        write,
        &scope,
        &topic,
        identity,
        cursor.resolved_through.as_ref(),
        MAX_CLAIM_SCAN_ROWS,
    )?;
    let at_page = ClaimPage {
        scope: &scope,
        consumer,
        identity,
        cursor: &cursor,
        page: &page,
    };
    let room = match reclaim_room(write, &at_page, &mut state, budget.now_ms())? {
        ClaimGate::Room(room) => room,
        ClaimGate::Deferred(outcome) => return Ok(outcome),
    };
    let claim_room = match decide_claim_room(write, &at_page, budget, allowance, room)? {
        ClaimGate::Room(room) => room,
        ClaimGate::Deferred(outcome) => return Ok(outcome),
    };
    let at = Claiming {
        scope: &scope,
        consumer,
        identity,
        budget,
        room: claim_room,
    };
    let outcome = install_leases(
        write,
        &at,
        &page.entries,
        cursor.acked_through.as_ref(),
        &mut state,
    )?;
    let resolved_through = outcome.resolved_through.or(cursor.resolved_through.clone());
    prune_resolved(
        write,
        &scope,
        consumer,
        identity,
        cursor.acked_through.as_ref(),
        resolved_through.as_ref(),
    )?;
    write_claim_cursor(
        write,
        &scope,
        consumer,
        &OutboxClaimCursor {
            resolved_through,
            ..cursor
        },
    )?;
    accrue(&mut state, budget, outcome.claimed.len() as u32);
    write_consumer_state(write, &scope, consumer, &state)?;
    Ok(OutboxClaimOutcome::claimed(
        outcome.claimed,
        page.truncated,
        outcome.dead_lettered,
    ))
}

/// Scan one bounded page of a topic's index from `from`, proving every entry
/// still names a live, correctly stamped primary row before the caller
/// decides what to do with it.
///
/// Shared by a claim's own selection scan, pruning, expiry and rewind's
/// delete walk: each walks the same commit-ordered index in bounded,
/// resumable pages and would otherwise repeat this exact scan-then-validate
/// loop a fourth time.
pub(crate) fn scan_validated_page<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    topic: &str,
    identity: &MutationScopeIdentity,
    from: Option<&OutboxPosition>,
    limit: usize,
) -> Result<index::IndexPage, String> {
    let page = index::scan_in_write(write, scope, topic, from, limit)?;
    for entry in &page.entries {
        index::validate_position_in_write(write, scope, topic, identity, &entry.position)
            .map_err(|error| format!("CORRUPT_OUTBOX_INDEX: {error}"))?;
    }
    Ok(page)
}

/// What one gate inside a claim decided: either room for `u32` rows, or the
/// deferred outcome the caller must return immediately.
enum ClaimGate {
    Room(u32),
    Deferred(OutboxClaimOutcome),
}

/// The scope, consumer, identity and bounded page every gate inside one
/// claim reads -- the same bundling `Claiming` already does for the leasing
/// step, so a gate call site passes one reference instead of four.
struct ClaimPage<'c> {
    scope: &'c str,
    consumer: &'c str,
    identity: &'c MutationScopeIdentity,
    cursor: &'c OutboxClaimCursor,
    page: &'c index::IndexPage,
}

/// Reclaim expired leases in the bounded page, then decide how much of the
/// bounded queue is free. A full queue defers rather than drops.
fn reclaim_room<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    at: &ClaimPage<'_>,
    state: &mut OutboxConsumerState,
    now_ms: u64,
) -> Result<ClaimGate, String> {
    // A stale durable in-flight counter must not make an expired row
    // permanently unreachable. Reclaim the bounded index page before applying
    // queue pressure; install_leases will then immediately claim the released
    // rows with a greater epoch.
    let reclaimed = reclaim_expired_page(
        write,
        at.scope,
        at.consumer,
        at.identity,
        at.cursor.acked_through.as_ref(),
        at.page,
        now_ms,
    )?;
    state.inflight = state.inflight.checked_sub(reclaimed).ok_or_else(|| {
        "CORRUPT_OUTBOX_FAIRNESS: reclaimed leases exceed in-flight counter".to_string()
    })?;
    let room = OUTBOX_QUEUE_CAPACITY.saturating_sub(state.inflight);
    if room > 0 {
        return Ok(ClaimGate::Room(room));
    }
    // Queue-full. The durable intention stays pending and the scan position
    // is untouched; nothing is dropped and the caller is told why this claim
    // did no work. Persist an adjusted counter if the bounded expiry sweep
    // reclaimed anything before discovering that the rest of the queue is
    // still full.
    if reclaimed > 0 {
        write_consumer_state(write, at.scope, at.consumer, state)?;
    }
    Ok(ClaimGate::Deferred(OutboxClaimOutcome::deferred(
        OutboxDeferral::QueueFull {
            inflight: state.inflight,
            capacity: OUTBOX_QUEUE_CAPACITY,
        },
    )))
}

/// A durable run cap applies to fresh work. It must not turn an idle worker
/// into backpressure while another worker still holds the rows, and it must
/// not strand a released or expired row behind a restart. Inspect the
/// bounded page first so only a fresh first candidate is deferred; a retry
/// candidate gets one bounded reclaim opportunity.
fn decide_claim_room<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    at: &ClaimPage<'_>,
    budget: &OutboxClaimBudget,
    allowance: u32,
    room: u32,
) -> Result<ClaimGate, String> {
    if allowance != 0 {
        return Ok(ClaimGate::Room(allowance.min(room)));
    }
    Ok(match fairness_candidate(
        write,
        at.scope,
        at.consumer,
        at.identity,
        at.cursor.acked_through.as_ref(),
        at.page,
        budget.now_ms(),
    )? {
        FairnessCandidate::Fresh => ClaimGate::Deferred(OutboxClaimOutcome::deferred(
            OutboxDeferral::FairnessCapped {
                consecutive: budget.consecutive(),
                cap: budget.consecutive_cap(),
            },
        )),
        FairnessCandidate::Retry => ClaimGate::Room(1),
        FairnessCandidate::None => ClaimGate::Room(0),
    })
}

/// Zero expired delivery leases from the bounded, commit-ordered claim page.
///
/// The delivery table is keyed by batch id, while eligibility is ordered by
/// the outbox index. Claiming through the index keeps this repair aligned with
/// the same bounded page a claim can actually reach, rather than allowing an
/// expired row to remain hidden behind an unrelated delivery-table prefix.
fn reclaim_expired_page<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
    acked_through: Option<&OutboxPosition>,
    page: &index::IndexPage,
    now_ms: u64,
) -> Result<u32, String> {
    let mut expired = Vec::new();
    let mut deliveries = write.scoped_table(OUTBOX_DELIVERIES)?;
    for entry in &page.entries {
        let position = &entry.position;
        let key = (
            scope,
            consumer,
            position.batch_id.as_str(),
            position.ordinal,
        );
        let delivery = deliveries
            .get(key)?
            .map(|value| decode_row::<OutboxDelivery>(value.value()))
            .transpose()?;
        let Some(delivery) = delivery else {
            continue;
        };
        validate_stamp(&delivery.identity, identity)?;
        validate_delivery_key(&delivery, consumer, position)?;
        validate_delivery_state(&delivery, acked_through)?;
        if !delivery.resolved() && delivery.lease_until_ms != 0 && delivery.lease_until_ms <= now_ms
        {
            expired.push((key, delivery));
        }
    }
    let count = expired.len() as u32;
    for ((_, _, batch_id, ordinal), mut delivery) in expired {
        delivery.lease_until_ms = 0;
        deliveries.insert(
            (scope, consumer, batch_id, ordinal),
            encode_row(&delivery, "outbox delivery row")?.as_slice(),
        )?;
    }
    Ok(count)
}

/// What the first unresolved row in a bounded selection page requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FairnessCandidate {
    None,
    Fresh,
    Retry,
}

/// Inspect only the rows a claim could visit before deciding whether a
/// durable fairness cap should defer it. Live leases and resolved rows are not
/// pending work for this worker; an expired or released row is retry work and
/// must remain reclaimable after a restart. If the page ends at its bound,
/// unseen rows are conservatively treated as fresh work.
fn fairness_candidate<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
    acked_through: Option<&OutboxPosition>,
    page: &index::IndexPage,
    now_ms: u64,
) -> Result<FairnessCandidate, String> {
    let deliveries = write.scoped_table(OUTBOX_DELIVERIES)?;
    for entry in &page.entries {
        let position = &entry.position;
        let key = (
            scope,
            consumer,
            position.batch_id.as_str(),
            position.ordinal,
        );
        let current = deliveries
            .get(key)?
            .map(|value| decode_row::<OutboxDelivery>(value.value()))
            .transpose()?;
        let Some(existing) = current else {
            return Ok(FairnessCandidate::Fresh);
        };
        validate_stamp(&existing.identity, identity)?;
        validate_delivery_key(&existing, consumer, position)?;
        validate_delivery_state(&existing, acked_through)?;
        if existing.resolved() || existing.leased_at(now_ms) {
            continue;
        }
        return Ok(FairnessCandidate::Retry);
    }
    Ok(if page.truncated {
        FairnessCandidate::Fresh
    } else {
        FairnessCandidate::None
    })
}

/// The scope-and-consumer context every step of one claim shares.
///
/// A struct rather than eight parameters: the scope key, the consumer and the
/// identity are read off the capability once and must be the same three
/// everywhere, and threading them separately is how one call site would come to
/// pass a caller-supplied scope instead.
struct Claiming<'c> {
    scope: &'c str,
    consumer: &'c str,
    identity: &'c MutationScopeIdentity,
    budget: &'c OutboxClaimBudget,
    room: u32,
}

/// One page's claimable outcome: the leases installed, the rows this pass
/// dead-lettered (X10-R4/R5), and how far the contiguous resolved prefix now
/// runs.
struct Installed {
    claimed: Vec<MutationOutboxLease>,
    dead_lettered: Vec<OutboxPosition>,
    resolved_through: Option<OutboxPosition>,
}

/// Install one lease per claimable row, dead-letter the stream head once it
/// has exhausted its retries, and report how far the contiguous resolved
/// prefix now runs.
///
/// Only the stream head -- the first row this pass finds still unresolved --
/// may accumulate an attempt or be dead-lettered (X10-R1, design 2.4 item 1).
/// Every row after it in this page is a successor: once the head is decided,
/// `is_head` is `false` for the rest of the page, so a failing head can never
/// spend a healthy successor's retry budget or reach its dead-letter check,
/// no matter how many times the head itself is re-leased or released. A
/// dead-lettered head still extends the resolved prefix in the SAME pass, so
/// its successor can become the new head and be claimed immediately.
fn install_leases<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    at: &Claiming<'_>,
    entries: &[index::IndexEntry],
    acked_through: Option<&OutboxPosition>,
    state: &mut OutboxConsumerState,
) -> Result<Installed, String> {
    let (scope, consumer, identity, budget, room) =
        (at.scope, at.consumer, at.identity, at.budget, at.room);
    let mut claimed = Vec::new();
    let mut dead_lettered = Vec::new();
    let mut resolved_through = None;
    let mut prefix_intact = true;
    let mut deliveries = write.scoped_table(OUTBOX_DELIVERIES)?;
    for entry in entries {
        let position = &entry.position;
        let key = (
            scope,
            consumer,
            position.batch_id.as_str(),
            position.ordinal,
        );
        let current = deliveries
            .get(key)?
            .map(|value| decode_row::<OutboxDelivery>(value.value()))
            .transpose()?;
        if let Some(existing) = &current {
            validate_existing_delivery(existing, identity, consumer, position, acked_through)?;
        }
        if current.as_ref().is_some_and(OutboxDelivery::resolved) {
            mark_resolved_if_prefix_intact(prefix_intact, &mut resolved_through, position);
            continue;
        }
        // Expiry is normally swept explicitly, but a claim may be the first
        // operation after a worker disappears; the two `expired` branches below keep
        // the durable in-flight counter aligned with the row resolved here too.
        let is_head = prefix_intact;
        let expired = current.as_ref().is_some_and(|existing| {
            existing.lease_until_ms != 0 && !existing.leased_at(budget.now_ms())
        });
        let at_row = LeaseAt {
            scope,
            identity,
            consumer,
            position,
            budget,
            is_head,
        };
        if try_dead_letter_head(
            &mut deliveries,
            key,
            current.as_ref(),
            &at_row,
            expired,
            state,
            &mut dead_lettered,
        )? {
            resolved_through = Some(position.clone());
            continue;
        }
        prefix_intact = false;
        if claimed.len() as u32 >= room {
            // The allowance is spent. Every row from here on stays pending,
            // which is the deferral this bound exists to produce.
            break;
        }
        if current
            .as_ref()
            .is_some_and(|existing| existing.leased_at(budget.now_ms()))
        {
            continue;
        }
        let (bytes, lease) = lease_row(write, &at_row, current, expired, state)?;
        deliveries.insert(key, bytes.as_slice())?;
        claimed.push(lease);
    }
    Ok(Installed {
        claimed,
        dead_lettered,
        resolved_through,
    })
}

/// The scope, consumer, identity, position, budget and head status one row's
/// lease decision needs -- bundled so `build_lease` and `lease_row` each take
/// one reference instead of six positional parameters.
struct LeaseAt<'c> {
    scope: &'c str,
    identity: &'c MutationScopeIdentity,
    consumer: &'c str,
    position: &'c OutboxPosition,
    budget: &'c OutboxClaimBudget,
    is_head: bool,
}

/// Dead-letter `current` when, and only when, it is the stream head and has
/// exhausted its retries (X10-R1): a non-head row is never even checked.
/// Writes the terminal row and updates the durable fairness counters and the
/// in-memory dead-letter report; returns whether the row was dead-lettered,
/// so the caller can advance the resolved prefix and move on to the next
/// entry in the same pass.
fn try_dead_letter_head(
    deliveries: &mut ScopedTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    key: (&str, &str, &str, u32),
    current: Option<&OutboxDelivery>,
    at: &LeaseAt<'_>,
    expired: bool,
    state: &mut OutboxConsumerState,
    dead_lettered: &mut Vec<OutboxPosition>,
) -> Result<bool, String> {
    if !at.is_head {
        return Ok(false);
    }
    let Some(dead) = dead_letter(current, at.identity, at.consumer, at.position, at.budget) else {
        return Ok(false);
    };
    let bytes = apply_dead_letter(dead, expired, state, dead_lettered)?;
    deliveries.insert(key, bytes.as_slice())?;
    Ok(true)
}

/// Install one fresh or retried lease: adjust the in-flight counter for a
/// row that was expired-but-unswept (`outbox_expire` normally performs this
/// half; claim must remain correct when it is the first sweep after a worker
/// disappears), build the next lease, and account for the new hold. Returns
/// the encoded row to write and the lease to hand the caller.
fn lease_row<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    at: &LeaseAt<'_>,
    current: Option<OutboxDelivery>,
    expired: bool,
    state: &mut OutboxConsumerState,
) -> Result<(Vec<u8>, MutationOutboxLease), String> {
    if expired {
        state.inflight = state.inflight.checked_sub(1).ok_or_else(|| {
            "CORRUPT_OUTBOX_FAIRNESS: retried lease is absent from in-flight counter".to_string()
        })?;
    }
    let (delivery, lease) = build_lease(write, at, current)?;
    let bytes = encode_row(&delivery, "outbox delivery row")?;
    state.inflight = state
        .inflight
        .checked_add(1)
        .ok_or_else(|| "CORRUPT_OUTBOX_FAIRNESS: in-flight counter overflow".to_string())?;
    Ok((bytes, lease))
}

/// Apply a dead-lettered head row's transition to the durable fairness
/// counters and the in-memory transition report, returning its encoded
/// terminal row for the caller to insert.
fn apply_dead_letter(
    dead: OutboxDelivery,
    expired: bool,
    state: &mut OutboxConsumerState,
    dead_lettered: &mut Vec<OutboxPosition>,
) -> Result<Vec<u8>, String> {
    record_dead_letter(state, expired)?;
    dead_lettered.push(dead.position.clone());
    encode_row(&dead, "outbox delivery row")
}

/// Read the primary row and build the next lease over it, packaged as both
/// the durable delivery row to write and the lease to hand the caller.
fn build_lease<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    at: &LeaseAt<'_>,
    current: Option<OutboxDelivery>,
) -> Result<(OutboxDelivery, MutationOutboxLease), String> {
    let record = read_outbox_row_in_write(write, at.scope, at.position)?;
    let delivery = next_lease(
        current,
        at.identity,
        at.consumer,
        at.position,
        at.budget,
        at.is_head,
    );
    let lease = MutationOutboxLease {
        record,
        consumer: delivery.consumer.clone(),
        lease_epoch: delivery.lease_epoch,
        lease_until_ms: delivery.lease_until_ms,
        attempt: delivery.attempt,
    };
    Ok((delivery, lease))
}

/// Record one head row's terminal dead-letter outcome in the durable fairness
/// counters. `held` is whether a live lease was believed outstanding for it
/// (a fresh row never leased before is not), since the in-flight counter only
/// counts leases still believed held.
pub(crate) fn record_dead_letter(state: &mut OutboxConsumerState, held: bool) -> Result<(), String> {
    if held {
        state.inflight = state.inflight.checked_sub(1).ok_or_else(|| {
            "CORRUPT_OUTBOX_FAIRNESS: dead-lettered lease is absent from counter".to_string()
        })?;
    }
    state.dead_lettered = state
        .dead_lettered
        .checked_add(1)
        .ok_or_else(|| "CORRUPT_OUTBOX_FAIRNESS: dead-letter counter overflow".to_string())?;
    Ok(())
}

/// The dead-letter row for an event that has exhausted its retry class, or
/// `None` while it still has attempts left.
///
/// A dead-lettered row is resolved, so the contiguous prefix and the ordering
/// gate both pass it and the consumer's stream continues. It is recorded and
/// counted, never silently skipped. Called only for the stream head; see
/// [`install_leases`].
fn dead_letter(
    current: Option<&OutboxDelivery>,
    identity: &MutationScopeIdentity,
    consumer: &str,
    position: &OutboxPosition,
    budget: &OutboxClaimBudget,
) -> Option<OutboxDelivery> {
    let existing = current?;
    if existing.attempt < MAX_DELIVERY_ATTEMPTS || existing.leased_at(budget.now_ms()) {
        return None;
    }
    let mut dead = existing.clone();
    dead.identity = identity.clone();
    dead.consumer = consumer.to_string();
    dead.position = position.clone();
    dead.lease_until_ms = 0;
    dead.dead_lettered_at_ms = Some(budget.now_ms());
    Some(dead)
}

/// The next lease over one row: a strictly greater epoch always, and one more
/// attempt only when `is_head` -- a successor's attempt is left exactly as it
/// was (X10-R1).
fn next_lease(
    current: Option<OutboxDelivery>,
    identity: &MutationScopeIdentity,
    consumer: &str,
    position: &OutboxPosition,
    budget: &OutboxClaimBudget,
    is_head: bool,
) -> OutboxDelivery {
    let (epoch, attempt) = current
        .map(|existing| (existing.lease_epoch, existing.attempt))
        .unwrap_or((0, 0));
    let attempt = head_only_attempt(attempt, is_head);
    OutboxDelivery {
        schema_version: MUTATION_BATCH_VERSION,
        identity: identity.clone(),
        consumer: consumer.to_string(),
        position: position.clone(),
        lease_epoch: epoch.saturating_add(1),
        lease_until_ms: budget.now_ms().saturating_add(budget.lease_ms()),
        attempt,
        delivered_at_ms: None,
        dead_lettered_at_ms: None,
    }
}

/// A successor's attempt is left exactly as it was; only the head's
/// increments (X10-R1).
fn head_only_attempt(attempt: u32, is_head: bool) -> u32 {
    if is_head {
        attempt.saturating_add(1)
    } else {
        attempt
    }
}

/// Reclaim a bounded number of delivered delivery rows strictly before the
/// resolved prefix. Dead-letter rows stay durable as retry evidence.
///
/// A claim scan starts at `resolved_through`, so a delivered row before it can
/// never be examined again: the prefix already proves it was resolved. The
/// durable prune cursor also moves past retained dead-letter evidence, so only
/// the intentional dead-letter history remains in the delivery table.
fn prune_resolved<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
    acked_through: Option<&OutboxPosition>,
    resolved_through: Option<&OutboxPosition>,
) -> Result<(), String> {
    let Some(boundary) = resolved_through else {
        return Ok(());
    };
    let topic = subscribed_topic(write, scope, consumer)?;
    let mut cursor = read_prune_cursor(write, scope, &topic, consumer, identity)?;
    if cursor.complete
        && cursor
            .boundary
            .as_ref()
            .is_some_and(|previous| boundary <= previous)
    {
        return Ok(());
    }
    if cursor.complete {
        cursor.complete = false;
        cursor.boundary = None;
    }
    let page = scan_validated_page(
        write,
        scope,
        &topic,
        identity,
        cursor.position.as_ref(),
        MAX_PRUNE_ROWS_PER_CLAIM,
    )?;
    let mut stale = Vec::new();
    let mut deliveries = write.scoped_table(OUTBOX_DELIVERIES)?;
    let mut last_examined = cursor.position.clone();
    let mut reached_boundary = false;
    for entry in page.entries {
        if entry.position >= *boundary {
            reached_boundary = true;
            break;
        }
        let position = entry.position;
        last_examined = Some(position.clone());
        let delivery = deliveries
            .get((
                scope,
                consumer,
                position.batch_id.as_str(),
                position.ordinal,
            ))?
            .map(|value| decode_row::<OutboxDelivery>(value.value()))
            .transpose()?;
        let Some(delivery) = delivery else {
            continue;
        };
        validate_stamp(&delivery.identity, identity)?;
        validate_delivery_key(&delivery, consumer, &position)?;
        validate_delivery_state(&delivery, acked_through)?;
        if delivery.delivered() {
            stale.push((position.batch_id, position.ordinal));
        }
    }
    for (batch_id, ordinal) in stale {
        deliveries.remove((scope, consumer, batch_id.as_str(), ordinal))?;
    }
    cursor.position = last_examined;
    cursor.complete = reached_boundary || !page.truncated;
    cursor.boundary = cursor.complete.then(|| boundary.clone());
    write_prune_cursor(write, scope, consumer, &cursor)?;
    Ok(())
}

/// Release one held lease so the row is immediately re-claimable.
///
/// The lease is invalidated by zeroing `lease_until_ms`, and an ack must
/// present the exact `lease_until_ms` it was issued -- that equality, not the
/// epoch, is what makes a released lease unacknowledgeable.
pub(crate) fn release<D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    owner: &OwnedStoreHandle<D>,
    lease: &MutationOutboxLease,
) -> Result<(), String> {
    validate_consumer(&lease.consumer)?;
    let write = AdmittedMutation::open(authority, owner)?;
    match release_in(&write, owner.identity(), lease) {
        Ok(()) => write.commit(),
        Err(error) => {
            write.abort()?;
            Err(error)
        }
    }
}

fn release_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    lease: &MutationOutboxLease,
) -> Result<(), String> {
    ensure_not_graft_fenced(write, identity)?;
    let scope = ledger_scope_key(identity);
    crate::outbox::cursor::refuse_if_rewind_pending(write, &scope, &lease.consumer, identity)?;
    let key = (
        scope.as_str(),
        lease.consumer.as_str(),
        lease.record.batch_id.as_str(),
        lease.record.ordinal,
    );
    let mut delivery = {
        let deliveries = write.scoped_table(OUTBOX_DELIVERIES)?;
        let found = deliveries
            .get(key)?
            .map(|value| decode_row::<OutboxDelivery>(value.value()))
            .transpose()?;
        found.ok_or_else(|| "outbox lease is not durably claimed".to_string())?
    };
    validate_stamp(&delivery.identity, identity)?;
    validate_delivery_event(
        &delivery,
        &lease.consumer,
        &lease.record.batch_id,
        lease.record.ordinal,
    )?;
    let topic = subscribed_topic(write, &scope, &lease.consumer)?;
    crate::outbox::cursor::read_cursor_in_write(write, &scope, &lease.consumer, identity)?;
    index::validate_position_in_write(write, &scope, &topic, identity, &delivery.position)
        .map_err(|error| format!("CORRUPT_OUTBOX_DELIVERY: {error}"))?;
    let claim_cursor = read_claim_cursor(write, &scope, &lease.consumer, identity)?;
    validate_claim_cursor(write, &scope, &topic, identity, &claim_cursor)?;
    validate_delivery_state(&delivery, claim_cursor.acked_through.as_ref())
        .map_err(|error| format!("CORRUPT_OUTBOX_DELIVERY: {error}"))?;
    if delivery.resolved() {
        return Err("STALE_OUTBOX_LEASE: event was already delivered".to_string());
    }
    if delivery.lease_epoch != lease.lease_epoch {
        return Err("STALE_OUTBOX_LEASE: consumer or epoch was superseded".to_string());
    }
    // A lease that was already zeroed (released or expired) is not counted in
    // flight, so giving it back must not decrement the counter twice.
    let held = delivery.lease_until_ms > 0;
    delivery.lease_until_ms = 0;
    write.scoped_table(OUTBOX_DELIVERIES)?.insert(
        key,
        encode_row(&delivery, "outbox delivery row")?.as_slice(),
    )?;
    if held {
        adjust_inflight(write, &scope, &lease.consumer, identity, -1)?;
    }
    Ok(())
}

/// Zero a bounded number of expired leases, so the queue's reported in-flight
/// count agrees with what a claim would find.
pub(crate) fn expire<D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    owner: &OwnedStoreHandle<D>,
    consumer: &str,
    now_ms: u64,
) -> Result<u32, String> {
    validate_consumer(consumer)?;
    let write = AdmittedMutation::open(authority, owner)?;
    match expire_in(&write, owner.identity(), consumer, now_ms) {
        Ok(expired) => {
            write.commit()?;
            Ok(expired)
        }
        Err(error) => {
            write.abort()?;
            Err(error)
        }
    }
}

fn expire_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    consumer: &str,
    now_ms: u64,
) -> Result<u32, String> {
    ensure_not_graft_fenced(write, identity)?;
    let scope = ledger_scope_key(identity);
    let topic = subscribed_topic(write, &scope, consumer)?;
    crate::outbox::cursor::read_cursor_in_write(write, &scope, consumer, identity)?;
    let claim_cursor = read_claim_cursor(write, &scope, consumer, identity)?;
    validate_claim_cursor(write, &scope, &topic, identity, &claim_cursor)?;
    let cursor = read_expiry_cursor(write, &scope, consumer, identity)?;
    if let Some(position) = cursor.position.as_ref() {
        index::validate_position_in_write(write, &scope, &topic, identity, position)
            .map_err(|error| format!("CORRUPT_OUTBOX_EXPIRY: {error}"))?;
    }
    let page = scan_validated_page(
        write,
        &scope,
        &topic,
        identity,
        cursor.position.as_ref(),
        MAX_CLAIM_SCAN_ROWS,
    )?;
    if page.entries.is_empty() {
        // We reached EOF. The next call starts a new bounded sweep; keeping
        // this reset in the same transaction makes the wrap explicit and
        // prevents an early lexical delivery prefix from pinning later index
        // positions forever.
        write_expiry_cursor(write, &scope, consumer, identity, None)?;
        return Ok(0);
    }
    let mut stale = Vec::new();
    let mut deliveries = write.scoped_table(OUTBOX_DELIVERIES)?;
    for entry in &page.entries {
        let position = &entry.position;
        let delivery = deliveries
            .get((
                scope.as_str(),
                consumer,
                position.batch_id.as_str(),
                position.ordinal,
            ))?
            .map(|value| decode_row::<OutboxDelivery>(value.value()))
            .transpose()?;
        let Some(delivery) = delivery else {
            continue;
        };
        validate_stamp(&delivery.identity, identity)?;
        validate_delivery_key(&delivery, consumer, position)?;
        validate_delivery_state(&delivery, claim_cursor.acked_through.as_ref())?;
        if !delivery.resolved() && delivery.lease_until_ms != 0 && delivery.lease_until_ms <= now_ms
        {
            stale.push(delivery);
        }
    }
    let expired = stale.len() as u32;
    for mut delivery in stale {
        delivery.lease_until_ms = 0;
        let key = (
            scope.as_str(),
            consumer,
            delivery.position.batch_id.as_str(),
            delivery.position.ordinal,
        );
        deliveries.insert(
            key,
            encode_row(&delivery, "outbox delivery row")?.as_slice(),
        )?;
    }
    adjust_inflight(write, &scope, consumer, identity, -(i64::from(expired)))?;
    // A non-truncated page reached EOF, so the next sweep must begin at the
    // first index row. Retaining its last position would make a row that was
    // live during this pass invisible until a later wraparound call.
    let next_cursor = page
        .truncated
        .then(|| page.entries.last().map(|entry| entry.position.clone()))
        .flatten();
    write_expiry_cursor(write, &scope, consumer, identity, next_cursor)?;
    Ok(expired)
}

fn expiry_cursor_key(consumer: &str) -> String {
    format!("{EXPIRY_CURSOR_PREFIX}{consumer}")
}

fn read_expiry_cursor<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
) -> Result<ExpiryCursor, String> {
    let key = expiry_cursor_key(consumer);
    let table = write.scoped_table(OUTBOX_CLAIM_CURSORS)?;
    let row = table
        .get((scope, key.as_str()))?
        .map(|value| decode_row::<ExpiryCursor>(value.value()))
        .transpose()?;
    let Some(cursor) = row else {
        return Ok(ExpiryCursor {
            schema_version: MUTATION_BATCH_VERSION,
            identity: identity.clone(),
            consumer: consumer.to_string(),
            position: None,
        });
    };
    validate_stamp(&cursor.identity, identity)?;
    if cursor.schema_version != MUTATION_BATCH_VERSION {
        return Err("CORRUPT_OUTBOX_EXPIRY: unsupported row schema".to_string());
    }
    if cursor.consumer != consumer {
        return Err("CORRUPT_OUTBOX_EXPIRY: consumer does not match its key".to_string());
    }
    Ok(cursor)
}

fn write_expiry_cursor<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
    position: Option<OutboxPosition>,
) -> Result<(), String> {
    let row = ExpiryCursor {
        schema_version: MUTATION_BATCH_VERSION,
        identity: identity.clone(),
        consumer: consumer.to_string(),
        position,
    };
    let key = expiry_cursor_key(consumer);
    write.scoped_table(OUTBOX_CLAIM_CURSORS)?.insert(
        (scope, key.as_str()),
        encode_row(&row, "outbox expiry cursor")?.as_slice(),
    )
}

fn prune_cursor_key(consumer: &str) -> String {
    format!("{PRUNE_CURSOR_PREFIX}{consumer}")
}

fn read_prune_cursor<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    topic: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
) -> Result<PruneCursor, String> {
    let key = prune_cursor_key(consumer);
    let stored = {
        let table = write.scoped_table(OUTBOX_CLAIM_CURSORS)?;
        let value = table
            .get((scope, key.as_str()))?
            .map(|value| decode_row::<PruneCursor>(value.value()));
        value.transpose()?
    };
    let Some(cursor) = stored else {
        return Ok(PruneCursor {
            schema_version: MUTATION_BATCH_VERSION,
            identity: identity.clone(),
            consumer: consumer.to_string(),
            position: None,
            boundary: None,
            complete: false,
        });
    };
    validate_stamp(&cursor.identity, identity)?;
    if cursor.schema_version != MUTATION_BATCH_VERSION {
        return Err("CORRUPT_OUTBOX_PRUNE: unsupported row schema".to_string());
    }
    if cursor.consumer != consumer {
        return Err("CORRUPT_OUTBOX_PRUNE: consumer does not match its key".to_string());
    }
    if cursor.complete != cursor.boundary.is_some() {
        return Err("CORRUPT_OUTBOX_PRUNE: completion does not match boundary".to_string());
    }
    if cursor
        .position
        .as_ref()
        .zip(cursor.boundary.as_ref())
        .is_some_and(|(position, boundary)| position >= boundary)
    {
        return Err("CORRUPT_OUTBOX_PRUNE: cursor is past its boundary".to_string());
    }
    for position in [cursor.position.as_ref(), cursor.boundary.as_ref()]
        .into_iter()
        .flatten()
    {
        index::validate_position_in_write(write, scope, topic, identity, position)
            .map_err(|error| format!("CORRUPT_OUTBOX_PRUNE: {error}"))?;
    }
    Ok(cursor)
}

fn write_prune_cursor<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    cursor: &PruneCursor,
) -> Result<(), String> {
    let key = prune_cursor_key(consumer);
    write.scoped_table(OUTBOX_CLAIM_CURSORS)?.insert(
        (scope, key.as_str()),
        encode_row(cursor, "outbox prune cursor")?.as_slice(),
    )
}

/// Move the durable in-flight counter, in the same transaction as the delivery
/// rows it counts.
pub(crate) fn adjust_inflight<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
    delta: i64,
) -> Result<(), String> {
    let mut state = read_consumer_state(write, scope, consumer, identity)?;
    state.inflight = if delta >= 0 {
        state.inflight.checked_add(
            u32::try_from(delta).map_err(|_| {
                "CORRUPT_OUTBOX_FAIRNESS: in-flight counter delta overflow".to_string()
            })?,
        )
    } else {
        let amount = u32::try_from(delta.unsigned_abs())
            .map_err(|_| "CORRUPT_OUTBOX_FAIRNESS: in-flight counter delta overflow".to_string())?;
        state.inflight.checked_sub(amount)
    }
    .ok_or_else(|| "CORRUPT_OUTBOX_FAIRNESS: in-flight counter underflow/overflow".to_string())?;
    if state.inflight > OUTBOX_QUEUE_CAPACITY {
        return Err("CORRUPT_OUTBOX_FAIRNESS: in-flight count exceeds capacity".to_string());
    }
    write_consumer_state(write, scope, consumer, &state)
}

/// Record one resolved delivery in the durable counters.
pub(crate) fn record_delivered<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
) -> Result<(), String> {
    let mut state = read_consumer_state(write, scope, consumer, identity)?;
    state.delivered = state
        .delivered
        .checked_add(1)
        .ok_or_else(|| "CORRUPT_OUTBOX_FAIRNESS: delivered counter overflow".to_string())?;
    state.inflight = state.inflight.checked_sub(1).ok_or_else(|| {
        "CORRUPT_OUTBOX_FAIRNESS: acknowledged lease is absent from in-flight counter".to_string()
    })?;
    write_consumer_state(write, scope, consumer, &state)
}

pub(crate) fn read_claim_cursor<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
) -> Result<OutboxClaimCursor, String> {
    let table = write.scoped_table(OUTBOX_CLAIM_CURSORS)?;
    let stored = table
        .get((scope, consumer))?
        .map(|value| decode_row::<OutboxClaimCursor>(value.value()))
        .transpose()?;
    match stored {
        Some(cursor) => {
            validate_stamp(&cursor.identity, identity)?;
            if cursor.schema_version != MUTATION_BATCH_VERSION {
                return Err("CORRUPT_OUTBOX_CURSOR: unsupported row schema".to_string());
            }
            if cursor.consumer != consumer {
                return Err("CORRUPT_OUTBOX_CURSOR: consumer does not match its key".to_string());
            }
            Ok(cursor)
        }
        None => Ok(OutboxClaimCursor {
            schema_version: MUTATION_BATCH_VERSION,
            identity: identity.clone(),
            consumer: consumer.to_string(),
            resolved_through: None,
            acked_through: None,
        }),
    }
}

pub(crate) fn read_claim_cursor_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
) -> Result<OutboxClaimCursor, String> {
    let table = read.scoped_table(OUTBOX_CLAIM_CURSORS)?;
    let stored = table
        .get((scope, consumer))?
        .map(|value| decode_row::<OutboxClaimCursor>(value.value()))
        .transpose()?;
    match stored {
        Some(cursor) => {
            validate_stamp(&cursor.identity, identity)?;
            if cursor.schema_version != MUTATION_BATCH_VERSION {
                return Err("CORRUPT_OUTBOX_CURSOR: unsupported row schema".to_string());
            }
            if cursor.consumer != consumer {
                return Err("CORRUPT_OUTBOX_CURSOR: consumer does not match its key".to_string());
            }
            Ok(cursor)
        }
        None => Ok(OutboxClaimCursor {
            schema_version: MUTATION_BATCH_VERSION,
            identity: identity.clone(),
            consumer: consumer.to_string(),
            resolved_through: None,
            acked_through: None,
        }),
    }
}

pub(crate) fn validate_claim_cursor<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    topic: &str,
    identity: &MutationScopeIdentity,
    cursor: &OutboxClaimCursor,
) -> Result<(), String> {
    for position in [
        cursor.resolved_through.as_ref(),
        cursor.acked_through.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        index::validate_position_in_write(write, scope, topic, identity, position)
            .map_err(|error| format!("CORRUPT_OUTBOX_CURSOR: {error}"))?;
    }
    if let Some(position) = cursor.resolved_through.as_ref() {
        let deliveries = write.scoped_table(OUTBOX_DELIVERIES)?;
        let delivery = deliveries
            .get((
                scope,
                cursor.consumer.as_str(),
                position.batch_id.as_str(),
                position.ordinal,
            ))?
            .map(|value| decode_row::<OutboxDelivery>(value.value()))
            .transpose()?
            .ok_or_else(|| {
                "CORRUPT_OUTBOX_CURSOR: resolved boundary has no delivery row".to_string()
            })?;
        validate_stamp(&delivery.identity, identity)
            .map_err(|error| format!("CORRUPT_OUTBOX_CURSOR: {error}"))?;
        validate_delivery_key(&delivery, &cursor.consumer, position)
            .map_err(|error| format!("CORRUPT_OUTBOX_CURSOR: {error}"))?;
        validate_delivery_state(&delivery, cursor.acked_through.as_ref())
            .map_err(|error| format!("CORRUPT_OUTBOX_CURSOR: {error}"))?;
        if !delivery.resolved() {
            return Err(
                "CORRUPT_OUTBOX_CURSOR: resolved boundary delivery is unresolved".to_string(),
            );
        }
    }
    if cursor
        .acked_through
        .as_ref()
        .zip(cursor.resolved_through.as_ref())
        .is_some_and(|(acked, resolved)| acked > resolved)
        || cursor.acked_through.is_some() && cursor.resolved_through.is_none()
    {
        return Err(
            "CORRUPT_OUTBOX_CURSOR: acknowledged position is beyond resolved prefix".to_string(),
        );
    }
    Ok(())
}

pub(crate) fn validate_claim_cursor_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    topic: &str,
    identity: &MutationScopeIdentity,
    cursor: &OutboxClaimCursor,
) -> Result<(), String> {
    for position in [
        cursor.resolved_through.as_ref(),
        cursor.acked_through.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        index::validate_position_in_read(read, scope, topic, identity, position)
            .map_err(|error| format!("CORRUPT_OUTBOX_CURSOR: {error}"))?;
    }
    if let Some(position) = cursor.resolved_through.as_ref() {
        let deliveries = read.scoped_table(OUTBOX_DELIVERIES)?;
        let delivery = deliveries
            .get((
                scope,
                cursor.consumer.as_str(),
                position.batch_id.as_str(),
                position.ordinal,
            ))?
            .map(|value| decode_row::<OutboxDelivery>(value.value()))
            .transpose()?
            .ok_or_else(|| {
                "CORRUPT_OUTBOX_CURSOR: resolved boundary has no delivery row".to_string()
            })?;
        validate_stamp(&delivery.identity, identity)
            .map_err(|error| format!("CORRUPT_OUTBOX_CURSOR: {error}"))?;
        validate_delivery_key(&delivery, &cursor.consumer, position)
            .map_err(|error| format!("CORRUPT_OUTBOX_CURSOR: {error}"))?;
        validate_delivery_state(&delivery, cursor.acked_through.as_ref())
            .map_err(|error| format!("CORRUPT_OUTBOX_CURSOR: {error}"))?;
        if !delivery.resolved() {
            return Err(
                "CORRUPT_OUTBOX_CURSOR: resolved boundary delivery is unresolved".to_string(),
            );
        }
    }
    if cursor
        .acked_through
        .as_ref()
        .zip(cursor.resolved_through.as_ref())
        .is_some_and(|(acked, resolved)| acked > resolved)
        || cursor.acked_through.is_some() && cursor.resolved_through.is_none()
    {
        return Err(
            "CORRUPT_OUTBOX_CURSOR: acknowledged position is beyond resolved prefix".to_string(),
        );
    }
    Ok(())
}

pub(crate) fn write_claim_cursor<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    cursor: &OutboxClaimCursor,
) -> Result<(), String> {
    let bytes = encode_row(cursor, "outbox claim cursor")?;
    write
        .scoped_table(OUTBOX_CLAIM_CURSORS)?
        .insert((scope, consumer), bytes.as_slice())
}

/// Accumulate this claim into the durable scope-local run accounting and totals.
fn accrue(state: &mut OutboxConsumerState, budget: &OutboxClaimBudget, claimed: u32) {
    state.consecutive_claims = if budget.is_consecutive(state.identity.tenant().as_str()) {
        state
            .consecutive_claims
            .saturating_add(claimed)
            .min(budget.consecutive_cap())
    } else {
        claimed
    };
    state.total_claims = state.total_claims.saturating_add(u64::from(claimed));
    state.last_claim_at_ms = budget.now_ms();
}

pub(crate) fn read_consumer_state<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
) -> Result<OutboxConsumerState, String> {
    let table = write.scoped_table(OUTBOX_FAIRNESS)?;
    let stored = table
        .get((scope, consumer))?
        .map(|value| decode_row::<OutboxConsumerState>(value.value()))
        .transpose()?;
    match stored {
        Some(state) => {
            validate_stamp(&state.identity, identity)?;
            if state.schema_version != MUTATION_BATCH_VERSION {
                return Err("CORRUPT_OUTBOX_FAIRNESS: unsupported row schema".to_string());
            }
            if state.consumer != consumer {
                return Err("CORRUPT_OUTBOX_FAIRNESS: consumer does not match its key".to_string());
            }
            if state.inflight > OUTBOX_QUEUE_CAPACITY {
                return Err("CORRUPT_OUTBOX_FAIRNESS: in-flight count exceeds capacity".to_string());
            }
            Ok(state)
        }
        None => Ok(OutboxConsumerState {
            schema_version: MUTATION_BATCH_VERSION,
            identity: identity.clone(),
            consumer: consumer.to_string(),
            consecutive_claims: 0,
            total_claims: 0,
            last_claim_at_ms: 0,
            inflight: 0,
            delivered: 0,
            dead_lettered: 0,
        }),
    }
}

pub(crate) fn write_consumer_state<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    state: &OutboxConsumerState,
) -> Result<(), String> {
    let bytes = encode_row(state, "outbox consumer state")?;
    write
        .scoped_table(OUTBOX_FAIRNESS)?
        .insert((scope, consumer), bytes.as_slice())
}
