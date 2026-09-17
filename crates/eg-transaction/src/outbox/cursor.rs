//! Acknowledgement, and the durable per-consumer projection cursor.
//!
//! `mutation_outbox_cursors` is `(scope, consumer) -> watermark`. The scope key
//! is the binding digest of `(tenant, graph-or-native scope, incarnation)`, so
//! the row IS the `(tenant, scope, projection) -> watermark` the retired shard
//! ledger kept under three separate key components.
//!
//! The cursor advances in the **same transaction** as the delivery row it
//! acknowledges. That is the whole point of putting both in one ledger: a crash
//! between them is not representable, so a projection can never record that it
//! consumed an event it did not, nor consume one twice believing it had not.
//!
//! # One order, taken from the index
//!
//! Every ordering decision here reads the [`OutboxPosition`] the claim stored
//! on the delivery row, and compares it against `acked_through` on the claim
//! cursor. Both are index positions, so the gap check and the advance check
//! cannot disagree -- the earlier design derived the advance check from
//! `CommittedVersion`, which is `None` for a `ControlPlane`/`Lifecycle` scope
//! and so ordered every row of one identically, wedging the consumer on the
//! first out-of-index-order ack.

use crate::admitted::AdmittedMutation;
use crate::outbox::rows::{
    decode_row, encode_row, validate_consumer, validate_delivery_event, validate_delivery_key,
    validate_delivery_state, validate_stamp, OutboxClaimCursor, OutboxDelivery, OutboxPosition,
    MAX_CLAIM_SCAN_ROWS, MAX_DELIVERY_ATTEMPTS,
};
use crate::outbox::stream::{read_outbox_row_in_write, subscribed_topic};
use crate::outbox::{claim, ensure_not_graft_fenced, index, rewind, OutboxRejectReason};
use crate::tables::{OUTBOX, OUTBOX_CURSORS, OUTBOX_DELIVERIES};
use eg_storage::{
    decode_outbox_record, ledger_scope_key, MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain,
    ScopedRead,
};
use eg_types::{
    MutationOutboxLease, MutationProjectionCursor, MutationScopeIdentity, MUTATION_BATCH_VERSION,
};

/// Acknowledge one held lease and advance the consumer's watermark atomically.
pub(crate) fn ack<D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    owner: &OwnedStoreHandle<D>,
    lease: &MutationOutboxLease,
    now_ms: u64,
) -> Result<MutationProjectionCursor, String> {
    validate_consumer(&lease.consumer)?;
    let write = AdmittedMutation::open(authority, owner)?;
    match ack_in(&write, owner.identity(), lease, now_ms) {
        Ok(Acked::Replayed(cursor)) => {
            write.abort()?;
            Ok(cursor)
        }
        Ok(Acked::Advanced(cursor)) => {
            write.commit()?;
            Ok(cursor)
        }
        Err(error) => {
            write.abort()?;
            Err(error)
        }
    }
}

/// What an acknowledgement resolved to: a fresh advance that must commit, or a
/// retry of one that is already durable and writes nothing.
enum Acked {
    Advanced(MutationProjectionCursor),
    Replayed(MutationProjectionCursor),
}

fn ack_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    lease: &MutationOutboxLease,
    now_ms: u64,
) -> Result<Acked, String> {
    match prepare_ack(write, identity, lease, now_ms)? {
        PreparedAck::Replayed(cursor) => Ok(Acked::Replayed(cursor)),
        PreparedAck::Advance(advance) => {
            let PreparedAdvance {
                scope,
                delivery,
                cursor,
                claim_cursor,
            } = *advance;
            commit_ack(
                write,
                &scope,
                lease,
                delivery,
                &cursor,
                &claim_cursor,
                now_ms,
            )?;
            claim::record_delivered(write, &scope, &lease.consumer, identity)?;
            Ok(Acked::Advanced(cursor))
        }
    }
}

/// Validate an acknowledgement against the caller's already-open admitted
/// transaction without writing delivery state or advancing its watermark.
pub(crate) fn validate_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    lease: &MutationOutboxLease,
    now_ms: u64,
) -> Result<(), String> {
    prepare_ack(write, identity, lease, now_ms).map(drop)
}

/// Apply an acknowledgement inside the caller's already-open admitted
/// transaction. The caller owns the final commit or abort, so domain effects,
/// their terminal receipt and this delivery transition remain atomic.
pub(crate) fn ack_in_transaction<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    lease: &MutationOutboxLease,
    now_ms: u64,
) -> Result<MutationProjectionCursor, String> {
    match ack_in(write, identity, lease, now_ms)? {
        Acked::Advanced(cursor) | Acked::Replayed(cursor) => Ok(cursor),
    }
}

/// Everything a validated advance needs in order to write. Named and boxed as
/// one payload because the fields are a single unit -- the delivery row, the
/// cursor it produces and the claim cursor it moves are only ever built and
/// consumed together.
struct PreparedAdvance {
    scope: String,
    delivery: OutboxDelivery,
    cursor: MutationProjectionCursor,
    claim_cursor: OutboxClaimCursor,
}

enum PreparedAck {
    Replayed(MutationProjectionCursor),
    /// Boxed to keep this call-local result small: the advance payload is far
    /// larger than `Replayed` -- it carries a delivery row, a projection cursor
    /// and a claim cursor holding two full [`OutboxPosition`]s
    /// (`resolved_through` and `acked_through`) -- so an unboxed variant would
    /// make every `PreparedAck`, replay included, pay the advance's width.
    /// `PreparedAck` never crosses a durable boundary, so the box has no wire
    /// effect.
    Advance(Box<PreparedAdvance>),
}

/// Perform every acknowledgement check before the first delivery-side write.
fn prepare_ack<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    lease: &MutationOutboxLease,
    now_ms: u64,
) -> Result<PreparedAck, String> {
    let (scope, delivery) = prepare_delivery_op(write, identity, lease, "ack")?;
    let position = delivery.position.clone();
    let durable = read_outbox_row_in_write(write, &scope, &position)?;
    if durable != lease.record {
        return Err("outbox lease record does not match durable event".to_string());
    }
    let current = read_cursor_in_write(write, &scope, &lease.consumer, identity)?;
    if let Some(replayed) = resolve_delivered(&delivery, lease, &current, now_ms)? {
        return Ok(PreparedAck::Replayed(replayed));
    }
    let mut claim_cursor = claim::read_claim_cursor(write, &scope, &lease.consumer, identity)?;
    let topic = subscribed_topic(write, &scope, &lease.consumer)?;
    claim::validate_claim_cursor(write, &scope, &topic, identity, &claim_cursor)?;
    validate_delivery_state(&delivery, claim_cursor.acked_through.as_ref())?;
    require_advance(claim_cursor.acked_through.as_ref(), &position)?;
    require_no_earlier_gap(write, &scope, lease, &claim_cursor, &position)?;
    let cursor = build_cursor(&lease.consumer, &lease.record, now_ms)?;
    validate_stamp(&cursor.identity, identity)?;
    claim_cursor.acked_through = Some(position.clone());
    if claim_cursor
        .resolved_through
        .as_ref()
        .is_none_or(|resolved| position > *resolved)
    {
        claim_cursor.resolved_through = Some(position.clone());
    }
    Ok(PreparedAck::Advance(Box::new(PreparedAdvance {
        scope,
        delivery,
        cursor,
        claim_cursor,
    })))
}

/// Everything an ack or a reject checks before it may even look at the
/// delivery row's own resolution state: consumer validity, graft fencing,
/// rewind fencing, and that the lease's own record matches the caller's
/// route. Shared so the two operations' prologues don't drift apart.
fn prepare_delivery_op<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    lease: &MutationOutboxLease,
    op: &str,
) -> Result<(String, OutboxDelivery), String> {
    validate_consumer(&lease.consumer)?;
    ensure_not_graft_fenced(write, identity)?;
    if lease.record.identity != *identity {
        return Err(format!(
            "outbox {op} route does not match the leased record"
        ));
    }
    lease.record.validate()?;
    let scope = ledger_scope_key(identity);
    refuse_if_rewind_pending(write, &scope, &lease.consumer, identity)?;
    let delivery = read_delivery(write, &scope, lease, identity)?;
    Ok((scope, delivery))
}

/// Fail a superseded, expired or released lease; replay an already-delivered
/// one only when the cursor already names exactly that event.
fn resolve_delivered(
    delivery: &OutboxDelivery,
    lease: &MutationOutboxLease,
    current: &Option<MutationProjectionCursor>,
    now_ms: u64,
) -> Result<Option<MutationProjectionCursor>, String> {
    if delivery.consumer != lease.consumer || delivery.lease_epoch != lease.lease_epoch {
        return Err("STALE_OUTBOX_LEASE: consumer or epoch was superseded".to_string());
    }
    if delivery.dead_lettered() {
        return Err("OUTBOX_DEAD_LETTER: event exhausted its retry class".to_string());
    }
    if delivery.delivered() {
        return match cursor_names(current, lease) {
            Some(cursor) => Ok(Some(cursor)),
            None => Err("STALE_OUTBOX_LEASE: event was already delivered".to_string()),
        };
    }
    if delivery.lease_until_ms <= now_ms || delivery.lease_until_ms != lease.lease_until_ms {
        return Err("STALE_OUTBOX_LEASE: lease expired or was replaced".to_string());
    }
    Ok(None)
}

fn cursor_names(
    current: &Option<MutationProjectionCursor>,
    lease: &MutationOutboxLease,
) -> Option<MutationProjectionCursor> {
    let cursor = current.as_ref()?;
    (cursor.batch_id == lease.record.batch_id && cursor.outbox_ordinal == lease.record.ordinal)
        .then(|| cursor.clone())
}

/// The watermark may only move forward, in the index's order.
fn require_advance(
    acked_through: Option<&OutboxPosition>,
    position: &OutboxPosition,
) -> Result<(), String> {
    match acked_through {
        Some(held) if position <= held => {
            Err("STALE_PROJECTION_CURSOR: event does not advance watermark".to_string())
        }
        _ => Ok(()),
    }
}

/// Every earlier row of this consumer's topic must already be resolved.
///
/// Claims are handed out in commit order, but a consumer may hold several at
/// once and acknowledge them out of order. A watermark that skipped an
/// unresolved predecessor would claim progress the projection has not made, so
/// the gap is refused rather than absorbed. A dead-lettered predecessor counts
/// as resolved: that is what stops one poison event from wedging the stream
/// permanently.
fn require_no_earlier_gap<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    lease: &MutationOutboxLease,
    claim_cursor: &OutboxClaimCursor,
    position: &OutboxPosition,
) -> Result<(), String> {
    let topic = subscribed_topic(write, scope, &lease.consumer)?;
    let page = index::scan_in_write(
        write,
        scope,
        &topic,
        claim_cursor.resolved_through.as_ref(),
        MAX_CLAIM_SCAN_ROWS,
    )?;
    let table = write.scoped_table(OUTBOX_DELIVERIES)?;
    let mut reached_position = false;
    for entry in page.entries {
        index::validate_position_in_write(
            write,
            scope,
            &topic,
            &lease.record.identity,
            &entry.position,
        )
        .map_err(|error| format!("CORRUPT_OUTBOX_INDEX: {error}"))?;
        if entry.position >= *position {
            reached_position = entry.position == *position;
            break;
        }
        let delivery = table
            .get((
                scope,
                lease.consumer.as_str(),
                entry.position.batch_id.as_str(),
                entry.position.ordinal,
            ))?
            .map(|value| decode_row::<OutboxDelivery>(value.value()))
            .transpose()?;
        let resolved = match delivery {
            Some(row) => {
                validate_stamp(&row.identity, &lease.record.identity)?;
                validate_delivery_key(&row, &lease.consumer, &entry.position)?;
                validate_delivery_state(&row, claim_cursor.acked_through.as_ref())?;
                row.resolved()
            }
            None => false,
        };
        if !resolved {
            return Err(format!(
                "OUTBOX_ORDER_GAP: event '{}:{}' is not yet delivered",
                entry.position.batch_id, entry.position.ordinal
            ));
        }
    }
    if !reached_position {
        return Err(
            "OUTBOX_ORDER_GAP: bounded predecessor scan did not reach the acknowledged event"
                .to_string(),
        );
    }
    Ok(())
}

/// Resolve one held lease as a consumer-declared terminal failure.
///
/// Fenced exactly like an acknowledgement -- lease, epoch, expiry and durable
/// -record checks are the same -- but it never moves the watermark, so a
/// superseded, expired, released or already-delivered lease is refused the
/// same way an ack refuses it (X10-T4).
pub(crate) fn reject<D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    owner: &OwnedStoreHandle<D>,
    lease: &MutationOutboxLease,
    reason: OutboxRejectReason,
    now_ms: u64,
) -> Result<(), String> {
    validate_consumer(&lease.consumer)?;
    let write = AdmittedMutation::open(authority, owner)?;
    match reject_in(&write, owner.identity(), lease, reason, now_ms) {
        Ok(()) => write.commit(),
        Err(error) => {
            write.abort()?;
            Err(error)
        }
    }
}

/// Reject inside the caller's already-open admitted transaction, so a
/// domain's own terminal evidence and this resolution commit atomically
/// (X10-T5, mirrors [`ack_in_transaction`]).
pub(crate) fn reject_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    lease: &MutationOutboxLease,
    reason: OutboxRejectReason,
    now_ms: u64,
) -> Result<(), String> {
    // Closed reason codes are the caller's own log, metric and domain
    // evidence; the kernel row never carries one (X10-R6, decision D4).
    let _ = reason;
    let prepared = prepare_reject(write, identity, lease, now_ms)?;
    commit_reject(write, &prepared.scope, lease, prepared.delivery, now_ms)?;
    record_rejected(write, &prepared.scope, &lease.consumer, identity)
}

/// Everything one rejection needs in order to write.
struct PreparedReject {
    scope: String,
    delivery: OutboxDelivery,
}

/// Perform every rejection check before the first delivery-side write. Reuses
/// [`resolve_delivered`]'s lease/epoch/expiry/delivered checks -- the same
/// ones an ack runs -- and adds none of the ordering checks, because a
/// rejection never advances the watermark.
fn prepare_reject<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    lease: &MutationOutboxLease,
    now_ms: u64,
) -> Result<PreparedReject, String> {
    let (scope, delivery) = prepare_delivery_op(write, identity, lease, "reject")?;
    let durable = read_outbox_row_in_write(write, &scope, &delivery.position)?;
    if durable != lease.record {
        return Err("outbox lease record does not match durable event".to_string());
    }
    let current = read_cursor_in_write(write, &scope, &lease.consumer, identity)?;
    if resolve_delivered(&delivery, lease, &current, now_ms)?.is_some() {
        return Err("STALE_OUTBOX_LEASE: event was already delivered".to_string());
    }
    let claim_cursor = claim::read_claim_cursor(write, &scope, &lease.consumer, identity)?;
    validate_delivery_state(&delivery, claim_cursor.acked_through.as_ref())?;
    Ok(PreparedReject { scope, delivery })
}

/// Write one rejected row's terminal dead-letter state: the same terminal
/// shape a bounded-retry exhaustion produces (`rows.rs` invariant unchanged),
/// so every reader of a dead-lettered row stays correct either way.
fn commit_reject<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    lease: &MutationOutboxLease,
    mut delivery: OutboxDelivery,
    now_ms: u64,
) -> Result<(), String> {
    delivery.attempt = delivery.attempt.max(MAX_DELIVERY_ATTEMPTS);
    delivery.lease_until_ms = 0;
    delivery.dead_lettered_at_ms = Some(now_ms);
    let bytes = encode_row(&delivery, "outbox delivery row")?;
    write.scoped_table(OUTBOX_DELIVERIES)?.insert(
        (
            scope,
            lease.consumer.as_str(),
            delivery.position.batch_id.as_str(),
            delivery.position.ordinal,
        ),
        bytes.as_slice(),
    )
}

/// A rejection always holds a live lease at the point it is accepted
/// (`prepare_reject` refused it otherwise), so the fairness counter update is
/// the held-lease case of [`claim::record_dead_letter`] every time.
fn record_rejected<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
) -> Result<(), String> {
    let mut state = claim::read_consumer_state(write, scope, consumer, identity)?;
    claim::record_dead_letter(&mut state, true)?;
    claim::write_consumer_state(write, scope, consumer, &state)
}

/// Refuse a delivery-side write for a consumer whose stream is mid-rewind. No
/// pre-rewind lease may move the watermark the rewind is about to relocate
/// (X10 2.6).
pub(crate) fn refuse_if_rewind_pending<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
) -> Result<(), String> {
    if rewind::read_rewind_cursor(write, scope, consumer, identity)?.is_some() {
        return Err("OUTBOX_REWIND_PENDING: rewind in progress for this consumer".to_string());
    }
    Ok(())
}

/// Move (or remove) one consumer's durable projection cursor and claim
/// watermark to `predecessor` -- the position immediately before a rewind's
/// target, or `None` when the rewind restarts the whole stream.
///
/// Kept here because this module is the one place that writes
/// `OUTBOX_CURSORS`, and [`read_cursor_in_write`] enforces the exact
/// invariant this must leave true: a claim watermark and a projection cursor
/// are set or absent together.
pub(crate) fn rewind_watermark_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
    predecessor: Option<&OutboxPosition>,
    now_ms: u64,
) -> Result<(), String> {
    let mut claim_cursor = claim::read_claim_cursor(write, scope, consumer, identity)?;
    claim_cursor.resolved_through = predecessor.cloned();
    claim_cursor.acked_through = predecessor.cloned();
    match predecessor {
        Some(position) => {
            let record = read_outbox_row_in_write(write, scope, position)?;
            let cursor = build_cursor(consumer, &record, now_ms)?;
            validate_stamp(&cursor.identity, identity)?;
            let bytes = encode_row(&cursor, "outbox projection cursor")?;
            write
                .scoped_table(OUTBOX_CURSORS)?
                .insert((scope, consumer), bytes.as_slice())?;
        }
        None => {
            write
                .scoped_table(OUTBOX_CURSORS)?
                .remove((scope, consumer))?;
        }
    }
    claim::write_claim_cursor(write, scope, consumer, &claim_cursor)
}

pub(crate) fn build_cursor(
    consumer: &str,
    record: &eg_types::MutationOutboxRecord,
    now_ms: u64,
) -> Result<MutationProjectionCursor, String> {
    let cursor = MutationProjectionCursor {
        schema_version: MUTATION_BATCH_VERSION,
        projection: consumer.to_string(),
        identity: record.identity.clone(),
        batch_id: record.batch_id.clone(),
        outbox_ordinal: record.ordinal,
        committed_version: record.committed_version,
        advanced_at_ms: now_ms,
    };
    cursor.validate()?;
    Ok(cursor)
}

/// The delivery row, the watermark and the claim cursor, in one transaction.
fn commit_ack<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    lease: &MutationOutboxLease,
    mut delivery: OutboxDelivery,
    cursor: &MutationProjectionCursor,
    claim_cursor: &OutboxClaimCursor,
    now_ms: u64,
) -> Result<(), String> {
    delivery.delivered_at_ms = Some(now_ms);
    let delivery_bytes = encode_row(&delivery, "outbox delivery row")?;
    let cursor_bytes = encode_row(cursor, "outbox projection cursor")?;
    write.scoped_table(OUTBOX_DELIVERIES)?.insert(
        (
            scope,
            lease.consumer.as_str(),
            delivery.position.batch_id.as_str(),
            delivery.position.ordinal,
        ),
        delivery_bytes.as_slice(),
    )?;
    write
        .scoped_table(OUTBOX_CURSORS)?
        .insert((scope, lease.consumer.as_str()), cursor_bytes.as_slice())?;
    claim::write_claim_cursor(write, scope, &lease.consumer, claim_cursor)
}

fn read_delivery<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    lease: &MutationOutboxLease,
    identity: &MutationScopeIdentity,
) -> Result<OutboxDelivery, String> {
    let table = write.scoped_table(OUTBOX_DELIVERIES)?;
    let delivery = table
        .get((
            scope,
            lease.consumer.as_str(),
            lease.record.batch_id.as_str(),
            lease.record.ordinal,
        ))?
        .map(|value| decode_row::<OutboxDelivery>(value.value()))
        .transpose()?
        .ok_or_else(|| "outbox lease is not durably claimed".to_string())?;
    validate_stamp(&delivery.identity, identity)?;
    validate_delivery_event(
        &delivery,
        &lease.consumer,
        &lease.record.batch_id,
        lease.record.ordinal,
    )?;
    let topic = subscribed_topic(write, scope, &lease.consumer)?;
    index::validate_position_in_write(write, scope, &topic, identity, &delivery.position)
        .map_err(|error| format!("CORRUPT_OUTBOX_DELIVERY: {error}"))?;
    Ok(delivery)
}

pub(crate) fn read_cursor_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
) -> Result<Option<MutationProjectionCursor>, String> {
    let table = write.scoped_table(OUTBOX_CURSORS)?;
    let cursor = table
        .get((scope, consumer))?
        .map(|value| decode_row::<MutationProjectionCursor>(value.value()))
        .transpose()?;
    if let Some(cursor) = &cursor {
        validate_stamp(&cursor.identity, identity)?;
        if cursor.schema_version != MUTATION_BATCH_VERSION {
            return Err("CORRUPT_OUTBOX_CURSOR: unsupported row schema".to_string());
        }
        validate_projection_cursor_in_write(write, scope, consumer, cursor)?;
    } else if claim::read_claim_cursor(write, scope, consumer, identity)?
        .acked_through
        .is_some()
    {
        return Err("CORRUPT_OUTBOX_CURSOR: claim watermark has no projection cursor".to_string());
    }
    Ok(cursor)
}

/// One consumer's durable watermark on this read's bound scope.
pub(crate) fn read_cursor<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    consumer: &str,
) -> Result<Option<MutationProjectionCursor>, String> {
    validate_consumer(consumer)?;
    let scope = ledger_scope_key(read.scope());
    let table = read.scoped_table(OUTBOX_CURSORS)?;
    let cursor = table
        .get((scope.as_str(), consumer))?
        .map(|value| decode_row::<MutationProjectionCursor>(value.value()))
        .transpose()?;
    if let Some(cursor) = &cursor {
        validate_stamp(&cursor.identity, read.scope())?;
        if cursor.schema_version != MUTATION_BATCH_VERSION {
            return Err("CORRUPT_OUTBOX_CURSOR: unsupported row schema".to_string());
        }
        if cursor.projection != consumer {
            return Err("CORRUPT_OUTBOX_CURSOR: projection does not match its key".to_string());
        }
        let topic = crate::outbox::stream::subscribed_topic_in_read(read, &scope, consumer)?
            .ok_or_else(|| "outbox cursor has no durable subscription".to_string())?;
        validate_projection_cursor_in_read(read, &scope, consumer, &topic, cursor)?;
    } else if claim::read_claim_cursor_in_read(read, &scope, consumer, read.scope())?
        .acked_through
        .is_some()
    {
        return Err("CORRUPT_OUTBOX_CURSOR: claim watermark has no projection cursor".to_string());
    }
    Ok(cursor)
}

fn validate_projection_cursor_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    cursor: &MutationProjectionCursor,
) -> Result<(), String> {
    if cursor.schema_version != MUTATION_BATCH_VERSION {
        return Err("CORRUPT_OUTBOX_CURSOR: unsupported row schema".to_string());
    }
    if cursor.projection != consumer {
        return Err("CORRUPT_OUTBOX_CURSOR: projection does not match its key".to_string());
    }
    let topic = subscribed_topic(write, scope, consumer)?;
    let record = write
        .scoped_table(OUTBOX)?
        .get((scope, cursor.batch_id.as_str(), cursor.outbox_ordinal))?
        .map(|value| decode_outbox_record(value.value()))
        .transpose()?
        .ok_or_else(|| "CORRUPT_OUTBOX_CURSOR: cursor names a missing event".to_string())?;
    record
        .validate()
        .map_err(|error| format!("CORRUPT_OUTBOX_CURSOR: {error}"))?;
    if record.batch_id != cursor.batch_id
        || record.ordinal != cursor.outbox_ordinal
        || record.identity != cursor.identity
        || record.intent.topic != topic
        || record.committed_version != cursor.committed_version
    {
        return Err("CORRUPT_OUTBOX_CURSOR: cursor does not match its event".to_string());
    }
    let position =
        index::position_for_record_in_write(write, scope, &topic, &record)?.ok_or_else(|| {
            "CORRUPT_OUTBOX_CURSOR: cursor event is absent from the topic index".to_string()
        })?;
    let claim_cursor = claim::read_claim_cursor(write, scope, consumer, &cursor.identity)?;
    claim::validate_claim_cursor(write, scope, &topic, &cursor.identity, &claim_cursor)?;
    if claim_cursor.acked_through.as_ref() != Some(&position) {
        return Err(
            "CORRUPT_OUTBOX_CURSOR: projection cursor disagrees with claim watermark".to_string(),
        );
    }
    Ok(())
}

fn validate_projection_cursor_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    consumer: &str,
    topic: &str,
    cursor: &MutationProjectionCursor,
) -> Result<(), String> {
    if cursor.schema_version != MUTATION_BATCH_VERSION {
        return Err("CORRUPT_OUTBOX_CURSOR: unsupported row schema".to_string());
    }
    let record = read
        .scoped_table(OUTBOX)?
        .get((scope, cursor.batch_id.as_str(), cursor.outbox_ordinal))?
        .map(|value| decode_outbox_record(value.value()))
        .transpose()?
        .ok_or_else(|| "CORRUPT_OUTBOX_CURSOR: cursor names a missing event".to_string())?;
    record
        .validate()
        .map_err(|error| format!("CORRUPT_OUTBOX_CURSOR: {error}"))?;
    if record.batch_id != cursor.batch_id
        || record.ordinal != cursor.outbox_ordinal
        || record.identity != cursor.identity
        || record.intent.topic != topic
        || record.committed_version != cursor.committed_version
    {
        return Err("CORRUPT_OUTBOX_CURSOR: cursor does not match its event".to_string());
    }
    let position =
        index::position_for_record_in_read(read, scope, topic, &record)?.ok_or_else(|| {
            "CORRUPT_OUTBOX_CURSOR: cursor event is absent from the topic index".to_string()
        })?;
    let claim_table = read.scoped_table(crate::tables::OUTBOX_CLAIM_CURSORS)?;
    let claim_cursor = claim_table
        .get((scope, consumer))?
        .map(|value| decode_row::<OutboxClaimCursor>(value.value()))
        .transpose()?
        .ok_or_else(|| {
            "CORRUPT_OUTBOX_CURSOR: projection cursor has no claim watermark".to_string()
        })?;
    validate_stamp(&claim_cursor.identity, read.scope())?;
    if claim_cursor.schema_version != MUTATION_BATCH_VERSION || claim_cursor.consumer != consumer {
        return Err("CORRUPT_OUTBOX_CURSOR: malformed claim watermark".to_string());
    }
    claim::validate_claim_cursor_in_read(read, scope, topic, read.scope(), &claim_cursor)?;
    if claim_cursor.acked_through.as_ref() != Some(&position) {
        return Err(
            "CORRUPT_OUTBOX_CURSOR: projection cursor disagrees with claim watermark".to_string(),
        );
    }
    Ok(())
}
