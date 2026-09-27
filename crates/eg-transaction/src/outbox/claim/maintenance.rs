//! Bounded delivery pruning and lease release or expiration.

use super::{adjust_inflight, read_claim_cursor, scan_validated_page, validate_claim_cursor};
use crate::admitted::AdmittedMutation;
use crate::outbox::rows::{
    decode_row, encode_row, validate_consumer, validate_delivery_event, validate_delivery_key,
    validate_delivery_state, validate_stamp, OutboxDelivery, OutboxPosition, MAX_CLAIM_SCAN_ROWS,
    MAX_PRUNE_ROWS_PER_CLAIM,
};
use crate::outbox::stream::subscribed_topic;
use crate::outbox::{ensure_not_graft_fenced, index};
use crate::tables::{OUTBOX_CLAIM_CURSORS, OUTBOX_DELIVERIES};
use eg_storage::{ledger_scope_key, MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain};
use eg_types::{MutationOutboxLease, MutationScopeIdentity, MUTATION_BATCH_VERSION};

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

/// Reclaim a bounded number of delivered delivery rows strictly before the
/// resolved prefix. Dead-letter rows stay durable as retry evidence.
///
/// A claim scan starts at `resolved_through`, so a delivered row before it can
/// never be examined again: the prefix already proves it was resolved. The
/// durable prune cursor also moves past retained dead-letter evidence, so only
/// the intentional dead-letter history remains in the delivery table.
pub(super) fn prune_resolved<D: OwnerDomain>(
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
        // A lease-free source supersession retains its terminal row as the
        // exact Raft retry receipt. Ordinary acknowledged rows remain prunable.
        if delivery.delivered() && delivery.lease_epoch != 0 {
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
