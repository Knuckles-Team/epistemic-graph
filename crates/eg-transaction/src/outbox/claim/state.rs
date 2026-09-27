//! Durable claim cursor validation and fairness accounting.

use crate::admitted::AdmittedMutation;
use crate::outbox::index;
use crate::outbox::rows::{
    decode_row, encode_row, validate_delivery_key, validate_delivery_state, validate_stamp,
    OutboxClaimCursor, OutboxConsumerState, OutboxDelivery, OutboxPosition, OUTBOX_QUEUE_CAPACITY,
};
use crate::outbox::OutboxClaimBudget;
use crate::tables::{OUTBOX_CLAIM_CURSORS, OUTBOX_DELIVERIES, OUTBOX_FAIRNESS};
use eg_storage::{OwnerDomain, ScopedRead};
use eg_types::{MutationScopeIdentity, MUTATION_BATCH_VERSION};

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
            validate_stored_claim_cursor(&cursor, identity, consumer)?;
            Ok(cursor)
        }
        None => Ok(empty_claim_cursor(identity, consumer)),
    }
}

pub(crate) fn read_claim_cursor_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
) -> Result<OutboxClaimCursor, String> {
    Ok(
        read_claim_cursor_optional_in_read(read, scope, consumer, identity)?
            .unwrap_or_else(|| empty_claim_cursor(identity, consumer)),
    )
}

/// Read a validated claim cursor without inventing a default row. Queue
/// status needs to distinguish an absent durable cursor from an idle one.
pub(crate) fn read_claim_cursor_optional_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
) -> Result<Option<OutboxClaimCursor>, String> {
    let table = read.scoped_table(OUTBOX_CLAIM_CURSORS)?;
    let stored = table
        .get((scope, consumer))?
        .map(|value| decode_row::<OutboxClaimCursor>(value.value()))
        .transpose()?;
    if let Some(cursor) = &stored {
        validate_stored_claim_cursor(cursor, identity, consumer)?;
    }
    Ok(stored)
}

fn validate_stored_claim_cursor(
    cursor: &OutboxClaimCursor,
    identity: &MutationScopeIdentity,
    consumer: &str,
) -> Result<(), String> {
    validate_stamp(&cursor.identity, identity)?;
    if cursor.schema_version != MUTATION_BATCH_VERSION {
        return Err("CORRUPT_OUTBOX_CURSOR: unsupported row schema".to_string());
    }
    if cursor.consumer != consumer {
        return Err("CORRUPT_OUTBOX_CURSOR: consumer does not match its key".to_string());
    }
    Ok(())
}

fn empty_claim_cursor(identity: &MutationScopeIdentity, consumer: &str) -> OutboxClaimCursor {
    OutboxClaimCursor {
        schema_version: MUTATION_BATCH_VERSION,
        identity: identity.clone(),
        consumer: consumer.to_string(),
        resolved_through: None,
        acked_through: None,
    }
}

macro_rules! load_resolved_boundary {
    ($snapshot:expr, $scope:expr, $cursor:expr, $position:expr) => {{
        let deliveries = $snapshot.scoped_table(OUTBOX_DELIVERIES)?;
        let resolved = deliveries
            .get((
                $scope,
                $cursor.consumer.as_str(),
                $position.batch_id.as_str(),
                $position.ordinal,
            ))?
            .map(|value| decode_row::<OutboxDelivery>(value.value()))
            .transpose()?;
        resolved.ok_or_else(|| {
            "CORRUPT_OUTBOX_CURSOR: resolved boundary has no delivery row".to_string()
        })
    }};
}

pub(crate) fn validate_claim_cursor<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    topic: &str,
    identity: &MutationScopeIdentity,
    cursor: &OutboxClaimCursor,
) -> Result<(), String> {
    validate_claim_cursor_boundaries(
        identity,
        cursor,
        |position| index::validate_position_in_write(write, scope, topic, identity, position),
        |position| load_resolved_boundary!(write, scope, cursor, position),
    )
}

pub(crate) fn validate_claim_cursor_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    topic: &str,
    identity: &MutationScopeIdentity,
    cursor: &OutboxClaimCursor,
) -> Result<(), String> {
    validate_claim_cursor_boundaries(
        identity,
        cursor,
        |position| index::validate_position_in_read(read, scope, topic, identity, position),
        |position| load_resolved_boundary!(read, scope, cursor, position),
    )
}

fn validate_claim_cursor_boundaries(
    identity: &MutationScopeIdentity,
    cursor: &OutboxClaimCursor,
    mut validate_position: impl FnMut(&OutboxPosition) -> Result<(), String>,
    mut read_delivery: impl FnMut(&OutboxPosition) -> Result<OutboxDelivery, String>,
) -> Result<(), String> {
    for position in [
        cursor.resolved_through.as_ref(),
        cursor.acked_through.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        validate_position(position).map_err(|error| format!("CORRUPT_OUTBOX_CURSOR: {error}"))?;
    }
    if let Some(position) = cursor.resolved_through.as_ref() {
        let delivery = read_delivery(position)?;
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
pub(super) fn accrue(state: &mut OutboxConsumerState, budget: &OutboxClaimBudget, claimed: u32) {
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
