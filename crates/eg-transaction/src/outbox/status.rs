//! What one consumer's queue on one scope is doing, read from a snapshot.
//!
//! DESIGN.md requires every queue to expose liveness, saturation, capacity,
//! inflight count, oldest age, retry/rejection counts and lag. A queue that
//! cannot be observed cannot be operated: overload is allowed to increase
//! stated freshness lag, but it must never be silent -- which is why the
//! pending figure says when it is a lower bound rather than reporting a
//! truncated count as a total.

use crate::outbox::rows::{
    decode_row, validate_delivery_key, validate_delivery_state, validate_stamp, OutboxClaimCursor,
    OutboxConsumerState, OutboxDelivery, OutboxPosition, MAX_CLAIM_SCAN_ROWS, OUTBOX_QUEUE_CAPACITY,
};
use crate::outbox::{claim, cursor, index, stream};
use crate::tables::{OUTBOX_CLAIM_CURSORS, OUTBOX_DELIVERIES, OUTBOX_FAIRNESS};
use eg_storage::{ledger_scope_key, OwnerDomain, ScopedRead};
use eg_types::MutationScopeIdentity;

/// One consumer's observable queue state on one bound scope.
/// Serializable: `Method::SemanticIndex`'s `StageStatus` returns it verbatim,
/// so a connector can see its own queue depth and saturation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutboxStatus {
    /// The consumer this status is about.
    pub consumer: String,
    /// The topic it is durably subscribed to, if it is subscribed at all.
    pub topic: Option<String>,
    /// Liveness: a consumer with no durable subscription claims nothing, and a
    /// scope that shows pending rows against a dead consumer is a real alert.
    pub live: bool,
    /// The bounded queue's per-consumer in-flight capacity.
    pub capacity: u32,
    /// Claimed, unexpired and unresolved rows in the bounded commit-order
    /// page. See `inflight_is_lower_bound` when the page is truncated.
    pub inflight: u32,
    /// Whether `inflight` is not an exact queue-wide observation because the
    /// bounded index page did not reach EOF or legacy index repair is pending.
    /// The composition layer should treat saturation as unknown in that case
    /// unless the durable counter itself is full.
    pub inflight_is_lower_bound: bool,
    /// Committed rows this consumer has not resolved.
    pub pending: u64,
    /// Whether `pending` and `oldest_pending_age_ms` are lower bounds because
    /// the scan hit its page bound.
    pub pending_is_lower_bound: bool,
    /// Rows this consumer has delivered.
    pub delivered: u64,
    /// Rows dead-lettered after exhausting their retry class.
    pub dead_lettered: u64,
    /// Age of the oldest pending row at `now_ms`.
    pub oldest_pending_age_ms: u64,
    /// Lag in rows: the same figure as `pending`, named as the queue metric.
    pub lag_rows: u64,
    /// Lag in versions: how far the watermark trails the scope's authoritative
    /// version.
    pub lag_versions: u64,
    /// Whether a further claim would be refused for want of in-flight room.
    pub saturated: bool,
    /// The consumer's scope-local consecutive-claim accounting. The
    /// composition scheduler carries the cross-scope sweep cap and owns tenant
    /// ordering, weights and restart debt.
    pub consecutive_claims: u32,
    /// Total rows this consumer has ever claimed on this scope.
    pub total_claims: u64,
    /// Whether a requested legacy index backfill has reached EOF. A false
    /// value makes pending/lag figures lower bounds until claims reopen.
    pub index_complete: bool,
    /// The stream head: the first unresolved row in commit order, if any is
    /// within the bounded scan. `None` with `pending == 0` is an idle queue;
    /// `None` with `pending_is_lower_bound` is an unknown head, not an idle
    /// one (X10-R5).
    pub head: Option<OutboxHead>,
}

/// Where one consumer's stream is currently stuck (or waiting), read from a
/// snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutboxHead {
    /// The head row's index position.
    pub position: OutboxPosition,
    /// How many attempts it has spent as the head (X10-R1: a row can only
    /// accumulate an attempt while it is the head).
    pub attempt: u32,
    /// Whether it is currently held by a live, unexpired lease.
    pub leased: bool,
    /// Its age at the snapshot time.
    pub age_ms: u64,
}

/// Read one consumer's queue state on the read's bound scope.
pub(crate) fn status<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    consumer: &str,
    now_ms: u64,
) -> Result<OutboxStatus, String> {
    crate::outbox::rows::validate_consumer(consumer)?;
    let identity = read.scope();
    let scope = ledger_scope_key(identity);
    let topic = stream::subscribed_topic_in_read(read, &scope, consumer)?;
    let state = consumer_state(read, &scope, consumer)?;
    let index_complete = index::backfill_complete_in_read(read, identity)?;
    let _projection_cursor = cursor::read_cursor(read, consumer)?;
    let cursor_row = claim_cursor(read, &scope, consumer)?;
    if let Some(topic) = &topic {
        validate_claim_cursor(read, &scope, topic, identity, cursor_row.as_ref())?;
    }
    let pending = match &topic {
        Some(topic) => pending_rows(read, &scope, topic, consumer, &cursor_row, now_ms)?,
        None => Pending::default(),
    };
    let inflight = if topic.is_some() {
        pending.inflight
    } else {
        state.inflight
    };
    let inflight_is_lower_bound = topic.is_some() && (pending.truncated || !index_complete);
    if topic.is_some()
        && index_complete
        && !pending.truncated
        && pending.inflight_accounted != state.inflight
    {
        return Err(format!(
            "CORRUPT_OUTBOX_FAIRNESS: in-flight counter {} disagrees with delivery rows {}",
            state.inflight, pending.inflight_accounted
        ));
    }
    Ok(OutboxStatus {
        consumer: consumer.to_string(),
        topic: topic.clone(),
        live: topic.is_some(),
        capacity: OUTBOX_QUEUE_CAPACITY,
        inflight,
        inflight_is_lower_bound,
        pending: pending.rows,
        pending_is_lower_bound: pending.truncated || !index_complete,
        delivered: state.delivered,
        dead_lettered: state.dead_lettered,
        oldest_pending_age_ms: pending.oldest_age_ms,
        lag_rows: pending.rows,
        lag_versions: version_lag(read, consumer)?,
        saturated: inflight >= OUTBOX_QUEUE_CAPACITY,
        consecutive_claims: state.consecutive_claims,
        total_claims: state.total_claims,
        index_complete,
        head: pending.head,
    })
}

#[derive(Default)]
struct Pending {
    inflight: u32,
    inflight_accounted: u32,
    rows: u64,
    oldest_age_ms: u64,
    truncated: bool,
    head: Option<OutboxHead>,
}

/// Pending rows, scanned from the consumer's own resolved prefix.
///
/// Scanning from position zero and capping at one page reported `0` for any
/// steady consumer whose first page of history was fully delivered -- a lag
/// figure that reads healthy while the queue is arbitrarily far behind, which
/// is the one thing DESIGN.md forbids a queue metric to do.
fn pending_rows<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    topic: &str,
    consumer: &str,
    cursor_row: &Option<OutboxClaimCursor>,
    now_ms: u64,
) -> Result<Pending, String> {
    let from = cursor_row
        .as_ref()
        .and_then(|cursor| cursor.resolved_through.as_ref());
    let page = index::scan_in_read(read, scope, topic, from, MAX_CLAIM_SCAN_ROWS)?;
    let table = read.scoped_table(OUTBOX_DELIVERIES)?;
    let acked_through = cursor_row
        .as_ref()
        .and_then(|cursor| cursor.acked_through.as_ref());
    let mut pending = Pending {
        truncated: page.truncated,
        ..Pending::default()
    };
    for entry in page.entries {
        index::validate_position_in_read(read, scope, topic, read.scope(), &entry.position)
            .map_err(|error| format!("CORRUPT_OUTBOX_INDEX: {error}"))?;
        let delivery = table
            .get((
                scope,
                consumer,
                entry.position.batch_id.as_str(),
                entry.position.ordinal,
            ))?
            .map(|value| decode_row::<OutboxDelivery>(value.value()))
            .transpose()?;
        let (resolved, attempt, leased) = fold_delivery(
            delivery,
            read.scope(),
            consumer,
            &entry.position,
            acked_through,
            now_ms,
            &mut pending,
        )?;
        if resolved {
            continue;
        }
        note_pending_entry(&mut pending, &entry.position, attempt, leased, now_ms);
    }
    Ok(pending)
}

/// One entry's delivery state, and the two inflight-related counter bumps a
/// present row implies. Extracted so `pending_rows`'s loop stays a flat
/// sequence of named steps instead of one long inline match.
fn fold_delivery(
    delivery: Option<OutboxDelivery>,
    identity: &MutationScopeIdentity,
    consumer: &str,
    position: &OutboxPosition,
    acked_through: Option<&OutboxPosition>,
    now_ms: u64,
    pending: &mut Pending,
) -> Result<(bool, u32, bool), String> {
    let Some(row) = delivery else {
        return Ok((false, 0, false));
    };
    validate_stamp(&row.identity, identity)?;
    validate_delivery_key(&row, consumer, position)?;
    validate_delivery_state(&row, acked_through)?;
    if !row.resolved() && row.lease_until_ms != 0 {
        pending.inflight_accounted = pending.inflight_accounted.saturating_add(1);
    }
    let leased = row.leased_at(now_ms);
    if leased {
        pending.inflight = pending.inflight.saturating_add(1);
    }
    Ok((row.resolved(), row.attempt, leased))
}

/// Record one still-pending row: the stream head (the first one seen) and the
/// running pending/age totals every unresolved row contributes to.
fn note_pending_entry(
    pending: &mut Pending,
    position: &OutboxPosition,
    attempt: u32,
    leased: bool,
    now_ms: u64,
) {
    let age_ms = now_ms.saturating_sub(position.created_at_ms);
    if pending.head.is_none() {
        pending.head = Some(OutboxHead {
            position: position.clone(),
            attempt,
            leased,
            age_ms,
        });
    }
    pending.rows = pending.rows.saturating_add(1);
    pending.oldest_age_ms = pending.oldest_age_ms.max(age_ms);
}

fn consumer_state<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    consumer: &str,
) -> Result<OutboxConsumerState, String> {
    let table = read.scoped_table(OUTBOX_FAIRNESS)?;
    let row = table
        .get((scope, consumer))?
        .map(|value| decode_row::<OutboxConsumerState>(value.value()))
        .transpose()?;
    match row {
        Some(state) => {
            validate_stamp(&state.identity, read.scope())?;
            if state.schema_version != eg_types::MUTATION_BATCH_VERSION {
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
            schema_version: eg_types::MUTATION_BATCH_VERSION,
            identity: read.scope().clone(),
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

fn claim_cursor<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    consumer: &str,
) -> Result<Option<OutboxClaimCursor>, String> {
    let table = read.scoped_table(OUTBOX_CLAIM_CURSORS)?;
    let row = table
        .get((scope, consumer))?
        .map(|value| decode_row::<OutboxClaimCursor>(value.value()))
        .transpose()?;
    if let Some(cursor) = &row {
        validate_stamp(&cursor.identity, read.scope())?;
        if cursor.schema_version != eg_types::MUTATION_BATCH_VERSION {
            return Err("CORRUPT_OUTBOX_CURSOR: unsupported row schema".to_string());
        }
        if cursor.consumer != consumer {
            return Err("CORRUPT_OUTBOX_CURSOR: consumer does not match its key".to_string());
        }
    }
    Ok(row)
}

fn validate_claim_cursor<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    topic: &str,
    identity: &MutationScopeIdentity,
    cursor: Option<&OutboxClaimCursor>,
) -> Result<(), String> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    claim::validate_claim_cursor_in_read(read, scope, topic, identity, cursor)
}

/// How far one consumer's watermark trails the scope's authoritative version.
fn version_lag<D: OwnerDomain>(read: &ScopedRead<'_, D>, consumer: &str) -> Result<u64, String> {
    let authoritative = crate::read::version(read)?;
    // `MutationProjectionCursor::committed_version` is intentionally absent
    // for ControlPlane/Lifecycle batches.  The claim cursor carries the same
    // index position for every scope shape, so its sequence is the one
    // watermark that agrees with delivery order and keeps unversioned scopes
    // from reporting their whole history as permanently lagging.
    let scope = ledger_scope_key(read.scope());
    let watermark = claim_cursor(read, &scope, consumer)?
        .and_then(|cursor| cursor.acked_through.map(|position| position.sequence))
        .unwrap_or(0);
    Ok(authoritative.saturating_sub(watermark))
}
