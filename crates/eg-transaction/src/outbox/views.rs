//! The operator and consumer surface over any owner's mutation outbox
//! (X10, PX10b items 7 and 8).
//!
//! The one place the kernel's delivery rules become the wire views of
//! `Method::MutationOutbox`, the consumer-declared `reject` and the operator
//! `rewind`. Every owner -- a graph shard, the Agent Library, the jobs store, a
//! semantic binding, a SQL catalog scope -- runs these SAME generic functions
//! over its own `(MutationKernel, OwnedStoreHandle)`, so a new outbox owner
//! gets the whole surface by providing that pair.

use eg_storage::{OwnedStoreHandle, OwnerDomain, ScopedRead};

use crate::kernel::MutationKernel;
use crate::outbox::{
    OutboxDelivery, OutboxPosition, OutboxRejectReason, OutboxRewindTarget, OutboxStatus,
};
use eg_types::mutation_batch::MutationOutboxLease;
use eg_types::mutation_outbox::{
    MutationOutboxStatusView, OutboxConsumerStatus, OutboxDeadLetterPage, OutboxDeadLetterView,
    OutboxHeadView, OutboxPositionView, OutboxRewindReceipt, RewindTarget,
    MUTATION_OUTBOX_VIEW_SCHEMA_VERSION,
};

/// Most bounded rewind transactions one operator call runs before it answers
/// `completed: false` and asks to be called again.
pub const MAX_REWIND_STEPS_PER_CALL: u32 = 64;

/// A read-only view of one consumer's stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxView {
    Status {
        consumer: String,
    },
    DeadLetters {
        consumer: String,
        after: Option<OutboxPosition>,
        limit: u32,
    },
}

/// What a view answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxViewAnswer {
    Status(MutationOutboxStatusView),
    DeadLetters(OutboxDeadLetterPage),
}

/// A delivery-side write on one consumer's stream.
#[derive(Debug, Clone, PartialEq)]
pub enum OutboxWrite {
    /// Consumer-declared terminal failure of one held lease.
    Reject {
        lease: Box<MutationOutboxLease>,
        reason: OutboxRejectReason,
        now_ms: u64,
    },
    /// Operator re-delivery from a position, in order.
    Rewind {
        consumer: String,
        to: RewindTarget,
        now_ms: u64,
    },
}

/// What a delivery-side write did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxWriteReply {
    Rejected,
    Rewound(OutboxRewindReceipt),
}

/// Answer one view from a snapshot of the owner's scope.
pub fn read_outbox_view<D: OwnerDomain>(
    kernel: &MutationKernel,
    read: &ScopedRead<'_, D>,
    view: &OutboxView,
    now_ms: u64,
) -> Result<OutboxViewAnswer, String> {
    match view {
        OutboxView::Status { consumer } => {
            let status = crate::outbox::outbox_status(read, consumer, now_ms)?;
            Ok(OutboxViewAnswer::Status(status_view(status)))
        }
        OutboxView::DeadLetters {
            consumer,
            after,
            limit,
        } => {
            let page = kernel.outbox_dead_letters(read, consumer, after.as_ref(), *limit)?;
            let mut items = Vec::with_capacity(page.rows.len());
            for row in &page.rows {
                items.push(dead_letter_view(read, row)?);
            }
            let next_after = page
                .truncated
                .then(|| page.rows.last().map(|row| position_view(&row.position)))
                .flatten();
            Ok(OutboxViewAnswer::DeadLetters(OutboxDeadLetterPage {
                schema_version: MUTATION_OUTBOX_VIEW_SCHEMA_VERSION,
                consumer: consumer.clone(),
                items: eg_types::contract::BoundedVec::new(items)?,
                next_after,
            }))
        }
    }
}

/// Apply one delivery-side write to the owner's scope.
pub fn operate_outbox<D: OwnerDomain>(
    kernel: &MutationKernel,
    owner: &OwnedStoreHandle<D>,
    write: OutboxWrite,
) -> Result<OutboxWriteReply, String> {
    match write {
        OutboxWrite::Reject {
            lease,
            reason,
            now_ms,
        } => {
            kernel.outbox_reject(owner, &lease, reason, now_ms)?;
            Ok(OutboxWriteReply::Rejected)
        }
        OutboxWrite::Rewind {
            consumer,
            to,
            now_ms,
        } => rewind(kernel, owner, consumer, to, now_ms).map(OutboxWriteReply::Rewound),
    }
}

/// Drive a rewind through at most [`MAX_REWIND_STEPS_PER_CALL`] bounded
/// transactions. A rewind already pending for the consumer is continued, never
/// restarted, so a repeated call with the same target finishes the same one.
fn rewind<D: OwnerDomain>(
    kernel: &MutationKernel,
    owner: &OwnedStoreHandle<D>,
    consumer: String,
    to: RewindTarget,
    now_ms: u64,
) -> Result<OutboxRewindReceipt, String> {
    let target = rewind_target(&to);
    let mut deleted = 0_u64;
    let mut completed = false;
    for _ in 0..MAX_REWIND_STEPS_PER_CALL {
        let outcome = kernel.outbox_rewind(owner, &consumer, target.clone(), now_ms)?;
        deleted = deleted.saturating_add(outcome.deleted);
        if outcome.complete {
            completed = true;
            break;
        }
    }
    Ok(OutboxRewindReceipt {
        schema_version: MUTATION_OUTBOX_VIEW_SCHEMA_VERSION,
        consumer,
        to,
        completed,
        deleted_deliveries: deleted,
    })
}

/// The kernel position a wire position names.
pub fn outbox_position(view: &OutboxPositionView) -> OutboxPosition {
    OutboxPosition {
        sequence: view.sequence,
        created_at_ms: view.created_at_ms,
        batch_id: view.batch_id.clone(),
        ordinal: view.ordinal,
    }
}

fn position_view(position: &OutboxPosition) -> OutboxPositionView {
    OutboxPositionView {
        sequence: position.sequence,
        created_at_ms: position.created_at_ms,
        batch_id: position.batch_id.clone(),
        ordinal: position.ordinal,
    }
}

fn rewind_target(to: &RewindTarget) -> OutboxRewindTarget {
    match to {
        RewindTarget::Start => OutboxRewindTarget::Start,
        RewindTarget::At { position: at } => OutboxRewindTarget::At(outbox_position(at)),
    }
}

/// The generic consumer standing, shared by every outbox view.
pub fn consumer_status(status: &OutboxStatus) -> OutboxConsumerStatus {
    OutboxConsumerStatus {
        consumer: status.consumer.clone(),
        topic: status.topic.clone(),
        live: status.live,
        capacity: status.capacity,
        inflight: status.inflight,
        inflight_is_lower_bound: status.inflight_is_lower_bound,
        pending: status.pending,
        pending_is_lower_bound: status.pending_is_lower_bound,
        delivered: status.delivered,
        dead_lettered: status.dead_lettered,
        oldest_pending_age_ms: status.oldest_pending_age_ms,
        lag_rows: status.lag_rows,
        lag_versions: status.lag_versions,
        saturated: status.saturated,
        index_complete: status.index_complete,
    }
}

fn status_view(status: OutboxStatus) -> MutationOutboxStatusView {
    let head = status.head.as_ref().map(|head| OutboxHeadView {
        position: position_view(&head.position),
        attempt: head.attempt,
        leased: head.leased,
        age_ms: head.age_ms,
    });
    MutationOutboxStatusView {
        schema_version: MUTATION_OUTBOX_VIEW_SCHEMA_VERSION,
        status: consumer_status(&status),
        head,
    }
}

/// One dead letter, naming its topic and schema but carrying only the
/// payload's digest: a listing is an operational read, never a way to read a
/// mutation's content.
fn dead_letter_view<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    row: &OutboxDelivery,
) -> Result<OutboxDeadLetterView, String> {
    use sha2::{Digest, Sha256};

    let record = crate::read::read_outbox(read, &row.position.batch_id)?
        .into_iter()
        .find(|record| record.ordinal == row.position.ordinal)
        .ok_or_else(|| "CORRUPT_OUTBOX: dead letter names no outbox row".to_string())?;
    let event_schema = record
        .intent
        .headers
        .get("event_schema")
        .cloned()
        .unwrap_or_else(|| record.intent.topic.clone());
    Ok(OutboxDeadLetterView {
        position: position_view(&row.position),
        topic: record.intent.topic,
        event_schema,
        attempt: row.attempt,
        payload_digest: format!(
            "sha256:{}",
            hex::encode(Sha256::digest(&record.intent.payload))
        ),
    })
}
