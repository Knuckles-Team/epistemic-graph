//! The durable rows of the outbox claim/deliver protocol, and the scope-bounded
//! key ranges that reach them.
//!
//! Six ledger tables were declared, censused, bootstrapped and purge-swept by
//! the storage kernel and written by nothing (`read.rs` said so on the record).
//! RF-RULING-007 closed that gap: the claim/deliver/fairness protocol is
//! implemented once, here, in `eg-transaction`. These are its rows.
//!
//! Every row is keyed, in its first key position, by the one
//! [`eg_storage::ledger_scope_key`], exactly as the receipt, idempotency,
//! version, fence and outbox rows are, and every row additionally *carries* the
//! [`MutationScopeIdentity`] it was written under. That is the same stamp
//! recovery validation requires of the content tables: a row resolves its own
//! binding by its own key, and the stamped identity must equal the bound one.
//! It is also what makes a graft (`crate::graft`) verifiable -- the copied row
//! still names the scope it belongs to, in a file that never saw it before.
//!
//! Nothing here is part of a batch's replay identity. The producer side --
//! `MutationOutboxIntent` and the committed `ledger_outbox` row -- is folded
//! into the batch's canonical payload digest and may not be appended to or
//! mutated after compile. The delivery side is this module's, and a claim, a
//! lease, a cursor or a consumer-state row can be written, expired and
//! rewritten without touching what the batch is.

use eg_storage::{decode_ledger_record, encode_bounded};
use eg_types::{MutationScopeIdentity, MUTATION_BATCH_VERSION};
use serde::{Deserialize, Serialize};

/// Greatest `&str` key component in redb's byte order.
///
/// `\u{10FFFF}` encodes as `F4 8F BF BF`, and no valid UTF-8 string may start
/// with a byte above `F4`, so it is a real maximum for a `&str` component --
/// the same sentinel a scope-bounded receipt scan already uses.
pub(crate) const MAX_TEXT_SENTINEL: &str = crate::commit::MAX_BATCH_ID_SENTINEL;

/// How many claimed-but-unresolved rows one consumer may hold on one scope.
///
/// DESIGN.md's medium queue class. The bound is the point: a full queue leaves
/// the durable intention pending and claims nothing, rather than growing an
/// unbounded in-flight set or dropping work.
pub(crate) const OUTBOX_QUEUE_CAPACITY: u32 = 2_048;

/// How many index rows one claim, ack or backfill may examine before it yields.
///
/// DESIGN.md's fast queue class. Every scan in this module is bounded by it,
/// and every caller that hits the bound reports that it did.
pub(crate) const MAX_CLAIM_SCAN_ROWS: usize = 8_192;

/// How many delivery rows one claim may examine and reclaim. Dead-letter rows
/// remain durable evidence even after they become part of the resolved prefix.
///
/// Pruning is amortised across claims rather than done in one sweep so that a
/// single claim's write transaction stays short: it holds the file's one write
/// lock, and a long prune would block every other writer.
pub(crate) const MAX_PRUNE_ROWS_PER_CLAIM: usize = 256;

/// How many times one row may be leased before it is dead-lettered.
///
/// DESIGN.md requires retry classes and a dead-letter/rejection state. Without
/// one, a permanently undeliverable event is re-leased forever AND blocks every
/// later acknowledgement for that consumer, because the watermark may not skip
/// an unresolved predecessor -- an unbounded head-of-line stall from one bad
/// row.
pub(crate) const MAX_DELIVERY_ATTEMPTS: u32 = 16;

/// One position in a scope's commit-ordered outbox index.
///
/// `sequence` is the scope's authoritative version **after** the batch that
/// emitted the row committed -- `ledger_versions` advances by exactly one on
/// every admitted batch, unversioned batches included, so it is a true commit
/// sequence for every scope. That is the one order this module uses: the
/// caller's `created_at_ms` is present only to break a tie and to report an
/// age, never to decide delivery order.
///
/// The previous shape used `CommittedVersion`'s `source`, which is `0` for
/// every row of a `ControlPlane`/`Lifecycle` scope (`CommittedVersion::None`),
/// so the whole order there collapsed onto the caller's wall clock.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxPosition {
    pub sequence: u64,
    pub created_at_ms: u64,
    pub batch_id: String,
    pub ordinal: u32,
}

/// Durable delivery state for one `(consumer, outbox row)` pair.
///
/// The lease epoch is monotonic per pair: every claim increments it, and an ack
/// must present the exact epoch it was issued. Two workers for one consumer are
/// therefore fenced against each other without a lock -- the older epoch simply
/// fails closed.
///
/// The row carries the index `position` it was claimed at because that position
/// is not recoverable from the outbox record alone: an unversioned scope's
/// record has no version at all, and the sequence lives only in the index key.
/// Every ordering decision an ack makes reads it from here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxDelivery {
    pub schema_version: u16,
    pub identity: MutationScopeIdentity,
    pub consumer: String,
    pub position: OutboxPosition,
    pub lease_epoch: u64,
    pub lease_until_ms: u64,
    pub attempt: u32,
    pub delivered_at_ms: Option<u64>,
    pub dead_lettered_at_ms: Option<u64>,
}

impl OutboxDelivery {
    /// Whether this row is currently held by a live, unexpired lease.
    pub(crate) fn leased_at(&self, now_ms: u64) -> bool {
        !self.resolved() && self.lease_until_ms > now_ms
    }

    pub(crate) fn delivered(&self) -> bool {
        self.delivered_at_ms.is_some()
    }

    pub(crate) fn dead_lettered(&self) -> bool {
        self.dead_lettered_at_ms.is_some()
    }

    /// Whether this row is finished with, either way.
    ///
    /// A dead-lettered row counts as resolved for the contiguous prefix and for
    /// the ordering gate: that is what stops one poison event from wedging the
    /// consumer's whole stream. It is recorded, counted and reported, never
    /// silently skipped.
    pub(crate) fn resolved(&self) -> bool {
        self.delivered() || self.dead_lettered()
    }
}

/// Prove that a terminal delivery row has the durable evidence its state
/// claims. A caller-visible `delivered_at_ms` is not sufficient by itself:
/// acknowledgement also advances the claim watermark in the same
/// transaction, so a delivered row must be at or before that watermark.
pub(crate) fn validate_delivery_state(
    delivery: &OutboxDelivery,
    acked_through: Option<&OutboxPosition>,
) -> Result<(), String> {
    let delivered = delivery.delivered();
    let dead_lettered = delivery.dead_lettered();
    if delivered && dead_lettered {
        return Err("CORRUPT_OUTBOX_DELIVERY: row is both delivered and dead-lettered".to_string());
    }
    if delivered && acked_through.is_none_or(|acked| delivery.position > *acked) {
        return Err(
            "CORRUPT_OUTBOX_DELIVERY: delivered row is beyond the acknowledged watermark"
                .to_string(),
        );
    }
    if dead_lettered && (delivery.attempt < MAX_DELIVERY_ATTEMPTS || delivery.lease_until_ms != 0) {
        return Err("CORRUPT_OUTBOX_DELIVERY: invalid dead-letter terminal state".to_string());
    }
    Ok(())
}

/// Where one consumer's scans resume.
///
/// `resolved_through` is the end of the *contiguous resolved prefix*, never
/// merely the last row looked at: a row that was claimed and then released, or
/// leased by a worker that died, is still before any later row, so a cursor
/// that advanced past it would silently drop it. `acked_through` is the last
/// position actually acknowledged, and it is what decides whether a further ack
/// advances -- one order, the index's, for every scope shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxClaimCursor {
    pub schema_version: u16,
    pub identity: MutationScopeIdentity,
    pub consumer: String,
    pub resolved_through: Option<OutboxPosition>,
    pub acked_through: Option<OutboxPosition>,
}

/// One consumer's durable accounting on one scope, in
/// `mutation_outbox_fairness`.
///
/// It carries the durable accounting the protocol cannot afford to recompute.
/// `consecutive_claims` is observability for the last scope-local run; fairness
/// decisions themselves remain sweep-local because this row cannot encode
/// whether another tenant took a turn while the process was down. The counters
/// make a claim O(claimed) instead of O(every row ever delivered): they are
/// maintained in the same transaction as the delivery rows they count, so they
/// cannot drift.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxConsumerState {
    pub schema_version: u16,
    pub identity: MutationScopeIdentity,
    pub consumer: String,
    pub consecutive_claims: u32,
    pub total_claims: u64,
    pub last_claim_at_ms: u64,
    pub inflight: u32,
    pub delivered: u64,
    pub dead_lettered: u64,
}

pub(crate) fn encode_row<T: Serialize>(value: &T, label: &str) -> Result<Vec<u8>, String> {
    encode_bounded(value, label)
}

pub(crate) fn decode_row<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    decode_ledger_record(bytes)
}

/// Reject a consumer name that cannot key a durable row.
///
/// The name is a key component of five tables, so an empty or padded name
/// would produce rows that no later claim can address by the name its caller
/// believes it used.
pub(crate) fn validate_consumer(consumer: &str) -> Result<(), String> {
    validate_key_text(consumer, "outbox consumer name")
}

pub(crate) fn validate_topic(topic: &str) -> Result<(), String> {
    validate_key_text(topic, "outbox topic")
}

fn validate_key_text(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty() || value.trim() != value {
        return Err(format!("{label} is empty or padded"));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("{label} carries a control character"));
    }
    if value.len() > 256 {
        return Err(format!("{label} exceeds its key budget"));
    }
    Ok(())
}

/// Prove a decoded delivery-side row is the one its key named, and that it was
/// written under the scope it is being read for.
pub(crate) fn validate_stamp(
    stamped: &MutationScopeIdentity,
    expected: &MutationScopeIdentity,
) -> Result<(), String> {
    stamped.validate_digest()?;
    if stamped != expected {
        return Err("outbox delivery row is not stamped with this scope identity".to_string());
    }
    Ok(())
}

/// Prove a decoded delivery row agrees with the key that selected it.
///
/// The scope stamp protects the physical binding, while this check protects
/// the consumer and position components.  Without both, a malformed row could
/// be read through one event's key and then acknowledge or prune another
/// position's delivery state.
pub(crate) fn validate_delivery_key(
    delivery: &OutboxDelivery,
    consumer: &str,
    position: &OutboxPosition,
) -> Result<(), String> {
    if delivery.schema_version != MUTATION_BATCH_VERSION {
        return Err("CORRUPT_OUTBOX_DELIVERY: unsupported row schema".to_string());
    }
    if delivery.consumer != consumer || delivery.position != *position {
        return Err("CORRUPT_OUTBOX_DELIVERY: row does not match its key".to_string());
    }
    Ok(())
}

pub(crate) fn validate_delivery_event(
    delivery: &OutboxDelivery,
    consumer: &str,
    batch_id: &str,
    ordinal: u32,
) -> Result<(), String> {
    if delivery.schema_version != MUTATION_BATCH_VERSION {
        return Err("CORRUPT_OUTBOX_DELIVERY: unsupported row schema".to_string());
    }
    if delivery.consumer != consumer
        || delivery.position.batch_id != batch_id
        || delivery.position.ordinal != ordinal
    {
        return Err("CORRUPT_OUTBOX_DELIVERY: row does not match its key".to_string());
    }
    Ok(())
}
