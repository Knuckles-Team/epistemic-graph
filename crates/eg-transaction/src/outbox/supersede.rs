//! Resolve a source event by immutable position during a replicated top-up.
//!
//! A leader's delivery lease is node-local. A replicated domain transition
//! therefore names the committed source row, not a lease epoch. This operation
//! replaces the old delivery with one canonical terminal row in the caller's
//! admitted transaction, alongside the replacement intent and domain receipt.

use crate::admitted::AdmittedMutation;
use crate::group::{AdmittedGroup, CurrentIntent, ScopedIntent};
use crate::kernel::{admit_group_member, current_batch, ensure_admitted_owner, MutationKernel};
use crate::outbox::claim;
use crate::outbox::cursor::{
    build_cursor, read_cursor_in_write, refuse_if_rewind_pending, require_advance,
    require_no_earlier_gap,
};
use crate::outbox::ensure_not_graft_fenced;
use crate::outbox::index;
use crate::outbox::rows::{
    decode_row, encode_row, validate_consumer, validate_delivery_key, validate_delivery_state,
    validate_stamp, OutboxClaimCursor, OutboxDelivery, OutboxPosition,
};
use crate::outbox::stream::{read_outbox_row_in_write, subscribe_in, subscribed_topic};
use crate::tables::{OUTBOX_CURSORS, OUTBOX_DELIVERIES};
use crate::{commit, Begin};
use eg_storage::{ledger_scope_key, OwnedStoreHandle, OwnerDomain};
use eg_types::{
    MutationBatch, MutationOutboxRecord, MutationProjectionCursor, MUTATION_BATCH_VERSION,
};

const ENRICHMENT_CONSUMER: &str = "repository-enrichment-v1";
const ENRICHMENT_TOPIC: &str = "repository.enrichment.pending";

impl MutationKernel {
    /// Admit one Raft control member and exactly one reserved replacement
    /// parent under the same physical write. The replacement is a verbatim
    /// leader-sealed batch: rebuilding at the follower's current graph version
    /// would change replay identity before maintenance idempotency can resolve.
    /// The generic group APIs refuse this zero-operation namespace.
    pub fn admit_group_current_repository_enrichment_top_up<'a, 'i, D: OwnerDomain>(
        &'a self,
        control: CurrentIntent<'i, D>,
        replacement: ScopedIntent<'i, D>,
    ) -> Result<(AdmittedGroup<'a, D>, Vec<MutationBatch>), String> {
        let owners = [control.owner, replacement.owner];
        let admitted = self.group_members(&owners)?;
        let control_batch = match current_batch(&admitted[0], control.owner, control.build) {
            Ok(batch) => batch,
            Err(error) => {
                let _ = AdmittedGroup::new(admitted, Vec::new()).end(false);
                return Err(error);
            }
        };
        let control_begin = match admit_group_member(&admitted[0], &control_batch) {
            Ok(begun) => begun,
            Err(error) => {
                let _ = AdmittedGroup::new(admitted, Vec::new()).end(false);
                return Err(error);
            }
        };
        let replacement_batch = replacement.batch;
        let replacement_begin =
            match commit::begin_repository_enrichment_top_up(&admitted[1], replacement_batch) {
                Ok(begun) => begun,
                Err(error) => {
                    let _ = AdmittedGroup::new(admitted, vec![control_begin]).end(false);
                    return Err(error);
                }
            };
        if matches!(replacement_begin, Begin::Replay(_)) {
            if let Err(error) = admitted[1].admit_replayed_batch(replacement_batch) {
                let _ = AdmittedGroup::new(admitted, vec![control_begin]).end(false);
                return Err(error);
            }
        }
        Ok((
            AdmittedGroup::new(admitted, vec![control_begin, replacement_begin]),
            vec![control_batch, replacement_batch.clone()],
        ))
    }

    /// Resolve an exact committed source event without a node-local lease.
    ///
    /// The enclosing admitted transaction must commit its replacement outbox
    /// intent and domain receipt with this transition. A retry returns the
    /// original cursor receipt only when the retained terminal marker matches
    /// the exact source and timestamp; a dead letter or intervening rewind is
    /// refused. Every scan is bounded by the existing outbox index contract.
    pub fn outbox_supersede_in<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        owner: &OwnedStoreHandle<D>,
        consumer: &str,
        expected: &MutationOutboxRecord,
        now_ms: u64,
    ) -> Result<MutationProjectionCursor, String> {
        ensure_admitted_owner(write, owner)?;
        supersede_in(write, owner.identity(), consumer, expected, now_ms)
    }

    /// Read-only proof that a prior top-up resolved this exact source row at
    /// the sealed time. A maintenance idempotency hit alone does not prove the
    /// old delivery was superseded; callers verify both receipts on retry.
    pub fn outbox_supersession_receipt_in<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        owner: &OwnedStoreHandle<D>,
        consumer: &str,
        expected: &MutationOutboxRecord,
        original_ms: u64,
    ) -> Result<MutationProjectionCursor, String> {
        ensure_admitted_owner(write, owner)?;
        validate_source_route(write, owner.identity(), consumer, expected)?;
        let scope = ledger_scope_key(owner.identity());
        refuse_if_rewind_pending(write, &scope, consumer, owner.identity())?;
        if index::backfill_pending_in_write(write, owner.identity())? {
            return Err("OUTBOX_INDEX_BACKFILL_PENDING: source prefix is not verified".into());
        }
        let position = source_position(expected)?;
        validate_source_row(write, &scope, owner.identity(), expected, &position)?;
        if subscribed_topic(write, &scope, consumer)? != expected.intent.topic {
            return Err("OUTBOX_SUPERSEDE_MISMATCH: subscription differs from source topic".into());
        }
        read_cursor_in_write(write, &scope, consumer, owner.identity())?;
        let claim_cursor = claim::read_claim_cursor(write, &scope, consumer, owner.identity())?;
        claim::validate_claim_cursor(
            write,
            &scope,
            &expected.intent.topic,
            owner.identity(),
            &claim_cursor,
        )?;
        let delivery = read_delivery(write, &scope, consumer, owner.identity(), &position)?
            .ok_or("OUTBOX_SUPERSEDE_MISMATCH: old delivery has no terminal receipt")?;
        if delivery.dead_lettered() {
            return Err("OUTBOX_DEAD_LETTER: source event is terminally rejected".into());
        }
        replay_receipt(&delivery, &claim_cursor, expected, original_ms)
    }
}

fn validate_source_route<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &eg_types::MutationScopeIdentity,
    consumer: &str,
    expected: &MutationOutboxRecord,
) -> Result<(), String> {
    validate_consumer(consumer)?;
    expected.validate_write_budget()?;
    if consumer != ENRICHMENT_CONSUMER || expected.intent.topic != ENRICHMENT_TOPIC {
        return Err("OUTBOX_SUPERSEDE_MISMATCH: reserved consumer or topic differs".into());
    }
    if expected.identity != *identity {
        return Err("OUTBOX_SUPERSEDE_MISMATCH: source scope differs from admitted owner".into());
    }
    ensure_not_graft_fenced(write, identity)
}

fn validate_source_row<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    identity: &eg_types::MutationScopeIdentity,
    expected: &MutationOutboxRecord,
    position: &OutboxPosition,
) -> Result<(), String> {
    let durable = read_outbox_row_in_write(write, scope, position)?;
    if durable != *expected {
        return Err("OUTBOX_SUPERSEDE_MISMATCH: source row differs from expected event".into());
    }
    index::validate_position_in_write(write, scope, &expected.intent.topic, identity, position)
}

fn supersede_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &eg_types::MutationScopeIdentity,
    consumer: &str,
    expected: &MutationOutboxRecord,
    now_ms: u64,
) -> Result<MutationProjectionCursor, String> {
    validate_source_route(write, identity, consumer, expected)?;
    let scope = ledger_scope_key(identity);
    refuse_if_rewind_pending(write, &scope, consumer, identity)?;
    if index::backfill_pending_in_write(write, identity)? {
        return Err("OUTBOX_INDEX_BACKFILL_PENDING: source prefix is not verified".into());
    }
    let position = source_position(expected)?;
    validate_source_row(write, &scope, identity, expected, &position)?;
    // Follower apply may run before its node-local worker subscribed. Install
    // the exact source topic under the same transaction, refusing a conflict.
    subscribe_in(write, identity, consumer, &expected.intent.topic)?;
    read_cursor_in_write(write, &scope, consumer, identity)?;
    let mut claim_cursor = claim::read_claim_cursor(write, &scope, consumer, identity)?;
    claim::validate_claim_cursor(
        write,
        &scope,
        &expected.intent.topic,
        identity,
        &claim_cursor,
    )?;
    let previous = read_delivery(write, &scope, consumer, identity, &position)?;
    if let Some(ref delivery) = previous {
        if delivery.dead_lettered() {
            return Err("OUTBOX_DEAD_LETTER: source event is terminally rejected".into());
        }
        if delivery.delivered() {
            return replay_receipt(delivery, &claim_cursor, expected, now_ms);
        }
    }
    require_advance(claim_cursor.acked_through.as_ref(), &position)?;
    require_no_earlier_gap(write, &scope, consumer, identity, &claim_cursor, &position)?;
    let receipt = build_cursor(consumer, expected, now_ms)?;
    let held = previous.as_ref().is_some_and(|row| row.lease_until_ms != 0);
    let terminal = OutboxDelivery {
        schema_version: MUTATION_BATCH_VERSION,
        identity: identity.clone(),
        consumer: consumer.into(),
        position: position.clone(),
        // Epoch zero is reserved for deterministic source supersession. A
        // worker's prior lease epoch is thereby fenced on every replica.
        lease_epoch: 0,
        lease_until_ms: 0,
        attempt: 0,
        delivered_at_ms: Some(now_ms),
        dead_lettered_at_ms: None,
    };
    write_delivery(write, &scope, consumer, &terminal)?;
    write.scoped_table(OUTBOX_CURSORS)?.insert(
        (scope.as_str(), consumer),
        encode_row(&receipt, "outbox projection cursor")?.as_slice(),
    )?;
    claim_cursor.acked_through = Some(position.clone());
    if claim_cursor
        .resolved_through
        .as_ref()
        .is_none_or(|resolved| position > *resolved)
    {
        claim_cursor.resolved_through = Some(position);
    }
    claim::write_claim_cursor(write, &scope, consumer, &claim_cursor)?;
    let mut state = claim::read_consumer_state(write, &scope, consumer, identity)?;
    state.delivered = state
        .delivered
        .checked_add(1)
        .ok_or("CORRUPT_OUTBOX_FAIRNESS: delivered counter overflow")?;
    if held {
        state.inflight = state
            .inflight
            .checked_sub(1)
            .ok_or("CORRUPT_OUTBOX_FAIRNESS: superseded lease absent from counter")?;
    }
    claim::write_consumer_state(write, &scope, consumer, &state)?;
    Ok(receipt)
}

fn source_position(record: &MutationOutboxRecord) -> Result<OutboxPosition, String> {
    Ok(OutboxPosition {
        sequence: record
            .commit_sequence
            .ok_or("OUTBOX_SUPERSEDE_MISMATCH: source has no authoritative commit sequence")?,
        created_at_ms: record.created_at_ms,
        batch_id: record.batch_id.clone(),
        ordinal: record.ordinal,
    })
}

fn replay_receipt(
    delivery: &OutboxDelivery,
    claim_cursor: &OutboxClaimCursor,
    expected: &MutationOutboxRecord,
    now_ms: u64,
) -> Result<MutationProjectionCursor, String> {
    if delivery.lease_epoch != 0
        || delivery.attempt != 0
        || delivery.delivered_at_ms != Some(now_ms)
        || claim_cursor
            .acked_through
            .as_ref()
            .is_none_or(|acked| delivery.position > *acked)
    {
        return Err("OUTBOX_SUPERSEDE_MISMATCH: event was resolved by another transition".into());
    }
    build_cursor(&delivery.consumer, expected, now_ms)
}

fn read_delivery<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &eg_types::MutationScopeIdentity,
    position: &OutboxPosition,
) -> Result<Option<OutboxDelivery>, String> {
    let row = write
        .scoped_table(OUTBOX_DELIVERIES)?
        .get((
            scope,
            consumer,
            position.batch_id.as_str(),
            position.ordinal,
        ))?
        .map(|value| decode_row::<OutboxDelivery>(value.value()))
        .transpose()?;
    if let Some(ref delivery) = row {
        validate_stamp(&delivery.identity, identity)?;
        validate_delivery_key(delivery, consumer, position)?;
        let claim_cursor = claim::read_claim_cursor(write, scope, consumer, identity)?;
        validate_delivery_state(delivery, claim_cursor.acked_through.as_ref())?;
    }
    Ok(row)
}

fn write_delivery<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    delivery: &OutboxDelivery,
) -> Result<(), String> {
    write.scoped_table(OUTBOX_DELIVERIES)?.insert(
        (
            scope,
            consumer,
            delivery.position.batch_id.as_str(),
            delivery.position.ordinal,
        ),
        encode_row(delivery, "outbox delivery row")?.as_slice(),
    )
}
