//! Operator re-delivery: re-deliver one consumer's stream, in order, from a
//! position (X10-R5, design 2.4 item 5).
//!
//! A rewind never resurrects one row out of order -- it re-delivers
//! everything from `target` on, keeping the ordering guarantee every other
//! part of this protocol relies on. It runs as a bounded sequence of
//! transactions rather than one long one, so it is safe to interrupt and
//! resume: a durable control row under a reserved key in
//! `mutation_outbox_claim_cursors` (the `BACKFILL_CURSOR_KEY` precedent)
//! marks it in progress, and while that row exists, claim defers with
//! [`crate::outbox::OutboxDeferral::RewindPending`] and ack/reject/release
//! refuse with `OUTBOX_REWIND_PENDING` -- so no pre-rewind lease can move the
//! watermark the rewind is about to relocate.
//!
//! No table or row shape changes: the control row reuses
//! `mutation_outbox_claim_cursors` exactly as the expiry, prune and backfill
//! cursors already do, and the delete pass only removes existing
//! [`crate::outbox::OutboxDelivery`] rows (X10-R6).

use crate::admitted::AdmittedMutation;
use crate::outbox::claim::scan_validated_page;
use crate::outbox::cursor::rewind_watermark_in_write;
use crate::outbox::rows::{
    decode_row, encode_row, validate_consumer, validate_stamp, OutboxPosition,
};
use crate::outbox::stream::subscribed_topic;
use crate::outbox::{claim, ensure_not_graft_fenced, index};
use crate::tables::{OUTBOX_CLAIM_CURSORS, OUTBOX_DELIVERIES};
use eg_storage::{ledger_scope_key, MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain};
use eg_types::{MutationScopeIdentity, MUTATION_BATCH_VERSION};

const REWIND_CURSOR_PREFIX: &str = "\u{1}kernel-outbox-rewind/";

/// Where a rewind re-delivers from: the very start of the stream, or one
/// named position onward (that position included).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxRewindTarget {
    Start,
    At(OutboxPosition),
}

/// One bounded rewind step's outcome. `complete` is durable state: the
/// caller drives repeated calls (an operator loop, or a test simulating a
/// restart between them) until it reports `true`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxRewindOutcome {
    pub complete: bool,
}

/// The durable control row. Consumer names reject control characters, so
/// this reserved key can never collide with one (the `BACKFILL_CURSOR_KEY`
/// precedent).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RewindCursor {
    schema_version: u16,
    identity: MutationScopeIdentity,
    consumer: String,
    /// `None` rewinds to the start of the stream; `Some` rewinds to (and
    /// re-delivers) this position onward.
    target: Option<OutboxPosition>,
    /// Where the bounded delete walk last stopped; `None` until the first
    /// delete step runs.
    position: Option<OutboxPosition>,
}

/// Perform one bounded rewind step: start a new rewind if none is pending for
/// `consumer`, otherwise continue its delete walk. `target` is consulted only
/// to start a rewind; a caller must not change it between calls of the same
/// rewind (mirroring the sweep-budget reuse contract elsewhere in this
/// module).
pub(crate) fn rewind<D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    owner: &OwnedStoreHandle<D>,
    consumer: &str,
    target: OutboxRewindTarget,
    now_ms: u64,
) -> Result<OutboxRewindOutcome, String> {
    validate_consumer(consumer)?;
    let write = AdmittedMutation::open(authority, owner)?;
    match rewind_step(&write, owner.identity(), consumer, target, now_ms) {
        Ok(complete) => {
            write.commit()?;
            Ok(OutboxRewindOutcome { complete })
        }
        Err(error) => {
            write.abort()?;
            Err(error)
        }
    }
}

fn rewind_step<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    consumer: &str,
    target: OutboxRewindTarget,
    now_ms: u64,
) -> Result<bool, String> {
    ensure_not_graft_fenced(write, identity)?;
    let scope = ledger_scope_key(identity);
    if read_rewind_cursor(write, &scope, consumer, identity)?.is_some() {
        return delete_step(write, &scope, identity, consumer);
    }
    prepare_rewind(write, &scope, identity, consumer, target, now_ms)?;
    Ok(false)
}

/// Transaction 1: validate `target`, roll the watermark back to its
/// predecessor (or clear it for `Start`), zero the in-flight counter and
/// write the control row that fences every other delivery-side write until
/// the delete walk finishes.
fn prepare_rewind<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    identity: &MutationScopeIdentity,
    consumer: &str,
    target: OutboxRewindTarget,
    now_ms: u64,
) -> Result<(), String> {
    let topic = subscribed_topic(write, scope, consumer)?;
    let target_position = match target {
        OutboxRewindTarget::Start => None,
        OutboxRewindTarget::At(position) => {
            index::validate_position_in_write(write, scope, &topic, identity, &position)
                .map_err(|error| format!("CORRUPT_OUTBOX_REWIND: {error}"))?;
            Some(position)
        }
    };
    let predecessor = match &target_position {
        None => None,
        Some(position) => index::predecessor_in_write(write, scope, &topic, position)?,
    };
    rewind_watermark_in_write(
        write,
        scope,
        consumer,
        identity,
        predecessor.as_ref(),
        now_ms,
    )?;
    let mut state = claim::read_consumer_state(write, scope, consumer, identity)?;
    state.inflight = 0;
    claim::write_consumer_state(write, scope, consumer, &state)?;
    write_rewind_cursor(
        write,
        scope,
        &RewindCursor {
            schema_version: MUTATION_BATCH_VERSION,
            identity: identity.clone(),
            consumer: consumer.to_string(),
            target: target_position,
            // The delete walk resumes AFTER this position (the same
            // resume-marker convention every other cursor in this module
            // uses), so starting it at the predecessor makes `target` itself
            // the walk's first entry -- exactly what must be deleted first.
            position: predecessor,
        },
    )
}

/// Transactions 2..n (and the last): delete one bounded page of this
/// consumer's delivery rows from the walk's resume position, unconditionally
/// -- every row from `target` on is being re-delivered, whatever state it was
/// last in. Removes the control row once the walk reaches the topic's end.
fn delete_step<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    identity: &MutationScopeIdentity,
    consumer: &str,
) -> Result<bool, String> {
    let Some(state) = read_rewind_cursor(write, scope, consumer, identity)? else {
        return Ok(true);
    };
    let topic = subscribed_topic(write, scope, consumer)?;
    let page = scan_validated_page(
        write,
        scope,
        &topic,
        identity,
        state.position.as_ref(),
        crate::outbox::rows::MAX_CLAIM_SCAN_ROWS,
    )?;
    let mut deliveries = write.scoped_table(OUTBOX_DELIVERIES)?;
    for entry in &page.entries {
        deliveries.remove((
            scope,
            consumer,
            entry.position.batch_id.as_str(),
            entry.position.ordinal,
        ))?;
    }
    if !page.truncated {
        remove_rewind_cursor(write, scope, consumer)?;
        return Ok(true);
    }
    let position = page.entries.last().map(|entry| entry.position.clone());
    write_rewind_cursor(write, scope, &RewindCursor { position, ..state })?;
    Ok(false)
}

fn rewind_cursor_key(consumer: &str) -> String {
    format!("{REWIND_CURSOR_PREFIX}{consumer}")
}

/// Whether -- and where -- a rewind is in progress for `consumer`.
pub(crate) fn read_rewind_cursor<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
    identity: &MutationScopeIdentity,
) -> Result<Option<RewindCursor>, String> {
    let key = rewind_cursor_key(consumer);
    let table = write.scoped_table(OUTBOX_CLAIM_CURSORS)?;
    let stored = table
        .get((scope, key.as_str()))?
        .map(|value| decode_row::<RewindCursor>(value.value()))
        .transpose()?;
    let Some(state) = stored else {
        return Ok(None);
    };
    validate_stamp(&state.identity, identity)?;
    if state.schema_version != MUTATION_BATCH_VERSION {
        return Err("CORRUPT_OUTBOX_REWIND: unsupported row schema".to_string());
    }
    if state.consumer != consumer {
        return Err("CORRUPT_OUTBOX_REWIND: consumer does not match its key".to_string());
    }
    Ok(Some(state))
}

fn write_rewind_cursor<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    state: &RewindCursor,
) -> Result<(), String> {
    let key = rewind_cursor_key(&state.consumer);
    let bytes = encode_row(state, "outbox rewind cursor")?;
    write
        .scoped_table(OUTBOX_CLAIM_CURSORS)?
        .insert((scope, key.as_str()), bytes.as_slice())
}

fn remove_rewind_cursor<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
) -> Result<(), String> {
    let key = rewind_cursor_key(consumer);
    write
        .scoped_table(OUTBOX_CLAIM_CURSORS)?
        .remove((scope, key.as_str()))
        .map(drop)
}
