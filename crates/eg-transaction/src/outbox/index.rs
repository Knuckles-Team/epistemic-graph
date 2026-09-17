//! The commit-ordered topic index over one scope's outbox rows.
//!
//! `ledger_outbox` is keyed `(scope, batch id, ordinal)`, so its own order is
//! lexical by batch id -- not the order rows became durable. A consumer that
//! resumed from a position in that order would skip any later-committed batch
//! whose id happens to sort earlier, which is the one thing an at-least-once
//! delivery protocol may not do.
//!
//! `mutation_outbox_topic_index` is the declared table that fixes it:
//! `(scope, topic, sequence, created at, batch id, ordinal)`. Within one topic
//! it is exactly commit order, so a claim is a forward scan from a durable
//! position and a consumer's subscription bounds what it sees.
//!
//! # The sequence is the scope's version, never the caller's clock
//!
//! `ledger_versions` advances by exactly one on every admitted batch --
//! `commit::write_version` says so, and it does it for unversioned batches too.
//! Read inside the committing transaction (after `write_version`, before
//! `write_outbox`), it is a true commit sequence for EVERY scope shape. The
//! earlier design used `CommittedVersion`'s `source`, which is `0` for every
//! row of a `ControlPlane`/`Lifecycle` scope, collapsing the whole order there
//! onto `batch.created_at_ms` -- caller wall clock, which is exactly the
//! "a later batch sorts earlier" hazard this index exists to remove.
//!
//! The index row is written in the same transaction as the outbox row it
//! indexes, from `crate::commit::write_outbox`. That is the only hook this
//! module places on the commit path.

use crate::admitted::AdmittedMutation;
use crate::outbox::ensure_not_graft_fenced;
use crate::outbox::rows::{
    decode_row, encode_row, validate_stamp, OutboxPosition, MAX_TEXT_SENTINEL,
};
use crate::tables::OUTBOX_CLAIM_CURSORS;
use crate::tables::{OUTBOX, OUTBOX_TOPIC_INDEX};
use eg_storage::{
    decode_outbox_record, ledger_scope_key, MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain,
    ScopedRead,
};
use eg_types::{MutationOutboxRecord, MutationScopeIdentity};

/// A control key in the claim-cursor table reserved for index migration.
/// Consumer names reject control characters, so public delivery operations can
/// never collide with this durable backfill state.
const BACKFILL_CURSOR_KEY: &str = "\u{1}kernel-outbox-index-backfill";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexBackfillState {
    schema_version: u16,
    identity: MutationScopeIdentity,
    batch_id: Option<String>,
    ordinal: Option<u32>,
    complete: bool,
}

/// One bounded legacy-index repair transaction. `complete` is durable state,
/// not inferred from whether this page inserted a row: an already-indexed
/// page may legitimately report `indexed == 0` while later primary rows still
/// need repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxBackfillOutcome {
    pub indexed: u64,
    pub examined: u64,
    pub complete: bool,
}

/// One indexed outbox row and where it sits in its topic's commit order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    pub position: OutboxPosition,
}

/// A bounded scan and whether it stopped early.
///
/// The flag is not optional: DESIGN.md permits overload to increase stated
/// freshness lag but requires that it never be silent, and a truncated count
/// reported as a total reads as a healthy queue.
pub(crate) struct IndexPage {
    pub entries: Vec<IndexEntry>,
    pub truncated: bool,
}

/// The scope's commit sequence for one outbox row.
///
/// This is persisted on the primary row by the commit path. It is deliberately
/// not reconstructed from `CommittedVersion`: unversioned rows have no target,
/// and a bounded index scan cannot prove that a legacy sequence is unique.
fn sequence_in_write(record: &MutationOutboxRecord) -> Result<u64, String> {
    let sequence = record
        .commit_sequence
        .ok_or_else(|| "CORRUPT_OUTBOX_SEQUENCE: outbox row has no commit sequence".to_string())?;
    if record
        .committed_version
        .target()
        .is_some_and(|target| target != sequence)
    {
        return Err("CORRUPT_OUTBOX_SEQUENCE: version and commit sequence disagree".to_string());
    }
    Ok(sequence)
}

/// Index one committed outbox row inside the transaction that wrote it.
///
/// Called from [`crate::commit::write_outbox`]. It writes no value -- the key
/// IS the fact -- so the index cannot disagree with the row it points at about
/// anything except existence, and scope retirement sweeps it with every other
/// scoped ledger table.
pub(crate) fn index_outbox_row<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    record: &MutationOutboxRecord,
) -> Result<(), String> {
    let scope = ledger_scope_key(&record.identity);
    let sequence = sequence_in_write(record)?;
    insert_index_row(write, &scope, record, sequence)
}

/// Mark a scope whose outbox rows were written by the current commit path as
/// index-ready. An existing incomplete migration remains authoritative and is
/// never overwritten by a producer racing the backfill.
pub(crate) fn mark_index_ready_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
) -> Result<(), String> {
    let scope = ledger_scope_key(identity);
    let stored = {
        let table = write.scoped_table(OUTBOX_CLAIM_CURSORS)?;
        let value = table
            .get((scope.as_str(), BACKFILL_CURSOR_KEY))?
            .map(|value| decode_row::<IndexBackfillState>(value.value()));
        value.transpose()?
    };
    if stored.is_none() {
        // The marker is created before a new producer appends its rows. If
        // rows already exist, this is a legacy scope whose index readiness is
        // unknown; leave it unmarked so claim/status remain fail-closed until
        // backfill examines the whole primary range.
        let has_rows = write
            .scoped_table(OUTBOX)?
            .range_inclusive(
                (scope.as_str(), "", 0),
                (scope.as_str(), MAX_TEXT_SENTINEL, u32::MAX),
            )?
            .next()
            .transpose()
            .map_err(|error| error.to_string())?
            .is_some();
        if has_rows {
            return Ok(());
        }
    }
    if let Some(state) = stored {
        validate_backfill_state_in_write(write, &scope, &state, identity)?;
        if !state.complete {
            return Ok(());
        }
    }
    write_backfill_state_in_write(
        write,
        &scope,
        &IndexBackfillState {
            schema_version: eg_types::MUTATION_BATCH_VERSION,
            identity: identity.clone(),
            batch_id: None,
            ordinal: None,
            complete: true,
        },
    )
}

fn insert_index_row<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    record: &MutationOutboxRecord,
    sequence: u64,
) -> Result<(), String> {
    let mut table = write.scoped_table(OUTBOX_TOPIC_INDEX)?;
    table.insert(
        (
            scope,
            record.intent.topic.as_str(),
            sequence,
            record.created_at_ms,
            record.batch_id.as_str(),
            record.ordinal,
        ),
        (),
    )
}

/// The inclusive lower bound of one topic's index range, resuming after `from`.
///
/// A resumed scan starts *at* the recorded position and skips it, rather than
/// trying to name its successor: `&str` and `u32` components have no usable
/// increment, so "the next key" is not expressible, only "this key onwards".
fn lower_bound(from: Option<&OutboxPosition>) -> (u64, u64, &str, u32) {
    match from {
        Some(position) => (
            position.sequence,
            position.created_at_ms,
            position.batch_id.as_str(),
            position.ordinal,
        ),
        None => (0, 0, "", 0),
    }
}

const UPPER_BOUND: (u64, u64, &str, u32) = (u64::MAX, u64::MAX, MAX_TEXT_SENTINEL, u32::MAX);

/// Every indexed row of one topic at or after `from`, in commit order, bounded
/// by `limit`.
///
/// Read inside the caller's own write transaction so the scan is serialized
/// against any concurrent producer: a row committed while the claim is deciding
/// cannot appear halfway through the decision.
pub(crate) fn scan_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    topic: &str,
    from: Option<&OutboxPosition>,
    limit: usize,
) -> Result<IndexPage, String> {
    let low = lower_bound(from);
    let table = write.scoped_table(OUTBOX_TOPIC_INDEX)?;
    let rows = table.range_inclusive(
        (scope, topic, low.0, low.1, low.2, low.3),
        (
            scope,
            topic,
            UPPER_BOUND.0,
            UPPER_BOUND.1,
            UPPER_BOUND.2,
            UPPER_BOUND.3,
        ),
    )?;
    collect(rows.map(|row| row.map_err(|e| e.to_string())), from, limit)
}

/// The same scan from a read capability, for status and lag reporting.
pub(crate) fn scan_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    topic: &str,
    from: Option<&OutboxPosition>,
    limit: usize,
) -> Result<IndexPage, String> {
    let low = lower_bound(from);
    let table = read.scoped_table(OUTBOX_TOPIC_INDEX)?;
    let rows = table.range_inclusive(
        (scope, topic, low.0, low.1, low.2, low.3),
        (
            scope,
            topic,
            UPPER_BOUND.0,
            UPPER_BOUND.1,
            UPPER_BOUND.2,
            UPPER_BOUND.3,
        ),
    )?;
    collect(rows.map(|row| row.map_err(|e| e.to_string())), from, limit)
}

/// The index entry immediately before `position` in `topic`'s commit order,
/// or `None` if `position` is the topic's first entry.
///
/// Uses the index's reverse iteration rather than a forward scan from zero,
/// so locating a deep rewind target costs two bounded seeks, not a scan of
/// the whole prefix (X10 2.4 item 5). `position` must already be proven to
/// name a real index row; a caller that has not done so gets
/// `CORRUPT_OUTBOX_INDEX` here instead of a wrong answer.
pub(crate) fn predecessor_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    topic: &str,
    position: &OutboxPosition,
) -> Result<Option<OutboxPosition>, String> {
    let table = write.scoped_table(OUTBOX_TOPIC_INDEX)?;
    let high = (
        scope,
        topic,
        position.sequence,
        position.created_at_ms,
        position.batch_id.as_str(),
        position.ordinal,
    );
    let mut rows = table.range_inclusive((scope, topic, 0u64, 0u64, "", 0u32), high)?;
    let last = rows.next_back().transpose().map_err(|error| error.to_string())?;
    let Some((key, _)) = last else {
        return Err("CORRUPT_OUTBOX_INDEX: rewind target is absent from the topic index".to_string());
    };
    if key.value().position() != *position {
        return Err("CORRUPT_OUTBOX_INDEX: rewind target is absent from the topic index".to_string());
    }
    let previous = rows.next_back().transpose().map_err(|error| error.to_string())?;
    Ok(previous.map(|(key, _)| key.value().position()))
}

/// Prove that a durable claim cursor position names an index row of its
/// subscribed topic.  Cursor fields are caller-visible durable bytes; using a
/// forged high position as a lower bound would silently skip the queue.
pub(crate) fn position_exists_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    topic: &str,
    position: &OutboxPosition,
) -> Result<bool, String> {
    Ok(write
        .scoped_table(OUTBOX_TOPIC_INDEX)?
        .get((
            scope,
            topic,
            position.sequence,
            position.created_at_ms,
            position.batch_id.as_str(),
            position.ordinal,
        ))?
        .is_some())
}

/// Validate every fact a durable index position claims about its primary
/// outbox row.  A key-only existence check accepts a planted dangling index
/// entry as a cursor boundary and can skip real events on the next sweep.
pub(crate) fn validate_position_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    topic: &str,
    identity: &MutationScopeIdentity,
    position: &OutboxPosition,
) -> Result<(), String> {
    let record = write
        .scoped_table(OUTBOX)?
        .get((scope, position.batch_id.as_str(), position.ordinal))?
        .map(|value| decode_outbox_record(value.value()))
        .transpose()?
        .ok_or_else(|| {
            "CORRUPT_OUTBOX_INDEX: position names a missing primary outbox row".to_string()
        })?;
    validate_position_record(&record, topic, identity, position)?;
    if !position_exists_in_write(write, scope, topic, position)? {
        return Err("CORRUPT_OUTBOX_INDEX: position is absent from the topic index".to_string());
    }
    Ok(())
}

pub(crate) fn validate_position_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    topic: &str,
    identity: &MutationScopeIdentity,
    position: &OutboxPosition,
) -> Result<(), String> {
    let record = read
        .scoped_table(OUTBOX)?
        .get((scope, position.batch_id.as_str(), position.ordinal))?
        .map(|value| decode_outbox_record(value.value()))
        .transpose()?
        .ok_or_else(|| {
            "CORRUPT_OUTBOX_INDEX: position names a missing primary outbox row".to_string()
        })?;
    validate_position_record(&record, topic, identity, position)?;
    if !position_exists_in_read(read, scope, topic, position)? {
        return Err("CORRUPT_OUTBOX_INDEX: position is absent from the topic index".to_string());
    }
    Ok(())
}

fn validate_position_record(
    record: &MutationOutboxRecord,
    topic: &str,
    identity: &MutationScopeIdentity,
    position: &OutboxPosition,
) -> Result<(), String> {
    validate_stamp(&record.identity, identity)?;
    record
        .validate()
        .map_err(|error| format!("CORRUPT_OUTBOX_INDEX: {error}"))?;
    let sequence = record
        .commit_sequence
        .ok_or_else(|| "CORRUPT_OUTBOX_SEQUENCE: primary row has no commit sequence".to_string())?;
    if record.batch_id != position.batch_id
        || record.ordinal != position.ordinal
        || record.intent.topic != topic
        || record.created_at_ms != position.created_at_ms
        || sequence != position.sequence
        || record
            .committed_version
            .target()
            .is_some_and(|version| version != sequence)
    {
        return Err("CORRUPT_OUTBOX_INDEX: position does not match its primary row".to_string());
    }
    Ok(())
}

pub(crate) fn position_exists_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    topic: &str,
    position: &OutboxPosition,
) -> Result<bool, String> {
    Ok(read
        .scoped_table(OUTBOX_TOPIC_INDEX)?
        .get((
            scope,
            topic,
            position.sequence,
            position.created_at_ms,
            position.batch_id.as_str(),
            position.ordinal,
        ))?
        .is_some())
}

/// Find the commit-order position for one durable outbox record. Projection
/// cursors carry the batch/ordinal but not the index sequence, so a planted
/// cursor must prove that its event is actually present in the subscribed
/// topic index before it can be reported or used.
pub(crate) fn position_for_record_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    topic: &str,
    record: &MutationOutboxRecord,
) -> Result<Option<OutboxPosition>, String> {
    let table = write.scoped_table(OUTBOX_TOPIC_INDEX)?;
    let sequence = record
        .commit_sequence
        .ok_or_else(|| "CORRUPT_OUTBOX_SEQUENCE: outbox row has no commit sequence".to_string())?;
    if record
        .committed_version
        .target()
        .is_some_and(|target| target != sequence)
    {
        return Err("CORRUPT_OUTBOX_SEQUENCE: version and commit sequence disagree".to_string());
    }
    let present = table
        .get((
            scope,
            topic,
            sequence,
            record.created_at_ms,
            record.batch_id.as_str(),
            record.ordinal,
        ))?
        .is_some();
    Ok(present.then(|| OutboxPosition {
        sequence,
        created_at_ms: record.created_at_ms,
        batch_id: record.batch_id.clone(),
        ordinal: record.ordinal,
    }))
}

pub(crate) fn position_for_record_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    topic: &str,
    record: &MutationOutboxRecord,
) -> Result<Option<OutboxPosition>, String> {
    let table = read.scoped_table(OUTBOX_TOPIC_INDEX)?;
    let sequence = record
        .commit_sequence
        .ok_or_else(|| "CORRUPT_OUTBOX_SEQUENCE: outbox row has no commit sequence".to_string())?;
    if record
        .committed_version
        .target()
        .is_some_and(|target| target != sequence)
    {
        return Err("CORRUPT_OUTBOX_SEQUENCE: version and commit sequence disagree".to_string());
    }
    let present = table
        .get((
            scope,
            topic,
            sequence,
            record.created_at_ms,
            record.batch_id.as_str(),
            record.ordinal,
        ))?
        .is_some();
    Ok(present.then(|| OutboxPosition {
        sequence,
        created_at_ms: record.created_at_ms,
        batch_id: record.batch_id.clone(),
        ordinal: record.ordinal,
    }))
}

/// Turn a raw index range into positions, dropping the resume position itself.
fn collect<'r, I, K>(
    rows: I,
    from: Option<&OutboxPosition>,
    limit: usize,
) -> Result<IndexPage, String>
where
    I: Iterator<Item = Result<(redb::AccessGuard<'r, K>, redb::AccessGuard<'r, ()>), String>>,
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: IndexKey,
{
    let mut entries = Vec::new();
    for row in rows {
        if entries.len() >= limit {
            return Ok(IndexPage {
                entries,
                truncated: true,
            });
        }
        let (key, _) = row?;
        let position = key.value().position();
        if from.is_some_and(|start| *start == position) {
            continue;
        }
        entries.push(IndexEntry { position });
    }
    Ok(IndexPage {
        entries,
        truncated: false,
    })
}

/// Index every outbox row of one scope that has no index row yet.
///
/// The index exists only because `commit::write_outbox` writes it. Every
/// `ledger_outbox` row committed before this protocol landed -- including rows
/// in a file this branch created before the hook existed -- has no index row,
/// and a claim only ever scans the index, so those rows would be invisible to
/// the protocol forever: never claimed, never delivered, and reported as no
/// lag at all. This is the one-shot repair, and it is idempotent: an index row
/// is keyed by the fact it records, so rewriting one is a no-op.
///
/// Rows written before the commit-sequence field landed fail closed rather
/// than receiving a fabricated sequence. Progress is a durable cursor in the
/// kernel-owned claim-cursor table, so one bounded page can be resumed without
/// rescanning an unbounded lexical prefix. Claims defer while incomplete;
/// status reports that fact explicitly.
pub(crate) fn backfill<D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    owner: &OwnedStoreHandle<D>,
) -> Result<OutboxBackfillOutcome, String> {
    // Close claims before releasing the first write lock.  A completed marker
    // is otherwise visible during the gap between an aborted scan and a
    // second transaction that records its corruption, allowing a concurrent
    // worker to skip the legacy row.
    let gate = AdmittedMutation::open(authority, owner)?;
    if let Err(error) = ensure_not_graft_fenced(&gate, owner.identity()) {
        gate.abort()?;
        return Err(error);
    }
    let scope = ledger_scope_key(owner.identity());
    let mut state = match read_backfill_state_in_write(&gate, owner.identity()) {
        Ok(state) => state,
        Err(error) => {
            gate.abort()?;
            return Err(error);
        }
    };
    if state.complete {
        state.batch_id = None;
        state.ordinal = None;
        state.complete = false;
        write_backfill_state_in_write(&gate, &scope, &state)?;
        gate.commit()?;
    } else {
        gate.abort()?;
    }

    let write = AdmittedMutation::open(authority, owner)?;
    match backfill_in(&write, owner.identity()) {
        Ok(indexed) => {
            write.commit()?;
            Ok(indexed)
        }
        Err(error) => {
            write.abort()?;
            Err(error)
        }
    }
}

fn backfill_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
) -> Result<OutboxBackfillOutcome, String> {
    ensure_not_graft_fenced(write, identity)?;
    let scope = ledger_scope_key(identity);
    let mut state = read_backfill_state_in_write(write, identity)?;
    // `backfill` durably transitions a completed state to incomplete before
    // this scan transaction opens.  An incomplete pass resumes at its durable
    // primary-key cursor and remains fail-closed if this transaction aborts.
    let mut indexed = 0u64;
    let low = (
        scope.as_str(),
        state.batch_id.as_deref().unwrap_or(""),
        state.ordinal.unwrap_or(0),
    );
    let mut examined = 0usize;
    let complete;
    {
        let outbox = write.scoped_table(OUTBOX)?;
        let mut rows =
            outbox.range_inclusive(low, (scope.as_str(), MAX_TEXT_SENTINEL, u32::MAX))?;
        loop {
            if examined >= MAX_BACKFILL_ROWS {
                complete = match rows.next() {
                    None => true,
                    Some(Ok(_)) => false,
                    Some(Err(error)) => return Err(error.to_string()),
                };
                break;
            }
            let Some(row) = rows.next() else {
                complete = true;
                break;
            };
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (row_scope, batch_id, ordinal) = key.value();
            if state.batch_id.as_deref() == Some(batch_id) && state.ordinal == Some(ordinal) {
                // The cursor names the last row committed by the previous
                // page. The range is inclusive because the key has no
                // expressible successor.
                continue;
            }
            examined = examined.saturating_add(1);
            state.batch_id = Some(batch_id.to_string());
            state.ordinal = Some(ordinal);
            let record = decode_outbox_record(value.value())
                .map_err(|error| format!("CORRUPT_OUTBOX_BACKFILL: {error}"))?;
            if row_scope != scope.as_str()
                || record.batch_id != batch_id
                || record.ordinal != ordinal
            {
                return Err(
                    "CORRUPT_OUTBOX_BACKFILL: primary key does not match outbox record".to_string(),
                );
            }
            validate_stamp(&record.identity, identity)
                .map_err(|error| format!("CORRUPT_OUTBOX_BACKFILL: {error}"))?;
            record
                .validate()
                .map_err(|error| format!("CORRUPT_OUTBOX_BACKFILL: {error}"))?;
            let sequence = sequence_for_backfill(&record)?;
            let index_key = (
                row_scope,
                record.intent.topic.as_str(),
                sequence,
                record.created_at_ms,
                record.batch_id.as_str(),
                record.ordinal,
            );
            let mut index = write.scoped_table(OUTBOX_TOPIC_INDEX)?;
            if index.get(index_key)?.is_none() {
                index.insert(index_key, ())?;
                indexed = indexed.saturating_add(1);
            }
        }
    }
    state.complete = complete;
    write_backfill_state_in_write(write, &scope, &state)?;
    Ok(OutboxBackfillOutcome {
        indexed,
        examined: examined as u64,
        complete,
    })
}

/// One backfill call indexes at most a fast-class page and retains one decoded
/// outbox row at a time, so a cutover does not allocate the whole legacy queue.
#[cfg(not(test))]
const MAX_BACKFILL_ROWS: usize = crate::outbox::rows::MAX_CLAIM_SCAN_ROWS;

#[cfg(test)]
const MAX_BACKFILL_ROWS: usize = 2;

fn sequence_for_backfill(record: &MutationOutboxRecord) -> Result<u64, String> {
    let sequence = record.commit_sequence.ok_or_else(|| {
        "CORRUPT_OUTBOX_BACKFILL: outbox row has no reconstructable commit sequence".to_string()
    })?;
    if record
        .committed_version
        .target()
        .is_some_and(|target| target != sequence)
    {
        return Err("CORRUPT_OUTBOX_BACKFILL: version and commit sequence disagree".to_string());
    }
    Ok(sequence)
}

fn validate_backfill_state(
    state: &IndexBackfillState,
    identity: &MutationScopeIdentity,
) -> Result<(), String> {
    validate_stamp(&state.identity, identity)
        .map_err(|error| format!("CORRUPT_OUTBOX_BACKFILL: {error}"))?;
    if state.schema_version != eg_types::MUTATION_BATCH_VERSION {
        return Err("CORRUPT_OUTBOX_BACKFILL: unsupported state schema".to_string());
    }
    if state.batch_id.is_some() != state.ordinal.is_some() {
        return Err("CORRUPT_OUTBOX_BACKFILL: cursor key is incomplete".to_string());
    }
    Ok(())
}

fn validate_backfill_cursor_record(
    record: &MutationOutboxRecord,
    batch_id: &str,
    ordinal: u32,
    identity: &MutationScopeIdentity,
) -> Result<(), String> {
    if record.identity != *identity || record.batch_id != batch_id || record.ordinal != ordinal {
        return Err("CORRUPT_OUTBOX_BACKFILL: cursor does not match its primary row".to_string());
    }
    validate_stamp(&record.identity, identity)
        .map_err(|error| format!("CORRUPT_OUTBOX_BACKFILL: {error}"))?;
    record
        .validate()
        .map_err(|error| format!("CORRUPT_OUTBOX_BACKFILL: {error}"))?;
    sequence_for_backfill(record)?;
    Ok(())
}

fn validate_backfill_state_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    state: &IndexBackfillState,
    identity: &MutationScopeIdentity,
) -> Result<(), String> {
    validate_backfill_state(state, identity)?;
    let Some((batch_id, ordinal)) = state.batch_id.as_deref().zip(state.ordinal) else {
        return Ok(());
    };
    let record = write
        .scoped_table(OUTBOX)?
        .get((scope, batch_id, ordinal))?
        .map(|value| decode_outbox_record(value.value()))
        .transpose()
        .map_err(|error| format!("CORRUPT_OUTBOX_BACKFILL: {error}"))?
        .ok_or_else(|| "CORRUPT_OUTBOX_BACKFILL: cursor names a missing primary row".to_string())?;
    validate_backfill_cursor_record(&record, batch_id, ordinal, identity)
}

fn validate_backfill_state_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    state: &IndexBackfillState,
    identity: &MutationScopeIdentity,
) -> Result<(), String> {
    validate_backfill_state(state, identity)?;
    let Some((batch_id, ordinal)) = state.batch_id.as_deref().zip(state.ordinal) else {
        return Ok(());
    };
    let record = read
        .scoped_table(OUTBOX)?
        .get((scope, batch_id, ordinal))?
        .map(|value| decode_outbox_record(value.value()))
        .transpose()
        .map_err(|error| format!("CORRUPT_OUTBOX_BACKFILL: {error}"))?
        .ok_or_else(|| "CORRUPT_OUTBOX_BACKFILL: cursor names a missing primary row".to_string())?;
    validate_backfill_cursor_record(&record, batch_id, ordinal, identity)
}

fn read_backfill_state_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
) -> Result<IndexBackfillState, String> {
    let scope = ledger_scope_key(identity);
    let table = write.scoped_table(OUTBOX_CLAIM_CURSORS)?;
    let stored = table
        .get((scope.as_str(), BACKFILL_CURSOR_KEY))?
        .map(|value| decode_row::<IndexBackfillState>(value.value()))
        .transpose()?;
    let Some(state) = stored else {
        let has_rows = write
            .scoped_table(OUTBOX)?
            .range_inclusive(
                (scope.as_str(), "", 0),
                (scope.as_str(), MAX_TEXT_SENTINEL, u32::MAX),
            )?
            .next()
            .transpose()
            .map_err(|error| error.to_string())?
            .is_some();
        return Ok(IndexBackfillState {
            schema_version: eg_types::MUTATION_BATCH_VERSION,
            identity: identity.clone(),
            batch_id: None,
            ordinal: None,
            // A missing readiness row on a non-empty scope means the file
            // predates the atomic index hook. Claims stay closed until a
            // bounded backfill creates a complete marker.
            complete: !has_rows,
        });
    };
    validate_backfill_state_in_write(write, &scope, &state, identity)?;
    Ok(state)
}

fn write_backfill_state_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    state: &IndexBackfillState,
) -> Result<(), String> {
    let bytes = encode_row(state, "outbox index backfill state")?;
    write
        .scoped_table(OUTBOX_CLAIM_CURSORS)?
        .insert((scope, BACKFILL_CURSOR_KEY), bytes.as_slice())
}

pub(crate) fn backfill_pending_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
) -> Result<bool, String> {
    Ok(!read_backfill_state_in_write(write, identity)?.complete)
}

pub(crate) fn backfill_complete_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    identity: &MutationScopeIdentity,
) -> Result<bool, String> {
    let scope = ledger_scope_key(identity);
    let table = read.scoped_table(OUTBOX_CLAIM_CURSORS)?;
    let Some(state) = table
        .get((scope.as_str(), BACKFILL_CURSOR_KEY))?
        .map(|value| decode_row::<IndexBackfillState>(value.value()))
        .transpose()?
    else {
        let has_rows = read
            .scoped_table(OUTBOX)?
            .range_inclusive(
                (scope.as_str(), "", 0),
                (scope.as_str(), MAX_TEXT_SENTINEL, u32::MAX),
            )?
            .next()
            .transpose()
            .map_err(|error| error.to_string())?
            .is_some();
        return Ok(!has_rows);
    };
    validate_backfill_state_in_read(read, &scope, &state, identity)?;
    Ok(state.complete)
}

/// The position an index key names.
///
/// A trait rather than an inline destructure so the read and write scans share
/// one decode: a second copy is how the two would drift about which component
/// is the sequence and which is the timestamp.
pub(crate) trait IndexKey {
    fn position(&self) -> OutboxPosition;
}

impl IndexKey for (&str, &str, u64, u64, &str, u32) {
    fn position(&self) -> OutboxPosition {
        OutboxPosition {
            sequence: self.2,
            created_at_ms: self.3,
            batch_id: self.4.to_string(),
            ordinal: self.5,
        }
    }
}
