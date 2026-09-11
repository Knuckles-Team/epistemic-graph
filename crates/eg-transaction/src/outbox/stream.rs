//! Consumer subscriptions, and reading one committed outbox row by position.
//!
//! `mutation_outbox_consumers` is `(scope, consumer) -> topic`: a consumer's
//! subscription is durable, so a claim cannot silently widen or narrow what a
//! projection sees by passing a different topic on the next poll, and a scope
//! can be asked which projections it owes work to without guessing.

use crate::admitted::AdmittedMutation;
use crate::outbox::ensure_not_graft_fenced;
use crate::outbox::rows::{validate_consumer, validate_topic, OutboxPosition};
use crate::tables::{OUTBOX, OUTBOX_CONSUMERS};
use eg_storage::{
    decode_outbox_record, ledger_scope_key, MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain,
    ScopedRead,
};
use eg_types::{MutationOutboxRecord, MutationScopeIdentity};

/// Durably subscribe one consumer of this scope to one topic.
///
/// Idempotent for the same topic and refused for a different one: a
/// subscription change would move the consumer's whole ordered stream, and the
/// cursor it already advanced names positions in the old one.
pub(crate) fn subscribe<D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    owner: &OwnedStoreHandle<D>,
    consumer: &str,
    topic: &str,
) -> Result<(), String> {
    validate_consumer(consumer)?;
    validate_topic(topic)?;
    let write = AdmittedMutation::open(authority, owner)?;
    match subscribe_in(&write, owner.identity(), consumer, topic) {
        Ok(()) => write.commit(),
        Err(error) => {
            write.abort()?;
            Err(error)
        }
    }
}

fn subscribe_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    consumer: &str,
    topic: &str,
) -> Result<(), String> {
    ensure_not_graft_fenced(write, identity)?;
    let scope = ledger_scope_key(identity);
    let mut table = write.scoped_table(OUTBOX_CONSUMERS)?;
    let existing = table
        .get((scope.as_str(), consumer))?
        .map(|value| value.value().to_string());
    match existing {
        Some(current) if current == topic => Ok(()),
        Some(_) => Err("outbox consumer is already subscribed to another topic".to_string()),
        None => table.insert((scope.as_str(), consumer), topic),
    }
}

/// The topic one consumer is durably subscribed to.
pub(crate) fn subscribed_topic<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    consumer: &str,
) -> Result<String, String> {
    let table = write.scoped_table(OUTBOX_CONSUMERS)?;
    let topic = table
        .get((scope, consumer))?
        .map(|value| value.value().to_string());
    let topic = topic.ok_or_else(|| "outbox consumer has no durable subscription".to_string())?;
    validate_topic(&topic)?;
    Ok(topic)
}

/// The topic one consumer is subscribed to, read from a snapshot.
pub(crate) fn subscribed_topic_in_read<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    scope: &str,
    consumer: &str,
) -> Result<Option<String>, String> {
    let table = read.scoped_table(OUTBOX_CONSUMERS)?;
    let topic = table
        .get((scope, consumer))?
        .map(|value| value.value().to_string());
    if let Some(topic) = &topic {
        validate_topic(topic)?;
    }
    Ok(topic)
}

/// The committed outbox row one index position names.
///
/// The index and the row are written in one transaction, so a position with no
/// row is a corrupt ledger rather than a race, and it is reported as one.
pub(crate) fn read_outbox_row_in_write<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    position: &OutboxPosition,
) -> Result<MutationOutboxRecord, String> {
    let table = write.scoped_table(OUTBOX)?;
    let bytes = table
        .get((scope, position.batch_id.as_str(), position.ordinal))?
        .map(|value| value.value().to_vec())
        .ok_or_else(|| "CORRUPT_MUTATION_LEDGER: outbox index names a missing event".to_string())?;
    decode_outbox_record(&bytes)
}
