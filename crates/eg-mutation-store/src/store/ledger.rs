use super::*;
use eg_types::VersionExpectation;
use redb::{Key, Value};
use serde::Serialize;
use std::io::Write;

pub(crate) fn open_product_tables(wtx: &WriteTransaction) -> Result<(), String> {
    ensure_table(wtx, STORE_ROOT)?;
    ensure_table(wtx, SCOPE_BINDINGS)?;
    ensure_table(wtx, OWNER_MANIFEST)?;
    ensure_table(wtx, BATCHES)?;
    ensure_table(wtx, IDEMPOTENCY)?;
    ensure_table(wtx, VERSIONS)?;
    ensure_table(wtx, FENCES)?;
    ensure_table(wtx, OUTBOX)?;
    ensure_table(wtx, PRIVATE_PAYLOADS)?;
    ensure_table(wtx, OUTBOX_TOPIC_INDEX)?;
    ensure_table(wtx, OUTBOX_CONSUMERS)?;
    ensure_table(wtx, OUTBOX_DELIVERIES)?;
    ensure_table(wtx, OUTBOX_CURSORS)?;
    ensure_table(wtx, OUTBOX_CLAIM_CURSORS)?;
    ensure_table(wtx, OUTBOX_FAIRNESS)
}

fn ensure_table<K, V>(
    wtx: &WriteTransaction,
    definition: TableDefinition<'static, K, V>,
) -> Result<(), String>
where
    K: Key + 'static,
    V: Value + 'static,
{
    wtx.open_table(definition)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub(crate) fn idempotency_batch_id(
    write: &MutationWrite,
    batch: &MutationBatch,
) -> Result<Option<String>, String> {
    let identity_key = scope_identity_key(&batch.identity);
    let table = write
        .transaction()
        .open_table(IDEMPOTENCY)
        .map_err(|error| error.to_string())?;
    let existing = table
        .get((identity_key.as_str(), batch.idempotency_key.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| value.value().to_string());
    Ok(existing)
}

pub(crate) fn source_version(
    write: &MutationWrite,
    batch: &MutationBatch,
) -> Result<Option<u64>, String> {
    if batch.version_expectation == VersionExpectation::Unversioned {
        return Ok(None);
    }
    let binding_key = batch.identity.binding_digest().to_hex();
    let table = write
        .transaction()
        .open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    let version = table
        .get(binding_key.as_str())
        .map_err(|error| error.to_string())?
        .map(|value| Some(value.value()))
        .ok_or_else(|| "mutation scope binding is missing its authoritative version".to_string());
    version
}

pub(crate) fn read_record_in_write(
    write: &MutationWrite,
    identity: &MutationScopeIdentity,
    batch_id: &str,
) -> Result<Option<MutationBatchRecord>, String> {
    let identity_key = scope_identity_key(identity);
    let table = write
        .transaction()
        .open_table(BATCHES)
        .map_err(|error| error.to_string())?;
    let record = table
        .get((identity_key.as_str(), batch_id))
        .map_err(|error| error.to_string())?
        .map(|value| decode_batch_record(value.value()))
        .transpose();
    record
}

pub(crate) fn persist_record(
    write: &MutationWrite,
    record: &MutationBatchRecord,
) -> Result<(), String> {
    record.validate_write_budget()?;
    let bytes = encode_bounded(record, "mutation batch record")?;
    let identity_key = scope_identity_key(&record.identity);
    write
        .transaction()
        .open_table(BATCHES)
        .map_err(|error| error.to_string())?
        .insert(
            (identity_key.as_str(), record.batch.batch_id.as_str()),
            bytes.as_slice(),
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) fn persist_idempotency(
    write: &MutationWrite,
    batch: &MutationBatch,
) -> Result<(), String> {
    let identity_key = scope_identity_key(&batch.identity);
    write
        .transaction()
        .open_table(IDEMPOTENCY)
        .map_err(|error| error.to_string())?
        .insert(
            (identity_key.as_str(), batch.idempotency_key.as_str()),
            batch.batch_id.as_str(),
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) fn persist_private(
    write: &MutationWrite,
    record: &MutationBatchRecord,
    sealed: &[u8],
) -> Result<(), String> {
    validate_private_size(sealed)?;
    let digest = private_payload_digest(record)
        .ok_or_else(|| "private recovery payload has no digest-bound parent".to_string())?;
    write.authenticate_private(sealed, digest)?;
    let identity_key = scope_identity_key(&record.identity);
    write
        .transaction()
        .open_table(PRIVATE_PAYLOADS)
        .map_err(|error| error.to_string())?
        .insert(
            (identity_key.as_str(), record.batch.batch_id.as_str()),
            sealed,
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) fn read_private_in_write(
    write: &MutationWrite,
    record: &MutationBatchRecord,
) -> Result<Option<Vec<u8>>, String> {
    let identity_key = scope_identity_key(&record.identity);
    let table = write
        .transaction()
        .open_table(PRIVATE_PAYLOADS)
        .map_err(|error| error.to_string())?;
    let sealed = table
        .get((identity_key.as_str(), record.batch.batch_id.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| value.value().to_vec());
    if let Some(bytes) = &sealed {
        validate_private_size(bytes)?;
        let digest = private_payload_digest(record)
            .ok_or_else(|| "private recovery payload has no digest-bound parent".to_string())?;
        write.authenticate_private(bytes, digest)?;
    }
    Ok(sealed)
}

pub(crate) fn remove_private(
    wtx: &WriteTransaction,
    identity_key: &str,
    batch_id: &str,
) -> Result<(), String> {
    wtx.open_table(PRIVATE_PAYLOADS)
        .map_err(|error| error.to_string())?
        .remove((identity_key, batch_id))
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) fn verify_replay_identity(
    proposed: &MutationBatch,
    stored: &MutationBatch,
) -> Result<(), String> {
    proposed.validate_write_budget()?;
    stored.validate_write_budget()?;
    if encode_bounded(proposed, "proposed mutation batch")?
        == encode_bounded(stored, "stored mutation batch")?
    {
        Ok(())
    } else {
        Err("IDEMPOTENCY_CONFLICT: key was already used by a different mutation".to_string())
    }
}

/// The digest that BINDS a sealed private payload to its parent record.
///
/// Deliberately does not inspect `event_type`. This value is an authentication
/// input on the hot read/write path (`persist_private`, `read_private_in_write`,
/// `read_private_payload`); what makes it valid is that the parent carries a
/// single `ApplyMutation` whose query is a `sha256:` digest, not which family of
/// plan it belongs to. Gating the binding on a hard-coded event type is what
/// silently broke every SPARQL-HTTP saga: before this store was refactored the
/// string check lived only in the backup scan, and extracting a shared helper
/// carried it onto paths that never had it.
pub(crate) fn private_payload_digest(record: &MutationBatchRecord) -> Option<&str> {
    let operation = record.batch.operations.first()?;
    if record.batch.operations.len() != 1 {
        return None;
    }
    match &operation.method {
        eg_types::protocol::Method::ApplyMutation { query, .. }
            if query.len() == 71
                && query.starts_with("sha256:")
                && query[7..]
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) =>
        {
            Some(&query[7..])
        }
        _ => None,
    }
}

/// A payload digest whose parent is one of the DECLARED private-payload plan
/// shapes (`eg_types::mutation_batch::PRIVATE_PAYLOAD_EVENT_TYPES`).
///
/// Used only by the recovery/backup scan, which legitimately asserts that every
/// sealed row it walks belongs to a known plan family. The hot path must use
/// [`private_payload_digest`] instead.
pub(crate) fn recovery_plan_digest(record: &MutationBatchRecord) -> Option<&str> {
    let operation = record.batch.operations.first()?;
    let eg_types::protocol::Method::ApplyMutation { event_type, .. } = &operation.method else {
        return None;
    };
    if !eg_types::mutation_batch::PRIVATE_PAYLOAD_EVENT_TYPES
        .iter()
        .any(|known| known == event_type)
    {
        return None;
    }
    private_payload_digest(record)
}

pub(crate) fn encode_bounded<T: Serialize>(value: &T, label: &str) -> Result<Vec<u8>, String> {
    let mut counter = ByteBudget::default();
    rmp_serde::encode::write_named(&mut counter, value)
        .map_err(|_| format!("{label} exceeds its serialization budget"))?;
    let bytes = rmp_serde::to_vec_named(value).map_err(|error| error.to_string())?;
    if bytes.len() != counter.written {
        return Err(format!(
            "{label} serialization length changed after preflight"
        ));
    }
    Ok(bytes)
}

fn validate_private_size(sealed: &[u8]) -> Result<(), String> {
    if sealed.is_empty() || sealed.len() > MAX_MUTATION_RECORD_BYTES {
        return Err("private recovery payload exceeds its write budget".to_string());
    }
    Ok(())
}

#[derive(Default)]
struct ByteBudget {
    written: usize,
}

impl Write for ByteBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.written = self
            .written
            .checked_add(bytes.len())
            .filter(|count| *count <= MAX_MUTATION_RECORD_BYTES)
            .ok_or_else(|| std::io::Error::other("mutation serialization budget exceeded"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
