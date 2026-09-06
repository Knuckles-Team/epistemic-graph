//! Durable ledger row primitives, written through a storage-kernel capability.

use crate::ledger_tables::{BATCHES, IDEMPOTENCY, PRIVATE_PAYLOADS, VERSIONS};
use crate::write::MutationWrite;
use eg_storage::{decode_batch_record, encode_bounded, private_payload_digest, OwnerDomain};
use eg_types::{MutationBatch, MutationBatchRecord, MutationScopeIdentity, VersionExpectation};
use redb::{ReadableTable, WriteTransaction};

const MAX_PRIVATE_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Stable per-scope ledger key.
pub(crate) fn scope_identity_key(identity: &MutationScopeIdentity) -> String {
    identity.identity_digest().to_hex()
}

pub(crate) fn idempotency_batch_id<D: OwnerDomain>(
    write: &MutationWrite<'_, D>,
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

pub(crate) fn source_version<D: OwnerDomain>(
    write: &MutationWrite<'_, D>,
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

pub(crate) fn read_record_in_write<D: OwnerDomain>(
    write: &MutationWrite<'_, D>,
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

pub(crate) fn persist_record<D: OwnerDomain>(
    write: &MutationWrite<'_, D>,
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

pub(crate) fn persist_idempotency<D: OwnerDomain>(
    write: &MutationWrite<'_, D>,
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

pub(crate) fn persist_private<D: OwnerDomain>(
    write: &MutationWrite<'_, D>,
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

pub(crate) fn read_private_in_write<D: OwnerDomain>(
    write: &MutationWrite<'_, D>,
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

fn validate_private_size(sealed: &[u8]) -> Result<(), String> {
    if sealed.is_empty() || sealed.len() > MAX_PRIVATE_PAYLOAD_BYTES {
        return Err("private recovery payload exceeds its write budget".to_string());
    }
    Ok(())
}
