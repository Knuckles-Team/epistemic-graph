//! Scoped ledger reads. Every entry point needs a kernel-issued
//! [`ScopedRead`], which already proved the scope is bound to this store.

use crate::ledger::scope_identity_key;
use crate::ledger_tables::{BATCHES, OUTBOX, PRIVATE_PAYLOADS, VERSIONS};
use eg_storage::{
    decode_batch_record, decode_outbox_record, private_payload_digest, CollectionBudget,
    OwnerDomain, ScopedRead,
};
use eg_types::{MutationBatchRecord, MutationOutboxRecord};

/// Authoritative version of the read's bound scope.
pub fn version<D: OwnerDomain>(read: &ScopedRead<'_, D>) -> Result<u64, String> {
    let key = read.scope().binding_digest().to_hex();
    let table = read
        .transaction()
        .open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    table
        .get(key.as_str())
        .map_err(|error| error.to_string())?
        .map(|value| value.value())
        .ok_or_else(|| "mutation scope binding is missing its version row".to_string())
}

/// One persisted batch receipt of the read's bound scope.
pub fn read_record<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    batch_id: &str,
) -> Result<Option<MutationBatchRecord>, String> {
    let key = scope_identity_key(read.scope());
    let table = read
        .transaction()
        .open_table(BATCHES)
        .map_err(|error| error.to_string())?;
    table
        .get((key.as_str(), batch_id))
        .map_err(|error| error.to_string())?
        .map(|value| decode_batch_record(value.value()))
        .transpose()
}

/// Every outbox row of one batch of the read's bound scope.
pub fn read_outbox<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    batch_id: &str,
) -> Result<Vec<MutationOutboxRecord>, String> {
    let identity_key = scope_identity_key(read.scope());
    let table = read
        .transaction()
        .open_table(OUTBOX)
        .map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    let mut budget = CollectionBudget::default();
    for row in table
        .range((identity_key.as_str(), batch_id, 0)..=(identity_key.as_str(), batch_id, u32::MAX))
        .map_err(|error| error.to_string())?
    {
        let (_, value) = row.map_err(|error| error.to_string())?;
        budget.account(value.value().len())?;
        rows.push(decode_outbox_record(value.value())?);
    }
    Ok(rows)
}

/// The sealed private recovery payload of one prepared batch, authenticated
/// against its parent receipt.
pub fn read_private_payload<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    batch_id: &str,
) -> Result<Option<Vec<u8>>, String> {
    let identity_key = scope_identity_key(read.scope());
    let record = read_record(read, batch_id)?
        .ok_or_else(|| "private recovery plan has no parent receipt".to_string())?;
    let table = read
        .transaction()
        .open_table(PRIVATE_PAYLOADS)
        .map_err(|error| error.to_string())?;
    let sealed = table
        .get((identity_key.as_str(), batch_id))
        .map_err(|error| error.to_string())?
        .map(|value| value.value().to_vec());
    if let Some(bytes) = &sealed {
        let digest = private_payload_digest(&record)
            .ok_or_else(|| "private recovery payload has no digest-bound parent".to_string())?;
        read.authenticate_private(bytes, digest)?;
    }
    Ok(sealed)
}
