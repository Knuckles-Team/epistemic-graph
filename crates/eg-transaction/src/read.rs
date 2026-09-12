//! Scoped ledger reads. Every entry point needs a kernel-issued
//! [`ScopedRead`], which already proved the scope is bound to this store.

use crate::commit::MAX_BATCH_ID_SENTINEL;
use crate::tables::{
    BATCHES, CLASSES, FENCES, OUTBOX, PRIVATE_PAYLOADS, REPLAY_OPERATIONS, VERSIONS,
};
use eg_storage::{
    decode_batch_record, decode_ledger_record, decode_outbox_record, ledger_scope_key,
    private_payload_digest, CollectionBudget, MutationClass, MutationClassRow, OperationReplayRow,
    OwnerDomain, ScopeFence, ScopedRead,
};
use eg_types::{MutationBatchRecord, MutationOutboxRecord};

/// Authoritative version of the read's bound scope.
pub fn version<D: OwnerDomain>(read: &ScopedRead<'_, D>) -> Result<u64, String> {
    let key = ledger_scope_key(read.scope());
    read.scoped_table(VERSIONS)?
        .get(key.as_str())?
        .map(|value| value.value())
        .ok_or_else(|| "mutation scope binding is missing its version row".to_string())
}

/// One persisted batch receipt of the read's bound scope.
pub fn read_ledger<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    batch_id: &str,
) -> Result<Option<MutationBatchRecord>, String> {
    let key = ledger_scope_key(read.scope());
    read.scoped_table(BATCHES)?
        .get((key.as_str(), batch_id))?
        .map(|value| decode_batch_record(value.value()))
        .transpose()
}

/// One durable typed replay operation row of the read's bound scope.
pub fn read_replay_operation<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    idempotency_key: &str,
) -> Result<Option<OperationReplayRow>, String> {
    let identity_key = ledger_scope_key(read.scope());
    read.scoped_table(REPLAY_OPERATIONS)?
        .get((identity_key.as_str(), idempotency_key))?
        .map(|value| decode_ledger_record::<OperationReplayRow>(value.value()))
        .transpose()
}

/// Every outbox row of one batch of the read's bound scope.
pub fn read_outbox<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    batch_id: &str,
) -> Result<Vec<MutationOutboxRecord>, String> {
    let identity_key = ledger_scope_key(read.scope());
    let table = read.scoped_table(OUTBOX)?;
    let mut rows = Vec::new();
    let mut budget = CollectionBudget::default();
    for row in table.range_inclusive(
        (identity_key.as_str(), batch_id, 0),
        (identity_key.as_str(), batch_id, u32::MAX),
    )? {
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
    let identity_key = ledger_scope_key(read.scope());
    let record = read_ledger(read, batch_id)?
        .ok_or_else(|| "private recovery plan has no parent receipt".to_string())?;
    let sealed = read
        .scoped_table(PRIVATE_PAYLOADS)?
        .get((identity_key.as_str(), batch_id))?
        .map(|value| value.value().to_vec());
    if let Some(bytes) = &sealed {
        let digest = private_payload_digest(&record)
            .ok_or_else(|| "private recovery payload has no digest-bound parent".to_string())?;
        read.authenticate_private(bytes, digest)?;
    }
    Ok(sealed)
}

/// Every persisted batch receipt of the read's bound scope, in key order.
pub fn read_batches<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
) -> Result<Vec<MutationBatchRecord>, String> {
    let key = ledger_scope_key(read.scope());
    let table = read.scoped_table(BATCHES)?;
    let mut records = Vec::new();
    let mut budget = CollectionBudget::default();
    for row in table.range_inclusive((key.as_str(), ""), (key.as_str(), MAX_BATCH_ID_SENTINEL))? {
        let (_, value) = row.map_err(|error| error.to_string())?;
        budget.account(value.value().len())?;
        records.push(decode_batch_record(value.value())?);
    }
    Ok(records)
}

/// The durable route fence of the read's bound scope, if one was ever written.
pub fn read_fences<D: OwnerDomain>(read: &ScopedRead<'_, D>) -> Result<Option<ScopeFence>, String> {
    let key = ledger_scope_key(read.scope());
    let fence = read
        .scoped_table(FENCES)?
        .get(key.as_str())?
        .map(|value| decode_ledger_record::<ScopeFence>(value.value()))
        .transpose()?;
    Ok(fence)
}

/// Position inside one scope's outbox stream.
///
/// This is a *paging* position over [`read_outbox`], not a durable delivery
/// claim or lease. Delivery claims, acknowledgements, release/expiry, and the
/// delivery-side fairness cap are implemented by `crate::outbox` and exposed
/// through `MutationKernel::outbox_*`; this cursor only advances through rows
/// returned by [`read_outbox`]. The six delivery tables
/// (`mutation_outbox_consumers`, `..._deliveries`, `..._cursors`,
/// `..._claim_cursors`, `..._fairness`, `..._topic_index`) are the durable
/// delivery ledger used by those APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxCursor {
    batch_id: String,
    next_ordinal: u32,
}

impl OutboxCursor {
    /// Start of one batch's outbox stream.
    pub fn start(batch_id: &str) -> Self {
        Self {
            batch_id: batch_id.to_string(),
            next_ordinal: 0,
        }
    }

    pub fn batch_id(&self) -> &str {
        &self.batch_id
    }

    pub fn next_ordinal(&self) -> u32 {
        self.next_ordinal
    }

    /// Advance past `record`, which must be the next row of this stream.
    pub fn advance(&mut self, record: &MutationOutboxRecord) -> Result<(), String> {
        if record.batch_id != self.batch_id || record.ordinal != self.next_ordinal {
            return Err("outbox cursor advanced past a row it does not name".to_string());
        }
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| "outbox cursor ordinal overflow".to_string())?;
        Ok(())
    }
}

/// The durable class of one committed batch of the read's bound scope.
pub fn read_class<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    batch_id: &str,
) -> Result<Option<MutationClass>, String> {
    let key = ledger_scope_key(read.scope());
    let row = read
        .scoped_table(CLASSES)?
        .get((key.as_str(), batch_id))?
        .map(|value| decode_ledger_record::<MutationClassRow>(value.value()))
        .transpose()?;
    Ok(row.map(|row| row.class))
}
