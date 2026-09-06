//! Bounded msgpack codec for every durable row the storage kernel validates.
//!
//! These helpers are serialization only. They carry no authority: a caller
//! still needs a kernel-issued read or write capability to reach a transaction.

use eg_types::{MutationBatchRecord, MutationOutboxRecord};
use serde::Serialize;
use std::io::Write;

pub(crate) const MAX_MUTATION_RECORD_BYTES: usize = 64 * 1024 * 1024;
const MAX_MUTATION_RECORD_ITEMS: usize = 1_000_000;
const MAX_MUTATION_COLLECTION_ROWS: usize = 100_000;
const MAX_MUTATION_COLLECTION_BYTES: usize = 512 * 1024 * 1024;

/// Decode one bounded durable row.
pub fn decode_ledger_record<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_MUTATION_RECORD_BYTES,
            MAX_MUTATION_RECORD_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "stored mutation record is invalid or exceeds resource limits".to_string())
}

/// Decode and validate one persisted batch receipt.
pub fn decode_batch_record(bytes: &[u8]) -> Result<MutationBatchRecord, String> {
    let record: MutationBatchRecord = decode_ledger_record(bytes)?;
    record.validate()?;
    Ok(record)
}

/// Decode and validate one persisted outbox row.
pub fn decode_outbox_record(bytes: &[u8]) -> Result<MutationOutboxRecord, String> {
    let record: MutationOutboxRecord = decode_ledger_record(bytes)?;
    record.validate()?;
    Ok(record)
}

/// Encode one durable row under its serialization budget.
pub fn encode_bounded<T: Serialize>(value: &T, label: &str) -> Result<Vec<u8>, String> {
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

/// Bounded accumulator for a decoded collection of durable rows.
#[derive(Default)]
pub struct CollectionBudget {
    rows: usize,
    bytes: usize,
}

impl CollectionBudget {
    pub fn account(&mut self, added: usize) -> Result<(), String> {
        self.rows = self
            .rows
            .checked_add(1)
            .filter(|count| *count <= MAX_MUTATION_COLLECTION_ROWS)
            .ok_or_else(|| "mutation record collection exceeds resource limits".to_string())?;
        self.bytes = self
            .bytes
            .checked_add(added)
            .filter(|count| *count <= MAX_MUTATION_COLLECTION_BYTES)
            .ok_or_else(|| "mutation record collection exceeds resource limits".to_string())?;
        Ok(())
    }
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
