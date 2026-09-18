//! Durable upload cursors: begin, append, commit, expiry, and the never-reused
//! cursor-id high-water mark.

use super::manifest::{
    decode_blob_value, encode_blob_value, encode_manifest_bytes, hex_digest, record_manifest,
    validate_owner_scope, BlobManifest, BLOB_MANIFEST_VERSION, MAX_BLOB_CHUNKS,
    MAX_BLOB_CHUNK_BYTES,
};
use super::{CAS_COUNTERS, CAS_UPLOADS};
use eg_storage::{BlobOwner, BlobSharedWrite};
use eg_transaction::AdmittedOwnerWrite;
use redb::ReadableTable;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// `cas_counters` key of the highest upload cursor id ever begun durably.
const UPLOAD_CURSOR_HIGH_WATER: &str = "upload_cursor_high_water";
const MAX_BATCH_ID_BYTES: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DurableUpload {
    owner_scope: String,
    chunk_digests: Vec<String>,
    chunk_lens: Vec<u32>,
    len: u64,
    chunk_size: u32,
    /// Commit timestamp of the latest begin or chunk batch: the expiry clock.
    last_active_ms: u64,
    /// Batches already applied, so an acknowledgement-lost chunk put replays
    /// without appending twice. Ordered, so the encoded row is deterministic.
    applied_batches: BTreeSet<String>,
}

impl DurableUpload {
    fn new(owner_scope: &str, chunk_size: u32, at_ms: u64) -> Self {
        Self {
            owner_scope: owner_scope.to_string(),
            chunk_digests: Vec::new(),
            chunk_lens: Vec::new(),
            len: 0,
            chunk_size,
            last_active_ms: at_ms,
            applied_batches: BTreeSet::new(),
        }
    }

    pub(super) fn manifest(&self) -> BlobManifest {
        BlobManifest {
            schema_version: BLOB_MANIFEST_VERSION,
            owner_scope: self.owner_scope.clone(),
            chunks: self.chunk_digests.clone(),
            chunk_lens: self.chunk_lens.clone(),
            len: self.len,
            chunk_size: self.chunk_size,
        }
    }

    pub(super) fn owner_scope(&self) -> &str {
        &self.owner_scope
    }

    pub(super) fn last_active_ms(&self) -> u64 {
        self.last_active_ms
    }

    pub(super) fn into_chunks(self) -> Vec<String> {
        self.chunk_digests
    }

    fn validate(&self) -> Result<(), String> {
        if self.chunk_size == 0
            || self.chunk_size as usize > MAX_BLOB_CHUNK_BYTES
            || self.applied_batches.len() > MAX_BLOB_CHUNKS
            || self
                .applied_batches
                .iter()
                .any(|batch_id| batch_id.is_empty() || batch_id.len() > MAX_BATCH_ID_BYTES)
        {
            return Err("blob upload exceeds resource limits".to_string());
        }
        self.manifest().validate()
    }

    /// Append one chunk for `batch_id` unless that batch was already applied.
    fn append(
        &mut self,
        batch_id: &str,
        digest: &str,
        length: usize,
        at_ms: u64,
    ) -> Result<(), String> {
        if !self.applied_batches.insert(batch_id.to_string()) {
            return Ok(());
        }
        if self.chunk_digests.len() >= MAX_BLOB_CHUNKS {
            return Err("blob upload exceeds resource limits".to_string());
        }
        let length32 =
            u32::try_from(length).map_err(|_| "blob chunk exceeds resource limits".to_string())?;
        self.chunk_digests.push(digest.to_string());
        self.chunk_lens.push(length32);
        self.len = self
            .len
            .checked_add(u64::from(length32))
            .ok_or_else(|| "blob upload exceeds resource limits".to_string())?;
        self.last_active_ms = self.last_active_ms.max(at_ms);
        self.validate()
    }
}

pub(super) fn decode_upload(bytes: &[u8]) -> Result<DurableUpload, String> {
    let upload: DurableUpload = decode_blob_value(bytes)?;
    upload.validate()?;
    Ok(upload)
}

fn read_upload(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    cursor: u64,
) -> Result<Option<DurableUpload>, String> {
    let uploads = wtx.open_table(CAS_UPLOADS)?;
    let row = uploads.get(cursor).map_err(|error| error.to_string())?;
    // Bind before returning: the guard borrows `uploads` (E0597 otherwise).
    let decoded = row.map(|value| decode_upload(value.value())).transpose();
    decoded
}

fn write_upload(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    cursor: u64,
    upload: &DurableUpload,
) -> Result<(), String> {
    upload.validate()?;
    let bytes = encode_blob_value(upload, "blob upload")?;
    wtx.open_table(CAS_UPLOADS)?
        .insert(cursor, bytes.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Begin upload `cursor` for `owner_scope`.
///
/// A cursor id names exactly one upload for the life of the store: a row that
/// already exists under this id is refused rather than adopted (adopting it
/// handed a restarted engine's reused id to a stale upload), and the high-water
/// mark moves in the same transaction so a restart never allocates at or below
/// any id that was ever begun.
pub(super) fn begin_upload(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    cursor: u64,
    chunk_size: u32,
    owner_scope: &str,
    at_ms: u64,
) -> Result<u64, String> {
    validate_owner_scope(owner_scope)?;
    if read_upload(wtx, cursor)?.is_some() {
        return Err("blob upload cursor id is already in use".to_string());
    }
    write_upload(
        wtx,
        cursor,
        &DurableUpload::new(owner_scope, chunk_size, at_ms),
    )?;
    let mut counters = wtx.open_table(CAS_COUNTERS)?;
    let high_water = counters
        .get(UPLOAD_CURSOR_HIGH_WATER)
        .map_err(|error| error.to_string())?
        .map_or(0, |value| value.value());
    counters
        .insert(UPLOAD_CURSOR_HIGH_WATER, high_water.max(cursor))
        .map_err(|error| error.to_string())?;
    Ok(cursor)
}

/// Store one chunk and append it to upload `cursor` in the same transaction.
pub(super) fn put_upload_chunk(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    shared: &BlobSharedWrite<'_>,
    cursor: u64,
    chunk: (&str, &[u8]),
    batch_id: &str,
    at_ms: u64,
) -> Result<(String, u32), String> {
    let (digest, bytes) = chunk;
    shared.insert_chunk_if_absent(digest, bytes)?;
    let mut upload =
        read_upload(wtx, cursor)?.ok_or_else(|| "unknown durable upload cursor".to_string())?;
    upload.append(batch_id, digest, bytes.len(), at_ms)?;
    write_upload(wtx, cursor, &upload)?;
    let count = u32::try_from(upload.chunk_digests.len())
        .map_err(|_| "blob upload exceeds resource limits".to_string())?;
    Ok((digest.to_string(), count))
}

/// Turn upload `cursor` into a committed manifest and retire the cursor.
pub(super) fn commit_upload(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    chunk_stored: &dyn Fn(&str) -> Result<bool, String>,
    cursor: u64,
    at_ms: u64,
) -> Result<String, String> {
    let upload =
        read_upload(wtx, cursor)?.ok_or_else(|| "unknown durable upload cursor".to_string())?;
    let manifest = upload.manifest();
    let digest = hex_digest(&encode_manifest_bytes(&manifest)?);
    record_manifest(wtx, chunk_stored, &digest, &manifest, at_ms)?;
    wtx.open_table(CAS_UPLOADS)?
        .remove(cursor)
        .map_err(|error| error.to_string())?;
    Ok(digest)
}

/// The highest upload cursor id ever begun in this store (0 when none).
pub(super) fn upload_cursor_high_water<T>(counters: &T) -> Result<u64, String>
where
    T: ReadableTable<&'static str, u64>,
{
    let high_water = counters
        .get(UPLOAD_CURSOR_HIGH_WATER)
        .map_err(|error| error.to_string())?
        .map_or(0, |value| value.value());
    Ok(high_water)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_is_idempotent_per_batch_and_moves_the_expiry_clock_forward_only() {
        let mut upload = DurableUpload::new("carrier-owner:a", 8, 100);
        let digest = "b".repeat(64);
        upload.append("batch-1", &digest, 4, 150).unwrap();
        upload.append("batch-1", &digest, 4, 175).unwrap();
        assert_eq!((upload.chunk_digests.len(), upload.len), (1, 4));
        assert_eq!(upload.last_active_ms(), 150);
        upload.append("batch-2", &digest, 4, 120).unwrap();
        assert_eq!((upload.chunk_digests.len(), upload.len), (2, 8));
        assert_eq!(upload.last_active_ms(), 150);
    }

    #[test]
    fn the_encoded_row_is_independent_of_batch_arrival_order() {
        let digest = "c".repeat(64);
        let mut first = DurableUpload::new("carrier-owner:a", 8, 1);
        let mut second = DurableUpload::new("carrier-owner:a", 8, 1);
        for batch in ["z", "a", "m"] {
            first.applied_batches.insert(batch.to_string());
        }
        for batch in ["m", "z", "a"] {
            second.applied_batches.insert(batch.to_string());
        }
        first.append("q", &digest, 2, 1).unwrap();
        second.append("q", &digest, 2, 1).unwrap();
        assert_eq!(
            encode_blob_value(&first, "upload").unwrap(),
            encode_blob_value(&second, "upload").unwrap()
        );
    }

    #[test]
    fn an_upload_row_with_an_unknown_field_is_refused() {
        #[derive(Serialize)]
        struct Widened<'a> {
            #[serde(flatten)]
            upload: &'a DurableUpload,
            extra: u8,
        }
        let upload = DurableUpload::new("carrier-owner:a", 8, 1);
        let bytes = rmp_serde::to_vec_named(&Widened {
            upload: &upload,
            extra: 1,
        })
        .unwrap();
        assert!(decode_upload(&bytes).is_err());
        assert!(decode_upload(&encode_blob_value(&upload, "upload").unwrap()).is_ok());
    }
}
