//! The content-addressed manifest shape, its resource bounds and codecs, and
//! the one row write every manifest commit goes through.

use super::{CAS_BLOBS, CAS_RETENTION};
use eg_storage::BlobOwner;
use eg_transaction::AdmittedOwnerWrite;
use redb::ReadableTable;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub(crate) const MAX_BLOB_CHUNK_BYTES: usize = 64 * 1024 * 1024;
const MAX_BLOB_MANIFEST_BYTES: usize = 128 * 1024 * 1024;
const MAX_BLOB_MANIFEST_ITEMS: usize = 3_000_000;
pub(super) const MAX_BLOB_CHUNKS: usize = 1_000_000;
pub(super) const MAX_BLOB_GC_TRACKED_DIGESTS: usize = 1_000_000;
/// Longest owner scope a manifest, upload or holder row may carry.
pub(super) const MAX_OWNER_SCOPE_BYTES: usize = 256;

/// Current manifest of a content-addressed blob: the ordered chunk digests,
/// exact per-chunk lengths, total length, and opaque owner. Serialized to
/// MessagePack; the blob digest is the SHA-256 of those bytes.
///
/// Only this explicitly versioned shape is accepted. Retired fixed-stride or
/// unowned manifests must be migrated offline before the engine starts; the live
/// serving path never guesses missing boundaries or authority.
pub const BLOB_MANIFEST_VERSION: u16 = 2;
pub const ENGINE_BLOB_OWNER_SCOPE: &str = "engine-internal";

pub(crate) fn validate_digest(digest: &str) -> Result<(), String> {
    if is_content_digest(digest) {
        Ok(())
    } else {
        Err("blob digest is invalid".to_string())
    }
}

fn is_content_digest(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn validate_owner_scope(owner_scope: &str) -> Result<(), String> {
    if owner_scope.is_empty() || owner_scope.len() > MAX_OWNER_SCOPE_BYTES {
        Err("blob owner scope is invalid or exceeds resource limits".to_string())
    } else {
        Ok(())
    }
}

/// Insert into a garbage-collection working set, failing closed past its bound.
pub(super) fn track_gc_digest(set: &mut BTreeSet<String>, digest: String) -> Result<(), String> {
    set.insert(digest);
    if set.len() > MAX_BLOB_GC_TRACKED_DIGESTS {
        Err("blob garbage collection exceeds resource limits".to_string())
    } else {
        Ok(())
    }
}

/// Bound one chunk body and return its content address.
pub(super) fn chunk_digest(bytes: &[u8]) -> Result<String, String> {
    if bytes.len() > MAX_BLOB_CHUNK_BYTES {
        return Err("blob chunk exceeds resource limits".to_string());
    }
    Ok(hex_digest(bytes))
}

fn blob_msgpack_limits() -> eg_types::msgpack::MsgpackLimits {
    eg_types::msgpack::MsgpackLimits::new(
        MAX_BLOB_MANIFEST_BYTES,
        MAX_BLOB_MANIFEST_ITEMS,
        eg_types::msgpack::DEFAULT_MAX_DEPTH,
    )
}

pub(super) fn decode_blob_value<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(bytes, blob_msgpack_limits())
        .map_err(|_| "blob metadata is invalid or exceeds resource limits".to_string())
}

/// Encode one blob-store row value and bound-check it the way every reader will
/// decode it, so no row is written that its own decoder would refuse.
pub(super) fn encode_blob_value<T: Serialize>(value: &T, what: &str) -> Result<Vec<u8>, String> {
    let bytes = rmp_serde::to_vec_named(value).map_err(|error| error.to_string())?;
    eg_types::msgpack::validate_single_value(&bytes, blob_msgpack_limits())
        .map_err(|_| format!("{what} exceeds resource limits"))?;
    Ok(bytes)
}

pub(super) fn decode_manifest(bytes: &[u8]) -> Result<BlobManifest, String> {
    let manifest: BlobManifest = decode_blob_value(bytes)?;
    manifest.validate()?;
    Ok(manifest)
}

/// Encode + bound-check a manifest and prove `blob_digest` IS its content address.
pub(super) fn encode_manifest(
    blob_digest: &str,
    manifest: &BlobManifest,
) -> Result<Vec<u8>, String> {
    let bytes = encode_manifest_bytes(manifest)?;
    validate_digest(blob_digest)?;
    if blob_digest != hex_digest(&bytes) {
        return Err("blob manifest digest does not match its content".to_string());
    }
    Ok(bytes)
}

/// Encode a validated manifest; its digest is `hex_digest` of the result.
pub(in crate::server::blob) fn encode_manifest_bytes(
    manifest: &BlobManifest,
) -> Result<Vec<u8>, String> {
    manifest.validate()?;
    encode_blob_value(manifest, "blob manifest")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobManifest {
    pub schema_version: u16,
    /// Verified tenant+principal ownership, or the reserved engine-internal scope.
    pub owner_scope: String,
    /// Hex sha256 chunk digests, in file order.
    pub chunks: Vec<String>,
    /// Exact per-chunk byte length, parallel to `chunks`.
    pub chunk_lens: Vec<u32>,
    /// Total length of the assembled blob in bytes.
    pub len: u64,
    /// Requested upload/chunker size. Zero denotes the native content-defined
    /// chunker default; boundaries are always read from `chunk_lens`.
    pub chunk_size: u32,
}

impl BlobManifest {
    pub(crate) fn validate(&self) -> Result<(), String> {
        self.validate_shape()?;
        if self.chunks.is_empty() {
            return if self.len == 0 && self.chunk_lens.is_empty() {
                Ok(())
            } else {
                Err("blob manifest is inconsistent".to_string())
            };
        }
        self.validate_nonempty_lengths()
    }

    fn validate_shape(&self) -> Result<(), String> {
        if self.schema_version != BLOB_MANIFEST_VERSION
            || validate_owner_scope(&self.owner_scope).is_err()
            || self.chunks.len() > MAX_BLOB_CHUNKS
            || !self.chunks.iter().all(|digest| is_content_digest(digest))
        {
            return Err("blob manifest is invalid or exceeds resource limits".to_string());
        }
        Ok(())
    }

    fn validate_nonempty_lengths(&self) -> Result<(), String> {
        if self.chunk_lens.len() != self.chunks.len()
            || self.chunk_size as usize > MAX_BLOB_CHUNK_BYTES
            || self
                .chunk_lens
                .iter()
                .any(|length| *length == 0 || *length as usize > MAX_BLOB_CHUNK_BYTES)
        {
            return Err("blob manifest is inconsistent".to_string());
        }
        let total = self
            .chunk_lens
            .iter()
            .try_fold(0u64, |sum, length| sum.checked_add(*length as u64));
        if total != Some(self.len) {
            return Err("blob manifest is inconsistent".to_string());
        }
        Ok(())
    }

    /// Exact per-chunk byte lengths, in file order.
    pub fn chunk_lengths(&self) -> Vec<u32> {
        self.chunk_lens.clone()
    }

    /// Per-chunk `(offset, len)` boundaries, in file order — the running prefix sum
    /// of [`chunk_lengths`](Self::chunk_lengths). For random-access/seek over a blob
    /// (CONCEPT:EG-KG.storage.backward-manifest-read variable boundaries).
    pub fn chunk_offsets(&self) -> Vec<(u64, u32)> {
        let mut out = Vec::with_capacity(self.chunks.len());
        let mut off = 0u64;
        for l in self.chunk_lengths() {
            out.push((off, l));
            off += l as u64;
        }
        out
    }
}

/// Hex sha256 of `bytes` — the content address of a chunk or a manifest.
pub fn hex_digest(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// Outcome of committing an upload cursor: the content address of the whole blob
/// plus the manifest needed to build the `:Blob` graph shape.
#[derive(Debug, Clone)]
pub struct CommittedBlob {
    pub digest: String,
    pub manifest: BlobManifest,
}

/// Write one manifest row inside an admitted blob write.
///
/// Every manifest commit goes through here, so two properties hold for all of
/// them at once:
/// * **no dangling manifest**: every chunk it names must already be a committed
///   chunk row in this same transaction. A chunk deduplicated against one that a
///   concurrent sweep then reclaimed fails the commit instead of committing a
///   manifest whose bytes are gone;
/// * **grace from the latest commit**: the manifest's retention clock moves to
///   `committed_at_ms` (never backwards), so an upload that has not yet taken its
///   first holder survives every sweep inside the grace period, even when
///   identical content was committed and released earlier.
pub(super) fn record_manifest(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    chunk_stored: &dyn Fn(&str) -> Result<bool, String>,
    digest: &str,
    manifest: &BlobManifest,
    committed_at_ms: u64,
) -> Result<(), String> {
    let bytes = encode_manifest(digest, manifest)?;
    for chunk in &manifest.chunks {
        if !chunk_stored(chunk)? {
            return Err("blob manifest names a chunk that is not stored".to_string());
        }
    }
    wtx.open_table(CAS_BLOBS)?
        .insert(digest, bytes.as_slice())
        .map_err(|error| error.to_string())?;
    let mut retention = wtx.open_table(CAS_RETENTION)?;
    let previous = retention
        .get(digest)
        .map_err(|error| error.to_string())?
        .map_or(0, |value| value.value());
    retention
        .insert(digest, previous.max(committed_at_ms))
        .map_err(|error| error.to_string())?;
    Ok(())
}
