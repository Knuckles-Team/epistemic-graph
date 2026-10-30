//! Multimodal memory over the blob substrate (EG-MEDIA-VOCAB-R650).
//!
//! Media the user sends is stored once, content-addressed by sha256. Each
//! derivation (transcript, caption) is a separate derived node. Recall is by
//! digest or by derivation text, with a deterministic order (digest, then
//! derivation id). Oversized blobs and blobs whose declared type disagrees
//! with the sniffed type are refused with a stable code and no state change.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Largest blob this memory accepts.
pub const MEDIA_MEMORY_MAX_BYTES: usize = 32 * 1024 * 1024;

/// A stored media blob node (tenant-scoped).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaMemory {
    pub blob_id: String,
    pub mime: String,
    pub transcript_ref: Option<String>,
    pub source_message: String,
}

/// A recall result: the blob plus its derivations and the citing memory entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaRecall {
    pub blob_id: String,
    pub mime: String,
    pub size: u64,
    pub derivation_ids: Vec<String>,
    pub citing_memory: String,
}

/// Stable refusal codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaMemoryRefusal {
    OversizeBlob,
    TypeMismatch,
    UnknownBlob,
}

impl MediaMemoryRefusal {
    pub fn code(self) -> &'static str {
        match self {
            Self::OversizeBlob => "media_memory_oversize_blob",
            Self::TypeMismatch => "media_memory_type_mismatch",
            Self::UnknownBlob => "media_memory_unknown_blob",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Entry {
    mime: String,
    size: u64,
    derivations: BTreeMap<String, String>,
    citing_memory: String,
    source_message: String,
}

/// Tenant-scoped media memory. One writer per tenant shard owns `&mut self`.
#[derive(Debug, Default)]
pub struct MediaMemoryStore {
    tenants: BTreeMap<String, BTreeMap<String, Entry>>,
}

/// Sniff a MIME type from leading magic bytes.
pub fn sniff_mime(bytes: &[u8]) -> Option<&'static str> {
    match bytes {
        [0x89, b'P', b'N', b'G', ..] => Some("image/png"),
        [0xFF, 0xD8, 0xFF, ..] => Some("image/jpeg"),
        [b'G', b'I', b'F', b'8', ..] => Some("image/gif"),
        [b'R', b'I', b'F', b'F', _, _, _, _, b'W', b'A', b'V', b'E', ..] => Some("audio/wav"),
        [b'O', b'g', b'g', b'S', ..] => Some("audio/ogg"),
        [b'I', b'D', b'3', ..] => Some("audio/mpeg"),
        _ => None,
    }
}

fn digest_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

impl MediaMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Write the blob once by digest. A repeat write of the same bytes returns
    /// the same node and keeps the first citing memory. Refusals change nothing.
    pub fn remember(
        &mut self,
        tenant: &str,
        bytes: &[u8],
        declared_mime: &str,
        source_message: &str,
        citing_memory: &str,
    ) -> Result<MediaMemory, MediaMemoryRefusal> {
        if bytes.len() > MEDIA_MEMORY_MAX_BYTES {
            return Err(MediaMemoryRefusal::OversizeBlob);
        }
        if sniff_mime(bytes) != Some(declared_mime) {
            return Err(MediaMemoryRefusal::TypeMismatch);
        }
        let blob_id = digest_hex(bytes);
        let entry = self
            .tenants
            .entry(tenant.to_string())
            .or_default()
            .entry(blob_id.clone())
            .or_insert_with(|| Entry {
                mime: declared_mime.to_string(),
                size: bytes.len() as u64,
                citing_memory: citing_memory.to_string(),
                source_message: source_message.to_string(),
                ..Entry::default()
            });
        Ok(MediaMemory {
            blob_id,
            mime: entry.mime.clone(),
            transcript_ref: entry.derivations.keys().next().cloned(),
            source_message: entry.source_message.clone(),
        })
    }

    /// Attach a derivation (transcript, caption) as a separate derived node.
    pub fn derive(
        &mut self,
        tenant: &str,
        blob_id: &str,
        derivation_id: &str,
        text: &str,
    ) -> Result<(), MediaMemoryRefusal> {
        let entry = self
            .tenants
            .get_mut(tenant)
            .and_then(|t| t.get_mut(blob_id))
            .ok_or(MediaMemoryRefusal::UnknownBlob)?;
        entry
            .derivations
            .insert(derivation_id.to_string(), text.to_string());
        Ok(())
    }

    fn recall_of(blob_id: &str, e: &Entry) -> MediaRecall {
        MediaRecall {
            blob_id: blob_id.to_string(),
            mime: e.mime.clone(),
            size: e.size,
            derivation_ids: e.derivations.keys().cloned().collect(),
            citing_memory: e.citing_memory.clone(),
        }
    }

    /// Recall by digest.
    pub fn recall_by_digest(&self, tenant: &str, blob_id: &str) -> Option<MediaRecall> {
        let e = self.tenants.get(tenant)?.get(blob_id)?;
        Some(Self::recall_of(blob_id, e))
    }

    /// Recall by derivation text (case-insensitive substring), ordered by digest.
    pub fn recall_by_text(&self, tenant: &str, needle: &str) -> Vec<MediaRecall> {
        let needle = needle.to_lowercase();
        self.tenants
            .get(tenant)
            .into_iter()
            .flatten()
            .filter(|(_, e)| {
                e.derivations
                    .values()
                    .any(|t| t.to_lowercase().contains(&needle))
            })
            .map(|(id, e)| Self::recall_of(id, e))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PNG: &[u8] = &[0x89, b'P', b'N', b'G', 1, 2, 3, 4];

    #[test]
    fn blob_written_once_and_recalled_by_digest_and_text() {
        let mut s = MediaMemoryStore::new();
        let a = s.remember("t", PNG, "image/png", "m1", "mem1").unwrap();
        let b = s.remember("t", PNG, "image/png", "m2", "mem2").unwrap();
        assert_eq!(a.blob_id, b.blob_id);
        s.derive("t", &a.blob_id, "d1", "A red chart").unwrap();
        s.derive("t", &a.blob_id, "d2", "caption").unwrap();
        let r = s.recall_by_digest("t", &a.blob_id).unwrap();
        assert_eq!(r.derivation_ids, vec!["d1", "d2"]);
        assert_eq!(r.citing_memory, "mem1");
        assert_eq!(s.recall_by_text("t", "RED"), vec![r]);
        assert!(s.recall_by_text("other", "red").is_empty());
    }

    #[test]
    fn refusals_have_stable_codes_and_change_nothing() {
        let mut s = MediaMemoryStore::new();
        assert_eq!(
            s.remember("t", PNG, "image/jpeg", "m", "c"),
            Err(MediaMemoryRefusal::TypeMismatch)
        );
        let big = vec![0u8; MEDIA_MEMORY_MAX_BYTES + 1];
        assert_eq!(
            s.remember("t", &big, "image/png", "m", "c"),
            Err(MediaMemoryRefusal::OversizeBlob)
        );
        assert!(s.tenants.is_empty());
        assert_eq!(MediaMemoryRefusal::OversizeBlob.code(), "media_memory_oversize_blob");
        assert_eq!(s.derive("t", "x", "d", "t"), Err(MediaMemoryRefusal::UnknownBlob));
    }
}
