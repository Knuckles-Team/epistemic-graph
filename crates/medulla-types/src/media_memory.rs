//! Multimodal memory wire types (MEDULLA-MEDIA-VOCAB-R650.1).
//!
//! `Method::MediaRemember` stores a media blob once by its sha256 digest and
//! answers [`MediaMemory`]; `Method::MediaRecall` answers the stored blob as
//! [`MediaRecall`] rows. The store itself lives in
//! `medulla_modality::media_memory`, which re-exports these types; they live here so
//! the protocol and the generated contract can name them.

use serde::{Deserialize, Serialize};

/// A stored media blob node (tenant-scoped).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MediaMemory {
    /// Lower-case hex sha256 of the blob bytes.
    pub blob_id: String,
    /// The declared MIME type, equal to the sniffed type.
    pub mime: String,
    /// The first derivation id (transcript, caption), if any.
    pub transcript_ref: Option<String>,
    /// The message the blob arrived with.
    pub source_message: String,
}

/// A recall result: the blob, its derivations and the citing memory entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MediaRecall {
    /// Lower-case hex sha256 of the blob bytes.
    pub blob_id: String,
    /// The stored MIME type.
    pub mime: String,
    /// The blob size in bytes.
    pub size: u64,
    /// Derivation node ids, in ascending order.
    pub derivation_ids: Vec<String>,
    /// The memory entry that first cited the blob.
    pub citing_memory: String,
}

/// One modality-specific derivation attached to a remembered blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MediaDerivation {
    /// The derived node id.
    pub derivation_id: String,
    /// The derived text (transcript, caption).
    pub text: String,
}
