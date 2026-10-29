//! Document enrichment contract (MEDULLA-UNIFIED-DATA-PLANE-R651.7).
//!
//! `EnrichDocument` chunks one held Document, embeds the chunks and links
//! every chunk and vector to the Document. The receipt names the fetch
//! backend that produced the page and the embedder that produced the vectors,
//! so a citation can say where its text came from and how it was indexed.

use serde::{Deserialize, Serialize};

/// The closed set of web fetch backends (MEDULLA-UNIFIED-DATA-PLANE-R651.7.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FetchBackend {
    Webarchiver,
    Archivebox,
}

impl FetchBackend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Webarchiver => "webarchiver",
            Self::Archivebox => "archivebox",
        }
    }

    /// Parse a stored backend name; any other value is not a backend.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "webarchiver" => Some(Self::Webarchiver),
            "archivebox" => Some(Self::Archivebox),
            _ => None,
        }
    }
}

/// Request body of `EnrichDocument`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EnrichDocumentRequest {
    /// The held Document node to enrich.
    pub document_id: String,
    /// The Document property holding its text. Absent tries `text`, then
    /// `content`, then `body`.
    #[serde(default)]
    pub text_property: Option<String>,
    /// The backend that fetched the page. Absent reads the Document's
    /// `fetch_backend` (or `backend`) property.
    #[serde(default)]
    pub fetch_backend: Option<FetchBackend>,
    /// Chunk window in UTF-8 bytes (64 to 65 536). Absent is 1 200.
    #[serde(default)]
    pub max_chunk_bytes: Option<u32>,
    /// Overlap between consecutive chunks in bytes, under half the window.
    /// Absent is 150.
    #[serde(default)]
    pub overlap_bytes: Option<u32>,
}

/// Result body of `EnrichDocument`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EnrichDocumentReceipt {
    pub document_id: String,
    /// The enrichment record node, keyed by the Document and its digest.
    pub enrichment_id: String,
    /// `sha256:<hex>` of the enriched text.
    pub content_digest: String,
    #[serde(default)]
    pub fetch_backend: Option<FetchBackend>,
    /// `openai-compatible` or `hash`.
    pub embedder: String,
    #[serde(default)]
    pub embedding_model: Option<String>,
    pub dimensions: u32,
    /// The chunker version tag.
    pub chunker: String,
    pub chunk_count: u32,
    pub vector_count: u32,
    /// Chunk nodes of an earlier digest of this Document that were removed.
    pub superseded_chunks: u32,
    /// True when this digest was already enriched and nothing was written.
    pub replayed: bool,
    /// Chunk node ids in document order.
    pub chunk_ids: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R651.7.1
    #[test]
    fn web_provenance_round_trips() {
        let request = EnrichDocumentRequest {
            document_id: "doc:1".into(),
            text_property: Some("text".into()),
            fetch_backend: Some(FetchBackend::Archivebox),
            max_chunk_bytes: Some(800),
            overlap_bytes: Some(100),
        };
        let bytes = rmp_serde::to_vec_named(&request).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<EnrichDocumentRequest>(&bytes).unwrap(),
            request
        );
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["fetch_backend"], "archivebox");
        let unknown = serde_json::json!({"document_id": "d", "fetch_backend": "curl"});
        assert!(serde_json::from_value::<EnrichDocumentRequest>(unknown).is_err());
        let missing = serde_json::json!({"text_property": "text"});
        assert!(serde_json::from_value::<EnrichDocumentRequest>(missing).is_err());
        for backend in [FetchBackend::Webarchiver, FetchBackend::Archivebox] {
            assert_eq!(FetchBackend::parse(backend.as_str()), Some(backend));
        }
    }
}
