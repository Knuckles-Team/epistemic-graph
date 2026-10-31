//! Normalized documents and their terminal source-ingestion outcomes
//! (MEDULLA-INGEST-WORKER-R003.7).
//!
//! A normalized document names the source record it was read from inside a
//! validated [`SourceIngestionRequest`]; tenant and policy identity are never
//! part of these records, because the server derives them from verified
//! authority. The validator, quarantine commit and replay are later rows
//! (R003.8 to R003.12).

use serde::{Deserialize, Serialize};

use crate::contract::Digest256;
use crate::source_ingestion::{
    SourceCheckpoint, SourceEntityRef, SourceIngestionReceipt, SourceIngestionRequest,
};

/// Maximum atom or concept references one normalized document may name.
pub const MAX_DOCUMENT_REFS: usize = 4_096;

/// One source record normalized into a document with atom and concept ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct NormalizedDocument {
    pub document_id: String,
    pub source: SourceEntityRef,
    pub source_digest: Digest256,
    pub mapping_revision: String,
    pub atom_ids: Vec<String>,
    #[serde(default)]
    pub concept_ids: Vec<String>,
}

/// Why a normalized document does not fit the request it claims to come from.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NormalizedDocumentShapeError {
    #[error("{0} must not be blank")]
    Blank(&'static str),
    #[error("{0} holds more than {MAX_DOCUMENT_REFS} entries")]
    TooMany(&'static str),
    #[error("normalized document names a record absent from the source request")]
    UnknownSourceRecord,
}

impl NormalizedDocument {
    /// Check the document's own shape and that it names a record of `request`.
    pub fn validate_against(
        &self,
        request: &SourceIngestionRequest,
    ) -> Result<(), NormalizedDocumentShapeError> {
        if self.document_id.trim().is_empty() {
            return Err(NormalizedDocumentShapeError::Blank("document_id"));
        }
        if self.mapping_revision.trim().is_empty() {
            return Err(NormalizedDocumentShapeError::Blank("mapping_revision"));
        }
        for (field, refs) in [
            ("atom_ids", &self.atom_ids),
            ("concept_ids", &self.concept_ids),
        ] {
            if refs.len() > MAX_DOCUMENT_REFS {
                return Err(NormalizedDocumentShapeError::TooMany(field));
            }
            if refs.iter().any(|id| id.trim().is_empty()) {
                return Err(NormalizedDocumentShapeError::Blank(field));
            }
        }
        let known = request.as_batch().records.as_slice().iter().any(|record| {
            record.stream == self.source.stream && record.record_id == self.source.record_id
        });
        if !known {
            return Err(NormalizedDocumentShapeError::UnknownSourceRecord);
        }
        Ok(())
    }
}

/// Evidence that a document was normalized and accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct NormalizationReceipt {
    pub document_id: String,
    pub source_digest: Digest256,
    pub mapping_revision: String,
    pub atom_ids: Vec<String>,
    #[serde(default)]
    pub concept_ids: Vec<String>,
}

/// Why a batch was quarantined instead of accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum QuarantineReason {
    NormalizationFailed,
    MissingProvenance,
    MappingRevisionMismatch,
}

/// Whether a quarantined batch may be replayed after repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ReplayDisposition {
    Replayable,
    Terminal,
}

/// Durable evidence of a quarantined batch. It never claims an accepted
/// checkpoint: the prior accepted checkpoint is optional and is `None` for the
/// first batch of a stream. The evidence reference is restricted; source text
/// is never carried here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct QuarantineReceipt {
    pub reason: QuarantineReason,
    pub rule_id: String,
    pub source_digest: Digest256,
    pub mapping_revision: String,
    pub restricted_evidence_ref: String,
    pub replay: ReplayDisposition,
    pub prior_accepted_checkpoint: Option<SourceCheckpoint>,
    pub committed_graph_version: u64,
}

/// The terminal outcome of one source-ingestion batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", content = "receipt", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SourceIngestionOutcome {
    Accepted(Box<SourceIngestionReceipt>),
    Quarantined(Box<QuarantineReceipt>),
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::contract::{BoundedVec, ResourceId};
    use crate::source_ingestion::{
        SourceIngestionBatch, SourceIngestionDisposition, SourceIngestionMode, SourceJson,
        SourceRecord, SourceRecordProvenance,
    };

    fn digest(byte: u8) -> Digest256 {
        Digest256::from_bytes([byte; 32])
    }

    fn checkpoint(page: u32) -> SourceCheckpoint {
        SourceCheckpoint {
            stream: ResourceId::new("docs").unwrap(),
            position: SourceJson::new(json!({ "page": page })).unwrap(),
            content_hash: None,
            watermark: None,
            pending_watermark: None,
        }
    }

    fn request() -> SourceIngestionRequest {
        SourceIngestionRequest::new(SourceIngestionBatch {
            connector: ResourceId::new("wiki").unwrap(),
            mode: SourceIngestionMode::Delta,
            strict_schema: true,
            records: BoundedVec::new(vec![SourceRecord {
                stream: ResourceId::new("docs").unwrap(),
                record_id: "page-1".into(),
                mapping_reference: "connector:wiki#mappings/page".into(),
                payload: SourceJson::new(json!({"title": "Runbook"})).unwrap(),
                updated_at: None,
                provenance: SourceRecordProvenance {
                    connector: ResourceId::new("wiki").unwrap(),
                    adapter_kind: ResourceId::new("mcp").unwrap(),
                    server: "wiki-server".into(),
                    tool: "list_pages".into(),
                    tool_schema_sha256: digest(1),
                    source_uri: "wiki://docs/page-1".into(),
                },
            }])
            .unwrap(),
            relationships: BoundedVec::new(Vec::new()).unwrap(),
            provider_checkpoint: checkpoint(1),
            expected_previous_checkpoint: None,
            authoritative_live_ids: None,
            empty_authoritative_approval: None,
            withdrawals: BoundedVec::new(Vec::new()).unwrap(),
        })
        .unwrap()
    }

    fn document() -> NormalizedDocument {
        NormalizedDocument {
            document_id: "doc-1".into(),
            source: SourceEntityRef {
                stream: ResourceId::new("docs").unwrap(),
                record_id: "page-1".into(),
            },
            source_digest: digest(2),
            mapping_revision: "rev-3".into(),
            atom_ids: vec!["atom-1".into(), "atom-2".into()],
            concept_ids: vec!["concept:runbook".into()],
        }
    }

    fn accepted() -> SourceIngestionReceipt {
        SourceIngestionReceipt {
            receipt_id: "source-ingest:abc".into(),
            disposition: SourceIngestionDisposition::Committed,
            mode: SourceIngestionMode::Delta,
            batch_digest: digest(3),
            mappings: BoundedVec::new(Vec::new()).unwrap(),
            raw_admissions: BoundedVec::new(Vec::new()).unwrap(),
            relationship_raw_admissions: BoundedVec::new(Vec::new()).unwrap(),
            tombstones: BoundedVec::new(Vec::new()).unwrap(),
            relationship_tombstones: BoundedVec::new(Vec::new()).unwrap(),
            accepted_checkpoint: checkpoint(1),
            accepted_checkpoint_digest: digest(4),
            content_hash: None,
            live_set_digest: None,
            relationship_live_set_digest: None,
            affected_count: 1,
            relationship_count: 0,
            tombstoned_count: 0,
            relationship_tombstoned_count: 0,
            committed_graph_version: 9,
            receipt_digest: digest(5),
        }
    }

    // spec: MEDULLA-INGEST-WORKER-R003.7
    #[test]
    fn document_receipt_round_trips() {
        let request = request();
        let doc = document();
        assert_eq!(doc.validate_against(&request), Ok(()));
        let encoded = serde_json::to_value(&doc).unwrap();
        assert_eq!(
            serde_json::from_value::<NormalizedDocument>(encoded.clone()).unwrap(),
            doc
        );

        // Tenant and policy come from verified authority, never the payload.
        let mut spoofed = encoded;
        spoofed["tenant"] = json!("other-tenant");
        assert!(serde_json::from_value::<NormalizedDocument>(spoofed).is_err());

        let foreign = NormalizedDocument {
            source: SourceEntityRef {
                stream: ResourceId::new("docs").unwrap(),
                record_id: "page-404".into(),
            },
            ..document()
        };
        assert_eq!(
            foreign.validate_against(&request),
            Err(NormalizedDocumentShapeError::UnknownSourceRecord)
        );
        let blank_revision = NormalizedDocument {
            mapping_revision: " ".into(),
            ..document()
        };
        assert_eq!(
            blank_revision.validate_against(&request),
            Err(NormalizedDocumentShapeError::Blank("mapping_revision"))
        );

        let normalization = NormalizationReceipt {
            document_id: doc.document_id.clone(),
            source_digest: doc.source_digest,
            mapping_revision: doc.mapping_revision.clone(),
            atom_ids: doc.atom_ids.clone(),
            concept_ids: doc.concept_ids.clone(),
        };
        let encoded = serde_json::to_value(&normalization).unwrap();
        assert_eq!(
            serde_json::from_value::<NormalizationReceipt>(encoded).unwrap(),
            normalization
        );

        let accepted = SourceIngestionOutcome::Accepted(Box::new(accepted()));
        let first_batch_quarantine =
            SourceIngestionOutcome::Quarantined(Box::new(QuarantineReceipt {
                reason: QuarantineReason::NormalizationFailed,
                rule_id: "complete_document".into(),
                source_digest: digest(6),
                mapping_revision: "rev-3".into(),
                restricted_evidence_ref: "evidence:restricted/7".into(),
                replay: ReplayDisposition::Replayable,
                prior_accepted_checkpoint: None,
                committed_graph_version: 10,
            }));
        for outcome in [&accepted, &first_batch_quarantine] {
            let encoded = serde_json::to_value(outcome).unwrap();
            assert_eq!(
                &serde_json::from_value::<SourceIngestionOutcome>(encoded).unwrap(),
                outcome
            );
        }
        let encoded = serde_json::to_value(&first_batch_quarantine).unwrap();
        assert_eq!(encoded["outcome"], "quarantined");
        assert!(encoded["receipt"]["prior_accepted_checkpoint"].is_null());
        assert!(encoded["receipt"].get("accepted_checkpoint").is_none());
        assert_eq!(
            serde_json::to_value(&accepted).unwrap()["outcome"],
            "accepted"
        );
    }
}
