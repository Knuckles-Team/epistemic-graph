//! Source transcript threads and messages (MEDULLA-INGEST-WORKER-R003.13).
//!
//! Tenant and policy identity are not fields: the server derives them from
//! verified authority. The connector and stream must match the validated
//! source request. The validator and its refusals are later rows (R003.14 to
//! R003.18).

use serde::{Deserialize, Serialize};

use crate::contract::ResourceId;
use crate::source_ingestion::SourceIngestionRequest;

/// Maximum messages one thread may reference.
pub const MAX_THREAD_MESSAGES: usize = 16_384;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TranscriptRole {
    System,
    User,
    Assistant,
    Tool,
}

/// Where a transcript message was read from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TranscriptProvenance {
    pub source_record_id: String,
    pub source_uri: String,
}

/// One message of a source transcript. Event time is when it happened at the
/// source; storage time is when the source stored it. Both are milliseconds
/// since the Unix epoch and are never conflated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TranscriptMessage {
    pub connector: ResourceId,
    pub stream: ResourceId,
    pub session_id: String,
    pub message_id: String,
    pub role: TranscriptRole,
    pub event_time_ms: i64,
    pub storage_time_ms: i64,
    pub text: String,
    #[serde(default)]
    pub concept_refs: Vec<String>,
    pub provenance: TranscriptProvenance,
}

/// One source session: an ordered list of message references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TranscriptThread {
    pub connector: ResourceId,
    pub stream: ResourceId,
    pub session_id: String,
    /// Message ids in source order.
    pub message_ids: Vec<String>,
}

/// Why a transcript record does not fit its source request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TranscriptShapeError {
    #[error("{0} must not be blank")]
    Blank(&'static str),
    #[error("transcript connector or stream does not match the source request")]
    SourceMismatch,
    #[error("thread references more than {MAX_THREAD_MESSAGES} messages")]
    TooManyMessages,
    #[error("thread references message {0} more than once")]
    RepeatedMessage(String),
}

fn matches_request(
    connector: &ResourceId,
    stream: &ResourceId,
    request: &SourceIngestionRequest,
) -> Result<(), TranscriptShapeError> {
    let batch = request.as_batch();
    if connector != &batch.connector || stream != &batch.provider_checkpoint.stream {
        return Err(TranscriptShapeError::SourceMismatch);
    }
    Ok(())
}

impl TranscriptMessage {
    pub fn validate_against(
        &self,
        request: &SourceIngestionRequest,
    ) -> Result<(), TranscriptShapeError> {
        if self.session_id.trim().is_empty() {
            return Err(TranscriptShapeError::Blank("session_id"));
        }
        if self.message_id.trim().is_empty() {
            return Err(TranscriptShapeError::Blank("message_id"));
        }
        matches_request(&self.connector, &self.stream, request)
    }
}

impl TranscriptThread {
    pub fn validate_against(
        &self,
        request: &SourceIngestionRequest,
    ) -> Result<(), TranscriptShapeError> {
        if self.session_id.trim().is_empty() {
            return Err(TranscriptShapeError::Blank("session_id"));
        }
        if self.message_ids.len() > MAX_THREAD_MESSAGES {
            return Err(TranscriptShapeError::TooManyMessages);
        }
        let mut seen = std::collections::BTreeSet::new();
        for id in &self.message_ids {
            if id.trim().is_empty() {
                return Err(TranscriptShapeError::Blank("message_ids"));
            }
            if !seen.insert(id) {
                return Err(TranscriptShapeError::RepeatedMessage(id.clone()));
            }
        }
        matches_request(&self.connector, &self.stream, request)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::contract::{BoundedVec, Digest256};
    use crate::source_ingestion::{
        SourceCheckpoint, SourceIngestionBatch, SourceIngestionMode, SourceJson, SourceRecord,
        SourceRecordProvenance,
    };

    fn rid(value: &str) -> ResourceId {
        ResourceId::new(value).unwrap()
    }

    fn request() -> SourceIngestionRequest {
        SourceIngestionRequest::new(SourceIngestionBatch {
            connector: rid("chat"),
            mode: SourceIngestionMode::Delta,
            strict_schema: true,
            records: BoundedVec::new(vec![SourceRecord {
                stream: rid("sessions"),
                record_id: "s1/m1".into(),
                mapping_reference: "connector:chat#mappings/message".into(),
                payload: SourceJson::new(json!({"text": "hello"})).unwrap(),
                updated_at: None,
                provenance: SourceRecordProvenance {
                    connector: rid("chat"),
                    adapter_kind: rid("mcp"),
                    server: "chat-server".into(),
                    tool: "list_messages".into(),
                    tool_schema_sha256: Digest256::from_bytes([1; 32]),
                    source_uri: "chat://s1/m1".into(),
                },
            }])
            .unwrap(),
            relationships: BoundedVec::new(Vec::new()).unwrap(),
            provider_checkpoint: SourceCheckpoint {
                stream: rid("sessions"),
                position: SourceJson::new(json!({"after": "m1"})).unwrap(),
                content_hash: None,
                watermark: None,
                pending_watermark: None,
            },
            expected_previous_checkpoint: None,
            authoritative_live_ids: None,
            empty_authoritative_approval: None,
            withdrawals: BoundedVec::new(Vec::new()).unwrap(),
        })
        .unwrap()
    }

    fn message() -> TranscriptMessage {
        TranscriptMessage {
            connector: rid("chat"),
            stream: rid("sessions"),
            session_id: "s1".into(),
            message_id: "m1".into(),
            role: TranscriptRole::Assistant,
            event_time_ms: 1_700_000_000_000,
            storage_time_ms: 1_700_000_005_000,
            text: "hello".into(),
            concept_refs: vec!["concept:greeting".into()],
            provenance: TranscriptProvenance {
                source_record_id: "s1/m1".into(),
                source_uri: "chat://s1/m1".into(),
            },
        }
    }

    // spec: MEDULLA-INGEST-WORKER-R003.13
    #[test]
    fn transcript_round_trips() {
        let request = request();
        let msg = message();
        assert_eq!(msg.validate_against(&request), Ok(()));
        let encoded = serde_json::to_value(&msg).unwrap();
        assert_eq!(encoded["role"], "assistant");
        assert_eq!(
            serde_json::from_value::<TranscriptMessage>(encoded.clone()).unwrap(),
            msg
        );
        for role in ["system", "user", "assistant", "tool"] {
            let mut value = encoded.clone();
            value["role"] = json!(role);
            assert!(serde_json::from_value::<TranscriptMessage>(value).is_ok());
        }
        let mut bad_role = encoded.clone();
        bad_role["role"] = json!("narrator");
        assert!(serde_json::from_value::<TranscriptMessage>(bad_role).is_err());
        let mut spoofed = encoded;
        spoofed["tenant"] = json!("other");
        assert!(serde_json::from_value::<TranscriptMessage>(spoofed).is_err());

        let thread = TranscriptThread {
            connector: rid("chat"),
            stream: rid("sessions"),
            session_id: "s1".into(),
            message_ids: vec!["m1".into(), "m2".into(), "m3".into()],
        };
        assert_eq!(thread.validate_against(&request), Ok(()));
        let encoded = serde_json::to_value(&thread).unwrap();
        let decoded: TranscriptThread = serde_json::from_value(encoded).unwrap();
        assert_eq!(
            decoded.message_ids,
            ["m1", "m2", "m3"],
            "order is preserved"
        );

        let other_stream = TranscriptMessage {
            stream: rid("other"),
            ..message()
        };
        assert_eq!(
            other_stream.validate_against(&request),
            Err(TranscriptShapeError::SourceMismatch)
        );
        let other_connector = TranscriptThread {
            connector: rid("mail"),
            ..thread.clone()
        };
        assert_eq!(
            other_connector.validate_against(&request),
            Err(TranscriptShapeError::SourceMismatch)
        );
        let repeated = TranscriptThread {
            message_ids: vec!["m1".into(), "m1".into()],
            ..thread
        };
        assert_eq!(
            repeated.validate_against(&request),
            Err(TranscriptShapeError::RepeatedMessage("m1".into()))
        );
    }
}
