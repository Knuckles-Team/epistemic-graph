//! Source-free transcript records for agent-session ingestion.
//!
//! `EG-REPO-INGEST-R800.1` owns the stable record shape.  Parsers for the
//! supported line-delimited dialects belong to the later behavior child and
//! must project into [`TranscriptTurn`] rather than introducing one record
//! type per producer.  Checkpoint identity is deliberately reused from
//! `eg-types`; this module does not create a second cursor contract.

use eg_types::contract::{Digest256, ResourceId, UtcUnixNanos};
pub use eg_types::source_ingestion::SourceCheckpoint;
use serde::{Deserialize, Serialize};

/// The closed set of roles accepted by a transcript parser.
///
/// Keeping this as an enum means an unknown role cannot be persisted as a
/// valid turn.  Dialect-specific parsers can report their stable refusal
/// before constructing a [`TranscriptTurn`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptRole {
    User,
    Assistant,
    System,
    Tool,
}

/// One opaque tool-call reference attached to a transcript turn.
///
/// Tool payloads remain outside the transcript record.  A parser stores the
/// producer's stable call identifier and a digest of the canonical call
/// payload, so the turn remains source-free while preserving provenance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptToolCall {
    pub call_id: String,
    pub payload_digest: Digest256,
}

/// Canonical source-free representation of one agent-session turn.
///
/// `content_digest` identifies canonical content bytes; `ts` is UTC Unix
/// nanoseconds.  The parser owns canonicalization and validation, while this
/// model only records values after that boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptTurn {
    pub session_id: String,
    pub ordinal: u64,
    pub role: TranscriptRole,
    pub content_digest: Digest256,
    pub tool_calls: Vec<TranscriptToolCall>,
    pub ts: UtcUnixNanos,
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-REPO-INGEST-R800.1
    #[test]
    fn turn_round_trips_without_source_payload() {
        let turn = TranscriptTurn {
            session_id: "session-42".into(),
            ordinal: 7,
            role: TranscriptRole::Assistant,
            content_digest: Digest256::sha256(b"answer"),
            tool_calls: vec![TranscriptToolCall {
                call_id: "call-1".into(),
                payload_digest: Digest256::sha256(b"tool-payload"),
            }],
            ts: UtcUnixNanos::new(1_725_000_000_000_000_000),
        };

        let encoded = serde_json::to_vec(&turn).expect("turn must serialize");
        let decoded: TranscriptTurn =
            serde_json::from_slice(&encoded).expect("turn must deserialize");
        assert_eq!(decoded, turn);
        assert!(!encoded.windows(b"answer".len()).any(|w| w == b"answer"));
    }

    // spec: EG-REPO-INGEST-R800.1
    #[test]
    fn checkpoint_is_the_existing_source_ingestion_type() {
        fn accepts_existing_checkpoint(_: &SourceCheckpoint) {}

        // This compile-time use guards against introducing a transcript-local
        // checkpoint with a subtly different cursor or digest contract.
        let checkpoint = SourceCheckpoint {
            stream: ResourceId::new("transcript:session-42").unwrap(),
            position: eg_types::source_ingestion::SourceJson::new(
                serde_json::json!({"ordinal": 7}),
            )
            .unwrap(),
            content_hash: Some(Digest256::sha256(b"transcript")),
            watermark: None,
            pending_watermark: None,
        };
        accepts_existing_checkpoint(&checkpoint);
    }
}
