//! Source-free transcript records for agent-session ingestion.
//!
//! `EG-REPO-INGEST-R800.1` owns the stable record shape and the declarative
//! dialect table.  Later behavior children parse each source using this table
//! and project into [`TranscriptTurn`] rather than introducing one record type
//! per producer.  Checkpoint identity is deliberately reused from `eg-types`;
//! this module does not create a second cursor contract.

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

/// The transcript dialects currently supported by the shared ingestion
/// boundary. These names mirror the existing agent-utilities discovery
/// registry; the Rust parser implementation remains a later behavior slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptDialect {
    Antigravity,
    Windsurf,
    Claude,
    Codex,
}

impl TranscriptDialect {
    /// Every dialect in the declarative table, in stable review order.
    pub const ALL: &'static [Self] =
        &[Self::Antigravity, Self::Windsurf, Self::Claude, Self::Codex];
}

/// The canonical record produced by every transcript dialect mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TranscriptRecordKind {
    Turn,
}

/// Declarative source-field mapping for one transcript dialect.
///
/// Field selectors are intentionally strings rather than parser callbacks:
/// this keeps the table source-free and lets the later line parser enforce
/// bounded input, role refusal and checkpoint restart semantics in one place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TranscriptParserSpec {
    pub dialect: TranscriptDialect,
    pub record_kind: TranscriptRecordKind,
    pub session_id_field: &'static str,
    pub ordinal_field: &'static str,
    pub role_field: &'static str,
    pub content_field: &'static str,
    pub tool_calls_field: &'static str,
    pub timestamp_field: &'static str,
}

/// One declarative table maps every supported dialect to [`TranscriptTurn`].
pub const TRANSCRIPT_PARSER_TABLE: &[TranscriptParserSpec] = &[
    TranscriptParserSpec {
        dialect: TranscriptDialect::Antigravity,
        record_kind: TranscriptRecordKind::Turn,
        session_id_field: "$parent_dir",
        ordinal_field: "step_index",
        role_field: "source",
        content_field: "content",
        tool_calls_field: "tool_calls",
        timestamp_field: "$file_mtime",
    },
    TranscriptParserSpec {
        dialect: TranscriptDialect::Windsurf,
        record_kind: TranscriptRecordKind::Turn,
        session_id_field: "$file_stem",
        ordinal_field: "$array_index",
        role_field: "messages[].role",
        content_field: "messages[].content",
        tool_calls_field: "messages[].tool_calls",
        timestamp_field: "timestamp",
    },
    TranscriptParserSpec {
        dialect: TranscriptDialect::Claude,
        record_kind: TranscriptRecordKind::Turn,
        session_id_field: "$file_stem",
        ordinal_field: "$line_index",
        role_field: "role",
        content_field: "content",
        tool_calls_field: "content[].type=tool_use",
        timestamp_field: "$file_mtime",
    },
    TranscriptParserSpec {
        dialect: TranscriptDialect::Codex,
        record_kind: TranscriptRecordKind::Turn,
        session_id_field: "$file_stem",
        ordinal_field: "$array_index",
        role_field: "messages[].role|history[].role",
        content_field: "messages[].content|history[].content|messages[].text|history[].text",
        tool_calls_field: "messages[].tool_calls|history[].tool_calls",
        timestamp_field: "$file_mtime",
    },
];

/// Look up the one canonical mapping for a supported dialect.
pub fn transcript_parser_spec(dialect: TranscriptDialect) -> Option<&'static TranscriptParserSpec> {
    TRANSCRIPT_PARSER_TABLE
        .iter()
        .find(|spec| spec.dialect == dialect)
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
    fn dialects_map_to_one_turn_record() {
        let expected = [
            (
                TranscriptDialect::Antigravity,
                "$parent_dir",
                "step_index",
                "source",
                "content",
                "tool_calls",
                "$file_mtime",
            ),
            (
                TranscriptDialect::Windsurf,
                "$file_stem",
                "$array_index",
                "messages[].role",
                "messages[].content",
                "messages[].tool_calls",
                "timestamp",
            ),
            (
                TranscriptDialect::Claude,
                "$file_stem",
                "$line_index",
                "role",
                "content",
                "content[].type=tool_use",
                "$file_mtime",
            ),
            (
                TranscriptDialect::Codex,
                "$file_stem",
                "$array_index",
                "messages[].role|history[].role",
                "messages[].content|history[].content|messages[].text|history[].text",
                "messages[].tool_calls|history[].tool_calls",
                "$file_mtime",
            ),
        ];

        assert_eq!(TRANSCRIPT_PARSER_TABLE.len(), expected.len());
        assert_eq!(
            TranscriptDialect::ALL,
            &[
                TranscriptDialect::Antigravity,
                TranscriptDialect::Windsurf,
                TranscriptDialect::Claude,
                TranscriptDialect::Codex,
            ]
        );

        for (spec, expected) in TRANSCRIPT_PARSER_TABLE.iter().zip(expected) {
            let (
                dialect,
                session_id_field,
                ordinal_field,
                role_field,
                content_field,
                tool_calls_field,
                timestamp_field,
            ) = expected;
            assert_eq!(spec.dialect, dialect);
            assert_eq!(spec.record_kind, TranscriptRecordKind::Turn);
            assert_eq!(spec.session_id_field, session_id_field);
            assert_eq!(spec.ordinal_field, ordinal_field);
            assert_eq!(spec.role_field, role_field);
            assert_eq!(spec.content_field, content_field);
            assert_eq!(spec.tool_calls_field, tool_calls_field);
            assert_eq!(spec.timestamp_field, timestamp_field);
            assert_eq!(transcript_parser_spec(dialect), Some(spec));
        }
    }

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
