//! Typed model for a captured Debezium Kafka change-event envelope
//! (EG-UNIFIED-DATA-PLANE-R021): the shape EG parses before converting it
//! into a [`crate::change_envelope::ChangeEnvelope`]. This is the typed-model
//! slice (`.1`): the closed operation-code vocabulary, the envelope shape,
//! and the refusal for an unrecognized operation code.
//!
//! `to_draft` (EG-UNIFIED-DATA-PLANE-R021.2) is the pure-function conversion
//! slice: a captured [`DebeziumChangeEvent`] becomes a
//! [`crate::change_envelope::ChangeEnvelopeDraft`] -- the caller-authored half
//! of a [`crate::change_envelope::ChangeEnvelope`] (operations, content
//! version, cursor, policy proof). It never mints the scope identity, OCC
//! version expectation or admission envelope that only a live request
//! boundary can mint (see `change_envelope::draft`'s module doc), so it needs
//! no Kafka connection and no database: exactly the fixture-testable boundary.
//! Wiring a real Kafka consumer that feeds these drafts to the engine for
//! compilation into full `ChangeEnvelope`s is a later child.
//!
//! `tests::replay` (EG-UNIFIED-DATA-PLANE-R021.3) feeds a captured, multi-op
//! Debezium event stream fixture through `to_draft` and confirms the
//! resulting draft sequence's operations match the source's actual changes.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::change_envelope::{
    ChangeCursor, ChangeEnvelopeDraft, ChangeMutationDraft, ContentVersion, ContentVersionPosition,
    CursorPosition, MaterialOperation, PolicyRecord, PrivacyAttestation, CHANGE_ENVELOPE_VERSION,
};
use crate::mutation_batch::{DurabilityDomain, MutationOperation, MutationSurface};
use crate::protocol::Method;

/// Debezium's `op` field: `c` (create), `u` (update), `d` (delete), `r` (read,
/// the initial snapshot). Closed — an unrecognized code is refused, never guessed at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DebeziumOp {
    #[serde(rename = "c")]
    Create,
    #[serde(rename = "u")]
    Update,
    #[serde(rename = "d")]
    Delete,
    #[serde(rename = "r")]
    Read,
}

impl DebeziumOp {
    /// Parse Debezium's single-letter wire code. Refuses an unrecognized
    /// code rather than defaulting it to `Update`.
    pub fn parse(code: &str) -> Result<Self, UnknownDebeziumOp> {
        match code {
            "c" => Ok(Self::Create),
            "u" => Ok(Self::Update),
            "d" => Ok(Self::Delete),
            "r" => Ok(Self::Read),
            other => Err(UnknownDebeziumOp(other.to_string())),
        }
    }
}

/// An unrecognized Debezium `op` code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownDebeziumOp(pub String);

impl std::fmt::Display for UnknownDebeziumOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unrecognized Debezium op code: {:?}", self.0)
    }
}

impl std::error::Error for UnknownDebeziumOp {}

/// The `source` block Debezium attaches to every change event: enough to
/// place the event in its source's own change order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebeziumSource {
    pub db: String,
    pub table: String,
    /// Source-native ordering position (Postgres LSN, MySQL binlog
    /// position, …) as the source emits it, opaque to EG.
    pub position: String,
    pub ts_ms: i64,
}

/// One captured Debezium change event, before conversion into a
/// `ChangeEnvelope`. `before`/`after` stay opaque JSON: this slice does not
/// interpret row contents, only the envelope's own shape.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DebeziumChangeEvent {
    pub op: DebeziumOp,
    pub source: DebeziumSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<serde_json::Value>,
}

/// An event failed the shape rules a `ChangeEnvelope` conversion depends on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidDebeziumEvent {
    /// A create/update/read event carried no `after` row.
    MissingAfter,
    /// A delete event carried no `before` row.
    MissingBefore,
}

impl std::fmt::Display for InvalidDebeziumEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingAfter => write!(f, "event requires an `after` row for its op"),
            Self::MissingBefore => write!(f, "delete event requires a `before` row"),
        }
    }
}

impl std::error::Error for InvalidDebeziumEvent {}

impl DebeziumChangeEvent {
    /// Confirm this event carries the row Debezium's own semantics require
    /// for its operation: `after` for create/update/read, `before` for
    /// delete. Refuses rather than converting a half-populated event.
    pub fn validate(&self) -> Result<(), InvalidDebeziumEvent> {
        match self.op {
            DebeziumOp::Delete if self.before.is_none() => Err(InvalidDebeziumEvent::MissingBefore),
            DebeziumOp::Create | DebeziumOp::Update | DebeziumOp::Read if self.after.is_none() => {
                Err(InvalidDebeziumEvent::MissingAfter)
            }
            _ => Ok(()),
        }
    }

    /// This event's source position as an engine `ChangeCursor`, opaque
    /// (never ordered lexically, since Debezium positions are source-format
    /// specific — an LSN, a binlog coordinate, a resume token).
    pub fn to_change_cursor(&self) -> ChangeCursor {
        ChangeCursor {
            source: format!("{}.{}", self.source.db, self.source.table),
            partition: String::new(),
            position: CursorPosition::Opaque {
                cursor_type: "debezium_source_position".to_string(),
                value: self.source.position.clone(),
            },
            expected_previous: None,
        }
    }

    /// The graph node this event's row addresses: `after`'s or `before`'s
    /// `id` field when present (the common Debezium row-key convention),
    /// otherwise a content digest of the row so the key stays deterministic
    /// across replays of the same captured fixture.
    fn node_id(&self) -> String {
        let row = self.after.as_ref().or(self.before.as_ref());
        let key = match row.and_then(|value| value.get("id")) {
            Some(serde_json::Value::String(id)) => id.clone(),
            Some(other) => other.to_string(),
            None => hex::encode(Sha256::digest(
                row.map(serde_json::Value::to_string)
                    .unwrap_or_default()
                    .as_bytes(),
            )),
        };
        format!("{}.{}:{key}", self.source.db, self.source.table)
    }

    /// Convert this captured event into a [`ChangeEnvelopeDraft`] -- EG-
    /// UNIFIED-DATA-PLANE-R021.2's pure conversion slice. Refuses exactly
    /// when `validate` refuses (an op missing the row its semantics
    /// require); never guesses at a missing row. `envelope_id`/`batch_id`
    /// are caller-supplied so a Kafka consumer (a later child) can derive
    /// them from the topic/partition/offset once that wiring exists.
    pub fn to_draft(
        &self,
        envelope_id: impl Into<String>,
        batch_id: impl Into<String>,
    ) -> Result<ChangeEnvelopeDraft, InvalidDebeziumEvent> {
        self.validate()?;
        let node_id = self.node_id();
        let row = self.after.as_ref().or(self.before.as_ref());
        let method = match self.op {
            DebeziumOp::Delete => Method::RemoveNode {
                node_id: node_id.clone(),
            },
            DebeziumOp::Create | DebeziumOp::Update | DebeziumOp::Read => Method::AddNode {
                node_id: node_id.clone(),
                properties_msgpack: rmp_serde::to_vec_named(row.expect(
                    "validate() already refused a create/update/read event with no `after` row",
                ))
                .unwrap_or_default(),
            },
        };
        let row_digest = hex::encode(Sha256::digest(
            row.map(serde_json::Value::to_string)
                .unwrap_or_default()
                .as_bytes(),
        ));
        Ok(ChangeEnvelopeDraft {
            schema_version: CHANGE_ENVELOPE_VERSION,
            envelope_id: envelope_id.into(),
            mutation: ChangeMutationDraft {
                batch_id: batch_id.into(),
                placement_epoch: 0,
                expected_graph_version: None,
                fencing_token: None,
                operations: vec![MutationOperation {
                    ordinal: 0,
                    surface: MutationSurface::Broker,
                    domain: DurabilityDomain::GraphRows,
                    method,
                }],
                outbox: Vec::new(),
            },
            content_version: ContentVersion {
                object_id: node_id.clone(),
                digest_algorithm: "sha256".to_string(),
                digest: row_digest.clone(),
                previous_digest: None,
                source_version: ContentVersionPosition::Opaque {
                    version_type: "debezium_source_position".to_string(),
                    value: self.source.position.clone(),
                },
            },
            cursor: Some(self.to_change_cursor()),
            lineage: Vec::new(),
            // A policy proof is required for `ChangeEnvelope::validate` to
            // accept any material object (see
            // `test_support::change_envelope::minimal_envelope`); the bridge
            // tags every bridged row with its source table as a placeholder
            // classification until a real policy mapping (a later child)
            // replaces it.
            policies: vec![PolicyRecord {
                policy_id: format!("debezium-bridge:{}.{}", self.source.db, self.source.table),
                operation: match self.op {
                    DebeziumOp::Delete => MaterialOperation::Delete,
                    DebeziumOp::Create | DebeziumOp::Update | DebeziumOp::Read => {
                        MaterialOperation::Upsert
                    }
                },
                object_id: node_id,
                tenant: self.source.db.clone(),
                classification: "internal".to_string(),
                policy_version: "debezium-bridge-v1".to_string(),
                subject_set_digest: "0".repeat(64),
                retention_policy: "default".to_string(),
                legal_hold: false,
            }],
            evidence: Vec::new(),
            features: Vec::new(),
            blobs: Vec::new(),
            privacy: PrivacyAttestation {
                policy_version: "debezium-bridge-v1".to_string(),
                sanitizer_version: "none".to_string(),
                sanitized_payload_digest: row_digest,
            },
            material_class: crate::change_envelope::MaterialClass::Attested,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn source() -> DebeziumSource {
        DebeziumSource {
            db: "gramps".to_string(),
            table: "person".to_string(),
            position: "0/1A2B3C".to_string(),
            ts_ms: 1_000,
        }
    }

    #[test]
    fn known_op_codes_parse() {
        assert_eq!(DebeziumOp::parse("c"), Ok(DebeziumOp::Create));
        assert_eq!(DebeziumOp::parse("u"), Ok(DebeziumOp::Update));
        assert_eq!(DebeziumOp::parse("d"), Ok(DebeziumOp::Delete));
        assert_eq!(DebeziumOp::parse("r"), Ok(DebeziumOp::Read));
    }

    #[test]
    fn unknown_op_code_is_refused() {
        assert_eq!(
            DebeziumOp::parse("t"),
            Err(UnknownDebeziumOp("t".to_string()))
        );
    }

    #[test]
    fn create_without_after_is_refused() {
        let event = DebeziumChangeEvent {
            op: DebeziumOp::Create,
            source: source(),
            before: None,
            after: None,
        };
        assert_eq!(event.validate(), Err(InvalidDebeziumEvent::MissingAfter));
    }

    #[test]
    fn delete_without_before_is_refused() {
        let event = DebeziumChangeEvent {
            op: DebeziumOp::Delete,
            source: source(),
            before: None,
            after: None,
        };
        assert_eq!(event.validate(), Err(InvalidDebeziumEvent::MissingBefore));
    }

    #[test]
    fn well_formed_update_validates() {
        let event = DebeziumChangeEvent {
            op: DebeziumOp::Update,
            source: source(),
            before: Some(json!({"name": "old"})),
            after: Some(json!({"name": "new"})),
        };
        assert_eq!(event.validate(), Ok(()));
    }

    #[test]
    fn cursor_position_is_opaque_and_carries_the_source_position() {
        let event = DebeziumChangeEvent {
            op: DebeziumOp::Read,
            source: source(),
            before: None,
            after: Some(json!({"name": "x"})),
        };
        let cursor = event.to_change_cursor();
        assert_eq!(cursor.source, "gramps.person");
        match cursor.position {
            CursorPosition::Opaque { cursor_type, value } => {
                assert_eq!(cursor_type, "debezium_source_position");
                assert_eq!(value, "0/1A2B3C");
            }
            other => panic!("expected opaque cursor position, got {other:?}"),
        }
    }

    #[test]
    fn event_serializes_round_trip() {
        let event = DebeziumChangeEvent {
            op: DebeziumOp::Create,
            source: source(),
            before: None,
            after: Some(json!({"name": "x"})),
        };
        let encoded = serde_json::to_string(&event).unwrap();
        let decoded: DebeziumChangeEvent = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, event);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R021.2
    #[test]
    fn to_draft_maps_create_update_delete_into_node_operations() {
        let create = DebeziumChangeEvent {
            op: DebeziumOp::Create,
            source: source(),
            before: None,
            after: Some(json!({"id": "p-1", "name": "Ada"})),
        };
        let draft = create.to_draft("envelope-1", "batch-1").unwrap();
        assert_eq!(draft.mutation.operations.len(), 1);
        match &draft.mutation.operations[0].method {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } => {
                assert_eq!(node_id, "gramps.person:p-1");
                let decoded: serde_json::Value = rmp_serde::from_slice(properties_msgpack).unwrap();
                assert_eq!(decoded, json!({"id": "p-1", "name": "Ada"}));
            }
            other => panic!("expected AddNode, got {other:?}"),
        }
        assert_eq!(draft.content_version.object_id, "gramps.person:p-1");
        assert_eq!(
            draft.cursor.as_ref().map(|cursor| cursor.source.clone()),
            Some("gramps.person".to_string())
        );

        let delete = DebeziumChangeEvent {
            op: DebeziumOp::Delete,
            source: source(),
            before: Some(json!({"id": "p-1", "name": "Ada"})),
            after: None,
        };
        let draft = delete.to_draft("envelope-2", "batch-2").unwrap();
        match &draft.mutation.operations[0].method {
            Method::RemoveNode { node_id } => assert_eq!(node_id, "gramps.person:p-1"),
            other => panic!("expected RemoveNode, got {other:?}"),
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R021.2
    #[test]
    fn to_draft_refuses_exactly_when_validate_refuses() {
        let invalid = DebeziumChangeEvent {
            op: DebeziumOp::Create,
            source: source(),
            before: None,
            after: None,
        };
        assert_eq!(
            invalid.to_draft("envelope-3", "batch-3").err(),
            Some(InvalidDebeziumEvent::MissingAfter)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R021.3
    #[test]
    fn replay_of_captured_fixture_stream_matches_source_changes() {
        let fixture = include_str!("../fixtures/debezium_replay_stream.json");
        let events: Vec<DebeziumChangeEvent> = serde_json::from_str(fixture).unwrap();
        assert_eq!(events.len(), 4, "fixture stream carries 4 captured events");

        // Replay every draft's operation into a plain map the way a graph
        // store would apply AddNode/RemoveNode, then compare against the
        // source's actual final state -- this is the replay-equivalence
        // check EG-UNIFIED-DATA-PLANE-R021/R021.3's acceptance criterion asks
        // for, run over a captured fixture instead of a live Kafka topic.
        let mut replayed: std::collections::BTreeMap<String, serde_json::Value> =
            std::collections::BTreeMap::new();
        for (index, event) in events.iter().enumerate() {
            let draft = event
                .to_draft(format!("envelope-{index}"), format!("batch-{index}"))
                .expect("every fixture event is well-formed");
            match &draft.mutation.operations[0].method {
                Method::AddNode {
                    node_id,
                    properties_msgpack,
                } => {
                    let row: serde_json::Value = rmp_serde::from_slice(properties_msgpack).unwrap();
                    replayed.insert(node_id.clone(), row);
                }
                Method::RemoveNode { node_id } => {
                    replayed.remove(node_id);
                }
                other => panic!("debezium bridge only emits AddNode/RemoveNode, got {other:?}"),
            }
        }

        // Source's actual final changes: p-1 (read then deleted) is gone;
        // p-2 (created then updated) holds its latest row.
        let mut expected = std::collections::BTreeMap::new();
        expected.insert(
            "gramps.person:p-2".to_string(),
            json!({"id": "p-2", "name": "Alan M. Turing"}),
        );
        assert_eq!(replayed, expected);
    }
}
