//! Typed model for a captured Debezium Kafka change-event envelope
//! (EG-UNIFIED-DATA-PLANE-R021): the shape EG parses before converting it
//! into a [`crate::change_envelope::ChangeEnvelope`]. This is the typed-model
//! slice (`.1`): the closed operation-code vocabulary, the envelope shape,
//! and the refusal for an unrecognized operation code. The Kafka consumer,
//! the conversion into a real `ChangeEnvelope`/`ChangeCursor`, and the replay
//! test against a captured event stream are later children.

use serde::{Deserialize, Serialize};

use crate::change_envelope::{ChangeCursor, CursorPosition};

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
}
