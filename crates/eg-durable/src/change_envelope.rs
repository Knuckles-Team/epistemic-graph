//! Typed `ChangeEnvelope` model for the unified-data-plane CDC framework
//! (`EG-UNIFIED-DATA-PLANE-R008.1`, child of `EG-UNIFIED-DATA-PLANE-R008`).
//!
//! This module ships ONLY the typed shape an adapter would emit into the future
//! `CdcHub` plus the refusal rule that proves it can carry the parent
//! requirement's "durable position per subscription" and "handles keyless
//! tables explicitly" language. It deliberately does **not** implement
//! `CdcHub` itself, durable position storage, replay, lag metrics, the WAL
//! retention cap, or catalog-drift detection — those stay with the parent row
//! `EG-UNIFIED-DATA-PLANE-R008` (BLOCKED on `EG-UNIFIED-DATA-PLANE-R002`,
//! itself still `BUILDING`) and its other children (`R009`/`R010`/`R021`).
//! `before`/`after` row payloads are likewise out of scope for this slice; a
//! `BTreeMap<String, String>` key is enough to prove the shape without
//! committing to a payload encoding this early.
//!
//! Kept simple and additive on purpose: `R009`/`R010`/`R021` are each
//! specified to reuse this type, so it carries no fields or variants beyond
//! what this slice's own tests exercise.
//!
//! Note on naming: `eg-types` separately defines its own, unrelated
//! `ChangeEnvelope` (the engine-native `MutationBatch`-carrying commit-kernel
//! envelope, `crates/eg-types/src/change_envelope.rs`). The two share a name
//! by coincidence of domain language, not by design — they live in different
//! crates, model different things (an external CDC record here vs. an
//! internal mutation-commit unit there), and neither depends on the other.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

/// The kind of change a [`ChangeEnvelope`] carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

/// One change-capture record an adapter would emit into `CdcHub`.
///
/// `source_position` is the durable cursor `CdcHub` would persist per
/// subscription so replay after a restart can resume exactly where capture
/// left off. `key` is the row's primary/unique key, required for `Update`
/// and `Delete` on any table that isn't explicitly declared keyless
/// (`table_is_keyless`) — see [`ChangeEnvelope::validate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeEnvelope {
    pub source_position: String,
    pub table: String,
    pub op: ChangeOp,
    pub key: Option<BTreeMap<String, String>>,
    pub table_is_keyless: bool,
}

/// Why a [`ChangeEnvelope`] was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidChangeEnvelope {
    /// `source_position` was empty — a change with no durable position can
    /// never be replayed, which breaks the parent requirement's "durable
    /// position per subscription" guarantee at its source.
    MissingSourcePosition,
    /// An `Update`/`Delete` carried no `key` on a table not explicitly
    /// declared keyless. Only a table marked `table_is_keyless: true` may
    /// omit the key — this is the "handles keyless tables explicitly" rule,
    /// not a silent default.
    MissingKeyForKeyedTable { table: String, op: ChangeOp },
}

impl fmt::Display for InvalidChangeEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvalidChangeEnvelope::MissingSourcePosition => {
                write!(
                    f,
                    "ChangeEnvelope refused: source_position is empty, so this change cannot be durably replayed"
                )
            }
            InvalidChangeEnvelope::MissingKeyForKeyedTable { table, op } => {
                write!(
                    f,
                    "ChangeEnvelope refused: table '{table}' is not declared keyless but op {op:?} carried no key"
                )
            }
        }
    }
}

impl Error for InvalidChangeEnvelope {}

impl ChangeEnvelope {
    /// Refuses a [`ChangeEnvelope`] that cannot be durably replayed or that
    /// silently drops a keyed table's key. See [`InvalidChangeEnvelope`] for
    /// the two refusal cases.
    pub fn validate(&self) -> Result<(), InvalidChangeEnvelope> {
        if self.source_position.is_empty() {
            return Err(InvalidChangeEnvelope::MissingSourcePosition);
        }
        let key_required = matches!(self.op, ChangeOp::Update | ChangeOp::Delete);
        if key_required && self.key.is_none() && !self.table_is_keyless {
            return Err(InvalidChangeEnvelope::MissingKeyForKeyedTable {
                table: self.table.clone(),
                op: self.op,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-UNIFIED-DATA-PLANE-R008.1
    #[test]
    fn insert_with_no_key_on_a_keyed_table_validates() {
        let envelope = ChangeEnvelope {
            source_position: "lsn-0001".to_string(),
            table: "orders".to_string(),
            op: ChangeOp::Insert,
            key: None,
            table_is_keyless: false,
        };
        assert_eq!(envelope.validate(), Ok(()));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R008.1
    #[test]
    fn update_with_a_key_on_a_keyed_table_validates() {
        let mut key = BTreeMap::new();
        key.insert("id".to_string(), "42".to_string());
        let envelope = ChangeEnvelope {
            source_position: "lsn-0002".to_string(),
            table: "orders".to_string(),
            op: ChangeOp::Update,
            key: Some(key),
            table_is_keyless: false,
        };
        assert_eq!(envelope.validate(), Ok(()));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R008.1
    #[test]
    fn empty_source_position_is_refused() {
        let envelope = ChangeEnvelope {
            source_position: String::new(),
            table: "orders".to_string(),
            op: ChangeOp::Insert,
            key: None,
            table_is_keyless: false,
        };
        assert_eq!(
            envelope.validate(),
            Err(InvalidChangeEnvelope::MissingSourcePosition)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R008.1
    #[test]
    fn update_with_no_key_on_a_keyed_table_is_refused() {
        let envelope = ChangeEnvelope {
            source_position: "lsn-0003".to_string(),
            table: "orders".to_string(),
            op: ChangeOp::Update,
            key: None,
            table_is_keyless: false,
        };
        assert_eq!(
            envelope.validate(),
            Err(InvalidChangeEnvelope::MissingKeyForKeyedTable {
                table: "orders".to_string(),
                op: ChangeOp::Update,
            })
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R008.1
    #[test]
    fn update_with_no_key_on_an_explicitly_keyless_table_validates() {
        let envelope = ChangeEnvelope {
            source_position: "lsn-0004".to_string(),
            table: "append_only_events".to_string(),
            op: ChangeOp::Update,
            key: None,
            table_is_keyless: true,
        };
        assert_eq!(envelope.validate(), Ok(()));
    }
}
