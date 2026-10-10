//! Typed model for comparing a change-capture replay's resulting row state
//! against a source snapshot (EG-UNIFIED-DATA-PLANE-R022.3): the pure
//! comparison and conformance-entry construction. This is the first slice
//! (`.1`): `compare_replay_to_snapshot` builds a
//! [`crate::dialect_conformance::DialectConformanceEntry`] (EG-UNIFIED-DATA-
//! PLANE-R022.1) from two already-captured row-state maps, never a live
//! replay stream or a live source connection. Running the comparison against
//! a real change-capture replay and a real source snapshot is a later
//! child.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::dialect_conformance::{ConformanceOutcome, DialectConformanceEntry};

/// A replay comparison refused before a conformance entry could be built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidReplayComparison {
    /// The caller gave no engine version to label the entry with.
    BlankEngineVersion,
}

impl std::fmt::Display for InvalidReplayComparison {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlankEngineVersion => {
                write!(f, "replay comparison needs a non-blank engine version")
            }
        }
    }
}

impl std::error::Error for InvalidReplayComparison {}

/// Compare a change-capture replay's resulting row state against a source
/// snapshot taken at the same cursor, and build the matching
/// [`DialectConformanceEntry`]. The two maps are keyed by the same row-id
/// convention the replay bridge uses (see `debezium_bridge::to_draft`'s
/// `node_id`); equal maps are `Green`, any difference is `Deviated` with no
/// deviation record -- a real difference must still go through a human
/// review (`DialectConformanceEntry::validate`) before the entry can ever
/// validate, exactly like a hand-authored deviation. Never guesses at a
/// reviewer. No live replay stream or source connection is read; both row
/// states are supplied already captured.
pub fn compare_replay_to_snapshot(
    adapter: impl Into<String>,
    engine_version: impl Into<String>,
    replayed: &BTreeMap<String, Value>,
    snapshot: &BTreeMap<String, Value>,
) -> Result<DialectConformanceEntry, InvalidReplayComparison> {
    let engine_version = engine_version.into();
    if engine_version.trim().is_empty() {
        return Err(InvalidReplayComparison::BlankEngineVersion);
    }
    let outcome = if replayed == snapshot {
        ConformanceOutcome::Green
    } else {
        ConformanceOutcome::Deviated
    };
    Ok(DialectConformanceEntry {
        adapter: adapter.into(),
        engine_version,
        outcome,
        deviation: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn state(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
        pairs
            .iter()
            .cloned()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    // spec: EG-UNIFIED-DATA-PLANE-R022.3.1
    #[test]
    fn matching_replay_and_snapshot_is_green() {
        let replayed = state(&[(
            "gramps.person:p-2",
            json!({"id": "p-2", "name": "Alan M. Turing"}),
        )]);
        let snapshot = replayed.clone();
        let entry = compare_replay_to_snapshot("postgres_cdc", "16", &replayed, &snapshot).unwrap();
        assert_eq!(entry.outcome, ConformanceOutcome::Green);
        assert_eq!(entry.deviation, None);
        assert_eq!(entry.validate(), Ok(()));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R022.3.1
    #[test]
    fn differing_replay_and_snapshot_is_deviated_and_needs_review() {
        let replayed = state(&[(
            "gramps.person:p-2",
            json!({"id": "p-2", "name": "Alan M. Turing"}),
        )]);
        let snapshot = state(&[(
            "gramps.person:p-2",
            json!({"id": "p-2", "name": "Alan Turing"}),
        )]);
        let entry = compare_replay_to_snapshot("postgres_cdc", "16", &replayed, &snapshot).unwrap();
        assert_eq!(entry.outcome, ConformanceOutcome::Deviated);
        // A deviated entry with no reviewed deviation record is refused --
        // this comparison never auto-suppresses a real difference.
        assert!(entry.validate().is_err());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R022.3.1
    #[test]
    fn blank_engine_version_is_refused() {
        let empty = BTreeMap::new();
        assert_eq!(
            compare_replay_to_snapshot("postgres_cdc", "   ", &empty, &empty),
            Err(InvalidReplayComparison::BlankEngineVersion)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R022.3.1
    #[test]
    fn fixture_snapshot_matches_replayed_state() {
        // A hand-authored source-snapshot fixture, taken independently of
        // the replay, compared against a row state a change-capture replay
        // would have produced for the same cursor.
        let fixture = include_str!("../fixtures/replay_conformance_snapshot.json");
        let snapshot: BTreeMap<String, Value> = serde_json::from_str(fixture).unwrap();
        let replayed = state(&[(
            "gramps.person:p-2",
            json!({"id": "p-2", "name": "Alan M. Turing"}),
        )]);
        let entry = compare_replay_to_snapshot("postgres_cdc", "16", &replayed, &snapshot).unwrap();
        assert_eq!(entry.outcome, ConformanceOutcome::Green);
    }
}
