//! The mirror-SINK side (EG-DURABLE-KERNEL-R036): a durable per-mirror
//! cursor and sink-kind declaration for EG's own mirror sinks (fan-out
//! parity + PostgreSQL table mirroring). Distinct from `eg-plan`'s
//! federation `MirrorTargetSpec` (EG-DURABLE-KERNEL-R024.2), which is the
//! SOURCE side: naming an external system a federation query mirrors
//! writes TO. This module owns the sink's own cursor and kind, not the
//! federation source registration.
//!
//! This is the typed model slice (`.1`): the closed sink-kind enum and a
//! cursor whose constructor and advance method both refuse an
//! empty-string position -- the requirement's non-empty-default guard, so
//! a mirror cursor can never silently start or resume replay from the
//! default/empty string. Outage/replay/reconcile behavior, the real
//! PostgreSQL mirroring, and attached-source registry registration are
//! later children.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The kind of mirror sink a durable cursor tracks replay position for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MirrorSinkKind {
    /// Parity with the existing fan-out mirror backend.
    FanOut,
    /// Mirroring SQL user tables to a real PostgreSQL target.
    Postgres,
}

/// A durable per-mirror cursor: the sink kind it tracks replay position
/// for, and that position. The constructor and [`MirrorCursor::advance`]
/// both refuse an empty-string position -- a cursor must never silently
/// start or resume from an empty/default position that could replay from
/// the wrong place.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MirrorCursor {
    kind: MirrorSinkKind,
    position: String,
}

impl MirrorCursor {
    /// Construct a cursor at `position`. Refuses an empty `position`
    /// rather than silently defaulting it.
    pub fn new(kind: MirrorSinkKind, position: String) -> Result<Self, EmptyCursorPosition> {
        if position.is_empty() {
            return Err(EmptyCursorPosition { kind });
        }
        Ok(Self { kind, position })
    }

    /// The sink kind this cursor tracks.
    pub const fn kind(&self) -> MirrorSinkKind {
        self.kind
    }

    /// The cursor's current replay position.
    pub fn position(&self) -> &str {
        &self.position
    }

    /// Advance the cursor to `next_position`. Refuses an empty
    /// `next_position`, leaving the cursor's prior position unchanged --
    /// the same non-empty-default guard the constructor enforces.
    pub fn advance(&mut self, next_position: String) -> Result<(), EmptyCursorPosition> {
        if next_position.is_empty() {
            return Err(EmptyCursorPosition { kind: self.kind });
        }
        self.position = next_position;
        Ok(())
    }
}

/// A mirror cursor was refused an empty-string position, either on
/// construction or on advance. Carries the sink kind it was refused for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmptyCursorPosition {
    pub kind: MirrorSinkKind,
}

impl fmt::Display for EmptyCursorPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} mirror cursor refused an empty-string position",
            self.kind
        )
    }
}

impl std::error::Error for EmptyCursorPosition {}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-DURABLE-KERNEL-R036.1
    #[test]
    fn sink_kinds_round_trip_through_their_wire_name() {
        for (kind, name) in [
            (MirrorSinkKind::FanOut, "fanout"),
            (MirrorSinkKind::Postgres, "postgres"),
        ] {
            let wire = serde_json::to_string(&kind).unwrap();
            assert_eq!(wire, format!("\"{name}\""));
            let back: MirrorSinkKind = serde_json::from_str(&wire).unwrap();
            assert_eq!(back, kind);
        }
    }

    // spec: EG-DURABLE-KERNEL-R036.1
    #[test]
    fn cursor_new_succeeds_with_nonempty_position_for_both_kinds() {
        for kind in [MirrorSinkKind::FanOut, MirrorSinkKind::Postgres] {
            let cursor = MirrorCursor::new(kind, "offset-1".to_string()).unwrap();
            assert_eq!(cursor.kind(), kind);
            assert_eq!(cursor.position(), "offset-1");
        }
    }

    // spec: EG-DURABLE-KERNEL-R036.1
    #[test]
    fn cursor_new_refuses_empty_position_for_both_kinds() {
        for kind in [MirrorSinkKind::FanOut, MirrorSinkKind::Postgres] {
            let err = MirrorCursor::new(kind, String::new()).unwrap_err();
            assert_eq!(err.kind, kind);
        }
    }

    #[test]
    fn advance_succeeds_to_a_new_nonempty_position() {
        let mut cursor = MirrorCursor::new(MirrorSinkKind::FanOut, "offset-1".to_string()).unwrap();
        cursor.advance("offset-2".to_string()).unwrap();
        assert_eq!(cursor.position(), "offset-2");
    }

    #[test]
    fn advance_refuses_empty_next_position() {
        let mut cursor =
            MirrorCursor::new(MirrorSinkKind::Postgres, "offset-1".to_string()).unwrap();
        let err = cursor.advance(String::new()).unwrap_err();
        assert_eq!(err.kind, MirrorSinkKind::Postgres);
    }

    #[test]
    fn refused_advance_leaves_prior_position_unchanged() {
        let mut cursor = MirrorCursor::new(MirrorSinkKind::FanOut, "offset-1".to_string()).unwrap();
        assert!(cursor.advance(String::new()).is_err());
        assert_eq!(cursor.position(), "offset-1");
    }
}
