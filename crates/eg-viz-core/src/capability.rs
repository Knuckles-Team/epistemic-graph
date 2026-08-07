//! The mark × surface capability matrix (D-VZ-1 lane V0).
//!
//! Which [`MarkKind`]s are supported for static export vs. the interactive client
//! vs. server-side rendering, encoded as **data** — not scattered `match` arms in
//! planner code and UI code that can silently drift apart. Mirrors
//! `open-source-libraries/xy`'s generated capability matrix
//! (`spec/api/capability-matrix.md`, sourced from `python/xy/styling/capabilities.py`
//! and pinned by that project's own test suite so it "cannot list a property the
//! implementation does not compile or omit one it does"). This crate's matrix plays
//! the same governance role for the planner (which tier/backend can even be
//! offered for a given mark+surface) and the UI (which controls to show/grey out)
//! — both read the SAME [`CapabilityMatrix`] rather than maintaining their own copy.
//!
//! V0 ships no renderer, so every entry here is [`Status::Planned`] naming the wave
//! that lands it (`plans/au-eg-program/PROGRAM.md` lane table) — never a claim of
//! present capability. A later lane flips an entry to [`Status::Shipped`] in the
//! SAME change that lands the backend, so this file never drifts ahead of reality.

use serde::{Deserialize, Serialize};

use crate::spec::MarkKind;

/// A rendering surface a mark can target.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Surface {
    /// PNG/SVG/PDF export (V3a) — the matplotlib replacement.
    StaticExport,
    /// Binary tile protocol + WebGPU/WebGL2 client (V3b).
    Interactive,
    /// Rendered/reduced server-side (V4 engine integration: tile cache keyed by
    /// `(query_hash, viewport, theme, tier)`).
    ServerSide,
}

impl Surface {
    pub const ALL: [Surface; 3] = [
        Surface::StaticExport,
        Surface::Interactive,
        Surface::ServerSide,
    ];
}

/// How completely a mark is supported on a surface — mirrors `xy`'s
/// full/partial/none vocabulary (`spec/api/capability-matrix.md` "In one line").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportLevel {
    Full,
    Partial,
    None,
}

/// Whether an entry's support level is realized yet. See the module doc: V0 ships
/// no renderer, so nothing is [`Status::Shipped`] today.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Shipped,
    Planned,
}

/// One (mark, surface) cell of the matrix.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityEntry {
    pub mark: MarkKind,
    pub surface: Surface,
    pub level: SupportLevel,
    pub status: Status,
    /// The lane (`plans/au-eg-program/PROGRAM.md` "Lanes" table) that lands this
    /// entry, e.g. `"V3a"`. `None` only for a [`Status::Shipped`] entry.
    ///
    /// Owned (`String`, not `&'static str`): a borrowed-`'static` field makes
    /// `#[derive(Deserialize)]` require the deserializer's own lifetime `'de` to
    /// outlive `'static` for every possible caller, which does not typecheck (a
    /// `Deserialize<'de>` impl must work for an arbitrary, possibly short-lived
    /// `'de`). Owned strings sidestep that entirely and cost nothing here — this
    /// matrix is built once (`default_matrix`) and then read, never hot-looped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_wave: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

/// A queryable table of [`CapabilityEntry`], one row per `(mark, surface)` pair.
/// Exactly `MarkKind::ALL.len() * Surface::ALL.len()` entries — [`Self::default_matrix`]
/// asserts this (a missing combination is exactly as much of a bug as a duplicate
/// one, the `xy` precedent this crate follows).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityMatrix {
    entries: Vec<CapabilityEntry>,
}

impl CapabilityMatrix {
    /// The seed matrix for V0: every mark × surface pair, all `Status::Planned`,
    /// naming the lane that will ship it per the program charter's lane table.
    pub fn default_matrix() -> Self {
        use MarkKind::{Area, Bar, Graph, Heatmap, Line, Scatter};
        use Surface::{Interactive, ServerSide, StaticExport};

        // Fully qualified throughout (never `use SupportLevel::*`): `SupportLevel`
        // shares the name `None` with `Option::None`, and this closure's `notes`
        // parameter genuinely needs `Option::None` — glob-importing the enum would
        // silently shadow the prelude and break every "no notes" call below.
        let mut entries = Vec::new();
        let mut push = |mark, surface, level, target_wave: &str, notes: Option<&str>| {
            entries.push(CapabilityEntry {
                mark,
                surface,
                level,
                status: Status::Planned,
                target_wave: Some(target_wave.to_string()),
                notes: notes.map(str::to_string),
            });
        };

        for mark in [Line, Scatter, Bar, Area, Heatmap] {
            push(mark, StaticExport, SupportLevel::Full, "V3a", None);
            push(mark, ServerSide, SupportLevel::Full, "V4", None);
        }
        // Interactive client lands after the LOD kernels (V2) and the binary tile
        // protocol (V3b); every non-graph mark reaches full interactive support
        // there.
        for mark in [Line, Scatter, Bar, Area, Heatmap] {
            push(mark, Interactive, SupportLevel::Full, "V3b", None);
        }

        // Graph (force-directed node-link) needs the graph-native mark work (V6)
        // before ANY surface can draw it — it has no representation on the
        // general-purpose LOD ladder V3a targets.
        for surface in [StaticExport, Interactive, ServerSide] {
            push(
                Graph,
                surface,
                SupportLevel::None,
                "V6",
                Some("force-directed layout ships with the graph-native marks lane"),
            );
        }

        let matrix = Self { entries };
        debug_assert_eq!(
            matrix.entries.len(),
            MarkKind::ALL.len() * Surface::ALL.len(),
            "capability matrix must cover every mark x surface combination exactly once"
        );
        matrix
    }

    pub fn entries(&self) -> &[CapabilityEntry] {
        &self.entries
    }

    /// The support level for one (mark, surface) pair, or `None` if the matrix has
    /// no entry for it at all (a data-completeness bug, not a "not supported"
    /// answer — use [`Self::default_matrix`], which is exhaustive).
    pub fn level(&self, mark: MarkKind, surface: Surface) -> Option<SupportLevel> {
        self.entries
            .iter()
            .find(|e| e.mark == mark && e.surface == surface)
            .map(|e| e.level)
    }

    pub fn supports(&self, mark: MarkKind, surface: Surface) -> bool {
        matches!(
            self.level(mark, surface),
            Some(SupportLevel::Full | SupportLevel::Partial)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matrix_covers_every_mark_and_surface_exactly_once() {
        let matrix = CapabilityMatrix::default_matrix();
        for mark in MarkKind::ALL {
            for surface in Surface::ALL {
                assert!(
                    matrix.level(mark, surface).is_some(),
                    "missing capability entry for {mark:?}/{surface:?}"
                );
            }
        }
        assert_eq!(
            matrix.entries().len(),
            MarkKind::ALL.len() * Surface::ALL.len()
        );
    }

    #[test]
    fn no_entry_is_shipped_yet_v0_ships_no_renderer() {
        let matrix = CapabilityMatrix::default_matrix();
        assert!(matrix.entries().iter().all(|e| e.status == Status::Planned));
    }

    #[test]
    fn scatter_static_export_is_planned_full_for_v3a() {
        let matrix = CapabilityMatrix::default_matrix();
        let entry = matrix
            .entries()
            .iter()
            .find(|e| e.mark == MarkKind::Scatter && e.surface == Surface::StaticExport)
            .unwrap();
        assert_eq!(entry.level, SupportLevel::Full);
        assert_eq!(entry.target_wave.as_deref(), Some("V3a"));
    }

    #[test]
    fn graph_mark_is_unsupported_on_every_surface_until_v6() {
        let matrix = CapabilityMatrix::default_matrix();
        for surface in Surface::ALL {
            assert!(!matrix.supports(MarkKind::Graph, surface));
            assert_eq!(
                matrix.level(MarkKind::Graph, surface),
                Some(SupportLevel::None)
            );
        }
    }

    #[test]
    fn matrix_is_data_round_trips_through_json_for_planner_and_ui_consumers() {
        let matrix = CapabilityMatrix::default_matrix();
        let json = serde_json::to_string(&matrix).unwrap();
        let restored: CapabilityMatrix = serde_json::from_str(&json).unwrap();
        assert_eq!(matrix, restored);
    }
}
