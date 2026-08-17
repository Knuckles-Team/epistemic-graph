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
//! V0 shipped no renderer, so every entry originally read [`Status::Planned`]
//! naming the wave that lands it (`plans/au-eg-program/PROGRAM.md` lane
//! table) — never a claim of present capability. A later lane flips an entry
//! to [`Status::Shipped`] **in the SAME change that lands the backend**, so
//! this file never drifts ahead of reality; see [`CapabilityMatrix::default_matrix`]'s
//! own doc for the current shipped/planned split (V3a static export and V4
//! server-side integration are shipped for every non-graph mark; V3b
//! interactive ships for the point-pair marks its tile protocol serves;
//! `Graph` has V6-lite static/server-side support but not interactive V6 yet).

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
    /// The matrix: every mark × surface pair, one entry each, `status` and
    /// `target_wave` kept honest against what has actually landed — a later
    /// lane flips `Status::Planned` to `Status::Shipped` in the SAME change
    /// that lands the backend (per this module's own doc), which is exactly
    /// what V2 (LOD kernels)/V3b (interactive)/V4 (engine integration, incl.
    /// `Graph`'s existing static-export/server-side support — V6-lite) did
    /// here.
    pub fn default_matrix() -> Self {
        use MarkKind::{Area, Bar, Graph, Heatmap, Line, Scatter};
        use Surface::{Interactive, ServerSide, StaticExport};

        // Fully qualified throughout (never `use SupportLevel::*`): `SupportLevel`
        // shares the name `None` with `Option::None`, and `push_shipped`/
        // `push_planned`'s `notes` parameter genuinely needs `Option::None` —
        // glob-importing the enum would silently shadow the prelude and break
        // every "no notes" call below. Plain functions (not closures) taking
        // `entries` explicitly: two closures both capturing `entries` mutably
        // cannot coexist (each borrows it for its own lifetime), and a single
        // closure juggling a `shipped: bool` flag reads worse at every call
        // site than two clearly-named functions.
        fn push_shipped(
            entries: &mut Vec<CapabilityEntry>,
            mark: MarkKind,
            surface: Surface,
            level: SupportLevel,
            notes: Option<&str>,
        ) {
            entries.push(CapabilityEntry {
                mark,
                surface,
                level,
                status: Status::Shipped,
                target_wave: None,
                notes: notes.map(str::to_string),
            });
        }
        fn push_planned(
            entries: &mut Vec<CapabilityEntry>,
            mark: MarkKind,
            surface: Surface,
            level: SupportLevel,
            target_wave: &str,
            notes: Option<&str>,
        ) {
            entries.push(CapabilityEntry {
                mark,
                surface,
                level,
                status: Status::Planned,
                target_wave: Some(target_wave.to_string()),
                notes: notes.map(str::to_string),
            });
        }

        let mut entries = Vec::new();

        // Static export (V3a, `eg-viz-export`) and server-side engine
        // integration (V4, `server::viz_engine` — the persistent ColumnStore +
        // content-addressed render cache + durable provenance, mark-agnostic)
        // are both shipped for every non-graph mark.
        for mark in [Line, Scatter, Bar, Area, Heatmap] {
            push_shipped(&mut entries, mark, StaticExport, SupportLevel::Full, None);
            push_shipped(&mut entries, mark, ServerSide, SupportLevel::Full, None);
        }

        // Interactive (V3b, `server::viz_interactive`) ships for the
        // point-pair marks the binary tile protocol's `eg_viz_kernels::lttb_reduce`
        // path actually serves (Line/Scatter/Area). Bar (a discrete-category
        // rect mark) and Heatmap (a z-matrix mark) have no representation on
        // that protocol yet — a documented gap, not a silent claim of support.
        for mark in [Line, Scatter, Area] {
            push_shipped(
                &mut entries,
                mark,
                Interactive,
                SupportLevel::Full,
                Some("binary viewport-tile protocol, eg_viz_kernels::lttb_reduce"),
            );
        }
        for mark in [Bar, Heatmap] {
            push_planned(
                &mut entries,
                mark,
                Interactive,
                SupportLevel::None,
                "V3b+",
                Some("the tile protocol serves point-pair (x,y) marks only today"),
            );
        }

        // Graph (force-directed node-link) has static-export + server-side
        // support today (V6-lite, `eg_viz_export::graph_layout` +
        // `resolve_graph`) but no interactive pan/zoom/picking yet (full V6).
        push_shipped(
            &mut entries,
            Graph,
            StaticExport,
            SupportLevel::Full,
            Some("V6-lite: force-directed layout, static PNG/SVG/PDF export only"),
        );
        push_shipped(
            &mut entries,
            Graph,
            ServerSide,
            SupportLevel::Full,
            Some("V6-lite: persistent ColumnStore + render cache, static export only"),
        );
        push_planned(
            &mut entries,
            Graph,
            Interactive,
            SupportLevel::None,
            "V6",
            Some("WebGPU pan/zoom/picking over a force-directed layout is full V6, not V3b"),
        );

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
    fn a_shipped_entry_never_carries_a_target_wave_and_a_planned_entry_always_does() {
        // The matrix's own governance invariant: `target_wave` names WHEN an
        // unshipped entry lands; a shipped entry has already landed, so it
        // must not claim a future wave (that would be a stale/contradictory
        // claim the matrix exists specifically to prevent).
        let matrix = CapabilityMatrix::default_matrix();
        for entry in matrix.entries() {
            match entry.status {
                Status::Shipped => assert!(
                    entry.target_wave.is_none(),
                    "{:?}/{:?} is Shipped but still carries target_wave={:?}",
                    entry.mark,
                    entry.surface,
                    entry.target_wave
                ),
                Status::Planned => assert!(
                    entry.target_wave.is_some(),
                    "{:?}/{:?} is Planned but has no target_wave",
                    entry.mark,
                    entry.surface
                ),
            }
        }
    }

    #[test]
    fn scatter_static_export_is_shipped_full_v3a() {
        let matrix = CapabilityMatrix::default_matrix();
        let entry = matrix
            .entries()
            .iter()
            .find(|e| e.mark == MarkKind::Scatter && e.surface == Surface::StaticExport)
            .unwrap();
        assert_eq!(entry.level, SupportLevel::Full);
        assert_eq!(entry.status, Status::Shipped);
    }

    #[test]
    fn point_pair_marks_are_shipped_full_on_the_interactive_surface_v3b() {
        let matrix = CapabilityMatrix::default_matrix();
        for mark in [MarkKind::Line, MarkKind::Scatter, MarkKind::Area] {
            let entry = matrix
                .entries()
                .iter()
                .find(|e| e.mark == mark && e.surface == Surface::Interactive)
                .unwrap();
            assert_eq!(entry.level, SupportLevel::Full, "{mark:?}");
            assert_eq!(entry.status, Status::Shipped, "{mark:?}");
        }
    }

    #[test]
    fn bar_and_heatmap_remain_planned_on_the_interactive_surface() {
        let matrix = CapabilityMatrix::default_matrix();
        for mark in [MarkKind::Bar, MarkKind::Heatmap] {
            assert!(!matrix.supports(mark, Surface::Interactive), "{mark:?}");
        }
    }

    #[test]
    fn graph_mark_has_static_and_server_side_support_but_not_interactive_yet() {
        let matrix = CapabilityMatrix::default_matrix();
        assert!(matrix.supports(MarkKind::Graph, Surface::StaticExport));
        assert!(matrix.supports(MarkKind::Graph, Surface::ServerSide));
        assert!(!matrix.supports(MarkKind::Graph, Surface::Interactive));
        assert_eq!(
            matrix.level(MarkKind::Graph, Surface::Interactive),
            Some(SupportLevel::None)
        );
    }

    #[test]
    fn matrix_is_data_round_trips_through_json_for_planner_and_ui_consumers() {
        let matrix = CapabilityMatrix::default_matrix();
        let json = serde_json::to_string(&matrix).unwrap();
        let restored: CapabilityMatrix = serde_json::from_str(&json).unwrap();
        assert_eq!(matrix, restored);
    }
}
