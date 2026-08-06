//! `ViewResult` — what actually got drawn (D-VZ-1 lane V0).
//!
//! A `ViewResult` is the render-plane sibling of the quantum lane's `QuantumResult`
//! (`plans/au-eg-program/PROGRAM.md` "quantum-native" section): both carry the same
//! trust concern under a different name. Quantum's rule is *"only `exact: true`
//! results may be treated as hard constraints; everything else is a proposal."*
//! Viz's rule is the visual equivalent: **a consumer must be able to tell an exact
//! plot from a decimated one**, because a density-approximated scatter and an exact
//! one can be visually indistinguishable at a glance while carrying very different
//! epistemic weight (an aggregated cell's color is a *mean*, not a value that
//! exists at a point — see `open-source-libraries/xy`'s
//! `spec/design/lod-architecture.md` §2 "no visual lying" rules, Invariant L1/L2).
//! [`ViewResult::exact`] is that bit, and it is derived — never set independently
//! of [`ViewResult::lod_tier`]/[`ViewResult::reduction`] — so it cannot drift from
//! the facts that justify it (see [`ViewResult::validate`]).

use serde::{Deserialize, Serialize};

use crate::tier::LodTier;

/// What kind of reduction produced this result, if any. Mirrors `xy`'s recorded
/// `reduction`/`binning` fields (`spec/design/wire-protocol.md` §3): every
/// reduction decision rides the result, never silently.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReductionKind {
    /// Every visible mark is drawn from real rows; nothing was aggregated,
    /// sampled, or dropped.
    None,
    /// Shape-preserving per-pixel-column decimation (M4/LTTB/min-max) — the
    /// reduced form still means what the mark means, but individual rows are not
    /// individually recoverable from it.
    Decimate,
    /// A screen-bounded density/aggregate surface (mean-color grid, count grid).
    Density,
    /// Out-of-core tiled paging — only the tiles covering the current viewport are
    /// resident.
    Tiled,
}

/// What a [`PayloadRef`] actually contains, so a consumer (planner or UI) can
/// dispatch on shape without inspecting bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadKind {
    /// Exact vertex/index geometry (Tier 0/1).
    Geometry,
    /// A density/aggregate grid (count and/or mean-color plane, Tier 2).
    DensityGrid,
    /// One out-of-core tile (Tier 3).
    Tile,
    /// A rendered static image (PNG/SVG/PDF bytes) — V3a's output shape.
    StaticImage,
}

/// A reference to one binary payload a render backend produced — content-addressed
/// or tile-keyed, never embedded inline here (this crate carries metadata, not
/// bytes; V1's ColumnStore and V3a/V3b's backends own the actual storage).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PayloadRef {
    /// Opaque id (content hash, tile key, or export blob id) a backend resolves.
    pub id: String,
    pub kind: PayloadKind,
    pub byte_len: u64,
}

impl PayloadRef {
    pub fn new(id: impl Into<String>, kind: PayloadKind, byte_len: u64) -> Self {
        Self {
            id: id.into(),
            kind,
            byte_len,
        }
    }
}

/// An inconsistent [`ViewResult`] construction — most importantly, `exact` set to a
/// value the tier/reduction facts do not support.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViewResultError {
    /// `exact` was `true` but `lod_tier`/`reduction` show a reduction occurred (or
    /// vice versa) — the exact bit must be a pure function of those two fields.
    ExactFlagInconsistentWithTier {
        lod_tier: LodTier,
        reduction: ReductionKind,
        exact: bool,
    },
    EmptyQueryHash,
}

impl std::fmt::Display for ViewResultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExactFlagInconsistentWithTier {
                lod_tier,
                reduction,
                exact,
            } => write!(
                f,
                "ViewResult.exact={exact} is inconsistent with lod_tier={lod_tier:?} \
                 reduction={reduction:?} (exact must equal `lod_tier == Direct && \
                 reduction == None`)"
            ),
            Self::EmptyQueryHash => write!(f, "ViewResult.query_hash must be non-empty"),
        }
    }
}

impl std::error::Error for ViewResultError {}

/// The result of resolving one [`crate::spec::ViewSpec`] against real data: which
/// tier actually ran, whether the result is exact or approximated, and where the
/// drawable payload(s) live. Carries no pixels itself.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ViewResult {
    /// Content hash identifying "this exact render" (spec + dataset + snapshot
    /// version) — the render-plane analogue of `eg_jobs::compute_result_ref`. See
    /// [`crate::job::query_hash`].
    pub query_hash: String,
    /// Deterministic sampling seed, when [`ReductionKind::Density`]/`Tiled` used
    /// one (`xy`'s deterministic-sampling contract, `lod-architecture.md` §3: a
    /// pure function of `(row_id, zoom_level)`, never wall-clock/RNG-seeded, so a
    /// re-render with the same seed reproduces the same picture).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    /// Rows actually considered in producing this result (post-filter, pre-LOD —
    /// the same count [`crate::tier::select_tier`] was given).
    pub row_count: u64,
    pub lod_tier: LodTier,
    pub reduction: ReductionKind,
    /// **The trust bit.** `true` only when `lod_tier == Direct` and
    /// `reduction == None` — real marks, every row, nothing aggregated or sampled.
    /// A caller must never infer this from `lod_tier` alone; use this field.
    pub exact: bool,
    pub payloads: Vec<PayloadRef>,
    pub wall_time_ms: u64,
    pub produced_at_unix_ms: i64,
}

impl ViewResult {
    /// Construct an exact (Tier 0, no reduction) result.
    pub fn exact(
        query_hash: impl Into<String>,
        row_count: u64,
        payloads: Vec<PayloadRef>,
        wall_time_ms: u64,
        produced_at_unix_ms: i64,
    ) -> Result<Self, ViewResultError> {
        Self::new(
            query_hash,
            None,
            row_count,
            LodTier::Direct,
            ReductionKind::None,
            payloads,
            wall_time_ms,
            produced_at_unix_ms,
        )
    }

    /// Construct a reduced (approximated) result. `lod_tier`/`reduction` must be
    /// consistent with each other (Direct+None is the only exact combination; use
    /// [`Self::exact`] for that case instead) — see [`Self::validate`].
    #[allow(clippy::too_many_arguments)]
    pub fn reduced(
        query_hash: impl Into<String>,
        seed: u64,
        row_count: u64,
        lod_tier: LodTier,
        reduction: ReductionKind,
        payloads: Vec<PayloadRef>,
        wall_time_ms: u64,
        produced_at_unix_ms: i64,
    ) -> Result<Self, ViewResultError> {
        Self::new(
            query_hash,
            Some(seed),
            row_count,
            lod_tier,
            reduction,
            payloads,
            wall_time_ms,
            produced_at_unix_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        query_hash: impl Into<String>,
        seed: Option<u64>,
        row_count: u64,
        lod_tier: LodTier,
        reduction: ReductionKind,
        payloads: Vec<PayloadRef>,
        wall_time_ms: u64,
        produced_at_unix_ms: i64,
    ) -> Result<Self, ViewResultError> {
        let query_hash = query_hash.into();
        let exact = Self::exact_bit_for(lod_tier, reduction);
        let result = Self {
            query_hash,
            seed,
            row_count,
            lod_tier,
            reduction,
            exact,
            payloads,
            wall_time_ms,
            produced_at_unix_ms,
        };
        result.validate()?;
        Ok(result)
    }

    /// The one place `exact` is computed. `true` iff drawn from real, unreduced
    /// marks — see the module doc's quantum-lane parallel.
    pub fn exact_bit_for(lod_tier: LodTier, reduction: ReductionKind) -> bool {
        matches!(lod_tier, LodTier::Direct) && matches!(reduction, ReductionKind::None)
    }

    /// Re-check the trust invariant on a value built or deserialized from
    /// elsewhere (e.g. a wire payload) rather than through [`Self::exact`]/
    /// [`Self::reduced`]. A consumer that trusts `exact` for anything
    /// safety-relevant should call this first — exactly as a quantum consumer
    /// must not treat a `QuantumResult` as authoritative without checking its own
    /// `exact` flag.
    pub fn validate(&self) -> Result<(), ViewResultError> {
        if self.query_hash.trim().is_empty() {
            return Err(ViewResultError::EmptyQueryHash);
        }
        let expected = Self::exact_bit_for(self.lod_tier, self.reduction);
        if self.exact != expected {
            return Err(ViewResultError::ExactFlagInconsistentWithTier {
                lod_tier: self.lod_tier,
                reduction: self.reduction,
                exact: self.exact,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_constructor_is_marked_exact() {
        let result = ViewResult::exact("qh1", 500, Vec::new(), 4, 0).unwrap();
        assert!(result.exact);
        assert_eq!(result.lod_tier, LodTier::Direct);
        assert_eq!(result.reduction, ReductionKind::None);
        assert!(result.seed.is_none());
    }

    #[test]
    fn density_result_is_marked_not_exact() {
        let result = ViewResult::reduced(
            "qh2",
            42,
            100_000_000,
            LodTier::Density,
            ReductionKind::Density,
            vec![PayloadRef::new("tile:0", PayloadKind::DensityGrid, 4096)],
            120,
            0,
        )
        .unwrap();
        assert!(!result.exact);
        assert_eq!(result.seed, Some(42));
    }

    #[test]
    fn a_consumer_can_always_tell_exact_from_decimated_by_the_flag_alone() {
        // The load-bearing property this module exists to guarantee: `exact` is
        // legible WITHOUT the caller separately reasoning about tier/reduction.
        let exact = ViewResult::exact("qh3", 10, Vec::new(), 1, 0).unwrap();
        let decimated = ViewResult::reduced(
            "qh3",
            7,
            10_000_000,
            LodTier::Decimate,
            ReductionKind::Decimate,
            Vec::new(),
            10,
            0,
        )
        .unwrap();
        assert_ne!(exact.exact, decimated.exact);
    }

    #[test]
    fn hand_built_inconsistent_result_fails_validation() {
        // Simulates a value arriving over the wire / from `serde` with a tampered
        // or buggy `exact` bit rather than one this crate computed.
        let mut result = ViewResult::exact("qh4", 10, Vec::new(), 1, 0).unwrap();
        result.exact = true;
        result.reduction = ReductionKind::Density; // now inconsistent
        let err = result.validate().unwrap_err();
        assert!(matches!(
            err,
            ViewResultError::ExactFlagInconsistentWithTier { .. }
        ));
    }

    #[test]
    fn empty_query_hash_is_rejected() {
        let err = ViewResult::exact("", 1, Vec::new(), 1, 0).unwrap_err();
        assert_eq!(err, ViewResultError::EmptyQueryHash);
    }

    #[test]
    fn round_trips_through_json() {
        let result = ViewResult::reduced(
            "qh5",
            1,
            2_000_000_000,
            LodTier::Tiled,
            ReductionKind::Tiled,
            vec![PayloadRef::new("tile:3:4", PayloadKind::Tile, 65536)],
            980,
            1_700_000_000_000,
        )
        .unwrap();
        let json = serde_json::to_string(&result).unwrap();
        let restored: ViewResult = serde_json::from_str(&json).unwrap();
        assert_eq!(result, restored);
    }
}
