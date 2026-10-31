//! Tier-selection rules (D-VZ-1 lane V0) — a pure, testable function.
//!
//! ★ The entire point of this module: budgets are expressed in **primitives per
//! frame and bytes**, never in "rows in the table". A row count alone says nothing
//! about render cost — a scatter row is one point, a bar row is a rect (four
//! corners), a heatmap "row" is already an aggregated cell. [`select_tier`] always
//! converts `row_count` to `(primitives, bytes)` via the mark's own per-row cost
//! ([`primitives_per_row`], [`bytes_per_row`]) before comparing against a
//! [`FrameBudget`] — see the module tests, which prove two marks at the *same*
//! `row_count` can land on different tiers.
//!
//! The four-tier ladder mirrors `open-source-libraries/xy`'s LOD architecture
//! (`spec/design/lod-architecture.md` §1): Tier 0 direct (exact), Tier 1
//! shape-preserving decimation (M4/LTTB/min-max — still means what the mark
//! means), Tier 2 density/aggregate surface (screen-bounded by construction), Tier
//! 3 out-of-core tiled paging. This module only decides WHICH tier applies; V2
//! implements the actual kernels.

use serde::{Deserialize, Serialize};

use crate::spec::{Encodings, MarkKind};

/// A frame's render-cost allowance. Both fields are mandatory — a caller derives
/// them from viewport pixel dimensions and device capability (a later lane's
/// concern), never from a row count.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameBudget {
    /// Maximum GPU-drawable primitives (vertices, instanced points, rects, …) this
    /// frame may cost.
    pub max_primitives: u64,
    /// Maximum payload bytes this frame's geometry/grid may occupy.
    pub max_bytes: u64,
}

impl FrameBudget {
    pub fn new(max_primitives: u64, max_bytes: u64) -> Self {
        Self {
            max_primitives,
            max_bytes,
        }
    }
}

/// The four-tier LOD ladder. Ordinal order matters (`Direct` < `Decimate` <
/// `Density` < `Tiled`) — a strictly cheaper/more-aggregated representation has a
/// strictly higher discriminant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LodTier {
    Direct = 0,
    Decimate = 1,
    Density = 2,
    Tiled = 3,
}

/// GPU-drawable primitives one row of this mark costs at Tier 0. These are
/// engine-tunable estimates (a placeholder for V2's real kernel-measured costs),
/// not exact geometry counts, and are documented rather than magic:
///
/// - `Scatter`/`Heatmap`: 1 — one instanced point, or one already-aggregated cell.
/// - `Line`/`Area`: 2 — one vertex plus its join/fill quad contribution.
/// - `Bar`: 4 — one rect's four corners.
/// - `Graph`: 3 — one node plus its average ~2 incident edges.
pub fn primitives_per_row(mark: MarkKind) -> u64 {
    match mark {
        MarkKind::Scatter | MarkKind::Heatmap => 1,
        MarkKind::Line | MarkKind::Area => 2,
        MarkKind::Bar => 4,
        MarkKind::Graph => 3,
    }
}

/// Encoded bytes one row costs: an offset-encoded `f32` x/y pair (8 bytes, the
/// `xy` dossier §16/§29 wire discipline this crate follows in spirit — no JSON
/// numbers, f32 geometry) plus 4 bytes per continuous channel present (`color`,
/// `size`) and 1 byte for a `shape` code.
pub fn bytes_per_row(encodings: &Encodings) -> u64 {
    let mut bytes = 8u64; // x, y as f32
    if encodings.color.is_some() {
        bytes += 4;
    }
    if encodings.size.is_some() {
        bytes += 4;
    }
    if encodings.shape.is_some() {
        bytes += 1;
    }
    bytes
}

/// Whether `mark` has a meaningful representation at `tier`, independent of
/// budget. Mirrors `xy`'s per-kind ladder (`lod-architecture.md` §1 table):
///
/// - `Line`/`Area`/`Bar` support Tier 1 (M4 / re-binning); they have no Tier 2 —
///   their Tier-1 form is already screen-bounded, so there is nothing a density
///   surface would add.
/// - `Scatter`/`Graph` have no shape-preserving Tier 1 (`xy`: "unordered
///   decimation lies" — dropping points silently claims a different dataset); they
///   go straight to a Tier 2 density/aggregate surface.
/// - `Heatmap` is "born aggregated" (`xy` §1): a row here is already a cell, so
///   Tier 1 does not apply; its Tier 2 is a mip-style tile reduction of the
///   already-binned grid.
pub fn mark_supports_tier(mark: MarkKind, tier: LodTier) -> bool {
    match tier {
        LodTier::Direct | LodTier::Tiled => true, // universal fallback/floor tiers
        LodTier::Decimate => matches!(mark, MarkKind::Line | MarkKind::Area | MarkKind::Bar),
        LodTier::Density => matches!(
            mark,
            MarkKind::Scatter | MarkKind::Graph | MarkKind::Heatmap
        ),
    }
}

/// Everything [`select_tier`] needs to decide, and nothing it can infer on its
/// own — in particular, whether the source is out-of-core is a fact about the
/// data's residency, not something derivable from `row_count` or `budget`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TierInput {
    pub mark: MarkKind,
    pub row_count: u64,
    pub encodings: Encodings,
    pub budget: FrameBudget,
    /// `true` when `row_count` describes data that cannot be scanned resident in
    /// memory in one pass (V1/V7's out-of-core regime) — forces Tier 3 once Tier 0
    /// is unaffordable, since Tier 1/2 kernels (V2) assume a resident scan.
    #[serde(default)]
    pub out_of_core: bool,
}

/// The decision [`select_tier`] returns: which tier, and the primitives/bytes
/// figures that justified it (never the raw `row_count` alone — the whole point of
/// this module).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TierDecision {
    pub tier: LodTier,
    pub estimated_primitives: u64,
    pub estimated_bytes: u64,
    pub reason: &'static str,
}

/// Deterministic, pure tier selection. First match wins, cheapest tier first —
/// same discipline as the quantum lane's planner rules (`PROGRAM.md`
/// "quantum-native" §Planner rules: "deterministic, first match wins").
pub fn select_tier(input: &TierInput) -> TierDecision {
    let primitives = input
        .row_count
        .saturating_mul(primitives_per_row(input.mark));
    let bytes = input
        .row_count
        .saturating_mul(bytes_per_row(&input.encodings));

    // Tier 0: direct. The ONLY tier `ViewResult::exact` can be true for — real
    // marks, every row — and it applies whenever the row-derived primitive/byte
    // cost already fits the budget, regardless of the absolute row count.
    if primitives <= input.budget.max_primitives && bytes <= input.budget.max_bytes {
        return TierDecision {
            tier: LodTier::Direct,
            estimated_primitives: primitives,
            estimated_bytes: bytes,
            reason: "row-derived primitives and bytes fit the frame budget",
        };
    }

    // Data that cannot be scanned resident in memory always needs Tier 3 once it
    // has overflowed Tier 0: Tier 1/2 kernels (V2) assume a resident scan, which
    // an out-of-core source cannot offer without first paging through tiles.
    if input.out_of_core {
        return TierDecision {
            tier: LodTier::Tiled,
            estimated_primitives: primitives,
            estimated_bytes: bytes,
            reason: "source exceeds the frame budget and is out-of-core; requires tiled residency",
        };
    }

    if mark_supports_tier(input.mark, LodTier::Decimate) {
        return TierDecision {
            tier: LodTier::Decimate,
            estimated_primitives: primitives,
            estimated_bytes: bytes,
            reason: "mark supports shape-preserving per-pixel-column decimation",
        };
    }

    if mark_supports_tier(input.mark, LodTier::Density) {
        return TierDecision {
            tier: LodTier::Density,
            estimated_primitives: primitives,
            estimated_bytes: bytes,
            reason: "mark supports a screen-bounded density/aggregate surface",
        };
    }

    // No cheaper in-core representation exists for this mark; fall back to tiled
    // paging even for resident data rather than violate the budget.
    TierDecision {
        tier: LodTier::Tiled,
        estimated_primitives: primitives,
        estimated_bytes: bytes,
        reason: "no bounded in-core representation available for this mark",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::EncodingSpec;

    fn xy_only_encodings() -> Encodings {
        Encodings {
            x: Some(EncodingSpec::field("x")),
            y: Some(EncodingSpec::field("y")),
            color: None,
            size: None,
            shape: None,
        }
    }

    fn small_budget() -> FrameBudget {
        // ~2M primitives, ~16MB — a generous single-frame interactive budget.
        FrameBudget::new(2_000_000, 16 * 1024 * 1024)
    }

    #[test]
    fn a_500_row_scatter_spec_selects_direct() {
        let decision = select_tier(&TierInput {
            mark: MarkKind::Scatter,
            row_count: 500,
            encodings: xy_only_encodings(),
            budget: small_budget(),
            out_of_core: false,
        });
        assert_eq!(decision.tier, LodTier::Direct);
        assert_eq!(decision.estimated_primitives, 500);
    }

    #[test]
    fn a_100_million_row_scatter_spec_selects_a_bounded_tier() {
        let decision = select_tier(&TierInput {
            mark: MarkKind::Scatter,
            row_count: 100_000_000,
            encodings: xy_only_encodings(),
            budget: small_budget(),
            out_of_core: false,
        });
        assert_ne!(decision.tier, LodTier::Direct);
        // Scatter has no Tier-1 shape-preserving decimation (xy: "unordered
        // decimation lies") — it must land on the screen-bounded density tier.
        assert_eq!(decision.tier, LodTier::Density);
    }

    #[test]
    fn a_100_million_row_line_spec_selects_decimate_not_density() {
        let decision = select_tier(&TierInput {
            mark: MarkKind::Line,
            row_count: 100_000_000,
            encodings: xy_only_encodings(),
            budget: small_budget(),
            out_of_core: false,
        });
        assert_eq!(decision.tier, LodTier::Decimate);
    }

    #[test]
    fn out_of_core_overflow_selects_tiled_even_for_a_decimatable_mark() {
        let decision = select_tier(&TierInput {
            mark: MarkKind::Line,
            row_count: 5_000_000_000,
            encodings: xy_only_encodings(),
            budget: small_budget(),
            out_of_core: true,
        });
        assert_eq!(decision.tier, LodTier::Tiled);
    }

    #[test]
    fn out_of_core_data_that_still_fits_the_budget_stays_direct() {
        // Residency alone must never override a budget-fitting result — the
        // budget, not the out_of_core flag, is the primary signal.
        let decision = select_tier(&TierInput {
            mark: MarkKind::Scatter,
            row_count: 10,
            encodings: xy_only_encodings(),
            budget: small_budget(),
            out_of_core: true,
        });
        assert_eq!(decision.tier, LodTier::Direct);
    }

    #[test]
    fn budgets_are_expressed_in_primitives_and_bytes_not_raw_row_count() {
        // The load-bearing property this module exists to prove: at the SAME
        // row_count, a mark with a higher per-row primitive/byte cost can land on
        // a DIFFERENT (more aggressive) tier than a cheaper mark. If the decision
        // were keyed on row_count alone, this would be impossible.
        let row_count = 600_000; // > 500 but comfortably inside a 2M primitive budget for cheap marks
        let budget = FrameBudget::new(2_000_000, 16 * 1024 * 1024);

        let scatter = select_tier(&TierInput {
            mark: MarkKind::Scatter, // 1 primitive/row -> 600_000 primitives, fits
            row_count,
            encodings: xy_only_encodings(),
            budget,
            out_of_core: false,
        });
        let bar = select_tier(&TierInput {
            mark: MarkKind::Bar, // 4 primitives/row -> 2_400_000 primitives, overflows
            row_count,
            encodings: xy_only_encodings(),
            budget,
            out_of_core: false,
        });

        assert_eq!(scatter.tier, LodTier::Direct);
        assert_ne!(bar.tier, LodTier::Direct);
        assert_eq!(scatter.estimated_primitives, 600_000);
        assert_eq!(bar.estimated_primitives, 2_400_000);
    }

    #[test]
    fn additional_channels_raise_the_byte_estimate_and_can_flip_the_tier() {
        let row_count = 1_500_000;
        // 8 bytes/row (x,y only) * 1.5M = 12MB, fits a 16MB budget.
        let xy_budget = FrameBudget::new(10_000_000, 16 * 1024 * 1024);
        let xy_decision = select_tier(&TierInput {
            mark: MarkKind::Scatter,
            row_count,
            encodings: xy_only_encodings(),
            budget: xy_budget,
            out_of_core: false,
        });
        assert_eq!(xy_decision.tier, LodTier::Direct);

        // Same row_count and budget, but color+size add 8 bytes/row -> 16 total ->
        // 1.5M * 16 = 24MB, overflows the SAME byte budget.
        let channelled = Encodings {
            color: Some(EncodingSpec::field("c")),
            size: Some(EncodingSpec::field("s")),
            ..xy_only_encodings()
        };
        let channelled_decision = select_tier(&TierInput {
            mark: MarkKind::Scatter,
            row_count,
            encodings: channelled,
            budget: xy_budget,
            out_of_core: false,
        });
        assert_ne!(channelled_decision.tier, LodTier::Direct);
    }

    #[test]
    fn tier_ordering_is_monotonic() {
        assert!(LodTier::Direct < LodTier::Decimate);
        assert!(LodTier::Decimate < LodTier::Density);
        assert!(LodTier::Density < LodTier::Tiled);
    }

    #[test]
    fn heatmap_has_no_decimate_tier_born_aggregated() {
        assert!(!mark_supports_tier(MarkKind::Heatmap, LodTier::Decimate));
        assert!(mark_supports_tier(MarkKind::Heatmap, LodTier::Density));
    }

    #[test]
    fn round_trips_through_json() {
        let input = TierInput {
            mark: MarkKind::Graph,
            row_count: 42,
            encodings: xy_only_encodings(),
            budget: small_budget(),
            out_of_core: false,
        };
        let json = serde_json::to_string(&input).unwrap();
        let restored: TierInput = serde_json::from_str(&json).unwrap();
        assert_eq!(input, restored);
    }
}
