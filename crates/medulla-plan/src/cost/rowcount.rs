//! Row-count estimators behind `ModalityCardinality::static_rows_out`'s per-op arms,
//! as free functions rather than methods, split out of `cost.rs` (KISS file/class
//! budget) purely so they don't grow the parent file or `ModalityCardinality`'s own
//! method count past their aggregate caps. Behaviour is unchanged from when this was
//! inline in `static_rows_out` (or a `ModalityCardinality` method).

use super::{ModalityCardinality, DEFAULT_TOP_K};

/// Fan-out expansion over `min..=max` hops of a `Traverse`: the per-hop degree ×
/// relationship-selectivity, summed across the hop range. Shared by
/// `ModalityCardinality::cost_of` (the per-edge cost) and [`traverse_static_rows_out`]
/// (the row narrowing) so the two stay literally in agreement instead of drifting
/// apart as separately copied math.
pub(super) fn traverse_edge_expansion(card: &ModalityCardinality, min: usize, max: usize) -> f64 {
    let d = card.stats.avg_out_degree.max(0.0) * ModalityCardinality::REL_SEL;
    (min..=max).map(|h| d.powi(h as i32)).sum::<f64>()
}

pub(super) fn traverse_static_rows_out(
    card: &ModalityCardinality,
    in_card: f64,
    min: usize,
    max: usize,
    n: f64,
) -> f64 {
    if in_card <= 0.0 {
        return 0.0;
    }
    let expansion = traverse_edge_expansion(card, min, max);
    (in_card * expansion * ModalityCardinality::DEDUP_DAMP).min(n.max(in_card))
}

pub(super) fn rank_static_rows_out(card: &ModalityCardinality, in_card: f64) -> f64 {
    if in_card > 0.0 {
        in_card * card.embed_coverage()
    } else {
        (card.stats.embedding_count as f64).min(DEFAULT_TOP_K as f64)
    }
}

pub(super) fn asof_static_rows_out(in_card: f64, n: f64) -> f64 {
    if in_card > 0.0 {
        in_card * ModalityCardinality::TEMPORAL_SEL
    } else {
        n * ModalityCardinality::TEMPORAL_SEL
    }
}

#[cfg(feature = "owl")]
pub(super) fn reason_static_rows_out(in_card: f64, n: f64) -> f64 {
    if in_card > 0.0 {
        in_card
            * ModalityCardinality::REASON_MEMBERSHIP_SEL
            * ModalityCardinality::REASON_CONF_RETENTION
    } else {
        n * ModalityCardinality::REASON_MEMBERSHIP_SEL
    }
}
