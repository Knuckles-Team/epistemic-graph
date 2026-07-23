//! CONCEPT:EG-KG.epistemic.provenance-ranking — EPI-P3-3: provenance-aware
//! retrieval ranking.
//!
//! Orders retrieval candidates by evidence QUALITY/PROVENANCE — source
//! reliability, corroboration (how much evidence backs the claim), calibration
//! precision (how narrow its credible interval is), and freshness — IN ADDITION
//! TO similarity, rather than by similarity alone. This directly reuses
//! [`crate::model::Calibration`] (EPI-P3-3's calibrated-interval signal): a
//! candidate whose belief was propagated with real evidence carries a real
//! `Calibration`, and its `evidence_count`/`interval` width feed straight into
//! its provenance score — no separate/invented quality metric.
//!
//! The point: a merely-more-similar-but-unsourced result should NOT
//! automatically outrank a well-sourced, well-corroborated one — the standard
//! "provenance beats raw similarity" retrieval-augmented-generation concern
//! (a single unreliable, uncorroborated near-duplicate should not dominate the
//! top of the list just because it happens to score marginally higher on
//! vector similarity).

use crate::model::Calibration;

/// One retrieval candidate, before ranking.
#[derive(Clone, Debug, PartialEq)]
pub struct RetrievalCandidate {
    pub id: String,
    /// Similarity score from the retrieval backend (embedding cosine, BM25,
    /// …), expected in `[0, 1]` (higher = more similar to the query).
    pub similarity: f64,
    /// Source authority/reliability in `[0, 1]` — e.g. the dominant source's
    /// `AuthorityPolicy::source_reliability`, or a stored per-source trust score.
    pub source_reliability: f64,
    /// How recent the evidence is, in `[0, 1]` (`1.0` = freshest). A caller
    /// typically derives this from the same recency-decay model the stored
    /// `NodeData.confidence` already carries.
    pub freshness: f64,
    /// This candidate's calibrated belief, if `propagate_confidence` computed
    /// one for it — `None` for a candidate with no evidence-graph backing
    /// (it then ranks on similarity/reliability/freshness alone; corroboration
    /// and precision both contribute `0.0`, an honest "unknown", not a
    /// fabricated middling score).
    pub calibration: Option<Calibration>,
}

/// Weights combining similarity with the provenance/evidence-quality signal.
/// Both `>= 0`; needn't sum to `1.0` (normalized internally by [`rank`]).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RankWeights {
    pub similarity: f64,
    pub evidence_quality: f64,
}

impl Default for RankWeights {
    /// Evidence quality weighted EQUAL to raw similarity — enough that a
    /// dramatically better-sourced/corroborated candidate CAN outrank a
    /// marginally more similar one, which is the entire point of a
    /// provenance-aware ranking (a pure-similarity rank would set this `0.0`).
    fn default() -> Self {
        RankWeights {
            similarity: 0.5,
            evidence_quality: 0.5,
        }
    }
}

/// One ranked candidate: the final blended score plus its components, kept
/// separate for explainability (why did this result rank where it did?).
#[derive(Clone, Debug, PartialEq)]
pub struct RankedResult {
    pub id: String,
    pub score: f64,
    pub similarity: f64,
    pub evidence_quality: f64,
}

/// This candidate's evidence-quality sub-score `[0, 1]` — an equal-weighted
/// average of:
/// * `source_reliability` (as given),
/// * corroboration: `evidence_count / (evidence_count + 2)` — a saturating
///   (diminishing-returns) function of how many supporting/contradicting/
///   attacking edges fed the calibrated posterior (`0` with no calibration),
/// * precision: `1 − interval_width` — a NARROWER calibrated credible interval
///   scores higher (a belief pinned tightly by lots of consistent evidence is
///   higher-quality than one with a wide, uncertain band),
/// * `freshness` (as given).
///
/// A straight average (not a product) deliberately: this is a RANKING signal,
/// not a hard gate, so one weak factor should not annihilate an otherwise
/// well-supported candidate's score.
pub fn evidence_quality(candidate: &RetrievalCandidate) -> f64 {
    let corroboration = candidate
        .calibration
        .map(|cal| {
            let n = cal.evidence_count as f64;
            n / (n + 2.0)
        })
        .unwrap_or(0.0);
    let precision = candidate
        .calibration
        .map(|cal| {
            let width = (cal.interval.1 - cal.interval.0).clamp(0.0, 1.0);
            1.0 - width
        })
        .unwrap_or(0.0);
    let components = [
        candidate.source_reliability.clamp(0.0, 1.0),
        corroboration,
        precision,
        candidate.freshness.clamp(0.0, 1.0),
    ];
    components.iter().sum::<f64>() / components.len() as f64
}

/// Rank `candidates` by a weighted blend of similarity and
/// [`evidence_quality`] (EPI-P3-3), highest score first. Ties broken by
/// insertion order (stable sort).
pub fn rank(candidates: &[RetrievalCandidate], weights: RankWeights) -> Vec<RankedResult> {
    let w_sim = weights.similarity.max(0.0);
    let w_eq = weights.evidence_quality.max(0.0);
    let total_w = (w_sim + w_eq).max(1e-9);

    let mut ranked: Vec<RankedResult> = candidates
        .iter()
        .map(|c| {
            let eq = evidence_quality(c);
            let similarity = c.similarity.clamp(0.0, 1.0);
            let score = (w_sim * similarity + w_eq * eq) / total_w;
            RankedResult {
                id: c.id.clone(),
                score,
                similarity,
                evidence_quality: eq,
            }
        })
        .collect();
    ranked.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    ranked
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Calibration;

    fn well_sourced(id: &str, similarity: f64) -> RetrievalCandidate {
        RetrievalCandidate {
            id: id.to_string(),
            similarity,
            source_reliability: 0.95,
            freshness: 0.9,
            calibration: Some(Calibration {
                interval: (0.85, 0.95),
                level: 0.95,
                evidence_count: 5,
            }),
        }
    }

    fn unsourced(id: &str, similarity: f64) -> RetrievalCandidate {
        RetrievalCandidate {
            id: id.to_string(),
            similarity,
            source_reliability: 0.3,
            freshness: 0.3,
            calibration: None,
        }
    }

    // THE core proof: a better-sourced, well-corroborated candidate beats a
    // MERELY more-similar, unsourced one.
    #[test]
    fn provenance_beats_raw_similarity() {
        let candidates = vec![
            well_sourced("well-sourced", 0.7),
            unsourced("more-similar", 0.9),
        ];
        let ranked = rank(&candidates, RankWeights::default());
        assert_eq!(
            ranked[0].id, "well-sourced",
            "the better-sourced/corroborated candidate should rank first despite lower similarity, got order {:?}",
            ranked.iter().map(|r| &r.id).collect::<Vec<_>>()
        );
        assert!(ranked[0].score > ranked[1].score);
    }

    // Similarity-only weighting (evidence_quality=0) recovers plain similarity
    // ranking — proves provenance awareness is additive, not a silent override.
    #[test]
    fn zero_evidence_weight_falls_back_to_similarity_order() {
        let candidates = vec![
            well_sourced("well-sourced", 0.7),
            unsourced("more-similar", 0.9),
        ];
        let ranked = rank(
            &candidates,
            RankWeights {
                similarity: 1.0,
                evidence_quality: 0.0,
            },
        );
        assert_eq!(ranked[0].id, "more-similar");
    }

    #[test]
    fn no_calibration_yields_zero_corroboration_and_precision_not_a_fabricated_score() {
        let c = unsourced("x", 0.5);
        // components: source_reliability=0.3, corroboration=0, precision=0, freshness=0.3
        let eq = evidence_quality(&c);
        assert!((eq - (0.3 + 0.0 + 0.0 + 0.3) / 4.0).abs() < 1e-9);
    }

    #[test]
    fn more_corroborating_evidence_and_a_tighter_interval_score_higher() {
        let loose = RetrievalCandidate {
            id: "loose".to_string(),
            similarity: 0.5,
            source_reliability: 0.5,
            freshness: 0.5,
            calibration: Some(Calibration {
                interval: (0.1, 0.9),
                level: 0.95,
                evidence_count: 1,
            }),
        };
        let tight = RetrievalCandidate {
            id: "tight".to_string(),
            similarity: 0.5,
            source_reliability: 0.5,
            freshness: 0.5,
            calibration: Some(Calibration {
                interval: (0.55, 0.65),
                level: 0.95,
                evidence_count: 10,
            }),
        };
        assert!(evidence_quality(&tight) > evidence_quality(&loose));
    }

    #[test]
    fn rank_is_stable_and_covers_every_candidate() {
        let candidates = vec![
            unsourced("a", 0.5),
            unsourced("b", 0.5),
            well_sourced("c", 0.5),
        ];
        let ranked = rank(&candidates, RankWeights::default());
        assert_eq!(ranked.len(), 3);
        assert_eq!(ranked[0].id, "c");
    }
}
