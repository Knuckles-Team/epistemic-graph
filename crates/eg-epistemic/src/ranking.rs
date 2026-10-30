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

/// Tie-break policy among candidates that share one code
/// (EG-DECISION-ENGINE-R130.1). Closed set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TieBreakPolicy {
    /// Order equal-code candidates by their item-level `tie_break_score`.
    ItemScore,
    /// Order equal-code candidates by their stable `id`.
    StableId,
}

/// One candidate for calibrated ranking (EG-DECISION-ENGINE-R130.1).
/// `calibrated_score` is `None` when no calibration exists; such a
/// candidate is never ranked on a substitute likelihood (R130.4).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RankedCandidate {
    pub id: String,
    pub calibrated_score: Option<f64>,
    /// The shared code (e.g. a generated sequence code) candidates may tie on.
    pub code: String,
    /// The item-level score that separates candidates sharing one code.
    pub tie_break_score: f64,
    pub evidence_class: String,
}

/// Named default: the calibrated score orders candidates first (R130.2).
pub const CALIBRATED_FIRST: &str = "calibrated_first";
/// Named default: equal codes are separated by the item-level score (R130.2).
pub const ITEM_LEVEL_TIE_BREAK: &str = "item_level_tie_break";
/// Abstention reason for a candidate without a calibrated score (R130.4).
pub const UNCALIBRATED_CANDIDATE: &str = "uncalibrated_candidate";

/// The replaceable ranking rules (R130.2). `Default` is the named defaults
/// `calibrated_first` plus `item_level_tie_break`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CalibratedRankRules {
    pub calibrated_first: bool,
    pub tie_break: TieBreakPolicy,
}

impl Default for CalibratedRankRules {
    fn default() -> Self {
        CalibratedRankRules {
            calibrated_first: true,
            tie_break: TieBreakPolicy::ItemScore,
        }
    }
}

/// A candidate the ranking refused to order (R130.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RankAbstention {
    pub id: String,
    pub reason: &'static str,
}

/// The outcome of [`rank_calibrated`]: the ordered candidates (each with
/// whether the tie-break decided its position against a neighbour) and the
/// candidates abstained on.
#[derive(Clone, Debug, PartialEq)]
pub struct CalibratedRanking {
    pub ranked: Vec<(RankedCandidate, bool)>,
    pub abstained: Vec<RankAbstention>,
}

/// Rank by calibrated score, highest first; candidates sharing one code are
/// ordered by the tie-break policy (EG-DECISION-ENGINE-R130.2). Uncalibrated
/// candidates abstain with `uncalibrated_candidate` (R130.4). Fully
/// deterministic: the final key is the candidate `id`.
pub fn rank_calibrated(
    candidates: &[RankedCandidate],
    rules: CalibratedRankRules,
) -> CalibratedRanking {
    use std::cmp::Ordering;
    let mut abstained = Vec::new();
    let mut ranked: Vec<RankedCandidate> = Vec::new();
    for c in candidates {
        match c.calibrated_score {
            Some(s) if s.is_finite() => ranked.push(c.clone()),
            _ => abstained.push(RankAbstention {
                id: c.id.clone(),
                reason: UNCALIBRATED_CANDIDATE,
            }),
        }
    }
    let score = |c: &RankedCandidate| c.calibrated_score.unwrap_or(f64::NEG_INFINITY);
    let tie = |a: &RankedCandidate, b: &RankedCandidate| match rules.tie_break {
        TieBreakPolicy::ItemScore => b
            .tie_break_score
            .partial_cmp(&a.tie_break_score)
            .unwrap_or(Ordering::Equal),
        TieBreakPolicy::StableId => Ordering::Equal,
    };
    ranked.sort_by(|a, b| {
        let primary = if rules.calibrated_first {
            score(b).partial_cmp(&score(a)).unwrap_or(Ordering::Equal)
        } else {
            Ordering::Equal
        };
        primary
            .then_with(|| a.code.cmp(&b.code).then_with(|| tie(a, b)))
            .then_with(|| a.id.cmp(&b.id))
    });
    let applied: Vec<bool> = (0..ranked.len())
        .map(|i| {
            let shares = |j: usize| {
                ranked
                    .get(j)
                    .is_some_and(|o| o.code == ranked[i].code && score(o) == score(&ranked[i]))
            };
            (i > 0 && shares(i - 1)) || shares(i + 1)
        })
        .collect();
    CalibratedRanking {
        ranked: ranked.into_iter().zip(applied).collect(),
        abstained,
    }
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

    fn cand(id: &str, score: Option<f64>, code: &str, tie: f64) -> RankedCandidate {
        RankedCandidate {
            id: id.into(),
            calibrated_score: score,
            code: code.into(),
            tie_break_score: tie,
            evidence_class: "measured".into(),
        }
    }

    #[test]
    fn ranked_candidate_round_trips() {
        let c = cand("a", Some(0.7), "C1", 0.4);
        let json = serde_json::to_value(&c).unwrap();
        assert_eq!(serde_json::from_value::<RankedCandidate>(json).unwrap(), c);
        for (p, w) in [
            (TieBreakPolicy::ItemScore, "item_score"),
            (TieBreakPolicy::StableId, "stable_id"),
        ] {
            assert_eq!(serde_json::to_value(p).unwrap(), serde_json::json!(w));
            assert_eq!(
                serde_json::from_value::<TieBreakPolicy>(serde_json::json!(w)).unwrap(),
                p
            );
        }
        assert!(serde_json::from_value::<TieBreakPolicy>(serde_json::json!("likelihood")).is_err());
        assert!(
            serde_json::from_value::<RankedCandidate>(serde_json::json!({
                "id": "a", "calibrated_score": 0.1, "code": "c", "tie_break_score": 0.0,
                "evidence_class": "x", "extra": 1
            }))
            .is_err()
        );
    }

    #[test]
    fn equal_codes_order_by_item_score() {
        let input = vec![
            cand("x", Some(0.5), "C", 0.2),
            cand("y", Some(0.5), "C", 0.9),
            cand("z", Some(0.5), "C", 0.5),
            cand("top", Some(0.8), "D", 0.0),
        ];
        let out = rank_calibrated(&input, CalibratedRankRules::default());
        let ids: Vec<&str> = out.ranked.iter().map(|(c, _)| c.id.as_str()).collect();
        assert_eq!(ids, ["top", "y", "z", "x"]);
        assert_eq!(
            out.ranked.iter().map(|(_, t)| *t).collect::<Vec<_>>(),
            [false, true, true, true]
        );
        let again = rank_calibrated(&input, CalibratedRankRules::default());
        assert_eq!(format!("{again:?}"), format!("{out:?}"));
        let stable = rank_calibrated(
            &input,
            CalibratedRankRules {
                tie_break: TieBreakPolicy::StableId,
                ..Default::default()
            },
        );
        let ids: Vec<&str> = stable.ranked.iter().map(|(c, _)| c.id.as_str()).collect();
        assert_eq!(ids, ["top", "x", "y", "z"]);
    }

    #[test]
    fn uncalibrated_candidate_abstains() {
        let input = vec![
            cand("cal", Some(0.3), "C", 0.0),
            cand("uncal", None, "C", 0.99),
            cand("nan", Some(f64::NAN), "C", 0.5),
        ];
        let out = rank_calibrated(&input, CalibratedRankRules::default());
        assert_eq!(out.ranked.len(), 1);
        assert_eq!(out.ranked[0].0.id, "cal");
        assert_eq!(
            out.abstained,
            vec![
                RankAbstention {
                    id: "uncal".into(),
                    reason: UNCALIBRATED_CANDIDATE
                },
                RankAbstention {
                    id: "nan".into(),
                    reason: UNCALIBRATED_CANDIDATE
                },
            ]
        );
    }
}
