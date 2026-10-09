//! Calibrated abstaining recommendations (EG-FINANCE-PRIMITIVES-R009.1): maps
//! a calibrated flip-confidence outcome (`confidence::flip_confidence`) to one
//! of the four recommendation actions the requirement names. Abstention is a
//! result, never an omission: an abstaining confidence outcome always
//! recommends `Abstain`, so insufficient evidence is never upgraded into a
//! directional call. Sealing a recommendation into an informational-only
//! `AnalysisSnapshot` record is a later slice (R009.2+).

#[cfg(test)]
use super::FlipAbstainReason;
use super::FlipConfidence;

/// One of the four actions a holding or watchlist item may be recommended.
/// A recommendation is informational only: it authorises no order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Recommendation {
    Accumulate,
    Hold,
    DeRisk,
    Abstain,
}

/// A recommendation together with the strategy version and the calibrated
/// evidence it cites.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RecommendationResult {
    pub strategy_version: u32,
    pub recommendation: Recommendation,
    pub evidence: FlipConfidence,
}

/// A calibrated follow-through probability (permille) at or above this
/// threshold is confident enough to recommend a directional action.
pub const CONFIDENT_PERMILLE: u32 = 650;

fn permille(probability: f64) -> u32 {
    (probability * 1000.0).round().clamp(0.0, 1000.0) as u32
}

/// Recommend an action for one calibrated flip-confidence outcome, citing
/// `strategy_version` and the evidence the recommendation is based on.
pub fn recommend(strategy_version: u32, confidence: FlipConfidence) -> RecommendationResult {
    let recommendation = match &confidence {
        FlipConfidence::Abstained { .. } => Recommendation::Abstain,
        FlipConfidence::Calibrated {
            probability,
            follows_through,
            ..
        } => {
            if permille(*probability) < CONFIDENT_PERMILLE {
                Recommendation::Hold
            } else if *follows_through {
                Recommendation::Accumulate
            } else {
                Recommendation::DeRisk
            }
        }
    };
    RecommendationResult {
        strategy_version,
        recommendation,
        evidence: confidence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn calibrated(probability: f64, follows_through: bool) -> FlipConfidence {
        FlipConfidence::Calibrated {
            horizon_bars: 5,
            probability,
            follows_through,
            alpha_permille: 100,
            n_calibration: 500,
        }
    }

    #[test]
    fn insufficient_evidence_always_abstains() {
        let confidence = FlipConfidence::Abstained {
            horizon_bars: 5,
            reason: FlipAbstainReason::InsufficientHistory { n: 2, n_min: 30 },
        };
        assert_eq!(
            recommend(1, confidence).recommendation,
            Recommendation::Abstain
        );
    }

    #[test]
    fn a_non_abstaining_recommendation_cites_strategy_version_and_evidence() {
        let confidence = calibrated(0.82, true);
        let result = recommend(3, confidence.clone());
        assert_eq!(result.strategy_version, 3);
        assert_eq!(result.evidence, confidence);
        assert_eq!(result.recommendation, Recommendation::Accumulate);
    }

    #[test]
    fn a_bearish_high_confidence_follow_through_recommends_de_risk() {
        assert_eq!(
            recommend(1, calibrated(0.9, false)).recommendation,
            Recommendation::DeRisk
        );
    }

    #[test]
    fn a_middling_probability_holds_rather_than_calling_a_direction() {
        assert_eq!(
            recommend(1, calibrated(0.55, true)).recommendation,
            Recommendation::Hold
        );
    }
}
