//! Document boosts learned from retrieval feedback
//! (MEDULLA-FEDERATED-QUERY-R705.1, R705.2).
//!
//! A boost is the half-life-decayed sum of a document's feedback weights,
//! clamped to `[-MAX_BOOST, MAX_BOOST]`. It is added after score calibration,
//! so it can reorder near ties but never outweighs a calibrated score gap
//! larger than one boost range.

use serde::{Deserialize, Serialize};

/// The boost range is `[-MAX_BOOST, MAX_BOOST]`.
pub const MAX_BOOST: f64 = 0.5;
/// The default half life: 30 days.
pub const DEFAULT_HALF_LIFE_S: u64 = 30 * 24 * 60 * 60;

/// The kind of a feedback event. Closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackKind {
    Upvote,
    Downvote,
    Cited,
    Ignored,
}

/// One feedback event (MEDULLA-FEDERATED-QUERY-R705.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrievalFeedback {
    pub tenant: String,
    pub principal: String,
    pub document_ref: String,
    pub kind: FeedbackKind,
    pub weight: f64,
    /// Unix seconds.
    pub at: u64,
}

/// A document's current boost (MEDULLA-FEDERATED-QUERY-R705.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentBoost {
    pub document_ref: String,
    pub value: f64,
    pub half_life_s: u64,
}

/// Compute `document_ref`'s boost at `now` from its feedback (MEDULLA-FEDERATED-QUERY-R705.2).
/// Events for other documents, non-finite weights and future events are ignored.
pub fn document_boost(
    document_ref: &str,
    events: &[RetrievalFeedback],
    now: u64,
    half_life_s: u64,
) -> DocumentBoost {
    let half_life = half_life_s.max(1) as f64;
    let sum: f64 = events
        .iter()
        .filter(|e| e.document_ref == document_ref && e.weight.is_finite() && e.at <= now)
        .map(|e| e.weight * 0.5f64.powf((now - e.at) as f64 / half_life))
        .sum();
    DocumentBoost {
        document_ref: document_ref.to_string(),
        value: sum.clamp(-MAX_BOOST, MAX_BOOST),
        half_life_s,
    }
}

/// Add boosts to calibrated scores and re-rank, ties by id.
pub fn apply_boosts(calibrated: &[(String, f64)], boosts: &[DocumentBoost]) -> Vec<(String, f64)> {
    let mut out: Vec<(String, f64)> = calibrated
        .iter()
        .map(|(id, score)| {
            let boost = boosts
                .iter()
                .find(|b| &b.document_ref == id)
                .map_or(0.0, |b| b.value);
            (id.clone(), score + boost)
        })
        .collect();
    out.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(doc: &str, kind: FeedbackKind, weight: f64, at: u64) -> RetrievalFeedback {
        RetrievalFeedback {
            tenant: "t1".into(),
            principal: "alice".into(),
            document_ref: doc.into(),
            kind,
            weight,
            at,
        }
    }

    // spec: MEDULLA-FEDERATED-QUERY-R705.1
    #[test]
    fn feedback_round_trips() {
        for kind in [
            FeedbackKind::Upvote,
            FeedbackKind::Downvote,
            FeedbackKind::Cited,
            FeedbackKind::Ignored,
        ] {
            let e = event("doc", kind, 0.1, 5);
            let json = serde_json::to_string(&e).unwrap();
            assert_eq!(serde_json::from_str::<RetrievalFeedback>(&json).unwrap(), e);
        }
        assert!(serde_json::from_str::<FeedbackKind>("\"starred\"").is_err());
        let boost = DocumentBoost {
            document_ref: "doc".into(),
            value: 0.25,
            half_life_s: DEFAULT_HALF_LIFE_S,
        };
        let json = serde_json::to_string(&boost).unwrap();
        assert_eq!(serde_json::from_str::<DocumentBoost>(&json).unwrap(), boost);
    }

    // spec: MEDULLA-FEDERATED-QUERY-R705.2
    #[test]
    fn boost_decays_and_clamps() {
        let h = DEFAULT_HALF_LIFE_S;
        let one = [event("d", FeedbackKind::Upvote, 0.2, 0)];
        assert!((document_boost("d", &one, 0, h).value - 0.2).abs() < 1e-12);
        assert!((document_boost("d", &one, h, h).value - 0.1).abs() < 1e-12);
        assert!((document_boost("d", &one, 2 * h, h).value - 0.05).abs() < 1e-12);

        let many_up: Vec<_> = (0..10)
            .map(|_| event("d", FeedbackKind::Upvote, 0.2, 0))
            .collect();
        assert_eq!(document_boost("d", &many_up, 0, h).value, MAX_BOOST);
        let many_down: Vec<_> = (0..10)
            .map(|_| event("d", FeedbackKind::Downvote, -0.2, 0))
            .collect();
        assert_eq!(document_boost("d", &many_down, 0, h).value, -MAX_BOOST);
        assert_eq!(document_boost("other", &many_up, 0, h).value, 0.0);

        // Applied after calibration: it settles a near tie, not a wide gap.
        let calibrated = vec![
            ("a".to_string(), 0.80),
            ("b".to_string(), 0.79),
            ("c".to_string(), 1.50),
        ];
        let up_b: Vec<_> = (0..10)
            .map(|_| event("b", FeedbackKind::Upvote, 0.2, 0))
            .collect();
        let boosts = [document_boost("b", &up_b, 0, h)];
        let ranked: Vec<_> = apply_boosts(&calibrated, &boosts)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ranked, vec!["c", "b", "a"]);
    }
}
