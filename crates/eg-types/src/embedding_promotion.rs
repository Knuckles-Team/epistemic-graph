//! Per-modality promotion gate for a candidate embedding model
//! (EG-UNIFIED-DATA-PLANE-R040). A candidate model and version promote only
//! when every modality it serves holds its recall on that modality's probe
//! set. One regressed modality blocks promotion even when the mean improves.
//!
//! The measured recall at k per modality comes from `eg_ann::recall_at_k`
//! over that modality's probe set; this module owns the typed checks, the
//! decision and the stored verdict shape.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The closed set of modalities a promotion check covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromotionModality {
    Text,
    Image,
    Audio,
}

/// The metric a modality check measures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum PromotionMetric {
    RecallAtK { k: u32 },
}

/// One modality's check: its probe set, the metric, the absolute floor and
/// the active version's baseline value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModalityCheck {
    pub modality: PromotionModality,
    pub probe_set_digest: String,
    pub metric: PromotionMetric,
    pub floor: f64,
    pub baseline: f64,
}

/// The candidate model and version and the modalities it serves.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionCandidate {
    pub model: String,
    pub version: String,
    pub modalities: Vec<PromotionModality>,
}

/// Default absolute recall drop that blocks promotion. A named, replaceable
/// default; a measurement on this platform replaces it.
pub const DEFAULT_PROMOTION_TOLERANCE: f64 = 0.02;

/// The per-modality line of a verdict.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModalityVerdict {
    pub modality: PromotionModality,
    pub probe_set_digest: Option<String>,
    pub metric: Option<PromotionMetric>,
    pub floor: Option<f64>,
    pub baseline: Option<f64>,
    pub measured: Option<f64>,
    pub passed: bool,
}

/// The stored promotion decision for one candidate.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PromotionVerdict {
    pub model: String,
    pub version: String,
    pub tolerance: f64,
    pub entries: Vec<ModalityVerdict>,
    pub promoted: bool,
}

impl PromotionVerdict {
    /// The modalities that blocked promotion.
    pub fn blocked_modalities(&self) -> Vec<PromotionModality> {
        self.entries
            .iter()
            .filter(|entry| !entry.passed)
            .map(|entry| entry.modality)
            .collect()
    }
}

fn judge(
    modality: PromotionModality,
    check: Option<&ModalityCheck>,
    measured: Option<f64>,
    tolerance: f64,
) -> ModalityVerdict {
    let Some(check) = check.filter(|check| !check.probe_set_digest.trim().is_empty()) else {
        return ModalityVerdict {
            modality,
            probe_set_digest: None,
            metric: None,
            floor: None,
            baseline: None,
            measured,
            passed: false,
        };
    };
    let passed = measured.is_some_and(|value| {
        value.is_finite() && value >= check.floor && value + tolerance >= check.baseline
    });
    ModalityVerdict {
        modality,
        probe_set_digest: Some(check.probe_set_digest.clone()),
        metric: Some(check.metric),
        floor: Some(check.floor),
        baseline: Some(check.baseline),
        measured,
        passed,
    }
}

/// Decide whether `candidate` promotes. `measured` holds the candidate's
/// recall at k per modality on that modality's probe set. A modality the
/// candidate serves with no probe set, no measurement, a value under its
/// floor, or a drop from baseline beyond `tolerance` blocks promotion.
pub fn evaluate_promotion(
    candidate: &PromotionCandidate,
    checks: &[ModalityCheck],
    measured: &BTreeMap<PromotionModality, f64>,
    tolerance: f64,
) -> PromotionVerdict {
    let mut modalities = candidate.modalities.clone();
    modalities.sort();
    modalities.dedup();
    let entries: Vec<ModalityVerdict> = modalities
        .into_iter()
        .map(|modality| {
            let check = checks.iter().find(|check| check.modality == modality);
            judge(modality, check, measured.get(&modality).copied(), tolerance)
        })
        .collect();
    let promoted = !entries.is_empty() && entries.iter().all(|entry| entry.passed);
    PromotionVerdict {
        model: candidate.model.clone(),
        version: candidate.version.clone(),
        tolerance,
        entries,
        promoted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(modality: PromotionModality, baseline: f64) -> ModalityCheck {
        ModalityCheck {
            modality,
            probe_set_digest: format!("sha256:{modality:?}"),
            metric: PromotionMetric::RecallAtK { k: 10 },
            floor: 0.5,
            baseline,
        }
    }

    fn candidate(modalities: Vec<PromotionModality>) -> PromotionCandidate {
        PromotionCandidate {
            model: "clip-large".to_string(),
            version: "2".to_string(),
            modalities,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R040.1
    #[test]
    fn checks_round_trip() {
        let original = check(PromotionModality::Image, 0.91);
        let json = serde_json::to_string(&original).unwrap();
        let back: ModalityCheck = serde_json::from_str(&json).unwrap();
        assert_eq!(back, original);
        let unknown = json.replace("\"image\"", "\"video\"");
        assert!(serde_json::from_str::<ModalityCheck>(&unknown).is_err());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R040.2
    #[test]
    fn one_regression_blocks_promotion() {
        use PromotionModality::{Audio, Image, Text};
        let checks = vec![check(Text, 0.80), check(Image, 0.90), check(Audio, 0.70)];
        // The mean improves (0.80 -> 0.85 avg), but image drops by 0.05.
        let measured = BTreeMap::from([(Text, 0.95), (Image, 0.85), (Audio, 0.75)]);
        let verdict = evaluate_promotion(
            &candidate(vec![Text, Image, Audio]),
            &checks,
            &measured,
            DEFAULT_PROMOTION_TOLERANCE,
        );
        assert!(!verdict.promoted);
        assert_eq!(verdict.blocked_modalities(), vec![Image]);

        let within = BTreeMap::from([(Text, 0.80), (Image, 0.89), (Audio, 0.70)]);
        let verdict = evaluate_promotion(
            &candidate(vec![Text, Image, Audio]),
            &checks,
            &within,
            DEFAULT_PROMOTION_TOLERANCE,
        );
        assert!(verdict.promoted, "a drop within the tolerance promotes");
    }

    // spec: EG-UNIFIED-DATA-PLANE-R040.2
    #[test]
    fn modality_without_probe_set_blocks_promotion() {
        use PromotionModality::{Audio, Text};
        let measured = BTreeMap::from([(Text, 0.95), (Audio, 0.95)]);
        let verdict = evaluate_promotion(
            &candidate(vec![Text, Audio]),
            &[check(Text, 0.8)],
            &measured,
            DEFAULT_PROMOTION_TOLERANCE,
        );
        assert!(!verdict.promoted);
        assert_eq!(verdict.blocked_modalities(), vec![Audio]);
        let audio = verdict
            .entries
            .iter()
            .find(|e| e.modality == Audio)
            .unwrap();
        assert_eq!(audio.probe_set_digest, None);
    }
}
