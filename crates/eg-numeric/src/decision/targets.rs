//! The training target of one admitted item (EH-027, EH-062).
//!
//! Uniform over the acceptability set for a full-label item; the executed
//! option for a successful bandit item, weighted by its clipped inverse
//! propensity. The linear fit and the scorer's fit both read targets here.

use eg_types::decision::statistical::dataset::{ItemLabel, LabelledItem};

/// Largest inverse-propensity weight a bandit item contributes.
pub const IPW_CLIP: f64 = 20.0;

/// The item's audit inverse-probability weight (1 when not audit-sampled).
pub(crate) fn audit_weight(item: &LabelledItem) -> f64 {
    item.audit_inclusion
        .filter(|p| p.numerator() > 0)
        .map_or(1.0, |p| p.denominator() as f64 / p.numerator() as f64)
}

/// The target distribution over the item's options and the item's weight;
/// `None` when the item carries no positive signal.
pub(crate) fn targets(item: &LabelledItem) -> Option<(Vec<f64>, f64)> {
    let n = item.candidate_ids.len();
    match &item.label {
        ItemLabel::Gold { acceptable, .. } => {
            let share = 1.0 / acceptable.len().max(1) as f64;
            let t = (0..n)
                .map(|i| {
                    if acceptable
                        .iter()
                        .any(|a| a == &item.candidate_ids.as_slice()[i])
                    {
                        share
                    } else {
                        0.0
                    }
                })
                .collect();
            Some((t, 1.0))
        }
        ItemLabel::Logged(logged) => {
            let executed = item.index_of(&logged.executed)?;
            if logged.evaluation.success != Some(true) {
                return None;
            }
            let p = logged.logging_propensities.as_slice()[executed];
            let weight = (p.denominator() as f64 / p.numerator().max(1) as f64).min(IPW_CLIP);
            let t = (0..n)
                .map(|i| if i == executed { 1.0 } else { 0.0 })
                .collect();
            Some((t, weight))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::contract::BoundedVec;
    use eg_types::decision::statistical::dataset::LabelSource;
    use eg_types::decision::UnitRationalWire;

    fn rational(n: u64, d: u64) -> UnitRationalWire {
        UnitRationalWire::new(n, d).expect("test rational")
    }

    fn item(audit_inclusion: Option<UnitRationalWire>) -> LabelledItem {
        LabelledItem {
            item_id: "gold-1".to_string(),
            recorded_at_ms: 1_000,
            class_key: "eg:task/research".to_string(),
            candidate_ids: BoundedVec::new(vec!["option-a".to_string(), "option-b".to_string()])
                .expect("inside bound"),
            features: BoundedVec::new(vec![0, 0]).expect("inside bound"),
            label: ItemLabel::Gold {
                acceptable: BoundedVec::new(vec!["option-a".to_string()]).expect("inside bound"),
                source: LabelSource::SyntheticConstruction,
            },
            audit_inclusion,
        }
    }

    // spec: EG-DECISION-ENGINE-R026.1
    #[test]
    fn calibration_applies_the_pub_crate_inverse_probability_audit_weight() {
        assert_eq!(audit_weight(&item(None)), 1.0);
        assert_eq!(audit_weight(&item(Some(rational(1, 20)))), 20.0);
        assert_eq!(audit_weight(&item(Some(rational(1, 1)))), 1.0);
        assert_eq!(audit_weight(&item(Some(rational(0, 5)))), 1.0);
    }
}
