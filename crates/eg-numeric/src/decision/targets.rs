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
