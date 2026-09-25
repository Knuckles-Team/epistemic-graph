//! Predictive-skill section of a full-label DecisionEval receipt.

use eg_types::decision::jobs::{FeatureSkillRequest, FeatureSkillView, HorizonSkillView};
use eg_types::decision::statistical::dataset::{ItemLabel, LabelledDataset, LabelledItem};
use eg_types::decision::statistical::StatisticalErrorCode;

use super::admission::Regime;
use super::quant::{q32, raw_value};
use super::refusal::{bounded, Refusal, RefusalResult};
use crate::evaluation::skill::{feature_skill, SkillSpec};

fn invalid(detail: impl Into<String>) -> Refusal {
    Refusal::new(StatisticalErrorCode::DatasetInvalid, detail)
}

fn checked_spec(request: &FeatureSkillRequest) -> RefusalResult<SkillSpec> {
    let horizons = request.horizons.as_slice();
    if request.feature_name.is_empty() || request.candidate_id.is_empty() {
        return Err(invalid("feature skill requires a feature and candidate"));
    }
    if horizons.is_empty() || horizons.iter().any(|&h| h == 0) {
        return Err(invalid("feature skill horizons must be positive"));
    }
    if horizons.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid(
            "feature skill horizons must increase without duplicates",
        ));
    }
    if request.window < 2 || !(1..=512).contains(&request.bootstrap_resamples) {
        return Err(invalid(
            "feature skill needs a window and bootstrap resamples",
        ));
    }
    Ok(SkillSpec {
        horizons: horizons.iter().map(|&h| h as usize).collect(),
        window: request.window as usize,
        resamples: request.bootstrap_resamples as usize,
        seed: request.seed,
    })
}

fn ordered_items<'a>(items: &[&'a LabelledItem]) -> Vec<&'a LabelledItem> {
    let mut ordered = items.to_vec();
    ordered.sort_by(|a, b| (a.recorded_at_ms, &a.item_id).cmp(&(b.recorded_at_ms, &b.item_id)));
    ordered
}

fn series(
    dataset: &LabelledDataset,
    items: &[&LabelledItem],
    request: &FeatureSkillRequest,
) -> RefusalResult<(Vec<f64>, Vec<f64>)> {
    let width = dataset.feature_names.len();
    let feature_index = dataset
        .feature_names
        .iter()
        .position(|name| name == &request.feature_name)
        .ok_or_else(|| invalid("feature skill names an absent feature"))?;
    let mut feature = Vec::new();
    let mut outcome = Vec::new();
    for item in ordered_items(items) {
        let Some(candidate_index) = item.index_of(&request.candidate_id) else {
            continue;
        };
        let ItemLabel::Gold { acceptable, .. } = &item.label else {
            return Err(invalid("feature skill requires full labels"));
        };
        let raw = item.features.as_slice()[candidate_index * width + feature_index];
        feature.push(raw_value(raw, dataset.scale));
        outcome.push(if acceptable.as_slice().contains(&request.candidate_id) {
            1.0
        } else {
            0.0
        });
    }
    Ok((feature, outcome))
}

/// A full-label candidate's feature at t predicts its acceptability at t+h.
/// Items where the candidate was not offered cannot contribute a feature row.
pub fn evaluate_feature_skill(
    dataset: &LabelledDataset,
    items: &[&LabelledItem],
    regime: Regime,
    request: &FeatureSkillRequest,
) -> RefusalResult<FeatureSkillView> {
    if regime != Regime::FullLabel {
        return Err(invalid("feature skill cannot use bandit labels"));
    }
    let spec = checked_spec(request)?;
    let (feature, outcome) = series(dataset, items, request)?;
    let max_horizon = spec.horizons.last().copied().unwrap_or(0);
    if feature.len() < spec.window + max_horizon {
        return Err(invalid("feature skill has insufficient admitted history"));
    }
    let result = feature_skill(&feature, &outcome, &spec)
        .map_err(|error| Refusal::new(StatisticalErrorCode::NumericRefused, error.to_string()))?;
    let horizons = result
        .horizons
        .iter()
        .map(|row| {
            Ok(HorizonSkillView {
                horizon: row.horizon as u16,
                n_windows: row.n as u64,
                mean_ic: q32(row.mean_ic)?,
                ic_std: q32(row.ic_std)?,
                icir: q32(row.icir)?,
                ci_lo: q32(row.ci_lo)?,
                ci_hi: q32(row.ci_hi)?,
            })
        })
        .collect::<RefusalResult<Vec<_>>>()?;
    Ok(FeatureSkillView {
        feature_name: request.feature_name.clone(),
        candidate_id: request.candidate_id.clone(),
        n_items: feature.len() as u64,
        window: request.window,
        bootstrap_resamples: request.bootstrap_resamples,
        seed: request.seed,
        horizons: bounded(horizons)?,
        effective_independent_n: q32(result.n_eff)?,
        information_ratio: q32(result.ir)?,
    })
}
