//! `SKILL` (EH-522 FeatureSkill): the predictive skill of one value channel for a later
//! one, per series — `eg_numeric::evaluation::skill::feature_skill`, the one kernel.
//!
//! Each series' rows are taken in timestamp order, keeping those where both channels
//! have a value; a series with fewer than two such rows reports nothing. The input rows
//! are replaced by one report row per series and horizon: id `<series>:skill@<h>` (a
//! series row id, so a later `DERIVE` or `SKILL` can read it), score the mean IC, and
//! the `SKILL_CHANNELS` values.

use std::collections::BTreeMap;

use eg_numeric::evaluation::skill::{feature_skill, FeatureSkill, SkillSpec};
use eg_types::series_expr::SkillOp;

use super::derive::{named_series_groups, read_channel};
use crate::rowset::{series_row_id, Row, RowSet, ValueChannels};

/// Replace the input's series rows with their skill report rows.
pub(crate) fn skill_op(input: RowSet, op: &SkillOp) -> Result<RowSet, String> {
    let spec = SkillSpec {
        horizons: op.horizons.iter().map(|&h| h as usize).collect(),
        window: op.window as usize,
        resamples: op.resamples as usize,
        seed: op.seed,
    };
    let (rows, values) = input.into_parts();
    let mut scored = Vec::new();
    let mut channels = ValueChannels::new();
    for (series, indices) in named_series_groups(&rows) {
        let (feature, outcome) = aligned(&rows, &indices, &values, op);
        if feature.len() < 2 {
            continue;
        }
        let report = feature_skill(&feature, &outcome, &spec).map_err(|e| format!("SKILL: {e}"))?;
        report_rows(series, &report, &mut scored, &mut channels);
    }
    Ok(RowSet::from_scored(scored).with_values(channels))
}

/// The series' `(feature, outcome)` values at the rows where both are present.
fn aligned(
    rows: &[Row],
    indices: &[usize],
    values: &ValueChannels,
    op: &SkillOp,
) -> (Vec<f64>, Vec<f64>) {
    indices
        .iter()
        .filter_map(|&i| {
            let row = &rows[i];
            Some((
                read_channel(values, row, &op.feature)?,
                read_channel(values, row, &op.outcome)?,
            ))
        })
        .unzip()
}

fn report_rows(
    series: &str,
    report: &FeatureSkill,
    scored: &mut Vec<(String, f32)>,
    channels: &mut ValueChannels,
) {
    let name = format!("{series}:skill");
    for h in &report.horizons {
        let id = series_row_id(&name, h.horizon as i64);
        scored.push((id.clone(), h.mean_ic as f32));
        let row: BTreeMap<String, f64> = [
            ("mean_ic", h.mean_ic),
            ("ic_std", h.ic_std),
            ("icir", h.icir),
            ("ic_lo", h.ci_lo),
            ("ic_hi", h.ci_hi),
            ("n", h.n as f64),
            ("n_eff", report.n_eff),
            ("ir", report.ir),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        channels.insert(id, row);
    }
}
