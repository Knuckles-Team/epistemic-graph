//! `ATTRIBUTE` (EH-523): contribution attribution over the incoming rows.
//!
//! The rows are the players, in input order. Each player's value is its row score or a
//! numeric property of its node; a coalition's value is the stage's aggregate of its
//! members' values (`v(∅) = 0`). The kernel is eg-numeric's attribution module — the
//! same one the served Decide slate report uses — so the query surface and the served
//! surface cannot disagree on what a contribution is. The output re-scores every row
//! with its contribution, ordered descending (ties keep input order); a sampled
//! estimate also reports each row's CI half-width as the `attribution_ci` channel.
//! Contributions sum to the grand coalition's value. Deterministic: the sampled
//! estimator's seed is part of the stage, so a re-run replays bit-identically.

use eg_types::wire::{AttributionInput, AttributionMethod, AttributionValue};

#[cfg(not(feature = "numeric"))]
use super::PlanCtx;
use crate::rowset::RowSet;

/// A stage's output: the re-scored rows and, for a sampled estimate, each row's CI
/// half-width.
pub(crate) struct Attributed {
    pub(crate) rows: RowSet,
    pub(crate) half_widths: Vec<(String, f32)>,
}

/// What an `ATTRIBUTE` stage asks for.
#[derive(Clone, Copy)]
pub(crate) struct AttributeSpec<'s> {
    pub(crate) input: &'s AttributionInput,
    pub(crate) value: AttributionValue,
    pub(crate) method: &'s AttributionMethod,
}

/// Run one `ATTRIBUTE` stage over `rows`. Without `numeric` there is no kernel: the
/// refusal names the exact stage (as UQL) that could not run.
#[cfg(not(feature = "numeric"))]
pub(crate) fn attribute(
    _ctx: &PlanCtx,
    _rows: RowSet,
    spec: AttributeSpec<'_>,
) -> Result<Attributed, String> {
    let stage = eg_types::wire::Op::Attribute {
        input: spec.input.clone(),
        value: spec.value,
        method: spec.method.clone(),
    };
    let printed = eg_types::wire::uql_op(&stage).unwrap_or_else(|e| format!("ATTRIBUTE ({e})"));
    Err(format!(
        "{}: `{printed}` needs the `numeric` feature",
        super::dispatch::UNSUPPORTED_MODALITY_OP
    ))
}

#[cfg(feature = "numeric")]
pub(crate) use kernel::attribute;

#[cfg(feature = "numeric")]
mod kernel {
    use std::collections::BTreeMap;

    use eg_numeric::attribution::{
        linear_split, owen_values, shapley_exact, shapley_sampled, Aggregate, AggregateGame,
        Attribution, AttributionCode, AttributionError, SampleSpec,
    };
    use eg_types::wire::{AttributionInput, AttributionMethod, AttributionValue};
    use serde_json::Value;

    use super::{AttributeSpec, Attributed};
    use crate::budget::BUDGET_EXCEEDED;
    use crate::exec::{row_value, PlanCtx};
    use crate::rowset::RowSet;

    /// Most coalition evaluations one stage may spend.
    pub(crate) const MAX_ATTRIBUTION_EVALUATIONS: u64 = 10_000_000;
    /// Two-sided confidence of a sampled estimate's per-row interval.
    const CONFIDENCE: f64 = 0.95;

    fn aggregate(value: AttributionValue) -> Aggregate {
        match value {
            AttributionValue::Sum => Aggregate::Sum,
            AttributionValue::Mean => Aggregate::Mean,
            AttributionValue::Max => Aggregate::Max,
            AttributionValue::Min => Aggregate::Min,
            AttributionValue::Percentile { p } => Aggregate::Quantile(f64::from(p) / 100.0),
        }
    }

    fn render(error: AttributionError) -> String {
        if error.code == AttributionCode::BudgetExceeded {
            return format!("{BUDGET_EXCEEDED}: ATTRIBUTE: {error}");
        }
        format!("ATTRIBUTE: {error}")
    }

    /// Node `id`'s property `name`, if the node has it.
    fn property(ctx: &PlanCtx, id: &str, name: &str) -> Option<Value> {
        row_value(ctx.view, id)?.get(name).cloned()
    }

    /// Each row's value as a player, in row order.
    fn player_values(
        ctx: &PlanCtx,
        rows: &RowSet,
        input: &AttributionInput,
    ) -> Result<Vec<f64>, String> {
        rows.rows()
            .iter()
            .map(|row| {
                let value = match input {
                    AttributionInput::Score => row.score.map(f64::from),
                    AttributionInput::Property { name } => {
                        property(ctx, &row.id, name).and_then(|v| v.as_f64())
                    }
                };
                value.ok_or_else(|| {
                    format!(
                        "ATTRIBUTE: row `{}` has no numeric value to attribute",
                        row.id
                    )
                })
            })
            .collect()
    }

    /// The rows grouped into unions by their node's `by` property, in key order.
    fn unions(ctx: &PlanCtx, rows: &RowSet, by: &str) -> Result<Vec<Vec<usize>>, String> {
        let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (index, row) in rows.rows().iter().enumerate() {
            let key = property(ctx, &row.id, by)
                .filter(|v| !v.is_null())
                .ok_or_else(|| format!("ATTRIBUTE: row `{}` has no `{by}` to group by", row.id))?;
            let key = key.as_str().map_or_else(|| key.to_string(), str::to_string);
            groups.entry(key).or_default().push(index);
        }
        Ok(groups.into_values().collect())
    }

    /// `SAMPLES n` counts permutations; they are drawn as antithetic pairs.
    fn pairs(samples: u32) -> u32 {
        samples.div_ceil(2).max(2)
    }

    fn solve(
        ctx: &PlanCtx,
        rows: &RowSet,
        game: &AggregateGame,
        method: &AttributionMethod,
    ) -> Result<Attribution, String> {
        let budget = MAX_ATTRIBUTION_EVALUATIONS;
        match method {
            AttributionMethod::Linear => linear_split(game),
            AttributionMethod::Shapley => shapley_exact(game, budget),
            AttributionMethod::ShapleySampled { samples, seed } => {
                let spec = SampleSpec {
                    pairs: pairs(*samples),
                    seed: *seed,
                    confidence: CONFIDENCE,
                };
                shapley_sampled(game, spec, budget)
            }
            AttributionMethod::Owen { by } => owen_values(game, &unions(ctx, rows, by)?, budget),
        }
        .map_err(render)
    }

    /// Rows re-scored by contribution, descending; ties keep input order.
    fn attributed(rows: &RowSet, attribution: &Attribution) -> Attributed {
        let mut order: Vec<usize> = (0..attribution.phi.len()).collect();
        order.sort_by(|&a, &b| {
            attribution.phi[b]
                .total_cmp(&attribution.phi[a])
                .then(a.cmp(&b))
        });
        let ids = rows.rows();
        let scored = order
            .iter()
            .map(|&i| (ids[i].id.clone(), attribution.phi[i] as f32));
        let half_widths = attribution
            .half_width
            .as_ref()
            .map_or_else(Vec::new, |widths| {
                order
                    .iter()
                    .map(|&i| (ids[i].id.clone(), widths[i] as f32))
                    .collect()
            });
        Attributed {
            rows: RowSet::from_scored(scored),
            half_widths,
        }
    }

    /// Run one `ATTRIBUTE` stage over `rows`.
    pub(crate) fn attribute(
        ctx: &PlanCtx,
        rows: RowSet,
        spec: AttributeSpec<'_>,
    ) -> Result<Attributed, String> {
        if rows.is_empty() {
            return Ok(Attributed {
                rows,
                half_widths: Vec::new(),
            });
        }
        let values = player_values(ctx, &rows, spec.input)?;
        let game = AggregateGame::new(values, aggregate(spec.value)).map_err(render)?;
        let attribution = solve(ctx, &rows, &game, spec.method)?;
        Ok(attributed(&rows, &attribution))
    }
}
