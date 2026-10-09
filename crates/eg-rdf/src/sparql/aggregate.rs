use super::*;

// ── GROUP BY + aggregates (CONCEPT:EG-KG.query.sparql-completeness) ────────────────────────────────────

/// Evaluate `GROUP BY group_vars` + the `aggregates` over `rows`. Returns one
/// solution per distinct group-key, binding each group-by var to its value AND each
/// aggregate-result var (the internal name spargebra assigns) to the computed scalar.
/// The wrapping `Extend` re-binds those internal vars to the user's projected names.
pub(super) fn eval_group(
    ctx: &Ctx,
    rows: Vec<Solution>,
    group_vars: &[spargebra::term::Variable],
    aggregates: &[(spargebra::term::Variable, AggregateExpression)],
) -> Vec<Solution> {
    use std::collections::BTreeMap;

    // Bucket rows by the tuple of group-by values (a stable string key keeps the
    // result deterministic). With no GROUP BY var, ALL rows fall in one "" group.
    let mut groups: BTreeMap<String, Vec<Solution>> = BTreeMap::new();
    for row in rows {
        let key = group_vars
            .iter()
            .map(|v| {
                row.get(v.as_str())
                    .map(|b| b.as_str().to_string())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join("\u{1f}");
        groups.entry(key).or_default().push(row);
    }

    let mut out = Vec::new();
    for (_key, members) in groups {
        let mut sol = Solution::new();
        // Carry the group-by var values (taken from the first member of the group).
        if let Some(first) = members.first() {
            for gv in group_vars {
                if let Some(b) = first.get(gv.as_str()) {
                    sol.insert(gv.as_str().to_string(), b.clone());
                }
            }
        }
        // Compute each aggregate over the group's members.
        for (out_var, agg) in aggregates {
            let value = compute_aggregate(ctx, agg, &members);
            sol.insert(out_var.as_str().to_string(), Binding::Literal(value));
        }
        out.push(sol);
    }
    out
}

/// Compute ONE aggregate over a group's member solutions, returning its lexical value.
pub(super) fn compute_aggregate(
    ctx: &Ctx,
    agg: &AggregateExpression,
    members: &[Solution],
) -> String {
    match agg {
        // COUNT(*) — count solutions (DISTINCT counts distinct whole solutions).
        AggregateExpression::CountSolutions { distinct } => {
            let n = if *distinct {
                let mut seen = std::collections::HashSet::new();
                members
                    .iter()
                    .filter(|s| seen.insert(canonical_solution(s)))
                    .count()
            } else {
                members.len()
            };
            n.to_string()
        }
        AggregateExpression::FunctionCall {
            name,
            expr,
            distinct,
        } => {
            // The per-row values of the aggregated expression (skipping unbound rows).
            let mut vals: Vec<String> = members
                .iter()
                .filter_map(|s| expr_str(ctx, expr, s))
                .collect();
            if *distinct {
                let mut seen = std::collections::HashSet::new();
                vals.retain(|v| seen.insert(v.clone()));
            }
            agg_over(name, &vals)
        }
    }
}

/// Apply an aggregate function to the already-collected per-row lexical values.
pub(super) fn agg_over(func: &AggregateFunction, vals: &[String]) -> String {
    let nums: Vec<f64> = vals.iter().filter_map(|v| v.parse::<f64>().ok()).collect();
    match func {
        AggregateFunction::Count => vals.len().to_string(),
        AggregateFunction::Sum => fmt_num(nums.iter().sum::<f64>()),
        AggregateFunction::Avg => agg_avg(&nums),
        AggregateFunction::Min => agg_extreme(vals, &nums, Extreme::Min),
        AggregateFunction::Max => agg_extreme(vals, &nums, Extreme::Max),
        AggregateFunction::GroupConcat { separator } => {
            vals.join(separator.as_deref().unwrap_or(" "))
        }
        AggregateFunction::Sample => vals.first().cloned().unwrap_or_default(),
        AggregateFunction::Custom(_) => String::new(),
    }
}

/// AVG over the numeric-parseable values; `0` when none parsed.
pub(super) fn agg_avg(nums: &[f64]) -> String {
    if nums.is_empty() {
        "0".to_string()
    } else {
        fmt_num(nums.iter().sum::<f64>() / nums.len() as f64)
    }
}

/// Which end of the ordering a MIN/MAX aggregate takes.
#[derive(Clone, Copy)]
pub(super) enum Extreme {
    Min,
    Max,
}

/// MIN/MAX: the numeric extreme when any value parsed as a number, else the lexical
/// extreme of the raw values. `f64::min`/`f64::max` keep SPARQL's NaN-skipping fold.
pub(super) fn agg_extreme(vals: &[String], nums: &[f64], end: Extreme) -> String {
    if nums.is_empty() {
        match end {
            Extreme::Min => vals.iter().min(),
            Extreme::Max => vals.iter().max(),
        }
        .cloned()
        .unwrap_or_default()
    } else {
        let (seed, fold): (f64, fn(f64, f64) -> f64) = match end {
            Extreme::Min => (f64::INFINITY, f64::min),
            Extreme::Max => (f64::NEG_INFINITY, f64::max),
        };
        fmt_num(nums.iter().copied().fold(seed, fold))
    }
}

/// Format an f64 aggregate result without a trailing `.0` for integral values, so a
/// `SUM`/`COUNT` of integers reads as an integer (matching the stored lexical form).
pub(super) fn fmt_num(n: f64) -> String {
    if n.fract() == 0.0 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}
