//! Solution ordering, grouping, and aggregation.

use super::*;

/// Stable-sort solutions by an `ORDER BY` comparator list (CONCEPT:EG-KG.ontology.order-by-values-exists). Each
/// `OrderExpression` is Asc/Desc over an expression; solutions compare on the first
/// expression that distinguishes them (numeric when both sides parse as numbers, else
/// lexical). An UNBOUND/error value sorts FIRST in ascending order (SPARQL orders the
/// unbound below every bound value), and Desc simply reverses that comparator.
pub(super) fn sort_solutions(ctx: &Ctx, sols: &mut [Solution], order: &[OrderExpression]) {
    sols.sort_by(|a, b| {
        for oe in order {
            let (expr, desc) = match oe {
                OrderExpression::Asc(e) => (e, false),
                OrderExpression::Desc(e) => (e, true),
            };
            let ord = cmp_binding(&eval_term(ctx, expr, a), &eval_term(ctx, expr, b));
            let ord = if desc { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// The SPARQL `ORDER BY` term-type precedence rank (CONCEPT:EG-KG.ontology.completing-eg-order-by). The spec fixes a
/// total order ACROSS term kinds — an unbound value sorts before any bound value, then
/// blank nodes, then IRIs, then literals — and only compares *values* within the same
/// kind. Prior to EG-135 the comparator ignored the kind and compared every bound value
/// by its lexical string, so a query ordering over MIXED IRI/literal (or blank/IRI)
/// columns came back in the wrong group order. Ranks: unbound(0) < blank(1) < IRI(2) <
/// literal(3).
pub(super) fn order_rank(b: &Option<Binding>) -> u8 {
    match b {
        None => 0,
        Some(Binding::Node(s)) if s.starts_with("_:") => 1,
        Some(Binding::Node(_)) => 2, // an `<iri>` node
        Some(Binding::Literal(_)) => 3,
    }
}

/// Compare two (possibly unbound) `ORDER BY` values under the full SPARQL term ordering
/// (CONCEPT:EG-KG.ontology.completing-eg-order-by, completing the EG-125 ORDER BY arm). Terms first order by KIND
/// ([`order_rank`]: unbound < blank node < IRI < literal); only within the SAME kind do
/// values compare — blank/IRI lexically by term id, and literals by a typed comparison:
/// numerically when both lexical forms parse as numbers, else lexically (xsd:dateTime /
/// xsd:date ISO-8601 lexicals already sort chronologically under a lexical compare for a
/// shared timezone, and plain strings compare by code point).
pub(super) fn cmp_binding(a: &Option<Binding>, b: &Option<Binding>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    // Cross-kind: the type precedence decides it outright.
    let (ra, rb) = (order_rank(a), order_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        // Same rank ⇒ both unbound, both nodes, or both literals.
        (None, None) => Ordering::Equal,
        (Some(x), Some(y)) => {
            let (xs, ys) = (x.as_str(), y.as_str());
            // Typed value compare only applies to literals; nodes (same kind) order by id.
            if matches!((x, y), (Binding::Literal(_), Binding::Literal(_))) {
                match (xs.parse::<f64>(), ys.parse::<f64>()) {
                    (Ok(nx), Ok(ny)) => nx.partial_cmp(&ny).unwrap_or(Ordering::Equal),
                    _ => xs.cmp(ys),
                }
            } else {
                xs.cmp(ys)
            }
        }
        // Unreachable: differing ranks were handled above.
        _ => Ordering::Equal,
    }
}

/// Turn an inline `VALUES` table into solutions (CONCEPT:EG-KG.ontology.order-by-values-exists): one solution per row,
/// binding each variable to its ground term; an `UNDEF` cell (`None`) leaves that
/// variable unbound in that row.
pub(super) fn values_solutions(
    variables: &[Variable],
    bindings: &[Vec<Option<GroundTerm>>],
) -> Vec<Solution> {
    bindings
        .iter()
        .map(|row| {
            let mut sol = Solution::new();
            for (var, cell) in variables.iter().zip(row) {
                if let Some(gt) = cell {
                    sol.insert(var.as_str().to_string(), ground_term_binding(gt));
                }
            }
            sol
        })
        .collect()
}

/// A `VALUES` ground term → a solution binding (CONCEPT:EG-KG.ontology.order-by-values-exists): an IRI becomes a `Node`
/// (`<iri>`), a literal its lexical `Literal` value (matching how the BGP matcher binds).
pub(super) fn ground_term_binding(gt: &GroundTerm) -> Binding {
    match gt {
        GroundTerm::NamedNode(n) => Binding::Node(format!("<{}>", n.as_str())),
        GroundTerm::Literal(l) => Binding::Literal(l.value().to_string()),
        #[allow(unreachable_patterns)]
        _ => Binding::Literal(String::new()),
    }
}

/// SPARQL MINUS compatibility (CONCEPT:EG-KG.ontology.minus): `l` and `r` are compatible iff they
/// agree on every variable bound in BOTH and share at least one such variable. A right
/// solution with a disjoint domain returns `false`, so it never removes a left solution.
pub(super) fn minus_compatible(l: &Solution, r: &Solution) -> bool {
    let mut shared = false;
    for (k, v) in l {
        if let Some(rv) = r.get(k) {
            shared = true;
            if rv != v {
                return false;
            }
        }
    }
    shared
}

pub(super) fn canonical_solution(s: &Solution) -> String {
    let mut kv: Vec<_> = s
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().to_string()))
        .collect();
    kv.sort();
    format!("{kv:?}")
}

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
enum Extreme {
    Min,
    Max,
}

/// MIN/MAX: the numeric extreme when any value parsed as a number, else the lexical
/// extreme of the raw values. `f64::min`/`f64::max` keep SPARQL's NaN-skipping fold.
fn agg_extreme(vals: &[String], nums: &[f64], end: Extreme) -> String {
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
