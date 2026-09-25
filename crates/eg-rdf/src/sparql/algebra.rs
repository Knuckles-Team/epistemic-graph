use super::*;

pub(super) fn collect_vars(p: &GraphPattern) -> Vec<String> {
    let mut vs = Vec::new();
    p.on_in_scope_variable(|v| {
        let n = v.as_str().to_string();
        if !vs.contains(&n) {
            vs.push(n);
        }
    });
    vs
}

/// The algebra walker.
pub(super) fn eval_pattern(ctx: &Ctx, p: &GraphPattern) -> Result<Vec<Solution>, String> {
    // Each arm below delegates to a same-named `eval_pattern_*` helper that owns that
    // operator's logic; this keeps the dispatcher itself a flat, low-complexity match
    // over every `GraphPattern` variant present in this build (the `Lateral` node is
    // gated behind spargebra's `sep-0006` feature, which we do not enable — no
    // catch-all needed).
    match p {
        GraphPattern::Bgp { patterns } => Ok(eval_bgp(ctx, patterns)),
        GraphPattern::Path {
            subject,
            path,
            object,
        } => eval_path(ctx, subject, path, object),
        GraphPattern::Filter { expr, inner } => eval_pattern_filter(ctx, expr, inner),
        GraphPattern::Join { left, right } => eval_pattern_join(ctx, left, right),
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => eval_pattern_left_join(ctx, left, right, expression.as_ref()),
        GraphPattern::Union { left, right } => eval_pattern_union(ctx, left, right),
        GraphPattern::Project { inner, variables } => eval_pattern_project(ctx, inner, variables),
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => eval_pattern_group(ctx, inner, variables, aggregates),
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => eval_pattern_extend(ctx, inner, variable, expression),
        GraphPattern::Graph { name, inner } => eval_pattern_graph(ctx, name, inner),
        GraphPattern::Distinct { inner } => eval_pattern_distinct(ctx, inner),
        GraphPattern::Reduced { inner } => eval_pattern(ctx, inner),
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => eval_pattern_slice(ctx, inner, *start, *length),
        GraphPattern::Minus { left, right } => eval_pattern_minus(ctx, left, right),
        GraphPattern::OrderBy { inner, expression } => {
            eval_pattern_order_by(ctx, inner, expression)
        }
        GraphPattern::Values {
            variables,
            bindings,
        } => Ok(values_solutions(variables, bindings)),
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => eval_service(ctx, name, inner, *silent),
    }
}

/// FILTER: keep only the inner solutions for which `expr` evaluates truthy.
pub(super) fn eval_pattern_filter(
    ctx: &Ctx,
    expr: &Expression,
    inner: &GraphPattern,
) -> Result<Vec<Solution>, String> {
    let inner_sols = eval_pattern(ctx, inner)?;
    Ok(inner_sols
        .into_iter()
        .filter(|s| eval_filter(ctx, expr, s))
        .collect())
}

/// JOIN: inner (hash) join of the two sides' solutions.
pub(super) fn eval_pattern_join(
    ctx: &Ctx,
    left: &GraphPattern,
    right: &GraphPattern,
) -> Result<Vec<Solution>, String> {
    let l = eval_pattern(ctx, left)?;
    let r = eval_pattern(ctx, right)?;
    Ok(hash_join(&l, &r))
}

/// OPTIONAL: keep every left solution; extend with a compatible right (passing the
/// optional FILTER) where one exists.
pub(super) fn eval_pattern_left_join(
    ctx: &Ctx,
    left: &GraphPattern,
    right: &GraphPattern,
    expression: Option<&Expression>,
) -> Result<Vec<Solution>, String> {
    let l = eval_pattern(ctx, left)?;
    let r = eval_pattern(ctx, right)?;
    Ok(left_join(ctx, &l, &r, expression))
}

/// UNION: the concatenation of both sides' solutions.
pub(super) fn eval_pattern_union(
    ctx: &Ctx,
    left: &GraphPattern,
    right: &GraphPattern,
) -> Result<Vec<Solution>, String> {
    let mut l = eval_pattern(ctx, left)?;
    let mut r = eval_pattern(ctx, right)?;
    l.append(&mut r);
    Ok(l)
}

/// Sub-SELECT (CONCEPT:EG-KG.ontology.sub-select): evaluate the inner pattern, then RESTRICT each
/// solution to the projected `variables` so inner-only bindings can't leak out and
/// corrupt an outer join. Top-level SELECT output is unchanged — the result columns
/// already derive from the projected set — so this is a pure correctness fix that
/// makes a nested `{ SELECT … }` join on its projected vars only.
pub(super) fn eval_pattern_project(
    ctx: &Ctx,
    inner: &GraphPattern,
    variables: &[Variable],
) -> Result<Vec<Solution>, String> {
    let projected: std::collections::HashSet<&str> = variables.iter().map(|v| v.as_str()).collect();
    Ok(eval_pattern(ctx, inner)?
        .into_iter()
        .map(|s| {
            s.into_iter()
                .filter(|(k, _)| projected.contains(k.as_str()))
                .collect()
        })
        .collect())
}

/// GROUP BY + aggregates (CONCEPT:EG-KG.query.sparql-completeness). `Group` produces one solution per
/// group binding the GROUP BY vars + the aggregate-result vars; the wrapping
/// `Extend` (below) re-binds those to the projected names. With no GROUP BY var
/// the whole result is one group (`SELECT (COUNT(*) AS ?n) …`).
pub(super) fn eval_pattern_group(
    ctx: &Ctx,
    inner: &GraphPattern,
    variables: &[Variable],
    aggregates: &[(Variable, AggregateExpression)],
) -> Result<Vec<Solution>, String> {
    let rows = eval_pattern(ctx, inner)?;
    Ok(eval_group(ctx, rows, variables, aggregates))
}

/// BIND / the aggregate-projection rename. `Extend` binds `variable` to the value of
/// `expression` in each solution. We evaluate the (already-aggregated or scalar)
/// expression and bind it; an unevaluable expression leaves it unbound (SPARQL: an
/// error in Extend yields no binding for that var).
pub(super) fn eval_pattern_extend(
    ctx: &Ctx,
    inner: &GraphPattern,
    variable: &Variable,
    expression: &Expression,
) -> Result<Vec<Solution>, String> {
    let rows = eval_pattern(ctx, inner)?;
    Ok(rows
        .into_iter()
        .map(|mut s| {
            if let Some(val) = expr_str(ctx, expression, &s) {
                s.insert(variable.as_str().to_string(), Binding::Literal(val));
            }
            s
        })
        .collect())
}

/// GRAPH … { … } — true named-graph scoping (CONCEPT:EG-KG.query.named-graph-support). A constant graph
/// IRI re-scopes evaluation to THAT named graph (empty if it is not in the dataset). A
/// variable `?g` ranges over EVERY named graph, evaluating the inner pattern against
/// each and binding `?g` to its IRI (the union).
pub(super) fn eval_pattern_graph(
    ctx: &Ctx,
    name: &NamedNodePattern,
    inner: &GraphPattern,
) -> Result<Vec<Solution>, String> {
    match name {
        NamedNodePattern::NamedNode(n) => match ctx.ds.named_view(n.as_str()) {
            Some(v) => eval_pattern(&ctx.with_active(v), inner),
            None => Ok(Vec::new()),
        },
        NamedNodePattern::Variable(v) => eval_pattern_graph_var(ctx, v, inner),
    }
}

/// The `GRAPH ?g { … }` arm of [`eval_pattern_graph`]: union the inner pattern over
/// every named graph, binding `?g` to each graph's IRI.
pub(super) fn eval_pattern_graph_var(
    ctx: &Ctx,
    v: &Variable,
    inner: &GraphPattern,
) -> Result<Vec<Solution>, String> {
    let mut out = Vec::new();
    for (gname, gview) in &ctx.ds.named {
        let binding = Binding::Node(format!("<{gname}>"));
        for mut s in eval_pattern(&ctx.with_active(gview), inner)? {
            match s.get(v.as_str()) {
                Some(existing) if *existing != binding => continue,
                _ => {
                    s.insert(v.as_str().to_string(), binding.clone());
                }
            }
            out.push(s);
        }
    }
    Ok(out)
}

/// DISTINCT: drop solutions whose canonical form has already been seen.
pub(super) fn eval_pattern_distinct(
    ctx: &Ctx,
    inner: &GraphPattern,
) -> Result<Vec<Solution>, String> {
    let mut seen = std::collections::HashSet::new();
    Ok(eval_pattern(ctx, inner)?
        .into_iter()
        .filter(|s| seen.insert(canonical_solution(s)))
        .collect())
}

/// LIMIT/OFFSET: slice the inner solutions to `[start, start + length)`.
pub(super) fn eval_pattern_slice(
    ctx: &Ctx,
    inner: &GraphPattern,
    start: usize,
    length: Option<usize>,
) -> Result<Vec<Solution>, String> {
    let all = eval_pattern(ctx, inner)?;
    let end = length.map(|l| start + l).unwrap_or(all.len());
    Ok(all
        .into_iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect())
}

/// MINUS (CONCEPT:EG-KG.ontology.minus): set-difference. Keep each LEFT solution that is NOT
/// compatible with ANY right solution. SPARQL MINUS compatibility is agreement on
/// the SHARED bound variables; a left solution whose domain is DISJOINT from a
/// right solution is NOT removed by it (so a right pattern sharing no variable
/// never deletes anything).
pub(super) fn eval_pattern_minus(
    ctx: &Ctx,
    left: &GraphPattern,
    right: &GraphPattern,
) -> Result<Vec<Solution>, String> {
    let l = eval_pattern(ctx, left)?;
    let r = eval_pattern(ctx, right)?;
    Ok(l.into_iter()
        .filter(|ls| !r.iter().any(|rs| minus_compatible(ls, rs)))
        .collect())
}

/// ORDER BY (CONCEPT:EG-KG.ontology.order-by-values-exists): a CORRECTNESS fix — the evaluator previously hit the
/// catch-all and errored, so ordered queries never returned in order. Evaluate the
/// inner pattern, then STABLE-sort its solutions by the `OrderExpression` list.
pub(super) fn eval_pattern_order_by(
    ctx: &Ctx,
    inner: &GraphPattern,
    expression: &[OrderExpression],
) -> Result<Vec<Solution>, String> {
    let mut sols = eval_pattern(ctx, inner)?;
    sort_solutions(ctx, &mut sols, expression);
    Ok(sols)
}

/// Evaluate a `SERVICE <ep> { inner }` clause (CONCEPT:EG-KG.query.sparql-service-federation-client) by delegating `inner` to a
/// remote SPARQL endpoint through `ctx.service`.
///
/// SILENT semantics: on ANY failure — a variable endpoint, no bound client, or a remote
/// HTTP/parse error — `silent` returns ONE empty solution (the join identity, so the
/// enclosing join passes the local side through unchanged); otherwise the error propagates.
/// Only a CONSTANT-IRI endpoint is supported (a `?var` endpoint is a failure per the rule).
pub(super) fn eval_service(
    ctx: &Ctx,
    name: &NamedNodePattern,
    inner: &GraphPattern,
    silent: bool,
) -> Result<Vec<Solution>, String> {
    // One empty solution = the neutral element for a join (pass-through under SILENT).
    let hushed = |e: String| -> Result<Vec<Solution>, String> {
        if silent {
            Ok(vec![Solution::new()])
        } else {
            Err(e)
        }
    };
    let endpoint = match name {
        NamedNodePattern::NamedNode(n) => n.as_str(),
        // A variable endpoint (`SERVICE ?ep { … }`) is unsupported: it requires binding the
        // endpoint from an earlier pattern, which this evaluator does not resolve.
        NamedNodePattern::Variable(_) => {
            return hushed("eg-rdf SPARQL: SERVICE with a variable endpoint is unsupported".into());
        }
    };
    let client = match ctx.service {
        Some(c) => c,
        // Fail-closed: no client bound (feature off / allowlist empty) ⇒ SERVICE is disabled.
        None => {
            return hushed(format!(
                "eg-rdf SPARQL: SERVICE <{endpoint}> requires a remote client (feature `sparql-service`); none bound"
            ));
        }
    };
    let remote_query = build_service_query(inner);
    match client.select(endpoint, &remote_query) {
        Ok(res) => Ok(res.solutions),
        Err(e) => hushed(format!("eg-rdf SPARQL: SERVICE <{endpoint}> failed: {e}")),
    }
}

/// Build the SPARQL SELECT text sent to a remote SERVICE endpoint (CONCEPT:EG-KG.query.sparql-service-federation-client): wrap
/// `inner` in a `SELECT` projecting its in-scope variables and render it with spargebra's
/// `Display` (which emits valid SPARQL 1.1). The projected vars are what the enclosing join
/// binds on, so the remote side returns exactly the columns the local pattern needs.
pub(super) fn build_service_query(inner: &GraphPattern) -> String {
    let mut variables: Vec<Variable> = Vec::new();
    inner.on_in_scope_variable(|v| {
        if !variables.contains(v) {
            variables.push(v.clone());
        }
    });
    let pattern = GraphPattern::Project {
        inner: Box::new(inner.clone()),
        variables,
    };
    Query::Select {
        dataset: None,
        pattern,
        base_iri: None,
    }
    .to_string()
}

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
