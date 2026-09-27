//! Algebra operators and remote SERVICE evaluation.

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
    let inner_sols = eval_filter_input(ctx, expr, inner)?;
    Ok(inner_sols
        .into_iter()
        .filter(|s| eval_filter(ctx, expr, s))
        .collect())
}

/// Push a filter into a remote SERVICE when supported, then apply the filter
/// locally as well so a remote endpoint cannot weaken the predicate.
fn eval_filter_input(
    ctx: &Ctx,
    expr: &Expression,
    inner: &GraphPattern,
) -> Result<Vec<Solution>, String> {
    match inner {
        GraphPattern::Service {
            name,
            inner: remote,
            silent,
        } => {
            let pushed = GraphPattern::Filter {
                expr: expr.clone(),
                inner: remote.clone(),
            };
            eval_service(ctx, name, &pushed, *silent)
                .or_else(|_| eval_service(ctx, name, remote, *silent))
        }
        _ => eval_pattern(ctx, inner),
    }
}

/// JOIN: inner (hash) join of the two sides' solutions.
pub(super) fn eval_pattern_join(
    ctx: &Ctx,
    left: &GraphPattern,
    right: &GraphPattern,
) -> Result<Vec<Solution>, String> {
    let l = eval_pattern(ctx, left)?;
    eval_join_right(ctx, &l, right)
}

fn eval_join_right(
    ctx: &Ctx,
    local: &[Solution],
    right: &GraphPattern,
) -> Result<Vec<Solution>, String> {
    match right {
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => eval_service_bind_join(ctx, local, name, inner, *silent),
        _ => Ok(hash_join(local, &eval_pattern(ctx, right)?)),
    }
}

/// Ship distinct local join keys in bounded VALUES blocks. A remote endpoint that
/// cannot evaluate VALUES falls back to the original full-fetch join, preserving
/// compatibility with endpoints whose supported SPARQL subset is unknown.
pub(super) fn eval_service_bind_join(
    ctx: &Ctx,
    local: &[Solution],
    name: &NamedNodePattern,
    inner: &GraphPattern,
    silent: bool,
) -> Result<Vec<Solution>, String> {
    if local.is_empty() {
        return Ok(Vec::new());
    }
    let remote = service_bind_keys(local, inner)
        .and_then(|(keys, rows)| fetch_service_batches(ctx, name, inner, &keys, &rows));
    match remote {
        Some(rows) => Ok(hash_join(local, &rows)),
        None => Ok(hash_join(local, &eval_service(ctx, name, inner, silent)?)),
    }
}

/// Binding is safe only when every local row has every shared key and the
/// key terms can be represented without losing RDF datatype or language.
fn service_bind_keys(
    local: &[Solution],
    inner: &GraphPattern,
) -> Option<(Vec<String>, Vec<Vec<Option<GroundTerm>>>)> {
    let mut keys: Vec<_> = collect_vars(inner)
        .into_iter()
        .filter(|key| local.iter().any(|row| row.contains_key(key)))
        .collect();
    keys.sort();
    if keys.is_empty()
        || local
            .iter()
            .any(|row| keys.iter().any(|key| !row.contains_key(key)))
    {
        return None;
    }
    let rows = service_values_rows(local, &keys)?;
    Some((keys, rows))
}

/// A failed batch invalidates the partial result, so the caller falls back to a
/// single unscoped SERVICE evaluation instead of joining incomplete remote rows.
fn fetch_service_batches(
    ctx: &Ctx,
    name: &NamedNodePattern,
    inner: &GraphPattern,
    keys: &[String],
    rows: &[Vec<Option<GroundTerm>>],
) -> Option<Vec<Solution>> {
    let NamedNodePattern::NamedNode(endpoint) = name else {
        return None;
    };
    let client = ctx.service?;
    let variables: Vec<Variable> = keys
        .iter()
        .map(|key| Variable::new(key).ok())
        .collect::<Option<_>>()?;
    let mut remote = Vec::new();
    for batch in rows.chunks(100) {
        let scoped = GraphPattern::Join {
            left: Box::new(inner.clone()),
            right: Box::new(GraphPattern::Values {
                variables: variables.clone(),
                bindings: batch.to_vec(),
            }),
        };
        let result = client
            .select(endpoint.as_str(), &build_service_query(&scoped))
            .ok()?;
        remote.extend(result.solutions);
    }
    Some(remote)
}

/// Convert only losslessly representable keys. The evaluator stores literal
/// lexical values without datatype/language metadata, so sending them as plain
/// literals would make typed/lang-tagged remote matches disappear. Blank nodes
/// cannot be sent as VALUES ground terms either. Both use full fetch.
pub(super) fn service_values_rows(
    local: &[Solution],
    keys: &[String],
) -> Option<Vec<Vec<Option<GroundTerm>>>> {
    use spargebra::term::NamedNode;
    let mut seen = std::collections::HashSet::new();
    let mut rows = Vec::new();
    for row in local {
        let terms: Vec<_> = keys
            .iter()
            .map(|key| match row.get(key)? {
                Binding::Node(node) => {
                    let iri = node.strip_prefix('<')?.strip_suffix('>')?;
                    Some(GroundTerm::NamedNode(NamedNode::new(iri).ok()?))
                }
                Binding::Literal(_) => None,
            })
            .collect::<Option<_>>()?;
        let signature: Vec<String> = terms.iter().map(ToString::to_string).collect();
        if seen.insert(signature) {
            rows.push(terms.into_iter().map(Some).collect());
        }
    }
    Some(rows)
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
    Ok(project_solutions(eval_pattern(ctx, inner)?, variables))
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
    let all = eval_slice_input(ctx, inner, start, length)?;
    let end = length.map(|l| start + l).unwrap_or(all.len());
    Ok(all
        .into_iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect())
}

/// A leading LIMIT can be pushed through a projection to a SERVICE. Other
/// slices retain local ordering and evaluation semantics.
fn eval_slice_input(
    ctx: &Ctx,
    inner: &GraphPattern,
    start: usize,
    length: Option<usize>,
) -> Result<Vec<Solution>, String> {
    match (start, length) {
        (0, Some(limit)) => match inner {
            GraphPattern::Service { .. } => eval_service_limited(ctx, inner, limit),
            GraphPattern::Project {
                inner: service,
                variables,
            } if matches!(service.as_ref(), GraphPattern::Service { .. }) => {
                let rows = eval_service_limited(ctx, service, limit)?;
                Ok(project_solutions(rows, variables))
            }
            _ => eval_pattern(ctx, inner),
        },
        _ => eval_pattern(ctx, inner),
    }
}

pub(super) fn eval_service_limited(
    ctx: &Ctx,
    pattern: &GraphPattern,
    limit: usize,
) -> Result<Vec<Solution>, String> {
    let GraphPattern::Service {
        name,
        inner,
        silent,
    } = pattern
    else {
        return eval_pattern(ctx, pattern);
    };
    let pushed = GraphPattern::Slice {
        inner: inner.clone(),
        start: 0,
        length: Some(limit),
    };
    eval_service(ctx, name, &pushed, *silent).or_else(|_| eval_service(ctx, name, inner, *silent))
}

pub(super) fn project_solutions(rows: Vec<Solution>, variables: &[Variable]) -> Vec<Solution> {
    let projected: std::collections::HashSet<&str> = variables.iter().map(|v| v.as_str()).collect();
    rows.into_iter()
        .map(|s| {
            s.into_iter()
                .filter(|(k, _)| projected.contains(k.as_str()))
                .collect()
        })
        .collect()
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
            return hushed("eg-rdf SPARQL: SERVICE requires a remote client; none bound".into());
        }
    };
    let remote_query = build_service_query(inner);
    match client.select(endpoint, &remote_query) {
        Ok(res) => Ok(res.solutions),
        Err(e) => hushed(format!("eg-rdf SPARQL: SERVICE failed: {e}")),
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
