use eg_rdf::oxrdf::{Graph, NamedNode, NamedOrBlankNodeRef, Term};
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};

use super::expression::{eval_filter, eval_term};
use super::{Ctx, Solution};

/// Evaluate a WHERE-clause graph pattern, threading pre-bound variables into
/// every base case.
pub(super) fn eval_pattern(
    ctx: &Ctx,
    pattern: &GraphPattern,
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    match pattern {
        GraphPattern::Bgp { .. }
        | GraphPattern::Join { .. }
        | GraphPattern::LeftJoin { .. }
        | GraphPattern::Filter { .. }
        | GraphPattern::Union { .. } => eval_pattern_basic(ctx, pattern, init),
        GraphPattern::Graph { .. }
        | GraphPattern::Extend { .. }
        | GraphPattern::Project { .. }
        | GraphPattern::Distinct { .. }
        | GraphPattern::Reduced { .. }
        | GraphPattern::OrderBy { .. }
        | GraphPattern::Slice { .. } => eval_pattern_transform(ctx, pattern, init),
        GraphPattern::Path { .. }
        | GraphPattern::Minus { .. }
        | GraphPattern::Values { .. }
        | GraphPattern::Group { .. }
        | GraphPattern::Service { .. } => eval_pattern_unsupported(pattern),
    }
}

fn eval_pattern_basic(
    ctx: &Ctx,
    pattern: &GraphPattern,
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    match pattern {
        GraphPattern::Bgp { patterns } => eval_bgp(ctx, patterns, init),
        GraphPattern::Join { left, right } => eval_join(ctx, left, right, init),
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => eval_left_join(ctx, left, right, expression.as_ref(), init),
        GraphPattern::Filter { expr, inner } => eval_filter_pattern(ctx, expr, inner, init),
        GraphPattern::Union { left, right } => eval_union(ctx, left, right, init),
        _ => Err("sh:sparql: invalid basic pattern dispatch".to_string()),
    }
}

fn eval_bgp(
    ctx: &Ctx,
    patterns: &[TriplePattern],
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    let mut accumulated = vec![init.clone()];
    for pattern in patterns {
        let matches = match_triple_pattern(ctx.active, pattern);
        accumulated = hash_join(&accumulated, &matches);
    }
    Ok(accumulated)
}

fn eval_join(
    ctx: &Ctx,
    left: &GraphPattern,
    right: &GraphPattern,
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    let left_rows = eval_pattern(ctx, left, init)?;
    let right_rows = eval_pattern(ctx, right, init)?;
    Ok(hash_join(&left_rows, &right_rows))
}

fn eval_left_join(
    ctx: &Ctx,
    left: &GraphPattern,
    right: &GraphPattern,
    expression: Option<&Expression>,
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    let left_rows = eval_pattern(ctx, left, init)?;
    let right_rows = eval_pattern(ctx, right, init)?;
    let mut output = Vec::new();
    for left_row in &left_rows {
        let mut matched = false;
        for right_row in &right_rows {
            let Some(merged) = merge(left_row, right_row) else {
                continue;
            };
            if optional_match(ctx, expression, &merged)? {
                matched = true;
                output.push(merged);
            }
        }
        if !matched {
            output.push(left_row.clone());
        }
    }
    Ok(output)
}

fn optional_match(
    ctx: &Ctx,
    expression: Option<&Expression>,
    solution: &Solution,
) -> Result<bool, String> {
    match expression {
        Some(expression) => eval_filter(ctx, expression, solution),
        None => Ok(true),
    }
}

fn eval_filter_pattern(
    ctx: &Ctx,
    expression: &Expression,
    inner: &GraphPattern,
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    let rows = eval_pattern(ctx, inner, init)?;
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        if eval_filter(ctx, expression, &row)? {
            output.push(row);
        }
    }
    Ok(output)
}

fn eval_union(
    ctx: &Ctx,
    left: &GraphPattern,
    right: &GraphPattern,
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    let mut left_rows = eval_pattern(ctx, left, init)?;
    let mut right_rows = eval_pattern(ctx, right, init)?;
    left_rows.append(&mut right_rows);
    Ok(left_rows)
}

fn eval_pattern_transform(
    ctx: &Ctx,
    pattern: &GraphPattern,
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    match pattern {
        GraphPattern::Graph { name, inner } => eval_graph(ctx, name, inner, init),
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => eval_extend(ctx, inner, variable, expression, init),
        GraphPattern::Project { inner, variables } => eval_project(ctx, inner, variables, init),
        GraphPattern::Distinct { inner } => {
            let rows = eval_pattern(ctx, inner, init)?;
            Ok(dedup(rows))
        }
        GraphPattern::Reduced { inner } | GraphPattern::OrderBy { inner, .. } => {
            eval_pattern(ctx, inner, init)
        }
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => eval_slice(ctx, inner, *start, *length, init),
        _ => Err("sh:sparql: invalid transform pattern dispatch".to_string()),
    }
}

fn eval_extend(
    ctx: &Ctx,
    inner: &GraphPattern,
    variable: &spargebra::term::Variable,
    expression: &Expression,
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    let name = variable.as_str();
    if super::is_protected_var(name) {
        return Err(format!(
            "sh:sparql: BIND/AS may not rebind the pre-bound variable ?{name}"
        ));
    }
    let rows = eval_pattern(ctx, inner, init)?;
    let mut output = Vec::with_capacity(rows.len());
    for mut row in rows {
        if let Some(value) = eval_term(ctx, expression, &row)? {
            row.insert(name.to_string(), value);
        }
        output.push(row);
    }
    Ok(output)
}

fn eval_project(
    ctx: &Ctx,
    inner: &GraphPattern,
    variables: &[spargebra::term::Variable],
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    let rows = eval_pattern(ctx, inner, init)?;
    let keep: Vec<&str> = variables.iter().map(|variable| variable.as_str()).collect();
    Ok(rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .filter(|(key, _)| keep.contains(&key.as_str()))
                .collect()
        })
        .collect())
}

fn eval_slice(
    ctx: &Ctx,
    inner: &GraphPattern,
    start: usize,
    length: Option<usize>,
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    let rows = eval_pattern(ctx, inner, init)?;
    let iter = rows.into_iter().skip(start);
    Ok(match length {
        Some(length) => iter.take(length).collect(),
        None => iter.collect(),
    })
}

fn eval_pattern_unsupported(pattern: &GraphPattern) -> Result<Vec<Solution>, String> {
    match pattern {
        GraphPattern::Path { .. } => {
            Err("sh:sparql: SPARQL property-path triples are not supported".to_string())
        }
        GraphPattern::Minus { .. } => Err("sh:sparql: MINUS is not supported".to_string()),
        GraphPattern::Values { .. } => Err("sh:sparql: VALUES is not supported".to_string()),
        GraphPattern::Group { .. } => {
            Err("sh:sparql: GROUP BY / aggregates are not supported".to_string())
        }
        GraphPattern::Service { silent: true, .. } => Ok(Vec::new()),
        GraphPattern::Service { .. } => Err("sh:sparql: SERVICE is not supported".to_string()),
        _ => Err("sh:sparql: this SPARQL construct is not supported".to_string()),
    }
}

/// `GRAPH <name> { inner }` over the evaluator's one named shapes graph.
fn eval_graph(
    ctx: &Ctx,
    name: &NamedNodePattern,
    inner: &GraphPattern,
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    let variable = match name {
        NamedNodePattern::NamedNode(node) => {
            return if Term::NamedNode(node.clone()) == *ctx.shapes_graph_term {
                eval_pattern(&ctx.with_active(ctx.shapes), inner, init)
            } else {
                Ok(Vec::new())
            };
        }
        NamedNodePattern::Variable(variable) => variable,
    };
    if let Some(bound) = init.get(variable.as_str()) {
        if bound != ctx.shapes_graph_term {
            return Ok(Vec::new());
        }
    }
    let rows = eval_pattern(&ctx.with_active(ctx.shapes), inner, init)?;
    Ok(rows
        .into_iter()
        .map(|mut row| {
            row.insert(variable.as_str().to_string(), ctx.shapes_graph_term.clone());
            row
        })
        .collect())
}

fn dedup(rows: Vec<Solution>) -> Vec<Solution> {
    let mut output: Vec<Solution> = Vec::with_capacity(rows.len());
    for row in rows {
        if !output.contains(&row) {
            output.push(row);
        }
    }
    output
}

/// Every graph triple whose shape matches `pattern`.
fn match_triple_pattern(graph: &Graph, pattern: &TriplePattern) -> Vec<Solution> {
    let mut output = Vec::new();
    for triple in graph.iter() {
        let mut solution = Solution::new();
        let subject: Term = match triple.subject {
            NamedOrBlankNodeRef::NamedNode(node) => Term::NamedNode(node.into_owned()),
            NamedOrBlankNodeRef::BlankNode(blank) => Term::BlankNode(blank.into_owned()),
        };
        if !bind_term(&pattern.subject, &subject, &mut solution) {
            continue;
        }
        if !bind_pred(
            &pattern.predicate,
            triple.predicate.into_owned(),
            &mut solution,
        ) {
            continue;
        }
        if !bind_term(&pattern.object, &triple.object.into_owned(), &mut solution) {
            continue;
        }
        output.push(solution);
    }
    output
}

fn bind_term(pattern: &TermPattern, actual: &Term, solution: &mut Solution) -> bool {
    if let TermPattern::Variable(variable) = pattern {
        bind_var(variable.as_str(), actual, solution)
    } else if let TermPattern::BlankNode(blank) = pattern {
        bind_var(&format!("__bnode_{}", blank.as_str()), actual, solution)
    } else {
        matches!(
            (pattern, actual),
            (TermPattern::NamedNode(node), Term::NamedNode(value)) if value == node
        ) || matches!(
            (pattern, actual),
            (TermPattern::Literal(literal), Term::Literal(value)) if value == literal
        )
    }
}

fn bind_pred(pattern: &NamedNodePattern, actual: NamedNode, solution: &mut Solution) -> bool {
    match pattern {
        NamedNodePattern::NamedNode(node) => &actual == node,
        NamedNodePattern::Variable(variable) => {
            bind_var(variable.as_str(), &Term::NamedNode(actual), solution)
        }
    }
}

fn bind_var(name: &str, actual: &Term, solution: &mut Solution) -> bool {
    match solution.get(name) {
        Some(existing) => existing == actual,
        None => {
            solution.insert(name.to_string(), actual.clone());
            true
        }
    }
}

fn merge(left: &Solution, right: &Solution) -> Option<Solution> {
    let mut output = left.clone();
    for (key, value) in right {
        match output.get(key) {
            Some(existing) if existing != value => return None,
            _ => {
                output.insert(key.clone(), value.clone());
            }
        }
    }
    Some(output)
}

fn hash_join(left: &[Solution], right: &[Solution]) -> Vec<Solution> {
    let mut output = Vec::new();
    for left_row in left {
        for right_row in right {
            if let Some(merged) = merge(left_row, right_row) {
                output.push(merged);
            }
        }
    }
    output
}
