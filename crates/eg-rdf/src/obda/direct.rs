//! Direct OBDA solutions for proven single-triple SQL shapes (EH-573 FO-08).

use super::*;

pub(super) fn try_direct_outcome(
    vg: &VirtualGraph,
    reg: &ObdaSourceRegistry,
    query: &spargebra::Query,
    proj: &Projection,
) -> Result<Option<QueryOutcome>, String> {
    if let Some(outcome) = try_direct_count(vg, reg, query, proj)? {
        return Ok(Some(outcome));
    }
    try_direct_ordered_select(vg, reg, query, proj)
}

/// COUNT(*) over one mapped triple can be answered from DISTINCT source rows:
/// the RDF graph stores one triple per unique (subject, object) pair. No GROUP BY,
/// FILTER, or alternate map may participate. This path does not create a graph.
fn try_direct_count(
    vg: &VirtualGraph,
    reg: &ObdaSourceRegistry,
    query: &spargebra::Query,
    proj: &Projection,
) -> Result<Option<QueryOutcome>, String> {
    if proj.base_iri.is_some() {
        return Ok(None);
    }
    let Some((group_input, out_var)) = count_group_shape(query) else {
        return Ok(None);
    };
    let Some(predicate) = single_triple_predicate(group_input) else {
        return Ok(None);
    };
    let Some((map, key, object_column, _)) = direct_map_columns(vg, predicate) else {
        return Ok(None);
    };
    let needed = BTreeSet::from([key.to_string(), object_column.to_string()]);
    let source = reg.resolve(&map.logical_source)?;
    let Some(count) = source.scan_distinct_count(&needed) else {
        return Ok(None);
    };
    Ok(Some(count_result(out_var.as_str(), count?)))
}

fn count_group_shape(
    query: &spargebra::Query,
) -> Option<(
    &spargebra::algebra::GraphPattern,
    &spargebra::term::Variable,
)> {
    use spargebra::algebra::{AggregateExpression, Expression, GraphPattern};
    let spargebra::Query::Select {
        dataset: None,
        pattern: GraphPattern::Project { inner, variables },
        ..
    } = query
    else {
        return None;
    };
    let [out_var] = variables.as_slice() else {
        return None;
    };
    let GraphPattern::Extend {
        inner,
        variable,
        expression: Expression::Variable(aggregate_var),
    } = inner.as_ref()
    else {
        return None;
    };
    if out_var != variable {
        return None;
    }
    let GraphPattern::Group {
        inner,
        variables,
        aggregates,
    } = inner.as_ref()
    else {
        return None;
    };
    if !variables.is_empty() {
        return None;
    }
    let [(group_var, AggregateExpression::CountSolutions { distinct: false })] =
        aggregates.as_slice()
    else {
        return None;
    };
    if group_var != aggregate_var {
        return None;
    }
    Some((inner.as_ref(), out_var))
}

fn single_triple_predicate(pattern: &spargebra::algebra::GraphPattern) -> Option<&str> {
    use spargebra::algebra::GraphPattern;
    use spargebra::term::{NamedNodePattern, TermPattern};
    let GraphPattern::Bgp { patterns } = pattern else {
        return None;
    };
    let [triple] = patterns.as_slice() else {
        return None;
    };
    let (TermPattern::Variable(s), NamedNodePattern::NamedNode(pred), TermPattern::Variable(o)) =
        (&triple.subject, &triple.predicate, &triple.object)
    else {
        return None;
    };
    if s == o {
        return None;
    }
    Some(pred.as_str())
}

fn count_result(out_var: &str, count: u64) -> QueryOutcome {
    // The current evaluator emits no group on an empty BGP input. Preserve its
    // observable result until its empty-group semantics are repaired separately.
    let solutions = if count == 0 {
        Vec::new()
    } else {
        let mut row = Solution::new();
        row.insert(out_var.to_string(), Binding::Literal(count.to_string()));
        vec![row]
    };
    QueryOutcome::Solutions(SparqlResult {
        vars: vec![out_var.to_string()],
        solutions,
    })
}

/// Execute the one shape whose SQL row order is provably the SPARQL term order:
/// a single map, single literal-column triple, SELECT of its subject/object,
/// ORDER BY the injective subject IRI template, and a bounded slice. The SQL
/// source performs DISTINCT and rejects empty cells before slicing. Every other
/// algebra shape continues through the full, established RDF evaluator.
fn try_direct_ordered_select(
    vg: &VirtualGraph,
    reg: &ObdaSourceRegistry,
    query: &spargebra::Query,
    proj: &Projection,
) -> Result<Option<QueryOutcome>, String> {
    if proj.base_iri.is_some() {
        return Ok(None);
    }
    let Some((triple, vars, descending, start, length)) = ordered_select_shape(query) else {
        return Ok(None);
    };
    let (
        spargebra::term::TermPattern::Variable(s),
        spargebra::term::NamedNodePattern::NamedNode(pred),
        spargebra::term::TermPattern::Variable(o),
    ) = (&triple.subject, &triple.predicate, &triple.object)
    else {
        return Ok(None);
    };
    let Some((map, key, object_column, object_map)) = direct_map_columns(vg, pred.as_str()) else {
        return Ok(None);
    };
    // A fixed suffix preserves uniqueness but can reverse lexical order when one
    // key prefixes another: `a` < `aa`, yet `aaz` < `az` for the suffix `z`.
    // COUNT only needs uniqueness; an ordered source slice also needs the key's
    // byte order to match the complete subject IRI's order.
    if !map.subject_template.ends_with('}') {
        return Ok(None);
    }
    let needed = BTreeSet::from([key.to_string(), object_column.to_string()]);
    let plan = ObdaOrder {
        column: key.to_string(),
        descending,
        start,
        length,
    };
    let source = reg.resolve(&map.logical_source)?;
    let Some(rows) = source.scan_ordered_distinct(&needed, &[], &plan) else {
        return Ok(None);
    };
    Ok(Some(ordered_rows_result(
        map, object_map, s, o, vars, rows?, length,
    )?))
}

fn ordered_rows_result(
    map: &TriplesMap,
    object_map: &ObjectMap,
    subject_var: &spargebra::term::Variable,
    object_var: &spargebra::term::Variable,
    vars: &[spargebra::term::Variable],
    rows: Vec<ForeignRow>,
    length: usize,
) -> Result<QueryOutcome, String> {
    let mut solutions = Vec::new();
    for row in rows {
        let (Some(subject), Some(object)) = (
            expand_template(&map.subject_template, &row),
            object_map.term_for(&row),
        ) else {
            return Err("obda: ordered source violated nonempty mapped-row contract".into());
        };
        let subject = NamedNode::new(subject)
            .map_err(|_| "obda: ordered source returned invalid subject IRI".to_string())?;
        let Term::Literal(literal) = object else {
            return Err("obda: ordered source returned nonliteral mapped object".into());
        };
        let mut solution = Solution::new();
        solution.insert(
            subject_var.as_str().to_string(),
            Binding::Node(subject.to_string()),
        );
        solution.insert(
            object_var.as_str().to_string(),
            Binding::Literal(literal.value().to_string()),
        );
        solutions.push(solution);
    }
    if solutions.len() > length {
        return Err("obda: ordered source exceeded requested LIMIT".into());
    }
    Ok(QueryOutcome::Solutions(SparqlResult {
        vars: vars.iter().map(|v| v.as_str().to_string()).collect(),
        solutions,
    }))
}

/// Recognize the exact algebra subset whose order can be represented by a
/// source-key sort. Keep algebra recognition separate from source execution so
/// unsupported wrappers immediately fall back to materialization.
fn ordered_select_shape(
    query: &spargebra::Query,
) -> Option<(
    &spargebra::term::TriplePattern,
    &[spargebra::term::Variable],
    bool,
    usize,
    usize,
)> {
    use spargebra::algebra::{Expression, GraphPattern, OrderExpression};
    use spargebra::term::{NamedNodePattern, TermPattern};
    let spargebra::Query::Select {
        dataset: None,
        pattern,
        ..
    } = query
    else {
        return None;
    };
    let (inner, vars, order, start, length) = peel_ordered_wrappers(pattern)?;
    let GraphPattern::Bgp { patterns } = inner else {
        return None;
    };
    let [triple] = patterns.as_slice() else {
        return None;
    };
    let (TermPattern::Variable(s), NamedNodePattern::NamedNode(_), TermPattern::Variable(o)) =
        (&triple.subject, &triple.predicate, &triple.object)
    else {
        return None;
    };
    if s == o || vars.len() != 2 || !vars.contains(s) || !vars.contains(o) {
        return None;
    }
    let descending = match order {
        OrderExpression::Asc(Expression::Variable(v)) if v == s => false,
        OrderExpression::Desc(Expression::Variable(v)) if v == s => true,
        _ => return None,
    };
    Some((triple, vars, descending, start, length))
}

/// Pull only the three allowed top-level wrappers; duplicate/unknown operators
/// are refused so they can keep the established evaluator's semantics.
fn peel_ordered_wrappers(
    pattern: &spargebra::algebra::GraphPattern,
) -> Option<(
    &spargebra::algebra::GraphPattern,
    &[spargebra::term::Variable],
    &spargebra::algebra::OrderExpression,
    usize,
    usize,
)> {
    use spargebra::algebra::GraphPattern;
    let mut inner = pattern;
    let mut vars = None;
    let mut order = None;
    let mut slice = None;
    loop {
        match inner {
            GraphPattern::Project {
                inner: next,
                variables,
            } if vars.is_none() => {
                vars = Some(variables.as_slice());
                inner = next;
            }
            GraphPattern::Slice {
                inner: next,
                start,
                length: Some(length),
            } if slice.is_none() && *length > 0 && *length <= 10_000 => {
                slice = Some((*start, *length));
                inner = next;
            }
            GraphPattern::OrderBy {
                inner: next,
                expression,
            } if order.is_none() && expression.len() == 1 => {
                order = Some(&expression[0]);
                inner = next;
            }
            _ => break,
        }
    }
    let (Some(vars), Some(order), Some((start, length))) = (vars, order, slice) else {
        return None;
    };
    Some((inner, vars, order, start, length))
}

/// A literal prefix + one placeholder + literal suffix is injective in its key.
fn single_subject_column(template: &str) -> Option<&str> {
    if template.contains("{{") || template.contains("}}") {
        return None;
    }
    let open = template.find('{')?;
    let close = open + template[open..].find('}')?;
    let key = &template[open + 1..close];
    let suffix = &template[close + 1..];
    (!key.is_empty() && !suffix.contains('{') && !suffix.contains('}')).then_some(key)
}

/// The SQL capability accepts only unreserved key bytes. Validate the constant
/// template with a representative safe key before any source slice is issued.
fn safe_subject_column(template: &str) -> Option<&str> {
    let key = single_subject_column(template)?;
    let sample = template.replacen(&format!("{{{key}}}"), "a", 1);
    NamedNode::new(sample).ok()?;
    Some(key)
}

/// A column whose R2RML object conversion cannot fail for a nonempty cell.
fn direct_literal_column(object: &ObjectMap) -> Option<&str> {
    match object {
        ObjectMap::Column(column) => Some(column),
        ObjectMap::TypedColumn(column, datatype) if NamedNode::new(datatype).is_ok() => {
            Some(column)
        }
        _ => None,
    }
}

/// The one-map, one-predicate, injective-template proof shared by ordered rows
/// and aggregate COUNT. Multiple object maps for one predicate require a union
/// of triples and therefore stay on the full evaluator.
fn direct_map_columns<'a>(
    vg: &'a VirtualGraph,
    predicate: &str,
) -> Option<(&'a TriplesMap, &'a str, &'a str, &'a ObjectMap)> {
    let [map] = vg.triples_maps.as_slice() else {
        return None;
    };
    // `rr:class` emits an additional rdf:type triple independently of any
    // explicit predicate-object map for rdf:type. A column-only SQL scan would
    // omit it from both ordered results and COUNT(*).
    if predicate == RDF_TYPE_IRI && map.subject_class.is_some() {
        return None;
    }
    let key = safe_subject_column(&map.subject_template)?;
    let mut objects = map
        .predicate_object_maps
        .iter()
        .filter(|(name, _)| name == predicate);
    let (_, object) = objects.next()?;
    if objects.next().is_some() {
        return None;
    }
    let column = direct_literal_column(object)?;
    Some((map, key, column, object))
}
