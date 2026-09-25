//! FO-08 direct OBDA evaluation for a provable, single-table SELECT shape.
//!
//! The general SPARQL algebra still uses the virtual graph. In particular, FILTER,
//! OPTIONAL, UNION, multi-valued predicates, and joins across rows are not rewritten
//! here: a LIMIT before any of those operators could silently lose answers.

use std::collections::BTreeSet;

use spargebra::algebra::{Expression, GraphPattern, OrderExpression};
use spargebra::term::{NamedNodePattern, TermPattern};
use spargebra::Query;

use super::{expand_template, DirectSelect, ObdaSourceRegistry, ObjectMap, VirtualGraph};
use crate::sparql::{Binding, SparqlResult};

/// Returns `None` unless the entire query can be answered without a graph evaluator.
pub(super) fn run(
    vg: &VirtualGraph,
    reg: &ObdaSourceRegistry,
    query: &Query,
) -> Result<Option<SparqlResult>, String> {
    let Query::Select {
        pattern, dataset, ..
    } = query
    else {
        return Ok(None);
    };
    if dataset.is_some() {
        return Ok(None);
    }

    let mut pattern = pattern;
    let mut limit = None;
    let mut offset = 0;
    let mut projected = None;
    let mut order = None;
    loop {
        match pattern {
            GraphPattern::Slice {
                inner,
                start,
                length,
            } if limit.is_none() && offset == 0 => {
                limit = *length;
                offset = *start;
                pattern = inner.as_ref();
            }
            GraphPattern::Project { inner, variables } if projected.is_none() => {
                projected = Some(
                    variables
                        .iter()
                        .map(|v| v.as_str().to_string())
                        .collect::<Vec<_>>(),
                );
                pattern = inner.as_ref();
            }
            GraphPattern::OrderBy { inner, expression } if order.is_none() => {
                if expression.len() != 1 {
                    return Ok(None);
                }
                order = Some(&expression[0]);
                pattern = inner.as_ref();
            }
            _ => break,
        }
    }
    let GraphPattern::Bgp { patterns } = pattern else {
        return Ok(None);
    };
    // A one-pattern BGP is important: duplicate subject keys can create cross-row
    // joins, and pushing LIMIT through such a join is unsound.
    let [triple] = patterns.as_slice() else {
        return Ok(None);
    };
    let NamedNodePattern::NamedNode(predicate) = &triple.predicate else {
        return Ok(None);
    };
    let (TermPattern::Variable(subject), TermPattern::Variable(object)) =
        (&triple.subject, &triple.object)
    else {
        return Ok(None);
    };
    if subject == object {
        return Ok(None);
    }
    let candidates: Vec<_> = vg
        .triples_maps
        .iter()
        .flat_map(|map| {
            map.predicate_object_maps
                .iter()
                .filter(move |(p, _)| p == predicate.as_str())
                .map(move |(_, obj)| (map, obj))
        })
        .collect();
    let [(map, obj)] = candidates.as_slice() else {
        return Ok(None);
    };
    let (ObjectMap::Column(object_column) | ObjectMap::TypedColumn(object_column, _)) = obj else {
        return Ok(None);
    };
    let Some(key_column) = single_column_template(&map.subject_template) else {
        return Ok(None);
    };
    let projected =
        projected.unwrap_or_else(|| vec![subject.as_str().into(), object.as_str().into()]);
    if projected
        .iter()
        .any(|v| v != subject.as_str() && v != object.as_str())
    {
        return Ok(None);
    }
    let order = match order {
        None => None,
        Some(OrderExpression::Asc(Expression::Variable(v)))
            if v == object && numeric_object(obj) =>
        {
            Some((object_column.clone(), false))
        }
        Some(OrderExpression::Desc(Expression::Variable(v)))
            if v == object && numeric_object(obj) =>
        {
            Some((object_column.clone(), true))
        }
        // SQL text collations need not match SPARQL codepoint ordering.
        _ => return Ok(None),
    };
    let columns = BTreeSet::from([key_column.to_owned(), object_column.clone()]);
    let plan = DirectSelect {
        columns: columns.clone(),
        nonempty: columns,
        order,
        limit,
        offset,
    };
    let source = reg.resolve(&map.logical_source)?;
    let Some(rows) = source.direct_select(&plan)? else {
        return Ok(None);
    };
    let mut solutions = Vec::with_capacity(rows.len());
    for row in rows {
        // A source promising `direct_select` must have applied the guards. Validate
        // again at this boundary so a misbehaving adapter cannot invent RDF terms.
        let Some(iri) = expand_template(&map.subject_template, &row) else {
            return Err("obda: direct source returned an empty subject key".into());
        };
        let Some(value) = row.get(object_column).filter(|v| !v.is_empty()) else {
            return Err("obda: direct source returned an empty object value".into());
        };
        let mut sol = crate::sparql::Solution::new();
        if projected.iter().any(|v| v == subject.as_str()) {
            sol.insert(subject.as_str().into(), Binding::Node(format!("<{iri}>")));
        }
        if projected.iter().any(|v| v == object.as_str()) {
            sol.insert(object.as_str().into(), Binding::Literal(value.clone()));
        }
        solutions.push(sol);
    }
    Ok(Some(SparqlResult {
        vars: projected,
        solutions,
    }))
}

fn single_column_template(template: &str) -> Option<&str> {
    if template.contains("{{") || template.contains("}}") {
        return None;
    }
    let open = template.find('{')?;
    let close = open + template[open..].find('}')?;
    let column = &template[open + 1..close];
    (!column.is_empty() && !template[close + 1..].chars().any(|c| c == '{' || c == '}'))
        .then_some(column)
}

fn numeric_object(obj: &ObjectMap) -> bool {
    matches!(obj, ObjectMap::TypedColumn(_, dt) if super::is_numeric_datatype(dt))
}
