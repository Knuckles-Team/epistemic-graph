//! `VALIDATE SHAPE` (EH-196): keep the rows whose node does — or does not — conform to
//! one named SHACL shape.
//!
//! Each row's node is validated as an explicit SHACL focus node against the named shape
//! (`eg_shacl::validate_nodes`), over the RDF projection of the SAME snapshot the rest
//! of the plan reads (`eg_rdf::mapping::export_view_triples`). The shapes graph is the
//! stage's own `USING` document when it has one, else the ctx-bound
//! [`crate::exec::ShapeSource`] (the graph's composed GraphSchema shapes on the served
//! path). The stage is a filter: an empty input stays empty, and row order and scores
//! are preserved.

use std::collections::HashSet;

use eg_core::graph::GraphView;
use eg_rdf::oxrdf::{BlankNode, NamedNode, Term};
use eg_types::wire::ShapeKeep;

use crate::exec::PlanCtx;
use crate::rowset::RowSet;

/// Run one `VALIDATE SHAPE` stage over `input`.
pub(super) fn validate_shape(
    ctx: &PlanCtx,
    input: RowSet,
    shape: &str,
    shapes: &str,
    keep: ShapeKeep,
) -> Result<RowSet, String> {
    if input.is_empty() {
        return Ok(input);
    }
    let shapes_graph = shapes_graph(ctx, shapes)?;
    let data_graph = data_graph(ctx.view)?;
    let focus = focus_terms(&input)?;
    let report = eg_shacl::validate_nodes(&shapes_graph, &data_graph, bare_iri(shape), &focus)
        .map_err(|error| format!("VALIDATE SHAPE: {error}"))?;
    let violating: HashSet<&str> = report
        .results
        .iter()
        .map(|result| result.focus_node.as_str())
        .collect();
    let kept: HashSet<&str> = input
        .rows()
        .iter()
        .map(|row| row.id.as_str())
        .filter(|id| violating.contains(id) == (keep == ShapeKeep::Violating))
        .collect();
    Ok(input.intersect_keep_order(&kept))
}

/// The stage's own `USING` document, else the ctx-bound graph shapes.
fn shapes_graph(ctx: &PlanCtx, shapes: &str) -> Result<eg_shacl::Graph, String> {
    if !shapes.trim().is_empty() {
        return eg_shacl::graph_from_turtle(shapes)
            .map_err(|error| format!("VALIDATE SHAPE: bad shapes graph: {error}"));
    }
    let Some(source) = ctx.shape_source else {
        return Err(
            "VALIDATE SHAPE needs a shapes graph: pass `USING \"<turtle>\"`, or query \
                    a graph whose GraphSchema shapes are bound to this plan"
                .into(),
        );
    };
    source
        .shapes()
        .map_err(|error| format!("VALIDATE SHAPE: graph shapes: {error}"))
}

/// The snapshot's RDF projection as a SHACL data graph.
fn data_graph(view: &GraphView) -> Result<eg_shacl::Graph, String> {
    let triples = eg_rdf::mapping::export_view_triples(view)
        .map_err(|error| format!("VALIDATE SHAPE: project the graph to RDF: {error}"))?;
    let mut graph = eg_shacl::Graph::new();
    for triple in &triples {
        graph.insert(triple);
    }
    Ok(graph)
}

/// Every row id as an RDF term. A row whose id is not a term has no RDF identity and
/// cannot be a focus node, so the stage refuses rather than guessing a verdict for it.
fn focus_terms(input: &RowSet) -> Result<Vec<Term>, String> {
    input.rows().iter().map(|row| term(&row.id)).collect()
}

fn term(id: &str) -> Result<Term, String> {
    let not_a_term = || format!("VALIDATE SHAPE: row `{id}` is not an RDF resource term");
    if let Some(iri) = id.strip_prefix('<').and_then(|rest| rest.strip_suffix('>')) {
        return NamedNode::new(iri)
            .map(Term::NamedNode)
            .map_err(|_| not_a_term());
    }
    match id.strip_prefix("_:") {
        Some(label) => BlankNode::new(label)
            .map(Term::BlankNode)
            .map_err(|_| not_a_term()),
        None => Err(not_a_term()),
    }
}

/// A shape reference as a bare IRI (`<iri>` and `iri` name the same shape).
fn bare_iri(shape: &str) -> &str {
    shape
        .strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
        .unwrap_or(shape)
}

#[cfg(test)]
mod tests;
