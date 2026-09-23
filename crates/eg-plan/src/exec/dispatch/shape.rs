//! `VALIDATE SHAPE` (EH-196): keep the rows whose node does — or does not — conform to
//! one named SHACL shape.
//!
//! Each row's node is validated as an explicit SHACL focus node against the named shape
//! (`eg_shacl::validate_nodes`), over the RDF projection of the SAME snapshot the rest
//! of the plan reads (`eg_rdf::mapping::export_view_triples`). The stage is a filter:
//! an empty input stays empty, and row order and scores are preserved.

use std::collections::HashSet;

use eg_core::graph::GraphView;
use eg_rdf::oxrdf::{BlankNode, NamedNode, Term};
use eg_types::wire::ShapeKeep;

use crate::rowset::RowSet;

/// Run one `VALIDATE SHAPE` stage over `input`.
pub(super) fn validate_shape(
    view: &GraphView,
    input: RowSet,
    shape: &str,
    shapes: &str,
    keep: ShapeKeep,
) -> Result<RowSet, String> {
    if input.is_empty() {
        return Ok(input);
    }
    if shapes.trim().is_empty() {
        return Err(
            "VALIDATE SHAPE needs a shapes graph: `VALIDATE SHAPE <shape> USING \"<turtle>\"`"
                .into(),
        );
    }
    let shapes_graph = eg_shacl::graph_from_turtle(shapes)
        .map_err(|error| format!("VALIDATE SHAPE: bad shapes graph: {error}"))?;
    let data_graph = data_graph(view)?;
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
