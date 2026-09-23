//! Explicit focus-node validation (EH-196 — the engine half of the UQL
//! `VALIDATE SHAPE` stage).
//!
//! [`crate::validate`] validates the nodes a shapes graph's TARGETS select. A query
//! stage has the opposite need: it already holds its candidate nodes and asks whether
//! each conforms to ONE named shape, whatever that shape targets. SHACL allows exactly
//! this (a focus node may be supplied by the processor rather than by a target), and
//! the per-focus check is the same [`super::Validator::validate_focus`] the target walk
//! uses — no second constraint engine.

use eg_rdf::oxrdf::{Graph, NamedNode, NamedNodeRef, Term};

use super::Validator;
use crate::report::ValidationReport;
use crate::shapes::ShapesGraph;

/// Validate each of `focus_nodes` against the shape `shape_iri` (a bare IRI) of
/// `shapes_graph`, over `data_graph`. The report holds every result for every focus
/// node; a focus node conforms iff no result names it. `Err` when `shape_iri` is not
/// an IRI or is not declared in the shapes graph (a misspelt shape must not make every
/// node vacuously conform), or when a `sh:sparql` constraint cannot be evaluated.
pub fn validate_nodes(
    shapes_graph: &Graph,
    data_graph: &Graph,
    shape_iri: &str,
    focus_nodes: &[Term],
) -> Result<ValidationReport, String> {
    let shape_node =
        NamedNode::new(shape_iri).map_err(|error| format!("bad shape IRI {shape_iri}: {error}"))?;
    if !declares(shapes_graph, shape_node.as_ref()) {
        return Err(format!(
            "shape <{shape_iri}> is not declared in the shapes graph"
        ));
    }
    let validator = Validator {
        shapes: ShapesGraph::new(shapes_graph),
        data: data_graph,
    };
    let shape = validator.shapes.parse_shape(&Term::NamedNode(shape_node));
    let mut results = Vec::new();
    if !shape.deactivated {
        for focus in focus_nodes {
            validator.validate_focus(&shape, focus, &mut results, 0)?;
        }
    }
    Ok(ValidationReport::from_results(results))
}

/// Whether `shape` is the subject of at least one triple of the shapes graph.
fn declares(shapes_graph: &Graph, shape: NamedNodeRef<'_>) -> bool {
    shapes_graph.triples_for_subject(shape).next().is_some()
}

#[cfg(test)]
mod tests {
    use eg_rdf::oxrdf::NamedNode;

    use super::*;
    use crate::validate::graph_from_turtle;

    const SHAPES: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://ex/> .
ex:NamedShape a sh:NodeShape ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"#;

    const DATA: &str = r#"
@prefix ex: <http://ex/> .
ex:alice ex:name "Alice" .
ex:bob ex:age 3 .
"#;

    fn node(local: &str) -> Term {
        Term::NamedNode(NamedNode::new(format!("http://ex/{local}")).unwrap())
    }

    /// The shape has NO target, so target-driven validation checks nothing; explicit
    /// focus validation checks exactly the nodes it is handed.
    #[test]
    fn a_targetless_shape_validates_the_nodes_it_is_handed() {
        let shapes = graph_from_turtle(SHAPES).unwrap();
        let data = graph_from_turtle(DATA).unwrap();
        assert!(crate::validate(&shapes, &data).unwrap().conforms);
        let report = validate_nodes(
            &shapes,
            &data,
            "http://ex/NamedShape",
            &[node("alice"), node("bob")],
        )
        .unwrap();
        assert!(!report.conforms);
        assert_eq!(report.results.len(), 1);
        assert_eq!(report.results[0].focus_node, "<http://ex/bob>");
    }

    /// An undeclared shape is an error, not a vacuous pass.
    #[test]
    fn an_undeclared_shape_is_refused() {
        let shapes = graph_from_turtle(SHAPES).unwrap();
        let data = graph_from_turtle(DATA).unwrap();
        let error =
            validate_nodes(&shapes, &data, "http://ex/Missing", &[node("alice")]).unwrap_err();
        assert!(error.contains("not declared"), "{error}");
    }
}
