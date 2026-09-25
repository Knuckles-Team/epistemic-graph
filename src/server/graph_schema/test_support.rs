//! Shared ontology fixtures for the finance and world-model contract tests.

use eg_rdf::oxrdf::{NamedOrBlankNode, Term, Triple};

use super::compose::scope_blank_nodes;
use crate::graph::GraphSchemaSources;

pub(super) const KG: &str = "http://knuckles.team/kg#";
pub(super) const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
pub(super) const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";

pub(super) fn kg(local: &str) -> String {
    format!("<{KG}{local}>")
}

pub(super) fn parse_scoped(document: &str, scope: &str) -> Vec<Triple> {
    eg_rdf::mapping::parse_turtle(document)
        .unwrap()
        .into_iter()
        .map(|triple| scope_blank_nodes(triple, scope).unwrap())
        .collect()
}

pub(super) fn wired_with_fixture(modules: &[&str], fixture: &str) -> Vec<Triple> {
    let sources = GraphSchemaSources::default();
    let mut triples = Vec::new();
    for (index, (source_id, document)) in sources.ontologies().enumerate() {
        if modules.contains(&source_id) {
            triples.extend(parse_scoped(document, &format!("m{index}")));
        }
    }
    triples.extend(parse_scoped(fixture, "fixture"));
    triples
}

pub(super) fn subject_iri(triple: &Triple) -> Option<&str> {
    match &triple.subject {
        NamedOrBlankNode::NamedNode(node) => Some(node.as_str()),
        NamedOrBlankNode::BlankNode(_) => None,
    }
}

pub(super) fn object_iri(triple: &Triple) -> Option<&str> {
    match &triple.object {
        Term::NamedNode(node) => Some(node.as_str()),
        _ => None,
    }
}
