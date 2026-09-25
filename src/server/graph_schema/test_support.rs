//! Shared ontology assertions and fixture assembly for the core schema suites.

use std::collections::BTreeSet;

use eg_rdf::oxrdf::{NamedOrBlankNode, Term, Triple};

use super::compose::{scope_blank_nodes, validate_and_compose};
use crate::graph::GraphSchemaSources;

pub(super) const KG: &str = "http://knuckles.team/kg#";
pub(super) const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
pub(super) const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
pub(super) const OWL_IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";
const SKOS_MAPPINGS: &[&str] = &[
    "http://www.w3.org/2004/02/skos/core#exactMatch",
    "http://www.w3.org/2004/02/skos/core#closeMatch",
    "http://www.w3.org/2004/02/skos/core#broadMatch",
    "http://www.w3.org/2004/02/skos/core#relatedMatch",
];

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

pub(super) fn assert_core_catalog(modules: &[&str]) -> eg_rdf::owl::Classification {
    let sources = GraphSchemaSources::default();
    for module in modules {
        assert!(
            sources.core.contains_key(&format!("core:{module}@1")),
            "{module}"
        );
    }
    assert!(sources.core.len() <= crate::graph::MAX_CORE_SCHEMA_SOURCES);
    let composed = validate_and_compose(&sources).unwrap();
    let classification = eg_rdf::owl::Reasoner::from_triples(&composed.ontology).classify();
    assert!(classification.consistent);
    assert!(
        classification.unsatisfiable.is_empty(),
        "{:?}",
        classification.unsatisfiable
    );
    classification
}

pub(super) fn unmapped_classes(triples: &[Triple]) -> Vec<&str> {
    let declared = declared_classes(triples);
    let mapped: BTreeSet<&str> = triples
        .iter()
        .filter(|triple| SKOS_MAPPINGS.contains(&triple.predicate.as_str()))
        .filter_map(subject_iri)
        .collect();
    declared
        .into_iter()
        .filter(|class| !mapped.contains(class))
        .collect()
}

pub(super) fn declared_classes(triples: &[Triple]) -> BTreeSet<&str> {
    triples
        .iter()
        .filter(|triple| {
            triple.predicate.as_str() == RDF_TYPE && object_iri(triple) == Some(OWL_CLASS)
        })
        .filter_map(subject_iri)
        .collect()
}

pub(super) fn assert_shape_targets(triples: &[Triple], expected: &[&str]) {
    let targets: BTreeSet<&str> = triples
        .iter()
        .filter(|triple| triple.predicate.as_str() == "http://www.w3.org/ns/shacl#targetClass")
        .filter_map(object_iri)
        .collect();
    let names: BTreeSet<String> = expected
        .iter()
        .map(|local| format!("{KG}{local}"))
        .collect();
    let expected: BTreeSet<&str> = names.iter().map(String::as_str).collect();
    assert_eq!(targets, expected);
}
