//! Helpers shared by the ontology vocabulary tests.
#![cfg(feature = "owl")]
#![allow(dead_code)]

use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, Triple};

pub const KG: &str = "http://knuckles.team/kg#";
pub const BFO: &str = "http://purl.obolibrary.org/obo/BFO_";
pub const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";

/// Parse every Turtle document and concatenate the triples.
pub fn parse_all(documents: &[&str]) -> Vec<Triple> {
    documents
        .iter()
        .flat_map(|document| parse_turtle(document).unwrap())
        .collect()
}

/// True when the triples hold `subject predicate object` with an IRI object.
pub fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}
