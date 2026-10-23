//! Native-parser contract for the shared supply-chain vocabulary declarations.
#![cfg(feature = "rdf")]

mod common;

use common::{has_iri, KG, RDFS};
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::NamedNode;

const DOCUMENT: &str = include_str!("../../eg-core/ontology/supply_chain-v1.ttl");
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL: &str = "http://www.w3.org/2002/07/owl#";

// spec: EG-SUPPLY-VOCAB-R001.1.1
#[test]
fn shared_classes_use_existing_iris_and_declared_superclasses() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    for (class, parent) in [
        ("Supplier", format!("{KG}Vendor")),
        (
            "SupplyChainItem",
            "http://purl.obolibrary.org/obo/BFO_0000031".into(),
        ),
    ] {
        let subject = format!("{KG}{class}");
        assert!(has_iri(
            &triples,
            &subject,
            RDF_TYPE,
            &format!("{OWL}Class")
        ));
        assert!(has_iri(
            &triples,
            &subject,
            &format!("{RDFS}subClassOf"),
            &parent
        ));
        assert!(triples.iter().any(|triple| {
            triple.subject == NamedNode::new(&subject).unwrap().into()
                && triple.predicate.as_str() == format!("{RDFS}label")
                && triple.object.is_literal()
        }));
    }
}

// spec: EG-SUPPLY-VOCAB-R001.1.1
#[test]
fn supplies_item_has_supplier_domain_and_item_range() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    for (predicate, object) in [
        (RDF_TYPE.to_string(), format!("{OWL}ObjectProperty")),
        (format!("{RDFS}domain"), format!("{KG}Supplier")),
        (format!("{RDFS}range"), format!("{KG}SupplyChainItem")),
    ] {
        assert!(has_iri(
            &triples,
            &format!("{KG}suppliesItem"),
            &predicate,
            &object
        ));
    }
}

// spec: EG-SUPPLY-VOCAB-R001.1.1
#[test]
fn shared_file_does_not_redeclare_existing_supplied_by_property() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    let existing = NamedNode::new(format!("{KG}suppliedBy")).unwrap();
    assert!(!triples
        .iter()
        .any(|triple| triple.subject == existing.clone().into()));
}
