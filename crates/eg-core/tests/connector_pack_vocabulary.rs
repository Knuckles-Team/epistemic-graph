//! Native parser contract for the connector-pack ingest vocabulary.

use eg_core::graph::GraphSchemaSources;
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, Triple};

const CP: &str = "http://knuckles.team/kg/connector-pack#";
const VG: &str = "http://knuckles.team/kg/virtual-graph#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

// spec: EG-TYPED-PACKS-R101.1
#[test]
fn contract_terms_resolve_with_domain_and_range() {
    let triples = parse_turtle(include_str!("../ontology/connector_pack-v1.ttl"))
        .expect("the connector-pack vocabulary parses");
    let class = format!("{RDF}type");
    let subclass = format!("{RDFS}subClassOf");

    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg/connector-pack",
        &class,
        &format!("{OWL}Ontology")
    ));
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg/connector-pack",
        &format!("{OWL}imports"),
        "http://knuckles.team/kg/virtual-graph"
    ));
    for (child, parent) in [
        ("IngestContract", format!("{VG}MetadataContract")),
        ("SourceObject", format!("{VG}VirtualMapping")),
        ("FieldMapping", format!("{VG}PredicateFieldPair")),
    ] {
        assert!(has_iri(
            &triples,
            &format!("{CP}{child}"),
            &subclass,
            &parent
        ));
    }
    for class_name in [
        "IngestContract",
        "SourceObject",
        "FieldMapping",
        "KeyRule",
        "CursorPolicy",
        "AccessPolicy",
        "PrivacyClass",
        "ProvenanceRequirement",
    ] {
        assert!(has_iri(
            &triples,
            &format!("{CP}{class_name}"),
            &class,
            &format!("{OWL}Class")
        ));
    }

    let object_properties = [
        (
            "contractOf",
            "IngestContract",
            format!("{VG}SourceConnection"),
        ),
        (
            "hasSourceObject",
            "IngestContract",
            format!("{CP}SourceObject"),
        ),
        ("hasKeyRule", "SourceObject", format!("{CP}KeyRule")),
        (
            "hasCursorPolicy",
            "IngestContract",
            format!("{CP}CursorPolicy"),
        ),
        (
            "hasAccessPolicy",
            "IngestContract",
            format!("{CP}AccessPolicy"),
        ),
        ("privacyClass", "AccessPolicy", format!("{CP}PrivacyClass")),
        (
            "requiresProvenance",
            "IngestContract",
            format!("{CP}ProvenanceRequirement"),
        ),
        ("governedBy", "Resource", format!("{CP}IngestContract")),
    ];
    for (property, domain, range) in object_properties {
        let property_iri = format!("{CP}{property}");
        assert!(has_iri(
            &triples,
            &property_iri,
            &class,
            &format!("{OWL}ObjectProperty")
        ));
        let domain_iri = if domain == "Resource" {
            format!("{RDFS}{domain}")
        } else {
            format!("{CP}{domain}")
        };
        assert!(has_iri(
            &triples,
            &property_iri,
            &format!("{RDFS}domain"),
            &domain_iri
        ));
        assert!(has_iri(
            &triples,
            &property_iri,
            &format!("{RDFS}range"),
            &range
        ));
    }
    for property in [
        "contractVersion",
        "contractDigest",
        "cursorKind",
        "defaultAcl",
        "aclField",
        "tenantField",
        "readRole",
    ] {
        let property_iri = format!("{CP}{property}");
        assert!(has_iri(
            &triples,
            &property_iri,
            &class,
            &format!("{OWL}DatatypeProperty")
        ));
        assert!(has_iri(
            &triples,
            &property_iri,
            &format!("{RDFS}range"),
            &format!("{XSD}string")
        ));
    }

    let sources = GraphSchemaSources::default();
    sources
        .validate()
        .expect("the registered core catalog validates");
    let source = sources
        .core
        .get("core:connector-pack@1")
        .expect("connector-pack is registered as a core source");
    assert!(source.ontology_ttl.is_some());
    let catalog = sources
        .core
        .get("core:catalog@1")
        .and_then(|source| source.ontology_ttl.as_deref())
        .expect("the aggregate catalog carries an ontology");
    assert!(catalog.contains("<http://knuckles.team/kg/connector-pack>"));
}
