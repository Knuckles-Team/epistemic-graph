//! Native parser contract for the shared data-platform vocabulary.

use eg_core::graph::{current_core_set_digest, GraphSchemaSources, SchemaSourceOrigin};
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, Triple};

const DP: &str = "http://knuckles.team/kg/data-platform#";
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

// spec: EG-DATAPLAT-VOCAB-R001.1
#[test]
fn shared_terms_present() {
    let triples = parse_turtle(include_str!("../ontology/data_platform-v1.ttl"))
        .expect("the data-platform vocabulary parses");

    assert!(has_iri(
        &triples,
        &format!("{DP}DataStore"),
        &format!("{RDF}type"),
        &format!("{OWL}Class")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}storeFamily"),
        &format!("{RDF}type"),
        &format!("{OWL}DatatypeProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}inStore"),
        &format!("{RDF}type"),
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}viaConnection"),
        &format!("{RDF}type"),
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}asOf"),
        &format!("{RDF}type"),
        &format!("{OWL}DatatypeProperty")
    ));

    assert!(has_iri(
        &triples,
        &format!("{DP}storeFamily"),
        &format!("{RDFS}domain"),
        &format!("{DP}DataStore")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}storeFamily"),
        &format!("{RDFS}range"),
        &format!("{XSD}string")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}inStore"),
        &format!("{RDFS}range"),
        &format!("{DP}DataStore")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}viaConnection"),
        &format!("{RDFS}domain"),
        &format!("{DP}DataStore")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}viaConnection"),
        &format!("{RDFS}range"),
        &format!("{VG}SourceConnection")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}asOf"),
        &format!("{RDFS}range"),
        &format!("{XSD}dateTime")
    ));
}

// spec: EG-DATAPLAT-VOCAB-R001.2
#[test]
fn store_mapping_subclass_of_virtual_mapping() {
    let triples = parse_turtle(include_str!("../ontology/data_platform-v1.ttl"))
        .expect("the data-platform vocabulary parses");

    assert!(has_iri(
        &triples,
        &format!("{DP}StoreMapping"),
        &format!("{RDFS}subClassOf"),
        &format!("{VG}VirtualMapping")
    ));
    for property in [
        "contractKind",
        "viaOperation",
        "accelerationMode",
        "materialisationReason",
    ] {
        let property_iri = format!("{DP}{property}");
        assert!(
            has_iri(
                &triples,
                &property_iri,
                &format!("{RDF}type"),
                &format!("{OWL}DatatypeProperty")
            ),
            "{property} is a datatype property"
        );
        assert!(
            has_iri(
                &triples,
                &property_iri,
                &format!("{RDFS}domain"),
                &format!("{DP}StoreMapping")
            ),
            "{property} is owned by StoreMapping"
        );
        assert!(
            has_iri(
                &triples,
                &property_iri,
                &format!("{RDFS}range"),
                &format!("{XSD}string")
            ),
            "{property} has the string range"
        );
    }
}

// spec: EG-DATAPLAT-VOCAB-R001.4
#[test]
fn core_source_registered() {
    let sources = GraphSchemaSources::default();
    sources
        .validate()
        .expect("the default core catalog validates");
    let source = sources
        .core
        .get("core:data-platform@1")
        .expect("the data-platform vocabulary is registered");
    let ontology = source
        .ontology_ttl
        .as_deref()
        .expect("the data-platform source carries an ontology");
    let triples = parse_turtle(ontology).expect("the registered ontology parses");
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg/data-platform",
        &format!("{RDF}type"),
        &format!("{OWL}Ontology")
    ));

    match &source.origin {
        SchemaSourceOrigin::Core {
            module,
            version,
            set_digest,
        } => {
            assert_eq!(module, "data-platform");
            assert_eq!(*version, 1);
            assert_eq!(*set_digest, current_core_set_digest());
        }
        other => panic!("unexpected source origin: {other:?}"),
    }

    let catalog = sources
        .core
        .get("core:catalog@1")
        .and_then(|source| source.ontology_ttl.as_deref())
        .expect("the aggregate catalog carries an ontology");
    assert!(catalog.contains("<http://knuckles.team/kg/data-platform>"));
}
