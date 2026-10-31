//! Native-parser contract for the host limit-breach and HostLimit vocabulary
//! (MEDULLA-CQ-XSRC-R008.2.1, MEDULLA-SUPPLY-VOCAB-R004.1).
#![cfg(feature = "rdf")]

mod common;

use common::has_iri;
use medulla_rdf::mapping::parse_turtle;
use medulla_rdf::oxrdf::NamedNode;

const DOCUMENT: &str = include_str!("../../medulla-core/ontology/infrastructure-v1.ttl");
const INFRA: &str = "http://knuckles.team/kg/infrastructure#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";

// spec: MEDULLA-CQ-XSRC-R008.2.1
#[test]
fn host_limit_breach_terms_listed_with_host_domain() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    for name in [
        "temperatureCelsius",
        "temperatureLimitCelsius",
        "capacityUtilization",
    ] {
        let subject = format!("{INFRA}{name}");
        assert!(has_iri(
            &triples,
            &subject,
            RDF_TYPE,
            &format!("{OWL}DatatypeProperty")
        ));
        assert!(has_iri(
            &triples,
            &subject,
            &format!("{RDFS}domain"),
            &format!("{INFRA}Host")
        ));
        assert!(has_iri(
            &triples,
            &subject,
            &format!("{RDFS}range"),
            &format!("{XSD}decimal")
        ));
    }
}

// spec: MEDULLA-SUPPLY-VOCAB-R004.1
#[test]
fn host_limit_entity_terms_listed_and_no_overlimit_class() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    assert!(has_iri(
        &triples,
        &format!("{INFRA}HostLimit"),
        RDF_TYPE,
        &format!("{OWL}Class")
    ));
    assert!(has_iri(
        &triples,
        &format!("{INFRA}HostLimit"),
        &format!("{RDFS}subClassOf"),
        "http://purl.obolibrary.org/obo/BFO_0000031"
    ));
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    assert!(has_iri(
        &triples,
        &format!("{INFRA}limitOf"),
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{INFRA}limitOf"),
        &domain,
        &format!("{INFRA}HostLimit")
    ));
    assert!(has_iri(
        &triples,
        &format!("{INFRA}limitOf"),
        &range,
        &format!("{INFRA}Host")
    ));
    for (name, value_range) in [
        ("limitKind", format!("{XSD}string")),
        ("readingValue", format!("{XSD}decimal")),
        ("limitValue", format!("{XSD}decimal")),
        ("limitUnit", format!("{XSD}string")),
    ] {
        let subject = format!("{INFRA}{name}");
        assert!(has_iri(
            &triples,
            &subject,
            RDF_TYPE,
            &format!("{OWL}DatatypeProperty")
        ));
        assert!(has_iri(
            &triples,
            &subject,
            &domain,
            &format!("{INFRA}HostLimit")
        ));
        assert!(has_iri(&triples, &subject, &range, &value_range));
    }
    // 'Over limit' is the query condition reading > limit; no breach class is stored.
    let over_limit = NamedNode::new(format!("{INFRA}OverLimit")).unwrap();
    assert!(
        !triples
            .iter()
            .any(|triple| triple.subject == over_limit.clone().into()),
        "no OverLimit class may be declared"
    );
}
