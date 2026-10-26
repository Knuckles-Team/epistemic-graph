//! Native-parser contract for the cross-source authority vocabulary
//! (EG-CQ-XSRC-R001.1).
#![cfg(feature = "rdf")]

mod common;

use common::has_iri;
use eg_rdf::mapping::parse_turtle;

const DOCUMENT: &str = include_str!("../../eg-core/ontology/virtual_graph-v1.ttl");
const VG: &str = "http://knuckles.team/kg/virtual-graph#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";

// spec: EG-CQ-XSRC-R001.1
#[test]
fn source_authority_vocabulary_declared_with_domain_and_range() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    assert!(has_iri(
        &triples,
        &format!("{VG}SourceAuthority"),
        RDF_TYPE,
        &format!("{OWL}Class")
    ));
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    for (property, kind, range_iri) in [
        (
            "authoritativeSource",
            "ObjectProperty",
            format!("{VG}SourceConnection"),
        ),
        ("authorityFor", "ObjectProperty", format!("{OWL}Class")),
        ("authorityRank", "DatatypeProperty", format!("{XSD}integer")),
    ] {
        let subject = format!("{VG}{property}");
        assert!(has_iri(
            &triples,
            &subject,
            RDF_TYPE,
            &format!("{OWL}{kind}")
        ));
        assert!(has_iri(
            &triples,
            &subject,
            &domain,
            &format!("{VG}SourceAuthority")
        ));
        assert!(has_iri(&triples, &subject, &range, &range_iri));
    }
}
