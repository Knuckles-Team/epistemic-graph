//! Registered-source acceptance for the infrastructure-v1 term additions
//! (MEDULLA-CQ-CONN-R068.13 to R068.16).
//!
//! Each test loads the registered `core:infrastructure@1` source through the
//! CoreSpec registry and asserts the added property with its stated domain and
//! range. The pullCount test additionally proves — per T-AUD-002 — that a
//! valid fixture conforms to the SHACL companion while a violating fixture
//! (a negative pull count) fails the shape.

use medulla_core::graph::{GraphSchemaSources, SchemaSourceOrigin};
use medulla_rdf::mapping::parse_turtle;
use medulla_rdf::oxrdf::{NamedNode, Triple};

// The infrastructure module's default namespace (its `:` prefix): the spec
// rows' `:term` names expand here, not in the shared `kg#` namespace.
const KG: &str = "http://knuckles.team/kg/infrastructure#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

fn infrastructure_ontology() -> Vec<Triple> {
    let sources = GraphSchemaSources::default();
    let source = sources
        .core
        .get("core:infrastructure@1")
        .unwrap_or_else(|| panic!("registered source core:infrastructure@1 is missing"));
    match &source.origin {
        SchemaSourceOrigin::Core {
            module, version, ..
        } => {
            assert_eq!(module, "infrastructure", "source module ownership changed");
            assert_eq!(*version, 1, "unexpected source version");
        }
        origin => panic!("core:infrastructure@1 has non-core origin {origin:?}"),
    }
    parse_turtle(
        source
            .ontology_ttl
            .as_deref()
            .unwrap_or_else(|| panic!("registered infrastructure source has no ontology")),
    )
    .expect("registered infrastructure ontology parses")
}

/// The SHACL companion (MEDULLA-CQ-CONN-R068.14 pullCount lower bound).
fn infrastructure_shapes() -> &'static str {
    include_str!("../ontology/infrastructure-v1.shapes.ttl")
}

#[allow(non_snake_case)]
mod ontology_audit_infrastructure_v1 {
    use super::*;

    // spec: MEDULLA-CQ-CONN-R068.13
    #[test]
    fn lastPushedAt_declared() {
        let triples = infrastructure_ontology();
        let term = format!("{KG}lastPushedAt");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}DatatypeProperty")),
            "lastPushedAt is declared as a datatype property"
        );
        assert!(
            has_iri(&triples, &term, RDFS_DOMAIN, &format!("{KG}ContainerImage")),
            "lastPushedAt's domain is ContainerImage"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{XSD}dateTime")),
            "lastPushedAt's range is xsd:dateTime"
        );
    }

    // spec: MEDULLA-CQ-CONN-R068.14
    #[test]
    fn pullCount_declared() {
        let triples = infrastructure_ontology();
        let term = format!("{KG}pullCount");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}DatatypeProperty")),
            "pullCount is declared as a datatype property"
        );
        assert!(
            has_iri(&triples, &term, RDFS_DOMAIN, &format!("{KG}ContainerImage")),
            "pullCount's domain is ContainerImage"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{XSD}integer")),
            "pullCount's range is xsd:integer"
        );

        // The non-negative bound lives in the SHACL companion: a valid fixture
        // conforms, a violating fixture fails the shape.
        let valid = r#"
@prefix : <http://knuckles.team/kg/infrastructure#> .
@prefix ex: <http://example.org/cq-conn/> .
ex:img-1 a :ContainerImage ;
    :pullCount 1200 .
"#;
        let report = medulla_shacl::validate_turtle(infrastructure_shapes(), valid)
            .expect("valid pullCount fixture parses and validates");
        assert!(
            report.conforms,
            "a non-negative pullCount conforms: {:?}",
            report.results
        );

        let violating = r#"
@prefix : <http://knuckles.team/kg/infrastructure#> .
@prefix ex: <http://example.org/cq-conn/> .
ex:img-2 a :ContainerImage ;
    :pullCount -1 .
"#;
        let report = medulla_shacl::validate_turtle(infrastructure_shapes(), violating)
            .expect("violating pullCount fixture parses before validation");
        assert!(!report.conforms, "a negative pullCount must fail the shape");
        // Report terms carry their N-Triples lexical form: an IRI path is
        // `<iri>`.
        assert!(
            report
                .results
                .iter()
                .any(|result| { result.path.as_deref() == Some(&format!("<{KG}pullCount>")) }),
            "the violation is reported on the pullCount path: {:?}",
            report.results
        );
    }

    // spec: MEDULLA-CQ-CONN-R068.15
    #[test]
    fn entityState_declared() {
        let triples = infrastructure_ontology();
        let term = format!("{KG}entityState");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}DatatypeProperty")),
            "entityState is declared as a datatype property"
        );
        assert!(
            has_iri(&triples, &term, RDFS_DOMAIN, &format!("{KG}Entity")),
            "entityState's domain is Entity"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{XSD}string")),
            "entityState's range is xsd:string"
        );
    }

    // spec: MEDULLA-CQ-CONN-R068.16
    #[test]
    fn unavailableSince_declared() {
        let triples = infrastructure_ontology();
        let term = format!("{KG}unavailableSince");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}DatatypeProperty")),
            "unavailableSince is declared as a datatype property"
        );
        assert!(
            has_iri(&triples, &term, RDFS_DOMAIN, &format!("{KG}Entity")),
            "unavailableSince's domain is Entity"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{XSD}dateTime")),
            "unavailableSince's range is xsd:dateTime"
        );
    }
}
