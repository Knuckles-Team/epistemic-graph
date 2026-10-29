//! Connector-pack competency proofs (MEDULLA-CQ-CONN-R084).
//!
//! The pack competency questions (R084.1 to R084.4) are answered by SPARQL over
//! imported connector packs. This file carries the vocabulary proofs those
//! questions depend on, starting with the finance method vocabulary (R084.5):
//! the terms are declared in the registered `finance` core source, and the
//! shapes companion registered as `finance-shapes` rejects an algorithm record
//! missing either applicability axis.

use eg_core::graph::{GraphSchemaSources, SchemaSourceOrigin};
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, Triple};

const KG: &str = "http://knuckles.team/kg#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
const OWL_OBJECT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#ObjectProperty";
const SHACL_NODE_SHAPE: &str = "http://www.w3.org/ns/shacl#NodeShape";

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

fn finance_documents() -> (std::sync::Arc<str>, std::sync::Arc<str>) {
    let sources = GraphSchemaSources::default();
    sources
        .validate()
        .expect("the default core catalog validates");
    for (source_id, expected_module) in [
        ("core:finance@1", "finance"),
        ("core:finance-shapes@1", "finance-shapes"),
    ] {
        let source = sources
            .core
            .get(source_id)
            .unwrap_or_else(|| panic!("registered source {source_id} is missing"));
        match &source.origin {
            SchemaSourceOrigin::Core {
                module, version, ..
            } => {
                assert_eq!(module, expected_module, "source module ownership changed");
                assert_eq!(*version, 1, "unexpected source version");
            }
            origin => panic!("{source_id} has non-core origin {origin:?}"),
        }
    }
    let ontology = sources
        .core
        .get("core:finance@1")
        .unwrap()
        .ontology_ttl
        .clone()
        .expect("the finance module carries an ontology");
    let shapes = sources
        .core
        .get("core:finance-shapes@1")
        .unwrap()
        .shapes_ttl
        .clone()
        .expect("the finance module carries its shapes companion");
    (ontology, shapes)
}

// spec: MEDULLA-CQ-CONN-R084.5
#[test]
fn finance_method_terms_and_shape() {
    let (ontology, shapes) = finance_documents();
    let ontology: &str = &ontology;
    let shapes: &str = &shapes;
    let triples = parse_turtle(ontology).expect("the registered finance ontology parses");

    // The two method classes are declared owl:Class in the registered module.
    for class in ["FinanceAlgorithm", "AnalysisType"] {
        assert!(
            has_iri(&triples, &format!("{KG}{class}"), RDF_TYPE, OWL_CLASS),
            "{class} is declared as an owl:Class"
        );
    }
    // The applicability and input properties are declared owl:ObjectProperty
    // with the finance-method domain and their declared ranges.
    for property in [
        "appliesToAnalysis",
        "appliesToInstrumentType",
        "requiresInput",
    ] {
        let subject = format!("{KG}{property}");
        assert!(
            has_iri(&triples, &subject, RDF_TYPE, OWL_OBJECT_PROPERTY),
            "{property} is declared as an owl:ObjectProperty"
        );
        assert!(
            has_iri(
                &triples,
                &subject,
                RDFS_DOMAIN,
                &format!("{KG}FinanceAlgorithm")
            ),
            "{property} carries the FinanceAlgorithm domain"
        );
    }
    assert!(has_iri(
        &triples,
        &format!("{KG}appliesToAnalysis"),
        RDFS_RANGE,
        &format!("{KG}AnalysisType")
    ));

    // The registered shapes companion carries the algorithm shape.
    let shape_triples = parse_turtle(shapes).expect("the registered finance shapes parse");
    assert!(has_iri(
        &shape_triples,
        &format!("{KG}FinanceAlgorithmShape"),
        RDF_TYPE,
        SHACL_NODE_SHAPE
    ));

    // A complete algorithm conforms; dropping either applicability axis
    // violates the shape.
    let complete = "@prefix : <http://knuckles.team/kg#> .
        :method a :FinanceAlgorithm ; :appliesToAnalysis :riskAnalysis ; :appliesToInstrumentType :Position .
        :riskAnalysis a :AnalysisType .";
    let report = eg_shacl::validate_turtle(shapes, complete).expect("validation runs");
    assert!(report.conforms, "a complete algorithm conforms: {report:?}");

    let missing_analysis = "@prefix : <http://knuckles.team/kg#> .
        :method a :FinanceAlgorithm ; :appliesToInstrumentType :Position .";
    let report = eg_shacl::validate_turtle(shapes, missing_analysis).expect("validation runs");
    assert!(
        !report.conforms,
        "an algorithm naming no analysis type violates the shape: {report:?}"
    );

    let missing_instrument = "@prefix : <http://knuckles.team/kg#> .
        :method a :FinanceAlgorithm ; :appliesToAnalysis :riskAnalysis .";
    let report = eg_shacl::validate_turtle(shapes, missing_instrument).expect("validation runs");
    assert!(
        !report.conforms,
        "an algorithm naming no instrument or position class violates the shape: {report:?}"
    );
}
