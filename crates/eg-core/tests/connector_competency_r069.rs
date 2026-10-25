//! Registered-source acceptance for the shared Repository and Branch classes.
//!
//! The connector-competency rows originally assigned these classes to the
//! lifecycle module.  They are already canonical in the registered software
//! module, so this test proves both the positive source and the wrong-module
//! negative case without declaring a second class.

use eg_core::graph::{GraphSchemaSources, SchemaSourceOrigin};
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, Triple};

const KG: &str = "http://knuckles.team/kg#";
const BFO: &str = "http://purl.obolibrary.org/obo/BFO_";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
const RDFS_SUBCLASS: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

fn registered_ontology<'a>(
    sources: &'a GraphSchemaSources,
    source_id: &str,
    expected_module: &str,
) -> &'a str {
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
    source
        .ontology_ttl
        .as_deref()
        .unwrap_or_else(|| panic!("registered source {source_id} has no ontology"))
}

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

fn registered_modules() -> (Vec<Triple>, Vec<Triple>) {
    let sources = GraphSchemaSources::default();
    let software = parse_turtle(registered_ontology(&sources, "core:software@1", "software"))
        .expect("registered software ontology parses");
    let sdlc = parse_turtle(registered_ontology(
        &sources,
        "core:sdlc_lifecycle@1",
        "sdlc_lifecycle",
    ))
    .expect("registered lifecycle ontology parses");
    (software, sdlc)
}

#[allow(non_snake_case)]
mod ontology_audit_software {
    use super::*;

    // spec: EG-CQ-CONN-R069.2
    #[test]
    fn Repository_declared() {
        let (software, sdlc) = registered_modules();
        let repository = format!("{KG}Repository");

        assert!(has_iri(&software, &repository, RDF_TYPE, OWL_CLASS));
        assert!(has_iri(
            &software,
            &repository,
            RDFS_SUBCLASS,
            &format!("{KG}Collection")
        ));
        assert!(
            !has_iri(&sdlc, &repository, RDF_TYPE, OWL_CLASS),
            "Repository must remain owned by software-v1, not sdlc_lifecycle-v1"
        );
    }

    // spec: EG-CQ-CONN-R069.3
    #[test]
    fn Branch_declared() {
        let (software, sdlc) = registered_modules();
        let branch = format!("{KG}Branch");

        assert!(has_iri(&software, &branch, RDF_TYPE, OWL_CLASS));
        assert!(has_iri(
            &software,
            &branch,
            RDFS_SUBCLASS,
            &format!("{BFO}0000031")
        ));
        assert!(
            !has_iri(&sdlc, &branch, RDF_TYPE, OWL_CLASS),
            "Branch must remain owned by software-v1, not sdlc_lifecycle-v1"
        );
    }
}
