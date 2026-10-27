//! Model fixture for one connector pack crossing into the shared ontology.
//!
//! This is deliberately limited to the R085.1 model seam.  Served import,
//! projection, source execution and query evidence belong to the later children
//! and are not inferred from this fixture.

use std::collections::BTreeSet;

use eg_core::graph::GraphSchemaSources;
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};
use eg_rdf::pack::{declared_classes_and_ontologies, ontology_orphan_classes};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_SUBCLASS: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const CAT: &str = "http://knuckles.team/kg/catalog#";
const CP: &str = "http://knuckles.team/kg/connector-pack#";
const KG: &str = "http://knuckles.team/kg#";

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

fn typed_subjects<'a>(triples: &'a [Triple], class: &str) -> BTreeSet<&'a str> {
    triples
        .iter()
        .filter(|triple| triple.predicate.as_str() == RDF_TYPE)
        .filter(|triple| matches!(&triple.object, Term::NamedNode(node) if node.as_str() == class))
        .filter_map(|triple| match &triple.subject {
            NamedOrBlankNode::NamedNode(node) => Some(node.as_str()),
            NamedOrBlankNode::BlankNode(_) => None,
        })
        .collect()
}

fn parent_classes() -> BTreeSet<String> {
    let sources = GraphSchemaSources::default();
    sources
        .validate()
        .expect("the registered parent catalog validates");
    sources
        .core
        .values()
        .filter_map(|source| source.ontology_ttl.as_deref())
        .flat_map(|document| declared_classes_and_ontologies(document).0)
        .collect()
}

#[test]
fn fixture_parses_and_is_linked_to_the_parent() {
    let fixture = include_str!("fixtures/connector_auto_onboard/connector_pack.ttl");
    let triples = parse_turtle(fixture).expect("connector pack fixture parses");
    let parents = parent_classes();

    let orphans = ontology_orphan_classes(&[fixture], &parents);
    assert!(
        orphans.is_empty(),
        "fixture ontology has orphan classes: {orphans:?}"
    );

    let connector = "http://knuckles.team/test/connector-auto-onboard#connector";
    let connector_class = "http://knuckles.team/test/connector-auto-onboard#ConnectorIncident";
    let parent_class = format!("{KG}WorkItem");
    let instance = "http://knuckles.team/test/connector-auto-onboard#incident-1";
    let source = "http://knuckles.team/test/connector-auto-onboard#incidents";

    assert!(parents.contains(&parent_class));
    assert!(has_iri(
        &triples,
        connector_class,
        RDFS_SUBCLASS,
        &format!("{KG}Ticket")
    ));
    assert!(has_iri(&triples, instance, RDF_TYPE, connector_class));
    assert!(has_iri(
        &triples,
        instance,
        &format!("{CAT}fromSource"),
        source
    ));
    assert!(has_iri(
        &triples,
        source,
        &format!("{CP}ofConnector"),
        connector
    ));
    assert!(has_iri(
        &triples,
        source,
        &format!("{CP}servesClass"),
        connector_class
    ));
    assert!(has_iri(
        &triples,
        source,
        &format!("{CAT}hasField"),
        "http://knuckles.team/test/connector-auto-onboard#incident-id"
    ));

    assert_eq!(
        typed_subjects(&triples, &format!("{CAT}PromptAsset")),
        ["http://knuckles.team/test/connector-auto-onboard#system-prompt"]
            .into_iter()
            .collect()
    );
    assert_eq!(
        typed_subjects(&triples, &format!("{CAT}SkillAsset")),
        ["http://knuckles.team/test/connector-auto-onboard#incident-skill"]
            .into_iter()
            .collect()
    );
    assert_eq!(
        typed_subjects(&triples, &format!("{CAT}ToolAsset")),
        ["http://knuckles.team/test/connector-auto-onboard#search-incidents"]
            .into_iter()
            .collect()
    );
    assert_eq!(
        typed_subjects(&triples, &format!("{CAT}DataResource")),
        [source].into_iter().collect()
    );

    let expected = include_str!("fixtures/connector_auto_onboard/expected.tsv");
    let row: Vec<&str> = expected.trim().split('\t').collect();
    assert_eq!(
        row,
        vec![parent_class.as_str(), connector_class, instance, source],
        "expected answer row is the fixture's parent/connector/instance/source contract"
    );
}
