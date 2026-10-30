//! Helpers shared by the ontology vocabulary tests.
#![cfg(feature = "rdf")]
#![allow(dead_code)]

use std::collections::BTreeSet;

use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, Triple};
use eg_rdf::source_selection::{question_abstention, NotUsedReason};

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

/// A fixture declaring each connector's access contract in the virtual-graph
/// vocabulary: one MCP `vg:SourceConnection` per source with a discovered
/// `vg:MetadataContract` and an approved `vg:VirtualMapping` per mapped class.
/// The connection is read-only (`vg:declaredCapability "read"`): nothing in it
/// asks the engine to hold a copy of the source.
#[cfg(feature = "sparql")]
pub fn connection_fixture(connections: &[(&str, &[&str])]) -> String {
    let mut ttl = String::from(
        "@prefix vg: <http://knuckles.team/kg/virtual-graph#> .\n\
         @prefix dc: <http://purl.org/dc/terms/> .\n\
         @prefix k: <urn:eg-cq-conn:contract/> .\n",
    );
    for (source, classes) in connections {
        ttl.push_str(&format!(
            "k:conn-{source} a vg:SourceConnection ; dc:identifier \"{source}\" ; \
             vg:sourceKind \"mcp\" ; vg:endpointRef k:ep-{source} ; vg:declaredCapability \"read\" .\n\
             k:ep-{source} a vg:EndpointRef ; vg:url \"mcp://{source}\" .\n\
             k:contract-{source} a vg:MetadataContract ; vg:contractSource k:conn-{source} ; \
             vg:discoveredEntity \"records\" .\n"
        ));
        for (index, class) in classes.iter().enumerate() {
            ttl.push_str(&format!(
                "k:map-{source}-{index} a vg:VirtualMapping ; vg:mapsContract k:contract-{source} ; \
                 vg:mapsEntity \"records\" ; vg:keyField \"id\" ; vg:approved true ; \
                 vg:mapsOntologyClass <{class}> .\n"
            ));
        }
    }
    ttl
}

/// One question of a `.3` acceptance row: the `(source, concept)` pairs it
/// reads (concepts are local names in the `KG` namespace) and the `.1`/`.2`
/// test that runs it over its fixture and asserts the non-empty,
/// provenance-carrying answer.
#[cfg(feature = "sparql")]
pub struct AcceptanceQuestion<'a> {
    pub id: &'a str,
    pub required: &'a [(&'a str, &'a str)],
    pub answer: fn(),
}

/// The `.3` acceptance proof for one connector row (`EG-CQ-CONN-Rnnn.3`).
///
/// Every source the questions read is declared through an approved virtual
/// mapping onto the concept it serves (no copy capability), then:
/// - with all sources connected, no question abstains and each question
///   answers (non-empty, provenance-carrying) through its own test;
/// - with any one required source unconnected, the question returns the typed
///   abstention (`eg_rdf::source_selection::question_abstention`) naming
///   exactly that source with the closed reason `Unconnected`, and uses no
///   source — never an empty success.
#[cfg(feature = "sparql")]
pub fn assert_acceptance(questions: &[AcceptanceQuestion<'_>]) {
    use std::collections::BTreeMap;

    use eg_rdf::source_selection::{
        connected_sources_from_turtle, question_abstention, NotUsedReason,
    };

    let mut mapped: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for (source, concept) in questions.iter().flat_map(|q| q.required) {
        let classes = mapped.entry(source).or_default();
        let iri = format!("{KG}{concept}");
        if !classes.contains(&iri) {
            classes.push(iri);
        }
    }
    let declare = |omit: Option<&str>| {
        let connections: Vec<(&str, Vec<&str>)> = mapped
            .iter()
            .filter(|(source, _)| Some(**source) != omit)
            .map(|(source, classes)| (*source, classes.iter().map(String::as_str).collect()))
            .collect();
        let borrowed: Vec<(&str, &[&str])> = connections
            .iter()
            .map(|(source, classes)| (*source, classes.as_slice()))
            .collect();
        connected_sources_from_turtle(&[&connection_fixture(&borrowed)]).unwrap()
    };

    let all = declare(None);
    assert_eq!(
        all.len(),
        mapped.len(),
        "every source is connected: {all:?}"
    );
    for question in questions {
        let pairs: Vec<(&str, String)> = question
            .required
            .iter()
            .map(|(source, concept)| (*source, format!("{KG}{concept}")))
            .collect();
        let pairs: Vec<(&str, &str)> = pairs.iter().map(|(s, c)| (*s, c.as_str())).collect();
        assert!(
            question_abstention(&pairs, &all).is_none(),
            "{}: every required source is connected, so it must not abstain",
            question.id
        );
        (question.answer)();

        for (omitted, _) in &pairs {
            let connected = declare(Some(omitted));
            let abstention = question_abstention(&pairs, &connected)
                .unwrap_or_else(|| panic!("{}: {omitted} unconnected must abstain", question.id));
            abstention
                .validate()
                .expect("the abstention is a well-formed wire value");
            assert!(
                abstention.used.is_empty(),
                "{}: {abstention:?}",
                question.id
            );
            let named: Vec<(&str, &str)> = abstention
                .not_used
                .iter()
                .map(|entry| {
                    assert_eq!(entry.reason, Some(NotUsedReason::Unconnected));
                    (entry.source.as_str(), entry.concept.as_str())
                })
                .collect();
            let expected: Vec<(&str, &str)> = pairs
                .iter()
                .copied()
                .filter(|(source, _)| source == omitted)
                .collect();
            assert_eq!(
                named, expected,
                "{}: names the unconnected source",
                question.id
            );
        }
    }
}

/// Declared access contracts for the named sources: a read-only
/// `vg:SourceConnection` (nothing in it asks the engine to hold a copy of the
/// source) and a discovered `vg:MetadataContract` per source, with no
/// approved `vg:VirtualMapping` — the ontology terms a mapping would target
/// are not on main yet, so none of the sources is connected.
#[cfg(feature = "sparql")]
pub fn unconnected_contract_fixture(sources: &[&str]) -> String {
    let mut ttl = String::from(
        "@prefix vg: <http://knuckles.team/kg/virtual-graph#> .\n\
         @prefix dc: <http://purl.org/dc/terms/> .\n\
         @prefix k: <urn:eg-cq-conn:contract/> .\n",
    );
    for source in sources {
        ttl.push_str(&format!(
            "k:conn-{source} a vg:SourceConnection ; dc:identifier \"{source}\" ; \
             vg:sourceKind \"mcp\" ; vg:endpointRef k:ep-{source} ; \
             vg:declaredCapability \"read\" .\n\
             k:ep-{source} a vg:EndpointRef ; vg:url \"mcp://{source}\" .\n\
             k:contract-{source} a vg:MetadataContract ; \
             vg:contractSource k:conn-{source} ; vg:discoveredEntity \"records\" .\n"
        ));
    }
    ttl
}

/// The typed-abstention proof for one question row whose required sources are
/// all unconnected: the engine's source selection must abstain — naming
/// exactly the question's `(source, concept)` pairs with the closed
/// `Unconnected` reason and using no source. Never an empty success
/// (`specs/connector-competency/test-spec.md`).
#[cfg(feature = "sparql")]
pub fn assert_unconnected_question(
    id: &str,
    connected: &BTreeSet<String>,
    required: &[(&str, &str)],
) {
    let abstention = question_abstention(required, connected)
        .unwrap_or_else(|| panic!("{id}: an unconnected question must abstain, never return an empty success"));
    abstention
        .validate()
        .expect("the typed abstention is a well-formed wire value");
    assert!(
        abstention.used.is_empty(),
        "{id}: the abstention uses no source: {abstention:?}"
    );
    let named: Vec<(&str, &str)> = abstention
        .not_used
        .iter()
        .map(|entry| {
            assert_eq!(
                entry.reason,
                Some(NotUsedReason::Unconnected),
                "{id}: the closed Unconnected reason: {abstention:?}"
            );
            (entry.source.as_str(), entry.concept.as_str())
        })
        .collect();
    let expected: Vec<(&str, &str)> = required.to_vec();
    assert_eq!(
        named, expected,
        "{id}: names exactly the unconnected sources: {abstention:?}"
    );
}
