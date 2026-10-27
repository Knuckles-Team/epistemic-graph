//! Native ontology and OWL classification coverage for the Q6 workflow handoff edge.
#![cfg(feature = "owl")]

use eg_rdf::mapping::parse_turtle;
use eg_rdf::owl::Reasoner;
use eg_rdf::oxrdf::{NamedNode, Triple};

const KG: &str = "http://knuckles.team/kg#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";

fn documents() -> Vec<Triple> {
    [
        include_str!("../../eg-core/ontology/core-foundation-v1.ttl"),
        include_str!("../../eg-core/ontology/orchestration-v1.ttl"),
        include_str!("../../eg-core/ontology/capability-v1.ttl"),
    ]
    .into_iter()
    .flat_map(|document| parse_turtle(document).unwrap())
    .collect()
}

fn iri_triple(subject: &str, predicate: &str, object: &str) -> Triple {
    Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    )
}

#[test]
fn task_step_handoff_resolves_and_reasoner_accepts_it() {
    let mut triples = documents();
    let handoff = format!("{KG}handsOffTo");
    assert!(triples.contains(&iri_triple(
        &handoff,
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://www.w3.org/2002/07/owl#ObjectProperty",
    )));
    assert!(triples.contains(&iri_triple(
        &handoff,
        &format!("{RDFS}domain"),
        &format!("{KG}TaskStep"),
    )));
    assert!(triples.contains(&iri_triple(
        &handoff,
        &format!("{RDFS}range"),
        &format!("{KG}TaskStep"),
    )));
    triples.extend(
        parse_turtle(
            "@prefix : <http://knuckles.team/kg#> .
         :source a :TaskStep ; :handsOffTo :target . :target a :TaskStep .",
        )
        .unwrap(),
    );
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    assert!(classification.unsatisfiable.is_empty());
}
