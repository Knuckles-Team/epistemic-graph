//! Native parser and OWL classification contracts for foundational orchestration classes.
#![cfg(feature = "owl")]

use eg_rdf::mapping::parse_turtle;
use eg_rdf::owl::Reasoner;
use eg_rdf::oxrdf::{NamedNode, Triple};

const KG: &str = "http://knuckles.team/kg#";
const ENTITY: &str = "http://purl.obolibrary.org/obo/BFO_0000001";
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

// spec: EG-ORCH-VOCAB-R001.1
#[test]
fn task_step_resolves_and_precedes_accepts_it() {
    let mut triples = documents();
    assert!(triples.contains(&iri_triple(
        &format!("{KG}TaskStep"),
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://www.w3.org/2002/07/owl#Class",
    )));
    for predicate in ["domain", "range"] {
        assert!(triples.contains(&iri_triple(
            &format!("{KG}precedes"),
            &format!("{RDFS}{predicate}"),
            ENTITY,
        )));
    }
    triples.extend(
        parse_turtle(
            "@prefix : <http://knuckles.team/kg#> .
         :first a :TaskStep ; :precedes :second . :second a :TaskStep .",
        )
        .unwrap(),
    );
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    assert!(classification.unsatisfiable.is_empty());
    assert!(classification.entails_subclass(&format!("<{KG}TaskStep>"), &format!("<{ENTITY}>")));
    assert!(!classification.entails_subclass(
        &format!("<{KG}TaskStep>"),
        &format!("<{KG}DevelopmentStandard>")
    ));
}

// spec: EG-ORCH-VOCAB-R004.1
#[test]
fn best_practice_is_a_development_standard() {
    let triples = documents();
    assert!(triples.contains(&iri_triple(
        &format!("{KG}BestPractice"),
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://www.w3.org/2002/07/owl#Class",
    )));
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    assert!(classification.entails_subclass(
        &format!("<{KG}BestPractice>"),
        &format!("<{KG}DevelopmentStandard>")
    ));
    assert!(!classification.entails_subclass(
        &format!("<{KG}DevelopmentStandard>"),
        &format!("<{KG}BestPractice>")
    ));
    assert!(!classification
        .entails_subclass(&format!("<{KG}BestPractice>"), &format!("<{KG}TaskStep>")));
}

// spec: EG-ORCH-VOCAB-R001.1, EG-ORCH-VOCAB-R004.1
#[test]
fn contradictory_parent_mutations_are_detected() {
    for (class, parent) in [
        (format!("{KG}TaskStep"), ENTITY.to_string()),
        (
            format!("{KG}BestPractice"),
            format!("{KG}DevelopmentStandard"),
        ),
    ] {
        let mut triples = documents();
        triples.push(iri_triple(
            &class,
            "http://www.w3.org/2002/07/owl#disjointWith",
            &parent,
        ));
        let classification = Reasoner::from_triples(&triples).classify();
        assert!(!classification.consistent);
        assert!(classification.unsatisfiable.contains(&format!("<{class}>")));
    }
}
