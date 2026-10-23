//! Parent classes for the connector concepts that recur across the fleet
//! (EG-TYPED-PACKS-R105.3).
#![cfg(feature = "owl")]

mod common;

use common::{has_iri, parse_all, BFO, KG, RDFS};
use eg_rdf::owl::Reasoner;
use eg_rdf::oxrdf::{NamedNode, Term, Triple};

const INFRA: &str = "http://knuckles.team/kg/infrastructure#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
const ALT_LABEL: &str = "http://www.w3.org/2004/02/skos/core#altLabel";

fn documents() -> Vec<Triple> {
    parse_all(&[
        include_str!("../../eg-core/ontology/core-foundation-v1.ttl"),
        include_str!("../../eg-core/ontology/software-v1.ttl"),
        include_str!("../../eg-core/ontology/sdlc_lifecycle-v1.ttl"),
        include_str!("../../eg-core/ontology/company-v1.ttl"),
        include_str!("../../eg-core/ontology/infrastructure-v1.ttl"),
        include_str!("../../eg-core/ontology/supply_chain-v1.ttl"),
    ])
}

fn has_literal(triples: &[Triple], subject: &str, predicate: &str) -> bool {
    let subject = NamedNode::new(subject).unwrap();
    triples.iter().any(|triple| {
        triple.subject == subject.clone().into()
            && triple.predicate.as_str() == predicate
            && matches!(triple.object, Term::Literal(_))
    })
}

// spec: EG-TYPED-PACKS-R105.3
#[test]
fn recurring_connector_concepts_sit_under_their_upper_classes() {
    let triples = documents();
    let bfo = |id: &str| format!("{BFO}{id}");
    let expected: Vec<(String, String)> = vec![
        (format!("{KG}Collection"), bfo("0000031")),
        (format!("{KG}Folder"), format!("{KG}Collection")),
        (format!("{KG}ResourceGroup"), format!("{KG}Collection")),
        (format!("{KG}Category"), format!("{KG}Concept")),
        (format!("{KG}Template"), bfo("0000031")),
        (format!("{KG}Message"), bfo("0000031")),
        (format!("{KG}Order"), bfo("0000031")),
        (format!("{KG}Metric"), bfo("0000031")),
        (format!("{KG}Repository"), format!("{KG}Collection")),
        (format!("{KG}Branch"), bfo("0000031")),
        (format!("{KG}Release"), bfo("0000031")),
        (format!("{KG}Job"), bfo("0000015")),
        (format!("{KG}Runner"), bfo("0000004")),
        (format!("{KG}Iteration"), bfo("0000015")),
        (format!("{KG}Milestone"), bfo("0000031")),
        (format!("{INFRA}Namespace"), bfo("0000004")),
        (format!("{INFRA}Volume"), bfo("0000004")),
        (format!("{INFRA}Network"), bfo("0000004")),
        (format!("{KG}Vulnerability"), bfo("0000031")),
    ];
    for (class, parent) in &expected {
        assert!(has_iri(&triples, class, RDF_TYPE, OWL_CLASS), "{class}");
        assert!(
            has_iri(&triples, class, &format!("{RDFS}subClassOf"), parent),
            "{class} under {parent}"
        );
        for predicate in [
            format!("{RDFS}label"),
            format!("{RDFS}comment"),
            ALT_LABEL.to_string(),
        ] {
            assert!(
                has_literal(&triples, class, &predicate),
                "{class} {predicate}"
            );
        }
    }
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    for (class, _) in &expected {
        assert!(!classification.unsatisfiable.contains(&format!("<{class}>")));
    }
    assert!(
        classification.entails_subclass(&format!("<{KG}Repository>"), &format!("<{BFO}0000031>"))
    );
}
