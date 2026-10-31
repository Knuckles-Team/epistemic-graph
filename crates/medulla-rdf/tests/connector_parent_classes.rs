//! Parent classes for the connector concepts that recur across the fleet
//! (MEDULLA-TYPED-PACKS-R105.3).
#![cfg(feature = "owl")]

mod common;

use common::{has_iri, parse_all, BFO, KG, RDFS};
use medulla_rdf::owl::Reasoner;
use medulla_rdf::oxrdf::{NamedNode, Term, Triple};

const INFRA: &str = "http://knuckles.team/kg/infrastructure#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
const OWL_EQUIVALENT_CLASS: &str = "http://www.w3.org/2002/07/owl#equivalentClass";
const ALT_LABEL: &str = "http://www.w3.org/2004/02/skos/core#altLabel";

fn documents() -> Vec<Triple> {
    parse_all(&[
        include_str!("../../medulla-core/ontology/core-foundation-v1.ttl"),
        include_str!("../../medulla-core/ontology/software-v1.ttl"),
        include_str!("../../medulla-core/ontology/sdlc_lifecycle-v1.ttl"),
        include_str!("../../medulla-core/ontology/company-v1.ttl"),
        include_str!("../../medulla-core/ontology/infrastructure-v1.ttl"),
        include_str!("../../medulla-core/ontology/supply_chain-v1.ttl"),
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

// spec: MEDULLA-TYPED-PACKS-R105.3
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

// spec: MEDULLA-TYPED-PACKS-R105.3.1
#[test]
fn source_record_classes_have_narrow_parents() {
    let triples = documents();
    let source_record = format!("{KG}SourceRecord");
    let opaque_reference = format!("{KG}OpaqueSourceReference");
    let bfo_gdc = format!("{BFO}0000031");
    let expected = [
        (source_record.clone(), bfo_gdc.clone()),
        (opaque_reference.clone(), source_record.clone()),
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
        assert!(!triples.iter().any(|triple| {
            triple.subject == NamedNode::new(class).unwrap().into()
                && triple.predicate.as_str() == OWL_EQUIVALENT_CLASS
        }));
    }

    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    assert!(classification.entails_subclass(
        &format!("<{opaque_reference}>"),
        &format!("<{source_record}>"),
    ));
    assert!(
        classification.entails_subclass(&format!("<{source_record}>"), &format!("<{bfo_gdc}>"),)
    );
    assert!(
        classification.entails_subclass(&format!("<{opaque_reference}>"), &format!("<{bfo_gdc}>"),)
    );

    for class in [&source_record, &opaque_reference] {
        for forbidden in [
            "AssetOccurrence",
            "Credential",
            "Document",
            "ExternalGraphReference",
            "Person",
            "SourceObject",
        ] {
            assert!(
                !classification
                    .entails_subclass(&format!("<{class}>"), &format!("<{KG}{forbidden}>"),),
                "{class} must not inherit {forbidden}"
            );
        }
    }
}
