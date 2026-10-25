//! Native-parser and reasoner contract for the commercial vocabulary
//! (EG-SUPPLY-VOCAB-R003.1/R003.10, EG-CQ-XSRC-R008.3.1).
#![cfg(feature = "owl")]

mod common;

use common::{has_iri, parse_all, KG, RDFS};
use eg_rdf::owl::Reasoner;
use eg_rdf::oxrdf::{NamedNode, Term, Triple};

const INFRA: &str = "http://knuckles.team/kg/infrastructure#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL: &str = "http://www.w3.org/2002/07/owl#";

fn documents() -> Vec<Triple> {
    parse_all(&[
        include_str!("../../eg-core/ontology/core-foundation-v1.ttl"),
        include_str!("../../eg-core/ontology/archimate-v1.ttl"),
        include_str!("../../eg-core/ontology/commercial-v1.ttl"),
    ])
}

// spec: EG-SUPPLY-VOCAB-R003.1
#[test]
fn customer_entails_organization_and_business_actor() {
    let triples = documents();
    let customer = format!("{KG}Customer");
    assert!(has_iri(&triples, &customer, RDF_TYPE, &format!("{OWL}Class")));
    assert!(has_iri(
        &triples,
        &customer,
        &format!("{RDFS}subClassOf"),
        &format!("{KG}Organization")
    ));
    assert!(has_iri(
        &triples,
        &customer,
        &format!("{RDFS}subClassOf"),
        &format!("{KG}BusinessActor")
    ));
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent, "the commercial vocabulary classifies consistently");
    assert!(classification.entails_subclass(
        &format!("<{customer}>"),
        &format!("<{KG}Organization>")
    ));
}

// spec: EG-SUPPLY-VOCAB-R003.10
#[test]
fn opportunity_terms_listed_with_domain_and_range() {
    let triples = documents();
    assert!(has_iri(
        &triples,
        &format!("{KG}Opportunity"),
        RDF_TYPE,
        &format!("{OWL}Class")
    ));
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    assert!(has_iri(
        &triples,
        &format!("{KG}opportunityCustomer"),
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}opportunityCustomer"),
        &domain,
        &format!("{KG}Opportunity")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}opportunityCustomer"),
        &range,
        &format!("{KG}Customer")
    ));
}

// spec: EG-CQ-XSRC-R008.3.1
#[test]
fn customer_contract_and_covers_service_declared() {
    let triples = documents();
    assert!(has_iri(
        &triples,
        &format!("{KG}CustomerContract"),
        RDF_TYPE,
        &format!("{OWL}Class")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}coversService"),
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}coversService"),
        &format!("{RDFS}domain"),
        &format!("{KG}CustomerContract")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}coversService"),
        &format!("{RDFS}range"),
        &format!("{INFRA}Service")
    ));
    // The EG-SUPPLY-VOCAB amendment (plan.md D3, recorded by R003.6 after both
    // PRs) replaces the planned `:ChangeRequest` with the existing `kg:Change`;
    // no ChangeRequest class is declared.
    let change_request = NamedNode::new(format!("{KG}ChangeRequest")).unwrap();
    assert!(
        !triples
            .iter()
            .any(|triple| triple.subject == change_request.clone().into()),
        "no ChangeRequest class exists"
    );
    let change = NamedNode::new(format!("{KG}Change")).unwrap();
    let owl_class = format!("{OWL}Class");
    assert!(
        triples.iter().any(|triple| {
            triple.subject == change.clone().into()
                && triple.predicate.as_str() == RDF_TYPE
                && matches!(&triple.object, Term::NamedNode(n) if n.as_str() == owl_class)
        }),
        "kg:Change (the ServiceNow change request) is the reused change class"
    );
}
