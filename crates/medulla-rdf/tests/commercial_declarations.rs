//! Native-parser and reasoner contract for the commercial vocabulary
//! (MEDULLA-SUPPLY-VOCAB-R003.1/R003.10, MEDULLA-CQ-XSRC-R008.3.1).
#![cfg(feature = "owl")]

mod common;

use common::{has_iri, parse_all, KG, RDFS};
use medulla_rdf::owl::Reasoner;
use medulla_rdf::oxrdf::{NamedNode, Term, Triple};

const INFRA: &str = "http://knuckles.team/kg/infrastructure#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn documents() -> Vec<Triple> {
    parse_all(&[
        include_str!("../../medulla-core/ontology/core-foundation-v1.ttl"),
        include_str!("../../medulla-core/ontology/archimate-v1.ttl"),
        include_str!("../../medulla-core/ontology/commercial-v1.ttl"),
    ])
}

// spec: MEDULLA-SUPPLY-VOCAB-R003.1
#[test]
fn customer_entails_organization_and_business_actor() {
    let triples = documents();
    let customer = format!("{KG}Customer");
    assert!(has_iri(
        &triples,
        &customer,
        RDF_TYPE,
        &format!("{OWL}Class")
    ));
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
    assert!(
        classification.consistent,
        "the commercial vocabulary classifies consistently"
    );
    assert!(
        classification.entails_subclass(&format!("<{customer}>"), &format!("<{KG}Organization>"))
    );
}

// spec: MEDULLA-SUPPLY-VOCAB-R003.10
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

// spec: MEDULLA-CQ-XSRC-R008.3.1
// spec: MEDULLA-SUPPLY-VOCAB-R003.6
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
    // The MEDULLA-SUPPLY-VOCAB amendment (plan.md D3, recorded by R003.6 after both
    // PRs) replaces the commercial `:ChangeRequest` plan with the existing
    // `kg:Change`. This fixture is intentionally scoped to the commercial
    // documents below; service_operations-v1.ttl retains its distinct
    // `kg:ChangeRequest` for IT4IT `:touches` semantics.
    let change_request = NamedNode::new(format!("{KG}ChangeRequest")).unwrap();
    assert!(
        !triples
            .iter()
            .any(|triple| triple.subject == change_request.clone().into()),
        "commercial vocabulary does not redeclare kg:ChangeRequest"
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
    assert!(
        has_iri(
            &triples,
            &format!("{KG}Change"),
            &format!("{RDFS}subClassOf"),
            &format!("{KG}BusinessProcess")
        ),
        "the reused kg:Change retains its canonical BusinessProcess hierarchy"
    );
}

// spec: MEDULLA-SUPPLY-VOCAB-R003.2
#[test]
fn customer_contract_terms_listed_with_domain_and_range() {
    let triples = documents();
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    // :CustomerContract and :coversService are already declared; this row
    // completes the term set with the customer, status and end terms.
    assert!(has_iri(
        &triples,
        &format!("{KG}contractCustomer"),
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}contractCustomer"),
        &domain,
        &format!("{KG}CustomerContract")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}contractCustomer"),
        &range,
        &format!("{KG}Customer")
    ));
    for property in ["contractStatus", "contractEnd"] {
        assert!(has_iri(
            &triples,
            &format!("{KG}{property}"),
            RDF_TYPE,
            &format!("{OWL}DatatypeProperty")
        ));
        assert!(has_iri(
            &triples,
            &format!("{KG}{property}"),
            &domain,
            &format!("{KG}CustomerContract")
        ));
    }
    assert!(has_iri(
        &triples,
        &format!("{KG}contractStatus"),
        &range,
        &format!("{XSD}string")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}contractEnd"),
        &range,
        &format!("{XSD}dateTime")
    ));
}

// spec: MEDULLA-SUPPLY-VOCAB-R003.4
#[test]
fn supports_capability_listed_with_domain_and_range() {
    let triples = documents();
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    assert!(has_iri(
        &triples,
        &format!("{KG}supportsCapability"),
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}supportsCapability"),
        &domain,
        &format!("{INFRA}Service")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}supportsCapability"),
        &range,
        &format!("{KG}BusinessCapability")
    ));
}

// spec: MEDULLA-SUPPLY-VOCAB-R003.8
#[test]
fn purchase_order_listed_with_exactly_one_declaration() {
    let triples = documents();
    assert!(has_iri(
        &triples,
        &format!("{KG}PurchaseOrder"),
        RDF_TYPE,
        &format!("{OWL}Class")
    ));
    // The grep check: exactly one class declaration for the term across the
    // parsed commercial documents.
    let subject = NamedNode::new(format!("{KG}PurchaseOrder")).unwrap();
    let owl_class = format!("{OWL}Class");
    let declarations = triples
        .iter()
        .filter(|triple| {
            triple.subject == subject.clone().into()
                && triple.predicate.as_str() == RDF_TYPE
                && matches!(&triple.object, Term::NamedNode(n) if n.as_str() == owl_class)
        })
        .count();
    assert_eq!(declarations, 1, "PurchaseOrder is declared exactly once");
}
