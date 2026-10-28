//! Native parser, reasoner and shape contracts for the shared supply-chain
//! vocabulary (EG-SUPPLY-VOCAB).

use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};

const KG: &str = "http://knuckles.team/kg#";
const INFRA: &str = "http://knuckles.team/kg/infrastructure#";
const EX: &str = "http://knuckles.team/test/supply-chain#";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

/// Canonical node id for a subject term (`<iri>` or `_:bnode`).
fn subject_key(triple: &Triple) -> String {
    match &triple.subject {
        NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
        NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
        #[allow(unreachable_patterns)]
        _ => String::new(),
    }
}

/// Canonical node id for an object term; `None` for a literal.
fn object_key(triple: &Triple) -> Option<String> {
    match &triple.object {
        Term::NamedNode(n) => Some(format!("<{}>", n.as_str())),
        Term::BlankNode(b) => Some(format!("_:{}", b.as_str())),
        _ => None,
    }
}

fn load_supply_chain() -> Vec<Triple> {
    parse_turtle(include_str!("../ontology/supply_chain-v1.ttl"))
        .expect("the supply-chain vocabulary parses")
}

// The TBox the physical chain closes across: the shared foundation, the TRM
// vendor/asset classes, the infrastructure runtime and the supply-chain module
// itself (the EG-CQ-XSRC-R002 document set).
fn physical_documents() -> Vec<Triple> {
    [
        include_str!("../ontology/core-foundation-v1.ttl"),
        include_str!("../ontology/trm-v1.ttl"),
        include_str!("../ontology/infrastructure-v1.ttl"),
        include_str!("../ontology/supply_chain-v1.ttl"),
    ]
    .iter()
    .flat_map(|document| parse_turtle(document).unwrap())
    .collect()
}

// spec: EG-SUPPLY-VOCAB-R001.3
#[test]
fn physical_chain_terms_declared() {
    let triples = load_supply_chain();
    assert!(has_iri(
        &triples,
        &format!("{INFRA}serviceRunsOn"),
        &format!("{RDF}type"),
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{INFRA}serviceRunsOn"),
        &format!("{RDFS}subPropertyOf"),
        &format!("{INFRA}runsOn")
    ));
    assert!(has_iri(
        &triples,
        &format!("{INFRA}serviceRunsOn"),
        &format!("{RDFS}domain"),
        &format!("{INFRA}Service")
    ));
    assert!(has_iri(
        &triples,
        &format!("{INFRA}serviceRunsOn"),
        &format!("{RDFS}range"),
        &format!("{INFRA}Host")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}suppliesService"),
        &format!("{RDF}type"),
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}suppliesService"),
        &format!("{RDFS}domain"),
        &format!("{KG}Supplier")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}suppliesService"),
        &format!("{RDFS}range"),
        &format!("{INFRA}Service")
    ));
    // The property-chain axiom names the four physical links in order, the
    // last one the inverse of serviceRunsOn.
    let list_iri = triples
        .iter()
        .find(|triple| {
            subject_key(triple) == format!("<{KG}suppliesService>")
                && triple
                    .predicate
                    .as_str()
                    .ends_with("propertyChainAxiom")
        })
        .and_then(object_key);
    let Some(head) = list_iri else {
        panic!("suppliesService declares a propertyChainAxiom list");
    };
    let mut members: Vec<String> = Vec::new();
    let mut cursor = Some(head);
    while let Some(node) = cursor {
        let member = triples
            .iter()
            .find(|triple| subject_key(triple) == node && triple.predicate.as_str().ends_with("first"))
            .and_then(object_key)
            .expect("every chain-list head carries a first");
        members.push(member);
        // A list cell carries a rest; the final member does not.
        cursor = triples
            .iter()
            .find(|triple| subject_key(triple) == node && triple.predicate.as_str().ends_with("rest"))
            .and_then(object_key)
            .filter(|next| !next.ends_with("#nil>"));
    }
    assert!(
        members.iter().any(|m| m.ends_with("#suppliesItem>")),
        "chain starts with suppliesItem: {members:?}"
    );
    assert!(
        members.iter().any(|m| m.ends_with("#realizedByAsset>")),
        "chain carries realizedByAsset: {members:?}"
    );
    assert!(
        members.iter().any(|m| m.ends_with("#assetHost>")),
        "chain carries assetHost: {members:?}"
    );
    let inverse_member = members
        .iter()
        .find(|m| m.starts_with("_:"))
        .expect("the last chain member is the blank-node inverse link");
    let inverse_of = triples
        .iter()
        .find(|triple| subject_key(triple) == *inverse_member && triple.predicate.as_str().ends_with("inverseOf"))
        .expect("the blank-node chain member declares its inverseOf");
    match &inverse_of.object {
        Term::NamedNode(n) => assert_eq!(n.as_str(), format!("{INFRA}serviceRunsOn")),
        other => panic!("inverse target is not an IRI: {other:?}"),
    }
}

// spec: EG-SUPPLY-VOCAB-R001.3
#[test]
fn physical_chain_infers_supplies_service() {
    let mut triples = physical_documents();
    triples.extend(
        parse_turtle(include_str!(
            "fixtures/supply_chain_service_chain.ttl"
        ))
        .expect("the physical-chain fixture parses"),
    );
    let supplier = format!("<{EX}acmeSupplier>");
    let service = format!("<{EX}payrollService>");
    let chain = format!("<{KG}suppliesService>");
    let entailed = eg_rdf::owl::saturate_object_properties(&triples);
    let Some(derived) = entailed
        .iter()
        .find(|fact| fact.subject == supplier && fact.predicate == chain && fact.object == service)
    else {
        panic!("the four-edge service path must derive {supplier} {chain} {service}");
    };
    assert!(!derived.asserted, "the edge is inferred, not asserted");
    assert_eq!(derived.rule, "RL-propertyChain");
}

// spec: EG-SUPPLY-VOCAB-R001.3
#[test]
fn container_only_runs_on_neither_infers_nor_retires() {
    let mut triples = physical_documents();
    triples.extend(
        parse_turtle(
            r#"
            @prefix : <http://knuckles.team/kg#> .
            @prefix infra: <http://knuckles.team/kg/infrastructure#> .
            @prefix ex: <http://knuckles.team/test/supply-chain#> .

            ex:sidecarContainer a infra:Container .
            ex:sidecarContainer infra:runsOn ex:rackHost .
            ex:sidecarPod a infra:Pod .
            ex:sidecarPod infra:podRunsOn ex:rackHost .
            ex:rackHost a infra:Host .
            "#,
        )
        .expect("the container-only fixture parses"),
    );
    let entailed = eg_rdf::owl::saturate_object_properties(&triples);
    let chain = format!("<{KG}suppliesService>");
    assert!(
        !entailed.iter().any(|fact| fact.predicate == chain),
        "a container-only or pod-only runsOn path must not infer suppliesService"
    );
    // Neither runtime entity is retyped as a Service by the chain's domains.
    let mut asserted: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    asserted
        .entry(format!("<{EX}sidecarContainer>"))
        .or_default()
        .insert(format!("<{INFRA}Container>"));
    asserted
        .entry(format!("<{EX}sidecarPod>"))
        .or_default()
        .insert(format!("<{INFRA}Pod>"));
    let classification = eg_rdf::owl::Reasoner::from_triples(&triples).classify();
    let service = format!("<{INFRA}Service>");
    assert!(
        eg_rdf::owl::instances_of(&classification, &asserted, &service).is_empty(),
        "a container-only or pod-only runsOn path must not retype its endpoint as a Service"
    );
}
