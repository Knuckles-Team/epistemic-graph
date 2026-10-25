//! Native reasoner acceptance for the shared supply-chain vocabulary: subclass
//! entailment across the kg/infrastructure modules and the `usesComponent`
//! runtime chain (EG-CQ-XSRC-R002, EG-SUPPLY-VOCAB-R002).
#![cfg(feature = "owl")]

mod common;

use common::parse_all;
use eg_rdf::mapping::parse_turtle;
use eg_rdf::owl::{saturate_object_properties, Reasoner};
use eg_rdf::oxrdf::Triple;

const KG: &str = "http://knuckles.team/kg#";
const EX: &str = "http://knuckles.team/test/supply-chain#";

fn documents() -> Vec<Triple> {
    parse_all(&[
        include_str!("../../eg-core/ontology/trm-v1.ttl"),
        include_str!("../../eg-core/ontology/infrastructure-v1.ttl"),
        include_str!("../../eg-core/ontology/supply_chain-v1.ttl"),
    ])
}

// spec: EG-CQ-XSRC-R002.1
#[test]
fn supplier_entails_vendor_under_the_native_reasoner() {
    let triples = documents();
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent, "the shared vocabulary classifies consistently");
    assert!(classification
        .entails_subclass(&format!("<{KG}Supplier>"), &format!("<{KG}Vendor>")));
}

// spec: EG-SUPPLY-VOCAB-R002.5
#[test]
fn uses_component_chain_infers_the_runtime_path() {
    let mut triples = documents();
    triples.extend(
        parse_turtle(include_str!(
            "../../eg-core/tests/fixtures/supply_chain_runtime_chain.ttl"
        ))
        .unwrap(),
    );
    // PropertyEntailment keys use the `<iri>` node-id convention.
    let service = format!("<{EX}checkoutService>");
    let version = format!("<{EX}openssl111k>");
    let chain = format!("<{KG}usesComponent>");
    let entailed = saturate_object_properties(&triples);
    let derived = entailed
        .iter()
        .find(|fact| fact.subject == service && fact.predicate == chain && fact.object == version);
    let Some(derived) = derived else {
        panic!("the runtime chain must derive {service} {chain} {version}");
    };
    assert!(!derived.asserted, "the edge is inferred, not asserted");
    assert_eq!(derived.rule, "RL-propertyChain");
}
