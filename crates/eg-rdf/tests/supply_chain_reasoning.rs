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
    assert!(
        classification.consistent,
        "the shared vocabulary classifies consistently"
    );
    assert!(classification.entails_subclass(&format!("<{KG}Supplier>"), &format!("<{KG}Vendor>")));
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

// spec: EG-SUPPLY-VOCAB-R002.1 / EG-TYPED-PACKS-R105.7
#[test]
fn module_version_edges_retain_the_shared_version_query_predicate() {
    let mut triples = parse_all(&[
        include_str!("../../eg-core/ontology/dataplatform-v1.ttl"),
        include_str!("../../eg-core/ontology/supply_chain-v1.ttl"),
    ]);
    triples.extend(
        parse_turtle(
            r#"
            @prefix : <http://knuckles.team/kg#> .
            @prefix ex: <http://knuckles.team/test/version-ownership#> .

            ex:datasetVersion a :DatasetVersion ;
                :dataplatformVersionOf ex:dataset .
            ex:dataset a :Dataset .
            ex:componentVersion a :ComponentVersion ;
                :supplyChainVersionOf ex:component .
            ex:component a :SoftwareComponent .
            ex:legacyVersion a :ComponentVersion ;
                :versionOf ex:legacyComponent .
            ex:legacyComponent a :SoftwareComponent .
            "#,
        )
        .unwrap(),
    );
    let entailed = saturate_object_properties(&triples);
    let generic = format!("<{KG}versionOf>");
    for (subject, object, local) in [
        (
            "<http://knuckles.team/test/version-ownership#datasetVersion>",
            "<http://knuckles.team/test/version-ownership#dataset>",
            format!("<{KG}dataplatformVersionOf>"),
        ),
        (
            "<http://knuckles.team/test/version-ownership#componentVersion>",
            "<http://knuckles.team/test/version-ownership#component>",
            format!("<{KG}supplyChainVersionOf>"),
        ),
    ] {
        assert!(entailed.iter().any(|fact| {
            fact.subject == subject && fact.predicate == local && fact.object == object
        }));
        let Some(shared) = entailed.iter().find(|fact| {
            fact.subject == subject && fact.predicate == generic && fact.object == object
        }) else {
            panic!("{local} must remain queryable as shared {generic}");
        };
        assert!(
            !shared.asserted,
            "shared edge must be inferred from the local sub-property"
        );
        assert_eq!(shared.rule, "RL-subPropertyOf");
    }
    assert!(entailed.iter().any(|fact| {
        fact.subject == "<http://knuckles.team/test/version-ownership#legacyVersion>"
            && fact.predicate == generic
            && fact.object == "<http://knuckles.team/test/version-ownership#legacyComponent>"
            && fact.asserted
    }));
}
