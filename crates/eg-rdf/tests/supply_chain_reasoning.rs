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
        include_str!("../../eg-core/ontology/core-foundation-v1.ttl"),
        include_str!("../../eg-core/ontology/sdd-v1.ttl"),
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

// spec: EG-CQ-XSRC-R002.2, EG-SUPPLY-VOCAB-R002.2
#[test]
fn container_image_entails_software_component_under_the_native_reasoner() {
    let triples = documents();
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(
        classification.consistent,
        "the SDD, infrastructure and supply-chain vocabularies classify consistently: {:?}",
        classification.unsatisfiable
    );
    assert!(classification.entails_subclass(
        "<http://knuckles.team/kg/infrastructure#ContainerImage>",
        "<http://knuckles.team/kg#SoftwareComponent>"
    ));
}

// spec: EG-CQ-XSRC-R004.1
#[test]
fn supplies_service_infers_supplier_to_service_from_the_software_path() {
    let mut triples = documents();
    triples.extend(
        parse_turtle(include_str!(
            "../../eg-core/tests/fixtures/supply_chain_runtime_chain.ttl"
        ))
        .unwrap(),
    );
    // The fixture asserts the shared `versionOf` link; the software chain
    // needs the supply-chain module property between version and component.
    triples.extend(
        parse_turtle(
            r#"
            @prefix : <http://knuckles.team/kg#> .
            @prefix ex: <http://knuckles.team/test/supply-chain#> .

            ex:openssl111k :supplyChainVersionOf ex:openssl .
            "#,
        )
        .unwrap(),
    );
    let service = format!("<{EX}checkoutService>");
    let supplier = format!("<{EX}opensslSupplier>");
    let entailed = saturate_object_properties(&triples);
    let inbound = entailed.iter().find(|fact| {
        fact.subject == service
            && fact.predicate == format!("<{KG}serviceSupplier>")
            && fact.object == supplier
    });
    let Some(inbound) = inbound else {
        panic!("the software chain must derive {service} serviceSupplier {supplier}");
    };
    assert!(!inbound.asserted, "the edge is inferred, not asserted");
    assert_eq!(inbound.rule, "RL-propertyChain");
    let derived = entailed.iter().find(|fact| {
        fact.subject == supplier
            && fact.predicate == format!("<{KG}suppliesService>")
            && fact.object == service
    });
    let Some(derived) = derived else {
        panic!("the inverse must derive {supplier} suppliesService {service}");
    };
    assert!(!derived.asserted, "the edge is inferred, not asserted");
    assert_eq!(derived.rule, "RL-inverseOf");
}

// spec: EG-SUPPLY-VOCAB-R001.3
#[test]
fn supplies_service_infers_supplier_to_service_from_the_physical_path() {
    let mut triples = documents();
    triples.extend(
        parse_turtle(include_str!(
            "../../eg-core/tests/fixtures/supply_chain_service_chain.ttl"
        ))
        .unwrap(),
    );
    let supplier = format!("<{EX}acmeSupplier>");
    let service = format!("<{EX}payrollService>");
    let entailed = saturate_object_properties(&triples);
    for predicate in ["suppliesServiceViaHost", "suppliesService"] {
        let found = entailed.iter().find(|fact| {
            fact.subject == supplier
                && fact.predicate == format!("<{KG}{predicate}>")
                && fact.object == service
        });
        let Some(found) = found else {
            panic!("the physical path must derive {supplier} {predicate} {service}");
        };
        assert!(!found.asserted, "the edge is inferred, not asserted");
    }
}

// spec: EG-SUPPLY-VOCAB-R001.3
#[test]
fn container_or_pod_runs_on_path_infers_no_supplier_service_edge() {
    let mut triples = documents();
    triples.extend(
        parse_turtle(
            r#"
            @prefix : <http://knuckles.team/kg#> .
            @prefix infra: <http://knuckles.team/kg/infrastructure#> .
            @prefix ex: <http://knuckles.team/test/supply-chain#> .

            ex:acmeSupplier a :Supplier ;
                :suppliesItem ex:psuItem .
            ex:psuItem a :SupplyChainItem ;
                :realizedByAsset ex:psuAsset .
            ex:psuAsset a :AssetInstance ;
                :assetHost ex:rackHost .
            ex:rackHost a infra:Host .
            ex:someContainer infra:containerRunsOn ex:rackHost .
            ex:somePod infra:podRunsOn ex:rackHost .
            "#,
        )
        .unwrap(),
    );
    let entailed = saturate_object_properties(&triples);
    for predicate in ["suppliesService", "suppliesServiceViaHost", "serviceSupplier"] {
        assert!(
            !entailed
                .iter()
                .any(|fact| fact.predicate == format!("<{KG}{predicate}>")),
            "a container-only or pod-only path must not derive {predicate}"
        );
    }
    let service_type = "<http://knuckles.team/kg/infrastructure#Service>";
    assert!(
        !entailed.iter().any(|fact| fact.object == service_type),
        "no endpoint is retyped as a Service"
    );
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
