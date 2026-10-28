//! Blocking-key fixture contract for the supply-chain concepts
//! (EG-SUPPLY-VOCAB-R007.1).

use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::NamedNode;

const KG: &str = "http://knuckles.team/kg#";
const INFRA: &str = "http://knuckles.team/kg/infrastructure#";
const EX: &str = "http://knuckles.team/test/supply-chain#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

// spec: EG-SUPPLY-VOCAB-R007.1
#[test]
fn blocking_key_fixture_parses_and_lists_two_keys_per_concept() {
    let fixture = parse_turtle(include_str!("fixtures/supply_chain_blocking_keys.ttl"))
        .expect("the blocking-key fixture parses");

    let concepts = [
        (format!("{KG}Supplier"), "supplierKeys"),
        (format!("{INFRA}Service"), "serviceKeys"),
        (format!("{INFRA}Host"), "hostKeys"),
        (format!("{INFRA}ContainerImage"), "imageKeys"),
        (format!("{KG}Customer"), "customerKeys"),
    ];

    for (concept, set) in concepts {
        let set_iri = format!("{EX}{set}");
        // Each concept is blocked by exactly one key set, applied to it.
        assert!(
            fixture.iter().any(|triple| {
                triple.subject == NamedNode::new(set_iri.clone()).unwrap().into()
                    && triple.predicate.as_str() == RDF_TYPE
                    && matches!(
                        &triple.object,
                        eg_rdf::oxrdf::Term::NamedNode(n)
                            if n.as_str() == format!("{EX}BlockingKeySet")
                    )
            }),
            "{set} is a blocking key set"
        );
        assert!(
            fixture.iter().any(|triple| {
                triple.subject == NamedNode::new(set_iri.clone()).unwrap().into()
                    && triple.predicate.as_str() == format!("{EX}appliesTo")
                    && matches!(
                        &triple.object,
                        eg_rdf::oxrdf::Term::NamedNode(n) if n.as_str() == concept
                    )
            }),
            "{set} applies to {concept}"
        );
        // Each key set lists at least two keys, strongest first.
        let has_key = format!("{EX}hasKey");
        let keys: Vec<&eg_rdf::oxrdf::Term> = fixture
            .iter()
            .filter(|triple| {
                triple.subject == NamedNode::new(set_iri.clone()).unwrap().into()
                    && triple.predicate.as_str() == has_key
            })
            .map(|triple| &triple.object)
            .collect();
        assert!(
            keys.len() >= 2,
            "{set} lists at least two keys, found {}",
            keys.len()
        );
        // Every listed key is a labelled blocking key.
        for key in &keys {
            let eg_rdf::oxrdf::Term::NamedNode(key_node) = key else {
                panic!("{set} keys are named");
            };
            assert!(
                fixture.iter().any(|triple| {
                    triple.subject == key_node.clone().into()
                        && triple.predicate.as_str() == RDF_TYPE
                        && matches!(
                            &triple.object,
                            eg_rdf::oxrdf::Term::NamedNode(n)
                                if n.as_str() == format!("{EX}BlockingKey")
                        )
                }),
                "key {key_node} is typed BlockingKey"
            );
            assert!(
                fixture.iter().any(|triple| {
                    triple.subject == key_node.clone().into()
                        && triple.predicate.as_str() == "http://www.w3.org/2000/01/rdf-schema#label"
                        && matches!(&triple.object, eg_rdf::oxrdf::Term::Literal(_))
                }),
                "key {key_node} carries a label"
            );
        }
    }
}
