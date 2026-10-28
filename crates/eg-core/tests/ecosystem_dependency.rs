//! Ecosystem package dependency and risk model (EG-SUPPLY-VOCAB-R009):
//! the ontology terms first; the propagation behavior rows follow in the
//! same file.

use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, Triple};

const KG: &str = "http://knuckles.team/kg#";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

// spec: EG-SUPPLY-VOCAB-R009.1
#[test]
fn package_class_and_dependency_declared() {
    let supply =
        parse_turtle(include_str!("../ontology/supply_chain-v1.ttl"))
            .expect("the supply-chain vocabulary parses");
    // The model declares the one class and the one new property.
    assert!(
        has_iri(
            &supply,
            &format!("{KG}EcosystemPackage"),
            &format!("{RDF}type"),
            &format!("{OWL}Class")
        ),
        "EcosystemPackage is a class"
    );
    assert!(
        has_iri(
            &supply,
            &format!("{KG}affectsPackage"),
            &format!("{RDF}type"),
            &format!("{OWL}ObjectProperty")
        ),
        "affectsPackage is an object property"
    );
    assert!(
        has_iri(
            &supply,
            &format!("{KG}affectsPackage"),
            &format!("{RDFS}domain"),
            &format!("{KG}Vulnerability")
        ),
        "affectsPackage domains the finding"
    );
    assert!(
        has_iri(
            &supply,
            &format!("{KG}affectsPackage"),
            &format!("{RDFS}range"),
            &format!("{KG}EcosystemPackage")
        ),
        "affectsPackage ranges to EcosystemPackage"
    );
    // Package dependency reuses the shared transitive :dependsOn, declared
    // exactly once in the core ontology set (software-v1.ttl) and never
    // redeclared by the supply-chain module.
    let software =
        parse_turtle(include_str!("../ontology/software-v1.ttl"))
            .expect("the software vocabulary parses");
    assert!(
        has_iri(
            &software,
            &format!("{KG}dependsOn"),
            &format!("{RDF}type"),
            &format!("{OWL}TransitiveProperty")
        ),
        "dependsOn is the transitive dependency property"
    );
    let mut transitive_declarations = 0;
    for entry in
        std::fs::read_dir("ontology").expect("the ontology directory is present")
    {
        let path = entry.unwrap().path();
        let document = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("{} reads: {err}", path.display()));
        let parsed = parse_turtle(&document)
            .unwrap_or_else(|err| panic!("{} parses: {err}", path.display()));
        transitive_declarations += parsed
            .iter()
            .filter(|triple| {
                let subject_iri = match &triple.subject {
                    eg_rdf::oxrdf::NamedOrBlankNode::NamedNode(n) => n.as_str(),
                    _ => return false,
                };
                subject_iri.ends_with("#dependsOn")
                    && triple.predicate.as_str().ends_with("type")
                    && matches!(
                        &triple.object,
                        eg_rdf::oxrdf::Term::NamedNode(n)
                            if n.as_str() == format!("{OWL}TransitiveProperty")
                    )
            })
            .count();
    }
    assert_eq!(
        transitive_declarations, 1,
        "exactly one transitive :dependsOn declaration in crates/eg-core/ontology"
    );
}
