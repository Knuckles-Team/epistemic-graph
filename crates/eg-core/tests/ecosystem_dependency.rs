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
    let supply = parse_turtle(include_str!("../ontology/supply_chain-v1.ttl"))
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
    let software = parse_turtle(include_str!("../ontology/software-v1.ttl"))
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
    for entry in std::fs::read_dir("ontology").expect("the ontology directory is present") {
        let path = entry.unwrap().path();
        // Only the module documents: ontology/ also holds a fixtures/ directory.
        if path.extension().and_then(|ext| ext.to_str()) != Some("ttl") {
            continue;
        }
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

mod propagation {
    use eg_core::ecosystem::{
        inherit_severity, propagate_package_risk, propagate_package_risk_with, PackageFinding,
        PackageImpact, PackageRefusal, Severity,
    };
    use eg_core::graph::GraphCore;
    use serde_json::json;

    /// app -> mid -> lib -> core, plus side -> core and an unrelated leaf.
    fn chain() -> GraphCore {
        let graph = GraphCore::new();
        for id in ["core", "lib", "mid", "app", "side", "leaf"] {
            graph.add_node(id.into(), rmp_serde::to_vec_named(&json!({})).unwrap());
        }
        for (dependent, dependency) in [
            ("lib", "core"),
            ("mid", "lib"),
            ("app", "mid"),
            ("side", "core"),
        ] {
            graph
                .add_edge(
                    dependent.into(),
                    dependency.into(),
                    rmp_serde::to_vec_named(&json!({"relationship": "DEPENDS_ON"})).unwrap(),
                )
                .unwrap();
        }
        // A non-dependency edge must never carry risk.
        graph
            .add_edge(
                "leaf".into(),
                "core".into(),
                rmp_serde::to_vec_named(&json!({"relationship": "MENTIONS"})).unwrap(),
            )
            .unwrap();
        graph
    }

    fn finding() -> PackageFinding {
        PackageFinding {
            id: "vuln-1".into(),
            severity: Severity::High,
        }
    }

    // spec: EG-SUPPLY-VOCAB-R009.2
    #[test]
    fn finding_reaches_transitive_dependents() {
        let impacts = propagate_package_risk(&chain(), "core", &finding()).unwrap();
        let reached: Vec<&str> = impacts.iter().map(|i| i.package.as_str()).collect();
        assert_eq!(reached, ["lib", "side", "mid", "app"]);
        assert!(impacts.iter().all(|i| i.severity == Severity::High));
        assert!(
            !reached.contains(&"leaf"),
            "a non-dependency edge carries no risk"
        );
        // The depth bound is honoured: one hop reaches only direct dependents.
        let shallow =
            propagate_package_risk_with(&chain(), "core", &finding(), 1, inherit_severity).unwrap();
        assert_eq!(
            shallow
                .iter()
                .map(|i| i.package.as_str())
                .collect::<Vec<_>>(),
            ["lib", "side"]
        );
        // The worst severity per dependent is kept when the rule varies by depth.
        fn attenuate(f: &PackageFinding, depth: usize) -> Severity {
            if depth > 1 {
                Severity::Low
            } else {
                f.severity
            }
        }
        let attenuated =
            propagate_package_risk_with(&chain(), "core", &finding(), 8, attenuate).unwrap();
        let severity_of = |name: &str| {
            attenuated
                .iter()
                .find(|i| i.package == name)
                .map(|i| i.severity)
        };
        assert_eq!(severity_of("lib"), Some(Severity::High));
        assert_eq!(severity_of("app"), Some(Severity::Low));
    }

    // spec: EG-SUPPLY-VOCAB-R009.4
    #[test]
    fn unknown_package_is_refused() {
        let outcome = propagate_package_risk(&chain(), "absent", &finding());
        assert_eq!(
            outcome,
            Err(PackageRefusal::UnknownPackage("absent".into()))
        );
        // A known package nothing depends on yields an empty list, not a refusal.
        let none = propagate_package_risk(&chain(), "app", &finding()).unwrap();
        assert!(none.is_empty());
    }

    // spec: EG-SUPPLY-VOCAB-R009.5
    #[test]
    fn impact_carries_dependency_path() {
        let impacts = propagate_package_risk(&chain(), "core", &finding()).unwrap();
        let app: &PackageImpact = impacts.iter().find(|i| i.package == "app").unwrap();
        assert_eq!(app.path, ["core", "lib", "mid", "app"]);
        let side = impacts.iter().find(|i| i.package == "side").unwrap();
        assert_eq!(side.path, ["core", "side"]);
    }
}
