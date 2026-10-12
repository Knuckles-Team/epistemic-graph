//! Validate task facts against the shapes registered by the core schema source.

use eg_shacl::{validate_turtle, ValidationReport};

fn task_report(facts: &str) -> ValidationReport {
    let data = format!(
        "@prefix swarm: <http://knuckles.team/kg/swarm#> .\n\
         @prefix ex: <http://example.org/> .\n{facts}"
    );
    validate_turtle(
        include_str!("../../eg-core/ontology/swarm_topology-v1.shapes.ttl"),
        &data,
    )
    .expect("registered swarm shapes and task facts must validate")
}

// spec: EG-TYPED-PACKS-R066.2
#[test]
fn shell_centric_accepts_absent_or_one_boolean() {
    for fact in [
        "",
        "; swarm:shellCentric true",
        "; swarm:shellCentric false",
    ] {
        let report = task_report(&format!("ex:task a swarm:TaskShape {fact} ."));
        assert!(report.conforms, "{fact}: {:?}", report.results);
    }
}

// spec: EG-TYPED-PACKS-R066.2
#[test]
fn shell_centric_must_be_boolean() {
    for value in [r#""true""#, "1", "ex:unknown"] {
        let report = task_report(&format!(
            "ex:task a swarm:TaskShape ; swarm:shellCentric {value} ."
        ));
        assert!(!report.conforms, "{value} must be refused");
        assert!(report.results.iter().any(|result| {
            result
                .constraint_component
                .ends_with("DatatypeConstraintComponent")
                && result.path.as_deref() == Some("<http://knuckles.team/kg/swarm#shellCentric>")
        }));
    }
}

// spec: EG-TYPED-PACKS-R066.2
#[test]
fn shell_centric_is_single_valued_on_task_classes() {
    let class = "ex:ShellTask a <http://www.w3.org/2002/07/owl#Class> ; \
        <http://www.w3.org/2000/01/rdf-schema#subClassOf> swarm:TaskShape";
    let accepted = task_report(&format!("{class} ; swarm:shellCentric true ."));
    assert!(accepted.conforms, "{:?}", accepted.results);
    let refused = task_report(&format!("{class} ; swarm:shellCentric true, false ."));
    assert!(!refused.conforms);
    assert!(refused.results.iter().any(|result| {
        result
            .constraint_component
            .ends_with("MaxCountConstraintComponent")
    }));
    let wrong_type = task_report(&format!("{class} ; swarm:shellCentric \"true\" ."));
    assert!(!wrong_type.conforms);
}
