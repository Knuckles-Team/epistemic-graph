//! SHACL coverage for measured model facts (EG-TYPED-PACKS-R066.1).

use eg_shacl::validate_turtle;

const SHAPES: &str = include_str!("../ontology/harness-v1.shapes.ttl");

fn report(facts: &str) -> eg_shacl::ValidationReport {
    let data = format!(
        r#"@prefix eg: <http://knuckles.team/kg#> .
@prefix ex: <http://example.org/model-harness/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

{facts}
"#
    );
    validate_turtle(SHAPES, &data).expect("model-fact Turtle should parse and validate")
}

#[test]
fn model_fact_shapes_accept_measured_facts() {
    let result = report(
        r#"ex:model a eg:ModelProfile ;
    eg:contextWindowTokens "128000"^^xsd:integer ;
    eg:shellProficiency [
        eg:value "high" ;
        eg:evaluationRun ex:shell-evaluation
    ] ;
    eg:stoppingBehaviour [
        eg:value "balanced" ;
        eg:evaluationRun ex:stopping-evaluation
    ] ."#,
    );
    assert!(
        result.conforms,
        "valid measured facts: {:?}",
        result.results
    );
}

#[test]
fn model_fact_shapes_reject_missing_required_facts() {
    for (missing_fact, facts) in [
        (
            "shellProficiency",
            r#"ex:model a eg:ModelProfile ;
    eg:contextWindowTokens "128000"^^xsd:integer ;
    eg:stoppingBehaviour [
        eg:value "balanced" ;
        eg:evaluationRun ex:stopping-evaluation
    ] ."#,
        ),
        (
            "stoppingBehaviour",
            r#"ex:model a eg:ModelProfile ;
    eg:contextWindowTokens "128000"^^xsd:integer ;
    eg:shellProficiency [
        eg:value "high" ;
        eg:evaluationRun ex:shell-evaluation
    ] ."#,
        ),
    ] {
        let result = report(facts);
        assert!(
            !result.conforms,
            "a ModelProfile missing required {missing_fact} must fail"
        );
        let expected_path = format!("<http://knuckles.team/kg#{missing_fact}>");
        assert!(
            result
                .results
                .iter()
                .any(|violation| violation.path.as_deref() == Some(expected_path.as_str())),
            "the missing {missing_fact} property must be the reported violation: {:?}",
            result.results
        );
    }
}

#[test]
fn model_fact_shapes_reject_missing_run() {
    let result = report(
        r#"ex:model a eg:ModelProfile ;
    eg:contextWindowTokens "128000"^^xsd:integer ;
    eg:shellProficiency [ eg:value "high" ] ."#,
    );
    assert!(
        !result.conforms,
        "a measured value without evaluationRun must fail"
    );
    assert!(result.results.iter().any(|violation| {
        violation
            .message
            .as_deref()
            .is_some_and(|message| message.contains("evaluationRun"))
            || violation.path.as_deref() == Some("<http://knuckles.team/kg#evaluationRun>")
    }));
}

#[test]
fn model_fact_shapes_reject_unknown_enum_values() {
    let result = report(
        r#"ex:model a eg:ModelProfile ;
    eg:contextWindowTokens "128000"^^xsd:integer ;
    eg:shellProficiency [
        eg:value "expert" ;
        eg:evaluationRun ex:shell-evaluation
    ] ;
    eg:stoppingBehaviour [
        eg:value "balanced" ;
        eg:evaluationRun ex:stopping-evaluation
    ] ."#,
    );
    assert!(
        !result.conforms,
        "unknown shell proficiency must fail closed"
    );
}

#[test]
fn model_fact_shapes_reject_unknown_stopping_behaviour() {
    let result = report(
        r#"ex:model a eg:ModelProfile ;
    eg:contextWindowTokens "128000"^^xsd:integer ;
    eg:stoppingBehaviour [
        eg:value "sometimes" ;
        eg:evaluationRun ex:stopping-evaluation
    ] ."#,
    );
    assert!(
        !result.conforms,
        "unknown stopping behaviour must fail closed"
    );
}

#[test]
fn model_fact_shapes_reject_non_positive_context_window() {
    let result = report(
        r#"ex:model a eg:ModelProfile ;
    eg:contextWindowTokens "0"^^xsd:integer ."#,
    );
    assert!(!result.conforms, "contextWindowTokens=0 must fail closed");
}
