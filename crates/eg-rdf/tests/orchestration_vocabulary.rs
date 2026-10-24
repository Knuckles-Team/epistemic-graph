//! Native parser and OWL classification contracts for foundational orchestration classes.
#![cfg(feature = "owl")]

use eg_rdf::mapping::parse_turtle;
use eg_rdf::owl::Reasoner;
use eg_rdf::oxrdf::{NamedNode, Triple};

const KG: &str = "http://knuckles.team/kg#";
const ENTITY: &str = "http://purl.obolibrary.org/obo/BFO_0000001";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";

fn documents() -> Vec<Triple> {
    [
        include_str!("../../eg-core/ontology/core-foundation-v1.ttl"),
        include_str!("../../eg-core/ontology/orchestration-v1.ttl"),
        include_str!("../../eg-core/ontology/capability-v1.ttl"),
    ]
    .into_iter()
    .flat_map(|document| parse_turtle(document).unwrap())
    .collect()
}

fn iri_triple(subject: &str, predicate: &str, object: &str) -> Triple {
    Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    )
}

// spec: EG-ORCH-VOCAB-R001.1
#[test]
fn task_step_resolves_and_precedes_accepts_it() {
    let mut triples = documents();
    assert!(triples.contains(&iri_triple(
        &format!("{KG}TaskStep"),
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://www.w3.org/2002/07/owl#Class",
    )));
    for predicate in ["domain", "range"] {
        assert!(triples.contains(&iri_triple(
            &format!("{KG}precedes"),
            &format!("{RDFS}{predicate}"),
            ENTITY,
        )));
    }
    triples.extend(
        parse_turtle(
            "@prefix : <http://knuckles.team/kg#> .
         :first a :TaskStep ; :precedes :second . :second a :TaskStep .",
        )
        .unwrap(),
    );
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    assert!(classification.unsatisfiable.is_empty());
    assert!(classification.entails_subclass(&format!("<{KG}TaskStep>"), &format!("<{ENTITY}>")));
    assert!(!classification.entails_subclass(
        &format!("<{KG}TaskStep>"),
        &format!("<{KG}DevelopmentStandard>")
    ));
}

// spec: EG-ORCH-VOCAB-R004.1
#[test]
fn best_practice_is_a_development_standard() {
    let triples = documents();
    assert!(triples.contains(&iri_triple(
        &format!("{KG}BestPractice"),
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://www.w3.org/2002/07/owl#Class",
    )));
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    assert!(classification.entails_subclass(
        &format!("<{KG}BestPractice>"),
        &format!("<{KG}DevelopmentStandard>")
    ));
    assert!(!classification.entails_subclass(
        &format!("<{KG}DevelopmentStandard>"),
        &format!("<{KG}BestPractice>")
    ));
    assert!(!classification
        .entails_subclass(&format!("<{KG}BestPractice>"), &format!("<{KG}TaskStep>")));
}

// Pending native acceptance: EG-ORCH-VOCAB-R001.1 and EG-ORCH-VOCAB-R004.1.
#[test]
fn contradictory_parent_mutations_are_detected() {
    for (class, parent) in [
        (format!("{KG}TaskStep"), ENTITY.to_string()),
        (
            format!("{KG}BestPractice"),
            format!("{KG}DevelopmentStandard"),
        ),
    ] {
        let mut triples = documents();
        triples.push(iri_triple(
            &class,
            "http://www.w3.org/2002/07/owl#disjointWith",
            &parent,
        ));
        let classification = Reasoner::from_triples(&triples).classify();
        assert!(!classification.consistent);
        assert!(classification.unsatisfiable.contains(&format!("<{class}>")));
    }
}

#[cfg(feature = "sparql")]
mod run_trace {
    use super::*;
    use eg_core::graph::{GraphCore, GraphSchemaSources};
    use eg_rdf::mapping::{load_triples, IriStore};
    use eg_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};

    const SHAPES: &str = include_str!("../../eg-core/ontology/orchestration-v1.shapes.ttl");
    const COUNTERS: &[&str] = &[
        "peakContextTokens",
        "contextBudgetTokens",
        "elisionCount",
        "summarizationCount",
        "turnCount",
        "toolCallCount",
    ];

    // spec: EG-ORCH-VOCAB-R009.1
    // spec: EG-ORCH-VOCAB-R009.2
    // spec: EG-ORCH-VOCAB-R009.3
    // spec: EG-ORCH-VOCAB-R009.4
    // spec: EG-ORCH-VOCAB-R009.5
    // spec: EG-ORCH-VOCAB-R009.6
    // spec: EG-ORCH-VOCAB-R009.7
    // spec: EG-ORCH-VOCAB-R009.8
    #[test]
    fn run_trace_properties_have_declared_domain_and_datatype() {
        let core = GraphCore::new();
        load_triples(&core, &mut IriStore::default(), "schema", documents()).unwrap();
        let view = core.analysis_snapshot();
        for (property, datatype) in COUNTERS
            .iter()
            .map(|p| (*p, "integer"))
            .chain([("terminationReason", "string"), ("costUsd", "decimal")])
        {
            let query = format!(
                "PREFIX : <{KG}> PREFIX owl: <http://www.w3.org/2002/07/owl#> \
                 PREFIX rdfs: <{RDFS}> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
                 ASK {{ :{property} a owl:DatatypeProperty ; rdfs:domain :RunTrace ; rdfs:range xsd:{datatype} }}"
            );
            let result = execute(
                &Dataset::new(&view, Vec::new()),
                &query,
                &Projection::raw(),
                None,
            )
            .unwrap();
            assert!(
                matches!(result, QueryOutcome::Boolean(true)),
                "{property}: {result:?}"
            );
        }
    }

    fn conforms(properties: &str) -> bool {
        let data = format!("@prefix : <{KG}> . :trace a :RunTrace {properties} .");
        eg_shacl::validate_turtle(SHAPES, &data).unwrap().conforms
    }

    // spec: EG-ORCH-VOCAB-R008.1
    #[test]
    fn orchestration_shapes_are_loaded() {
        let sources = GraphSchemaSources::default();
        sources.validate().unwrap();
        let source = &sources.core["core:orchestration-shapes@1"];
        assert_eq!(source.shapes_ttl.as_deref(), Some(SHAPES));
        assert!(source.shapes_sha256.is_some());
    }

    // spec: EG-ORCH-VOCAB-R009.1
    #[test]
    fn termination_reason_uses_the_closed_vocabulary() {
        for reason in [
            "final_answer",
            "context_overflow",
            "stuck",
            "step_budget",
            "error",
        ] {
            assert!(conforms(&format!("; :terminationReason \"{reason}\"")));
        }
        assert!(!conforms("; :terminationReason \"success\""));
        assert!(!conforms("; :terminationReason 1"));
    }

    // spec: EG-ORCH-VOCAB-R009.12
    #[test]
    fn counter_shape_rejects_negative_and_fractional_counts() {
        assert!(conforms(""), "unknown measurements need not be invented");
        for counter in COUNTERS {
            for valid in ["0", "42"] {
                assert!(
                    conforms(&format!("; :{counter} {valid}")),
                    "{counter} {valid}"
                );
            }
            for invalid in [
                "-1",
                "0.5",
                "\"3\"",
                "\"0.5\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                "\"NaN\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            ] {
                assert!(
                    !conforms(&format!("; :{counter} {invalid}")),
                    "{counter} {invalid}"
                );
            }
        }
        for valid in ["0.0", "0.125"] {
            assert!(conforms(&format!("; :costUsd {valid}")));
        }
        assert!(!conforms("; :costUsd -0.1"));
        assert!(!conforms(&format!("; :costUsd -0.{}1", "0".repeat(400))));
        assert!(!conforms("; :costUsd \"unknown\""));
    }

    // spec: EG-ORCH-VOCAB-R009.13
    #[test]
    fn budget_shape_allows_overshoot_only_for_overflow() {
        for reason in [
            "final_answer",
            "context_overflow",
            "stuck",
            "step_budget",
            "error",
        ] {
            let reason_property = format!("; :terminationReason \"{reason}\"");
            assert!(conforms(&format!(
                "{reason_property}; :peakContextTokens 10 ; :contextBudgetTokens 10"
            )));
            assert_eq!(
                conforms(&format!(
                    "{reason_property}; :peakContextTokens 11 ; :contextBudgetTokens 10"
                )),
                reason == "context_overflow"
            );
        }
        assert!(!conforms(
            "; :peakContextTokens 11 ; :contextBudgetTokens 10"
        ));
        assert!(conforms("; :peakContextTokens 0 ; :contextBudgetTokens 0"));
        assert!(!conforms(
            "; :peakContextTokens 9007199254740993 ; :contextBudgetTokens 9007199254740992"
        ));
        assert!(conforms(
            "; :peakContextTokens 9007199254740992 ; :contextBudgetTokens 9007199254740993"
        ));
    }
}
