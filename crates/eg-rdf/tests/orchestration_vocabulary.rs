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

#[cfg(feature = "sparql")]
mod vocabulary {
    use super::*;
    use eg_core::graph::{GraphCore, GraphView};
    use eg_rdf::mapping::{load_triples, IriStore};
    use eg_rdf::owl::saturate_object_properties;
    use eg_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};
    use eg_shacl::validate_turtle;
    use eg_types::decision::guardrail::{entailed_guardrails, GuardrailRule};

    const A2A: &str = include_str!("../../eg-core/ontology/a2a-v1.ttl");
    const SDD: &str = include_str!("../../eg-core/ontology/sdd-v1.ttl");
    const SWARM: &str = include_str!("../../eg-core/ontology/swarm_topology-v1.ttl");
    const SWARM_SHAPES: &str = include_str!("../../eg-core/ontology/swarm_topology-v1.shapes.ttl");
    const ORCHESTRATION: &str = include_str!("../../eg-core/ontology/orchestration-v1.ttl");
    const SHAPES: &str = include_str!("../../eg-core/ontology/orchestration-v1.shapes.ttl");
    const FIXTURE: &str =
        include_str!("../../eg-core/tests/fixtures/orchestration_vocabulary/deploy_container.ttl");
    const SWARM_NS: &str = "http://knuckles.team/kg/swarm#";
    const PREFIXES: &str = "PREFIX : <http://knuckles.team/kg#> \
        PREFIX swarm: <http://knuckles.team/kg/swarm#> \
        PREFIX owl: <http://www.w3.org/2002/07/owl#> \
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
    const TURTLE_PREFIXES: &str = "@prefix : <http://knuckles.team/kg#> . \
        @prefix swarm: <http://knuckles.team/kg/swarm#> . \
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . \n";

    fn schema() -> Vec<Triple> {
        let mut triples = documents();
        for document in [A2A, SDD, SWARM] {
            triples.extend(parse_turtle(document).unwrap());
        }
        triples
    }

    fn view_of(extra: &str) -> GraphView {
        let core = GraphCore::new();
        let mut iris = IriStore::default();
        load_triples(&core, &mut iris, "schema", schema()).unwrap();
        let data = format!("{TURTLE_PREFIXES}{extra}");
        load_triples(&core, &mut iris, "fixture", parse_turtle(&data).unwrap()).unwrap();
        core.analysis_snapshot()
    }

    fn outcome(view: &GraphView, query: &str) -> QueryOutcome {
        execute(
            &Dataset::new(view, Vec::new()),
            &format!("{PREFIXES}{query}"),
            &Projection::raw(),
            None,
        )
        .unwrap()
    }

    fn ask(view: &GraphView, pattern: &str) -> bool {
        match outcome(view, &format!("ASK {{ {pattern} }}")) {
            QueryOutcome::Boolean(value) => value,
            other => panic!("expected a boolean, got {other:?}"),
        }
    }

    fn rows(view: &GraphView, query: &str) -> Vec<Vec<Option<String>>> {
        match outcome(view, query) {
            QueryOutcome::Solutions(table) => table.to_rows().1,
            other => panic!("expected solutions, got {other:?}"),
        }
    }

    /// A cell's local name (`<…#s1>` → `s1`); an unbound cell stays `None`.
    fn local(cell: &Option<String>) -> Option<String> {
        cell.as_ref().map(|value| {
            value
                .trim_matches(|c| c == '<' || c == '>' || c == '"')
                .rsplit(['#', '/'])
                .next()
                .unwrap()
                .to_string()
        })
    }

    fn table(view: &GraphView, query: &str) -> Vec<Vec<Option<String>>> {
        let mut result: Vec<Vec<Option<String>>> = rows(view, query)
            .iter()
            .map(|row| row.iter().map(local).collect())
            .collect();
        result.sort();
        result
    }

    fn cells(values: &[&[Option<&str>]]) -> Vec<Vec<Option<String>>> {
        let mut result: Vec<Vec<Option<String>>> = values
            .iter()
            .map(|row| row.iter().map(|v| v.map(str::to_string)).collect())
            .collect();
        result.sort();
        result
    }

    fn declared(view: &GraphView, term: &str, kind: &str, domain: Option<&str>, range: &str) {
        assert!(
            ask(view, &format!("{term} a {kind} ; rdfs:range {range}")),
            "{term} {kind} range {range}"
        );
        match domain {
            Some(domain) => assert!(
                ask(view, &format!("{term} rdfs:domain {domain}")),
                "{term} domain {domain}"
            ),
            None => assert!(
                !ask(view, &format!("{term} rdfs:domain ?domain")),
                "{term} must declare no domain"
            ),
        }
    }

    fn class_declared(view: &GraphView, class: &str) {
        assert!(ask(view, &format!("{class} a owl:Class")), "{class}");
    }

    /// Result messages of one named shape for the data, validated against
    /// `shapes`. With `property_shapes`, the shape's anonymous property shapes
    /// count too; use it only for data that triggers no other shape.
    fn shape_results(shapes: &str, shape: &str, property_shapes: bool, data: &str) -> Vec<String> {
        validate_turtle(shapes, &format!("{TURTLE_PREFIXES}{data}"))
            .unwrap()
            .results
            .into_iter()
            .filter(|result| {
                result.source_shape.contains(shape)
                    || (property_shapes && result.source_shape.starts_with("_:"))
            })
            .map(|result| result.message.unwrap_or_default())
            .collect()
    }

    fn violations(shapes: &str, shape: &str, data: &str) -> Vec<String> {
        shape_results(shapes, shape, shapes == SHAPES, data)
    }

    fn consistent_with(extra: &str) -> eg_rdf::owl::Classification {
        let mut triples = schema();
        triples.extend(parse_turtle(&format!("{TURTLE_PREFIXES}{extra}")).unwrap());
        let classification = Reasoner::from_triples(&triples).classify();
        assert!(classification.consistent);
        assert!(classification.unsatisfiable.is_empty());
        classification
    }

    fn kg(name: &str) -> String {
        format!("<{KG}{name}>")
    }

    // spec: EG-ORCH-VOCAB-R001.2
    #[test]
    fn has_step_resolves() {
        let view = view_of("");
        declared(&view, ":hasStep", "owl:ObjectProperty", None, ":TaskStep");
        consistent_with(FIXTURE);
    }

    // spec: EG-ORCH-VOCAB-R001.3
    #[test]
    fn step_shape_rejects_a_precedes_cycle() {
        assert!(violations(SHAPES, "TaskStepShape", FIXTURE).is_empty());
        let cycle = ":t :hasStep :a, :b . :a a :TaskStep ; :precedes :b . \
                     :b a :TaskStep ; :precedes :a .";
        let found = violations(SHAPES, "TaskStepShape", cycle);
        assert!(found.iter().any(|m| m.contains("precedes itself")), "{found:?}");
        let ownerless = ":a a :TaskStep .";
        assert!(violations(SHAPES, "TaskStepShape", ownerless)
            .iter()
            .any(|m| m.contains("no owning task")));
        let shared = ":t :hasStep :a . :u :hasStep :a . :a a :TaskStep .";
        assert!(violations(SHAPES, "TaskStepShape", shared)
            .iter()
            .any(|m| m.contains("more than one owning task")));
    }

    // spec: EG-ORCH-VOCAB-R001.4
    #[test]
    fn q1_returns_four_ordered_steps() {
        let view = view_of(FIXTURE);
        let answer = table(
            &view,
            "SELECT ?s ?next WHERE { :DeployContainerTask :hasStep ?s . \
             OPTIONAL { ?s :precedes ?next } }",
        );
        assert_eq!(
            answer,
            cells(&[
                &[Some("s1"), Some("s2")],
                &[Some("s2"), Some("s3")],
                &[Some("s3"), Some("s4")],
                &[Some("s4"), None],
            ])
        );
    }

    // spec: EG-ORCH-VOCAB-R002.1
    #[test]
    fn agent_kind_members_resolve() {
        let view = view_of("");
        class_declared(&view, ":AgentKind");
        let kinds = table(&view, "SELECT ?kind WHERE { ?kind a :AgentKind }");
        let expected: Vec<Vec<Option<String>>> = [
            "agentKindAggregator",
            "agentKindExecutor",
            "agentKindPlanner",
            "agentKindResearcher",
            "agentKindSupervisor",
            "agentKindVerifier",
            "agentKindWorker",
        ]
        .iter()
        .map(|name| vec![Some(name.to_string())])
        .collect();
        assert_eq!(kinds, expected);
    }

    // spec: EG-ORCH-VOCAB-R002.2
    #[test]
    fn has_agent_kind_resolves() {
        let view = view_of("");
        declared(&view, ":hasAgentKind", "owl:ObjectProperty", Some(":Agent"), ":AgentKind");
    }

    // spec: EG-ORCH-VOCAB-R002.3
    #[test]
    fn slot_requires_agent_kind_resolves() {
        let view = view_of("");
        declared(
            &view,
            "swarm:requiresAgentKind",
            "owl:ObjectProperty",
            Some("swarm:Slot"),
            ":AgentKind",
        );
        let imports = |document: &str| -> Vec<String> {
            parse_turtle(document)
                .unwrap()
                .into_iter()
                .filter(|t| t.predicate.as_str() == "http://www.w3.org/2002/07/owl#imports")
                .map(|t| t.object.to_string())
                .collect()
        };
        let swarm = imports(SWARM);
        assert!(swarm.contains(&"<http://knuckles.team/kg/orchestration>".to_string()));
        // The import closure has no cycle: orchestration imports nothing back.
        assert!(imports(ORCHESTRATION).is_empty());
        eg_core::graph::GraphSchemaSources::default().validate().unwrap();
        consistent_with(FIXTURE);
    }

    // spec: EG-ORCH-VOCAB-R002.4
    #[test]
    fn slot_shape_rejects_a_non_kind() {
        let kind = format!("{SWARM_NS}requiresAgentKind");
        let slot = |kinds: &str| {
            format!(
                ":kindA a :AgentKind . :kindB a :AgentKind . :notKind a :Agent . \
                 :slot a swarm:Slot ; {kinds} ."
            )
        };
        let class_failures = |data: &str| {
            validate_turtle(SWARM_SHAPES, &format!("{TURTLE_PREFIXES}{data}"))
                .unwrap()
                .results
                .into_iter()
                .filter(|r| r.path.as_deref() == Some(&format!("<{kind}>")))
                .map(|r| r.constraint_component)
                .collect::<Vec<_>>()
        };
        assert!(class_failures(&slot("swarm:requiresAgentKind :kindA")).is_empty());
        assert!(class_failures(&slot("swarm:requiresAgentKind :notKind"))
            .iter()
            .any(|c| c.ends_with("ClassConstraintComponent")));
        assert!(class_failures(&slot("swarm:requiresAgentKind :kindA, :kindB"))
            .iter()
            .any(|c| c.ends_with("MaxCountConstraintComponent")));
    }

    // spec: EG-ORCH-VOCAB-R002.5
    #[test]
    fn q2_returns_slots_roles_kinds_and_width() {
        let view = view_of(FIXTURE);
        let answer = table(
            &view,
            "SELECT ?slot ?role ?kind ?min ?max WHERE { :tmplPipeline swarm:hasSlot ?slot . \
             ?slot swarm:role ?role ; swarm:requiresAgentKind ?kind ; \
             swarm:minWidth ?min ; swarm:maxWidth ?max }",
        );
        assert_eq!(
            answer,
            cells(&[
                &[
                    Some("slotA"),
                    Some("Child"),
                    Some("agentKindExecutor"),
                    Some("1"),
                    Some("1")
                ],
                &[
                    Some("slotB"),
                    Some("Verifier"),
                    Some("agentKindVerifier"),
                    Some("1"),
                    Some("1")
                ],
            ])
        );
    }

    // spec: EG-ORCH-VOCAB-R003.1
    #[test]
    fn agent_template_is_an_agent() {
        let view = view_of("");
        class_declared(&view, ":AgentTemplate");
        let classification = consistent_with(FIXTURE);
        assert!(classification.entails_subclass(&kg("AgentTemplate"), &kg("Agent")));
        assert!(!classification.entails_subclass(&kg("Agent"), &kg("AgentTemplate")));
        assert!(!classification.entails_subclass(
            &kg("AgentTemplate"),
            &format!("<{SWARM_NS}TopologyTemplate>")
        ));
        // :hasSkill and :usesModel are agent-domain properties: the template takes both.
        assert!(ask(&view_of(FIXTURE), ":agentContainerOps a :AgentTemplate ; :hasSkill ?s ; :usesModel ?m"));
        assert!(ask(&view, ":hasSkill rdfs:domain :Agent"));
        assert!(ask(&view, ":usesModel rdfs:domain :Agent"));
    }

    // spec: EG-ORCH-VOCAB-R003.2
    #[test]
    fn agent_library_resolves() {
        class_declared(&view_of(""), ":AgentLibrary");
    }

    // spec: EG-ORCH-VOCAB-R003.3
    #[test]
    fn member_of_library_resolves() {
        declared(
            &view_of(""),
            ":memberOfLibrary",
            "owl:ObjectProperty",
            None,
            ":AgentLibrary",
        );
    }

    // spec: EG-ORCH-VOCAB-R003.4
    #[test]
    fn instance_of_template_resolves() {
        declared(
            &view_of(""),
            ":instanceOfTemplate",
            "owl:ObjectProperty",
            Some(":Agent"),
            ":AgentTemplate",
        );
    }

    // spec: EG-ORCH-VOCAB-R003.5
    #[test]
    fn member_shape_rejects_a_member_without_capability() {
        assert!(violations(SHAPES, "LibraryMemberShape", FIXTURE).is_empty());
        let found = violations(
            SHAPES,
            "LibraryMemberShape",
            ":useless a :Agent ; :memberOfLibrary :libMain .",
        );
        assert_eq!(found.len(), 1, "{found:?}");
    }

    // spec: EG-ORCH-VOCAB-R003.7
    #[test]
    fn q3_lists_template_and_a2a_candidates() {
        let view = view_of(FIXTURE);
        let answer = table(
            &view,
            "SELECT ?candidate ?kind WHERE { ?candidate :memberOfLibrary :libMain ; \
             :providesCapability :ContainerOrchestrationCapability ; a ?kind . \
             FILTER (?kind IN (:AgentTemplate, :A2AAgentCard, :Agent)) }",
        );
        assert_eq!(
            answer,
            cells(&[
                &[Some("agentContainerOps"), Some("AgentTemplate")],
                &[Some("cardRemoteOps"), Some("A2AAgentCard")],
            ])
        );
    }

    // spec: EG-ORCH-VOCAB-R004.2
    #[test]
    fn applies_to_task_class_resolves() {
        let view = view_of("");
        declared(
            &view,
            ":appliesToTaskClass",
            "owl:ObjectProperty",
            Some(":DevelopmentStandard"),
            "owl:Class",
        );
        let classification = consistent_with(FIXTURE);
        assert!(classification
            .entails_subclass(&kg("BestPractice"), &kg("DevelopmentStandard")));
    }

    // spec: EG-ORCH-VOCAB-R004.3
    #[test]
    fn rule_scope_shape_rejects_a_rule_without_a_class() {
        assert!(violations(SHAPES, "RuleScopeShape", FIXTURE).is_empty());
        for bad in [
            ":rule a :BestPractice .",
            ":rule a :BestPractice ; :appliesToTaskClass swarm:A, swarm:B .",
            ":rule a :BestPractice ; :appliesToTaskClass \"a class\" .",
        ] {
            assert!(!violations(SHAPES, "RuleScopeShape", bad).is_empty(), "{bad}");
        }
    }

    // spec: EG-ORCH-VOCAB-R004.5
    #[test]
    fn q4_returns_entailed_rules_only() {
        let view = view_of(
            "<urn:rule:all> a :BestPractice ; :appliesToTaskClass <eg:task> . \
             <urn:rule:review> a :BestPractice ; :appliesToTaskClass <eg:task/review> .",
        );
        let rules: Vec<GuardrailRule> = rows(
            &view,
            "SELECT ?rule ?class WHERE { ?rule a :BestPractice ; :appliesToTaskClass ?class }",
        )
        .into_iter()
        .map(|row| {
            let strip = |cell: &Option<String>| {
                cell.as_deref().unwrap().trim_matches(['<', '>']).to_string()
            };
            GuardrailRule {
                rule_id: strip(&row[0]),
                applies_to_class: strip(&row[1]),
                source_generation: "gen-1".to_string(),
            }
        })
        .collect();
        assert_eq!(rules.len(), 2);
        let entailed = entailed_guardrails("eg:task/research", &rules);
        assert_eq!(entailed.len(), 1);
        assert_eq!(entailed[0].rule_id, "urn:rule:all");
        assert_eq!(entailed[0].entailment_path.len(), 1);
        assert_eq!(entailed[0].entailment_path[0].narrower, "eg:task/research");
        assert_eq!(entailed[0].entailment_path[0].broader, "eg:task");
    }

    // spec: EG-ORCH-VOCAB-R005.1
    #[test]
    fn model_profile_is_a_model() {
        let view = view_of("");
        class_declared(&view, ":ModelProfile");
        let classification = consistent_with(FIXTURE);
        assert!(classification.entails_subclass(&kg("ModelProfile"), &kg("Model")));
        assert!(ask(&view, ":contextWindow rdfs:domain :Model"));
        assert!(ask(&view_of(FIXTURE), ":profileLocal a :ModelProfile ; :contextWindow 32768"));
    }

    // spec: EG-ORCH-VOCAB-R005.2
    #[test]
    fn needs_skill_resolves() {
        declared(
            &view_of(""),
            ":needsSkill",
            "owl:ObjectProperty",
            Some(":TaskStep"),
            ":Skill",
        );
    }

    // spec: EG-ORCH-VOCAB-R005.3
    #[test]
    fn step_assigned_agent_resolves() {
        let view = view_of("");
        assert!(ask(&view, ":stepAssignedAgent a owl:ObjectProperty ; rdfs:domain :TaskStep"));
        assert!(!ask(&view, ":stepAssignedAgent rdfs:range ?range"));
        // The existing :assignedAgent keeps its A2A-card range and is left alone.
        assert!(ask(&view, ":assignedAgent rdfs:range :A2AAgentCard"));
        let classification = consistent_with(FIXTURE);
        assert!(!classification.entails_subclass(&kg("Agent"), &kg("A2AAgentCard")));
        assert!(!classification.entails_subclass(&kg("AgentTemplate"), &kg("A2AAgentCard")));
    }

    // spec: EG-ORCH-VOCAB-R005.4
    #[test]
    fn binding_shape_rejects_agent_without_model() {
        assert!(violations(SHAPES, "StepBindingShape", FIXTURE).is_empty());
        let step = |agent: &str| format!(":s a :TaskStep ; :stepAssignedAgent :x . {agent}");
        let a2a = step(":x a :A2AAgentCard .");
        assert!(violations(SHAPES, "StepBindingShape", &a2a).is_empty());
        let cases = [
            (step(":x a :Agent ; :hasPrompt :p ."), "no model"),
            (step(":x a :Agent ; :usesModel :m ."), "no prompt"),
            (
                step(":x a :Agent ; :usesModel :m, :n ; :hasPrompt :p ."),
                "more than one model",
            ),
            (
                step(":x a :Agent ; :usesModel :m ; :hasPrompt :p, :q ."),
                "more than one prompt",
            ),
            (step(":x a :Tool ."), "neither an agent nor an A2A"),
        ];
        for (data, expected) in cases {
            let found = violations(SHAPES, "StepBindingShape", &data);
            assert!(found.iter().any(|m| m.contains(expected)), "{expected}: {found:?}");
        }
    }

    // spec: EG-ORCH-VOCAB-R005.6
    #[test]
    fn q5_returns_bindings_per_step() {
        let view = view_of(FIXTURE);
        let answer = table(
            &view,
            "SELECT ?s ?agent ?skill ?model ?prompt WHERE { ?s :stepAssignedAgent ?agent . \
             ?agent :hasSkill ?skill ; :usesModel ?model ; :hasPrompt ?prompt }",
        );
        assert_eq!(
            answer,
            cells(&[
                &[
                    Some("s3"),
                    Some("agentContainerOps"),
                    Some("skillContainerLifecycle"),
                    Some("profileLocal"),
                    Some("promptOps")
                ],
                &[
                    Some("s4"),
                    Some("agentVerifier"),
                    Some("skillHealthVerify"),
                    Some("profileLocal"),
                    Some("promptVerify")
                ],
            ])
        );
        let window = table(
            &view,
            "SELECT ?window WHERE { :s3 :stepAssignedAgent ?agent . ?agent :usesModel ?m . \
             ?m :contextWindow ?window }",
        );
        assert_eq!(window, cells(&[&[Some("32768")]]));
    }

    // spec: EG-ORCH-VOCAB-R006.1
    #[test]
    fn hands_off_to_implies_precedes() {
        let view = view_of("");
        declared(&view, ":handsOffTo", "owl:ObjectProperty", Some(":TaskStep"), ":TaskStep");
        assert!(ask(&view, ":handsOffTo rdfs:subPropertyOf :precedes"));
        let mut triples = schema();
        triples.extend(
            parse_turtle(&format!(
                "{TURTLE_PREFIXES}:a a :TaskStep ; :handsOffTo :b . :b a :TaskStep ."
            ))
            .unwrap(),
        );
        let entailed = saturate_object_properties(&triples);
        assert!(entailed.iter().any(|fact| {
            fact.subject == format!("<{KG}a>")
                && fact.predicate == format!("<{KG}precedes>")
                && fact.object == format!("<{KG}b>")
                && !fact.asserted
        }));
        consistent_with(FIXTURE);
    }

    // spec: EG-ORCH-VOCAB-R006.2
    #[test]
    fn feeds_resolves() {
        declared(
            &view_of(""),
            "swarm:feeds",
            "owl:ObjectProperty",
            Some("swarm:Slot"),
            "swarm:Slot",
        );
    }

    // spec: EG-ORCH-VOCAB-R006.3
    #[test]
    fn loops_back_to_resolves() {
        declared(
            &view_of(""),
            "swarm:loopsBackTo",
            "owl:ObjectProperty",
            Some("swarm:Slot"),
            "swarm:Slot",
        );
    }

    // spec: EG-ORCH-VOCAB-R006.4
    #[test]
    fn feeds_shape_rejects_a_forward_cycle_and_accepts_a_loop_back_edge() {
        let shape = "FeedsAcyclicShape";
        assert!(violations(SWARM_SHAPES, shape, FIXTURE).is_empty());
        let template = |edges: &str| {
            format!(
                ":t a swarm:TopologyTemplate ; swarm:hasSlot :a, :b . \
                 :a a swarm:Slot . :b a swarm:Slot . {edges}"
            )
        };
        let looped = template(":a swarm:feeds :b . :b swarm:loopsBackTo :a .");
        assert!(violations(SWARM_SHAPES, shape, &looped).is_empty());
        let cycle = template(":a swarm:feeds :b . :b swarm:feeds :a .");
        assert!(violations(SWARM_SHAPES, shape, &cycle)
            .iter()
            .any(|m| m.contains("reaches itself")));
        let outside = template(":a swarm:feeds :elsewhere .");
        assert!(violations(SWARM_SHAPES, shape, &outside)
            .iter()
            .any(|m| m.contains("template's own slots")));
    }

    // spec: EG-ORCH-VOCAB-R006.6
    #[test]
    fn q6_returns_handoffs() {
        let view = view_of(FIXTURE);
        let answer = table(
            &view,
            "SELECT ?s ?t WHERE { :DeployContainerTask :hasStep ?s . ?s :handsOffTo ?t }",
        );
        assert_eq!(answer, cells(&[&[Some("s1"), Some("s2")]]));
    }

    // spec: EG-ORCH-VOCAB-R006.6
    #[test]
    fn q7_returns_dag_adjacency() {
        let view = view_of(FIXTURE);
        let nodes = table(&view, "SELECT ?slot WHERE { :tmplPipeline swarm:hasSlot ?slot }");
        assert_eq!(nodes, cells(&[&[Some("slotA")], &[Some("slotB")]]));
        let edges = table(
            &view,
            "SELECT ?a ?b WHERE { :tmplPipeline swarm:hasSlot ?a, ?b . ?a swarm:feeds ?b }",
        );
        assert_eq!(edges, cells(&[&[Some("slotA"), Some("slotB")]]));
        assert!(violations(SWARM_SHAPES, "FeedsAcyclicShape", FIXTURE).is_empty());
    }

    // spec: EG-ORCH-VOCAB-R009.9
    #[test]
    fn applied_harness_config_links_trace_to_decision() {
        let view = view_of("");
        declared(
            &view,
            ":appliedHarnessConfig",
            "owl:ObjectProperty",
            Some(":RunTrace"),
            ":Decision",
        );
        let linked = view_of(":trace a :RunTrace ; :appliedHarnessConfig :decision . :decision a :Decision .");
        assert!(ask(&linked, ":trace :appliedHarnessConfig ?d . ?d a :Decision"));
    }

    // spec: EG-ORCH-VOCAB-R009.10
    #[test]
    fn run_model_declared() {
        declared(
            &view_of(""),
            ":runModel",
            "owl:ObjectProperty",
            Some(":RunTrace"),
            ":ModelProfile",
        );
        // :usesModel keeps its agent domain; per-model run queries use :runModel.
        assert!(ask(&view_of(""), ":usesModel rdfs:domain :Agent"));
    }

    // spec: EG-ORCH-VOCAB-R009.11
    #[test]
    fn run_task_class_declared() {
        let view = view_of("");
        declared(
            &view,
            ":runTaskClass",
            "owl:ObjectProperty",
            Some(":RunTrace"),
            ":TaskClass",
        );
        class_declared(&view, ":TaskClass");
    }

    fn trace(without: &str) -> String {
        let required = [
            ("terminationReason", "\"final_answer\""),
            ("peakContextTokens", "10"),
            ("contextBudgetTokens", "20"),
            ("elisionCount", "0"),
            ("summarizationCount", "0"),
            ("turnCount", "3"),
            ("toolCallCount", "2"),
            ("runModel", ":profileLocal"),
            ("runTaskClass", ":taskClass"),
            ("appliedHarnessConfig", ":decision"),
        ];
        let properties: String = required
            .iter()
            .filter(|(name, _)| *name != without)
            .map(|(name, value)| format!("; :{name} {value} "))
            .collect();
        format!(":trace a :RunTrace ; :status \"completed\" {properties}.")
    }

    // spec: EG-ORCH-VOCAB-R009.14
    #[test]
    fn completed_trace_shape_rejects_missing_termination_reason() {
        let shape = "RunTraceCompletedShape";
        assert!(violations(SHAPES, shape, &trace("")).is_empty());
        for missing in [
            "terminationReason",
            "peakContextTokens",
            "contextBudgetTokens",
            "elisionCount",
            "summarizationCount",
            "turnCount",
            "toolCallCount",
            "runModel",
            "runTaskClass",
            "appliedHarnessConfig",
        ] {
            assert!(
                !violations(SHAPES, shape, &trace(missing)).is_empty(),
                "{missing} must be required"
            );
        }
        let running = ":trace a :RunTrace ; :status \"running\" .";
        assert!(violations(SHAPES, shape, running).is_empty());
        assert!(violations(SHAPES, shape, ":trace a :RunTrace .").is_empty());
    }

    // spec: EG-ORCH-VOCAB-R011.1
    #[test]
    fn evaluation_run_is_a_run_trace() {
        class_declared(&view_of(""), ":EvaluationRun");
        let classification = consistent_with("");
        assert!(classification.entails_subclass(&kg("EvaluationRun"), &kg("RunTrace")));
        assert!(!classification.entails_subclass(&kg("RunTrace"), &kg("EvaluationRun")));
    }

    // spec: EG-ORCH-VOCAB-R011.2
    #[test]
    fn measured_model_fact_declared() {
        class_declared(&view_of(""), ":MeasuredModelFact");
    }

    // spec: EG-ORCH-VOCAB-R011.3
    #[test]
    fn fact_of_model_and_measured_property_declared() {
        let view = view_of("");
        declared(&view, ":factOfModel", "owl:ObjectProperty", Some(":MeasuredModelFact"), ":ModelProfile");
        declared(&view, ":measuredProperty", "owl:DatatypeProperty", Some(":MeasuredModelFact"), "xsd:string");
    }

    // spec: EG-ORCH-VOCAB-R011.4
    #[test]
    fn evidenced_by_run_declared() {
        declared(
            &view_of(""),
            ":evidencedByRun",
            "owl:ObjectProperty",
            Some(":MeasuredModelFact"),
            ":EvaluationRun",
        );
    }

    // spec: EG-ORCH-VOCAB-R011.5
    #[test]
    fn measured_supersedes_declared() {
        declared(
            &view_of(""),
            ":measuredSupersedes",
            "owl:ObjectProperty",
            Some(":MeasuredModelFact"),
            ":MeasuredModelFact",
        );
    }

    // spec: EG-ORCH-VOCAB-R011.6
    #[test]
    fn measured_fact_shape_rejects_missing_evidence() {
        let shape = "MeasuredModelFactShape";
        let fact = |extra: &str| {
            format!(
                ":fact a :MeasuredModelFact ; :factOfModel :profileLocal {extra} ."
            )
        };
        for good in [
            "; :evidencedByRun :run ; :measuredProperty \"shellProficiency\" ; :measuredValue \"high\"",
            "; :evidencedByRun :run ; :measuredProperty \"stoppingBehaviour\" ; :measuredValue \"balanced\"",
            "; :evidencedByRun :run ; :measuredProperty \"contextWindowTokens\" ; :measuredValue 32768",
        ] {
            assert!(violations(SHAPES, shape, &fact(good)).is_empty(), "{good}");
        }
        for bad in [
            "; :measuredProperty \"shellProficiency\" ; :measuredValue \"high\"",
            "; :evidencedByRun :run, :other ; :measuredProperty \"shellProficiency\" ; :measuredValue \"high\"",
            "; :evidencedByRun :run ; :measuredProperty \"speed\" ; :measuredValue \"high\"",
            "; :evidencedByRun :run ; :measuredProperty \"shellProficiency\" ; :measuredValue \"extreme\"",
            "; :evidencedByRun :run ; :measuredProperty \"stoppingBehaviour\" ; :measuredValue \"high\"",
            "; :evidencedByRun :run ; :measuredProperty \"contextWindowTokens\" ; :measuredValue 0",
            "; :evidencedByRun :run ; :measuredProperty \"contextWindowTokens\" ; :measuredValue \"big\"",
        ] {
            assert!(!violations(SHAPES, shape, &fact(bad)).is_empty(), "{bad}");
        }
    }
}
