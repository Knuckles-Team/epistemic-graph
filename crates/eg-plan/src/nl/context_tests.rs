//! Schema-grounded NL planning over ingested ServiceNow data
//! (EG-FEDERATED-QUERY-R058.4.7, R058.4.8).
//!
//! The fixture is shaped the way the connector source runner stores ServiceNow
//! records: `node_type` is the declared record type and the properties keep the
//! ServiceNow field names (`assigned_to`, `assignment_group`, `owned_by`,
//! `managed_by_group`). Relationships carry the ServiceNow pack's object
//! property names. The pack's ontology properties are linked the way the
//! served handler links the composed GraphSchema.
//!
//! The question table holds the 26 evidence-run questions
//! (`plans/arbora-validation-run-2026-10-24.md`, C3). A scripted planner answers
//! each one; where the run recorded a bad answer, the first scripted answer is
//! that answer, so the table proves the correction loop refuses or repairs it.
//! Every row asserts the outcome: valid UQL whose names are all in the schema,
//! and the expected rows; or a typed refusal when the graph holds no data for
//! the question (GitLab, GitHub and Plane are not ingested; prompts and skills
//! are not graph nodes).

use std::collections::BTreeSet;
use std::sync::Mutex;

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use serde_json::json;

use super::*;
use crate::exec::{execute, PlanCtx};
use crate::nl::{NlPlanner, NlQueryBudget, NlQueryError};

/// Answers each call with the next scripted text (the last one repeats) and
/// records the schema hint it was given.
struct ScriptedPlanner {
    answers: Vec<String>,
    calls: Mutex<Vec<String>>,
}

impl ScriptedPlanner {
    fn new(answers: &[&str]) -> Self {
        Self {
            answers: answers.iter().map(|a| a.to_string()).collect(),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn hints(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl NlPlanner for ScriptedPlanner {
    fn plan(&self, _nl: &str, hint: &str) -> Result<String, String> {
        let mut calls = self.calls.lock().unwrap();
        let index = calls.len().min(self.answers.len() - 1);
        calls.push(hint.to_string());
        Ok(self.answers[index].clone())
    }
}

fn blob(value: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&value).unwrap()
}

/// ServiceNow data as the source runner stores it.
fn servicenow_view() -> GraphView {
    let core = GraphCore::new();
    let nodes = [
        (
            "inc_a",
            json!({"node_type": "Incident", "number": "INC-A", "short_description": "PostgreSQL 11 outage on db01", "state": "2", "priority": "1", "assigned_to": "u_alice", "assignment_group": "g_dba", "cmdb_ci": "db01", "sys_updated_on": "2026-10-24 07:00:00"}),
        ),
        (
            "inc_b",
            json!({"node_type": "Incident", "number": "INC-B", "short_description": "Billing Engine and Payments down", "state": "1", "priority": "2", "assigned_to": "u_bob", "assignment_group": "g_fin", "cmdb_ci": "app01", "sys_updated_on": "2026-10-24 07:05:00"}),
        ),
        (
            "inc_c",
            json!({"node_type": "Incident", "number": "INC-C", "short_description": "Slow report", "state": "7", "priority": "4", "assignment_group": "g_fin", "cmdb_ci": "app02", "sys_updated_on": "2026-10-20 07:05:00"}),
        ),
        (
            "db01",
            json!({"node_type": "ConfigurationItem", "name": "db01", "sys_class_name": "cmdb_ci_server", "owned_by": "u_carol", "managed_by_group": "g_dba", "support_group": "g_dba", "install_status": "1", "operational_status": "1"}),
        ),
        (
            "web01",
            json!({"node_type": "ConfigurationItem", "name": "web01", "sys_class_name": "cmdb_ci_server", "owned_by": "u_carol", "managed_by_group": "g_ops", "support_group": "g_ops", "install_status": "1", "operational_status": "1"}),
        ),
        (
            "app01",
            json!({"node_type": "ConfigurationItem", "name": "Billing Engine", "sys_class_name": "cmdb_ci_appl", "owned_by": "u_dave", "managed_by_group": "g_fin", "support_group": "g_fin", "install_status": "1", "operational_status": "1"}),
        ),
        (
            "app02",
            json!({"node_type": "ConfigurationItem", "name": "app02", "sys_class_name": "cmdb_ci_appl", "owned_by": "u_dave", "managed_by_group": "g_fin", "support_group": "g_fin", "install_status": "1", "operational_status": "1"}),
        ),
        (
            "svc_pay",
            json!({"node_type": "ConfigurationItem", "name": "Payments", "sys_class_name": "cmdb_ci_service", "owned_by": "u_erin", "managed_by_group": "g_pay", "support_group": "g_pay", "install_status": "1", "operational_status": "1"}),
        ),
        (
            "chg1",
            json!({"node_type": "ChangeRequest", "number": "CHG-1", "short_description": "Upgrade db01", "state": "assess", "risk": "high"}),
        ),
        (
            "chg2",
            json!({"node_type": "ChangeRequest", "number": "CHG-2", "short_description": "Patch web01", "state": "scheduled", "risk": "low"}),
        ),
        (
            "pg11",
            json!({"node_type": "TRMStandard", "name": "PostgreSQL 11", "phase": "retired"}),
        ),
        (
            "java17",
            json!({"node_type": "TRMStandard", "name": "Java 17", "phase": "approved"}),
        ),
        (
            "tr1",
            json!({"node_type": "TechnologyRequest", "number": "TR-1", "requested_technology": "pg11", "state": "open"}),
        ),
        (
            "tr2",
            json!({"node_type": "TechnologyRequest", "number": "TR-2", "requested_technology": "java17", "state": "closed"}),
        ),
        (
            "debt1",
            json!({"node_type": "TRMTechnicalDebt", "short_description": "PostgreSQL 11 past end of support", "severity": "high"}),
        ),
    ];
    for (id, props) in nodes {
        core.add_node(id.into(), blob(props));
    }
    let edges = [
        ("app01", "db01", "hostedOn"),
        ("app02", "db01", "hostedOn"),
        ("svc_pay", "app02", "dependsOn"),
        ("app02", "app01", "dependsOn"),
        ("app01", "pg11", "usesTechnology"),
        ("app02", "pg11", "usesTechnology"),
        ("inc_a", "db01", "affects"),
        ("inc_b", "app01", "affects"),
        ("inc_c", "app02", "affects"),
        ("chg1", "db01", "touches"),
        ("chg2", "web01", "touches"),
        ("tr1", "pg11", "requestsTechnology"),
        ("debt1", "pg11", "debtOn"),
    ];
    for (source, target, rel) in edges {
        core.add_edge(
            source.into(),
            target.into(),
            blob(json!({ "relationship": rel })),
        )
        .unwrap();
    }
    core.analysis_snapshot()
}

/// The ServiceNow pack's datatype and object properties that name record fields.
fn servicenow_ontology() -> Vec<OntologyProperty> {
    let property = |name: &str, label: &str| OntologyProperty {
        name: name.into(),
        label: Some(label.into()),
        domains: Vec::new(),
    };
    vec![
        property("assignedTo", "assigned to"),
        property("number", "number"),
        property("priority", "priority"),
        property("shortDescription", "short description"),
        property("state", "state"),
        property("hostedOn", "hosted on"),
        property("affects", "affects"),
    ]
}

fn servicenow_context(view: &GraphView) -> SchemaContext {
    let mut context = SchemaContext::from_view(view);
    context.link_ontology(&servicenow_ontology());
    context.declare_foreign_sources(BTreeSet::new());
    context
}

/// What one question must produce.
enum Expect {
    /// Rows come back; every `contains` id is among them and no `excludes` id is.
    Rows {
        contains: &'static [&'static str],
        excludes: &'static [&'static str],
    },
    /// The request is refused, the last step with this reason code.
    Refused(&'static str),
}

struct Case {
    id: &'static str,
    question: &'static str,
    answers: &'static [&'static str],
    expect: Expect,
}

const BLAST_DB01: &str = "MATCH (:ConfigurationItem) WHERE name = 'db01' |> PROPAGATE NOISY_OR <-[:hostedOn]- HOPS 3 |> RETURN impact |> LIMIT 50";
const BLAST_APP02: &str = "MATCH (:ConfigurationItem) WHERE name = 'app02' |> PROPAGATE NOISY_OR <-[:dependsOn]- HOPS 3 |> RETURN impact |> LIMIT 50";
const RETIRED_SERVICES: &str = "MATCH (:TRMStandard) WHERE phase = 'retired' |> TRAVERSE <-[:usesTechnology]- |> TRAVERSE <-[:dependsOn]-";
const OPEN_TRM: &str = "MATCH (:TechnologyRequest) WHERE state = 'open'";
const P1P2_CHANGES: &str = "MATCH (:Incident) WHERE priority IN ('1', '2') AND state != '7' |> TRAVERSE -[:affects]-> |> TRAVERSE <-[:touches]-";
const CHANGE_TECH: &str = "MATCH (:ChangeRequest) |> TRAVERSE -[:touches]-> |> TRAVERSE <-[:hostedOn]- |> TRAVERSE -[:usesTechnology]->";
const DEBT: &str = "MATCH (:TRMTechnicalDebt) |> TRAVERSE -[:debtOn]->";
const SERVERS: &str = "MATCH (:ConfigurationItem) WHERE sys_class_name = 'cmdb_ci_server'";
const INCIDENT_OWNERS: &str = "MATCH (:Incident) |> WHERE assignedTo IS NOT NULL";

/// The 26 evidence-run questions (Q1-Q3, CMDB1-10, CQ-a..c = Q1-Q3, CQ-d1-d10).
fn cases() -> Vec<Case> {
    use Expect::{Refused, Rows};
    let q1 = Case {
        id: "Q1",
        question: "Who owns ServiceNow incidents, and where does that data come from?",
        // The run's answer filtered on a property Incident rows do not store.
        answers: &[
            "MATCH (:Incident) |> WHERE source = 'ServiceNow'",
            INCIDENT_OWNERS,
        ],
        expect: Rows {
            contains: &["inc_a", "inc_b"],
            excludes: &["inc_c", "db01"],
        },
    };
    let q2 = Case {
        id: "Q2",
        question: "Show open issues across GitHub, GitLab and Plane.",
        answers: &["MATCH (:Issue) |> WHERE status = 'open'"],
        expect: Refused("NL_SCHEMA_UNKNOWN_LABEL"),
    };
    let q3 = Case {
        id: "Q3",
        question: "Which prompts can act as a system prompt for incident triage?",
        answers: &["MATCH (:Prompt) |> WHERE kind = 'system'"],
        expect: Refused("NL_SCHEMA_UNKNOWN_LABEL"),
    };
    let mut out = vec![
        Case {
            id: "CMDB1",
            question: "What is the blast radius of a db01 outage: which applications, business services and owner groups are affected?",
            answers: &[BLAST_DB01],
            expect: Rows { contains: &["app01", "app02"], excludes: &["web01", "chg2"] },
        },
        Case {
            id: "CMDB2",
            question: "What is the blast radius of an app02 outage, including application-to-application dependencies?",
            // A spelling variant of the stored relationship is repaired.
            answers: &["MATCH (:ConfigurationItem) WHERE name = 'app02' |> PROPAGATE NOISY_OR <-[:DEPENDS_ON]- HOPS 3 |> RETURN impact |> LIMIT 50"],
            expect: Rows { contains: &["svc_pay"], excludes: &["web01"] },
        },
        Case {
            id: "CMDB3",
            question: "Which business services depend on retired or prohibited technology and have open incidents?",
            answers: &["MATCH (:TRMStandard) |> WHERE status = 'retired'", RETIRED_SERVICES],
            expect: Rows { contains: &["svc_pay"], excludes: &["web01", "java17"] },
        },
        Case {
            id: "CMDB4",
            question: "Which technologies in use have an open TRM request, and where are they deployed?",
            // The run's answer RETURNed a property as a score channel.
            answers: &["MATCH (:TechnologyRequest) |> RETURN technology", OPEN_TRM],
            expect: Rows { contains: &["tr1"], excludes: &["tr2"] },
        },
        Case {
            id: "CMDB5",
            question: "Which change requests touch CIs with open P1 or P2 incidents?",
            // The run's answer had a stray `(` after the pipeline.
            answers: &["MATCH (:ChangeRequest) |> LIMIT 10 (touches)", P1P2_CHANGES],
            expect: Rows { contains: &["chg1"], excludes: &["chg2"] },
        },
        Case {
            id: "CMDB6",
            question: "What is the change risk by technology state for each change request, joining touched CIs to technology and TRM phase?",
            answers: &[CHANGE_TECH],
            expect: Rows { contains: &["pg11"], excludes: &["java17"] },
        },
        Case {
            id: "CMDB7",
            question: "What is the technology debt by owner group of the dependent service?",
            answers: &["MATCH (:TRMTechnicalDebt) |> RETURN owner_group", DEBT],
            expect: Rows { contains: &["pg11"], excludes: &["java17"] },
        },
        Case {
            id: "CMDB8",
            question: "Which CIs are orphaned or isolated, such as servers that host no application?",
            // One pipeline has no anti-join: the grounded answer is the server set.
            answers: &["MATCH (:ConfigurationItem) |> WHERE status = 'orphan'", SERVERS],
            expect: Rows { contains: &["db01", "web01"], excludes: &["app01"] },
        },
        Case {
            id: "CMDB9",
            question: "Which GitLab or GitHub repositories and Plane work items relate to the Billing Engine and Payments applications affected by incident INC-B?",
            answers: &["MATCH (:Repository) |> WHERE incident_id = 'INC-B'"],
            expect: Refused("NL_SCHEMA_UNKNOWN_LABEL"),
        },
        Case {
            id: "CMDB10",
            question: "Which prompts, skills or agents could triage incident INC-A, a PostgreSQL 11 database outage on db01?",
            answers: &["MATCH (:Skill) |> WHERE incident_id = 'INC-A'"],
            expect: Refused("NL_SCHEMA_UNKNOWN_LABEL"),
        },
        Case {
            id: "CQ-d1",
            question: "Blast radius of a server outage: which applications run on it, which business services depend on them, who owns those services?",
            // The run grounded on the only label it had then.
            answers: &["MATCH (:ActionDecision) |> PROPAGATE NOISY_OR <-[:dependsOn]- HOPS 4 |> RETURN impact |> LIMIT 50", BLAST_DB01],
            expect: Rows { contains: &["app01", "app02"], excludes: &["web01"] },
        },
        Case {
            id: "CQ-d2",
            question: "Blast radius of an application outage, including application-to-application dependency.",
            answers: &[BLAST_APP02],
            expect: Rows { contains: &["svc_pay"], excludes: &["web01"] },
        },
        Case {
            id: "CQ-d3",
            question: "Which business services depend on retired technology and have open incidents?",
            answers: &[RETIRED_SERVICES],
            expect: Rows { contains: &["svc_pay"], excludes: &["web01"] },
        },
        Case {
            id: "CQ-d4",
            question: "Which technologies in use have an open TRM request, and where?",
            answers: &["MATCH (:TechnologyRequest) |> RETURN technology", OPEN_TRM],
            expect: Rows { contains: &["tr1"], excludes: &["tr2"] },
        },
        Case {
            id: "CQ-d5",
            question: "Which change requests touch CIs with open P1/P2 incidents?",
            answers: &["MATCH (:Incident) |> WHERE status = 'open'", P1P2_CHANGES],
            expect: Rows { contains: &["chg1"], excludes: &["chg2"] },
        },
        Case {
            id: "CQ-d6",
            question: "Change risk by technology state: touched CIs joined to technology and TRM phase.",
            answers: &["MATCH (:ChangeRequest) |> LIMIT 5 (", CHANGE_TECH],
            expect: Rows { contains: &["pg11"], excludes: &["java17"] },
        },
        Case {
            id: "CQ-d7",
            question: "Technology debt by owner group of the dependent service.",
            answers: &["MATCH (:TRMTechnicalDebt) |> RETURN owner_group", DEBT],
            expect: Rows { contains: &["pg11"], excludes: &["java17"] },
        },
        Case {
            id: "CQ-d8",
            question: "Orphan and isolated CIs: servers that host no application.",
            answers: &[SERVERS],
            expect: Rows { contains: &["db01", "web01"], excludes: &["app02"] },
        },
        Case {
            id: "CQ-d9",
            question: "Cross-source: repositories and work items for an affected application.",
            // The run invented a federation source from the grammar example.
            answers: &["FOREIGN 'peer-east' |> LIMIT 10", "MATCH (:Repository)"],
            expect: Refused("NL_SCHEMA_UNKNOWN_LABEL"),
        },
        Case {
            id: "CQ-d10",
            question: "Which prompts, skills or agents could triage this incident.",
            answers: &["MATCH (:Agent) |> LIMIT 10"],
            expect: Refused("NL_SCHEMA_UNKNOWN_LABEL"),
        },
    ];
    let aliases = [("CQ-a", &q1), ("CQ-b", &q2), ("CQ-c", &q3)].map(|(id, case)| Case {
        id,
        question: case.question,
        answers: case.answers,
        expect: match case.expect {
            Rows { contains, excludes } => Rows { contains, excludes },
            Refused(code) => Refused(code),
        },
    });
    out.splice(0..0, [q1, q2, q3]);
    out.extend(aliases);
    out
}

fn run_case(case: &Case, view: &GraphView, context: &SchemaContext) {
    let planner = ScriptedPlanner::new(case.answers);
    let outcome = plan_grounded(
        &planner,
        case.question,
        context,
        NlQueryBudget::default(),
        DEFAULT_MAX_STEPS,
    );
    match (&case.expect, outcome) {
        (Expect::Rows { contains, excludes }, Ok(candidate)) => {
            // The disclosed UQL parses and is already fully grounded.
            let reparsed = crate::uql::parse(&candidate.uql)
                .unwrap_or_else(|e| panic!("{}: disclosed UQL is invalid: {e:?}", case.id));
            let regrounded = context
                .ground(&reparsed)
                .expect("disclosed UQL is grounded");
            assert!(
                regrounded.repairs.is_empty(),
                "{}: {:?}",
                case.id,
                regrounded.repairs
            );
            let semantic = SemanticStore::new();
            let rows = execute(&candidate.plan, &PlanCtx::new(view, &semantic))
                .unwrap_or_else(|e| panic!("{}: execution failed: {e}", case.id));
            let ids = rows.id_set();
            for id in *contains {
                assert!(ids.contains(id), "{}: {id} missing from {ids:?}", case.id);
            }
            for id in *excludes {
                assert!(
                    !ids.contains(id),
                    "{}: {id} must not be in {ids:?}",
                    case.id
                );
            }
        }
        (Expect::Refused(code), Err(NlQueryError::Ungrounded { steps })) => {
            let last = steps.last().expect("a refused request has steps");
            assert!(
                last.outcome.starts_with(code),
                "{}: {}",
                case.id,
                last.outcome
            );
            assert_eq!(steps.len(), DEFAULT_MAX_STEPS, "{}", case.id);
        }
        (_, other) => panic!("{}: unexpected outcome {other:?}", case.id),
    }
}

// spec: EG-FEDERATED-QUERY-R058.4.7
#[test]
fn evidence_questions_are_grounded_or_refused_over_servicenow_data() {
    let view = servicenow_view();
    let context = servicenow_context(&view);
    let cases = cases();
    assert_eq!(cases.len(), 26);
    for case in &cases {
        run_case(case, &view, &context);
    }
}

// spec: EG-FEDERATED-QUERY-R058.4.8
#[test]
fn mapped_servicenow_properties_are_offered_under_their_ontology_names() {
    let view = servicenow_view();
    let context = servicenow_context(&view);
    let incident = context.label("Incident").expect("Incident is stored");
    assert_eq!(incident.ontology["assignedTo"], "assigned_to");
    assert_eq!(incident.ontology["shortDescription"], "short_description");
    let hint = context.hint("Who owns ServiceNow incidents?");
    assert!(hint.contains(":Incident (3 nodes)"), "{hint}");
    assert!(hint.contains("assignedTo = assigned_to"), "{hint}");
    assert!(hint.contains("assignment_group"), "{hint}");
    assert!(hint.contains("owned_by"), "{hint}");
    assert!(hint.contains("hostedOn"), "{hint}");
    // The question's label comes first.
    let first = hint.lines().nth(2).unwrap_or_default();
    assert!(first.starts_with("  :Incident"), "{hint}");

    // An ontology name in a predicate runs as the stored key, disclosed as such.
    let planner = ScriptedPlanner::new(&[INCIDENT_OWNERS]);
    let candidate =
        plan_grounded(&planner, "owners", &context, NlQueryBudget::default(), 2).expect("grounded");
    assert!(candidate.uql.contains("assigned_to"), "{}", candidate.uql);
    assert!(!candidate.uql.contains("assignedTo"), "{}", candidate.uql);
    assert_eq!(
        candidate.repairs,
        vec![Repair {
            from: "assignedTo".into(),
            to: "assigned_to".into()
        }]
    );
}

// spec: EG-FEDERATED-QUERY-R058.4.7
#[test]
fn a_refused_step_goes_back_to_the_planner_with_its_typed_reason() {
    let view = servicenow_view();
    let context = servicenow_context(&view);
    let planner = ScriptedPlanner::new(&[
        "MATCH (:Incident) |> WHERE source = 'ServiceNow'",
        INCIDENT_OWNERS,
    ]);
    let candidate = plan_grounded(&planner, "owners", &context, NlQueryBudget::default(), 2)
        .expect("second step grounded");
    assert_eq!(candidate.steps.len(), 2);
    assert!(candidate.steps[0]
        .outcome
        .starts_with("NL_SCHEMA_UNKNOWN_PROPERTY: property `source`"));
    assert_eq!(candidate.steps[1].outcome, "accepted");
    let hints = planner.hints();
    assert!(!hints[0].contains("previous answer was refused"));
    assert!(
        hints[1].contains("NL_SCHEMA_UNKNOWN_PROPERTY"),
        "{}",
        hints[1]
    );
}

// spec: EG-FEDERATED-QUERY-R058.4.7
#[test]
fn unknown_names_are_typed_violations_and_never_execute() {
    let view = servicenow_view();
    let context = servicenow_context(&view);
    let violation = |uql: &str| {
        context
            .ground(&crate::uql::parse(uql).unwrap())
            .unwrap_err()
    };
    assert!(matches!(
        violation("MATCH (:Ticket)"),
        SchemaViolation::UnknownLabel { .. }
    ));
    assert!(matches!(
        violation("MATCH (:Incident) |> WHERE owner = 'x'"),
        SchemaViolation::UnknownProperty { .. }
    ));
    // A property another label stores is still unknown on this one.
    assert!(matches!(
        violation("MATCH (:Incident) |> WHERE owned_by = 'u_carol'"),
        SchemaViolation::UnknownProperty { .. }
    ));
    assert!(matches!(
        violation("MATCH (:Incident) |> TRAVERSE -[:ownedBy]->"),
        SchemaViolation::UnknownRelationship { .. }
    ));
    assert!(matches!(
        violation("FOREIGN 'peer-east'"),
        SchemaViolation::UnknownSource { .. }
    ));
    // Nested predicates are checked too.
    assert!(matches!(
        violation("MATCH (:Incident) WHERE priority = '1' AND NOT (owner = 'x')"),
        SchemaViolation::UnknownProperty { .. }
    ));
}

// spec: EG-FEDERATED-QUERY-R058.4.7
#[test]
fn rows_from_outside_the_local_graph_are_not_checked_against_it() {
    let view = servicenow_view();
    let mut context = SchemaContext::from_view(&view);
    // An undeclared source set leaves FOREIGN to the served source registry.
    let grounded = context
        .ground(&crate::uql::parse("FOREIGN 'remote' |> WHERE anything = 1").unwrap())
        .expect("external rows are not local-schema checked");
    assert!(grounded.repairs.is_empty());
    // A label only the foreign source holds is left to the foreign leg.
    assert!(context
        .ground(&crate::uql::parse("MATCH (:RemoteDoc) |> FOREIGN 'remote' |> LIMIT 10").unwrap())
        .is_ok());
    // Without a federation stage the same label is unknown.
    assert!(matches!(
        context.ground(&crate::uql::parse("MATCH (:RemoteDoc) |> LIMIT 10").unwrap()),
        Err(SchemaViolation::UnknownLabel { .. })
    ));
    context.declare_foreign_sources(BTreeSet::from(["remote".to_string()]));
    assert!(context
        .ground(&crate::uql::parse("FOREIGN 'remote'").unwrap())
        .is_ok());
}

// spec: EG-FEDERATED-QUERY-R058.4.8
#[test]
fn ontology_links_respect_domains_and_ambiguity() {
    let view = servicenow_view();
    let mut context = SchemaContext::from_view(&view);
    context.link_ontology(&[
        OntologyProperty {
            name: "ownedBy".into(),
            label: None,
            domains: vec!["ConfigurationItem".into()],
        },
        OntologyProperty {
            name: "assignedTo".into(),
            label: None,
            domains: vec!["ChangeRequest".into()],
        },
    ]);
    assert_eq!(
        context.label("ConfigurationItem").unwrap().ontology["ownedBy"],
        "owned_by"
    );
    // Outside its domain a property links nothing.
    assert!(context.label("Incident").unwrap().ontology.is_empty());
    assert!(context.label("ChangeRequest").unwrap().ontology.is_empty());
}

// spec: EG-FEDERATED-QUERY-R058.4.7
#[test]
fn an_empty_graph_refuses_every_labelled_match() {
    let context = SchemaContext::from_view(&GraphCore::new().analysis_snapshot());
    assert!(context.is_empty());
    assert_eq!(
        context.hint("anything"),
        "The graph holds no labelled nodes."
    );
    let planner = ScriptedPlanner::new(&["MATCH (:Incident)"]);
    let err = plan_grounded(&planner, "x", &context, NlQueryBudget::default(), 2).unwrap_err();
    assert!(matches!(err, NlQueryError::Ungrounded { ref steps } if steps.len() == 2));
}

// spec: EG-FEDERATED-QUERY-R058.4.7
#[test]
fn an_over_budget_answer_is_refused_at_once() {
    let view = servicenow_view();
    let context = servicenow_context(&view);
    let planner = ScriptedPlanner::new(&["MATCH (:Incident) |> LIMIT 1                    "]);
    let err =
        plan_grounded(&planner, "x", &context, NlQueryBudget { max_uql_len: 8 }, 2).unwrap_err();
    assert!(matches!(err, NlQueryError::BudgetExceeded { limit: 8, .. }));
    assert_eq!(planner.hints().len(), 1);
}
