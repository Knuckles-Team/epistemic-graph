//! Schema-grounded NL planning over ingested ServiceNow data
//! (MEDULLA-FEDERATED-QUERY-R058.4.7.1, R058.4.8).
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

use medulla_core::compute::semantic::SemanticStore;
use medulla_core::graph::{GraphCore, GraphView};
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

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
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

// spec: MEDULLA-FEDERATED-QUERY-R058.4.8
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

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
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

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
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

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[test]
fn foreign_row_properties_are_checked_against_the_local_snapshot() {
    let view = servicenow_view();
    let mut context = SchemaContext::from_view(&view);
    context.declare_foreign_sources(BTreeSet::from(["remote".to_string()]));
    // RowSet carries ids and scores only: WHERE rematerializes local properties.
    assert!(matches!(
        context.ground(&crate::uql::parse("FOREIGN 'remote' |> WHERE anything = 1").unwrap()),
        Err(SchemaViolation::UnknownProperty { .. })
    ));
    assert!(context
        .ground(&crate::uql::parse("FOREIGN 'remote' |> WHERE priority = '1'").unwrap())
        .is_ok());
    // A later foreign source cannot exempt an earlier local label scan.
    assert!(matches!(
        context.ground(
            &crate::uql::parse("MATCH (:RemoteDoc) |> FOREIGN 'remote' |> LIMIT 10").unwrap()
        ),
        Err(SchemaViolation::UnknownLabel { .. })
    ));
    // Nor can it exempt a local predicate before the source changes.
    assert!(matches!(
        context.ground(
            &crate::uql::parse("MATCH (:Incident) WHERE invented = 1 |> FOREIGN 'remote'").unwrap()
        ),
        Err(SchemaViolation::UnknownProperty { .. })
    ));
    // A local scan after foreign rows also returns to local schema checking.
    assert!(matches!(
        context.ground(&Plan::new(vec![
            Op::Foreign {
                name: "remote".into()
            },
            Op::Scan {
                label: "RemoteDoc".into()
            },
        ])),
        Err(SchemaViolation::UnknownLabel { .. })
    ));
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

// spec: MEDULLA-FEDERATED-QUERY-R058.4.8
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

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
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

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
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

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[cfg(feature = "text")]
#[test]
fn fused_branches_ground_local_names_and_repair_properties() {
    let mut context = SchemaContext::from_view(&servicenow_view());
    context.declare_foreign_sources(BTreeSet::from(["remote".to_string()]));
    let plan = |label: &str, prop: &str| {
        Plan::new(vec![
            Op::Scan {
                label: "Incident".into(),
            },
            Op::FuseRrf {
                branches: vec![vec![
                    Op::Scan {
                        label: label.into(),
                    },
                    Op::Filter {
                        preds: vec![Pred::Eq {
                            prop: prop.into(),
                            value: "1".into(),
                        }],
                    },
                ]],
                k: 60.0,
            },
        ])
    };
    assert!(matches!(
        context.ground(&plan("Imaginary", "priority")),
        Err(SchemaViolation::UnknownLabel { .. })
    ));
    assert!(matches!(
        context.ground(&plan("Incident", "imaginary")),
        Err(SchemaViolation::UnknownProperty { .. })
    ));
    let grounded = context.ground(&plan("Incident", "Priority")).unwrap();
    assert_eq!(
        grounded.repairs,
        vec![Repair {
            from: "Priority".into(),
            to: "priority".into()
        }]
    );
    let mut local = plan("Incident", "priority");
    local.ops.push(Op::Filter {
        preds: vec![Pred::Eq {
            prop: "owned_by".into(),
            value: "alice".into(),
        }],
    });
    assert!(matches!(
        context.ground(&local),
        Err(SchemaViolation::UnknownProperty { .. })
    ));
    let external = Plan::new(vec![
        Op::FuseRrf {
            branches: vec![vec![Op::Foreign {
                name: "remote".into(),
            }]],
            k: 60.0,
        },
        Op::Filter {
            preds: vec![Pred::Eq {
                prop: "remote_only".into(),
                value: "x".into(),
            }],
        },
    ]);
    assert!(matches!(
        context.ground(&external),
        Err(SchemaViolation::UnknownProperty { .. })
    ));
}

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[test]
fn stored_label_fields_and_unlabelled_node_fields_remain_queryable() {
    let core = GraphCore::new();
    core.add_node(
        "labelled".into(),
        blob(json!({"node_type": "Incident", "number": "INC-1"})),
    );
    core.add_node(
        "unlabelled".into(),
        blob(json!({"description": "standalone"})),
    );
    let view = core.analysis_snapshot();
    let context = SchemaContext::from_view(&view);
    let semantic = SemanticStore::new();
    let ctx = PlanCtx::new(&view, &semantic);
    for (query, expected) in [
        ("MATCH (:Incident) WHERE node_type = 'Incident'", "labelled"),
        ("MATCH () WHERE description = 'standalone'", "unlabelled"),
    ] {
        let grounded = context.ground(&crate::uql::parse(query).unwrap()).unwrap();
        let rows = execute(&grounded.plan, &ctx).unwrap();
        assert_eq!(rows.ids(), vec![expected.to_string()]);
    }
    assert!(context.hint("description").contains("description"));
}

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[test]
fn edge_predicates_are_grounded_against_edges_instead_of_nodes() {
    use medulla_types::wire::{EdgeDir, PropagateModel};
    let core = GraphCore::new();
    for id in ["a", "b"] {
        core.add_node(id.into(), blob(json!({"type": "Item", "node_only": true})));
    }
    core.add_edge(
        "a".into(),
        "b".into(),
        blob(json!({"relationship": "LINK", "edge_only": "yes"})),
    )
    .unwrap();
    core.add_edge("a".into(), "b".into(), blob(json!({"untyped_only": "yes"})))
        .unwrap();
    let context = SchemaContext::from_view(&core.analysis_snapshot());
    for prop in ["edge_only", "Edge_Only", "node_only", "invented"] {
        let preds = vec![Pred::Eq {
            prop: prop.into(),
            value: "yes".into(),
        }];
        for op in [
            Op::Expand {
                rel: Some("LINK".into()),
                dir: EdgeDir::Out,
                min: 1,
                max: 1,
                edge_preds: preds.clone(),
            },
            Op::Propagate {
                model: PropagateModel::NoisyOr,
                rel: None,
                dir: EdgeDir::Out,
                edge_preds: preds.clone(),
                hops: 1,
                default_transmission: 1.0,
            },
        ] {
            let result = context.ground(&Plan::new(vec![op]));
            if prop == "edge_only" || prop == "Edge_Only" {
                let grounded = result.unwrap();
                assert_eq!(grounded.repairs.len(), usize::from(prop == "Edge_Only"));
            } else {
                assert!(matches!(
                    result,
                    Err(SchemaViolation::UnknownProperty { .. })
                ));
            }
        }
    }
    for (rel, accepted) in [(None, true), (Some("LINK".into()), false)] {
        let op = Op::Expand {
            rel,
            dir: EdgeDir::Out,
            min: 1,
            max: 1,
            edge_preds: vec![Pred::Eq {
                prop: "untyped_only".into(),
                value: "yes".into(),
            }],
        };
        assert_eq!(context.ground(&Plan::new(vec![op])).is_ok(), accepted);
    }
}

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[test]
fn jsonpath_root_keys_are_checked_without_guessing_nested_document_shapes() {
    let context = SchemaContext::from_view(&servicenow_view());
    for path in [
        "$.invented.value",
        "$['invented']",
        "invented",
        "$.id",
        "$.Priority",
    ] {
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Incident".into(),
            },
            Op::Filter {
                preds: vec![Pred::JsonPath {
                    path: path.into(),
                    op: medulla_types::wire::JsonPathOp::Exists,
                }],
            },
        ]);
        assert!(matches!(
            context.ground(&plan),
            Err(SchemaViolation::UnknownProperty { .. })
        ));
    }
    for path in ["$.priority", "$['priority']", "$", "$.*"] {
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Incident".into(),
            },
            Op::Filter {
                preds: vec![Pred::JsonPath {
                    path: path.into(),
                    op: medulla_types::wire::JsonPathOp::Exists,
                }],
            },
        ]);
        assert!(context.ground(&plan).is_ok());
    }
}

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[test]
fn snapshot_schema_is_complete_past_the_former_scan_cap() {
    const FORMER_SCAN_CAP: usize = 4096;
    let core = GraphCore::new();
    for n in 0..FORMER_SCAN_CAP + 17 {
        core.add_node(
            format!("node{n}"),
            blob(json!({"node_type": format!("Label{n}"), "amount": n})),
        );
    }
    let view = core.analysis_snapshot();
    let late = medulla_types::msgpack::decode_property_value(
        view.node_properties.values().nth(FORMER_SCAN_CAP).unwrap(),
    )
    .unwrap();
    let label = late["node_type"].as_str().unwrap();
    let context = SchemaContext::from_view(&view);
    assert_eq!(context.labels.len(), FORMER_SCAN_CAP + 17);
    assert!(context
        .ground(&Plan::new(vec![
            Op::Scan {
                label: label.into()
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "amount".into(),
                    n: -1.0
                }]
            }
        ]))
        .is_ok());
}

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[test]
fn attribution_properties_and_owen_groups_are_grounded_and_repaired() {
    use medulla_types::wire::{AttributionInput, AttributionMethod, AttributionValue};
    let context = SchemaContext::from_view(&servicenow_view());
    let plan = |name: &str, by: &str| {
        Plan::new(vec![
            Op::Scan {
                label: "Incident".into(),
            },
            Op::Attribute {
                input: AttributionInput::Property { name: name.into() },
                value: AttributionValue::Sum,
                method: AttributionMethod::Owen { by: by.into() },
            },
        ])
    };
    for plan in [
        plan("invented", "assignment_group"),
        plan("priority", "invented"),
    ] {
        assert!(matches!(
            context.ground(&plan),
            Err(SchemaViolation::UnknownProperty { .. })
        ));
    }
    let plan = context
        .ground(&plan("Priority", "Assignment_Group"))
        .unwrap();
    assert_eq!(plan.repairs.len(), 2);
    let text = plan.plan.to_uql().unwrap();
    assert!(text.contains("OF priority"), "{text}");
    assert!(text.contains("BY assignment_group"), "{text}");
}

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[cfg(feature = "federation")]
#[test]
fn named_foreign_scans_require_authority_and_joins_preserve_local_scope() {
    use medulla_types::wire::ForeignSourceSpec;
    let mut context = SchemaContext::from_view(&servicenow_view());
    let scan = |name: &str, join| Op::ForeignScan {
        source: Box::new(ForeignSourceSpec::Named { name: name.into() }),
        join,
    };
    let local = |op| {
        Plan::new(vec![
            Op::Scan {
                label: "Incident".into(),
            },
            op,
            Op::Filter {
                preds: vec![Pred::Eq {
                    prop: "invented".into(),
                    value: "x".into(),
                }],
            },
        ])
    };
    assert!(matches!(
        context.ground(&local(scan("remote", true))),
        Err(SchemaViolation::UnboundForeignSource)
    ));
    context.declare_foreign_sources(BTreeSet::from(["remote".to_string()]));
    assert!(matches!(
        context.ground(&local(scan("other-owner", false))),
        Err(SchemaViolation::UnknownSource { .. })
    ));
    assert!(matches!(
        context.ground(&local(scan("remote", true))),
        Err(SchemaViolation::UnknownProperty { .. })
    ));
    assert!(matches!(
        context.ground(&local(scan("remote", false))),
        Err(SchemaViolation::UnknownProperty { .. })
    ));
}

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[cfg(feature = "text")]
#[test]
fn mixed_fusion_cannot_erase_local_property_checks() {
    let mut context = SchemaContext::from_view(&servicenow_view());
    context.declare_foreign_sources(BTreeSet::from(["remote".to_string()]));
    let make = |property: &str| {
        Plan::new(vec![
            Op::FuseRrf {
                branches: vec![
                    vec![Op::Scan {
                        label: "Incident".into(),
                    }],
                    vec![Op::Foreign {
                        name: "remote".into(),
                    }],
                ],
                k: 60.0,
            },
            Op::Filter {
                preds: vec![Pred::Eq {
                    prop: property.into(),
                    value: "1".into(),
                }],
            },
        ])
    };
    assert!(matches!(
        context.ground(&make("invented")),
        Err(SchemaViolation::UnknownProperty { .. })
    ));
    assert!(context.ground(&make("priority")).is_ok());
    // Nested mixed branches retain the same local constituent.
    let mut nested = make("invented");
    let stage = nested.ops.remove(0);
    nested.ops.insert(
        0,
        Op::FuseRrf {
            branches: vec![
                vec![stage],
                vec![Op::Foreign {
                    name: "remote".into(),
                }],
            ],
            k: 60.0,
        },
    );
    assert!(matches!(
        context.ground(&nested),
        Err(SchemaViolation::UnknownProperty { .. })
    ));
}

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[cfg(feature = "federation")]
#[test]
fn inline_foreign_adapter_cannot_bypass_the_declared_source_catalog() {
    use medulla_types::wire::{ForeignSourceSpec, HttpFieldMap};
    let mut context = SchemaContext::from_view(&servicenow_view());
    context.declare_foreign_sources(BTreeSet::from(["remote".to_string()]));
    let plan = Plan::new(vec![Op::ForeignScan {
        source: Box::new(ForeignSourceSpec::HttpJson {
            url: "https://example.invalid/data".into(),
            json_path: String::new(),
            field_map: HttpFieldMap {
                id: "id".into(),
                score: None,
                columns: Default::default(),
            },
        }),
        join: false,
    }]);
    let violation = context.ground(&plan).unwrap_err();
    assert_eq!(violation.code(), "NL_SCHEMA_UNBOUND_FOREIGN_SOURCE");
    assert!(violation.to_string().contains("owner-scoped"));
    assert!(matches!(violation, SchemaViolation::UnboundForeignSource));
}

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[cfg(any(feature = "owl", feature = "geo", feature = "tensor"))]
#[test]
fn modal_source_stages_replace_incoming_node_scope_and_validate_layers() {
    let context = SchemaContext::from_view(&servicenow_view());
    let mut sources = Vec::new();
    #[cfg(feature = "owl")]
    {
        sources.push(Op::SparqlBgp {
            query: "SELECT ?s WHERE { ?s ?p ?o }".into(),
            var: "s".into(),
        });
        sources.push(Op::Reason {
            target_class: "InferredClass".into(),
            ontology: String::new(),
        });
    }
    #[cfg(feature = "geo")]
    sources.push(Op::SpatialScan {
        layer: "ConfigurationItem".into(),
        bbox: [-180.0, -90.0, 180.0, 90.0],
    });
    #[cfg(feature = "tensor")]
    sources.push(Op::TensorScan {
        layer: "ConfigurationItem".into(),
    });
    for source in sources {
        for (property, accepted) in [("owned_by", true), ("invented", false)] {
            let plan = Plan::new(vec![
                Op::Scan {
                    label: "Incident".into(),
                },
                source.clone(),
                Op::Filter {
                    preds: vec![Pred::Eq {
                        prop: property.into(),
                        value: "alice".into(),
                    }],
                },
            ]);
            assert_eq!(
                context.ground(&plan).is_ok(),
                accepted,
                "{source:?} / {property}"
            );
        }
    }
    let mut missing_layers = Vec::new();
    #[cfg(feature = "geo")]
    missing_layers.push(Op::SpatialScan {
        layer: "InventedLayer".into(),
        bbox: [0.0; 4],
    });
    #[cfg(feature = "tensor")]
    missing_layers.push(Op::TensorScan {
        layer: "InventedLayer".into(),
    });
    for source in missing_layers {
        assert!(matches!(
            context.ground(&Plan::new(vec![source])),
            Err(SchemaViolation::UnknownLabel { .. })
        ));
    }
}

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[cfg(feature = "federation")]
#[test]
fn foreign_id_filters_rematerialize_local_properties_before_execution() {
    let view = servicenow_view();
    let mut context = SchemaContext::from_view(&view);
    context.declare_foreign_sources(BTreeSet::from(["remote".into()]));
    let mut registry = crate::federation::ForeignSourceRegistry::new();
    registry.register_table(
        "remote",
        vec![("inc_a".into(), None), ("foreign-only".into(), None)],
    );
    let semantic = SemanticStore::new();
    let ctx = PlanCtx::new(&view, &semantic).with_foreign(&registry);
    let plan = context
        .ground(&crate::uql::parse("FOREIGN 'remote' |> WHERE priority = '1'").unwrap())
        .unwrap();
    assert_eq!(
        execute(&plan.plan, &ctx).unwrap().ids(),
        vec!["inc_a".to_string()]
    );
    assert!(matches!(
        context.ground(&crate::uql::parse("FOREIGN 'remote' |> WHERE remote_only = 1").unwrap()),
        Err(SchemaViolation::UnknownProperty { .. })
    ));
}

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[cfg(feature = "numeric")]
#[test]
fn grounded_attribution_runs_numeric_input_and_owen_group_properties() {
    let core = GraphCore::new();
    core.add_node(
        "a".into(),
        blob(json!({"type": "Player", "amount": 2.0, "team": "red"})),
    );
    core.add_node(
        "b".into(),
        blob(json!({"type": "Player", "amount": 3.0, "team": "blue"})),
    );
    let view = core.analysis_snapshot();
    let context = SchemaContext::from_view(&view);
    let semantic = SemanticStore::new();
    let ctx = PlanCtx::new(&view, &semantic);
    for method in ["SHAPLEY", "OWEN BY Team"] {
        let text = format!("MATCH (:Player) |> ATTRIBUTE SUM OF Amount {method}");
        let grounded = context.ground(&crate::uql::parse(&text).unwrap()).unwrap();
        let rows = execute(&grounded.plan, &ctx).unwrap();
        assert_eq!(rows.id_set(), std::collections::HashSet::from(["a", "b"]));
        assert!(
            (rows
                .rows()
                .iter()
                .map(|r| r.score.unwrap() as f64)
                .sum::<f64>()
                - 5.0)
                .abs()
                < 1e-5
        );
    }
}

// spec: MEDULLA-FEDERATED-QUERY-R058.4.7.1
#[test]
fn a_native_record_source_cannot_retain_the_previous_graph_label_scope() {
    let context = SchemaContext::from_view(&servicenow_view());
    let plan = |property: &str| {
        Plan::new(vec![
            Op::Scan {
                label: "Incident".into(),
            },
            Op::DecisionScan { preds: vec![] },
            Op::Filter {
                preds: vec![Pred::Eq {
                    prop: property.into(),
                    value: "alice".into(),
                }],
            },
        ])
    };
    assert!(context.ground(&plan("owned_by")).is_ok());
    assert!(matches!(
        context.ground(&plan("invented")),
        Err(SchemaViolation::UnknownProperty { .. })
    ));
}
