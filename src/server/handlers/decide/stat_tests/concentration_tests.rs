//! EG-DECISION-ENGINE-R173.3/.5: a served `EditConcentration` question and
//! the composed `ShaclValidate` shape judge the same edit the same way, and
//! the sealed record lists what the verdict counted.

use super::*;
use eg_types::decision::{PremiseRef, ResolutionKind};

const GRAPH: &str = "kg-concentration";
const KG: &str = "http://knuckles.team/kg#";
const EX: &str = "http://example.org/concentration/";
const CC: &str = "http://knuckles.team/kg#EditConcentrationConstraintComponent";

/// Policy: three shipped edits on one dimension inside three rounds revert.
/// D4 holds shipped edits in rounds 2 and 3 and a proposed third in round 4;
/// D2 holds one shipped edit and a proposed second.
const DATA: &str = r#"@prefix kg: <http://knuckles.team/kg#> .
@prefix ex: <http://example.org/concentration/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:policy a kg:ConcentrationPolicy ;
    kg:minShipped "3"^^xsd:integer ;
    kg:windowRounds "3"^^xsd:integer ;
    kg:concentrationRevision "1"^^xsd:integer .

ex:e2 a kg:HarnessEdit ; kg:governedByConcentrationPolicy ex:policy ;
    kg:targetsDimension kg:D4 ; kg:editRound "2"^^xsd:integer ;
    kg:editStatus "shipped" ; kg:judgedUnderRevision "1"^^xsd:integer .
ex:e3 a kg:HarnessEdit ; kg:governedByConcentrationPolicy ex:policy ;
    kg:targetsDimension kg:D4 ; kg:editRound "3"^^xsd:integer ;
    kg:editStatus "shipped" ; kg:judgedUnderRevision "1"^^xsd:integer .
ex:e4 a kg:HarnessEdit ; kg:governedByConcentrationPolicy ex:policy ;
    kg:targetsDimension kg:D4 ; kg:editRound "4"^^xsd:integer ;
    kg:editStatus "proposed" ; kg:judgedUnderRevision "1"^^xsd:integer .
ex:k3 a kg:HarnessEdit ; kg:governedByConcentrationPolicy ex:policy ;
    kg:targetsDimension kg:D2 ; kg:editRound "3"^^xsd:integer ;
    kg:editStatus "shipped" ; kg:judgedUnderRevision "1"^^xsd:integer .
ex:k4 a kg:HarnessEdit ; kg:governedByConcentrationPolicy ex:policy ;
    kg:targetsDimension kg:D2 ; kg:editRound "4"^^xsd:integer ;
    kg:editStatus "proposed" ; kg:judgedUnderRevision "1"^^xsd:integer .
"#;

async fn harness_with_edits(data: &str) -> Harness {
    let h = Harness::with_isolation(ServerState::test_isolation("decider")).await;
    let mut guard = h.state.write().await;
    guard
        .registry
        .create_graph(GRAPH, crate::protocol::GraphType::Team, None)
        .unwrap();
    let core = guard.registry.get(GRAPH).unwrap().core.clone();
    let mut iris = eg_rdf::mapping::IriStore::default();
    eg_rdf::mapping::load_triples(
        &core,
        &mut iris,
        GRAPH,
        eg_rdf::mapping::parse_turtle(data).unwrap(),
    )
    .unwrap();
    drop(guard);
    h
}

fn concentration_request(schema: &ComponentDependency, edit: &str) -> DecideRequest {
    DecideRequest {
        tenant_id: TENANT.to_string(),
        question: StatisticalQuestion {
            question_id: "harness.edit-concentration".to_string(),
            kind: QuestionKind::EditConcentration,
            safety: QuestionSafety::Ordinary,
        },
        candidates: CandidateSource::Declared {
            options: BoundedVec::default(),
        },
        feature_schema: schema.clone(),
        head: None,
        policy: DecisionPolicyRef::Default,
        params: BoundedVec::new(vec![
            TypedParam {
                name: "candidate_edit".to_string(),
                value: TypedValue::Iri(format!("{EX}{edit}")),
            },
            TypedParam {
                name: "graph".to_string(),
                value: TypedValue::Text(GRAPH.to_string()),
            },
        ])
        .unwrap(),
        max_records: None,
        belief_as_of: BoundedVec::default(),
    }
}

fn action(record: &StatisticalDecisionRecord) -> &str {
    let StatisticalOutcome::Advisory { scores, calibrated } = &record.outcome else {
        panic!("expected an advisory verdict: {:?}", record.outcome)
    };
    assert!(!calibrated);
    assert_eq!(scores.len(), 1);
    &scores.as_slice()[0].option_id
}

/// The edits the served composed shapes flag through the concentration
/// component, by focus node.
async fn shacl_reverted(h: &Harness) -> Vec<String> {
    let core = h
        .state
        .read()
        .await
        .registry
        .get(GRAPH)
        .unwrap()
        .core
        .clone();
    let response = crate::server::handlers::rdf::validation::handle_shacl_validate(
        7,
        GRAPH,
        &core,
        None,
        crate::server::handlers::rdf::validation::ShaclData {
            turtle: "",
            triples: None,
        },
    )
    .await;
    assert!(response.error.is_none(), "{:?}", response.error);
    let report: eg_types::rdf_report::ShaclValidationReport = match response.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
        Some(ResultPayload::Json(value)) => serde_json::from_value(value).unwrap(),
        other => panic!("unexpected payload {other:?}"),
    };
    let mut flagged: Vec<String> = report
        .results
        .into_iter()
        .filter(|result| result.constraint_component == CC)
        .map(|result| result.focus_node)
        .collect();
    flagged.sort();
    flagged
}

// spec: EG-DECISION-ENGINE-R173.3
#[tokio::test]
async fn decide_and_shacl_validate_give_the_same_concentration_verdict() {
    let h = harness_with_edits(DATA).await;
    let (schema_pin, _digest) = publish_route_schema(&h);

    let mut verdicts = Vec::new();
    for edit in ["e2", "e3", "e4", "k3", "k4"] {
        let batch = decide(&h, concentration_request(&schema_pin, edit))
            .await
            .unwrap();
        let record = &batch.records.as_slice()[0];
        assert_eq!(record.question.kind, QuestionKind::EditConcentration);
        assert_eq!(record.resolution_kind, ResolutionKind::Constraint);
        verdicts.push((edit, action(record).to_string()));
    }
    assert_eq!(
        verdicts,
        vec![
            ("e2", "keep".to_string()),
            ("e3", "keep".to_string()),
            ("e4", "revert".to_string()),
            ("k3", "keep".to_string()),
            ("k4", "keep".to_string()),
        ],
        "the third same-dimension edit inside the window reverts; two edits keep"
    );

    let decided_revert: Vec<String> = verdicts
        .iter()
        .filter(|(_, action)| action == "revert")
        .map(|(edit, _)| format!("<{EX}{edit}>"))
        .collect();
    assert_eq!(
        shacl_reverted(&h).await,
        decided_revert,
        "ShaclValidate over the composed shapes flags exactly the edits Decide reverts"
    );
}

#[tokio::test]
async fn a_missing_policy_abstains_on_both_paths() {
    let without_policy = DATA.replace("kg:minShipped \"3\"^^xsd:integer ;", "");
    let h = harness_with_edits(&without_policy).await;
    let (schema_pin, _digest) = publish_route_schema(&h);
    let batch = decide(&h, concentration_request(&schema_pin, "e4"))
        .await
        .unwrap();
    let record = &batch.records.as_slice()[0];
    assert_eq!(
        record.outcome,
        StatisticalOutcome::Abstained {
            reasons: BoundedVec::new(vec![
                eg_types::decision::AbstainReason::NoConcentrationPolicy
            ])
            .unwrap()
        }
    );
    assert_eq!(record.resolution_kind, ResolutionKind::Abstention);
    assert_eq!(
        shacl_reverted(&h).await.len(),
        5,
        "every governed edit fails closed"
    );
}

fn premise<'a>(record: &'a StatisticalDecisionRecord, subject: &str) -> Vec<&'a PremiseRef> {
    record
        .premises
        .iter()
        .filter(|premise| premise.subject == subject)
        .collect()
}

// spec: EG-DECISION-ENGINE-R173.5
#[tokio::test]
async fn the_record_lists_counted_edits_window_threshold_and_revision() {
    let h = harness_with_edits(DATA).await;
    let (schema_pin, _digest) = publish_route_schema(&h);
    let batch = decide(&h, concentration_request(&schema_pin, "e4"))
        .await
        .unwrap();
    // Read the sealed record back off the wire form.
    let wire = rmp_serde::to_vec_named(&batch).unwrap();
    let read: DecisionBatch = rmp_serde::from_slice(&wire).unwrap();
    let record = &read.records.as_slice()[0];
    assert_eq!(action(record), "revert");

    let policy = premise(record, &format!("{EX}policy"));
    assert_eq!(policy.len(), 1);
    assert_eq!(
        policy[0].fact,
        "concentration_policy min_shipped=3 revision=1"
    );

    let window = premise(record, &format!("{KG}D4"));
    assert_eq!(window.len(), 1);
    assert_eq!(
        window[0].fact,
        "concentration_window first_round=2 last_round=4 count=3"
    );

    let counted: Vec<&str> = record
        .premises
        .iter()
        .filter(|premise| premise.fact == "counted_edit")
        .map(|premise| premise.subject.as_str())
        .collect();
    assert_eq!(
        counted,
        vec![format!("{EX}e2"), format!("{EX}e3"), format!("{EX}e4")]
    );
    assert!(
        premise(record, &format!("{EX}k3")).is_empty(),
        "an edit on another dimension is not counted"
    );
}
