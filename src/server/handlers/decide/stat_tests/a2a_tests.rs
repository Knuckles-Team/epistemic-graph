//! EG-DECISION-ENGINE-R044: an inbound A2A task is routed to an existing or
//! assembled agent graph by applying the same typed decision ladder used for
//! every other question family. `resolution_kind` and `evidence_class` are
//! computed generically from the outcome and premises (see
//! `stat_decide::resolution`/`evidence_class`), never from `question.kind`,
//! so this question kind needs no executor branch of its own.

use super::*;

#[tokio::test]
async fn a2a_task_question_is_routed_through_the_shared_decision_ladder() {
    let h = Harness::new().await;
    let (schema_pin, _digest) = publish_route_schema(&h);

    let mut route_request = request(
        &schema_pin,
        None,
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    route_request.question.question_id = "route.compare".to_string();

    let mut a2a_request = request(
        &schema_pin,
        None,
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    a2a_request.question.question_id = "a2a.route-task".to_string();
    a2a_request.question.kind = QuestionKind::A2aTask;

    let route_batch = decide(&h, route_request).await.unwrap();
    let a2a_batch = decide(&h, a2a_request).await.unwrap();

    let route_record = &route_batch.records.as_slice()[0];
    let a2a_record = &a2a_batch.records.as_slice()[0];

    // The recorded question kind round-trips: the caller's routing question
    // is preserved on the sealed decision record.
    assert_eq!(a2a_record.question.kind, QuestionKind::A2aTask);

    // Resolution kind and evidence class come from the shared ladder, not
    // from the question kind: an A2A routing question and an otherwise
    // identical `Route` question over the same candidates, schema and
    // policy are decided the same way.
    assert_eq!(a2a_record.resolution_kind, route_record.resolution_kind);
    assert_eq!(a2a_record.evidence_class, route_record.evidence_class);
}
