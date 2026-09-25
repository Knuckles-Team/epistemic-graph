//! Served resident scorer (EH-291, EH-295, EH-300): `DecisionFit` fits an
//! `OptionAttention` head; a receipt that fails the promotion protocol cannot
//! publish it; a passing one does; `Decide` then acts through it, records no
//! (inexact) linear explanation, and the record verify-replays in the log.

use super::*;

fn scorer_fit(fixture: &RouteFixture, data: &LabelledDataset) -> DecisionFitOp {
    DecisionFitOp::Submit {
        request: Box::new(DecisionFitRequest {
            tenant_id: TENANT.to_string(),
            idempotency_key: "fit-scorer".to_string(),
            head_kind: HeadKind::OptionAttention,
            feature_schema: fixture.schema_pin.clone(),
            policy: DecisionPolicyRef::Default,
            label_regime: LabelRegime::FullLabel {
                gold_set_digest: super::super::stat_jobs::dataset_digest(data).unwrap(),
            },
            window: window(),
            optimiser: OptimiserSpec {
                max_iterations: 60,
                tolerance: QuantisedValue {
                    scale: QuantScaleTag::Q32,
                    value: 1 << 12,
                },
                seed: 0,
            },
            source: DatasetSource::Inline {
                dataset: Box::new(data.clone()),
            },
        }),
    }
}

async fn evaluated(
    h: &Harness,
    request_id: u64,
    (sha256, length): (String, u64),
    data: LabelledDataset,
) -> eg_types::decision::jobs::DecisionEvalReceipt {
    let eval = DecisionEvalRequest {
        tenant_id: TENANT.to_string(),
        idempotency_key: format!("eval-scorer-{request_id}"),
        candidate: EvalCandidate::DraftArtifact { sha256, length },
        policy: DecisionPolicyRef::Default,
        estimators: BoundedVec::default(),
        gold_set_digest: Some(super::super::stat_jobs::dataset_digest(&data).unwrap()),
        window: window(),
        source: DatasetSource::Inline {
            dataset: Box::new(data),
        },
        mode: eg_types::decision::EvalMode::OffPolicy,
    };
    let op = DecisionEvalOp::Submit {
        request: Box::new(eval),
    };
    let job: DecisionJobRecord = decode(
        super::super::jobs::handle_decision_eval(&h.state, request_id, &verified(), op).await,
    )
    .unwrap();
    let DecisionJobOutput::Eval { receipt } = succeeded(&job).clone() else {
        panic!("eval output")
    };
    *receipt
}

/// The fixture's gold set with every label moved to the last option.
fn mislabelled(data: &LabelledDataset, ids: &[String]) -> LabelledDataset {
    let mut wrong = data.clone();
    let items = wrong
        .items
        .iter()
        .cloned()
        .map(|mut item| {
            item.label = ItemLabel::Gold {
                acceptable: BoundedVec::new(vec![ids[2].clone()]).unwrap(),
                source: LabelSource::SyntheticConstruction,
            };
            item
        })
        .collect();
    wrong.items = BoundedVec::new(items).unwrap();
    wrong
}

#[tokio::test]
async fn a_scorer_head_is_promoted_only_through_the_protocol_and_then_acts() {
    let h = Harness::new().await;
    let fixture = route_fixture(&h).await;
    let data = dataset(&fixture.schema_digest, &fixture.ids, &fixture.values, 400);
    let job: DecisionJobRecord = decode(
        super::super::jobs::handle_decision_fit(
            &h.state,
            2,
            &verified(),
            scorer_fit(&fixture, &data),
        )
        .await,
    )
    .unwrap();
    let DecisionJobOutput::Fit {
        draft,
        draft_sha256,
        draft_length,
        ..
    } = succeeded(&job).clone()
    else {
        panic!("fit output")
    };
    assert_eq!(draft.kind, HeadKind::OptionAttention);
    assert!(draft.scorer.is_some() && draft.calibration.is_some());
    let pin = (draft_sha256, draft_length);

    let failed = evaluated(&h, 3, pin.clone(), mislabelled(&data, &fixture.ids)).await;
    assert!(
        !failed.passed,
        "a mislabelled frozen set fails the protocol"
    );
    assert!(failed.promotion.is_some());
    let refused = h.publish(
        "head-scorer",
        AgentComponentKind::DecisionHead,
        "resident scorer",
        Some(draft.as_ref()),
        Some(failed.receipt_digest.clone()),
    );
    assert!(refused
        .unwrap_err()
        .starts_with("EVALUATION_RECEIPT_MISMATCH"));

    let passed = evaluated(&h, 4, pin, data).await;
    assert!(passed.passed, "gates: {:?}", passed.failed_gates.as_slice());
    let head_pin = h
        .publish(
            "head-scorer",
            AgentComponentKind::DecisionHead,
            "resident scorer",
            Some(draft.as_ref()),
            Some(passed.receipt_digest.clone()),
        )
        .unwrap();

    belief_slices_are_refused_without_a_head_or_in_the_future(&h, &fixture).await;
    let mut request = request(
        &fixture.schema_pin,
        Some(head_pin),
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    request.belief_as_of = BoundedVec::new(vec![1_000, 2_000]).unwrap();
    let batch = decide(&h, request).await.unwrap();
    let record = &batch.records.as_slice()[0];
    let StatisticalOutcome::Acted { option_id, .. } = &record.outcome else {
        panic!("expected to act: {:?}", record.outcome)
    };
    assert_eq!(option_id, "tool-a-search");
    assert!(
        record.explanation.is_none(),
        "no partial explanation labelled exact"
    );
    assert_eq!(
        record.calibration.map(|c| c.method),
        Some(eg_types::decision::statistical::CalibrationMethod::Conformal)
    );
    belief_is_recorded_and_replayed(&h, record).await;
    decision_log_round_trip(&h, record.clone()).await;
}

/// EH-297 refusals: a belief needs a head, and no slice is after the clock.
async fn belief_slices_are_refused_without_a_head_or_in_the_future(
    h: &Harness,
    fixture: &RouteFixture,
) {
    let mut headless = request(
        &fixture.schema_pin,
        None,
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    headless.belief_as_of = BoundedVec::new(vec![1_000]).unwrap();
    let refused = decide(h, headless.clone()).await.unwrap_err();
    assert!(refused.starts_with("PARAMETER_INVALID"), "{refused}");
    headless.belief_as_of = BoundedVec::new(vec![2_000, 1_000]).unwrap();
    let refused = decide(h, headless).await.unwrap_err();
    assert!(refused.starts_with("PARAMETER_INVALID"), "{refused}");
}

/// EH-297: one belief point per slice over the same options, each slice's
/// matrix stored; a record whose belief was altered fails verify-replay.
async fn belief_is_recorded_and_replayed(h: &Harness, record: &StatisticalDecisionRecord) {
    let times: Vec<u64> = record.belief.iter().map(|p| p.as_of_ms).collect();
    assert_eq!(times, vec![1_000, 2_000]);
    assert_eq!(record.inputs.belief_slices.len(), 2);
    for point in &record.belief {
        let p = point
            .probabilities
            .as_ref()
            .expect("the head reads every slice");
        assert_eq!(p.len(), 3, "the same three options, no more");
    }
    let mut forged = record.clone();
    forged.belief = BoundedVec::new(vec![forged.belief.as_slice()[1].clone()]).unwrap();
    forged.record_digest = eg_types::decision::digest::statistical_record_digest(&forged);
    let op = DecisionLogOp::Commit {
        record: Box::new(forged),
        evaluator: None,
    };
    let refused = decode::<DecisionLogCommitted>(super::log_tests::log_op(h, "decider", op).await);
    assert!(refused.unwrap_err().starts_with("DECISION_REPLAY_MISMATCH"));
}
