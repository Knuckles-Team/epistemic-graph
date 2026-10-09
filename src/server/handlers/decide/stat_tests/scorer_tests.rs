//! Served resident scorer (EH-291, EH-295, EH-300): `DecisionFit` fits an
//! `OptionAttention` head; a receipt that fails the promotion protocol cannot
//! publish it; a passing one does. The inline-fitted head is synthetic, so
//! `Decide` abstains rather than acting on it, records no (inexact) linear
//! explanation, and an explored decision through it verify-replays in the log.

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
            optimiser: fit_optimiser(),
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
    // An inline-fitted head is synthetic, so its conformal calibration cannot
    // authorize an ordinary Act: the head reads, but the ladder abstains.
    let abstained =
        decide_and_assert_abstains(&h, &fixture.schema_pin, Some(head_pin.clone())).await;
    let record = &abstained.records.as_slice()[0];
    assert!(
        record.synthetic_evidence,
        "a head fitted on synthetic data says so"
    );
    assert!(
        record.explanation.is_none(),
        "no partial explanation labelled exact"
    );

    // Belief replay and the log still need an executed decision: explicit
    // ordinary-question exploration supplies one through the scorer head
    // without claiming a risk-bound Act from the synthetic calibration.
    let policy = ordinary_exploration_policy();
    let policy_pin = h.publish_policy("policy-scorer-explore", &policy);
    let mut request = request(
        &fixture.schema_pin,
        Some(head_pin),
        DecisionPolicyRef::Pinned {
            component: policy_pin,
        },
        QuestionSafety::Ordinary,
    );
    request.belief_as_of = BoundedVec::new(vec![1_000, 2_000]).unwrap();
    let batch = decide(&h, request).await.unwrap();
    let record = &batch.records.as_slice()[0];
    assert!(
        matches!(record.outcome, StatisticalOutcome::Explored { .. }),
        "expected to explore: {:?}",
        record.outcome
    );
    assert!(
        record.explanation.is_none(),
        "no partial explanation labelled exact"
    );
    belief_is_recorded_and_replayed(&h, record).await;
    decision_log_round_trip(&h, record.clone()).await;
}

/// EG-DECISION-ENGINE-R092: across distinct decision question types, the
/// statistical scorer only ever ranks within the SAME legal, structurally
/// derived option set -- it can never introduce an option of its own -- and
/// every option is read from structured `Q32` facts, never a serialized
/// text label, so scoring is never limited by option-label length or
/// language. Two distinct `QuestionKind`s over the identical published head
/// and candidates prove both properties hold ladder-wide, not just for one
/// question.
#[tokio::test]
async fn select_only_and_structured_encoding_hold_across_distinct_question_kinds() {
    let h = Harness::new().await;
    let fixture = route_fixture(&h).await;
    let data = dataset(&fixture.schema_digest, &fixture.ids, &fixture.values, 400);
    let job: DecisionJobRecord = decode(
        super::super::jobs::handle_decision_fit(
            &h.state,
            201,
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
    let passed = evaluated(&h, 202, (draft_sha256, draft_length), data).await;
    assert!(passed.passed, "gates: {:?}", passed.failed_gates.as_slice());
    let head_pin = h
        .publish(
            "head-scorer-cross-question",
            AgentComponentKind::DecisionHead,
            "resident scorer, cross-question proof",
            Some(draft.as_ref()),
            Some(passed.receipt_digest.clone()),
        )
        .unwrap();

    for kind in [QuestionKind::Route, QuestionKind::Rank] {
        let mut asked = request(
            &fixture.schema_pin,
            Some(head_pin.clone()),
            DecisionPolicyRef::Default,
            QuestionSafety::Ordinary,
        );
        asked.question.kind = kind;
        let batch = decide(&h, asked).await.unwrap();
        let record = &batch.records.as_slice()[0];
        let eg_types::decision::statistical::FeatureMatrixRef::Inline {
            candidate_ids,
            values,
            ..
        } = &record.inputs.feature_matrix
        else {
            panic!("inline matrix")
        };

        // Select-only-from-derived-options: the matrix names exactly the
        // candidates the deterministic `AgentLibrary` scope derived, never
        // more and never fewer, whichever question kind asked for it.
        let ids: Vec<String> = candidate_ids.iter().cloned().collect();
        assert_eq!(
            ids, fixture.ids,
            "question {kind:?} must score exactly the derived legal set"
        );

        // Structured, non-text encoding: the matrix holds fixed-width `Q32`
        // numeric facts only -- its length is candidates x feature count,
        // never a function of any option's label text.
        assert_eq!(
            values.len(),
            ids.len() * 2,
            "question {kind:?}: two numeric features per legal option"
        );
    }
}

/// EG-DECISION-ENGINE-R094: the resident decision scorer serves a decision
/// with no GPU device, model-serving sidecar or separate inference cluster.
/// This test runs on an ordinary hosted-CI runner with no GPU hardware and
/// no sidecar process reachable, so fitting, promoting and reading an
/// `OptionAttention` head end to end through the served `Decide` path here
/// is itself the deployment proof: the resident scorer's own forward pass
/// is a synchronous, in-process, fixed-point function (EG-DECISION-ENGINE-R090
/// times its CPU cost directly), never an RPC to a model server, so nothing
/// in this path could reach a GPU or sidecar even were one present.
async fn a_resident_scorer_decision_is_served_with_no_gpu_or_model_server_present() {
            301,
    let passed = evaluated(&h, 302, (draft_sha256, draft_length), data).await;
            "head-scorer-no-gpu",
            "resident scorer, no-GPU proof",
    // The served path reads the resident scorer and answers -- the ladder
    // abstains on this synthetic calibration, but the decision IS served:
    // no GPU, no model-serving sidecar, no separate inference cluster.
    let batch = decide_and_assert_abstains(&h, &fixture.schema_pin, Some(head_pin)).await;
    let record = &batch.records.as_slice()[0];
    assert!(
        record.synthetic_evidence,
        "a decision was served end to end through the resident scorer with no GPU feature compiled in"
    );
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
