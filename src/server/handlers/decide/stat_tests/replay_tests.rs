//! Served walk-forward replay evaluation (EH-528): `DecisionEval` in replay
//! mode fits, replays and seals an `EvaluationRun`, and refuses what replay
//! cannot answer honestly.

use super::*;
use crate::server::persistence::decision_jobs::evaluation_run_key;
use eg_types::decision::replay::{
    AllocationRule, EvalMode, EvaluationRun, RefitSpec, ReplayEnvironment, ReplaySpec, SharedCap,
    TrialLog, WalkForward,
};

const Q32_ONE: i64 = 1 << 32;

/// A fitted draft plus the gold set it was fitted on (items one ms apart).
struct Fitted {
    data: LabelledDataset,
    gold: String,
    draft: EvalCandidate,
}

fn optimiser() -> OptimiserSpec {
    OptimiserSpec {
        max_iterations: 30,
        tolerance: QuantisedValue {
            scale: QuantScaleTag::Q32,
            value: 1 << 12,
        },
        seed: 0,
    }
}

async fn fitted(h: &Harness) -> Fitted {
    for (id, summary) in [
        ("tool-a-search", "web search engine for pages"),
        ("tool-b-files", "write files to disk"),
        ("tool-c-mail", "send an email message"),
    ] {
        h.publish(id, AgentComponentKind::Tool, summary, None::<&()>, None)
            .unwrap();
    }
    let body = schema();
    let schema_pin = h
        .publish(
            "schema-route",
            AgentComponentKind::FeatureSchema,
            "route features",
            Some(&body),
            None,
        )
        .unwrap();
    let schema_digest = encode_body(&body).unwrap().content_digest;
    let batch = decide(
        h,
        request(
            &schema_pin,
            None,
            DecisionPolicyRef::Default,
            QuestionSafety::Ordinary,
        ),
    )
    .await
    .unwrap();
    let FeatureMatrixRef::Inline {
        candidate_ids,
        values,
        ..
    } = &batch.records.as_slice()[0].inputs.feature_matrix
    else {
        panic!("inline matrix")
    };
    let ids: Vec<String> = candidate_ids.iter().cloned().collect();
    let mut data = dataset(&schema_digest, &ids, values.as_slice(), 240);
    let mut items: Vec<LabelledItem> = data.items.iter().cloned().collect();
    for (i, item) in items.iter_mut().enumerate() {
        item.recorded_at_ms = 10_000 + i as u64;
        // `dataset` jitters every candidate row alike, which a linear logit's
        // softmax cannot see, so a confident head earns the same utility on
        // every item. Label noise -- one item in five accepts the second
        // option -- makes the replayed utility path vary, as real gold sets do.
        if i % 5 == 0 {
            item.label = ItemLabel::Gold {
                acceptable: BoundedVec::new(vec![ids[1].clone()]).unwrap(),
                source: LabelSource::SyntheticConstruction,
            };
        }
    }
    data.items = BoundedVec::new(items).unwrap();
    let gold = super::super::stat_jobs::dataset_digest(&data).unwrap();
    let fit = DecisionFitRequest {
        tenant_id: TENANT.to_string(),
        idempotency_key: "replay-fit".to_string(),
        head_kind: HeadKind::ListwiseLogistic,
        feature_schema: schema_pin,
        policy: DecisionPolicyRef::Default,
        label_regime: LabelRegime::FullLabel {
            gold_set_digest: gold.clone(),
        },
        window: window(),
        optimiser: optimiser(),
        source: DatasetSource::Inline {
            dataset: Box::new(data.clone()),
        },
    };
    let op = DecisionFitOp::Submit {
        request: Box::new(fit),
    };
    let job: DecisionJobRecord =
        decode(super::super::jobs::handle_decision_fit(&h.state, 40, &verified(), op).await)
            .unwrap();
    let DecisionJobOutput::Fit {
        draft_sha256,
        draft_length,
        ..
    } = succeeded(&job).clone()
    else {
        panic!("fit output")
    };
    Fitted {
        data,
        gold,
        draft: EvalCandidate::DraftArtifact {
            sha256: draft_sha256,
            length: draft_length,
        },
    }
}

fn spec() -> ReplaySpec {
    ReplaySpec {
        folds: WalkForward {
            train: 100,
            test: 40,
            step: 40,
            purge: 5,
            embargo: 5,
        },
        budget: SharedCap {
            cap: QuantisedValue {
                scale: QuantScaleTag::Q32,
                value: Q32_ONE,
            },
            rule: AllocationRule::Proportional,
        },
        env: ReplayEnvironment::PolicyIndependent,
        refit: Some(RefitSpec {
            head_kind: HeadKind::ListwiseLogistic,
            optimiser: optimiser(),
        }),
        trials: TrialLog {
            declared: 3,
            searched: 3,
        },
        incumbent: None,
        supersedes: None,
    }
}

fn replay_request(fitted: &Fitted, key: &str, spec: ReplaySpec) -> DecisionEvalRequest {
    DecisionEvalRequest {
        tenant_id: TENANT.to_string(),
        idempotency_key: key.to_string(),
        candidate: fitted.draft.clone(),
        policy: DecisionPolicyRef::Default,
        estimators: BoundedVec::new(Vec::new()).unwrap(),
        gold_set_digest: Some(fitted.gold.clone()),
        window: window(),
        source: DatasetSource::Inline {
            dataset: Box::new(fitted.data.clone()),
        },
        mode: EvalMode::Replay {
            spec: Box::new(spec),
        },
    }
}

async fn submit(h: &Harness, request: DecisionEvalRequest) -> DecisionJobRecord {
    let op = DecisionEvalOp::Submit {
        request: Box::new(request),
    };
    decode(super::super::jobs::handle_decision_eval(&h.state, 41, &verified(), op).await).unwrap()
}

fn sealed(job: &DecisionJobRecord) -> EvaluationRun {
    let DecisionJobOutput::Replay { run } = succeeded(job).clone() else {
        panic!("replay output")
    };
    *run
}

fn failed(job: &DecisionJobRecord) -> &str {
    match &job.state {
        DecisionJobState::Failed { code } => code,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn a_replay_is_sealed_stored_and_supersedable() {
    let h = Harness::new().await;
    let fitted = fitted(&h).await;
    let run = sealed(&submit(&h, replay_request(&fitted, "replay-1", spec())).await);

    // 240 items: tests start at 105 and step 40 -> 105..145, 145..185, 185..225.
    assert_eq!(run.folds.len(), 3);
    assert_eq!(run.path.len(), 120);
    assert!(run
        .folds
        .iter()
        .all(|fold| fold.first_test_ms < fold.last_test_ms));
    assert_eq!(run.folds.as_slice()[0].first_test_ms, 10_105);
    // Refits differ from the submitted draft (they see only their window).
    let EvalCandidate::DraftArtifact { sha256, .. } = &fitted.draft else {
        unreachable!()
    };
    assert_eq!(run.head_digest, *sha256);
    assert!(run.folds.iter().all(|fold| fold.head_digest != *sha256));
    assert_eq!(run.validation.n_trials, 3);
    // The utility is linear in the applied budget: contributions sum to the path.
    let path: i64 = run.path.iter().map(|u| u.value).sum();
    let credited: i64 = run.contributions.iter().map(|c| c.utility.value).sum();
    assert!((path - credited).abs() < 1 << 12, "{path} vs {credited}");
    // Each step's applied budget is capped at one.
    let applied: i64 = run.contributions.iter().map(|c| c.applied.value).sum();
    assert!(applied <= 120 * Q32_ONE + (1 << 12));

    // Content-addressed: the digest reproduces, and the row is stored under it.
    let mut unsealed = run.clone();
    unsealed.run_digest = String::new();
    assert_eq!(
        eg_types::decision::digest::digest_text("eg/decision/evaluation-run/v1", &unsealed),
        run.run_digest
    );
    assert!(run.verify());
    let mut tampered = run.clone();
    tampered.synthetic = !tampered.synthetic;
    assert!(!tampered.verify());
    assert!(h
        .store
        .decision_artifact(TENANT, &evaluation_run_key(&run.run_digest))
        .unwrap()
        .is_some());

    // A revision names the run it supersedes; an unknown one is refused.
    let mut revised = spec();
    revised.supersedes = Some(run.run_digest.clone());
    revised.trials.declared = 4;
    let next = sealed(&submit(&h, replay_request(&fitted, "replay-2", revised)).await);
    assert_eq!(
        next.spec.supersedes.as_deref(),
        Some(run.run_digest.as_str())
    );
    assert_ne!(next.run_digest, run.run_digest);
    let mut dangling = spec();
    dangling.supersedes = Some(format!("sha256:{}", "0".repeat(64)));
    let refused = submit(&h, replay_request(&fitted, "replay-3", dangling)).await;
    assert!(failed(&refused).starts_with("REPLAY_SPEC_INVALID"));

    // A row under the expected key is insufficient: supersedes must verify
    // the stored record's content address before accepting it as evidence.
    h.store
        .replace_decision_artifact(
            TENANT,
            &evaluation_run_key(&run.run_digest),
            crate::server::persistence::decision_jobs::encode_artifact(&tampered).unwrap(),
        )
        .unwrap();
    let mut corrupt_parent = spec();
    corrupt_parent.supersedes = Some(run.run_digest.clone());
    let refused = submit(
        &h,
        replay_request(&fitted, "replay-corrupt-parent", corrupt_parent),
    )
    .await;
    assert!(failed(&refused).starts_with("REPLAY_SPEC_INVALID"));
}

#[tokio::test]
async fn replay_refuses_what_it_cannot_answer() {
    let h = Harness::new().await;
    let fitted = fitted(&h).await;

    let mut understated = spec();
    understated.trials = TrialLog {
        declared: 1,
        searched: 20,
    };
    let job = submit(&h, replay_request(&fitted, "understated", understated)).await;
    assert!(failed(&job).starts_with("TRIALS_UNDERSTATED"));

    let mut dependent = spec();
    dependent.env = ReplayEnvironment::PolicyDependent;
    let job = submit(&h, replay_request(&fitted, "dependent", dependent)).await;
    assert!(failed(&job).starts_with("REPLAY_POLICY_DEPENDENT"));

    // No gold pin: the bandit regime, whose outcomes depend on the executed option.
    let mut bandit = replay_request(&fitted, "bandit", spec());
    bandit.gold_set_digest = None;
    let job = submit(&h, bandit).await;
    assert!(failed(&job).starts_with("REPLAY_POLICY_DEPENDENT"));

    // Mutation: items 95..=110 share one instant, so fold 0's last training
    // items (95..100) were recorded no earlier than its first test item (105):
    // same-instant data leaks into training.
    let mut future = fitted.data.clone();
    let mut items: Vec<LabelledItem> = future.items.iter().cloned().collect();
    for item in &mut items[95..=110] {
        item.recorded_at_ms = 10_105;
    }
    future.items = BoundedVec::new(items).unwrap();
    let gold = super::super::stat_jobs::dataset_digest(&future).unwrap();
    let mut look_ahead = replay_request(&fitted, "look-ahead", spec());
    look_ahead.source = DatasetSource::Inline {
        dataset: Box::new(future),
    };
    look_ahead.gold_set_digest = Some(gold);
    let job = submit(&h, look_ahead).await;
    assert!(failed(&job).starts_with("LOOK_AHEAD"), "{}", failed(&job));
}
