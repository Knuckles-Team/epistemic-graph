//! Served decision-log tests: commit, evaluate, aggregate, fitting from the
//! log, and retention (EH-060, EH-061, EH-062).

use super::*;

/// Engine-log fitting: a bandit fit reads its labels from the decision log
/// (committed record + independent evaluation), admitted only because the
/// pinned policy approves the committing principal.
pub(super) async fn fit_from_the_engine_log(
    h: &Harness,
    schema_pin: &ComponentDependency,
    record: &StatisticalDecisionRecord,
) {
    let mut policy = eg_types::decision::DecisionPolicy::engine_default();
    let mut statistical = super::super::stat_support::default_statistical_policy();
    statistical.approved_commit_principals =
        BoundedVec::new(vec![record.caller_principal.clone()]).unwrap();
    policy.statistical = Some(statistical);
    let policy_pin = h.publish_policy("policy-log-fit", &policy);
    let request = DecisionFitRequest {
        tenant_id: TENANT.to_string(),
        idempotency_key: "fit-from-log".to_string(),
        head_kind: HeadKind::WeightedFeatures,
        feature_schema: schema_pin.clone(),
        policy: DecisionPolicyRef::Pinned {
            component: policy_pin,
        },
        label_regime: LabelRegime::BanditLabel,
        window: window(),
        optimiser: OptimiserSpec {
            max_iterations: 20,
            tolerance: QuantisedValue {
                scale: QuantScaleTag::Q32,
                value: 1 << 12,
            },
            seed: 0,
        },
        source: DatasetSource::Logged {
            question_id: record.question.question_id.clone(),
        },
    };
    let op = DecisionFitOp::Submit {
        request: Box::new(request),
    };
    let job: DecisionJobRecord =
        decode(super::super::jobs::handle_decision_fit(&h.state, 12, &verified(), op).await)
            .unwrap();
    let DecisionJobOutput::Fit {
        draft, exclusions, ..
    } = succeeded(&job).clone()
    else {
        panic!("fit output")
    };
    assert_eq!(
        draft.n_training, 1,
        "the one logged, independently evaluated success trains"
    );
    assert_eq!(exclusions.unapproved_principal, 0);
}

pub(super) async fn log_op(h: &Harness, who: &str, op: DecisionLogOp) -> crate::protocol::Response {
    let verified = VerifiedRequestContext::verified_for_test_in_tenant(who, TENANT);
    super::super::log::handle_decision_log(&h.state, 9, &verified, op).await
}

/// DL-5/DL-5b: an acted-on record is logged only after verify-replay, only by
/// the principal that decided, and joins independent evaluations; the
/// aggregate reports nothing below `min_support`.
pub(super) async fn decision_log_round_trip(h: &Harness, record: StatisticalDecisionRecord) {
    let mut tampered = record.clone();
    tampered.logging_propensities = BoundedVec::default();
    tampered.record_digest = eg_types::decision::digest::statistical_record_digest(&tampered);
    let commit = |r: StatisticalDecisionRecord| DecisionLogOp::Commit {
        record: Box::new(r),
        evaluator: None,
    };
    let refused = decode::<DecisionLogCommitted>(log_op(h, "decider", commit(tampered)).await);
    assert!(refused.unwrap_err().starts_with("DECISION_REPLAY_MISMATCH"));
    let stranger =
        decode::<DecisionLogCommitted>(log_op(h, "stranger", commit(record.clone())).await);
    assert!(stranger.unwrap_err().starts_with("ACCESS_DENIED"));

    let committed: DecisionLogCommitted =
        decode(log_op(h, "decider", commit(record.clone())).await).unwrap();
    assert!(!committed.replayed);
    let again: DecisionLogCommitted =
        decode(log_op(h, "decider", commit(record.clone())).await).unwrap();
    assert!(again.replayed, "a repeat commit is an idempotent replay");

    let evaluation = DecisionOutcomeEvaluation {
        record_id: record.record_id.clone(),
        evaluation_id: "evaluation-1".to_string(),
        class: EvidenceClass::Observation,
        selected_agent: "agent-a".to_string(),
        lease_holder: "worker-a".to_string(),
        fidelity: OutcomeFidelity::ToolCalls,
        success: Some(true),
    };
    let op = DecisionLogOp::Evaluate {
        tenant_id: TENANT.to_string(),
        evaluation,
    };
    let stored: StoredEvaluation = decode(log_op(h, "evaluator", op).await).unwrap();
    assert_ne!(stored.producer, record.caller_principal);

    let get = DecisionLogOp::Get {
        tenant_id: TENANT.to_string(),
        record_id: record.record_id.clone(),
    };
    let entry: Option<DecisionLogEntry> = decode(log_op(h, "evaluator", get).await).unwrap();
    assert!(
        entry.is_some(),
        "a library-sourced record is tenant-visible"
    );

    let request = OutcomeAggregateRequest {
        tenant_id: TENANT.to_string(),
        question_id: None,
        window: window(),
    };
    let aggregate: OutcomeAggregate =
        decode(log_op(h, "evaluator", DecisionLogOp::Aggregate { request }).await).unwrap();
    assert!(
        aggregate.rows.is_empty(),
        "one evaluation is below min_support and is not reported"
    );
}

#[cfg(feature = "blob")]
fn retention_policy(
    compact_after_ms: u64,
    drop_blob_after_ms: Option<u64>,
) -> eg_types::decision::DecisionPolicy {
    let mut policy = eg_types::decision::DecisionPolicy::engine_default();
    let mut statistical = super::super::stat_support::default_statistical_policy();
    statistical.compact_after_ms = Some(compact_after_ms);
    statistical.drop_blob_after_ms = drop_blob_after_ms;
    policy.statistical = Some(statistical);
    policy
}

#[cfg(feature = "blob")]
async fn compact_with(
    h: &Harness,
    policy_id: &str,
    drop_after: Option<u64>,
) -> eg_types::decision::statistical::log::DecisionLogCompacted {
    let pin = h.publish_policy(policy_id, &retention_policy(0, drop_after));
    let op = DecisionLogOp::Compact {
        tenant_id: TENANT.to_string(),
        policy: DecisionPolicyRef::Pinned { component: pin },
        limit: 16,
    };
    decode(log_op(h, "decider", op).await).unwrap()
}

#[cfg(feature = "blob")]
async fn verify_log(
    h: &Harness,
    record_id: &str,
) -> Result<eg_types::decision::statistical::log::DecisionLogVerification, String> {
    let op = DecisionLogOp::Verify {
        tenant_id: TENANT.to_string(),
        record_id: record_id.to_string(),
    };
    decode(log_op(h, "decider", op).await)
}

#[cfg(feature = "blob")]
fn log_entry(h: &Harness, record_id: &str) -> DecisionLogEntry {
    let key = crate::server::persistence::decision_jobs::record_key(record_id);
    let bytes = h
        .store
        .decision_artifact(TENANT, &key)
        .unwrap()
        .expect("logged");
    crate::server::persistence::decision_jobs::decode_artifact(&bytes, "entry").unwrap()
}

#[cfg(feature = "blob")]
fn write_entry(h: &Harness, entry: &DecisionLogEntry) {
    let key = crate::server::persistence::decision_jobs::record_key(&entry.record.record_id);
    let bytes = crate::server::persistence::decision_jobs::encode_artifact(entry).unwrap();
    h.store
        .replace_decision_artifact(TENANT, &key, bytes)
        .unwrap();
}

/// A blob with valid CAS framing but other content: the record digest refuses it.
#[cfg(feature = "blob")]
fn tampered_blob(
    h: &Harness,
    entry: &DecisionLogEntry,
    original: &StatisticalDecisionRecord,
) -> eg_types::decision::statistical::log::InputsBlob {
    use sha2::{Digest, Sha256};
    let FeatureMatrixRef::Inline {
        candidate_ids,
        feature_names,
        scale,
        values,
    } = &original.inputs.feature_matrix
    else {
        panic!("inline original")
    };
    let mut forged = values.as_slice().to_vec();
    forged[0] += 1;
    let matrix = FeatureMatrixRef::Inline {
        candidate_ids: candidate_ids.clone(),
        feature_names: feature_names.clone(),
        scale: *scale,
        values: BoundedVec::new(forged).unwrap(),
    };
    let body = serde_json::to_vec(&matrix).unwrap();
    let sha256 = eg_types::contract::Digest256::from_bytes(Sha256::digest(&body).into());
    let blob = h.state.try_read().unwrap().blob.clone().unwrap();
    let stored = blob
        .store
        .put_engine_bodies(
            TENANT,
            &[crate::server::blob::engine_bodies::EngineBody { sha256, body }],
            1,
        )
        .unwrap();
    let stored = &stored[0];
    assert!(matches!(entry.inputs, EntryInputs::Compacted { .. }));
    eg_types::decision::statistical::log::InputsBlob {
        sha256: format!("sha256:{}", stored.sha256.to_hex()),
        manifest_digest: stored.manifest_digest.clone(),
        holder_id: stored.holder_id.clone(),
        length: stored.length,
    }
}

/// EH-060: compaction keeps the record digest, verify restores from CAS, and a
/// forged blob is refused.
#[cfg(feature = "blob")]
pub(super) async fn retention_compacts_and_verifies(
    h: &Harness,
    record: &StatisticalDecisionRecord,
) {
    use eg_types::decision::statistical::log::DecisionLogVerification;
    let first = compact_with(h, "policy-compact", None).await;
    assert_eq!((first.compacted, first.retired), (1, 0));
    let again = compact_with(h, "policy-compact-again", None).await;
    assert_eq!(
        (again.compacted, again.retired),
        (0, 0),
        "compaction is idempotent"
    );
    let entry = log_entry(h, &record.record_id);
    assert!(matches!(
        entry.record.inputs.feature_matrix,
        FeatureMatrixRef::Blob { .. }
    ));
    assert!(matches!(
        verify_log(h, &record.record_id).await.unwrap(),
        DecisionLogVerification::Verified { .. }
    ));

    let EntryInputs::Compacted {
        compacted_at_ms, ..
    } = entry.inputs.clone()
    else {
        panic!("compacted")
    };
    let mut forged = entry.clone();
    forged.inputs = EntryInputs::Compacted {
        blob: tampered_blob(h, &entry, record),
        compacted_at_ms,
    };
    write_entry(h, &forged);
    let refused = verify_log(h, &record.record_id).await.unwrap_err();
    assert!(refused.starts_with("DECISION_REPLAY_MISMATCH"), "{refused}");
    write_entry(h, &entry);
}

/// EH-060: past the drop age the blob is released and verify answers
/// INPUTS_RETIRED with the attested digest -- neither a pass nor a failure.
#[cfg(feature = "blob")]
pub(super) async fn retention_retires(h: &Harness, record: &StatisticalDecisionRecord) {
    use eg_types::decision::statistical::log::DecisionLogVerification;
    let entry = log_entry(h, &record.record_id);
    let dropped = compact_with(h, "policy-drop", Some(0)).await;
    assert_eq!(dropped.retired, 1);
    let verdict = verify_log(h, &record.record_id).await.unwrap();
    assert_eq!(
        verdict,
        DecisionLogVerification::InputsRetired {
            record_digest: log_entry(h, &record.record_id).record.record_digest,
            blob_sha256: compacted_sha(&entry),
        }
    );
}

#[cfg(feature = "blob")]
fn compacted_sha(entry: &DecisionLogEntry) -> String {
    let EntryInputs::Compacted { blob, .. } = &entry.inputs else {
        panic!("a compacted entry")
    };
    blob.sha256.clone()
}
