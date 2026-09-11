//! RF-RULING-004's two-identity replay matrix.
//!
//! Each case fixes the stable [`OperationReplayIdentity`] or varies exactly
//! one of its fields, and varies the attempt nonce independently, so the four
//! outcomes are separated by construction rather than by coincidence.

use super::*;
use crate::tables::{OUTBOX, REPLAY_OPERATIONS};
use crate::ReplayResolution;
use eg_storage::RecordedOperation;
use eg_types::MutationOutboxIntent;

/// Record one committed attempt: consume its nonce and store its receipt in the
/// same admitted write as the batch, then commit.
///
/// The terminal record carries the receipt's OWN encoded result, exactly as
/// every production owner does (`agent_library`'s commit path passes
/// `encode_domain_result(&stable_result)` to `finish_with_replay` while the
/// receipt it files carries `domain_result_for(&stable_result)` -- the same
/// value). `finalize_replay_receipt` proves that equality before it proves
/// anything about the operation row, so a fixture that committed `None` here
/// could never reach the row checks at all: every finalization against it
/// failed on the result comparison regardless of what the row said.
fn record_attempt(
    fixture: &Fixture,
    owner: &OwnedStoreHandle<LedgerOnlyOwner>,
    batch: &MutationBatch,
    operation: &OperationReplayIdentity,
    nonce: &NonceReplayKey,
    receipt: &MutationReceipt,
) {
    let (write, begun) = fixture.mutations.admit(owner, batch).unwrap();
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    fixture
        .mutations
        .record_replay(&write, operation, nonce, receipt)
        .unwrap();
    let result_msgpack =
        eg_storage::encode_bounded(&receipt.result, "typed replay result").unwrap();
    fixture
        .mutations
        .finish(&write, batch, Some(result_msgpack), 2, source_version)
        .unwrap();
    fixture.mutations.commit(write, batch).unwrap();
}

fn committed_fixture(
    dir: &Path,
) -> (
    Fixture,
    OwnedStoreHandle<LedgerOnlyOwner>,
    AuthorityContext,
    OperationReplayIdentity,
    NonceReplayKey,
    MutationReceipt,
) {
    let identity = native_identity("tenant-a", "incarnation:replay");
    let (fixture, owner) = ledger_fixture(&dir.join("replay.redb"), identity.clone());
    let first = context(1, "request-1", "idem:stable");
    let operation = operation_identity(&first, "mutation.apply", digest_of(30));
    let nonce = NonceReplayKey::from_context(&first).unwrap();
    let recorded = receipt("receipt-1", &operation, &nonce);
    record_attempt(
        &fixture,
        &owner,
        &batch(identity, "replay-batch"),
        &operation,
        &nonce,
        &recorded,
    );
    (fixture, owner, first, operation, nonce, recorded)
}

#[test]
fn a_fresh_attempt_resolves_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:replay");
    let (fixture, owner) = ledger_fixture(&dir.path().join("replay.redb"), identity);
    let attempt = context(1, "request-1", "idem:stable");
    let operation = operation_identity(&attempt, "mutation.apply", digest_of(30));
    let nonce = NonceReplayKey::from_context(&attempt).unwrap();
    assert_eq!(
        resolve(&fixture, &owner, &operation, &nonce),
        ReplayResolution::Fresh
    );
}

#[test]
fn nonce_only_resolution_precedes_operation_lookup_and_is_abort_safe() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:nonce-only");
    let (fixture, owner) = ledger_fixture(&dir.path().join("nonce-only.redb"), identity.clone());
    let first = batch(identity, "nonce-only");
    apply_batch(&fixture, &owner, &first);
    let operation = first.envelope.operation().unwrap();
    let consumed_nonce = operation.nonce_replay_key().unwrap();
    let consumed_key = operation.operation_identity().unwrap().idempotency_key;

    // A nonce-only check reports the consumed attempt without constructing or
    // comparing any stable operation identity, so it remains the first guard
    // even when a caller's eventual operation body would conflict.
    let write = fixture.mutations.open_write(&owner).unwrap();
    assert_eq!(
        fixture
            .mutations
            .resolve_nonce(&write, &consumed_nonce)
            .unwrap(),
        Some(consumed_key.as_str().to_string())
    );
    write.abort().unwrap();

    // A fresh attempt is a read-only `None`; aborting that probe leaves the
    // nonce available for the full replay resolver and later finalization.
    let retry = retry_of(&first);
    let retry_nonce = retry.envelope.operation().unwrap().nonce_replay_key().unwrap();
    let write = fixture.mutations.open_write(&owner).unwrap();
    assert_eq!(
        fixture.mutations.resolve_nonce(&write, &retry_nonce).unwrap(),
        None
    );
    write.abort().unwrap();
    let (write, begun) = fixture.mutations.admit(&owner, &retry).unwrap();
    assert!(matches!(begun, Begin::Replay(_)));
    write.abort().unwrap();
}

#[test]
fn the_same_nonce_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (fixture, owner, _, operation, nonce, _) = committed_fixture(dir.path());
    assert_eq!(
        resolve(&fixture, &owner, &operation, &nonce),
        ReplayResolution::NonceRejected {
            idempotency_key: "idem:stable".to_string()
        }
    );
}

#[test]
fn a_fresh_nonce_over_the_same_stable_identity_replays_the_recorded_result() {
    let dir = tempfile::tempdir().unwrap();
    let (fixture, owner, first, operation, nonce, recorded) = committed_fixture(dir.path());
    // A second attempt of the SAME operation: new request id, new trace id, new
    // nonce -- so the attempt-specific context digest and nonce digest both
    // change while the stable operation digest does not.
    let second = context(2, "request-2", "idem:stable");
    let retried = operation_identity(&second, "mutation.apply", digest_of(30));
    let fresh_nonce = NonceReplayKey::from_context(&second).unwrap();
    assert_ne!(first.context_digest, second.context_digest);
    assert_ne!(nonce.digest().unwrap(), fresh_nonce.digest().unwrap());
    assert_eq!(operation.digest().unwrap(), retried.digest().unwrap());
    assert_eq!(
        resolve(&fixture, &owner, &retried, &fresh_nonce),
        ReplayResolution::ReplayedResult(Box::new(RecordedOperation::Receipt(Box::new(
            recorded
        ))))
    );
}

#[test]
fn replay_nonce_is_finalized_only_when_the_replay_write_commits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replay-finalize.redb");
    let identity = native_identity("tenant-a", "incarnation:replay-finalize");
    let (fixture, owner) = ledger_fixture(&path, identity.clone());
    let first = batch(identity.clone(), "replay-finalize");
    apply_batch(&fixture, &owner, &first);

    let original_nonce = match fixture.mutations.admit(&owner, &first) {
        Err(error) => error,
        Ok((write, _)) => {
            write
                .abort()
                .expect("unexpected successful replay admission aborts");
            panic!("the original attempt nonce remains consumed");
        }
    };
    assert!(
        original_nonce.contains("REPLAY_NONCE_CONSUMED"),
        "{original_nonce}"
    );

    // A fresh replay probe can be discarded without consuming its nonce.
    let probe_batch = retry_of(&first);
    let (probe, begun) = fixture.mutations.admit(&owner, &probe_batch).unwrap();
    assert!(matches!(begun, Begin::Replay(_)));
    probe.abort().unwrap();
    let (probe, begun) = fixture.mutations.admit(&owner, &probe_batch).unwrap();
    assert!(matches!(begun, Begin::Replay(_)));
    probe.abort().unwrap();

    // Committing the replay consumes only the fresh attempt nonce and leaves
    // the original receipt and authoritative version unchanged.
    let committed_retry = retry_of(&first);
    let (write, begun) = fixture.mutations.admit(&owner, &committed_retry).unwrap();
    assert!(matches!(begun, Begin::Replay(_)));
    fixture.mutations.commit(write, &committed_retry).unwrap();
    assert_eq!(
        version(&fixture.kernel.read_scope(&owner).unwrap()).unwrap(),
        1
    );

    let reused = match fixture.mutations.admit(&owner, &committed_retry) {
        Err(error) => error,
        Ok((write, _)) => {
            write
                .abort()
                .expect("unexpected successful replay admission aborts");
            panic!("a committed replay nonce cannot be reused");
        }
    };
    assert!(reused.contains("REPLAY_NONCE_CONSUMED"), "{reused}");
}

#[test]
fn concurrent_replays_have_one_nonce_winner() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:replay-concurrent");
    let (fixture, owner) =
        ledger_fixture(&dir.path().join("replay-concurrent.redb"), identity.clone());
    let first = batch(identity.clone(), "replay-concurrent");
    apply_batch(&fixture, &owner, &first);

    // Both contenders share the one opened authority and scope handle. Opening
    // the same redb path independently in each thread is invalid (redb rejects
    // the second live Database handle), and can turn this test into a scheduler
    // accident instead of a replay race.
    let fixture = std::sync::Arc::new(fixture);
    let owner = std::sync::Arc::new(owner);
    let candidate = std::sync::Arc::new(retry_of(&first));
    let ready = std::sync::Arc::new(std::sync::Barrier::new(2));
    let run = |fixture: std::sync::Arc<Fixture>,
               owner: std::sync::Arc<OwnedStoreHandle<LedgerOnlyOwner>>,
               candidate: std::sync::Arc<MutationBatch>,
               ready: std::sync::Arc<std::sync::Barrier>| {
        ready.wait();
        match fixture.mutations.admit(&owner, &candidate) {
            Ok((write, Begin::Replay(_))) => fixture.mutations.commit(write, &candidate),
            Ok((write, Begin::Apply { .. })) => {
                write.abort()?;
                Err("a replay candidate was admitted as fresh".to_string())
            }
            Err(error) => Err(error),
        }
    };
    let (first_result, second_result) = std::thread::scope(|scope| {
        let first = scope.spawn({
            let fixture = std::sync::Arc::clone(&fixture);
            let owner = std::sync::Arc::clone(&owner);
            let candidate = std::sync::Arc::clone(&candidate);
            let ready = std::sync::Arc::clone(&ready);
            move || run(fixture, owner, candidate, ready)
        });
        let second = scope.spawn({
            let fixture = std::sync::Arc::clone(&fixture);
            let owner = std::sync::Arc::clone(&owner);
            let candidate = std::sync::Arc::clone(&candidate);
            let ready = std::sync::Arc::clone(&ready);
            move || run(fixture, owner, candidate, ready)
        });
        (first.join().unwrap(), second.join().unwrap())
    });
    let results = [first_result, second_result];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| {
                result
                    .as_ref()
                    .is_err_and(|error| error.contains("REPLAY_NONCE_CONSUMED"))
            })
            .count(),
        1
    );
}

/// A changed payload, method, scope or policy under the same idempotency key is
/// a conflict, never a replay.
#[test]
fn a_changed_operation_identity_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let (fixture, owner, _, operation, _, _) = committed_fixture(dir.path());
    let recorded = operation.digest().unwrap();

    let mut changed_payload = operation.clone();
    changed_payload.canonical_payload_digest = digest_of(31);
    let mut changed_method = operation.clone();
    changed_method.method = MethodId::new("mutation.replace").unwrap();
    let mut changed_scope = operation.clone();
    changed_scope.authority_scope.parent_scope_ids =
        BoundedVec::new(vec![ResourceId::new("tenant:a").unwrap(), ResourceId::new("zone:a").unwrap()])
            .unwrap();
    let mut changed_policy = operation.clone();
    changed_policy.policy_digest = digest_of(32);

    for proposal in [
        changed_payload,
        changed_method,
        changed_scope,
        changed_policy,
    ] {
        let attempt = context(3, "request-3", "idem:stable");
        let fresh_nonce = NonceReplayKey::from_context(&attempt).unwrap();
        let proposed = proposal.digest().unwrap();
        assert_ne!(proposed, recorded);
        assert_eq!(
            resolve(&fixture, &owner, &proposal, &fresh_nonce),
            ReplayResolution::Conflict { recorded, proposed }
        );
    }
}

/// The attempt-specific context digest can never decide operation replay: two
/// attempts of one operation have different context and nonce digests but the
/// same operation digest, and the receipt binds both replay digests separately.
#[test]
fn the_context_digest_is_never_the_operation_replay_key() {
    let first = context(1, "request-1", "idem:stable");
    let second = context(2, "request-2", "idem:stable");
    let one = operation_identity(&first, "mutation.apply", digest_of(30));
    let two = operation_identity(&second, "mutation.apply", digest_of(30));
    let first_nonce = NonceReplayKey::from_context(&first).unwrap();
    let second_nonce = NonceReplayKey::from_context(&second).unwrap();
    assert_ne!(first.context_digest, second.context_digest);
    assert_ne!(
        first_nonce.digest().unwrap(),
        second_nonce.digest().unwrap()
    );
    assert_eq!(one.digest().unwrap(), two.digest().unwrap());
    assert_ne!(one.digest().unwrap(), first.context_digest);

    let evidence = receipt("receipt-1", &one, &first_nonce);
    assert_eq!(evidence.operation_replay_digest, one.digest().unwrap());
    assert_eq!(
        evidence.nonce_replay_digest,
        first_nonce.digest().unwrap()
    );
    assert_ne!(
        evidence.operation_replay_digest,
        evidence.nonce_replay_digest
    );
}

/// A receipt that does not bind both digests cannot be recorded.
#[test]
fn a_receipt_must_bind_both_replay_digests() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:replay");
    let (fixture, owner) = ledger_fixture(&dir.path().join("replay.redb"), identity.clone());
    let attempt = context(1, "request-1", "idem:stable");
    let operation = operation_identity(&attempt, "mutation.apply", digest_of(30));
    let nonce = NonceReplayKey::from_context(&attempt).unwrap();
    let other = context(9, "request-9", "idem:stable");
    let mismatched = receipt(
        "receipt-1",
        &operation,
        &NonceReplayKey::from_context(&other).unwrap(),
    );
    let (write, _) = fixture
        .mutations
        .admit(&owner, &batch(identity, "replay-batch"))
        .unwrap();
    assert!(fixture
        .mutations
        .record_replay(&write, &operation, &nonce, &mismatched)
        .unwrap_err()
        .contains("does not bind both replay digests"));
    write.abort().unwrap();
}

/// Two attempts that both saw `Fresh` must not both apply. Recording is the
/// enforcing point: the second fails closed instead of overwriting the receipt.
#[test]
fn a_second_recording_of_the_same_operation_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let (fixture, owner, _, operation, nonce, recorded) = committed_fixture(dir.path());
    let identity = owner.identity().clone();

    // Same nonce: already consumed. The scope is at version 1 after
    // `committed_fixture`, so a follow-up batch must expect that version.
    let mut second_attempt = batch(identity.clone(), "second-attempt");
    second_attempt.version_expectation = VersionExpectation::Native(1);
    let (write, _) = fixture.mutations.admit(&owner, &second_attempt).unwrap();
    assert!(fixture
        .mutations
        .record_replay(&write, &operation, &nonce, &recorded)
        .unwrap_err()
        .contains("REPLAY_NONCE_CONSUMED"));
    write.abort().unwrap();

    // Fresh nonce, identical stable identity: the caller should have replayed.
    let second = context(2, "request-2", "idem:stable");
    let retried = operation_identity(&second, "mutation.apply", digest_of(30));
    let fresh_nonce = NonceReplayKey::from_context(&second).unwrap();
    let other = receipt("receipt-2", &retried, &fresh_nonce);
    let mut third_attempt = batch(identity, "third-attempt");
    third_attempt.version_expectation = VersionExpectation::Native(1);
    let (write, _) = fixture.mutations.admit(&owner, &third_attempt).unwrap();
    assert!(fixture
        .mutations
        .record_replay(&write, &retried, &fresh_nonce, &other)
        .unwrap_err()
        .contains("REPLAY_ALREADY_RECORDED"));
    write.abort().unwrap();

    // The originally recorded receipt is intact.
    assert_eq!(
        resolve(&fixture, &owner, &retried, &fresh_nonce),
        ReplayResolution::ReplayedResult(Box::new(RecordedOperation::Receipt(Box::new(
            recorded
        ))))
    );
}

/// A receipt naming a different authority scope than the operation it records
/// is refused.
#[test]
fn a_receipt_naming_another_scope_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:replay");
    let (fixture, owner) = ledger_fixture(&dir.path().join("replay.redb"), identity.clone());
    let attempt = context(1, "request-1", "idem:stable");
    let operation = operation_identity(&attempt, "mutation.apply", digest_of(30));
    let nonce = NonceReplayKey::from_context(&attempt).unwrap();
    let mut mismatched = receipt("receipt-1", &operation, &nonce);
    mismatched.scope.scope_id = ResourceId::new("graph:tenant:a/other").unwrap();
    let (write, _) = fixture
        .mutations
        .admit(&owner, &batch(identity, "scope-mismatch"))
        .unwrap();
    assert!(fixture
        .mutations
        .record_replay(&write, &operation, &nonce, &mismatched)
        .unwrap_err()
        .contains("names a different scope"));
    write.abort().unwrap();
}

#[test]
fn finalize_replay_receipt_rejects_persisted_operation_row_tampering() {
    for tamper in ["idempotency", "nonce"] {
        let dir = tempfile::tempdir().unwrap();
        let (fixture, owner, _, operation, _, receipt) = committed_fixture(dir.path());
        let scope_key = eg_storage::ledger_scope_key(owner.identity());
        let write = fixture.mutations.open_write(&owner).unwrap();
        let mut operations = write.scoped_table(REPLAY_OPERATIONS).unwrap();
        let row_bytes = operations
            .get((scope_key.as_str(), operation.idempotency_key.as_str()))
            .unwrap()
            .expect("committed operation replay row")
            .value()
            .to_vec();
        let mut row: eg_storage::OperationReplayRow =
            eg_storage::decode_ledger_record(&row_bytes).unwrap();
        match tamper {
            "idempotency" => row.idempotency_key = "tampered-key".to_string(),
            "nonce" => row.nonce_replay_digest = digest_of(99),
            _ => unreachable!(),
        }
        let encoded = eg_storage::encode_bounded(&row, "tampered operation replay row").unwrap();
        operations
            .insert(
                (scope_key.as_str(), operation.idempotency_key.as_str()),
                encoded.as_slice(),
            )
            .unwrap();
        drop(operations);
        write.commit().unwrap();

        let retry = context(2, "request-2", "idem:stable");
        let fresh_nonce = NonceReplayKey::from_context(&retry).unwrap();
        let write = fixture.mutations.open_write(&owner).unwrap();
        let error = fixture
            .mutations
            .finalize_replay_receipt(&write, &operation, &fresh_nonce, &receipt)
            .unwrap_err();
        assert!(
            error.contains("typed replay receipt differs from durable row"),
            "{tamper}: {error}"
        );
        write.abort().unwrap();
    }
}

#[test]
fn replay_evidence_rejects_a_persisted_outbox_key_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:outbox-key");
    let (fixture, owner) = ledger_fixture(&dir.path().join("outbox-key.redb"), identity.clone());
    let mut candidate = batch(identity, "outbox-key");
    candidate.outbox.push(MutationOutboxIntent {
        topic: "agent-library.test".to_string(),
        key: "outbox-key".to_string(),
        payload: b"payload".to_vec(),
        headers: Default::default(),
    });
    // `batch()` seals its envelope over the body it returns, which has an EMPTY
    // outbox. The compile path mints from final content, so a fixture that
    // appends to the body afterwards must re-mint the same way or the envelope
    // still describes bytes the batch no longer has -- which is exactly what
    // `validate` refuses (and what it refused here, before this test ever
    // reached the kernel it means to exercise).
    candidate
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("a fixture batch reseals its envelope over its final body");
    candidate.validate().unwrap();
    apply_batch(&fixture, &owner, &candidate);

    // Rewrite the persisted row under a different physical ordinal while
    // retaining its encoded ordinal. The kernel must compare both key and row
    // in the same write before a domain can treat the evidence as authoritative.
    let scope_key = eg_storage::ledger_scope_key(owner.identity());
    let write = fixture.mutations.open_write(&owner).unwrap();
    let mut outbox = write.scoped_table(OUTBOX).unwrap();
    let bytes = outbox
        .get((scope_key.as_str(), candidate.batch_id.as_str(), 0))
        .unwrap()
        .expect("committed physical outbox row")
        .value()
        .to_vec();
    outbox
        .remove((scope_key.as_str(), candidate.batch_id.as_str(), 0))
        .unwrap();
    outbox
        .insert(
            (scope_key.as_str(), candidate.batch_id.as_str(), 1),
            bytes.as_slice(),
        )
        .unwrap();
    drop(outbox);
    write.commit().unwrap();

    let write = fixture.mutations.open_write(&owner).unwrap();
    let error = fixture
        .mutations
        .read_replay_evidence(&write, &candidate.batch_id)
        .unwrap_err();
    assert!(error.contains("physical key"), "{error}");
    write.abort().unwrap();
}
