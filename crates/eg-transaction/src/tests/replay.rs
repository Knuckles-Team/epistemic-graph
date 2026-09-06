//! RF-RULING-004's two-identity replay matrix.
//!
//! Each case fixes the stable [`OperationReplayIdentityV1`] or varies exactly
//! one of its fields, and varies the attempt nonce independently, so the four
//! outcomes are separated by construction rather than by coincidence.

use super::*;
use crate::ReplayResolution;

/// Record one committed attempt: consume its nonce and store its receipt in the
/// same admitted write as the batch, then commit.
fn record_attempt(
    fixture: &Fixture,
    owner: &OwnedStoreHandle<LedgerOnlyOwner>,
    batch: &MutationBatch,
    operation: &OperationReplayIdentityV1,
    nonce: &NonceReplayKeyV1,
    receipt: &MutationReceiptV1,
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
    fixture
        .mutations
        .finish(&write, batch, None, 2, source_version)
        .unwrap();
    fixture.mutations.commit(write, batch).unwrap();
}

fn committed_fixture(
    dir: &Path,
) -> (
    Fixture,
    OwnedStoreHandle<LedgerOnlyOwner>,
    AuthorityContextV1,
    OperationReplayIdentityV1,
    NonceReplayKeyV1,
    MutationReceiptV1,
) {
    let identity = native_identity("tenant-a", "incarnation:replay");
    let (fixture, owner) = ledger_fixture(&dir.join("replay.redb"), identity.clone());
    let first = context(1, "request-1", "idem:stable");
    let operation = operation_identity(&first, "mutation.apply", digest_of(30));
    let nonce = NonceReplayKeyV1::from_context(&first).unwrap();
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
    let nonce = NonceReplayKeyV1::from_context(&attempt).unwrap();
    assert_eq!(
        fixture
            .mutations
            .resolve_replay(&owner, &operation, &nonce)
            .unwrap(),
        ReplayResolution::Fresh
    );
}

#[test]
fn the_same_nonce_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (fixture, owner, _, operation, nonce, _) = committed_fixture(dir.path());
    assert_eq!(
        fixture
            .mutations
            .resolve_replay(&owner, &operation, &nonce)
            .unwrap(),
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
    let fresh_nonce = NonceReplayKeyV1::from_context(&second).unwrap();
    assert_ne!(first.context_digest, second.context_digest);
    assert_ne!(nonce.digest().unwrap(), fresh_nonce.digest().unwrap());
    assert_eq!(operation.digest().unwrap(), retried.digest().unwrap());
    assert_eq!(
        fixture
            .mutations
            .resolve_replay(&owner, &retried, &fresh_nonce)
            .unwrap(),
        ReplayResolution::ReplayedResult(Box::new(recorded))
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
    changed_method.method = MethodIdV1::new("mutation.replace").unwrap();
    let mut changed_scope = operation.clone();
    changed_scope.authority_scope.parent_scope_ids =
        BoundedVecV1::new(vec![ResourceIdV1::new("tenant:a").unwrap(), ResourceIdV1::new("zone:a").unwrap()])
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
        let fresh_nonce = NonceReplayKeyV1::from_context(&attempt).unwrap();
        let proposed = proposal.digest().unwrap();
        assert_ne!(proposed, recorded);
        assert_eq!(
            fixture
                .mutations
                .resolve_replay(&owner, &proposal, &fresh_nonce)
                .unwrap(),
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
    let first_nonce = NonceReplayKeyV1::from_context(&first).unwrap();
    let second_nonce = NonceReplayKeyV1::from_context(&second).unwrap();
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
    let nonce = NonceReplayKeyV1::from_context(&attempt).unwrap();
    let other = context(9, "request-9", "idem:stable");
    let mismatched = receipt(
        "receipt-1",
        &operation,
        &NonceReplayKeyV1::from_context(&other).unwrap(),
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
