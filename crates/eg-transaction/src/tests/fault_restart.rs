//! Crash points around the ledger write and its replay receipt.
//!
//! Every case drops the admitted write without committing (the physical
//! equivalent of dying before `commit` returns), reopens the file through a new
//! storage kernel, and asserts the store is on exactly one side of the seam --
//! never half.

use super::*;
use crate::admitted::AdmittedMutation;
use crate::ReplayResolution;

/// Reopen the same owner file and rebind its serving scope.
fn reopen(
    path: &Path,
    identity: MutationScopeIdentity,
) -> (Fixture, OwnedStoreHandle<LedgerOnlyOwner>) {
    let fixture = Fixture::open::<LedgerOnlyOwner>(path, "physical:test:ledger-only", None);
    let owner = fixture.bind::<LedgerOnlyOwner>(
        &verifier("tenant-a", OwnerLayout::LedgerOnly),
        identity,
    );
    (fixture, owner)
}

#[test]
fn a_crash_between_admit_and_commit_leaves_no_batch_and_no_version_change() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:fault:1");
    let batch = batch(identity.clone(), "half-batch");
    {
        let (fixture, owner) = ledger_fixture(&path, identity.clone());
        let (write, begun) = fixture.mutations.admit(&owner, &batch).unwrap();
        let source_version = match begun {
            Begin::Apply { source_version } => source_version,
            Begin::Replay(_) => panic!("unexpected replay"),
        };
        fixture
            .mutations
            .finish(&write, &batch, None, 2, source_version)
            .unwrap();
        drop(write);
    }
    let (fixture, owner) = reopen(&path, identity);
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert!(read_ledger(&read, "half-batch").unwrap().is_none());
    assert_eq!(version(&read).unwrap(), 0);
    assert!(crate::read::read_batches(&read).unwrap().is_empty());
    assert!(crate::read::read_fences(&read).unwrap().is_none());
}

/// The effect row and its replay receipt share one physical transaction, so a
/// crash after the ledger write but before the receipt is durable cannot leave
/// a consumed nonce without its operation row -- or an operation row without
/// its batch.
#[test]
fn a_crash_between_the_ledger_write_and_its_receipt_rolls_back_both() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:fault:2");
    let batch = batch(identity.clone(), "receipt-batch");
    let attempt = context(1, "request-1", "idem:stable");
    let operation = operation_identity(&attempt, "mutation.apply", digest_of(30));
    let nonce = NonceReplayKeyV1::from_context(&attempt).unwrap();
    let recorded = receipt("receipt-1", &operation, &nonce);
    {
        let (fixture, owner) = ledger_fixture(&path, identity.clone());
        let (write, begun) = fixture.mutations.admit(&owner, &batch).unwrap();
        let source_version = match begun {
            Begin::Apply { source_version } => source_version,
            Begin::Replay(_) => panic!("unexpected replay"),
        };
        fixture
            .mutations
            .finish(&write, &batch, None, 2, source_version)
            .unwrap();
        fixture
            .mutations
            .record_replay(&write, &operation, &nonce, &recorded)
            .unwrap();
        drop(write);
    }
    let (fixture, owner) = reopen(&path, identity);
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert!(read_ledger(&read, "receipt-batch").unwrap().is_none());
    assert_eq!(
        resolve(&fixture, &owner, &operation, &nonce),
        ReplayResolution::Fresh
    );
}

/// A retry after that crash is admissible again, and once it commits the
/// original nonce is consumed and a fresh nonce replays the recorded receipt.
#[test]
fn a_retry_after_the_crash_commits_once_and_then_replays() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:fault:3");
    let batch = batch(identity.clone(), "retry-batch");
    let attempt = context(1, "request-1", "idem:stable");
    let operation = operation_identity(&attempt, "mutation.apply", digest_of(30));
    let nonce = NonceReplayKeyV1::from_context(&attempt).unwrap();
    let recorded = receipt("receipt-1", &operation, &nonce);
    {
        let (fixture, owner) = ledger_fixture(&path, identity.clone());
        let (write, _) = fixture.mutations.admit(&owner, &batch).unwrap();
        fixture
            .mutations
            .record_replay(&write, &operation, &nonce, &recorded)
            .unwrap();
        drop(write);
    }
    let (fixture, owner) = reopen(&path, identity);
    let (write, begun) = fixture.mutations.admit(&owner, &batch).unwrap();
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    fixture
        .mutations
        .record_replay(&write, &operation, &nonce, &recorded)
        .unwrap();
    fixture
        .mutations
        .finish(&write, &batch, None, 2, source_version)
        .unwrap();
    fixture.mutations.commit(write, &batch).unwrap();

    assert_eq!(
        resolve(&fixture, &owner, &operation, &nonce),
        ReplayResolution::NonceRejected {
            idempotency_key: "idem:stable".to_string()
        }
    );
    let second = context(2, "request-2", "idem:stable");
    let retried = operation_identity(&second, "mutation.apply", digest_of(30));
    let fresh_nonce = NonceReplayKeyV1::from_context(&second).unwrap();
    assert_eq!(
        resolve(&fixture, &owner, &retried, &fresh_nonce),
        ReplayResolution::ReplayedResult(Box::new(recorded))
    );
}

/// `bootstrap_ledger` is the one storage/ledger table-ownership seam: the
/// declared census either carries every ledger table this crate owns, or the
/// kernel refuses to admit anything. Re-running it is idempotent.
#[test]
fn bootstrap_ledger_proves_the_whole_ledger_census_and_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:bootstrap");
    let (fixture, owner) = ledger_fixture(&path, identity.clone());
    fixture.mutations.bootstrap_ledger(&owner).unwrap();
    fixture.mutations.bootstrap_ledger(&owner).unwrap();
    drop(fixture);

    let (fixture, owner) = reopen(&path, identity.clone());
    fixture.mutations.bootstrap_ledger(&owner).unwrap();
    apply_batch(&fixture, &owner, &batch(identity, "post-bootstrap"));
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert!(read_ledger(&read, "post-bootstrap").unwrap().is_some());
}

/// Purging one logical generation removes its replay evidence with its rows, so
/// a rebound generation never inherits another generation's nonces.
#[test]
fn purging_a_scope_removes_its_replay_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:purge");
    let (fixture, owner) = ledger_fixture(&path, identity.clone());
    let batch = batch(identity.clone(), "purged-batch");
    let attempt = context(1, "request-1", "idem:stable");
    let operation = operation_identity(&attempt, "mutation.apply", digest_of(30));
    let nonce = NonceReplayKeyV1::from_context(&attempt).unwrap();
    let recorded = receipt("receipt-1", &operation, &nonce);
    let (write, begun) = fixture.mutations.admit(&owner, &batch).unwrap();
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    fixture
        .mutations
        .record_replay(&write, &operation, &nonce, &recorded)
        .unwrap();
    fixture
        .mutations
        .finish(&write, &batch, None, 2, source_version)
        .unwrap();
    fixture.mutations.commit(write, &batch).unwrap();

    fixture.mutations.purge_scope(&owner, &identity).unwrap();
    let rebound = fixture.bind::<LedgerOnlyOwner>(
        &verifier("tenant-a", OwnerLayout::LedgerOnly),
        identity,
    );
    assert_eq!(
        resolve(&fixture, &rebound, &operation, &nonce),
        ReplayResolution::Fresh
    );
}

/// An owner-maintenance write is a full, durable, ledgered mutation, but it
/// carries no caller identity: it is labelled `Maintenance` in the ledger and
/// can never consume an attempt nonce or record an operation receipt.
#[test]
fn a_maintenance_write_is_ledgered_and_outside_operation_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:maintenance");
    let maintenance = batch(identity.clone(), "compaction");
    let attempt = context(1, "request-1", "idem:stable");
    let operation = operation_identity(&attempt, "mutation.apply", digest_of(30));
    let nonce = NonceReplayKeyV1::from_context(&attempt).unwrap();
    let recorded = receipt("receipt-1", &operation, &nonce);
    {
        let (fixture, owner) = ledger_fixture(&path, identity.clone());
        let (write, begun) = fixture
            .mutations
            .admit_maintenance(&owner, &maintenance)
            .unwrap();
        let source_version = match begun {
            Begin::Apply { source_version } => source_version,
            Begin::Replay(_) => panic!("unexpected replay"),
        };
        assert!(fixture
            .mutations
            .record_replay(&write, &operation, &nonce, &recorded)
            .unwrap_err()
            .contains("MAINTENANCE_HAS_NO_REPLAY_IDENTITY"));
        fixture
            .mutations
            .finish(&write, &maintenance, None, 2, source_version)
            .unwrap();
        fixture.mutations.commit(write, &maintenance).unwrap();
    }
    let (fixture, owner) = reopen(&path, identity.clone());
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert!(read_ledger(&read, "compaction").unwrap().is_some());
    assert_eq!(version(&read).unwrap(), 1);
    assert_eq!(
        crate::read::read_class(&read, "compaction").unwrap(),
        Some(eg_storage::MutationClass::Maintenance)
    );
    // It consumed no nonce and recorded no operation identity.
    assert_eq!(
        resolve(&fixture, &owner, &operation, &nonce),
        ReplayResolution::Fresh
    );

    // An ordinary operation in the same scope is labelled Operation. The
    // maintenance write bumped the scope to version 1, which it must expect.
    let mut operation_batch = batch(identity, "caller-op");
    operation_batch.version_expectation = VersionExpectation::Native(1);
    apply_batch(&fixture, &owner, &operation_batch);
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert_eq!(
        crate::read::read_class(&read, "caller-op").unwrap(),
        Some(eg_storage::MutationClass::Operation)
    );
}

/// RF-RULING-005: a maintenance write is a real mutation, so it advances the
/// scope version like any other. The bump is now unconditional rather than
/// driven by `VersionExpectation`, so a batch cannot be ledgered while leaving
/// the counter untouched and the write invisible to any reader doing OCC.
///
/// `VersionExpectation::Unversioned` — the case that previously skipped the
/// bump entirely — is not exercised here because `MutationBatch` validation
/// restricts it to a reserved-system tenant on a control-plane/lifecycle scope
/// with a verified capability, which this fixture is not. The bump no longer
/// consults the expectation at all, so that path is closed by construction.
#[test]
fn every_admitted_batch_advances_the_scope_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:versions");
    let (fixture, owner) = ledger_fixture(&path, identity.clone());
    assert_eq!(version(&fixture.kernel.read_scope(&owner).unwrap()).unwrap(), 0);

    let maintenance = batch(identity.clone(), "compaction");
    let (write, begun) = fixture
        .mutations
        .admit_maintenance(&owner, &maintenance)
        .unwrap();
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    fixture
        .mutations
        .finish(&write, &maintenance, None, 2, source_version)
        .unwrap();
    fixture.mutations.commit(write, &maintenance).unwrap();
    assert_eq!(version(&fixture.kernel.read_scope(&owner).unwrap()).unwrap(), 1);

    let mut versioned = batch(identity, "caller-operation");
    versioned.version_expectation = VersionExpectation::Native(1);
    apply_batch(&fixture, &owner, &versioned);
    assert_eq!(version(&fixture.kernel.read_scope(&owner).unwrap()).unwrap(), 2);
}

/// A maintenance label and a recorded operation identity are contradictory, so
/// a store carrying both refuses to reopen. This is what makes the label
/// checkable rather than merely asserted.
#[test]
fn a_maintenance_batch_carrying_an_operation_identity_fails_to_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:mislabel");
    let maintenance = batch(identity.clone(), "mislabelled");
    let attempt = context(1, "request-1", &maintenance.idempotency_key);
    let operation = operation_identity(&attempt, "mutation.apply", digest_of(30));
    let nonce = NonceReplayKeyV1::from_context(&attempt).unwrap();
    let recorded = receipt("receipt-1", &operation, &nonce);
    {
        let (fixture, owner) = ledger_fixture(&path, identity.clone());
        let (write, begun) = fixture
            .mutations
            .admit_maintenance(&owner, &maintenance)
            .unwrap();
        let source_version = match begun {
            Begin::Apply { source_version } => source_version,
            Begin::Replay(_) => panic!("unexpected replay"),
        };
        fixture
            .mutations
            .finish(&write, &maintenance, None, 2, source_version)
            .unwrap();
        fixture.mutations.commit(write, &maintenance).unwrap();

        // Plant the contradiction the kernel's own write path refuses to make.
        let write = AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
        let row = eg_storage::OperationReplayRow {
            identity: identity.clone(),
            idempotency_key: maintenance.idempotency_key.clone(),
            operation_replay_digest: operation.digest().unwrap(),
            nonce_replay_digest: nonce.digest().unwrap(),
            receipt: recorded,
        };
        let bytes = eg_storage::encode_bounded(&row, "planted replay row").unwrap();
        let scope = eg_storage::ledger_scope_key(&identity);
        write
            .scoped_table(crate::tables::REPLAY_OPERATIONS)
            .unwrap()
            .insert(
                (scope.as_str(), maintenance.idempotency_key.as_str()),
                bytes.as_slice(),
            )
            .unwrap();
        write.commit().unwrap();
    }
    match StorageKernelV1::open_owner::<LedgerOnlyOwner>(
        &path,
        PhysicalStoreIdentity::new("physical:test:ledger-only").unwrap(),
        None,
    ) {
        Ok(_) => panic!("a contradictory maintenance label must not reopen"),
        Err(error) => assert!(error.contains("recorded operation replay identity"), "{error}"),
    }
}
