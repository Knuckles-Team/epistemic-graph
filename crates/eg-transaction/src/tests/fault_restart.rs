//! Crash points around the ledger write and its replay receipt.
//!
//! Every case drops the admitted write without committing (the physical
//! equivalent of dying before `commit` returns), reopens the file through a new
//! storage kernel, and asserts the store is on exactly one side of the seam --
//! never half.

use super::*;
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
