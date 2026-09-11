//! Backup, fingerprint and adoption must cover the whole ledger census.
//!
//! Format v2 added two replay tables. The sweeps that copy, hash and pin a
//! store were hand-maintained lists that did not include them, so a backup
//! dropped every consumed nonce and recorded receipt while still claiming the
//! copy was proved exactly, and the adoption TOCTOU guard could not see a
//! replay row change. These cases fail on any table the authoritative list
//! declares but a sweep forgets.

use super::*;
use crate::admitted::AdmittedMutation;
use crate::tables::REPLAY_NONCES;
use crate::ReplayResolution;
use eg_storage::RecordedOperation;
use eg_storage::{
    backup_recovery_store, backup_strict_recovery_store, recovery_store_fingerprint, BlobOwner,
};
use redb::TableDefinition;

struct Attempt {
    operation: OperationReplayIdentity,
    nonce: NonceReplayKey,
    receipt: MutationReceipt,
}

fn attempt(nonce_byte: u8, request: &str, idempotency: &str) -> Attempt {
    let context = context(nonce_byte, request, idempotency);
    let operation = operation_identity(&context, "mutation.apply", digest_of(30));
    let nonce = NonceReplayKey::from_context(&context).unwrap();
    let receipt = receipt(&format!("receipt-{request}"), &operation, &nonce);
    Attempt {
        operation,
        nonce,
        receipt,
    }
}

/// Commit one batch together with its replay evidence.
fn commit_with_replay(
    fixture: &Fixture,
    owner: &OwnedStoreHandle<LedgerOnlyOwner>,
    batch: &MutationBatch,
    attempt: &Attempt,
) {
    let (write, begun) = fixture.mutations.admit(owner, batch).unwrap();
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    assert_eq!(
        fixture
            .mutations
            .resolve_replay(&write, &attempt.operation, &attempt.nonce)
            .unwrap(),
        ReplayResolution::Fresh
    );
    fixture
        .mutations
        .record_replay(&write, &attempt.operation, &attempt.nonce, &attempt.receipt)
        .unwrap();
    // The receipt row recorded above and the batch row written by `finish` must
    // agree on the result: a typed replay receipt's own result IS the linked
    // batch's durable result, which is what `finish_with_replay` checks at
    // commit time and recovery validation re-checks on reopen. Passing `None`
    // here would record a receipt that disagrees with its own batch.
    let result_msgpack =
        eg_storage::encode_bounded(&attempt.receipt.result, "mutation receipt result").unwrap();
    fixture
        .mutations
        .finish(&write, batch, Some(result_msgpack), 2, source_version)
        .unwrap();
    fixture.mutations.commit(write, batch).unwrap();
}

fn seeded(dir: &Path, file: &str) -> (Fixture, OwnedStoreHandle<LedgerOnlyOwner>, Attempt) {
    let identity = native_identity("tenant-a", "incarnation:backup");
    let (fixture, owner) = ledger_fixture(&dir.join(file), identity.clone());
    let first = attempt(1, "request-1", "idem:stable");
    commit_with_replay(&fixture, &owner, &batch(identity, "backed-up"), &first);
    (fixture, owner, first)
}

/// A consumed nonce and its recorded receipt must survive the coordinator
/// backup. Before the sweep was driven by the authoritative list this returned
/// `Fresh` in the copy — the double-apply the ruling exists to prevent.
#[test]
fn a_coordinator_backup_preserves_every_consumed_nonce_and_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:backup");
    let destination = dir.path().join("backup.redb");
    let first = {
        let (fixture, _, first) = seeded(dir.path(), "source.redb");
        backup_recovery_store(&fixture.kernel, &destination).unwrap();
        first
    };
    let copy = Fixture::open::<LedgerOnlyOwner>(&destination, "physical:test:ledger-only", None);
    let owner =
        copy.bind::<LedgerOnlyOwner>(&verifier("tenant-a", OwnerLayout::LedgerOnly), identity);
    assert_eq!(
        resolve(&copy, &owner, &first.operation, &first.nonce),
        ReplayResolution::NonceRejected {
            idempotency_key: "idem:stable".to_string()
        }
    );
    let second = attempt(2, "request-2", "idem:stable");
    assert_eq!(
        resolve(&copy, &owner, &second.operation, &second.nonce),
        ReplayResolution::ReplayedResult(Box::new(RecordedOperation::Receipt(Box::new(
            first.receipt
        ))))
    );
}

/// The strict backup proves its copy table by table. That proof must include
/// the replay tables, so their rows are copied and their fingerprints match.
#[test]
fn a_strict_backup_proves_the_replay_tables_it_copied() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:backup");
    let destination = dir.path().join("strict.redb");
    let (fixture, _, first) = seeded(dir.path(), "source.redb");
    let evidence = backup_strict_recovery_store(
        &fixture.kernel,
        &destination,
        PhysicalStoreIdentity::new("physical:test:strict").unwrap(),
    )
    .unwrap();
    for table in ["mutation_replay_nonces", "mutation_replay_operations"] {
        let row = evidence
            .tables
            .iter()
            .find(|entry| entry.table_id == table)
            .unwrap_or_else(|| panic!("{table} is missing from strict backup evidence"));
        // TWO rows, not one: the seed both COMMITS an operation batch -- which
        // now records its own `(operation, nonce)` pair under the batch's own
        // idempotency key, because the batch path and the authority-context path
        // share the one replay table -- and records an authority-context receipt
        // under `idem:stable`. They are two different operations, and a backup
        // that copied only one of them would resolve the other `Fresh`.
        assert_eq!(row.rows, 2, "{table}");
    }
    drop(fixture);

    let copy = Fixture::open::<LedgerOnlyOwner>(&destination, "physical:test:strict", None);
    let owner =
        copy.bind::<LedgerOnlyOwner>(&verifier("tenant-a", OwnerLayout::LedgerOnly), identity);
    assert_eq!(
        resolve(&copy, &owner, &first.operation, &first.nonce),
        ReplayResolution::NonceRejected {
            idempotency_key: "idem:stable".to_string()
        }
    );
}

/// Two stores differing only in their replay ledger must not share a
/// fingerprint, or the adoption guard cannot see a replay row change.
#[test]
fn the_recovery_fingerprint_covers_the_replay_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let (fixture, owner, first) = seeded(dir.path(), "source.redb");
    let before = recovery_store_fingerprint(&fixture.kernel).unwrap();

    // Delete exactly one consumed nonce row and nothing else.
    let write = AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope_key = eg_storage::ledger_scope_key(owner.identity());
    let nonce_digest = first.nonce.digest().unwrap().to_hex();
    write
        .scoped_table(REPLAY_NONCES)
        .unwrap()
        .remove((scope_key.as_str(), nonce_digest.as_str()))
        .unwrap();
    write.commit().unwrap();

    let after = recovery_store_fingerprint(&fixture.kernel).unwrap();
    assert_ne!(before, after, "a removed nonce must change the fingerprint");
}

/// The coordinator backup must carry the domain payload, not just the ledger.
/// It copied no owner tables at all, so a backup of an Rbac or Kv store came
/// back with every domain row missing and validated as good.
#[test]
fn a_coordinator_backup_round_trips_every_owner_row() {
    // `cas_blobs` is `&str -> &[u8]`: the Blob layout's owner tables are bounded
    // by the LAYOUT, not by a scope component in the key, so the serving scope is
    // written into the key text here rather than being a tuple element.
    const BLOB_ROWS: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_blobs");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("owner-source.redb");
    let destination = dir.path().join("owner-backup.redb");
    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let scope = eg_storage::ledger_scope_key(&identity);
    {
        let fixture = Fixture::create::<BlobOwner>(&path, "physical:blob:test", None);
        let owner =
            fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());
        let owner_batch = batch(identity.clone(), "owner-payload");
        let (write, begun) = fixture.mutations.admit(&owner, &owner_batch).unwrap();
        let source_version = match begun {
            Begin::Apply { source_version } => source_version,
            Begin::Replay(_) => panic!("unexpected replay"),
        };
        let rows = write.owner_rows(&owner, &owner_batch).unwrap();
        rows.open_table(BLOB_ROWS)
            .unwrap()
            .insert(
                blob_key(scope.as_str(), "object").as_str(),
                b"domain-row".as_slice(),
            )
            .unwrap();
        rows.finish_owner().unwrap();
        fixture
            .mutations
            .finish(&write, &owner_batch, None, 2, source_version)
            .unwrap();
        fixture.mutations.commit(write, &owner_batch).unwrap();
        backup_recovery_store(&fixture.kernel, &destination).unwrap();
    }
    let copy = Fixture::open::<BlobOwner>(&destination, "physical:blob:test", None);
    let owner = copy.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity);
    let read = copy.kernel.read_scope(&owner).unwrap();
    assert!(read_ledger(&read, "owner-payload").unwrap().is_some());
    let table = read.open_owner_table(BLOB_ROWS).unwrap();
    assert_eq!(
        table
            .get(blob_key(scope.as_str(), "object").as_str())
            .unwrap()
            .expect("the backup must carry the owner row")
            .value(),
        b"domain-row"
    );
}
