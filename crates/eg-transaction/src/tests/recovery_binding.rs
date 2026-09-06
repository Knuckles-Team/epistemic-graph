//! The ledger-row/scope-binding contract, proved on known-bad inputs.
//!
//! Format v2's whole claim is that a ledger row resolves its own binding by its
//! own key and then must match the stamped identity. These cases corrupt
//! exactly one of those two halves and require the store to refuse to reopen.

use super::*;
use crate::admitted::AdmittedMutation;
use crate::tables::BATCHES;
use eg_storage::{encode_bounded, ledger_scope_key, ScopeFence};

/// Reopening must run the recovery-content check, so a corrupted row is
/// rejected at open rather than served.
fn reopen_error(path: &Path) -> String {
    match StorageKernelV1::open_owner::<LedgerOnlyOwner>(
        path,
        PhysicalStoreIdentity::new("physical:test:ledger-only").unwrap(),
        None,
    ) {
        Ok(_) => panic!("a corrupted ledger row must not reopen"),
        Err(error) => error,
    }
}

#[test]
fn a_committed_store_reopens_and_revalidates_every_ledger_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:bound");
    {
        let (fixture, owner) = ledger_fixture(&path, identity.clone());
        apply_batch(&fixture, &owner, &batch(identity.clone(), "bound-batch"));
    }
    let fixture = Fixture::open::<LedgerOnlyOwner>(&path, "physical:test:ledger-only", None);
    let owner = fixture.bind::<LedgerOnlyOwner>(
        &verifier("tenant-a", OwnerLayout::LedgerOnly),
        identity,
    );
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert!(read_ledger(&read, "bound-batch").unwrap().is_some());
    assert!(crate::read::read_fences(&read).unwrap().is_some());
}

#[test]
fn a_fence_stamped_with_a_foreign_identity_fails_to_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:fence-stamp");
    let foreign = native_identity("tenant-a", "incarnation:other-generation");
    {
        let (fixture, owner) = ledger_fixture(&path, identity.clone());
        apply_batch(&fixture, &owner, &batch(identity.clone(), "stamped-batch"));
        let write = AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
        let forged = ScopeFence {
            identity: foreign,
            placement_epoch: 0,
            fencing_token: 0,
        };
        let bytes = encode_bounded(&forged, "mutation fence").unwrap();
        let key = ledger_scope_key(&identity);
        write
            .transaction()
            .open_table(FENCES)
            .unwrap()
            .insert(key.as_str(), bytes.as_slice())
            .unwrap();
        write.commit().unwrap();
    }
    assert!(reopen_error(&path).contains("not stamped with its bound identity"));
}

#[test]
fn a_batch_row_under_an_unbound_scope_key_fails_to_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:unbound-key");
    {
        let (fixture, owner) = ledger_fixture(&path, identity.clone());
        let committed = batch(identity.clone(), "misfiled-batch");
        apply_batch(&fixture, &owner, &committed);
        let record = {
            let read = fixture.kernel.read_scope(&owner).unwrap();
            read_ledger(&read, "misfiled-batch").unwrap().unwrap()
        };
        let write = AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
        let bytes = encode_bounded(&record, "mutation batch record").unwrap();
        write
            .transaction()
            .open_table(BATCHES)
            .unwrap()
            .insert(
                (record.identity.identity_digest().to_hex().as_str(), "misfiled-batch"),
                bytes.as_slice(),
            )
            .unwrap();
        write.commit().unwrap();
    }
    assert!(reopen_error(&path).contains("unbound identity"));
}
