//! A capability serves exactly one scope.
//!
//! One physical owner file deliberately serves many scopes
//! (`one_physical_root_serves_multiple_scopes_without_cross_tenant_aliasing`),
//! and `OwnerLayout::accepts` gates on the mutation *domain*, never on tenant.
//! Key non-collision is therefore not confinement: these cases prove that a
//! handle for tenant A cannot reach, purge, retire or read tenant B's rows.

use super::*;
use crate::admitted::AdmittedMutation;
use crate::tables::BATCHES;
use eg_storage::{ledger_scope_key, BlobOwner};
use redb::TableDefinition;

struct Tenants {
    fixture: Fixture,
    a: OwnedStoreHandle<LedgerOnlyOwner>,
    b: OwnedStoreHandle<LedgerOnlyOwner>,
    identity_a: MutationScopeIdentity,
    identity_b: MutationScopeIdentity,
}

/// Two tenants bound to one physical file, each with one committed batch.
fn two_tenants(path: &Path) -> Tenants {
    let identity_a = native_identity("tenant-a", "incarnation:blob:a");
    let identity_b = native_identity("tenant-b", "incarnation:blob:b");
    let (fixture, a) = ledger_fixture(path, identity_a.clone());
    let b = fixture.bind::<LedgerOnlyOwner>(
        &verifier("tenant-b", OwnerLayout::LedgerOnly),
        identity_b.clone(),
    );
    apply_batch(&fixture, &a, &batch(identity_a.clone(), "batch-a"));
    apply_batch(&fixture, &b, &batch(identity_b.clone(), "batch-b"));
    Tenants {
        fixture,
        a,
        b,
        identity_a,
        identity_b,
    }
}

#[test]
fn a_capability_cannot_purge_or_retire_another_tenants_scope() {
    let dir = tempfile::tempdir().unwrap();
    let tenants = two_tenants(&dir.path().join("shared.redb"));

    // Holding A's handle, name B's identity.
    assert!(tenants
        .fixture
        .mutations
        .purge_scope(&tenants.a, &tenants.identity_b)
        .unwrap_err()
        .contains("does not serve this scope"));

    // The direct capability method takes no scope at all, so B's binding and
    // version row cannot even be named through A's write.
    let write = AdmittedMutation::open(tenants.fixture.mutations_authority(), &tenants.a).unwrap();
    write.retire_scope_binding().unwrap();
    write.abort().unwrap();

    // B is intact: its receipt, its version row and its binding all survive.
    let read_b = tenants.fixture.kernel.read_scope(&tenants.b).unwrap();
    assert!(read_ledger(&read_b, "batch-b").unwrap().is_some());
    assert_eq!(version(&read_b).unwrap(), 1);

    // And A purging its own scope still works.
    tenants
        .fixture
        .mutations
        .purge_scope(&tenants.a, &tenants.identity_a)
        .unwrap();
    let read_b = tenants.fixture.kernel.read_scope(&tenants.b).unwrap();
    assert!(read_ledger(&read_b, "batch-b").unwrap().is_some());
}

#[test]
fn a_scoped_read_cannot_address_another_tenants_rows() {
    let dir = tempfile::tempdir().unwrap();
    let tenants = two_tenants(&dir.path().join("shared.redb"));
    let read_a = tenants.fixture.kernel.read_scope(&tenants.a).unwrap();
    let key_a = ledger_scope_key(&tenants.identity_a);
    let key_b = ledger_scope_key(&tenants.identity_b);
    let batches = read_a.scoped_table(BATCHES).unwrap();

    assert_eq!(batches.scope_key(), key_a.as_str());
    assert!(batches.get((key_a.as_str(), "batch-a")).unwrap().is_some());
    match batches.get((key_b.as_str(), "batch-b")) {
        Ok(_) => panic!("a scoped read must not address another scope"),
        Err(error) => assert!(error.contains("another scope's rows"), "{error}"),
    }
    match batches.range_inclusive((key_b.as_str(), ""), (key_b.as_str(), "~")) {
        Ok(_) => panic!("a scoped range must not address another scope"),
        Err(error) => assert!(error.contains("another scope's rows"), "{error}"),
    }

    // The typed readers only ever see A's own rows.
    assert!(read_ledger(&read_a, "batch-b").unwrap().is_none());
    assert_eq!(crate::read::read_batches(&read_a).unwrap().len(), 1);
}

/// The write side has the same row-level bound the read side got: a capability
/// for A cannot reach, overwrite or delete B's ledger rows through any public
/// path. `open_table` no longer exists on the write capability at all.
#[test]
fn a_scoped_write_cannot_touch_another_tenants_ledger_rows() {
    let dir = tempfile::tempdir().unwrap();
    let tenants = two_tenants(&dir.path().join("shared.redb"));
    let key_a = ledger_scope_key(&tenants.identity_a);
    let key_b = ledger_scope_key(&tenants.identity_b);
    let write = AdmittedMutation::open(tenants.fixture.mutations_authority(), &tenants.a).unwrap();
    let mut batches = write.scoped_table(BATCHES).unwrap();

    assert_eq!(batches.scope_key(), key_a.as_str());
    assert!(batches.get((key_a.as_str(), "batch-a")).unwrap().is_some());
    for outcome in [
        batches.remove((key_b.as_str(), "batch-b")),
        batches.insert((key_b.as_str(), "batch-b"), b"forged".as_slice()),
    ] {
        match outcome {
            Ok(()) => panic!("a scoped write must not address another scope"),
            Err(error) => assert!(error.contains("another scope's rows"), "{error}"),
        }
    }
    match batches.get((key_b.as_str(), "batch-b")) {
        Ok(_) => panic!("a scoped write must not read another scope"),
        Err(error) => assert!(error.contains("another scope's rows"), "{error}"),
    }
    drop(batches);
    write.abort().unwrap();

    let read_b = tenants.fixture.kernel.read_scope(&tenants.b).unwrap();
    assert!(read_ledger(&read_b, "batch-b").unwrap().is_some());
}

/// A layout that owns tables cannot retire a generation's authority without
/// retiring its payload: owner keys carry no scope component, so the rows would
/// be read by the next binding of the same logical name as its own.
#[test]
fn purging_a_layout_with_owner_tables_requires_an_owner_payload_retirement() {
    // `cas_blobs` is `&str -> &[u8]`: the Blob layout's owner tables are bounded
    // by the LAYOUT, not by a scope component in the key, so the serving scope is
    // written into the key text here rather than being a tuple element.
    const BLOB_ROWS: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_blobs");

    struct BlobRetirement;
    impl eg_storage::OwnerPayloadRetirement<BlobOwner> for BlobRetirement {
        fn retire_owner_payload(
            &self,
            write: &eg_storage::PhysicalWriteCapability<'_, BlobOwner>,
            scope: &MutationScopeIdentity,
        ) -> Result<(), String> {
            let key = ledger_scope_key(scope);
            let mut rows = write.open_owner_write(BLOB_ROWS)?;
            // The scope lives in the key TEXT, not in a tuple element, so the
            // sweep is a prefix match on the same `blob_key` shape.
            let prefix = format!("{key}|");
            rows.retain(|row, _| !row.starts_with(&prefix))
                .map_err(|error| error.to_string())
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blob.redb");
    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let scope = ledger_scope_key(&identity);
    let fixture = Fixture::create::<BlobOwner>(&path, "physical:blob:test", None);
    let owner =
        fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());
    let seeded = batch(identity.clone(), "payload");
    let (write, begun) = fixture.mutations.admit(&owner, &seeded).unwrap();
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    let rows = write.owner_rows(&owner, &seeded).unwrap();
    rows.open_table(BLOB_ROWS)
        .unwrap()
        .insert(
            blob_key(scope.as_str(), "object").as_str(),
            b"payload".as_slice(),
        )
        .unwrap();
    rows.finish_owner().unwrap();
    fixture
        .mutations
        .finish(&write, &seeded, None, 2, source_version)
        .unwrap();
    fixture.mutations.commit(write, &seeded).unwrap();

    // Refused without a payload retirement.
    assert!(fixture
        .mutations
        .purge_scope(&owner, &identity)
        .unwrap_err()
        .contains("owner-payload retirement"));
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert!(read_ledger(&read, "payload").unwrap().is_some());
    drop(read);

    // With one, ledger and payload retire together.
    fixture
        .mutations
        .purge_scope_with(&owner, &identity, &BlobRetirement)
        .unwrap();
    let rebound = fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity);
    let read = fixture.kernel.read_scope(&rebound).unwrap();
    assert!(read_ledger(&read, "payload").unwrap().is_none());
    assert!(read
        .open_owner_table(BLOB_ROWS)
        .unwrap()
        .get(blob_key(scope.as_str(), "object").as_str())
        .unwrap()
        .is_none());
}

/// The shared-service CAS write is reachable from an admitted mutation and from
/// an open owner-row admission, and it is the SAME transaction: a chunk row,
/// the refcount that accounts for it and the batch's own `cas_blobs` row commit
/// together, or a failed batch leaves none of them.
#[test]
fn a_shared_chunk_and_its_refcount_commit_with_the_batch_or_not_at_all() {
    const BLOB_ROWS: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_blobs");
    const DIGEST: &str = "aa11bb22cc33dd44ee55ff6600778899aa11bb22cc33dd44ee55ff6600778899";

    struct SharedVerifier;
    impl eg_storage::BlobSharedServiceVerifier for SharedVerifier {
        fn verify(
            &self,
            _physical: &eg_storage::PhysicalStoreIdentity,
            principal: &str,
            proof: &[u8],
        ) -> Result<(), String> {
            (principal == "blob-service" && proof == b"verified")
                .then_some(())
                .ok_or_else(|| "test shared blob authority rejected".to_string())
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blob.redb");
    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let fixture = Fixture::create::<BlobOwner>(&path, "physical:blob:shared", None);
    let service = fixture
        .kernel
        .authenticate_blob_shared_service(&SharedVerifier, "blob-service".to_string(), b"verified")
        .unwrap();
    let owner =
        fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());

    // A batch that writes an owner row AND the shared CAS rows, then fails.
    let failed = batch(identity.clone(), "batch-failed");
    let (write, _) = fixture.mutations.admit(&owner, &failed).unwrap();
    {
        let rows = write.owner_rows(&owner, &failed).unwrap();
        rows.open_table(BLOB_ROWS)
            .unwrap()
            .insert(blob_key("scope", "object").as_str(), b"manifest".as_slice())
            .unwrap();
        let shared = rows.blob_shared_write(&service, "blob-service").unwrap();
        assert!(shared.insert_chunk_if_absent(DIGEST, b"chunk").unwrap());
        assert_eq!(shared.adjust_refcount(DIGEST, 1).unwrap(), 1);
        rows.finish_owner().unwrap();
    }
    write.abort().unwrap();

    let read = fixture.kernel.read_blob_shared(&service, "blob-service").unwrap();
    assert!(!read.chunk_present(DIGEST).unwrap());
    assert_eq!(read.refcount(DIGEST).unwrap(), 0);
    drop(read);
    let scoped = fixture.kernel.read_scope(&owner).unwrap();
    assert!(scoped
        .open_owner_table(BLOB_ROWS)
        .unwrap()
        .get(blob_key("scope", "object").as_str())
        .unwrap()
        .is_none());
    drop(scoped);

    // The same shape, committed: every row of the batch lands together.
    let applied = batch(identity.clone(), "batch-applied");
    let (write, begun) = fixture.mutations.admit(&owner, &applied).unwrap();
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    {
        let shared = write.blob_shared_write(&service, "blob-service").unwrap();
        assert!(shared.insert_chunk_if_absent(DIGEST, b"chunk").unwrap());
        assert_eq!(shared.adjust_refcount(DIGEST, 1).unwrap(), 1);
    }
    let rows = write.owner_rows(&owner, &applied).unwrap();
    rows.open_table(BLOB_ROWS)
        .unwrap()
        .insert(blob_key("scope", "object").as_str(), b"manifest".as_slice())
        .unwrap();
    rows.finish_owner().unwrap();
    fixture
        .mutations
        .finish(&write, &applied, None, 2, source_version)
        .unwrap();
    fixture.mutations.commit(write, &applied).unwrap();

    let read = fixture.kernel.read_blob_shared(&service, "blob-service").unwrap();
    assert!(read.chunk_present(DIGEST).unwrap());
    assert_eq!(read.refcount(DIGEST).unwrap(), 1);
    assert_eq!(read.chunk_bytes(DIGEST).unwrap().as_deref(), Some(&b"chunk"[..]));
}
