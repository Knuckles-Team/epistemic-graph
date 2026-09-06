use crate::codec::encode_bounded;
use crate::kernel::{
    authenticate_scope_in, bind_serving_scope_in, create_physical, current_owner_manifest,
    open_physical,
};
use crate::owner::blob_shared::{
    authenticate_blob_shared_service, read_blob_shared, BlobSharedServiceVerifier,
};
use crate::owner::domain::{KvOwner, StatechartOwner};
use crate::owner::grant::ScopeGrantVerifier;
use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::OwnerLayout;
use crate::owner::table_api::{owner_table_access, OwnerTableAccess};
use crate::owner::validate_manifest_write;
use crate::physical::binding::{bind_scope_in, binding_for_read, ScopeBinding};
use crate::physical::incarnation::STORAGE_KERNEL_SCHEMA_VERSION;
use crate::physical::manifest::OwnerManifestDigest;
use crate::physical::read_only::open_read_only;
use crate::physical::root::PhysicalStore;
use crate::recovery::adopt::{
    adopt_recovery, adopt_staged_mutation_store, inspect_staged_mutation_store, open_recovery,
};
use crate::recovery::evidence::{backup_strict_recovery_store_of, strict_evidence_of};
use crate::recovery::evidence::strict_recovery_evidence;
use crate::recovery::validate::{validate_live_recovery_store, validate_recovery_store_read_only};
use crate::tables::{OWNER_MANIFEST, SCOPE_BINDINGS, STORE_ROOT, VERSIONS};
use eg_types::mutation_batch::{IncarnationId, LogicalName, MutationDomain, TenantId};
use eg_types::MutationScopeIdentity;
use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle,
};
use std::path::Path;

const TEST_PHYSICAL: &str = "physical:test:ledger-only";

struct TestScopeVerifier {
    tenant: &'static str,
    principal: &'static str,
    layout: OwnerLayout,
}

struct TestBlobSharedVerifier;

impl BlobSharedServiceVerifier for TestBlobSharedVerifier {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        (principal == "blob-service" && proof == b"verified")
            .then_some(())
            .ok_or_else(|| "test shared blob authority rejected".to_string())
    }
}

impl ScopeGrantVerifier for TestScopeVerifier {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        if layout != self.layout
            || identity.tenant().as_str() != self.tenant
            || principal != self.principal
            || proof != b"verified"
        {
            return Err("test scope authority rejected".to_string());
        }
        Ok(())
    }
}

fn native_identity(tenant: &str, incarnation: &str) -> MutationScopeIdentity {
    domain_identity(
        tenant,
        MutationDomain::BlobStore,
        "blob-catalog",
        incarnation,
    )
}

fn domain_identity(
    tenant: &str,
    domain: MutationDomain,
    resource: &str,
    incarnation: &str,
) -> MutationScopeIdentity {
    MutationScopeIdentity::native(
        TenantId::new(tenant).unwrap(),
        domain,
        LogicalName::new(resource).unwrap(),
        IncarnationId::new(incarnation).unwrap(),
    )
    .unwrap()
}

fn kv_identity(tenant: &str, incarnation: &str) -> MutationScopeIdentity {
    domain_identity(tenant, MutationDomain::KvStore, "kv-catalog", incarnation)
}

/// Create a ledger-only owner file and bind one serving scope to it.
fn open_store(path: &Path, identity: &MutationScopeIdentity) -> PhysicalStore {
    let store = create_physical(
        path,
        PhysicalStoreIdentity::new(TEST_PHYSICAL).unwrap(),
        None,
        OwnerLayout::LedgerOnly,
    )
    .unwrap();
    bind_test_scope(&store, identity, 0).unwrap();
    store
}

/// Reopen an existing ledger-only owner file.
fn reopen_store(path: &Path) -> Result<PhysicalStore, String> {
    open_physical(
        path,
        PhysicalStoreIdentity::new(TEST_PHYSICAL).unwrap(),
        None,
        OwnerLayout::LedgerOnly,
    )
}

fn bind_test_scope(
    store: &PhysicalStore,
    identity: &MutationScopeIdentity,
    initial_version: u64,
) -> Result<bool, String> {
    let transaction = store.begin_write()?;
    let inserted = bind_scope_in(&store.handle, &transaction, identity, initial_version)?;
    transaction.commit().map_err(|error| error.to_string())?;
    Ok(inserted)
}

fn owner_authority_digest(store: &PhysicalStore) -> Result<[u8; 32], String> {
    Ok(current_owner_manifest(store)?.authority_digest(store.incarnation()))
}

fn owner_manifest_digest(store: &PhysicalStore) -> Result<OwnerManifestDigest, String> {
    current_owner_manifest(store)?.digest()
}

fn read_binding_error(store: &PhysicalStore, identity: &MutationScopeIdentity) -> String {
    let transaction = match store.begin_read() {
        Ok(transaction) => transaction,
        Err(error) => return error,
    };
    binding_for_read(store, &transaction, identity)
        .expect_err("scoped read must reject a tampered root")
}

#[test]
fn physical_root_reentry_is_idempotent_but_initial_version_mismatch_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let first = open_store(&path, &identity);
    let digest = first.incarnation().identity_digest();
    drop(first);
    let second = reopen_store(&path).unwrap();
    assert_eq!(digest, second.incarnation().identity_digest());
    // Exact re-entry is idempotent: the binding already exists, so nothing is inserted.
    assert!(!bind_test_scope(&second, &identity, 0).unwrap());
    drop(second);
    let third = reopen_store(&path).unwrap();
    assert!(bind_test_scope(&third, &identity, 1).is_err());
}

#[test]
fn mismatched_rebinding_is_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let store = open_store(&dir.path().join("native.redb"), &identity);
    let error = bind_test_scope(&store, &identity, 7).unwrap_err();
    assert!(error.contains("rebinding mismatch"));
}

#[test]
fn partial_initialization_rolls_back_as_one_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let store = create_physical(
        &path,
        PhysicalStoreIdentity::new(TEST_PHYSICAL).unwrap(),
        None,
        OwnerLayout::LedgerOnly,
    )
    .unwrap();
    // A binding that is abandoned instead of committed leaves no partial row.
    let abandoned = store.begin_write().unwrap();
    bind_scope_in(&store.handle, &abandoned, &identity, 0).unwrap();
    abandoned.abort().unwrap();
    validate_live_recovery_store(&store).unwrap();
    assert_eq!(validate_live_recovery_store(&store).unwrap().scope_bindings, 0);
    bind_test_scope(&store, &identity, 0).unwrap();
    assert_eq!(validate_live_recovery_store(&store).unwrap().scope_bindings, 1);
}

#[test]
fn incompatible_prototype_tables_are_quarantined_without_translation() {
    // NOT `"mutation_batches"`: that bare name is still the live, in-service table
    // name of `epistemic-graph`'s own pre-`eg-mutation-store` per-graph-shard
    // mutation bookkeeping (`src/redb_store.rs`), which legitimately shares a
    // physical file with this crate's own tables (see `RETIRED_PROTOTYPE_TABLES`'s
    // doc comment in `identity.rs`), so it must NOT be treated as a retired
    // prototype of THIS crate. Use a name that is exclusively this crate's own
    // abandoned prototype schema instead.
    for table_name in ["mutation_versions", "mutation_batches_v3"] {
        let retired: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new(table_name);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prototype.redb");
        let db = Database::create(&path).unwrap();
        let wtx = db.begin_write().unwrap();
        wtx.open_table(retired).unwrap();
        wtx.commit().unwrap();
        drop(db);
        let error = reopen_store(&path).err().unwrap();
        assert!(error.contains("quarantine before serving"), "{error}");
    }
}

#[test]
fn persisted_root_digest_tampering_is_rejected_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let store = open_store(&dir.path().join("native.redb"), &identity);
    let mut bytes = encode_bounded(store.incarnation(), "test root").unwrap();
    *bytes.last_mut().unwrap() ^= 0x01;
    let wtx = store.database().begin_write().unwrap();
    wtx.open_table(STORE_ROOT)
        .unwrap()
        .insert("root", bytes.as_slice())
        .unwrap();
    wtx.commit().unwrap();
    assert!(read_binding_error(&store, &identity).contains("digest mismatch"));
}

#[test]
fn extra_root_rows_are_rejected_by_every_store_entry_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let store = open_store(&path, &identity);
    let bytes = encode_bounded(store.incarnation(), "test root").unwrap();
    let wtx = store.database().begin_write().unwrap();
    wtx.open_table(STORE_ROOT)
        .unwrap()
        .insert("shadow-root", bytes.as_slice())
        .unwrap();
    wtx.commit().unwrap();

    for error in [
        read_binding_error(&store, &identity),
        store
            .begin_write()
            .err()
            .expect("write must reject an extra root"),
        validate_live_recovery_store(&store).unwrap_err(),
    ] {
        assert!(error.contains("exactly one canonical root"), "{error}");
    }
    drop(store);

    let read_only = open_read_only(&path, None).unwrap();
    let error = validate_recovery_store_read_only(&read_only).unwrap_err();
    assert!(error.contains("exactly one canonical root"), "{error}");
    drop(read_only);

    let error = reopen_store(&path)
        .err()
        .expect("reopen must reject an extra root");
    assert!(error.contains("exactly one canonical root"), "{error}");
}

#[test]
fn strict_create_materializes_manifest_without_a_serving_binding() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("strict.redb");
    let physical = PhysicalStoreIdentity::new("physical:blob:test").unwrap();
    let store = create_physical(&path, physical.clone(), None, OwnerLayout::Blob).unwrap();

    let rtx = store.database().begin_read().unwrap();
    assert_eq!(rtx.open_table(OWNER_MANIFEST).unwrap().len().unwrap(), 1);
    assert_eq!(rtx.open_table(SCOPE_BINDINGS).unwrap().len().unwrap(), 0);
    drop(rtx);
    drop(store);

    assert!(open_physical(&path, physical.clone(), None, OwnerLayout::Kv).is_err());
    assert!(open_physical(
        &path,
        PhysicalStoreIdentity::new("physical:blob:other").unwrap(),
        None,
        OwnerLayout::Blob,
    )
    .is_err());
    open_physical(&path, physical, None, OwnerLayout::Blob).unwrap();
}

#[test]
fn strict_open_rejects_missing_or_extra_owner_manifest_rows() {
    for extra in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("strict.redb");
        let physical = PhysicalStoreIdentity::new("physical:blob:test").unwrap();
        let store = create_physical(&path, physical.clone(), None, OwnerLayout::Blob).unwrap();
        let wtx = store.database().begin_write().unwrap();
        let mut manifest = wtx.open_table(OWNER_MANIFEST).unwrap();
        if extra {
            let bytes = manifest.get("manifest").unwrap().unwrap().value().to_vec();
            manifest.insert("shadow", bytes.as_slice()).unwrap();
        } else {
            manifest.remove("manifest").unwrap();
        }
        drop(manifest);
        wtx.commit().unwrap();
        drop(store);

        let error = open_physical(&path, physical, None, OwnerLayout::Blob)
            .err()
            .expect("invalid owner manifest must fail closed");
        assert!(error.contains("manifest"), "{error}");
    }
}

#[test]
fn typed_recovery_reanchors_physical_authority_once() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.redb");
    let destination_path = dir.path().join("destination.redb");
    let source_identity = PhysicalStoreIdentity::new("physical:blob:source").unwrap();
    let destination_identity = PhysicalStoreIdentity::new("physical:blob:destination").unwrap();
    let store = create_physical(
        &source_path,
        source_identity.clone(),
        None,
        OwnerLayout::Blob,
    )
    .unwrap();
    drop(store);
    std::fs::copy(&source_path, &destination_path).unwrap();

    let token = open_recovery(&destination_path, source_identity, None, OwnerLayout::Blob).unwrap();
    let adopted = adopt_recovery(token, destination_identity.clone()).unwrap();
    drop(adopted);
    open_physical(
        &destination_path,
        destination_identity,
        None,
        OwnerLayout::Blob,
    )
    .unwrap();
}

#[test]
fn owner_row_crud_is_reserved_for_authenticated_domain_services() {
    for layout in [
        OwnerLayout::Rbac,
        OwnerLayout::Jobs,
        OwnerLayout::Statechart,
        OwnerLayout::TimeSeries,
        OwnerLayout::Kv,
        OwnerLayout::Blob,
        OwnerLayout::SemanticIndex,
    ] {
        for table in crate::owner::registry::owner_table_names(layout) {
            assert!(matches!(
                owner_table_access(table),
                OwnerTableAccess::DomainService | OwnerTableAccess::SharedService
            ));
        }
    }
}

#[test]
fn adoption_streams_more_than_one_hundred_thousand_scope_bindings() {
    const BINDING_ROWS: u64 = 100_001;

    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source-many-bindings.redb");
    let destination_path = dir.path().join("destination-many-bindings.redb");
    let source_identity = PhysicalStoreIdentity::new("physical:many-bindings:source").unwrap();
    let destination_identity =
        PhysicalStoreIdentity::new("physical:many-bindings:destination").unwrap();
    let store = create_physical(
        &source_path,
        source_identity.clone(),
        None,
        OwnerLayout::LedgerOnly,
    )
    .unwrap();
    let write = store.database().begin_write().unwrap();
    {
        let mut bindings = write.open_table(SCOPE_BINDINGS).unwrap();
        let mut versions = write.open_table(VERSIONS).unwrap();
        for ordinal in 0..BINDING_ROWS {
            let identity = MutationScopeIdentity::native(
                TenantId::new("tenant-many-bindings").unwrap(),
                MutationDomain::BlobStore,
                LogicalName::new(format!("bulk-binding-{ordinal}")).unwrap(),
                IncarnationId::new("incarnation:bulk-binding:test").unwrap(),
            )
            .unwrap();
            let key = identity.binding_digest().to_hex();
            let binding = ScopeBinding {
                schema_version: STORAGE_KERNEL_SCHEMA_VERSION,
                store_identity_digest: store.incarnation().identity_digest(),
                identity,
                initial_version: ordinal,
            };
            let encoded = encode_bounded(&binding, "bulk scope binding").unwrap();
            bindings.insert(key.as_str(), encoded.as_slice()).unwrap();
            versions.insert(key.as_str(), ordinal).unwrap();
        }
    }
    write.commit().unwrap();
    drop(store);
    std::fs::copy(&source_path, &destination_path).unwrap();

    let token = open_recovery(
        &destination_path,
        source_identity,
        None,
        OwnerLayout::LedgerOnly,
    )
    .unwrap();
    let adopted = adopt_recovery(token, destination_identity).unwrap();
    let evidence = strict_recovery_evidence(&adopted).unwrap();
    assert_eq!(
        evidence
            .tables
            .iter()
            .find(|table| table.table_id == SCOPE_BINDINGS.name())
            .unwrap()
            .rows,
        BINDING_ROWS
    );
}

#[test]
fn strict_backup_copies_owner_rows_and_reopens_with_stable_evidence() {
    const KV_ROWS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("kv");
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source-kv.redb");
    let destination_path = dir.path().join("destination-kv.redb");
    let second_destination_path = dir.path().join("destination-kv-epoch-two.redb");
    let source_physical = PhysicalStoreIdentity::new("physical:kv:source").unwrap();
    let destination_physical = PhysicalStoreIdentity::new("physical:kv:destination").unwrap();
    let second_destination_physical =
        PhysicalStoreIdentity::new("physical:kv:destination:epoch-two").unwrap();
    let store = create_physical(&source_path, source_physical, None, OwnerLayout::Kv).unwrap();
    let source_authority = owner_authority_digest(&store).unwrap();
    let write = store.database().begin_write().unwrap();
    write
        .open_table(KV_ROWS)
        .unwrap()
        .insert(("namespace-a", "doc-a"), b"value-a".as_slice())
        .unwrap();
    write.commit().unwrap();

    let evidence =
        backup_strict_recovery_store_of(&store, &destination_path, destination_physical.clone())
            .unwrap();
    assert_eq!(evidence.owner_rows, 1);
    // 18 ledger tables + the Kv layout's `kv` and `eg_kvcache_cold`.
    assert_eq!(evidence.tables.len(), 20);
    assert_eq!(
        evidence
            .tables
            .iter()
            .find(|table| table.table_id == "kv")
            .unwrap()
            .rows,
        1
    );
    let reopened = open_physical(
        &destination_path,
        destination_physical,
        None,
        OwnerLayout::Kv,
    )
    .unwrap();
    assert_ne!(owner_authority_digest(&reopened).unwrap(), source_authority);
    assert_eq!(reopened.manifest().authority_epoch, 1);
    assert_eq!(strict_evidence_of(&reopened).unwrap(), evidence);
    let second_evidence = backup_strict_recovery_store_of(
        &reopened,
        &second_destination_path,
        second_destination_physical.clone(),
    )
    .unwrap();
    let second_reopened = open_physical(
        &second_destination_path,
        second_destination_physical,
        None,
        OwnerLayout::Kv,
    )
    .unwrap();
    assert_eq!(
        second_reopened.manifest().authority_epoch,
        2
    );
    assert_eq!(
        strict_evidence_of(&second_reopened).unwrap(),
        second_evidence
    );
    let read = second_reopened.database.begin_read().unwrap();
    let table = read.open_table(KV_ROWS).unwrap();
    assert_eq!(
        table
            .get(("namespace-a", "doc-a"))
            .unwrap()
            .unwrap()
            .value(),
        b"value-a"
    );
}

#[test]
fn cached_manifest_epoch_is_revalidated_before_owner_authority_use() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stale-manifest.redb");
    let store = create_physical(
        &path,
        PhysicalStoreIdentity::new("physical:stale-manifest").unwrap(),
        None,
        OwnerLayout::Kv,
    )
    .unwrap();
    let principal = format!("principal:sha256:{}", "a".repeat(64));
    let verifier = TestScopeVerifier {
        tenant: "tenant-a",
        principal:
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        layout: OwnerLayout::Kv,
    };
    let identity = kv_identity("tenant-a", "incarnation:stale-manifest");
    let _owner = bind_serving_scope_in(
        &store,
        authenticate_scope_in::<KvOwner>(
            &store,
            &verifier,
            identity.clone(),
            principal.clone(),
            b"verified",
        )
        .unwrap(),
        0,
    )
    .unwrap();

    let write = store.database().begin_write().unwrap();
    let mut manifest = validate_manifest_write(
        &write,
        &store.manifest().physical_identity,
        OwnerLayout::Kv,
    )
    .unwrap();
    manifest.authority_epoch += 1;
    let encoded = encode_bounded(&manifest, "tampered authority epoch").unwrap();
    write
        .open_table(OWNER_MANIFEST)
        .unwrap()
        .insert("manifest", encoded.as_slice())
        .unwrap();
    write.commit().unwrap();

    assert!(store.begin_write().is_err());
    assert!(owner_authority_digest(&store).is_err());
    assert!(
        authenticate_scope_in::<KvOwner>(&store, &verifier, identity, principal, b"verified")
            .is_err()
    );
}

#[test]
fn shared_blob_read_revalidates_root_and_closed_table_registry() {
    for extra_root in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared-read-tamper.redb");
        let store = create_physical(
            &path,
            PhysicalStoreIdentity::new("physical:shared-read-tamper").unwrap(),
            None,
            OwnerLayout::Blob,
        )
        .unwrap();
        let owner = authenticate_blob_shared_service(
            &store,
            &TestBlobSharedVerifier,
            "blob-service".to_string(),
            b"verified",
        )
        .unwrap();
        let write = store.database().begin_write().unwrap();
        if extra_root {
            write
                .open_table(STORE_ROOT)
                .unwrap()
                .insert("shadow", b"tamper".as_slice())
                .unwrap();
        } else {
            let unknown: TableDefinition<&str, &str> =
                TableDefinition::new("unknown_shared_blob_table");
            write.open_table(unknown).unwrap();
        }
        write.commit().unwrap();

        assert!(read_blob_shared(&store, &owner, "blob-service").is_err());
    }
}

#[test]
fn staged_adoption_allows_absent_integrity_only_when_private_rows_are_empty() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("empty-private-source.redb");
    let staged_path = dir.path().join("empty-private-target.redb");
    let physical = PhysicalStoreIdentity::new("physical:empty-private").unwrap();
    let store = create_physical(
        &source_path,
        physical.clone(),
        None,
        OwnerLayout::LedgerOnly,
    )
    .unwrap();
    let expected_manifest = owner_manifest_digest(&store).unwrap();
    drop(store);
    std::fs::copy(source_path, &staged_path).unwrap();

    let staged =
        inspect_staged_mutation_store(&staged_path, physical, OwnerLayout::LedgerOnly, None)
            .unwrap();
    assert_eq!(staged.owner_manifest_digest(), expected_manifest);
    let adopted = adopt_staged_mutation_store(staged).unwrap();
    assert_eq!(adopted.owner_manifest_digest().unwrap(), expected_manifest);
}

#[test]
fn statechart_staged_inspection_requires_its_one_fixed_serving_scope() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("statechart-staged.redb");
    let physical = PhysicalStoreIdentity::new("physical:statechart:staged").unwrap();
    let store = create_physical(&path, physical.clone(), None, OwnerLayout::Statechart).unwrap();
    drop(store);
    assert!(
        inspect_staged_mutation_store(&path, physical.clone(), OwnerLayout::Statechart, None,)
            .is_err()
    );
    let store = open_physical(&path, physical.clone(), None, OwnerLayout::Statechart).unwrap();

    let principal = format!("principal:sha256:{}", "a".repeat(64));
    let verifier = TestScopeVerifier {
        tenant: "native",
        principal:
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        layout: OwnerLayout::Statechart,
    };
    let fixed = domain_identity(
        "native",
        MutationDomain::Lifecycle,
        "statechart-instances",
        "incarnation:eg-statechart:statechart-instances:1",
    );
    bind_serving_scope_in(
        &store,
        authenticate_scope_in::<StatechartOwner>(
            &store,
            &verifier,
            fixed,
            principal.clone(),
            b"verified",
        )
        .unwrap(),
        7,
    )
    .unwrap();
    drop(store);
    let token =
        inspect_staged_mutation_store(&path, physical.clone(), OwnerLayout::Statechart, None)
            .unwrap();
    assert_eq!(token.recovery_counts().scope_bindings, 1);
    assert_eq!(token.recovery_counts().versions, 1);
    drop(token);

    let store = open_physical(&path, physical.clone(), None, OwnerLayout::Statechart).unwrap();
    let extra = domain_identity(
        "native",
        MutationDomain::Lifecycle,
        "statechart-other",
        "incarnation:eg-statechart:statechart-other:1",
    );
    bind_serving_scope_in(
        &store,
        authenticate_scope_in::<StatechartOwner>(&store, &verifier, extra, principal, b"verified")
            .unwrap(),
        0,
    )
    .unwrap();
    drop(store);
    assert!(
        inspect_staged_mutation_store(&path, physical, OwnerLayout::Statechart, None,).is_err()
    );
}

#[test]
fn staged_adoption_token_rejects_post_inspection_change() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("changed-after-inspection.redb");
    let physical = PhysicalStoreIdentity::new("physical:changed-after-inspection").unwrap();
    let store = create_physical(&path, physical.clone(), None, OwnerLayout::LedgerOnly).unwrap();
    drop(store);
    let staged =
        inspect_staged_mutation_store(&path, physical, OwnerLayout::LedgerOnly, None).unwrap();
    let database = Database::open(&path).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(STORE_ROOT)
        .unwrap()
        .insert("shadow", b"changed".as_slice())
        .unwrap();
    write.commit().unwrap();
    drop(database);

    assert!(adopt_staged_mutation_store(staged).is_err());
}
