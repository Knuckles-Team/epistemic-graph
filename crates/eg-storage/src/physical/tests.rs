use super::*;
use eg_types::mutation_batch::{
    IncarnationId, LogicalName, MutationDomain, MutationRequestContext, MutationSurface, TenantId,
    VersionExpectation,
};
use eg_types::protocol::Method;
use eg_types::{MutationOperation, MUTATION_BATCH_VERSION};
use redb::{ReadableTableMetadata, TableHandle};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct TestIntegrity;

impl PrivatePayloadIntegrity for TestIntegrity {
    fn authenticate(&self, sealed: &[u8], expected_digest: &str) -> Result<(), String> {
        let expected = format!("sealed:{expected_digest}");
        (sealed == expected.as_bytes())
            .then_some(())
            .ok_or_else(|| "test canonical integrity rejection".to_string())
    }
}

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

fn domain_batch(
    identity: MutationScopeIdentity,
    batch_id: &str,
    domain: MutationDomain,
) -> MutationBatch {
    let mut batch = batch(identity, batch_id);
    batch.operations[0].domain = domain;
    batch
}

fn batch(identity: MutationScopeIdentity, batch_id: &str) -> MutationBatch {
    MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.to_string(),
        context: MutationRequestContext {
            request_id: 1,
            principal: format!("principal:sha256:{}", "a".repeat(64)),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            verified_capabilities: BTreeSet::new(),
        },
        identity,
        placement_epoch: 0,
        idempotency_key: format!("retry-{batch_id}"),
        version_expectation: VersionExpectation::Native(0),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Other,
            domain: MutationDomain::BlobStore,
            method: Method::ApplyMutation {
                event_type: "blob_test".to_string(),
                query: "opaque".to_string(),
            },
        }],
        outbox: Vec::new(),
        created_at_ms: 1,
    }
}

fn recovery_batch(identity: MutationScopeIdentity, batch_id: &str) -> (MutationBatch, Vec<u8>) {
    let digest = "b".repeat(64);
    let mut batch = batch(identity, batch_id);
    batch.operations[0].method = Method::ApplyMutation {
        event_type: "transaction_recovery_plan".to_string(),
        query: format!("sha256:{digest}"),
    };
    (batch, format!("sealed:{digest}").into_bytes())
}

fn open_store(path: &std::path::Path, identity: &MutationScopeIdentity) -> MutationStore {
    initialize(path, identity, 0, None, |_| Ok(())).unwrap()
}

fn apply_batch(store: &MutationStore, batch: &MutationBatch) {
    let write = store.write().unwrap();
    let source_version = match begin(&write, batch).unwrap() {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    finish(&write, batch, None, 2, source_version).unwrap();
    commit(write, batch).unwrap();
}

#[test]
fn physical_root_reentry_is_idempotent_but_initial_version_mismatch_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let bootstrap_calls = Arc::new(AtomicUsize::new(0));
    let first_calls = Arc::clone(&bootstrap_calls);
    let first = initialize(&path, &identity, 0, None, move |_| {
        first_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    let digest = first.incarnation().identity_digest();
    drop(first);
    let second_calls = Arc::clone(&bootstrap_calls);
    let second = initialize(&path, &identity, 0, None, move |_| {
        second_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    assert_eq!(digest, second.incarnation().identity_digest());
    assert_eq!(bootstrap_calls.load(Ordering::SeqCst), 1);
    drop(second);
    assert!(initialize(&path, &identity, 1, None, |_| Ok(())).is_err());
}

#[test]
fn mismatched_rebinding_is_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let store = open_store(&dir.path().join("native.redb"), &identity);
    let error = bind_scope(&store, &identity, 7, |_| Ok(())).unwrap_err();
    assert!(error.contains("rebinding mismatch"));
}

#[test]
fn partial_initialization_rolls_back_as_one_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    assert!(initialize(&path, &identity, 0, None, |_| Err(
        "owner bootstrap failed".into()
    ))
    .is_err());
    let replacement = open_store(&path, &identity);
    validate_recovery_store(&replacement).unwrap();
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
        let error = initialize(
            &path,
            &native_identity("tenant-a", "incarnation:blob:1"),
            0,
            None,
            |_| Ok(()),
        )
        .err()
        .unwrap();
        assert!(error.contains("quarantine before serving"));
    }
}

#[test]
fn one_physical_root_serves_multiple_scopes_without_cross_tenant_aliasing() {
    let dir = tempfile::tempdir().unwrap();
    let tenant_a = native_identity("tenant-a", "incarnation:blob:a");
    let tenant_b = native_identity("tenant-b", "incarnation:blob:b");
    let store = open_store(&dir.path().join("native.redb"), &tenant_a);
    bind_scope(&store, &tenant_b, 0, |_| Ok(())).unwrap();

    apply_batch(&store, &batch(tenant_a.clone(), "same-batch-id"));
    apply_batch(&store, &batch(tenant_b.clone(), "same-batch-id"));

    assert!(read_record(&store, &tenant_a, "same-batch-id")
        .unwrap()
        .is_some());
    assert!(read_record(&store, &tenant_b, "same-batch-id")
        .unwrap()
        .is_some());
    assert_eq!(version(&store, &tenant_a).unwrap(), 1);
    assert_eq!(version(&store, &tenant_b).unwrap(), 1);
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
    assert!(version(&store, &identity)
        .unwrap_err()
        .contains("digest mismatch"));
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
        version(&store, &identity).unwrap_err(),
        store
            .write()
            .err()
            .expect("write must reject an extra root"),
        validate_recovery_store(&store).unwrap_err(),
    ] {
        assert!(error.contains("exactly one canonical root"), "{error}");
    }
    drop(store);

    let read_only = open_read_only(&path, None).unwrap();
    let error = validate_recovery_store_read_only(&read_only).unwrap_err();
    assert!(error.contains("exactly one canonical root"), "{error}");
    drop(read_only);

    let error = initialize(&path, &identity, 0, None, |_| Ok(()))
        .err()
        .expect("reopen must reject an extra root");
    assert!(error.contains("exactly one canonical root"), "{error}");

    let error = adopt_restored_store(&path, None)
        .err()
        .expect("adoption must reject an extra root");
    assert!(error.contains("exactly one canonical root"), "{error}");
}

#[test]
fn backup_derives_a_distinct_physical_root_and_rebinds_scopes() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let source = open_store(&dir.path().join("source.redb"), &identity);
    apply_batch(&source, &batch(identity.clone(), "backup-batch"));
    let destination = dir.path().join("backup.redb");
    backup_recovery_store(&source, &destination).unwrap();
    drop(source);
    let backup = open_store(&destination, &identity);
    assert!(read_record(&backup, &identity, "backup-batch")
        .unwrap()
        .is_some());
}

#[test]
fn private_recovery_authenticity_uses_injected_canonical_authority() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let store = initialize(
        &dir.path().join("native.redb"),
        &identity,
        0,
        Some(Arc::new(TestIntegrity)),
        |_| Ok(()),
    )
    .unwrap();
    let (batch, sealed) = recovery_batch(identity.clone(), "recovery-batch");
    prepare_saga_with_private_payload(&store, &batch, 2, Some(&sealed)).unwrap();
    assert_eq!(
        read_private_payload(&store, &identity, &batch.batch_id).unwrap(),
        Some(sealed)
    );
}

#[test]
fn forged_private_recovery_payload_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let store = initialize(
        &dir.path().join("native.redb"),
        &identity,
        0,
        Some(Arc::new(TestIntegrity)),
        |_| Ok(()),
    )
    .unwrap();
    let (batch, _) = recovery_batch(identity, "forged-recovery");
    assert!(prepare_saga_with_private_payload(&store, &batch, 2, Some(b"\xe6forged")).is_err());
}

#[test]
fn missing_private_integrity_authority_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let store = open_store(&dir.path().join("native.redb"), &identity);
    let (batch, sealed) = recovery_batch(identity, "missing-integrity");
    assert!(prepare_saga_with_private_payload(&store, &batch, 2, Some(&sealed)).is_err());
}

#[test]
fn strict_create_materializes_manifest_without_a_serving_binding() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("strict.redb");
    let physical = PhysicalStoreIdentity::new("physical:blob:test").unwrap();
    let store = create(&path, physical.clone(), None, OwnerLayout::Blob).unwrap();

    let rtx = store.database().begin_read().unwrap();
    assert_eq!(rtx.open_table(OWNER_MANIFEST).unwrap().len().unwrap(), 1);
    assert_eq!(rtx.open_table(SCOPE_BINDINGS).unwrap().len().unwrap(), 0);
    drop(rtx);
    drop(store);

    assert!(open(&path, physical.clone(), None, OwnerLayout::Kv).is_err());
    assert!(open(
        &path,
        PhysicalStoreIdentity::new("physical:blob:other").unwrap(),
        None,
        OwnerLayout::Blob,
    )
    .is_err());
    open(&path, physical, None, OwnerLayout::Blob).unwrap();
}

#[test]
fn strict_open_rejects_missing_or_extra_owner_manifest_rows() {
    for extra in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("strict.redb");
        let physical = PhysicalStoreIdentity::new("physical:blob:test").unwrap();
        let store = create(&path, physical.clone(), None, OwnerLayout::Blob).unwrap();
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

        let error = open(&path, physical, None, OwnerLayout::Blob)
            .err()
            .expect("invalid owner manifest must fail closed");
        assert!(error.contains("manifest"), "{error}");
    }
}

#[test]
fn authenticated_binding_rejects_cross_tenant_and_different_actor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("strict.redb");
    let physical = PhysicalStoreIdentity::new("physical:blob:test").unwrap();
    let store = create(&path, physical, None, OwnerLayout::Blob).unwrap();
    let verifier = TestScopeVerifier {
        tenant: "tenant-a",
        layout: OwnerLayout::Blob,
        principal:
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    };
    let tenant_b = native_identity("tenant-b", "incarnation:blob:b");
    assert!(store
        .authenticate_scope::<BlobOwner>(
            &verifier,
            tenant_b,
            verifier.principal.to_string(),
            b"verified",
        )
        .is_err());

    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let grant = store
        .authenticate_scope::<BlobOwner>(
            &verifier,
            identity.clone(),
            verifier.principal.to_string(),
            b"verified",
        )
        .unwrap();
    let owner = bind_serving_scope(&store, grant, 0).unwrap();
    let mut wrong_actor = batch(identity, "wrong-actor");
    wrong_actor.context.principal = format!("principal:sha256:{}", "b".repeat(64));
    let write = store.write().unwrap();
    assert!(matches!(
        begin(&write, &wrong_actor).unwrap(),
        Begin::Apply { .. }
    ));
    assert!(write.begin_owner(&owner, &wrong_actor).is_err());
}

#[test]
fn unfinished_owner_capability_poisons_the_outer_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("strict.redb");
    let physical = PhysicalStoreIdentity::new("physical:blob:test").unwrap();
    let store = create(&path, physical, None, OwnerLayout::Blob).unwrap();
    let principal = format!("principal:sha256:{}", "a".repeat(64));
    let verifier = TestScopeVerifier {
        tenant: "tenant-a",
        layout: OwnerLayout::Blob,
        principal:
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    };
    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let owner = bind_serving_scope(
        &store,
        store
            .authenticate_scope::<BlobOwner>(&verifier, identity.clone(), principal, b"verified")
            .unwrap(),
        0,
    )
    .unwrap();
    let batch = batch(identity, "poisoned-owner");
    let write = store.write().unwrap();
    let source = match begin(&write, &batch).unwrap() {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    drop(write.begin_owner(&owner, &batch).unwrap());
    assert!(finish(&write, &batch, None, 2, source)
        .unwrap_err()
        .contains("poisoned"));
}

#[test]
fn strict_owner_preserves_sequential_batches_occ_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("strict.redb");
    let physical = PhysicalStoreIdentity::new("physical:blob:test").unwrap();
    let store = create(&path, physical, None, OwnerLayout::Blob).unwrap();
    let principal = format!("principal:sha256:{}", "a".repeat(64));
    let verifier = TestScopeVerifier {
        tenant: "tenant-a",
        layout: OwnerLayout::Blob,
        principal:
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    };
    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let owner = bind_serving_scope(
        &store,
        store
            .authenticate_scope::<BlobOwner>(&verifier, identity.clone(), principal, b"verified")
            .unwrap(),
        0,
    )
    .unwrap();
    let first = batch(identity.clone(), "strict-first");
    let mut second = batch(identity.clone(), "strict-second");
    second.version_expectation = VersionExpectation::Native(1);
    let write = store.write().unwrap();
    let first_source = match begin(&write, &first).unwrap() {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    write
        .begin_owner(&owner, &first)
        .unwrap()
        .finish_owner()
        .unwrap();
    finish(&write, &first, None, 2, first_source).unwrap();
    let second_source = match begin(&write, &second).unwrap() {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    write
        .begin_owner(&owner, &second)
        .unwrap()
        .finish_owner()
        .unwrap();
    finish(&write, &second, None, 3, second_source).unwrap();
    commit(write, &second).unwrap();
    assert_eq!(version(&store, &identity).unwrap(), 2);

    let replay = store.write().unwrap();
    assert!(matches!(begin(&replay, &first).unwrap(), Begin::Replay(_)));
}

#[test]
fn typed_recovery_reanchors_physical_authority_once() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.redb");
    let destination_path = dir.path().join("destination.redb");
    let source_identity = PhysicalStoreIdentity::new("physical:blob:source").unwrap();
    let destination_identity = PhysicalStoreIdentity::new("physical:blob:destination").unwrap();
    let store = create(
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
    open(
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
        for table in crate::owner_registry::owner_table_names(layout) {
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
    let store = create(
        &source_path,
        source_identity.clone(),
        None,
        OwnerLayout::LedgerOnly,
    )
    .unwrap();
    let write = store.database.begin_write().unwrap();
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
                schema_version: MUTATION_STORE_SCHEMA_VERSION,
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
    let store = create(&source_path, source_physical, None, OwnerLayout::Kv).unwrap();
    let source_authority = store.owner_authority_digest().unwrap();
    let write = store.database.begin_write().unwrap();
    write
        .open_table(KV_ROWS)
        .unwrap()
        .insert(("namespace-a", "doc-a"), b"value-a".as_slice())
        .unwrap();
    write.commit().unwrap();

    let evidence =
        backup_strict_recovery_store(&store, &destination_path, destination_physical.clone())
            .unwrap();
    assert_eq!(evidence.owner_rows, 1);
    assert_eq!(evidence.tables.len(), 16);
    assert_eq!(
        evidence
            .tables
            .iter()
            .find(|table| table.table_id == "kv")
            .unwrap()
            .rows,
        1
    );
    let reopened = open(
        &destination_path,
        destination_physical,
        None,
        OwnerLayout::Kv,
    )
    .unwrap();
    assert_ne!(reopened.owner_authority_digest().unwrap(), source_authority);
    assert_eq!(reopened.strict_manifest().unwrap().authority_epoch, 1);
    assert_eq!(strict_recovery_evidence(&reopened).unwrap(), evidence);
    let second_evidence = backup_strict_recovery_store(
        &reopened,
        &second_destination_path,
        second_destination_physical.clone(),
    )
    .unwrap();
    let second_reopened = open(
        &second_destination_path,
        second_destination_physical,
        None,
        OwnerLayout::Kv,
    )
    .unwrap();
    assert_eq!(
        second_reopened.strict_manifest().unwrap().authority_epoch,
        2
    );
    assert_eq!(
        strict_recovery_evidence(&second_reopened).unwrap(),
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
    let store = create(
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
    let _owner = bind_serving_scope(
        &store,
        store
            .authenticate_scope::<KvOwner>(
                &verifier,
                identity.clone(),
                principal.clone(),
                b"verified",
            )
            .unwrap(),
        0,
    )
    .unwrap();

    let write = store.database.begin_write().unwrap();
    let mut manifest = validate_manifest_write(
        &write,
        &store.strict_manifest().unwrap().physical_identity,
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

    assert!(store.write().is_err());
    assert!(store.owner_authority_digest().is_err());
    assert!(store
        .authenticate_scope::<KvOwner>(&verifier, identity, principal, b"verified")
        .is_err());
}

#[test]
fn shared_blob_read_revalidates_root_and_closed_table_registry() {
    for extra_root in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared-read-tamper.redb");
        let store = create(
            &path,
            PhysicalStoreIdentity::new("physical:shared-read-tamper").unwrap(),
            None,
            OwnerLayout::Blob,
        )
        .unwrap();
        let owner = store
            .authenticate_blob_shared_service(
                &TestBlobSharedVerifier,
                "blob-service".to_string(),
                b"verified",
            )
            .unwrap();
        let write = store.database.begin_write().unwrap();
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

        assert!(store.read_blob_shared(&owner, "blob-service").is_err());
    }
}

#[test]
fn staged_adoption_reanchors_only_root_and_bindings() {
    const BLOB_ROWS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("cas_blobs");
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("staged-source.redb");
    let staged_path = dir.path().join("staged-target.redb");
    let physical = PhysicalStoreIdentity::new("physical:staged-adoption").unwrap();
    let integrity: Arc<dyn PrivatePayloadIntegrity> = Arc::new(TestIntegrity);
    let store = create(
        &source_path,
        physical.clone(),
        Some(Arc::clone(&integrity)),
        OwnerLayout::Blob,
    )
    .unwrap();
    let identity = native_identity("tenant-a", "incarnation:staged-adoption");
    let principal = format!("principal:sha256:{}", "a".repeat(64));
    let verifier = TestScopeVerifier {
        tenant: "tenant-a",
        principal:
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        layout: OwnerLayout::Blob,
    };
    let owner = bind_serving_scope(
        &store,
        store
            .authenticate_scope::<BlobOwner>(
                &verifier,
                identity.clone(),
                principal.clone(),
                b"verified",
            )
            .unwrap(),
        0,
    )
    .unwrap();
    let (private_batch, sealed) = recovery_batch(identity.clone(), "staged-private");
    prepare_saga_with_private_payload(&store, &private_batch, 2, Some(&sealed)).unwrap();
    let mut committed_batch = batch(identity.clone(), "staged-committed");
    committed_batch.placement_epoch = 7;
    committed_batch.fencing_token = Some(9);
    committed_batch.outbox.push(eg_types::MutationOutboxIntent {
        topic: "staged.adoption".to_string(),
        key: "staged-committed".to_string(),
        payload: b"preserved".to_vec(),
        headers: Default::default(),
    });
    let owner_scope = identity.binding_digest().to_hex();
    let write = store.write().unwrap();
    let source_version = match begin(&write, &committed_batch).unwrap() {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    let owner_write = write.begin_owner(&owner, &committed_batch).unwrap();
    write
        .transaction()
        .open_table(BLOB_ROWS)
        .unwrap()
        .insert(
            (owner_scope.as_str(), "staged-object"),
            b"owner-row".as_slice(),
        )
        .unwrap();
    owner_write.finish_owner().unwrap();
    finish(&write, &committed_batch, None, 3, source_version).unwrap();
    commit(write, &committed_batch).unwrap();

    let expected_manifest = store.owner_manifest_digest().unwrap();
    let source_root = store.incarnation().identity_digest();
    let source_evidence = strict_recovery_evidence(&store).unwrap();
    drop(store);
    std::fs::copy(&source_path, &staged_path).unwrap();

    assert!(
        inspect_staged_mutation_store(&staged_path, physical.clone(), OwnerLayout::Blob, None,)
            .is_err()
    );
    assert!(inspect_staged_mutation_store(
        &staged_path,
        physical.clone(),
        OwnerLayout::Jobs,
        Some(Arc::clone(&integrity)),
    )
    .is_err());
    assert!(inspect_staged_mutation_store(
        &staged_path,
        PhysicalStoreIdentity::new("physical:staged-adoption:other").unwrap(),
        OwnerLayout::Blob,
        Some(Arc::clone(&integrity)),
    )
    .is_err());
    let staged = inspect_staged_mutation_store(
        &staged_path,
        physical,
        OwnerLayout::Blob,
        Some(Arc::clone(&integrity)),
    )
    .unwrap();
    assert_eq!(staged.owner_manifest_digest(), expected_manifest);
    let adopted = adopt_staged_mutation_store(staged).unwrap();
    assert_ne!(adopted.incarnation().identity_digest(), source_root);
    assert_eq!(adopted.owner_manifest_digest().unwrap(), expected_manifest);
    let read = adopted.database.begin_read().unwrap();
    let table = read.open_table(BLOB_ROWS).unwrap();
    assert_eq!(
        table
            .get((owner_scope.as_str(), "staged-object"))
            .unwrap()
            .unwrap()
            .value(),
        b"owner-row"
    );
    assert!(read_record(&adopted, &identity, "staged-committed")
        .unwrap()
        .is_some());
    assert_eq!(version(&adopted, &identity).unwrap(), 1);
    assert_eq!(
        read_outbox(&adopted, &identity, "staged-committed")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        read_private_payload(&adopted, &identity, "staged-private").unwrap(),
        Some(sealed)
    );
    let adopted_evidence = strict_recovery_evidence(&adopted).unwrap();
    for table in [
        FENCES.name(),
        OUTBOX.name(),
        PRIVATE_PAYLOADS.name(),
        "cas_blobs",
    ] {
        let before = source_evidence
            .tables
            .iter()
            .find(|evidence| evidence.table_id == table)
            .unwrap();
        let after = adopted_evidence
            .tables
            .iter()
            .find(|evidence| evidence.table_id == table)
            .unwrap();
        assert_eq!(
            (before.rows, before.fingerprint),
            (after.rows, after.fingerprint)
        );
        assert_eq!(after.rows, 1);
    }
}

#[test]
fn staged_adoption_allows_absent_integrity_only_when_private_rows_are_empty() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("empty-private-source.redb");
    let staged_path = dir.path().join("empty-private-target.redb");
    let physical = PhysicalStoreIdentity::new("physical:empty-private").unwrap();
    let store = create(
        &source_path,
        physical.clone(),
        None,
        OwnerLayout::LedgerOnly,
    )
    .unwrap();
    let expected_manifest = store.owner_manifest_digest().unwrap();
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
    let store = create(&path, physical.clone(), None, OwnerLayout::Statechart).unwrap();
    drop(store);
    assert!(
        inspect_staged_mutation_store(&path, physical.clone(), OwnerLayout::Statechart, None,)
            .is_err()
    );
    let store = open(&path, physical.clone(), None, OwnerLayout::Statechart).unwrap();

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
    bind_serving_scope(
        &store,
        store
            .authenticate_scope::<StatechartOwner>(&verifier, fixed, principal.clone(), b"verified")
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

    let store = open(&path, physical.clone(), None, OwnerLayout::Statechart).unwrap();
    let extra = domain_identity(
        "native",
        MutationDomain::Lifecycle,
        "statechart-other",
        "incarnation:eg-statechart:statechart-other:1",
    );
    bind_serving_scope(
        &store,
        store
            .authenticate_scope::<StatechartOwner>(&verifier, extra, principal, b"verified")
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
    let store = create(&path, physical.clone(), None, OwnerLayout::LedgerOnly).unwrap();
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
