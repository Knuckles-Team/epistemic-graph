use super::*;
use eg_types::mutation_batch::{
    IncarnationId, LogicalName, MutationDomain, MutationRequestContext, MutationSurface, TenantId,
    VersionExpectation,
};
use eg_types::protocol::Method;
use eg_types::{MutationOperation, MUTATION_BATCH_VERSION};
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

fn native_identity(tenant: &str, incarnation: &str) -> MutationScopeIdentity {
    MutationScopeIdentity::native(
        TenantId::new(tenant).unwrap(),
        MutationDomain::BlobStore,
        LogicalName::new("blob-catalog").unwrap(),
        IncarnationId::new(incarnation).unwrap(),
    )
    .unwrap()
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
