use crate::apply::{begin, commit, finish};
use crate::ledger_tables::{FENCES, OUTBOX, PRIVATE_PAYLOADS};
use crate::read::{read_outbox, read_private_payload, read_record, version};
use crate::saga::prepare_saga_with_private_payload;
use crate::write::MutationWrite;
use crate::Begin;
use eg_storage::{
    strict_recovery_evidence, BlobOwner, LedgerOnlyOwner, MutationOwnerAuthority, OwnedStoreHandle,
    OwnerDomain, OwnerLayout, PhysicalStoreIdentity, PrivatePayloadIntegrity, ScopeGrantVerifier,
    StorageKernelV1,
};
use eg_types::mutation_batch::{
    IncarnationId, LogicalName, MutationDomain, MutationRequestContext, MutationSurface, TenantId,
    VersionExpectation,
};
use eg_types::protocol::Method;
use eg_types::{
    MutationBatch, MutationOperation, MutationScopeIdentity, MUTATION_BATCH_VERSION,
};
use redb::{TableDefinition, TableHandle};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

const PRINCIPAL: &str =
    "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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

fn verifier(tenant: &'static str, layout: OwnerLayout) -> TestScopeVerifier {
    TestScopeVerifier {
        tenant,
        principal: PRINCIPAL,
        layout,
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
            principal: PRINCIPAL.to_string(),
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

/// One owner file plus its single mutation authority.
struct Fixture {
    kernel: StorageKernelV1,
    authority: MutationOwnerAuthority,
}

impl Fixture {
    fn create<D: OwnerDomain>(
        path: &Path,
        physical: &str,
        integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    ) -> Self {
        let kernel = StorageKernelV1::create_owner::<D>(
            path,
            PhysicalStoreIdentity::new(physical).unwrap(),
            integrity,
        )
        .unwrap();
        Self::split(kernel)
    }

    fn open<D: OwnerDomain>(
        path: &Path,
        physical: &str,
        integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    ) -> Self {
        let kernel = StorageKernelV1::open_owner::<D>(
            path,
            PhysicalStoreIdentity::new(physical).unwrap(),
            integrity,
        )
        .unwrap();
        Self::split(kernel)
    }

    fn split(kernel: StorageKernelV1) -> Self {
        let (kernel, authority) = kernel.into_read_and_mutation_authority().unwrap();
        Self { kernel, authority }
    }

    fn bind<D: OwnerDomain>(
        &self,
        verifier: &dyn ScopeGrantVerifier,
        identity: MutationScopeIdentity,
    ) -> OwnedStoreHandle<D> {
        let grant = self
            .kernel
            .authenticate_scope::<D>(verifier, identity, PRINCIPAL.to_string(), b"verified")
            .unwrap();
        self.kernel.bind_serving_scope(grant, 0).unwrap()
    }
}

fn ledger_fixture(path: &Path, identity: MutationScopeIdentity) -> (Fixture, OwnedStoreHandle<LedgerOnlyOwner>) {
    let fixture = Fixture::create::<LedgerOnlyOwner>(path, "physical:test:ledger-only", None);
    let owner = fixture.bind::<LedgerOnlyOwner>(&verifier("tenant-a", OwnerLayout::LedgerOnly), identity);
    (fixture, owner)
}

fn apply_batch<D: OwnerDomain>(
    fixture: &Fixture,
    owner: &OwnedStoreHandle<D>,
    batch: &MutationBatch,
) {
    let write = MutationWrite::open(&fixture.authority, owner).unwrap();
    let source_version = match begin(&write, batch).unwrap() {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    if D::LAYOUT != OwnerLayout::LedgerOnly {
        write
            .begin_owner(owner, batch)
            .unwrap()
            .finish_owner()
            .unwrap();
    }
    finish(&write, batch, None, 2, source_version).unwrap();
    commit(write, batch).unwrap();
}

#[test]
fn one_physical_root_serves_multiple_scopes_without_cross_tenant_aliasing() {
    let dir = tempfile::tempdir().unwrap();
    let tenant_a = native_identity("tenant-a", "incarnation:blob:a");
    let tenant_b = native_identity("tenant-b", "incarnation:blob:b");
    let (fixture, owner_a) = ledger_fixture(&dir.path().join("native.redb"), tenant_a.clone());
    let owner_b = fixture.bind::<LedgerOnlyOwner>(
        &verifier("tenant-b", OwnerLayout::LedgerOnly),
        tenant_b.clone(),
    );

    apply_batch(&fixture, &owner_a, &batch(tenant_a, "same-batch-id"));
    apply_batch(&fixture, &owner_b, &batch(tenant_b, "same-batch-id"));

    let read_a = fixture.kernel.read_scope(&owner_a).unwrap();
    let read_b = fixture.kernel.read_scope(&owner_b).unwrap();
    assert!(read_record(&read_a, "same-batch-id").unwrap().is_some());
    assert!(read_record(&read_b, "same-batch-id").unwrap().is_some());
    assert_eq!(version(&read_a).unwrap(), 1);
    assert_eq!(version(&read_b).unwrap(), 1);
}

#[test]
fn backup_derives_a_distinct_physical_root_and_rebinds_scopes() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let source_path = dir.path().join("source.redb");
    let (fixture, owner) = ledger_fixture(&source_path, identity.clone());
    apply_batch(&fixture, &owner, &batch(identity.clone(), "backup-batch"));
    let destination = dir.path().join("backup.redb");
    eg_storage::backup_recovery_store(&fixture.kernel, &destination).unwrap();
    let source_root = fixture.kernel.incarnation().identity_digest();
    drop(fixture);

    let backup = Fixture::open::<LedgerOnlyOwner>(&destination, "physical:test:ledger-only", None);
    assert_ne!(backup.kernel.incarnation().identity_digest(), source_root);
    let owner = backup.bind::<LedgerOnlyOwner>(
        &verifier("tenant-a", OwnerLayout::LedgerOnly),
        identity,
    );
    let read = backup.kernel.read_scope(&owner).unwrap();
    assert!(read_record(&read, "backup-batch").unwrap().is_some());
}

#[test]
fn private_recovery_authenticity_uses_injected_canonical_authority() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let fixture = Fixture::create::<LedgerOnlyOwner>(
        &dir.path().join("native.redb"),
        "physical:test:ledger-only",
        Some(Arc::new(TestIntegrity)),
    );
    let owner = fixture.bind::<LedgerOnlyOwner>(
        &verifier("tenant-a", OwnerLayout::LedgerOnly),
        identity,
    );
    let (batch, sealed) = recovery_batch(owner.identity().clone(), "recovery-batch");
    prepare_saga_with_private_payload(&fixture.authority, &owner, &batch, 2, Some(&sealed)).unwrap();
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert_eq!(
        read_private_payload(&read, &batch.batch_id).unwrap(),
        Some(sealed)
    );
}

#[test]
fn forged_private_recovery_payload_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let fixture = Fixture::create::<LedgerOnlyOwner>(
        &dir.path().join("native.redb"),
        "physical:test:ledger-only",
        Some(Arc::new(TestIntegrity)),
    );
    let owner = fixture.bind::<LedgerOnlyOwner>(
        &verifier("tenant-a", OwnerLayout::LedgerOnly),
        identity,
    );
    let (batch, _) = recovery_batch(owner.identity().clone(), "forged-recovery");
    assert!(prepare_saga_with_private_payload(
        &fixture.authority,
        &owner,
        &batch,
        2,
        Some(b"\xe6forged")
    )
    .is_err());
}

#[test]
fn missing_private_integrity_authority_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let (fixture, owner) = ledger_fixture(&dir.path().join("native.redb"), identity);
    let (batch, sealed) = recovery_batch(owner.identity().clone(), "missing-integrity");
    assert!(prepare_saga_with_private_payload(
        &fixture.authority,
        &owner,
        &batch,
        2,
        Some(&sealed)
    )
    .is_err());
}

#[test]
fn authenticated_binding_rejects_cross_tenant_and_different_actor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("strict.redb");
    let fixture = Fixture::create::<BlobOwner>(&path, "physical:blob:test", None);
    let verifier = verifier("tenant-a", OwnerLayout::Blob);
    let tenant_b = native_identity("tenant-b", "incarnation:blob:b");
    assert!(fixture
        .kernel
        .authenticate_scope::<BlobOwner>(
            &verifier,
            tenant_b,
            PRINCIPAL.to_string(),
            b"verified"
        )
        .is_err());

    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let owner = fixture.bind::<BlobOwner>(&verifier, identity.clone());
    let mut wrong_actor = batch(identity, "wrong-actor");
    wrong_actor.context.principal = format!("principal:sha256:{}", "b".repeat(64));
    let write = MutationWrite::open(&fixture.authority, &owner).unwrap();
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
    let fixture = Fixture::create::<BlobOwner>(&path, "physical:blob:test", None);
    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let owner = fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());
    let batch = batch(identity, "poisoned-owner");
    let write = MutationWrite::open(&fixture.authority, &owner).unwrap();
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
    let fixture = Fixture::create::<BlobOwner>(&path, "physical:blob:test", None);
    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let owner = fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());
    let first = batch(identity.clone(), "strict-first");
    let mut second = batch(identity, "strict-second");
    second.version_expectation = VersionExpectation::Native(1);
    let write = MutationWrite::open(&fixture.authority, &owner).unwrap();
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
    assert_eq!(version(&fixture.kernel.read_scope(&owner).unwrap()).unwrap(), 2);

    let replay = MutationWrite::open(&fixture.authority, &owner).unwrap();
    assert!(matches!(begin(&replay, &first).unwrap(), Begin::Replay(_)));
}

#[test]
fn staged_adoption_reanchors_only_root_and_bindings() {
    const BLOB_ROWS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("cas_blobs");
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("staged-source.redb");
    let staged_path = dir.path().join("staged-target.redb");
    let physical = PhysicalStoreIdentity::new("physical:staged-adoption").unwrap();
    let integrity: Arc<dyn PrivatePayloadIntegrity> = Arc::new(TestIntegrity);
    let fixture = Fixture::create::<BlobOwner>(
        &source_path,
        "physical:staged-adoption",
        Some(Arc::clone(&integrity)),
    );
    let identity = native_identity("tenant-a", "incarnation:staged-adoption");
    let owner = fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());
    let (private_batch, sealed) = recovery_batch(identity.clone(), "staged-private");
    prepare_saga_with_private_payload(&fixture.authority, &owner, &private_batch, 2, Some(&sealed))
        .unwrap();
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
    let write = MutationWrite::open(&fixture.authority, &owner).unwrap();
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

    let expected_manifest = fixture.kernel.owner_manifest_digest().unwrap();
    let source_root = fixture.kernel.incarnation().identity_digest();
    let source_evidence = strict_recovery_evidence(&fixture.kernel).unwrap();
    drop(fixture);
    std::fs::copy(&source_path, &staged_path).unwrap();

    assert!(eg_storage::inspect_staged_mutation_store(
        &staged_path,
        physical.clone(),
        OwnerLayout::Blob,
        None,
    )
    .is_err());
    assert!(eg_storage::inspect_staged_mutation_store(
        &staged_path,
        physical.clone(),
        OwnerLayout::Jobs,
        Some(Arc::clone(&integrity)),
    )
    .is_err());
    assert!(eg_storage::inspect_staged_mutation_store(
        &staged_path,
        PhysicalStoreIdentity::new("physical:staged-adoption:other").unwrap(),
        OwnerLayout::Blob,
        Some(Arc::clone(&integrity)),
    )
    .is_err());
    let staged = eg_storage::inspect_staged_mutation_store(
        &staged_path,
        physical,
        OwnerLayout::Blob,
        Some(Arc::clone(&integrity)),
    )
    .unwrap();
    assert_eq!(staged.owner_manifest_digest(), expected_manifest);
    let adopted = eg_storage::adopt_staged_mutation_store(staged).unwrap();
    assert_ne!(adopted.incarnation().identity_digest(), source_root);
    assert_eq!(adopted.owner_manifest_digest().unwrap(), expected_manifest);
    let adopted_evidence = strict_recovery_evidence(&adopted).unwrap();
    let adopted = Fixture::split(adopted);
    let owner = adopted.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity);
    let read = adopted.kernel.read_scope(&owner).unwrap();
    let table = read.transaction().open_table(BLOB_ROWS).unwrap();
    assert_eq!(
        table
            .get((owner_scope.as_str(), "staged-object"))
            .unwrap()
            .unwrap()
            .value(),
        b"owner-row"
    );
    drop(table);
    assert!(read_record(&read, "staged-committed").unwrap().is_some());
    assert_eq!(version(&read).unwrap(), 1);
    assert_eq!(read_outbox(&read, "staged-committed").unwrap().len(), 1);
    assert_eq!(
        read_private_payload(&read, "staged-private").unwrap(),
        Some(sealed)
    );
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
fn ledger_table_declarations_match_the_storage_kernel_census() {
    let declared = eg_storage::declared_table_names(OwnerLayout::LedgerOnly)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let mine = crate::ledger_tables::ledger_table_names()
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert!(mine.is_subset(&declared), "{mine:?} vs {declared:?}");
    assert_eq!(declared.len(), mine.len() + 3);
}
