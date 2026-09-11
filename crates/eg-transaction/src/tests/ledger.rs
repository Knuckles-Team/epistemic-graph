//! Ledger admission, ordering, fencing, saga and adoption behaviour.

use super::*;

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
    assert!(read_ledger(&read_a, "same-batch-id").unwrap().is_some());
    assert!(read_ledger(&read_b, "same-batch-id").unwrap().is_some());
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
    let owner =
        backup.bind::<LedgerOnlyOwner>(&verifier("tenant-a", OwnerLayout::LedgerOnly), identity);
    let read = backup.kernel.read_scope(&owner).unwrap();
    assert!(read_ledger(&read, "backup-batch").unwrap().is_some());
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
    let owner =
        fixture.bind::<LedgerOnlyOwner>(&verifier("tenant-a", OwnerLayout::LedgerOnly), identity);
    let (batch, sealed) = recovery_batch(owner.identity().clone(), "recovery-batch");
    fixture
        .mutations
        .saga_step(&owner, &batch, 2, Some(&sealed))
        .unwrap();
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
    let owner =
        fixture.bind::<LedgerOnlyOwner>(&verifier("tenant-a", OwnerLayout::LedgerOnly), identity);
    let (batch, _) = recovery_batch(owner.identity().clone(), "forged-recovery");
    assert!(fixture
        .mutations
        .saga_step(&owner, &batch, 2, Some(b"\xe6forged"))
        .is_err());
}

#[test]
fn missing_private_integrity_authority_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:blob:1");
    let (fixture, owner) = ledger_fixture(&dir.path().join("native.redb"), identity);
    let (batch, sealed) = recovery_batch(owner.identity().clone(), "missing-integrity");
    assert!(fixture
        .mutations
        .saga_step(&owner, &batch, 2, Some(&sealed))
        .is_err());
}

#[test]
fn committed_saga_fresh_replay_consumes_nonce_and_rejects_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:saga-replay");
    let fixture = Fixture::create::<LedgerOnlyOwner>(
        &dir.path().join("native.redb"),
        "physical:test:ledger-only",
        Some(Arc::new(TestIntegrity)),
    );
    let owner = fixture.bind::<LedgerOnlyOwner>(
        &verifier("tenant-a", OwnerLayout::LedgerOnly),
        identity,
    );
    let (batch, sealed) = recovery_batch(owner.identity().clone(), "committed-saga-replay");
    assert!(matches!(
        fixture
            .mutations
            .saga_step(&owner, &batch, 2, Some(&sealed))
            .unwrap(),
        crate::SagaBegin::Execute
    ));
    fixture
        .mutations
        .saga_end(&owner, &batch, b"saga-result".to_vec(), 3)
        .unwrap();

    let retry = retry_of(&batch);
    assert!(matches!(
        fixture.mutations.saga_step(&owner, &retry, 4, None).unwrap(),
        crate::SagaBegin::Committed(_)
    ));
    let consumed = fixture
        .mutations
        .saga_step(&owner, &retry, 5, None)
        .expect_err("a committed saga replay nonce cannot be reused");
    assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");
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
        .authenticate_scope::<BlobOwner>(&verifier, tenant_b, PRINCIPAL.to_string(), b"verified")
        .is_err());

    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let owner = fixture.bind::<BlobOwner>(&verifier, identity.clone());
    let mut wrong_actor = batch(identity, "wrong-actor");
    // The principal the committing ledger requires is the envelope's, so a
    // batch naming another owner is refused by `owner_rows` -- exactly as it
    // was when the value lived in the deleted `context.principal`.
    let eg_types::mutation_batch::MutationEnvelope::Operation(envelope) =
        &mut wrong_actor.envelope
    else {
        panic!("a fixture operation batch has an operation envelope");
    };
    envelope.serving_principal = format!("principal:sha256:{}", "b".repeat(64));
    let (write, begun) = fixture.mutations.admit(&owner, &wrong_actor).unwrap();
    assert!(matches!(begun, Begin::Apply { .. }));
    assert!(write.owner_rows(&owner, &wrong_actor).is_err());
}

#[test]
fn unfinished_owner_capability_poisons_the_outer_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("strict.redb");
    let fixture = Fixture::create::<BlobOwner>(&path, "physical:blob:test", None);
    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let owner =
        fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());
    let batch = batch(identity, "poisoned-owner");
    let (write, begun) = fixture.mutations.admit(&owner, &batch).unwrap();
    let source = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    drop(write.owner_rows(&owner, &batch).unwrap());
    assert!(fixture
        .mutations
        .finish(&write, &batch, None, 2, source)
        .unwrap_err()
        .contains("poisoned"));
}

#[test]
fn strict_owner_preserves_sequential_batches_occ_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("strict.redb");
    let fixture = Fixture::create::<BlobOwner>(&path, "physical:blob:test", None);
    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let owner =
        fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());
    let first = batch(identity.clone(), "strict-first");
    let mut second = batch(identity, "strict-second");
    second.version_expectation = VersionExpectation::Native(1);
    let (write, begun) = fixture.mutations.admit(&owner, &first).unwrap();
    let first_source = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    write
        .owner_rows(&owner, &first)
        .unwrap()
        .finish_owner()
        .unwrap();
    fixture
        .mutations
        .finish(&write, &first, None, 2, first_source)
        .unwrap();
    let second_source = match write.begin(&second).unwrap() {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    write
        .owner_rows(&owner, &second)
        .unwrap()
        .finish_owner()
        .unwrap();
    fixture
        .mutations
        .finish(&write, &second, None, 3, second_source)
        .unwrap();
    fixture.mutations.commit(write, &second).unwrap();
    assert_eq!(
        version(&fixture.kernel.read_scope(&owner).unwrap()).unwrap(),
        2
    );

    // A retry is a FRESH attempt over the unchanged stable identity, which is
    // what a producer compiles; re-submitting the byte-identical batch value
    // would be a duplicated attempt, and the kernel refuses that by name.
    let (replay, begun) = fixture
        .mutations
        .admit(&owner, &retry_of(&first))
        .unwrap();
    assert!(matches!(begun, Begin::Replay(_)));
    replay.abort().unwrap();
}

#[test]
fn staged_adoption_reanchors_only_root_and_bindings() {
    // `cas_blobs` is `&str -> &[u8]`: the Blob layout's owner tables are bounded
    // by the LAYOUT, not by a scope component in the key, so the serving scope is
    // written into the key text here rather than being a tuple element.
    const BLOB_ROWS: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_blobs");
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
    let owner =
        fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());
    let (private_batch, sealed) = recovery_batch(identity.clone(), "staged-private");
    fixture
        .mutations
        .saga_step(&owner, &private_batch, 2, Some(&sealed))
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
    committed_batch
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("the staged-adoption fixture reseals its final outbox");
    let owner_scope = eg_storage::ledger_scope_key(&identity);
    let (write, begun) = fixture.mutations.admit(&owner, &committed_batch).unwrap();
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    let owner_write = write.owner_rows(&owner, &committed_batch).unwrap();
    owner_write
        .open_table(BLOB_ROWS)
        .unwrap()
        .insert(
            blob_key(owner_scope.as_str(), "staged-object").as_str(),
            b"owner-row".as_slice(),
        )
        .unwrap();
    owner_write.finish_owner().unwrap();
    fixture
        .mutations
        .finish(&write, &committed_batch, None, 3, source_version)
        .unwrap();
    fixture.mutations.commit(write, &committed_batch).unwrap();

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
    let table = read.open_owner_table(BLOB_ROWS).unwrap();
    assert_eq!(
        table
            .get(blob_key(owner_scope.as_str(), "staged-object").as_str())
            .unwrap()
            .unwrap()
            .value(),
        b"owner-row"
    );
    drop(table);
    assert!(read_ledger(&read, "staged-committed").unwrap().is_some());
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
    let mine = crate::tables::ledger_table_names()
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert!(mine.is_subset(&declared), "{mine:?} vs {declared:?}");
    // The three the storage kernel keeps for itself: the store root, the owner
    // manifest, and the scope-binding table it alone writes.
    assert_eq!(declared.len(), mine.len() + 3);
}

/// A domain crate writes owner rows through `AdmittedOwnerWrite::open_table`,
/// which is bounded to its own layout: the ledger, the physical-identity
/// tables and every other layout's tables fail closed.
#[test]
fn an_owner_write_reaches_only_its_own_layouts_tables() {
    // `cas_blobs` is `&str -> &[u8]`: the Blob layout's owner tables are bounded
    // by the LAYOUT, not by a scope component in the key, so the serving scope is
    // written into the key text here rather than being a tuple element.
    const BLOB_ROWS: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_blobs");
    const LEDGER: TableDefinition<(&str, &str), &[u8]> =
        TableDefinition::new("ledger_batches");
    const OTHER_LAYOUT: TableDefinition<&str, &[u8]> = TableDefinition::new("rbac");
    const IDENTITY: TableDefinition<&str, &[u8]> = TableDefinition::new("mutation_store_root");

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("owner-rows.redb");
    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let batch = batch(identity.clone(), "owner-row-batch");
    {
        let fixture = Fixture::create::<BlobOwner>(&path, "physical:blob:test", None);
        let owner =
            fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());
        let (write, begun) = fixture.mutations.admit(&owner, &batch).unwrap();
        let source_version = match begun {
            Begin::Apply { source_version } => source_version,
            Begin::Replay(_) => panic!("unexpected replay"),
        };
        let rows = write.owner_rows(&owner, &batch).unwrap();
        for table in [LEDGER, TableDefinition::new("mutation_classes")] {
            assert!(rows
                .open_table(table)
                .unwrap_err()
                .contains("outside its layout"));
        }
        assert!(rows
            .open_table(OTHER_LAYOUT)
            .unwrap_err()
            .contains("outside its layout"));
        assert!(rows
            .open_table(IDENTITY)
            .unwrap_err()
            .contains("outside its layout"));
        let scope = eg_storage::ledger_scope_key(&identity);
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
            .finish(&write, &batch, None, 2, source_version)
            .unwrap();
        fixture.mutations.commit(write, &batch).unwrap();
    }
    let fixture = Fixture::open::<BlobOwner>(&path, "physical:blob:test", None);
    let owner =
        fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());
    let read = fixture.kernel.read_scope(&owner).unwrap();
    let scope = eg_storage::ledger_scope_key(&identity);
    let table = read.open_owner_table(BLOB_ROWS).unwrap();
    assert_eq!(
        table
            .get(blob_key(scope.as_str(), "object").as_str())
            .unwrap()
            .unwrap()
            .value(),
        b"domain-row"
    );
}

/// A domain that decides what to write from what it reads must do both in one
/// admitted write, or the decision is not serialized. `open_read_table` gives
/// that read before any batch exists, and no wider: the ledger, another
/// layout's tables and the identity tables all fail closed.
#[test]
fn an_admitted_write_can_read_its_own_owner_rows_before_a_batch_exists() {
    // `cas_blobs` is `&str -> &[u8]`: the Blob layout's owner tables are bounded
    // by the LAYOUT, not by a scope component in the key, so the serving scope is
    // written into the key text here rather than being a tuple element.
    const BLOB_ROWS: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_blobs");
    const LEDGER: TableDefinition<(&str, &str), &[u8]> =
        TableDefinition::new("ledger_batches");
    const OTHER_LAYOUT: TableDefinition<&str, &[u8]> = TableDefinition::new("rbac");
    const IDENTITY: TableDefinition<&str, &[u8]> = TableDefinition::new("mutation_store_root");

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("decide-then-write.redb");
    let identity = native_identity("tenant-a", "incarnation:blob:a");
    let scope = eg_storage::ledger_scope_key(&identity);
    let fixture = Fixture::create::<BlobOwner>(&path, "physical:blob:test", None);
    let owner =
        fixture.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());

    // Seed one row through a normal admitted mutation.
    let seed = batch(identity.clone(), "seed");
    let (write, begun) = fixture.mutations.admit(&owner, &seed).unwrap();
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    let rows = write.owner_rows(&owner, &seed).unwrap();
    rows.open_table(BLOB_ROWS)
        .unwrap()
        .insert(
            blob_key(scope.as_str(), "object").as_str(),
            b"v1".as_slice(),
        )
        .unwrap();
    rows.finish_owner().unwrap();
    fixture
        .mutations
        .finish(&write, &seed, None, 2, source_version)
        .unwrap();
    fixture.mutations.commit(write, &seed).unwrap();

    // Decide-then-write: open the write first, read inside it, and only then
    // build the batch from what was read.
    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    match write.open_read_table(LEDGER) {
        Ok(_) => panic!("an owner read must not reach the ledger"),
        Err(error) => assert!(error.contains("outside its layout"), "{error}"),
    }
    match write.open_read_table(OTHER_LAYOUT) {
        Ok(_) => panic!("an owner read must not reach another layout"),
        Err(error) => assert!(error.contains("outside its layout"), "{error}"),
    }
    match write.open_read_table(IDENTITY) {
        Ok(_) => panic!("an owner read must not reach a physical-identity table"),
        Err(error) => assert!(error.contains("outside its layout"), "{error}"),
    }

    let view = write.open_read_table(BLOB_ROWS).unwrap();
    assert_eq!(view.len().unwrap(), 1);
    let observed = view
        .get(blob_key(scope.as_str(), "object").as_str())
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    drop(view);
    assert_eq!(observed, b"v1");

    let mut derived = batch(identity.clone(), "derived-from-read");
    derived.version_expectation = VersionExpectation::Native(1);
    let begun = write.begin(&derived).unwrap();
    assert!(matches!(begun, Begin::Apply { .. }));
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => unreachable!(),
    };
    let rows = write.owner_rows(&owner, &derived).unwrap();
    rows.open_table(BLOB_ROWS)
        .unwrap()
        .insert(
            blob_key(scope.as_str(), "object").as_str(),
            b"v2".as_slice(),
        )
        .unwrap();
    // The uncommitted write is visible to this same transaction's read view.
    let view = write.open_read_table(BLOB_ROWS).unwrap();
    let seen = view
        .get(blob_key(scope.as_str(), "object").as_str())
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    drop(view);
    assert_eq!(seen, b"v2");
    rows.finish_owner().unwrap();
    fixture
        .mutations
        .finish(&write, &derived, None, 3, source_version)
        .unwrap();
    fixture.mutations.commit(write, &derived).unwrap();

    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert_eq!(
        read.open_owner_table(BLOB_ROWS)
            .unwrap()
            .get(blob_key(scope.as_str(), "object").as_str())
            .unwrap()
            .unwrap()
            .value(),
        b"v2"
    );
}
