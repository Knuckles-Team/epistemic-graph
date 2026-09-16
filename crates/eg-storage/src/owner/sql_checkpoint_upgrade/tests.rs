//! Storage-native proofs for one atomic, offline SQL layout transition.

use super::*;
use crate::codec::{decode_ledger_record, encode_bounded};
use crate::recovery::evidence::strict_recovery_evidence;
use redb::{MultimapTableDefinition, ReadableTable, TableDefinition};
use sha2::{Digest, Sha256};

mod support;
use support::{
    create_predecessor, physical, private_tempdir, read_record, snapshot_old, FixtureIntegrity,
};

fn file_digest(path: &Path) -> [u8; 32] {
    Sha256::digest(std::fs::read(path).unwrap()).into()
}

fn inspect(path: &Path) -> Result<ValidatedSqlSourceCheckpointUpgrade, String> {
    inspect_sql_source_checkpoint_upgrade(path, physical(), None, inspection_options(path))
}

fn inspection_options(path: &Path) -> SqlSourceCheckpointInspectionOptions {
    SqlSourceCheckpointInspectionOptions::new(
        path.parent().unwrap().join("sql-checkpoint-inspection"),
        256 * 1024 * 1024,
    )
    .unwrap()
}

fn mutate_manifest(path: &Path, change: impl FnOnce(&mut OwnerManifest)) {
    let database = upgrade_builder().open(path).unwrap();
    let write = database.begin_write().unwrap();
    let mut table = write.open_table(OWNER_MANIFEST).unwrap();
    let mut manifest = read_manifest_slot(&table).unwrap();
    change(&mut manifest);
    table
        .insert(
            "manifest",
            encode_bounded(&manifest, "altered fixture manifest")
                .unwrap()
                .as_slice(),
        )
        .unwrap();
    drop(table);
    write.commit().unwrap();
}

#[test]
fn predecessor_and_successor_layout_digests_are_pinned() {
    let contracts = contract::predecessor_contracts().unwrap();
    assert_eq!(contracts.len(), 36);
    assert_eq!(
        contract::predecessor_digest(&contracts),
        contract::PRE_CHECKPOINT_LAYOUT
    );
    let current = OwnerManifest::new(physical(), OwnerLayout::Sql).unwrap();
    assert_eq!(current.tables.len(), 37);
    assert_eq!(
        current.layout_digest,
        [
            0x2d, 0x56, 0x2f, 0xab, 0xc7, 0x5b, 0x19, 0xe9, 0xc3, 0x63, 0x09, 0xb1, 0xbd, 0xa7,
            0xfc, 0xc9, 0xa0, 0x50, 0xc6, 0x8a, 0xcb, 0xec, 0x87, 0x6f, 0xc0, 0x69, 0xd3, 0xb5,
            0x06, 0xd9, 0x3e, 0x1e,
        ]
    );
    let checkpoint = current.tables.last().unwrap();
    assert_eq!(checkpoint.table_id, SQL_SOURCE_CHECKPOINTS.name());
    assert_eq!(checkpoint.key_type_id, "(&str,&str,&str)");
    assert_eq!(checkpoint.value_type_id, "&[u8]");
    assert_eq!(checkpoint.logical_codec_id, "msgpack-v1");
}

#[test]
fn upgrade_preserves_old_owner_and_kernel_bytes_and_changes_only_layout_authority() {
    let directory = private_tempdir();
    let path = directory.path().join("sql.redb");
    let recorded = create_predecessor(&path, false);
    let before = snapshot_old(&path);
    assert!(StorageKernel::open_owner::<SqlOwner>(&path, physical(), None).is_err());
    assert!(crate::open_recovery(&path, physical(), None, OwnerLayout::Sql).is_err());
    // Ordinary writable opens can change native allocator metadata on refusal;
    // the physical token is minted after those deliberate outside operations.
    let token = inspect(&path).unwrap();
    let old_root = token.incarnation.clone();
    let (kernel, report) = upgrade_sql_source_checkpoints(token).unwrap();
    assert_eq!(kernel.incarnation(), &old_root);
    assert_eq!(report.previous_authority_epoch, 0);
    assert_eq!(report.current_authority_epoch, 1);
    assert_ne!(
        report.previous_authority_digest,
        report.current_authority_digest
    );
    let after = strict_recovery_evidence(&kernel).unwrap();
    assert_eq!(after.tables.len(), before.tables.len() + 1);
    for previous in &before.tables {
        let current = after
            .tables
            .iter()
            .find(|table| table.table_id == previous.table_id)
            .unwrap();
        if previous.table_id != OWNER_MANIFEST.name() {
            assert_eq!(current, previous);
        }
    }
    let checkpoint = after
        .tables
        .iter()
        .find(|table| table.table_id == SQL_SOURCE_CHECKPOINTS.name())
        .unwrap();
    assert_eq!(checkpoint.rows, 0);
    let replayed = read_record(&kernel);
    assert_eq!(
        encode_bounded(&replayed, "readback").unwrap(),
        encode_bounded(&recorded, "recorded").unwrap()
    );
    assert_eq!(
        rmp_serde::from_slice::<u64>(replayed.result_msgpack.as_ref().unwrap()).unwrap(),
        17
    );
    crate::validate_recovery_store(&kernel).unwrap();
    drop(kernel);
    assert!(
        inspect(&path).is_err(),
        "an already upgraded store cannot advance authority twice"
    );
    let reopened = StorageKernel::open_owner::<SqlOwner>(&path, physical(), None).unwrap();
    assert_eq!(
        reopened.owner_authority_digest().unwrap(),
        report.current_authority_digest
    );
}

#[test]
fn retained_replay_identity_accepts_a_fresh_nonce_after_layout_upgrade() {
    let directory = private_tempdir();
    let path = directory.path().join("sql.redb");
    let mut recorded = create_predecessor(&path, false);
    let old_envelope = recorded.batch.envelope.operation().unwrap();
    let old_nonce = old_envelope.nonce_replay_key().unwrap().digest().unwrap();
    let old_operation = old_envelope.operation_identity().unwrap().digest().unwrap();
    let (kernel, _) = upgrade_sql_source_checkpoints(inspect(&path).unwrap()).unwrap();
    let eg_types::mutation_batch::MutationEnvelope::Operation(envelope) =
        &mut recorded.batch.envelope
    else {
        panic!("fixture operation");
    };
    envelope.authority.nonce = eg_types::contract::Nonce::from_bytes([8; 32]);
    envelope.authority.context_digest = envelope.authority.recompute_context_digest().unwrap();
    recorded.batch.validate().unwrap();
    let retry = recorded.batch.envelope.operation().unwrap();
    assert_eq!(
        retry.operation_identity().unwrap().digest().unwrap(),
        old_operation
    );
    assert_ne!(
        retry.nonce_replay_key().unwrap().digest().unwrap(),
        old_nonce
    );
    let read = kernel.store().begin_read().unwrap();
    let operations = read.open_table(crate::tables::REPLAY_OPERATIONS).unwrap();
    let scope = support::scope().binding_digest().to_hex();
    let row: crate::tables::OperationReplayRow = decode_ledger_record(
        operations
            .get((scope.as_str(), recorded.batch.idempotency_key()))
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap();
    assert_eq!(row.operation_replay_digest, old_operation);
    assert_eq!(
        row.recorded.batch_id(),
        Some(recorded.batch.batch_id.as_str())
    );
    let nonces = read.open_table(crate::tables::REPLAY_NONCES).unwrap();
    assert_eq!(nonces.iter().unwrap().count(), 1);
    assert!(nonces
        .get((
            scope.as_str(),
            retry
                .nonce_replay_key()
                .unwrap()
                .digest()
                .unwrap()
                .to_hex()
                .as_str()
        ))
        .unwrap()
        .is_none());
}

#[test]
fn prepared_private_recovery_requires_authentication_and_survives_upgrade_unchanged() {
    let directory = private_tempdir();
    let path = directory.path().join("sql.redb");
    let record = create_predecessor(&path, true);
    let before = file_digest(&path);
    assert!(inspect(&path).is_err());
    assert_eq!(file_digest(&path), before);
    let token = inspect_sql_source_checkpoint_upgrade(
        &path,
        physical(),
        Some(Arc::new(FixtureIntegrity)),
        inspection_options(&path),
    )
    .unwrap();
    let evidence = token.evidence.clone();
    let (kernel, _) = upgrade_sql_source_checkpoints(token).unwrap();
    assert_eq!(
        encode_bounded(&read_record(&kernel), "readback").unwrap(),
        encode_bounded(&record, "prepared").unwrap()
    );
    let current = strict_recovery_evidence(&kernel).unwrap();
    for name in [
        crate::tables::PRIVATE_PAYLOADS.name(),
        crate::tables::BATCHES.name(),
        crate::tables::SCOPE_BINDINGS.name(),
    ] {
        assert_eq!(
            current.tables.iter().find(|table| table.table_id == name),
            evidence.tables.iter().find(|table| table.table_id == name)
        );
    }
}

#[test]
fn altered_manifest_shapes_and_authority_overflow_fail_without_writes() {
    for case in 0..7 {
        let directory = private_tempdir();
        let path = directory.path().join("sql.redb");
        create_predecessor(&path, false);
        mutate_manifest(&path, |manifest| match case {
            0 => manifest.schema_version = 1,
            1 => manifest.layout = OwnerLayout::Kv,
            2 => manifest.layout_digest[0] ^= 1,
            3 => {
                manifest.tables.pop();
            }
            4 => manifest.tables.push(manifest.tables[0].clone()),
            5 => manifest.tables[0].key_codec = "untrusted".to_string(),
            _ => manifest.authority_epoch = u64::MAX,
        });
        let before = file_digest(&path);
        assert!(inspect(&path).is_err(), "case {case}");
        assert_eq!(file_digest(&path), before, "case {case}");
    }
}

#[test]
fn missing_extra_multimap_and_wrong_typed_tables_fail_without_recreating_them() {
    for case in 0..4 {
        let directory = private_tempdir();
        let path = directory.path().join("sql.redb");
        create_predecessor(&path, false);
        let database = upgrade_builder().open(&path).unwrap();
        let write = database.begin_write().unwrap();
        corrupt_census(&write, case);
        write.commit().unwrap();
        drop(database);
        let before = file_digest(&path);
        assert!(inspect(&path).is_err(), "case {case}");
        assert_eq!(file_digest(&path), before);
    }
}

fn corrupt_census(write: &redb::WriteTransaction, case: usize) {
    match case {
        0 => {
            write
                .delete_table(TableDefinition::<(&str, u64), &[u8]>::new("__sql_rows__"))
                .unwrap();
        }
        1 => {
            write
                .open_table(TableDefinition::<&str, &[u8]>::new("unexpected"))
                .unwrap();
        }
        2 => {
            write
                .open_multimap_table(MultimapTableDefinition::<&str, &str>::new("unexpected"))
                .unwrap();
        }
        _ => {
            write
                .delete_table(TableDefinition::<(&str, u64), &[u8]>::new("__sql_rows__"))
                .unwrap();
            write
                .open_table(TableDefinition::<(&str, u64), u64>::new("__sql_rows__"))
                .unwrap();
        }
    }
}

#[test]
fn copied_roots_and_wrong_physical_owner_are_refused() {
    let directory = private_tempdir();
    let path = directory.path().join("sql.redb");
    create_predecessor(&path, false);
    let before = file_digest(&path);
    assert!(inspect_sql_source_checkpoint_upgrade(
        &path,
        PhysicalStoreIdentity::new("wrong-owner").unwrap(),
        None,
        inspection_options(&path)
    )
    .is_err());
    assert_eq!(file_digest(&path), before);
    let copy = directory.path().join("copied.redb");
    std::fs::copy(&path, &copy).unwrap();
    let before = file_digest(&copy);
    assert!(inspect(&copy).is_err());
    assert_eq!(file_digest(&copy), before);
}

#[test]
fn token_rejects_changed_rows_and_replaced_inode_before_creating_checkpoint() {
    let directory = private_tempdir();
    let path = directory.path().join("sql.redb");
    create_predecessor(&path, false);
    let token = inspect(&path).unwrap();
    let database = upgrade_builder().open(&path).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(TableDefinition::<(&str, u64), &[u8]>::new("__sql_rows__"))
        .unwrap()
        .insert(("tenant-a:source-table", 7), b"changed row".as_slice())
        .unwrap();
    write.commit().unwrap();
    drop(database);
    let before = file_digest(&path);
    assert!(upgrade_sql_source_checkpoints(token).is_err());
    assert_eq!(file_digest(&path), before);
    let token = inspect(&path).unwrap();
    let replacement = directory.path().join("replacement.redb");
    std::fs::copy(&path, &replacement).unwrap();
    std::fs::rename(&replacement, &path).unwrap();
    let before = file_digest(&path);
    assert!(upgrade_sql_source_checkpoints(token).is_err());
    assert_eq!(file_digest(&path), before);
}

#[test]
fn preview_budget_rejects_large_source_before_staging_or_writes() {
    let directory = private_tempdir();
    let path = directory.path().join("sql.redb");
    create_predecessor(&path, false);
    let before = file_digest(&path);
    let staging = directory.path().join("bounded-inspection");
    let size = std::fs::metadata(&path).unwrap().len();
    let options = SqlSourceCheckpointInspectionOptions::new(staging.clone(), size - 1).unwrap();
    assert!(inspect_sql_source_checkpoint_upgrade(&path, physical(), None, options).is_err());
    assert_eq!(file_digest(&path), before);
    assert!(
        !staging.exists(),
        "oversized source cannot allocate a preview"
    );
}

#[test]
fn inspection_refuses_a_live_exclusive_writer_without_changing_bytes() {
    let directory = private_tempdir();
    let path = directory.path().join("sql.redb");
    create_predecessor(&path, false);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let (backend, descriptor) = preview::exclusive_backend(file, &path).unwrap();
    let before = file_digest(&path);
    assert!(inspect(&path).is_err());
    assert_eq!(file_digest(&path), before);
    drop(backend);
    drop(descriptor);
    assert!(inspect(&path).is_ok());
}

#[test]
fn opened_copy_cannot_borrow_restored_original_path_authority() {
    let directory = private_tempdir();
    let path = directory.path().join("sql.redb");
    create_predecessor(&path, false);
    let token = inspect(&path).unwrap();
    let original = directory.path().join("original.redb");
    let copy = directory.path().join("copy.redb");
    std::fs::copy(&path, &copy).unwrap();
    let before_original = file_digest(&path);
    let before_copy = file_digest(&copy);
    assert_eq!(before_original, before_copy);
    std::fs::rename(&path, &original).unwrap();
    std::fs::rename(&copy, &path).unwrap();
    let opened_copy = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    // The descriptor remains B while every later pathname check sees A.
    std::fs::rename(&path, &copy).unwrap();
    std::fs::rename(&original, &path).unwrap();
    assert_eq!(
        StoreIncarnation::derive(&path).unwrap().0,
        token.incarnation
    );
    assert!(upgrade_opened_predecessor(token, opened_copy).is_err());
    assert_eq!(file_digest(&path), before_original);
    assert_eq!(file_digest(&copy), before_copy);
    assert!(inspect(&path).is_ok());
    assert!(inspect(&copy).is_err());
}

#[test]
fn strict_backup_and_adoption_include_the_new_checkpoint_table() {
    let directory = private_tempdir();
    let path = directory.path().join("sql.redb");
    create_predecessor(&path, false);
    let (kernel, _) = upgrade_sql_source_checkpoints(inspect(&path).unwrap()).unwrap();
    let backup = directory.path().join("backup.redb");
    let backup_identity = PhysicalStoreIdentity::new("sql-checkpoint-backup").unwrap();
    let evidence =
        crate::backup_strict_recovery_store(&kernel, &backup, backup_identity.clone()).unwrap();
    assert_eq!(
        evidence
            .tables
            .iter()
            .find(|table| table.table_id == SQL_SOURCE_CHECKPOINTS.name())
            .unwrap()
            .rows,
        0
    );
    let token =
        crate::open_recovery(&backup, backup_identity.clone(), None, OwnerLayout::Sql).unwrap();
    let adopted = crate::adopt_recovery(token, backup_identity).unwrap();
    crate::validate_recovery_store(&adopted).unwrap();
}

// Abrupt child termination without unwinding or core dumps. Only the isolated
// child receives this environment variable; normal tests never change it.
pub(super) fn crash_at(stage: &str) {
    if std::env::var("EG_SQL_CHECKPOINT_UPGRADE_CRASH")
        .ok()
        .as_deref()
        == Some(stage)
    {
        std::process::exit(73);
    }
}

#[test]
fn checkpoint_upgrade_crash_child() {
    let Some(path) = std::env::var_os("EG_SQL_CHECKPOINT_UPGRADE_CHILD_PATH") else {
        return;
    };
    let path = PathBuf::from(path);
    let token = inspect(&path).unwrap();
    upgrade_sql_source_checkpoints(token).unwrap();
    panic!("child must terminate at its requested durable stage");
}

#[test]
fn interruption_exposes_exact_old_or_exact_new_layout_and_retry_is_safe() {
    for stage in [
        "before_table",
        "after_table",
        "after_manifest",
        "after_commit",
    ] {
        let directory = private_tempdir();
        let path = directory.path().join("sql.redb");
        create_predecessor(&path, false);
        let old = snapshot_old(&path);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "owner::sql_checkpoint_upgrade::tests::checkpoint_upgrade_crash_child",
                "--nocapture",
            ])
            .env("EG_SQL_CHECKPOINT_UPGRADE_CHILD_PATH", &path)
            .env("EG_SQL_CHECKPOINT_UPGRADE_CRASH", stage)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(73), "{stage}");
        if stage == "after_commit" {
            let kernel = StorageKernel::open_owner::<SqlOwner>(&path, physical(), None).unwrap();
            assert!(inspect(&path).is_err());
            assert_eq!(
                read_record(&kernel).result_msgpack.as_deref(),
                Some(rmp_serde::to_vec_named(&17_u64).unwrap().as_slice())
            );
        } else {
            let token = inspect(&path).unwrap();
            assert_eq!(token.evidence, old, "{stage}");
            assert!(StorageKernel::open_owner::<SqlOwner>(&path, physical(), None).is_err());
            // The refused ordinary writable open repairs native allocator state.
            // It cannot lend its changed physical bytes to the earlier token.
            let repaired_bytes = file_digest(&path);
            assert!(upgrade_sql_source_checkpoints(token).is_err());
            assert_eq!(file_digest(&path), repaired_bytes, "{stage}");
            let token = inspect(&path).unwrap();
            assert_eq!(token.evidence, old, "{stage}");
            let (kernel, report) = upgrade_sql_source_checkpoints(token).unwrap();
            assert_eq!(report.current_authority_epoch, 1);
            crate::validate_recovery_store(&kernel).unwrap();
        }
    }
}
