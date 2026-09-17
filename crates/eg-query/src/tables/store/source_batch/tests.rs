//! Native rollback, provider CAS, replay and exact source-cell fidelity.

use super::*;
use crate::tables::schema::{Cell, Column, ColumnType, TableSchema};
use eg_types::change_envelope::CursorPosition;
use eg_types::contract::{BoundedVec, Digest256, RecordBytes};
use eg_types::storage_wire::{
    SqlSourceCell, SqlSourceFloat, SqlSourceJson, SqlSourceText, SqlSourceVector,
};
use serde_json::json;

mod support;
use support::{
    batch, change, checkpoint_bytes, commit_first, epoch, id, next, omitted_default_schema,
    one_row, request, result, seeded, with_id_rows, Fixture, Publication,
};

#[test]
fn rows_provider_checkpoint_result_and_outbox_publish_at_the_exact_staged_epoch() {
    let (fixture, request) = seeded();
    let store = fixture.store();
    let before = epoch(store);
    let batch = batch(&request, "first", "source-a", 0);
    let commit = store.commit_source_batch(&batch, 101).unwrap();
    let terminal = result(&commit);
    assert!(!commit.replayed);
    assert_eq!(
        terminal.canonical_digests,
        request.canonical_digests().unwrap()
    );
    assert_eq!(terminal.accepted_position, request.as_batch().position);
    assert_eq!(terminal.affected_count, 1);
    assert_eq!(terminal.committed_source_epoch.get(), before + 1);
    assert_eq!(terminal.committed_source_epoch.get(), epoch(store));
    assert_eq!(
        terminal.authority_digest.as_bytes(),
        &store.authority.source_authority_digest()
    );
    let checkpoint: checkpoint::Checkpoint =
        eg_storage::decode_ledger_record(&checkpoint_bytes(store, &request).unwrap()).unwrap();
    assert_eq!(checkpoint.result, terminal);
    assert_eq!(checkpoint.batch_id, batch.batch_id);
    assert_eq!(store.scan("issues").unwrap().len(), 1);
    assert_eq!(
        store
            .mutation_outbox(&batch.identity, &batch.batch_id)
            .unwrap()
            .len(),
        1
    );
    let persisted = store
        .mutation_batch(&batch.identity, &batch.batch_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        eg_storage::encode_bounded(&persisted, "persisted batch record").unwrap(),
        eg_storage::encode_bounded(&commit.record, "committed batch record").unwrap()
    );
}

#[test]
fn fresh_nonce_replay_after_later_batches_keeps_original_epoch_and_has_no_duplicate_effects() {
    let (fixture, request) = seeded();
    let store = fixture.store();
    let first = batch(&request, "replay", "source-a", 0);
    let original = store.commit_source_batch(&first, 101).unwrap();
    let later = next(&request, 2, 2);
    store
        .commit_source_batch(&batch(&later, "later", "source-a", 1), 102)
        .unwrap();
    let before_epoch = epoch(store);
    let before_checkpoint = checkpoint_bytes(store, &request);
    let retry = batch(&request, "replay", "source-a", 2);
    let replayed = store.commit_source_batch(&retry, 103).unwrap();
    assert!(replayed.replayed);
    assert_eq!(
        eg_storage::encode_bounded(&replayed.record, "replayed batch record").unwrap(),
        eg_storage::encode_bounded(&original.record, "original batch record").unwrap()
    );
    assert_eq!(epoch(store), before_epoch);
    assert_eq!(checkpoint_bytes(store, &request), before_checkpoint);
    assert_eq!(store.scan("issues").unwrap().len(), 2);
    assert_eq!(
        store
            .mutation_outbox(&first.identity, &first.batch_id)
            .unwrap()
            .len(),
        1
    );
    let duplicate = store.commit_source_batch(&retry, 104).unwrap_err();
    assert!(duplicate.contains("REPLAY"), "{duplicate}");
    let changed = change(&request, |request| {
        request.rows = one_row(vec![SqlSourceCell::Int(99), SqlSourceCell::Null])
    });
    let error = store
        .commit_source_batch(&batch(&changed, "replay", "source-a", 2), 105)
        .unwrap_err();
    assert!(error.contains("IDEMPOTENCY_CONFLICT"), "{error}");
    assert_eq!(epoch(store), before_epoch);
}

#[test]
fn checkpoint_compare_and_swap_is_serialized_across_distinct_admitted_owner_scopes() {
    let (fixture, request) = seeded();
    let store = fixture.store();
    commit_first(store, &request, "initial");
    let winner = next(&request, 2, 2);
    let loser = next(&request, 3, 3);
    let before = epoch(store);
    // Hold the actual winning owner write while the already-prepared loser
    // starts on another scope. Ledger OCC cannot conceal the provider CAS.
    let winning_batch = batch(&winner, "winner", "scope-winner", 0);
    let invocation = Invocation::check(store, &winning_batch).unwrap();
    let (mut mutation, begun) = store.authority.begin_operation(&winning_batch).unwrap();
    let Begin::Apply { source_version } = begun else {
        panic!("fresh winner");
    };
    mutation.set_source_version(source_version);
    let competing_store = store.clone();
    let competing_batch = batch(&loser, "loser", "scope-loser", 0);
    let (started, received) = std::sync::mpsc::channel();
    let competitor = std::thread::spawn(move || {
        started.send(()).unwrap();
        competing_store.commit_source_batch(&competing_batch, 103)
    });
    received
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    publish(store, mutation, &winning_batch, &invocation, 102, None).unwrap();
    let error = competitor.join().unwrap().unwrap_err();
    assert!(error.contains("compare-and-swap"), "{error}");
    assert_eq!(epoch(store), before + 1);
    assert_eq!(store.scan("issues").unwrap().len(), 2);
    assert!(store
        .mutation_batch(
            &batch(&loser, "loser", "scope-loser", 0).identity,
            "batch-loser"
        )
        .unwrap()
        .is_none());
}

#[test]
fn interruption_at_each_native_boundary_rolls_back_or_replays_one_complete_publication() {
    for point in [
        SqlMutationCrashpoint::BeforeRows,
        SqlMutationCrashpoint::AfterRowsBeforeMetadata,
        SqlMutationCrashpoint::BeforeCommit,
        SqlMutationCrashpoint::AfterCommitBeforeAck,
    ] {
        let (fixture, request) = seeded();
        let store = fixture.store();
        let original = batch(&request, "interruption", "source-a", 0);
        let before = epoch(store);
        assert!(commit(store, &original, 101, Some(point)).is_err());
        let committed = point == SqlMutationCrashpoint::AfterCommitBeforeAck;
        assert_eq!(
            Publication::observe(store, &request, &original),
            Publication::expected(
                before + u64::from(committed),
                usize::from(committed),
                committed
            )
        );
        let retry = batch(&request, "interruption", "source-a", u64::from(committed));
        let recovered = store.commit_source_batch(&retry, 102).unwrap();
        assert_eq!(recovered.replayed, committed);
        assert_eq!(epoch(store), before + 1);
        assert_eq!(store.scan("issues").unwrap().len(), 1);
    }
}

#[test]
fn descriptor_mapping_table_and_schema_changes_cannot_rebind_a_provider_stream() {
    for case in 0..5 {
        let (fixture, request) = seeded();
        let store = fixture.store();
        commit_first(store, &request, "original");
        let changed = change(&next(&request, 2, 2), |request| match case {
            0 => request.source_descriptor.dataset = id("other-dataset"),
            1 => {
                request.mapping_descriptor.content =
                    RecordBytes::new(b"other mapping".to_vec()).unwrap()
            }
            2 => request.table = id("other-table"),
            3 => request.expected_schema_digest = Digest256::from_bytes([7; 32]),
            _ => request.expected_schema_version = 1,
        });
        let before_epoch = epoch(store);
        let before_checkpoint = checkpoint_bytes(store, &request);
        assert!(
            store
                .commit_source_batch(&batch(&changed, "changed", "other-scope", 0), 102)
                .is_err(),
            "case {case}"
        );
        assert_eq!(epoch(store), before_epoch);
        assert_eq!(checkpoint_bytes(store, &request), before_checkpoint);
        assert_eq!(store.scan("issues").unwrap().len(), 1);
    }
}

#[test]
fn schema_change_after_request_preparation_is_seen_inside_the_admitted_writer() {
    let (fixture, request) = seeded();
    let store = fixture.store();
    store
        .add_column(
            "issues",
            Column::new("extra", ColumnType::Text, true, false),
        )
        .unwrap();
    let before = epoch(store);
    let error = store
        .commit_source_batch(&batch(&request, "schema-race", "source-a", 0), 101)
        .unwrap_err();
    assert!(error.contains("schema compare-and-swap"), "{error}");
    assert_eq!(epoch(store), before);
    assert!(checkpoint_bytes(store, &request).is_none());
    assert!(store.scan("issues").unwrap().is_empty());
}

#[test]
fn partition_and_source_have_separate_cursors_and_cross_tenant_batch_is_refused() {
    let (fixture, request) = seeded();
    let store = fixture.store();
    commit_first(store, &request, "original");
    for (source, partition, row_id) in [("jira", "project-b", 2), ("servicenow", "project-a", 3)] {
        let separate = change(&request, |request| {
            request.source = id(source);
            request.partition = SqlSourceText::new(partition.into()).unwrap();
            request.rows = one_row(vec![SqlSourceCell::Int(row_id), SqlSourceCell::Null]);
        });
        let commit = store
            .commit_source_batch(&batch(&separate, source, source, 0), 102)
            .unwrap();
        assert_eq!(
            result(&commit).accepted_position,
            CursorPosition::Sequence(1)
        );
    }
    let wrong = support::batch_in_tenant(&request, "tenant-tamper", "source-a", 1, "tenant-b");
    let before = epoch(store);
    let error = store.commit_source_batch(&wrong, 103).unwrap_err();
    assert!(error.contains("tenant"), "{error}");
    assert_eq!(epoch(store), before);
    assert_eq!(store.scan("issues").unwrap().len(), 3);
}

#[test]
fn opaque_provider_tokens_use_exact_cas_and_never_lexical_ordering() {
    let (fixture, seeded_request) = seeded();
    let store = fixture.store();
    let request = change(&seeded_request, |request| {
        request.position = CursorPosition::Opaque {
            cursor_type: "jira-page".into(),
            value: "z".into(),
        };
    });
    store
        .commit_source_batch(&batch(&request, "opaque-start", "source-a", 0), 101)
        .unwrap();
    let lexical_backwards = change(&request, |request| {
        request.expected_previous = Some(request.position.clone());
        request.rows = one_row(vec![SqlSourceCell::Int(2), SqlSourceCell::Null]);
        request.position = CursorPosition::Opaque {
            cursor_type: "jira-page".into(),
            value: "a".into(),
        };
    });
    store
        .commit_source_batch(
            &batch(&lexical_backwards, "opaque-next", "source-a", 1),
            102,
        )
        .unwrap();
    let stale = change(&lexical_backwards, |request| {
        request.position = CursorPosition::Opaque {
            cursor_type: "jira-page".into(),
            value: "b".into(),
        };
        request.expected_previous = Some(CursorPosition::Opaque {
            cursor_type: "jira-page".into(),
            value: "stale".into(),
        });
    });
    let error = store
        .commit_source_batch(&batch(&stale, "opaque-stale", "other-scope", 0), 103)
        .unwrap_err();
    assert!(error.contains("compare-and-swap"), "{error}");
}

#[test]
fn native_cells_preserve_binary_json_null_sql_null_extremes_and_finite_vectors() {
    let schema = TableSchema::new(
        "issues",
        vec![
            Column::new("id", ColumnType::BigInt, false, true),
            Column::new("negative_zero", ColumnType::Double, false, false),
            Column::new("text", ColumnType::Text, false, false),
            Column::new("flag", ColumnType::Bool, false, false),
            Column::new("timestamp", ColumnType::Timestamp, false, false),
            Column::new("bytes", ColumnType::Bytes, false, false),
            Column::new("json_null", ColumnType::Json, false, false),
            Column::new("sql_null", ColumnType::Json, true, false),
            Column::new("document", ColumnType::Json, false, false),
            Column::new("embedding", ColumnType::Vector(Some(2)), false, false),
        ],
    );
    let fixture = Fixture::new(&schema);
    let store = fixture.store();
    let bytes = vec![0, 10, 255, 0];
    let vector = vec![f32::MAX, f32::MIN_POSITIVE];
    let document = json!({"n":u64::MAX,"a":{"z":2,"b":1},"array":[null,false]});
    let request = request(
        &schema,
        vec![
            SqlSourceCell::Int(i64::MAX),
            SqlSourceCell::FiniteFloat(SqlSourceFloat::new(-0.0).unwrap()),
            SqlSourceCell::Text(SqlSourceText::new("東京\nissue".into()).unwrap()),
            SqlSourceCell::Bool(false),
            SqlSourceCell::Timestamp(i64::MIN),
            SqlSourceCell::Bytes(RecordBytes::new(bytes.clone()).unwrap()),
            SqlSourceCell::Json(SqlSourceJson::new(json!(null)).unwrap()),
            SqlSourceCell::Null,
            SqlSourceCell::Json(SqlSourceJson::new(document.clone()).unwrap()),
            SqlSourceCell::FiniteVector(SqlSourceVector::new(vector.clone()).unwrap()),
        ],
    );
    store
        .commit_source_batch(&batch(&request, "fidelity", "source-a", 0), 101)
        .unwrap();
    let rows = store.scan("issues").unwrap();
    let Cell::Float(negative_zero) = rows[0][1] else {
        panic!("expected float");
    };
    assert_eq!(negative_zero.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(rows[0][0], Cell::Int(i64::MAX));
    assert_eq!(rows[0][2], Cell::Text("東京\nissue".into()));
    assert_eq!(rows[0][3], Cell::Bool(false));
    assert_eq!(rows[0][4], Cell::Timestamp(i64::MIN));
    assert_eq!(rows[0][5], Cell::Bytes(bytes));
    assert_eq!(rows[0][6], Cell::Json(serde_json::Value::Null));
    assert_eq!(rows[0][7], Cell::Null);
    assert_eq!(rows[0][8], Cell::Json(document));
    assert_eq!(rows[0][9], Cell::Vector(vector));
}

#[test]
fn sql_null_not_null_type_mismatch_and_vector_dimension_abort_without_publication() {
    let schema = TableSchema::new(
        "issues",
        vec![
            Column::new("id", ColumnType::BigInt, false, true),
            Column::new("payload", ColumnType::Json, false, false),
            Column::new("embedding", ColumnType::Vector(Some(2)), false, false),
        ],
    );
    for bad in [
        vec![
            SqlSourceCell::Int(1),
            SqlSourceCell::Null,
            SqlSourceCell::FiniteVector(SqlSourceVector::new(vec![1.0, 2.0]).unwrap()),
        ],
        vec![
            SqlSourceCell::Text(SqlSourceText::new("1".into()).unwrap()),
            SqlSourceCell::Json(SqlSourceJson::new(json!(null)).unwrap()),
            SqlSourceCell::FiniteVector(SqlSourceVector::new(vec![1.0, 2.0]).unwrap()),
        ],
        vec![
            SqlSourceCell::Int(1),
            SqlSourceCell::Json(SqlSourceJson::new(json!(null)).unwrap()),
            SqlSourceCell::FiniteVector(SqlSourceVector::new(vec![1.0]).unwrap()),
        ],
    ] {
        let fixture = Fixture::new(&schema);
        let store = fixture.store();
        let request = request(&schema, bad);
        let before = epoch(store);
        assert!(store
            .commit_source_batch(&batch(&request, "invalid-cell", "source-a", 0), 101)
            .is_err());
        assert_eq!(epoch(store), before);
        assert!(store.scan("issues").unwrap().is_empty());
        assert!(checkpoint_bytes(store, &request).is_none());
    }
}

#[test]
fn typed_insert_reuses_defaults_serial_uniqueness_and_foreign_key_checks() {
    use crate::tables::schema::{RefAction, TableConstraint};
    let mut serial = Column::new("id", ColumnType::BigInt, false, true);
    serial.serial = true;
    let mut parent = Column::new("parent_id", ColumnType::BigInt, false, false);
    parent.unique = true;
    let mut title = Column::new("title", ColumnType::Text, false, false);
    title.default = Some(json!("untitled"));
    let schema = TableSchema::new(
        "issues",
        vec![
            serial,
            parent,
            title,
            Column::new("payload", ColumnType::Json, false, false),
        ],
    )
    .with_constraints(vec![TableConstraint::ForeignKey {
        name: Some("issues_parent".into()),
        columns: vec!["parent_id".into()],
        ref_table: "parents".into(),
        ref_columns: vec!["id".into()],
        on_delete: RefAction::NoAction,
        on_update: RefAction::NoAction,
    }]);
    let parents = TableSchema::new(
        "parents",
        vec![Column::new("id", ColumnType::BigInt, false, true)],
    );
    let fixture = Fixture::new(&parents);
    let store = fixture.store();
    store.create_table(&schema, false).unwrap();
    store
        .insert_rows("parents", &["id".into()], &[vec![json!(1)]])
        .unwrap();
    let request = change(
        &request(&schema, vec![SqlSourceCell::Int(1), SqlSourceCell::Int(1)]),
        |request| {
            request.columns = BoundedVec::new(vec![id("parent_id"), id("payload")]).unwrap();
            request.rows = one_row(vec![
                SqlSourceCell::Int(1),
                SqlSourceCell::Json(SqlSourceJson::new(json!(null)).unwrap()),
            ]);
        },
    );
    store
        .commit_source_batch(&batch(&request, "defaults", "source-a", 0), 101)
        .unwrap();
    assert_eq!(
        store.scan("issues").unwrap()[0],
        vec![
            Cell::Int(1),
            Cell::Int(1),
            Cell::Text("untitled".into()),
            Cell::Json(serde_json::Value::Null)
        ]
    );
    for parent_id in [1, 2] {
        let bad = change(&request, |request| {
            request.expected_previous = Some(request.position.clone());
            request.position = CursorPosition::Sequence(2);
            request.rows = one_row(vec![
                SqlSourceCell::Int(parent_id),
                SqlSourceCell::Json(SqlSourceJson::new(json!(null)).unwrap()),
            ]);
        });
        let before = epoch(store);
        assert!(store
            .commit_source_batch(
                &batch(&bad, &format!("bad-parent-{parent_id}"), "other-scope", 0),
                102
            )
            .is_err());
        assert_eq!(epoch(store), before);
        assert_eq!(store.scan("issues").unwrap().len(), 1);
    }
    // Ordinary SQL still uses its original JSON literal builder and defaults.
    store
        .insert_rows("parents", &["id".into()], &[vec![json!(2)]])
        .unwrap();
    store
        .insert_rows(
            "issues",
            &["parent_id".into(), "payload".into()],
            &[vec![json!(2), json!({"sql":"literal"})]],
        )
        .unwrap();
    assert_eq!(
        store.scan("issues").unwrap()[1],
        vec![
            Cell::Int(2),
            Cell::Int(2),
            Cell::Text("untitled".into()),
            Cell::Json(json!({"sql":"literal"}))
        ]
    );
}

#[test]
fn terminal_receipt_tampering_and_wrong_operation_shapes_fail_closed() {
    let (fixture, request) = seeded();
    let store = fixture.store();
    let original = batch(&request, "report", "source-a", 0);
    let commit = store.commit_source_batch(&original, 101).unwrap();
    let invocation = Invocation::check(store, &original).unwrap();
    for case in 0..4 {
        let mut changed = commit.clone();
        let mut report = result(&changed);
        match case {
            0 => report.canonical_digests.batch_digest = Digest256::from_bytes([7; 32]),
            1 => report.accepted_position = CursorPosition::Sequence(99),
            2 => report.affected_count = 99,
            _ => {
                changed.record.result_msgpack =
                    Some(rmp_serde::to_vec_named(&json!({"success":true})).unwrap());
            }
        }
        if case != 3 {
            changed.record.result_msgpack =
                Some(eg_storage::encode_bounded(&report, "changed report").unwrap());
        }
        assert!(
            validate_terminal_result(changed, &invocation).is_err(),
            "case {case}"
        );
    }
    let mut wrong = batch(&request, "wrong-shape", "source-a", 1);
    wrong.operations[0].method = Method::ApplyMutation {
        event_type: "sql_source".into(),
        query: "arbitrary".into(),
    };
    wrong
        .reseal_envelope(Digest256::from_bytes([0; 32]))
        .unwrap();
    assert!(store.commit_source_batch(&wrong, 102).is_err());
    assert_eq!(epoch(store), result(&commit).committed_source_epoch.get());
}

#[test]
fn missing_previous_and_corrupted_checkpoint_key_bindings_cannot_reset_a_stream() {
    for case in 0..4 {
        let (fixture, request) = seeded();
        let store = fixture.store();
        let missing = change(&request, |request| {
            request.position = CursorPosition::Sequence(2);
            request.expected_previous = Some(CursorPosition::Sequence(1));
        });
        assert!(store
            .commit_source_batch(&batch(&missing, "missing", "source-a", 0), 101)
            .is_err());
        assert!(checkpoint_bytes(store, &request).is_none());
        store
            .commit_source_batch(&batch(&request, "start", "source-a", 0), 102)
            .unwrap();
        let mut image: serde_json::Value =
            rmp_serde::from_slice(&checkpoint_bytes(store, &request).unwrap()).unwrap();
        match case {
            0 => image["tenant"] = json!("tenant-b"),
            1 => image["source"] = json!("other-source"),
            2 => image["partition"] = json!("other-partition"),
            _ => image["schema_version"] = json!(99),
        }
        let encoded = eg_storage::encode_bounded(&image, "corrupted checkpoint").unwrap();
        store
            .authority
            .maintain("checkpoint-fixture", "jira", |write| {
                write
                    .open_table(eg_storage::SQL_SOURCE_CHECKPOINTS)?
                    .insert(("tenant-a", "jira", "project-a"), encoded.as_slice())
                    .map_err(|error| error.to_string())?;
                Ok(())
            })
            .unwrap();
        let before = epoch(store);
        let error = store
            .commit_source_batch(
                &batch(&next(&request, 2, 2), "after-corruption", "other-scope", 0),
                103,
            )
            .unwrap_err();
        assert!(error.contains("binding"), "{error}");
        assert_eq!(epoch(store), before);
        assert_eq!(store.scan("issues").unwrap().len(), 1);
        assert_eq!(checkpoint_bytes(store, &request), Some(encoded));
    }
}

#[test]
fn source_publication_crash_child() {
    let Some(path) = std::env::var_os("EG_SQL_SOURCE_PUBLICATION_CRASH_CHILD") else {
        return;
    };
    let store = TableStore::open_scoped(
        std::path::PathBuf::from(path),
        "tenant-a",
        super::super::dev_scope_grant::dev_verifier(),
        super::super::dev_scope_grant::DEV_PRINCIPAL,
        super::super::dev_scope_grant::DEV_PROOF,
    )
    .unwrap();
    let schema = store.get_schema("issues").unwrap().unwrap();
    let request = request(&schema, vec![SqlSourceCell::Int(1), SqlSourceCell::Null]);
    let batch = batch(&request, "process-crash", "source-a", 0);
    let invocation = Invocation::check(&store, &batch).unwrap();
    let (mut mutation, begun) = store.authority.begin_operation(&batch).unwrap();
    let Begin::Apply { source_version } = begun else {
        panic!("fresh crash fixture");
    };
    mutation.set_source_version(source_version);
    let terminal = mutation
        .owner_rows_with_epoch(
            |write| apply_rows(&store, write, &batch, &invocation, None),
            |write, affected, epoch| {
                checkpoint::finalize(write, &batch, &invocation, affected, epoch)
            },
        )
        .unwrap();
    mutation.finish(Some(terminal), 101).unwrap();
    // Genuine abrupt termination with rows, epoch, checkpoint, kernel receipt
    // and outbox staged. No unwinding, global live fault arm or core dump.
    std::process::exit(73);
}

#[test]
fn process_death_after_staged_checkpoint_and_receipt_recovers_no_partial_publication() {
    let (mut fixture, request) = seeded();
    let before = epoch(fixture.store());
    let path = fixture.path();
    fixture.close();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tables::store::source_batch::tests::source_publication_crash_child",
            "--nocapture",
        ])
        .env("EG_SQL_SOURCE_PUBLICATION_CRASH_CHILD", &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    fixture.reopen();
    let store = fixture.store();
    let retry = batch(&request, "process-crash", "source-a", 0);
    assert_eq!(
        Publication::observe(store, &request, &retry),
        Publication::expected(before, 0, false)
    );
    let committed = store.commit_source_batch(&retry, 102).unwrap();
    assert!(!committed.replayed);
    assert_eq!(result(&committed).committed_source_epoch.get(), before + 1);
}

#[test]
fn omitted_default_amplification_is_refused_without_any_publication() {
    let schema = omitted_default_schema(64 * 1024);
    let fixture = Fixture::new(&schema);
    let store = fixture.store();
    let request = with_id_rows(&request(&schema, vec![SqlSourceCell::Int(1)]), 1024);
    assert!(request.canonical_bytes().unwrap().len() < 128 * 1024);
    let before = epoch(store);
    let batch = batch(&request, "amplification", "source-a", 0);
    let error = store.commit_source_batch(&batch, 101).unwrap_err();
    assert!(error.contains("materialization"), "{error}");
    assert_eq!(
        Publication::observe(store, &request, &batch),
        Publication::expected(before, 0, false)
    );
    // The native allocator is also untouched by failed admission.
    store
        .insert_rows("issues", &["id".into()], &[vec![json!(1)]])
        .unwrap();
    let snapshot = store.row_snapshot("issues", None, None).unwrap();
    assert_eq!(snapshot.rows[0].row_id, 0);
}

#[test]
fn aggregate_materialization_reservation_accepts_its_boundary_and_rejects_one_byte_more() {
    // Per row: <=5-byte array header, <=32-byte Int cell and Text's 32-byte
    // enum/length allowance. The default content occupies the remaining bound.
    let rows = 1024;
    let maximum_default =
        eg_types::storage_wire::source_batch::MAX_SQL_SOURCE_BATCH_BYTES / rows - 69;
    for (extra, succeeds) in [(0, true), (1, false)] {
        let schema = omitted_default_schema(maximum_default + extra);
        let fixture = Fixture::new(&schema);
        let store = fixture.store();
        let request = with_id_rows(&request(&schema, vec![SqlSourceCell::Int(1)]), rows as i64);
        let before = epoch(store);
        let batch = batch(&request, "boundary", "source-a", 0);
        let committed = store.commit_source_batch(&batch, 101);
        assert_eq!(committed.is_ok(), succeeds);
        assert_eq!(
            Publication::observe(store, &request, &batch),
            Publication::expected(
                before + u64::from(succeeds),
                if succeeds { rows } else { 0 },
                succeeds
            )
        );
    }
}

#[test]
fn wide_omitted_null_rows_are_rejected_by_structural_expansion_before_cloning_cells() {
    let mut columns = vec![Column::new("id", ColumnType::BigInt, false, true)];
    columns.extend(
        (1..1024).map(|n| Column::new(format!("extra_{n}"), ColumnType::Json, true, false)),
    );
    let schema = TableSchema::new("issues", columns);
    let fixture = Fixture::new(&schema);
    let store = fixture.store();
    let request = with_id_rows(&request(&schema, vec![SqlSourceCell::Int(1)]), 1024);
    // Codec reservation is below 16MiB; omitted cell/tag nodes independently
    // exceed the same standard structural ceiling used by the bounded decoder.
    assert!(
        1024 * (5 + 32 + 1023 * 8)
            < eg_types::storage_wire::source_batch::MAX_SQL_SOURCE_BATCH_BYTES
    );
    let before = epoch(store);
    let batch = batch(&request, "wide-expansion", "source-a", 0);
    let error = store.commit_source_batch(&batch, 101).unwrap_err();
    assert!(error.contains("structural"), "{error}");
    assert_eq!(
        Publication::observe(store, &request, &batch),
        Publication::expected(before, 0, false)
    );
}
