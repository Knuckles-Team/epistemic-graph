//! The eg-ann contract request (RF-019 / EH-352): generations survive a
//! restart, an UPDATE is exact before any refresh and is folded into the next
//! generation incrementally, staleness is per table, and a table's ANN state is
//! forgotten with it.

use std::sync::Arc;

use super::*;
use crate::tables::ann_authority::durable::{encode, verified_manifest, Encoding};

/// Far outside every fixture cluster (their coordinates stay within ±3).
fn far_vector() -> Vec<f32> {
    vec![9.0; DIM]
}

fn every_method() -> [AnnIndexPlan; 5] {
    [
        plan(AnnMethod::Hnsw, VectorMetric::L2),
        plan(AnnMethod::Hnsw, VectorMetric::Cosine),
        plan(AnnMethod::IvfFlat, VectorMetric::L2),
        plan(AnnMethod::IvfFlat, VectorMetric::Cosine),
        plan(AnnMethod::IvfFlat, VectorMetric::InnerProduct),
    ]
}

fn reopen(path: &std::path::Path) -> TableStore {
    TableStore::open(path, dev_verifier(), DEV_PRINCIPAL, DEV_PROOF).unwrap()
}

fn set_vector(store: &TableStore, table: &str, id: i64, vector: &[f32]) {
    let mut set = serde_json::Map::new();
    set.insert("emb".to_string(), json!(vector));
    let selector = RowPredicate::Cmp {
        col: "id".to_string(),
        op: CmpOp::Eq,
        value: json!(id),
    };
    assert_eq!(store.update_where(table, &set, &selector).unwrap(), 1);
}

fn live(store: &TableStore, index: &AnnIndexPlan) -> Arc<AnnGeneration> {
    store
        .ann_authority()
        .existing_slot(&TableStore::ann_index_key(index))
        .and_then(|slot| slot.live_for(index.method))
        .expect("a live generation")
}

fn status_of(store: &TableStore, table: &str) -> AnnIndexStatus {
    store
        .ann_index_status()
        .unwrap()
        .into_iter()
        .find(|status| status.table == table)
        .unwrap()
}

#[test]
fn every_method_serves_its_persisted_generation_after_a_restart() {
    let rows = vectors(300, 40);
    let queries = queries_near(&rows, 5, 41);
    for index in every_method() {
        let (store, path) = open_docs(&rows, &index);
        refresh(&store);
        let before: Vec<Vec<i64>> = queries
            .iter()
            .map(|query| ids(&top(&store, &index, query, 5, None).rows))
            .collect();
        drop(store);

        let reopened = reopen(&path);
        for (query, want) in queries.iter().zip(&before) {
            let answer = top(&reopened, &index, query, 5, None);
            assert_eq!(answer.receipt.path, maintained(1), "{index:?}");
            assert_eq!(&ids(&answer.rows), want, "{index:?}");
        }
    }
}

#[test]
fn an_update_is_served_exactly_before_any_refresh() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(600, 30), &index);
    refresh(&store);
    let target = far_vector();
    set_vector(&store, "docs", 7, &target);

    let answer = top(&store, &index, &target, 3, None);

    assert_eq!(
        answer.receipt.path,
        maintained(1),
        "served without a refresh"
    );
    assert_eq!(ids(&answer.rows)[0], 7, "ranked on its NEW vector");
    assert_eq!(
        ids(&answer.rows),
        exact_ids(&store, &index, &target, 3, None)
    );
}

#[test]
fn an_update_is_folded_into_the_next_generation_without_a_rebuild() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(200, 31), &index);
    refresh(&store);
    set_vector(&store, "docs", 11, &far_vector());
    assert!(status_of(&store, "docs").stale);

    assert!(matches!(
        refresh(&store).as_slice(),
        [AnnRefreshOutcome::Activated { generation: 2, .. }]
    ));

    let extension = live(&store, &index);
    assert_eq!(
        (extension.base, extension.delta_len()),
        (Some(1), 1),
        "an extension of generation 1, not a rebuild"
    );
    assert!(
        store
            .ann_changed_rows("docs", "emb", 0, 16)
            .unwrap()
            .rows
            .is_empty(),
        "the change the extension covers was pruned from the log"
    );
    let answer = top(&store, &index, &far_vector(), 1, None);
    assert_eq!(answer.receipt.path, maintained(2));
    assert_eq!(ids(&answer.rows), vec![11], "the generation indexes it now");
    assert!(!status_of(&store, "docs").stale);
}

#[test]
fn an_extension_is_restored_on_its_base_after_a_restart() {
    for index in [hnsw_l2(), plan(AnnMethod::IvfFlat, VectorMetric::L2)] {
        let (store, path) = open_docs(&vectors(200, 32), &index);
        refresh(&store);
        set_vector(&store, "docs", 5, &far_vector());
        refresh(&store);
        drop(store);

        let reopened = reopen(&path);
        let status = status_of(&reopened, "docs");
        assert_eq!(
            (status.state, status.stale, status.generation),
            (ManagedIndexState::Active, false, Some(2)),
            "{index:?}"
        );
        assert_eq!(live(&reopened, &index).base, Some(1), "{index:?}");
        let answer = top(&reopened, &index, &far_vector(), 1, None);
        assert_eq!(answer.receipt.path, maintained(2), "{index:?}");
        assert_eq!(ids(&answer.rows), vec![5], "{index:?}");
    }
}

#[test]
fn recall_holds_after_incremental_updates() {
    let rows = vectors(600, 50);
    let moved = vectors(40, 51);
    let mut queries = queries_near(&rows, 20, 52);
    queries.extend(moved.iter().take(10).cloned());
    for index in every_method() {
        let (store, _path) = open_docs(&rows, &index);
        refresh(&store);
        for (offset, vector) in moved.iter().enumerate() {
            set_vector(&store, "docs", (offset * 13) as i64, vector);
        }
        refresh(&store);
        assert_eq!(live(&store, &index).base, Some(1), "{index:?}");

        let mut total = 0.0;
        for query in &queries {
            let answer = top(&store, &index, query, 10, None);
            assert_eq!(answer.receipt.path, maintained(2), "{index:?}");
            total += recall(
                &ids(&answer.rows),
                &exact_ids(&store, &index, query, 10, None),
            );
        }
        let mean = total / queries.len() as f64;
        assert!(mean >= 0.9, "{index:?}: mean recall@10 {mean} < 0.9");
    }
}

fn open_notes(store: &TableStore, index: &AnnIndexPlan, rows: &[Vec<f32>]) {
    let schema = TableSchema::new(
        "notes",
        vec![
            Column::new("id", ColumnType::BigInt, false, true),
            Column::new("owner", ColumnType::Text, true, false),
            Column::new("emb", ColumnType::Vector(Some(DIM)), true, false),
        ],
    );
    store.create_table(&schema, false).unwrap();
    insert_notes(store, 0, rows);
    store.put_ann_index(index).unwrap();
}

fn insert_notes(store: &TableStore, first_id: i64, rows: &[Vec<f32>]) {
    let columns = vec!["id".to_string(), "owner".to_string(), "emb".to_string()];
    let values: Vec<Vec<Value>> = rows
        .iter()
        .zip(first_id..)
        .map(|(vector, id)| vec![json!(id), owner_of(id), json!(vector)])
        .collect();
    store.insert_rows("notes", &columns, &values).unwrap();
}

#[test]
fn staleness_is_tracked_per_table() {
    let docs = hnsw_l2();
    let notes = AnnIndexPlan {
        table: "notes".to_string(),
        name: Some("notes_emb".to_string()),
        ..hnsw_l2()
    };
    let (store, _path) = open_docs(&vectors(80, 60), &docs);
    open_notes(&store, &notes, &vectors(80, 61));
    refresh(&store);

    insert_notes(&store, 80, &vectors(1, 62));

    let docs_status = status_of(&store, "docs");
    assert_eq!(
        (docs_status.stale, docs_status.lag_epochs),
        (false, 0),
        "a write to another table never stales this index"
    );
    assert!(status_of(&store, "notes").stale);
    let outcomes = refresh(&store);
    assert!(outcomes.iter().any(|outcome| matches!(
        outcome,
        AnnRefreshOutcome::Current { index, generation: 1 } if index.starts_with("docs.")
    )));
    assert!(outcomes.iter().any(|outcome| matches!(
        outcome,
        AnnRefreshOutcome::Activated { index, generation: 2, .. } if index.starts_with("notes.")
    )));
}

#[test]
fn a_tampered_generation_payload_is_refused_by_its_digest() {
    let index = hnsw_l2();
    let source = AnnSourceRows {
        epoch: 3,
        dim: Some(DIM),
        max_rowid: Some(29),
        rows: vectors(30, 70)
            .into_iter()
            .enumerate()
            .map(|(rowid, vector)| (rowid as u64, vector))
            .collect(),
    };
    let generation = AnnGeneration::build(1, index.method, index.metric, source);
    let mut stored = encode(&generation, Encoding::Full).unwrap();
    assert!(verified_manifest(&stored, &index).is_ok());

    stored.payload[0] ^= 1;

    let error = verified_manifest(&stored, &index).unwrap_err();
    assert!(error.contains("digest"), "{error}");
}

#[test]
fn dropping_the_table_forgets_its_registrations_generations_and_changes() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(40, 80), &index);
    refresh(&store);
    insert(&store, 40, &vectors(2, 81));
    let key = TableStore::ann_index_key(&index);
    assert!(store.live_ann_generation(&key).unwrap().is_some());
    assert!(store.ann_table_change_epoch("docs").unwrap() > 0);

    store.drop_table("docs", false).unwrap();

    assert!(store.list_ann_indexes().unwrap().is_empty());
    assert!(store.live_ann_generation(&key).unwrap().is_none());
    assert_eq!(store.ann_table_change_epoch("docs").unwrap(), 0);
}
