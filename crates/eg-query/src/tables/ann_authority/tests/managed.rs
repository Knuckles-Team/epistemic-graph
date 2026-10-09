//! EH-352: the SQL family of the user-managed index lifecycle — typed
//! create/status/drop, `requested -> backfilling -> active | blocked` with typed
//! bounded diagnostics, a fenced drop, and the status as a SQL catalog relation.

use eg_core::index::{IndexBlockReason, ManagedIndexTarget};

use super::*;
use crate::tables::ann_authority::durable::{encode, Encoding};
use crate::tables::store::GenerationWrite;

fn status_relation(store: &TableStore) -> Vec<Vec<Value>> {
    crate::sql::exec_sql_typed_with_tables(
        &GraphView::default(),
        store,
        "SELECT index_name, family, target_kind, relation_name, attribute_name, state, \
         generation, indexed, block_reason FROM information_schema.eg_index_status \
         ORDER BY index_name",
    )
    .unwrap()
    .rows
}

fn docs_without_index(rows: usize) -> TableStore {
    let (store, _path) = TableStore::open_temp().unwrap();
    store
        .create_table(&vector_table_schema("docs"), false)
        .unwrap();
    insert(&store, 0, &vectors(rows, 90));
    store
}

#[test]
fn the_typed_lifecycle_runs_requested_backfilling_active_then_drops() {
    let store = docs_without_index(50);
    let index = hnsw_l2();

    let created = store.create_ann_index(&index).unwrap();
    assert_eq!(
        (created.state, created.generation),
        (ManagedIndexState::Requested, None)
    );

    let slot = store
        .ann_authority()
        .slot(&TableStore::ann_index_key(&index));
    let ticket = slot
        .begin_build(AnnRefreshPolicy::Immediate, Duration::ZERO)
        .ok()
        .unwrap();
    assert_eq!(
        store.ann_index_status().unwrap()[0].state,
        ManagedIndexState::Backfilling,
        "a running first build backfills; queries take the exact path"
    );
    drop(ticket);

    // The held ticket numbered generation 1; the worker's build is generation 2.
    refresh(&store);
    let managed = store.managed_index_status().unwrap();
    assert_eq!(managed.len(), 1);
    assert_eq!(
        (&managed[0].name, managed[0].state, managed[0].indexed),
        (&"docs_emb".to_string(), ManagedIndexState::Active, Some(50))
    );
    assert_eq!(
        managed[0].target,
        ManagedIndexTarget::TableColumn {
            table: "docs".to_string(),
            column: "emb".to_string()
        }
    );
    assert_eq!(
        status_relation(&store),
        vec![vec![
            json!("docs_emb"),
            json!("vector"),
            json!("table_column"),
            json!("docs"),
            json!("emb"),
            json!("active"),
            json!(2),
            json!(50),
            Value::Null,
        ]]
    );

    assert_eq!(store.drop_ann_index("docs_emb").unwrap(), 1);
    let key = TableStore::ann_index_key(&index);
    assert!(store.list_ann_indexes().unwrap().is_empty());
    assert!(store.live_ann_generation(&key).unwrap().is_none());
    assert!(store.ann_authority().slot_keys().is_empty());
    assert!(status_relation(&store).is_empty());
    assert_eq!(store.drop_ann_index("docs_emb").unwrap(), 0);
}

#[test]
fn a_blocked_index_carries_a_typed_bounded_diagnostic() {
    let store = docs_without_index(30);
    let not_a_vector = AnnIndexPlan {
        column: "owner".to_string(),
        name: Some("owner_ann".to_string()),
        ..hnsw_l2()
    };
    store.create_ann_index(&not_a_vector).unwrap();
    store.create_ann_index(&hnsw_l2()).unwrap();
    store.ann_authority().set_limits(AnnLimits {
        build_rows: 5,
        ..AnnLimits::default()
    });

    refresh(&store);

    let reasons: Vec<(String, ManagedIndexState, Option<IndexBlockReason>)> = store
        .managed_index_status()
        .unwrap()
        .into_iter()
        .map(|status| {
            let reason = status.block.as_ref().map(|block| block.reason);
            (status.name, status.state, reason)
        })
        .collect();
    assert_eq!(
        reasons,
        vec![
            (
                "docs_emb".to_string(),
                ManagedIndexState::Blocked,
                Some(IndexBlockReason::BuildBound)
            ),
            (
                "owner_ann".to_string(),
                ManagedIndexState::Blocked,
                Some(IndexBlockReason::NotIndexable)
            ),
        ]
    );
    assert!(store
        .managed_index_status()
        .unwrap()
        .iter()
        .all(|status| status.block.as_ref().unwrap().detail.len()
            <= eg_core::index::MAX_BLOCK_DETAIL_BYTES));
    assert_eq!(status_relation(&store)[0][8], json!("build_bound"));
}

#[test]
fn a_build_finishing_after_its_drop_never_persists_a_generation() {
    let store = docs_without_index(20);
    let index = hnsw_l2();
    store.create_ann_index(&index).unwrap();
    refresh(&store);
    let key = TableStore::ann_index_key(&index);
    let late = live_generation(&store, &index);
    store.drop_ann_index("docs_emb").unwrap();

    let stored = encode(&late, Encoding::Full).unwrap();
    let error = store
        .persist_ann_generation(&GenerationWrite {
            index: &key,
            table: "docs",
            stored: &stored,
            keeps: None,
            prune_through: None,
        })
        .unwrap_err();

    assert!(error.contains("was dropped"), "{error}");
    assert!(store.live_ann_generation(&key).unwrap().is_none());
}

fn live_generation(store: &TableStore, index: &AnnIndexPlan) -> AnnGeneration {
    let source = store
        .ann_source_rows(&index.table, &index.column, 1_000)
        .unwrap();
    AnnGeneration::build(9, index.method, index.metric, source)
}

// spec: EG-FEDERATED-QUERY-R009
#[test]
fn drop_index_is_a_typed_sql_statement_over_the_same_fenced_drop() {
    let store = docs_without_index(10);
    let index = hnsw_l2();
    store.create_ann_index(&index).unwrap();
    refresh(&store);
    let classified = crate::classify("DROP INDEX IF EXISTS docs_emb;").unwrap();
    assert!(matches!(
        classified,
        crate::StatementKind::DropAnnIndex { ref name, if_exists: true } if name == "docs_emb"
    ));
    assert_eq!(
        store.ann_index_table("docs_emb").unwrap().as_deref(),
        Some("docs")
    );
    let mut txn = crate::TableTxn::new();
    txn.push(crate::TxnOp::IndexCatalog(
        crate::IndexCatalogTxnOp::DropAnnIndex {
            table: "docs".to_string(),
            name: "docs_emb".to_string(),
        },
    ));

    store.commit_txn(&txn).unwrap();

    assert!(store.list_ann_indexes().unwrap().is_empty());
    let key = TableStore::ann_index_key(&index);
    assert!(store.live_ann_generation(&key).unwrap().is_none());
    let again = store.commit_txn(&txn).unwrap_err();
    assert!(again.contains("does not exist"), "{again}");
}
