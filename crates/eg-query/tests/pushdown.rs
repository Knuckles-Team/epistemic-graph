//! W3-D (CONCEPT:EG-KG.query.concept-12): secondary/property-index predicate pushdown on the
//! `nodes` table. A `WHERE indexed_prop = 'x'` resolves through the eg-core/provider
//! index instead of scanning + decoding every node, and MUST return the SAME rows as
//! the full-scan path. A WHERE on a non-indexed predicate still works (fallback).
#![cfg(feature = "sql")]

use eg_core::graph::GraphCore;
use eg_query::{exec_sql, QueryResult};
use serde_json::json;

/// A graph large enough that the pushdown path is meaningfully exercised: 500 nodes
/// across two teams and three types, each with a unique `name` and an integer `rank`.
fn graph(n: usize) -> GraphCore {
    let core = GraphCore::new();
    for i in 0..n {
        let team = if i % 2 == 0 { "blue" } else { "red" };
        let kind = ["Agent", "Tool", "Service"][i % 3];
        let val = json!({
            "type": kind,
            "team": team,
            "name": format!("node-{i}"),
            "rank": (i % 7) as i64,
        });
        core.add_node(format!("n{i}"), rmp_serde::to_vec_named(&val).unwrap());
    }
    core
}

fn rows(r: &QueryResult) -> Vec<Vec<serde_json::Value>> {
    r.rows
        .iter()
        .map(|b| rmp_serde::from_slice::<Vec<serde_json::Value>>(b).unwrap())
        .collect()
}

/// Helper: run a query, return sorted id list.
fn ids(core: &GraphCore, sql: &str) -> Vec<String> {
    let snap = core.analysis_snapshot();
    let r = exec_sql(&snap, sql, &eg_query::CancellationToken::new()).unwrap();
    let mut v: Vec<String> = rows(&r)
        .into_iter()
        .map(|row| row[0].as_str().unwrap().to_string())
        .collect();
    v.sort();
    v
}

/// The pushdown result on an indexed property column must equal the unfiltered scan
/// filtered in the test harness — i.e. the index narrowing changes performance, not
/// the answer.
#[test]
fn pushdown_equality_matches_full_scan() {
    let core = graph(500);

    // Through the provider's pushdown path: WHERE team = 'blue'.
    let pushed = ids(
        &core,
        "SELECT id FROM nodes WHERE team = 'blue' ORDER BY id",
    );

    // Ground truth: the SAME predicate but computed without relying on a single
    // indexed column — pull ALL ids + team, filter in the test. (The provider serves
    // the full batch when there is no equality predicate, so this is the full-scan
    // path.)
    let snap = core.analysis_snapshot();
    let all = exec_sql(
        &snap,
        "SELECT id, team FROM nodes",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let mut truth: Vec<String> = rows(&all)
        .into_iter()
        .filter(|row| row[1].as_str() == Some("blue"))
        .map(|row| row[0].as_str().unwrap().to_string())
        .collect();
    truth.sort();

    assert_eq!(pushed, truth);
    assert_eq!(pushed.len(), 250, "half the nodes are blue");
}

/// Equality on the `id` column (the always-present node-id column) is pushable too.
#[test]
fn pushdown_on_id_column() {
    let core = graph(100);
    let got = ids(&core, "SELECT id FROM nodes WHERE id = 'n42'");
    assert_eq!(got, vec!["n42".to_string()]);
}

/// A numeric indexed column pushes down correctly.
#[test]
fn pushdown_integer_equality() {
    let core = graph(70);
    let pushed = ids(&core, "SELECT id FROM nodes WHERE rank = 3 ORDER BY id");
    let snap = core.analysis_snapshot();
    let all = exec_sql(
        &snap,
        "SELECT id, rank FROM nodes",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let mut truth: Vec<String> = rows(&all)
        .into_iter()
        .filter(|row| row[1].as_i64() == Some(3))
        .map(|row| row[0].as_str().unwrap().to_string())
        .collect();
    truth.sort();
    assert_eq!(pushed, truth);
    assert!(!pushed.is_empty());
}

/// Two ANDed equality predicates (composite) — both pushed, intersected by the index.
#[test]
fn pushdown_composite_and() {
    let core = graph(300);
    let pushed = ids(
        &core,
        "SELECT id FROM nodes WHERE team = 'blue' AND type = 'Tool' ORDER BY id",
    );
    let snap = core.analysis_snapshot();
    let all = exec_sql(
        &snap,
        "SELECT id, team, type FROM nodes",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let mut truth: Vec<String> = rows(&all)
        .into_iter()
        .filter(|row| row[1].as_str() == Some("blue") && row[2].as_str() == Some("Tool"))
        .map(|row| row[0].as_str().unwrap().to_string())
        .collect();
    truth.sort();
    assert_eq!(pushed, truth);
}

/// A WHERE on a NON-indexable predicate (a `LIKE`, and an inequality) still works via
/// the full-scan fallback — DataFusion keeps it as a Filter above the scan.
#[test]
fn non_indexed_predicate_full_scan_fallback() {
    let core = graph(60);

    // LIKE is not an equality predicate → Unsupported → DataFusion filters above.
    let like = ids(
        &core,
        "SELECT id FROM nodes WHERE name LIKE 'node-1%' ORDER BY id",
    );
    let snap = core.analysis_snapshot();
    let all = exec_sql(
        &snap,
        "SELECT id, name FROM nodes",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let mut truth: Vec<String> = rows(&all)
        .into_iter()
        .filter(|row| {
            row[1]
                .as_str()
                .map(|s| s.starts_with("node-1"))
                .unwrap_or(false)
        })
        .map(|row| row[0].as_str().unwrap().to_string())
        .collect();
    truth.sort();
    assert_eq!(like, truth, "LIKE must full-scan correctly");

    // An inequality (>) is also not equality-pushable.
    let gt = ids(&core, "SELECT id FROM nodes WHERE rank > 4 ORDER BY id");
    let all2 = exec_sql(
        &snap,
        "SELECT id, rank FROM nodes",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let mut truth2: Vec<String> = rows(&all2)
        .into_iter()
        .filter(|row| row[1].as_i64().map(|r| r > 4).unwrap_or(false))
        .map(|row| row[0].as_str().unwrap().to_string())
        .collect();
    truth2.sort();
    assert_eq!(gt, truth2, "inequality must full-scan correctly");
}

/// Mixed: one equality (pushed, indexed) AND one non-equality (kept as a Filter) —
/// the index narrows, DataFusion re-applies the rest. Result equals the full scan.
#[test]
fn pushdown_mixed_equality_and_inequality() {
    let core = graph(140);
    let got = ids(
        &core,
        "SELECT id FROM nodes WHERE team = 'red' AND rank > 3 ORDER BY id",
    );
    let snap = core.analysis_snapshot();
    let all = exec_sql(
        &snap,
        "SELECT id, team, rank FROM nodes",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let mut truth: Vec<String> = rows(&all)
        .into_iter()
        .filter(|row| {
            row[1].as_str() == Some("red") && row[2].as_i64().map(|r| r > 3).unwrap_or(false)
        })
        .map(|row| row[0].as_str().unwrap().to_string())
        .collect();
    truth.sort();
    assert_eq!(got, truth);
}

/// Ordinary user-table indexes are explicit catalog objects, not an implicit
/// scan hint. This fixture covers deterministic CREATE/DROP, schema binding,
/// equality/range narrowing, and the owner scope that is part of the physical
/// key. Root should run it alongside the provider pushdown suite after wiring
/// SQL DDL classification to the public TableStore methods.
#[test]
fn secondary_index_catalog_and_scalar_lookup_contract() {
    use eg_query::tables::{
        Cell, Column, ColumnType, SecondaryIndexColumn, SecondaryIndexLookup, TableSchema,
        TableStore,
    };

    let path = std::env::temp_dir().join(format!(
        "eg_secondary_contract_{}_{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let store = TableStore::open_scoped(&path, "tenant-a").unwrap();
    let schema = TableSchema::new(
        "events",
        vec![
            Column::new("id", ColumnType::BigInt, false, false),
            Column::new("team", ColumnType::Text, false, false),
            Column::new("score", ColumnType::BigInt, false, false),
        ],
    );
    store.create_table(&schema, false).unwrap();
    let team = store
        .secondary_index_spec(
            "events",
            "events_team_idx",
            vec![SecondaryIndexColumn::ascending("team")],
            &schema,
        )
        .unwrap();
    let score = store
        .secondary_index_spec(
            "events",
            "events_score_idx",
            vec![SecondaryIndexColumn::ascending("score")],
            &schema,
        )
        .unwrap();
    let mut stale = team.clone();
    stale.schema_digest = "stale".into();
    assert!(store.create_secondary_index(&stale, false).is_err());
    let mut wrong_scope = team.clone();
    wrong_scope.tenant_scope = "tenant-b".into();
    assert!(store.create_secondary_index(&wrong_scope, false).is_err());
    assert!(store.create_secondary_index(&team, false).unwrap());
    assert!(store.create_secondary_index(&score, false).unwrap());
    store
        .insert_rows(
            "events",
            &["id".into(), "team".into(), "score".into()],
            &[
                vec![1i64.into(), "blue".into(), 10i64.into()],
                vec![2i64.into(), "red".into(), 20i64.into()],
                vec![3i64.into(), "blue".into(), 30i64.into()],
            ],
        )
        .unwrap();
    let blue = store
        .secondary_index_rows(
            "events",
            &SecondaryIndexLookup::Eq {
                column: "team".into(),
                value: Cell::Text("blue".into()),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(blue.len(), 2);
    assert!(blue.iter().all(|row| row[1] == Cell::Text("blue".into())));

    let high_scores = store
        .secondary_index_rows(
            "events",
            &SecondaryIndexLookup::Ge {
                column: "score".into(),
                value: Cell::Int(20),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(high_scores.len(), 2);
    let page = store
        .secondary_index_ordered_rows(
            "events",
            "events_score_idx",
            eg_query::tables::SecondaryIndexOrder::Desc,
            1,
            Some(1),
        )
        .unwrap()
        .unwrap();
    assert_eq!(page[0][2], Cell::Int(20));

    let mut set = serde_json::Map::new();
    set.insert("team".into(), "green".into());
    store
        .update_where(
            "events",
            &set,
            &eg_types::RowPredicate::Cmp {
                col: "id".into(),
                op: eg_types::CmpOp::Eq,
                value: 1i64.into(),
            },
        )
        .unwrap();
    let blue_after_update = store
        .secondary_index_rows(
            "events",
            &SecondaryIndexLookup::Eq {
                column: "team".into(),
                value: Cell::Text("blue".into()),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(blue_after_update.len(), 1);

    store
        .delete_where(
            "events",
            &eg_types::RowPredicate::Cmp {
                col: "id".into(),
                op: eg_types::CmpOp::Eq,
                value: 3i64.into(),
            },
        )
        .unwrap();
    let blue_after_delete = store
        .secondary_index_rows(
            "events",
            &SecondaryIndexLookup::Eq {
                column: "team".into(),
                value: Cell::Text("blue".into()),
            },
        )
        .unwrap()
        .unwrap();
    assert!(blue_after_delete.is_empty());

    assert!(store
        .drop_secondary_index("events", "events_team_idx", false)
        .unwrap());
    assert!(store
        .secondary_index_rows(
            "events",
            &SecondaryIndexLookup::Eq {
                column: "team".into(),
                value: Cell::Text("green".into()),
            },
        )
        .unwrap()
        .is_none());

    drop(store);
    let reopened = TableStore::open_scoped(&path, "tenant-a").unwrap();
    assert_eq!(
        reopened
            .list_secondary_indexes(Some("events"))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(reopened.scan("events").unwrap().len(), 2);
}

/// Vector columns remain ANN-only; a generic scalar index request is rejected
/// before any catalog row is written.
#[test]
fn secondary_index_rejects_vector_columns_without_touching_ann_catalog() {
    use eg_query::tables::{Column, ColumnType, SecondaryIndexColumn, TableSchema, TableStore};
    let (store, _path) = TableStore::open_temp().unwrap();
    let schema = TableSchema::new(
        "embeddings",
        vec![Column::new(
            "embedding",
            ColumnType::Vector(Some(3)),
            false,
            false,
        )],
    );
    store.create_table(&schema, false).unwrap();
    let spec = store.secondary_index_spec(
        "embeddings",
        "embedding_btree",
        vec![SecondaryIndexColumn::ascending("embedding")],
        &schema,
    );
    assert!(spec.is_err());
    assert!(store.list_ann_indexes().unwrap().is_empty());
}
