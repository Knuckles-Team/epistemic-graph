//! RF-019 proofs for the maintained user-table ANN authority: the query path never
//! builds; the worker builds and activates atomically and monotonically; the
//! maintained probe is exact over what it returns, within recall of the exact
//! top-k, filters inside the probe (CX-022: an unresolved visibility identity is
//! denied), honours tombstones and post-build inserts, stays inside its resource
//! bounds, reports its lag, and serves its persisted generation after a restart.
//! The durability, incremental-refresh and per-table staleness proofs are in
//! `durability`.

use std::path::PathBuf;
use std::time::Duration;

use eg_core::graph::GraphView;
use eg_types::{CmpOp, RowPredicate};
use serde_json::{json, Value};

use super::generation::AnnGeneration;
use super::*;
use crate::sql::{metric_to_ann, AnnIndexPlan, AnnMethod, VectorMetric};
use crate::tables::schema::Cell;
use crate::tables::store::dev_scope_grant::{dev_verifier, DEV_PRINCIPAL, DEV_PROOF};
use crate::tables::store::AnnSourceRows;
use crate::tables::{Column, ColumnType, TableSchema, TableStore};

const DIM: usize = 8;

fn plan(method: AnnMethod, metric: VectorMetric) -> AnnIndexPlan {
    AnnIndexPlan {
        name: Some("docs_emb".to_string()),
        table: "docs".to_string(),
        column: "emb".to_string(),
        method,
        metric,
        if_not_exists: false,
    }
}

fn hnsw_l2() -> AnnIndexPlan {
    plan(AnnMethod::Hnsw, VectorMetric::L2)
}

/// SplitMix64: a deterministic, dependency-free stream of `f32`s in `[-1, 1)`.
fn stream(seed: u64) -> impl FnMut() -> f32 {
    let mut state = seed;
    move || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        ((z >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    }
}

/// `n` vectors around eight cluster centres drawn from `[-spread, spread)`, each
/// perturbed by up to `noise` per coordinate.
fn clustered(n: usize, seed: u64, spread: f32, noise: f32) -> Vec<Vec<f32>> {
    let mut next = stream(seed);
    let centres: Vec<Vec<f32>> = (0..8)
        .map(|_| (0..DIM).map(|_| next() * spread).collect())
        .collect();
    (0..n)
        .map(|i| {
            centres[i % centres.len()]
                .iter()
                .map(|c| c + next() * noise)
                .collect()
        })
        .collect()
}

/// Overlapping clusters: the everyday fixture.
fn vectors(n: usize, seed: u64) -> Vec<Vec<f32>> {
    clustered(n, seed, 2.0, 1.0)
}

/// Tight, well-separated clusters.
fn separated(n: usize, seed: u64) -> Vec<Vec<f32>> {
    clustered(n, seed, 4.0, 0.5)
}

/// `count` queries near the data: stored rows perturbed by small noise, the
/// shape a real nearest-neighbour workload has.
fn queries_near(rows: &[Vec<f32>], count: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut next = stream(seed);
    (0..count)
        .map(|i| {
            rows[(i * 29) % rows.len()]
                .iter()
                .map(|x| x + next() * 0.3)
                .collect()
        })
        .collect()
}

/// Who may see row `id`: alice, bob, or nobody (a NULL owner).
fn owner_of(id: i64) -> Value {
    match id % 3 {
        0 => json!("alice"),
        1 => json!("bob"),
        _ => Value::Null,
    }
}

fn open_docs(rows: &[Vec<f32>], index: &AnnIndexPlan) -> (TableStore, PathBuf) {
    let (store, path) = TableStore::open_temp().unwrap();
    let schema = TableSchema::new(
        "docs",
        vec![
            Column::new("id", ColumnType::BigInt, false, true),
            Column::new("owner", ColumnType::Text, true, false),
            Column::new("emb", ColumnType::Vector(Some(DIM)), true, false),
        ],
    );
    store.create_table(&schema, false).unwrap();
    insert(&store, 0, rows);
    store.put_ann_index(index).unwrap();
    (store, path)
}

fn insert(store: &TableStore, first_id: i64, rows: &[Vec<f32>]) {
    let columns = vec!["id".to_string(), "owner".to_string(), "emb".to_string()];
    let values: Vec<Vec<Value>> = rows
        .iter()
        .zip(first_id..)
        .map(|(vector, id)| vec![json!(id), owner_of(id), json!(vector)])
        .collect();
    store.insert_rows("docs", &columns, &values).unwrap();
}

fn owned_by(owner: &str) -> RowPredicate {
    RowPredicate::Cmp {
        col: "owner".to_string(),
        op: CmpOp::Eq,
        value: json!(owner),
    }
}

fn id_of(row: &[Cell]) -> i64 {
    match row[0] {
        Cell::Int(id) => id,
        ref other => panic!("id cell is not an integer: {other:?}"),
    }
}

fn ids(rows: &[Vec<Cell>]) -> Vec<i64> {
    rows.iter().map(|row| id_of(row)).collect()
}

fn row_map(row: &[Cell]) -> serde_json::Map<String, Value> {
    ["id", "owner", "emb"]
        .iter()
        .zip(row)
        .map(|(name, cell)| (name.to_string(), cell.to_json()))
        .collect()
}

/// The brute-force reference: every admitted row, exactly ranked.
fn exact_ids(
    store: &TableStore,
    index: &AnnIndexPlan,
    query: &[f32],
    k: usize,
    filter: Option<&RowPredicate>,
) -> Vec<i64> {
    let metric = metric_to_ann(index.metric);
    let mut scored: Vec<(f32, i64)> = store
        .scan("docs")
        .unwrap()
        .iter()
        .filter(|row| filter.is_none_or(|predicate| predicate.eval(&row_map(row))))
        .filter_map(|row| match &row[2] {
            Cell::Vector(vector) if vector.len() == query.len() => {
                Some((metric.distance(query, vector), id_of(row)))
            }
            _ => None,
        })
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

fn top(
    store: &TableStore,
    index: &AnnIndexPlan,
    query: &[f32],
    k: usize,
    prefilter: Option<&RowPredicate>,
) -> AnnTopK {
    store
        .ann_top_k(&AnnTopKRequest {
            index,
            query,
            k,
            prefilter,
        })
        .unwrap()
}

fn refresh(store: &TableStore) -> Vec<AnnRefreshOutcome> {
    store
        .refresh_ann_generations(AnnRefreshPolicy::Immediate)
        .unwrap()
}

fn maintained(generation: u64) -> AnnServingPath {
    AnnServingPath::MaintainedIndex { generation }
}

fn exact(reason: AnnFallbackReason) -> AnnServingPath {
    AnnServingPath::BoundedExact { reason }
}

fn recall(got: &[i64], want: &[i64]) -> f64 {
    let hits = got.iter().filter(|id| want.contains(id)).count();
    hits as f64 / want.len().max(1) as f64
}

#[test]
fn a_query_never_builds_a_generation() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(120, 1), &index);
    let query = vectors(1, 99).remove(0);

    let answer = top(&store, &index, &query, 5, None);

    assert_eq!(
        answer.receipt.path,
        exact(AnnFallbackReason::GenerationBuilding)
    );
    assert_eq!(
        ids(&answer.rows),
        exact_ids(&store, &index, &query, 5, None)
    );
    let status = store.ann_index_status().unwrap();
    assert_eq!(status[0].state, ManagedIndexState::Requested);
    assert_eq!(status[0].generation, None, "the query path built nothing");
}

#[test]
fn the_worker_activates_and_the_probe_serves_the_maintained_generation() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(120, 2), &index);

    let outcomes = refresh(&store);

    assert!(matches!(
        outcomes.as_slice(),
        [AnnRefreshOutcome::Activated {
            generation: 1,
            rows: 120,
            ..
        }]
    ));
    let query = vectors(1, 77).remove(0);
    let answer = top(&store, &index, &query, 5, None);
    assert_eq!(answer.receipt.path, maintained(1));
    assert_eq!(
        ids(&answer.rows),
        exact_ids(&store, &index, &query, 5, None)
    );
    let status = &store.ann_index_status().unwrap()[0];
    assert_eq!(
        (status.state, status.stale),
        (ManagedIndexState::Active, false)
    );
    assert_eq!((status.generation, status.lag_epochs), (Some(1), 0));
}

/// Every registration method/metric over `rows`: the maintained probe's rows are
/// exactly ranked, and mean recall@10 against the exact top-10 is at least 0.9.
fn assert_recall(rows: &[Vec<f32>], queries: &[Vec<f32>]) {
    for index in [
        plan(AnnMethod::Hnsw, VectorMetric::L2),
        plan(AnnMethod::Hnsw, VectorMetric::Cosine),
        plan(AnnMethod::IvfFlat, VectorMetric::L2),
        plan(AnnMethod::IvfFlat, VectorMetric::Cosine),
        plan(AnnMethod::IvfFlat, VectorMetric::InnerProduct),
    ] {
        let (store, _path) = open_docs(rows, &index);
        refresh(&store);
        let metric = metric_to_ann(index.metric);
        let mut total = 0.0;
        for query in queries {
            let answer = top(&store, &index, query, 10, None);
            assert_eq!(answer.receipt.path, maintained(1), "{index:?}");
            let distances: Vec<f32> = answer
                .rows
                .iter()
                .map(|row| match &row[2] {
                    Cell::Vector(vector) => metric.distance(query, vector),
                    other => panic!("no vector: {other:?}"),
                })
                .collect();
            assert!(
                distances.windows(2).all(|pair| pair[0] <= pair[1]),
                "{index:?}: returned rows must be exactly ranked"
            );
            total += recall(
                &ids(&answer.rows),
                &exact_ids(&store, &index, query, 10, None),
            );
        }
        let mean = total / queries.len() as f64;
        assert!(mean >= 0.9, "{index:?}: mean recall@10 {mean} < 0.9");
    }
}

#[test]
fn maintained_results_are_exactly_ranked_and_within_recall_of_the_exact_top_k() {
    let rows = vectors(600, 3);
    assert_recall(&rows, &queries_near(&rows, 20, 1_000));
}

/// The distribution that exposed a nearest-only HNSW neighbour selection: eight
/// well-separated tight clusters, queried from points outside all of them.
#[test]
fn recall_holds_on_separated_clusters_with_outlying_queries() {
    assert_recall(&separated(600, 3), &separated(20, 1_000));
}

#[test]
fn a_replayed_refresh_is_idempotent_and_a_write_makes_the_index_stale() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(60, 4), &index);
    refresh(&store);

    assert!(matches!(
        refresh(&store).as_slice(),
        [AnnRefreshOutcome::Current { generation: 1, .. }]
    ));
    insert(&store, 60, &vectors(1, 5));
    let stale = &store.ann_index_status().unwrap()[0];
    assert_eq!(
        (stale.state, stale.stale),
        (ManagedIndexState::Active, true)
    );
    assert!(stale.lag_epochs >= 1, "a write is visible as lag");

    assert!(matches!(
        refresh(&store).as_slice(),
        [AnnRefreshOutcome::Activated {
            generation: 2,
            rows: 61,
            ..
        }]
    ));
    let live = &store.ann_index_status().unwrap()[0];
    assert_eq!(
        (live.state, live.stale, live.lag_epochs),
        (ManagedIndexState::Active, false, 0)
    );
}

#[test]
fn a_throttled_worker_defers_a_recent_rebuild() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(40, 6), &index);
    store.ann_authority().set_limits(AnnLimits {
        rebuild_interval: Duration::from_secs(3_600),
        ..AnnLimits::default()
    });
    refresh(&store);
    insert(&store, 40, &vectors(1, 7));

    let outcomes = store
        .refresh_ann_generations(AnnRefreshPolicy::Throttled)
        .unwrap();

    assert!(matches!(
        outcomes.as_slice(),
        [AnnRefreshOutcome::Deferred { .. }]
    ));
}

#[test]
fn activation_never_moves_an_index_backwards_in_source_time() {
    let source = |epoch: u64| AnnSourceRows {
        epoch,
        dim: Some(DIM),
        max_rowid: Some(9),
        rows: vectors(10, epoch)
            .into_iter()
            .enumerate()
            .map(|(rowid, vector)| (rowid as u64, vector))
            .collect(),
    };
    let authority = UserAnnAuthority::default();
    let slot = authority.slot("docs.emb.L2");
    let newer = AnnGeneration::build(2, AnnMethod::Hnsw, VectorMetric::L2, source(9));
    let older = AnnGeneration::build(1, AnnMethod::Hnsw, VectorMetric::L2, source(4));

    assert!(matches!(
        slot.activate(newer, "docs.emb.L2".to_string()),
        AnnRefreshOutcome::Activated { built_epoch: 9, .. }
    ));
    assert_eq!(
        slot.activate(older, "docs.emb.L2".to_string()),
        AnnRefreshOutcome::Superseded {
            index: "docs.emb.L2".to_string(),
            built_epoch: 4
        }
    );
    let live = slot.live_for(AnnMethod::Hnsw).unwrap();
    assert_eq!((live.generation, live.built_epoch), (2, 9));
}

#[test]
fn rows_inserted_after_the_build_are_served_before_any_rebuild() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(80, 8), &index);
    refresh(&store);
    let query = vectors(1, 55).remove(0);
    insert(&store, 80, std::slice::from_ref(&query));

    let answer = top(&store, &index, &query, 3, None);

    assert_eq!(answer.receipt.path, maintained(1));
    assert_eq!(
        ids(&answer.rows)[0],
        80,
        "the exact match inserted after the build"
    );
    assert_eq!(
        ids(&answer.rows),
        exact_ids(&store, &index, &query, 3, None)
    );
}

#[test]
fn a_deleted_row_is_never_served_from_a_generation_that_indexed_it() {
    let index = hnsw_l2();
    let rows = vectors(80, 9);
    let (store, _path) = open_docs(&rows, &index);
    refresh(&store);
    let nearest = ids(&top(&store, &index, &rows[17], 1, None).rows)[0];
    assert_eq!(nearest, 17);

    store
        .delete_where(
            "docs",
            &RowPredicate::Cmp {
                col: "id".to_string(),
                op: CmpOp::Eq,
                value: json!(17),
            },
        )
        .unwrap();
    let answer = top(&store, &index, &rows[17], 5, None);

    assert_eq!(
        answer.receipt.path,
        maintained(1),
        "served without a rebuild"
    );
    assert!(
        !ids(&answer.rows).contains(&17),
        "a tombstone is never served"
    );
    assert_eq!(
        ids(&answer.rows),
        exact_ids(&store, &index, &rows[17], 5, None)
    );
}

#[test]
fn visibility_is_applied_inside_the_probe_and_an_unresolved_identity_is_denied() {
    let index = hnsw_l2();
    let rows = vectors(150, 10);
    let (store, _path) = open_docs(&rows, &index);
    let alice = owned_by("alice");
    // Row 2 has a NULL owner (CX-022's shape: no identity to match).
    let query = rows[2].clone();
    for phase in ["building", "maintained"] {
        let answer = top(&store, &index, &query, 6, Some(&alice));
        let got = ids(&answer.rows);
        assert_eq!(
            got.len(),
            6,
            "{phase}: k visible rows, not k minus the hidden ones"
        );
        assert!(
            got.iter().all(|id| id % 3 == 0),
            "{phase}: only alice's rows: {got:?}"
        );
        assert!(
            !got.contains(&2),
            "{phase}: a NULL-owner row is never visible"
        );
        assert_eq!(
            got,
            exact_ids(&store, &index, &query, 6, Some(&alice)),
            "{phase}"
        );
        refresh(&store);
    }
    let unresolvable = RowPredicate::Cmp {
        col: "tenant".to_string(),
        op: CmpOp::Eq,
        value: json!("t1"),
    };
    let denied = top(&store, &index, &query, 6, Some(&unresolvable));
    assert!(
        denied.rows.is_empty(),
        "an unresolved identity admits nothing"
    );
}

#[test]
fn the_bounded_exact_fallback_refuses_past_its_bound() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(50, 11), &index);
    store.ann_authority().set_limits(AnnLimits {
        exact_rows: 10,
        ..AnnLimits::default()
    });

    let error = store
        .ann_top_k(&AnnTopKRequest {
            index: &index,
            query: &vectors(1, 12)[0],
            k: 3,
            prefilter: None,
        })
        .unwrap_err();

    assert!(
        error.contains("bounded exact fallback of 10 rows"),
        "{error}"
    );
    assert!(error.contains("GenerationBuilding"), "{error}");
}

#[test]
fn too_many_rows_since_the_build_take_the_bounded_exact_path() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(60, 13), &index);
    refresh(&store);
    store.ann_authority().set_limits(AnnLimits {
        delta_rows: 2,
        ..AnnLimits::default()
    });
    insert(&store, 60, &vectors(5, 14));
    let query = vectors(1, 15).remove(0);

    let answer = top(&store, &index, &query, 4, None);

    assert_eq!(answer.receipt.path, exact(AnnFallbackReason::DeltaOverflow));
    assert_eq!(
        ids(&answer.rows),
        exact_ids(&store, &index, &query, 4, None)
    );
}

#[test]
fn an_exhausted_probe_budget_takes_the_bounded_exact_path() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(120, 16), &index);
    refresh(&store);
    store.ann_authority().set_limits(AnnLimits {
        probe_rows: 2,
        ..AnnLimits::default()
    });
    let query = vectors(1, 17).remove(0);
    let alice = owned_by("alice");

    let answer = top(&store, &index, &query, 4, Some(&alice));

    assert_eq!(answer.receipt.path, exact(AnnFallbackReason::ProbeBudget));
    assert_eq!(
        ids(&answer.rows),
        exact_ids(&store, &index, &query, 4, Some(&alice))
    );
}

#[test]
fn a_query_of_another_width_never_probes_the_generation() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(40, 18), &index);
    refresh(&store);

    let answer = top(&store, &index, &[0.5; 4], 3, None);

    assert_eq!(
        answer.receipt.path,
        exact(AnnFallbackReason::DimensionMismatch)
    );
    assert!(answer.rows.is_empty(), "no stored vector has width 4");
}

#[test]
fn a_failed_build_is_visible_and_a_dropped_registration_is_forgotten() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(30, 19), &index);
    let broken = AnnIndexPlan {
        column: "owner".to_string(),
        ..plan(AnnMethod::Hnsw, VectorMetric::Cosine)
    };
    store.put_ann_index(&broken).unwrap();

    let outcomes = refresh(&store);

    assert!(outcomes
        .iter()
        .any(|outcome| matches!(outcome, AnnRefreshOutcome::Failed { reason, .. } if reason.contains("not a vector column"))));
    let failed = store
        .ann_index_status()
        .unwrap()
        .into_iter()
        .find(|status| status.column == "owner")
        .unwrap();
    assert_eq!(failed.state, ManagedIndexState::Blocked);
    assert_eq!(
        failed.block.map(|block| block.reason),
        Some(eg_core::index::IndexBlockReason::NotIndexable),
        "a typed diagnostic"
    );

    store.drop_ann_indexes_for_column("docs", "emb").unwrap();
    store.drop_ann_indexes_for_column("docs", "owner").unwrap();
    assert!(refresh(&store).is_empty());
    assert!(store.ann_authority().slot_keys().is_empty());
}

#[test]
fn a_restart_serves_the_persisted_generation_without_a_rebuild() {
    let index = hnsw_l2();
    let rows = vectors(90, 20);
    let (store, path) = open_docs(&rows, &index);
    refresh(&store);
    let query = vectors(1, 21).remove(0);
    let before = ids(&top(&store, &index, &query, 5, None).rows);
    drop(store);

    let reopened = TableStore::open(&path, dev_verifier(), DEV_PRINCIPAL, DEV_PROOF).unwrap();
    let status = &reopened.ann_index_status().unwrap()[0];
    assert_eq!(
        (status.state, status.generation),
        (ManagedIndexState::Active, Some(1))
    );
    let after = top(&reopened, &index, &query, 5, None);
    assert_eq!(after.receipt.path, maintained(1), "no exact window");
    assert_eq!(ids(&after.rows), before);
    assert!(matches!(
        refresh(&reopened).as_slice(),
        [AnnRefreshOutcome::Current { generation: 1, .. }]
    ));
}

fn literal(vector: &[f32]) -> String {
    let parts: Vec<String> = vector.iter().map(f32::to_string).collect();
    format!("'[{}]'", parts.join(","))
}

fn sql_ids(store: &TableStore, sql: &str) -> Vec<i64> {
    crate::sql::exec_sql_typed_with_tables(&GraphView::default(), store, sql)
        .unwrap()
        .rows
        .iter()
        .map(|row| row[0].as_i64().unwrap())
        .collect()
}

#[test]
fn sql_with_where_and_offset_is_served_by_the_maintained_index() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(150, 22), &index);
    refresh(&store);
    let query = vectors(1, 23).remove(0);
    let sql = format!(
        "SELECT id FROM docs WHERE owner = 'alice' ORDER BY emb <-> {} LIMIT 4 OFFSET 2",
        literal(&query)
    );

    let got = sql_ids(&store, &sql);

    let want = exact_ids(&store, &index, &query, 6, Some(&owned_by("alice")));
    assert_eq!(got, want[2..].to_vec());
    let receipt = store.ann_authority().recent_receipts().pop().unwrap();
    assert_eq!(
        receipt.path,
        maintained(1),
        "the WHERE was pushed into the probe"
    );
    assert_eq!(receipt.returned_rows, 6, "LIMIT + OFFSET rows narrowed");
}

#[test]
fn sql_the_probe_cannot_filter_exactly_keeps_the_full_scan() {
    let index = hnsw_l2();
    let (store, _path) = open_docs(&vectors(60, 24), &index);
    refresh(&store);
    let query = vectors(1, 25).remove(0);
    let sql = format!(
        "SELECT id FROM docs WHERE NOT owner = 'bob' ORDER BY emb <-> {} LIMIT 3",
        literal(&query)
    );

    let got = sql_ids(&store, &sql);

    // SQL's `NOT owner = 'bob'` is unknown — excluded — on a NULL owner, so only
    // alice's rows (id % 3 == 0) qualify.
    let want: Vec<i64> = exact_ids(&store, &index, &query, 60, None)
        .into_iter()
        .filter(|id| id % 3 == 0)
        .take(3)
        .collect();
    assert_eq!(got, want, "SQL's NOT excludes the NULL-owner rows too");
    assert!(
        store.ann_authority().recent_receipts().is_empty(),
        "a declined statement never reaches the authority"
    );
}

mod durability;
mod managed;
