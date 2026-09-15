//! NE-033/NE-049 acceptance tests for the lake as-of read seam.
//!
//! These are real integration tests over the public `LakeManager` API.  They
//! prove that a caller can read the current and historical projections, that
//! the valid empty-history boundary remains available, and that nonexistent or
//! out-of-range LSNs are denied.  The same API applies `LakeVisibility` before
//! validating an LSN, so a cross-owner request is indistinguishable from a
//! missing table even when it supplies an invalid LSN.
//!
//! The authenticated Iceberg REST `LoadTable` route now accepts the explicit
//! `?as_of=<LSN>` extension and calls this same scoped API; focused handler tests
//! in `src/server/lake/rest.rs` prove that served mapping.  This integration file
//! retains the manager-level contract proof so the storage owner remains tested
//! independently of HTTP parsing and carrier setup.

#![cfg(feature = "lake")]

use eg_lake::{LakeField, LakeType};
use eg_tsdb::point::Point;
use eg_tsdb::store::SeriesStore;
use epistemic_graph::server::blob::store::RedbChunkStore;
use epistemic_graph::server::lake::{
    LakeManager, LakeVisibility, LoadTableAsOfError, DEFAULT_NAMESPACE,
};

const TEST_BUCKET_NS: u64 = 3_600_000_000_000;

fn store() -> RedbChunkStore {
    let dir = tsdb_dir("chunk-store");
    std::fs::create_dir_all(&dir).expect("create chunk store dir");
    RedbChunkStore::open(&dir.to_string_lossy()).expect("open chunk store")
}

fn tsdb_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "eg-lake-adopt-asof-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after unix epoch")
            .as_nanos()
    ))
}

fn points(from: i64, n: i64) -> Vec<Point> {
    (0..n)
        .map(|i| Point::single(from + i, (from + i) as f64 * 1.5))
        .collect()
}

fn current_snapshot(response: &serde_json::Value) -> &serde_json::Value {
    let current_id = &response["metadata"]["current-snapshot-id"];
    response["metadata"]["snapshots"]
        .as_array()
        .expect("Iceberg snapshots array")
        .iter()
        .find(|snapshot| &snapshot["snapshot-id"] == current_id)
        .expect("current-snapshot-id references an emitted snapshot")
}

fn total_data_files(response: &serde_json::Value) -> &str {
    current_snapshot(response)["summary"]["total-data-files"]
        .as_str()
        .expect("Iceberg total-data-files summary")
}

#[test]
fn as_of_reads_current_historical_and_empty_history_but_denies_uncommitted_lsns() {
    let s = store();
    let tsdb = SeriesStore::open_in_dir(
        &tsdb_dir("history"),
        epistemic_graph::store_authority::process_verifier(),
        epistemic_graph::store_authority::process_authority().principal(),
        &epistemic_graph::store_authority::process_authority().proof(),
    )
    .expect("open series store");
    let series_id = "adopt-asof-history";
    tsdb.append_batch(
        series_id,
        1,
        TEST_BUCKET_NS,
        &["v".to_string()],
        &points(0, 3),
    )
    .expect("append first points");

    let mgr = LakeManager::new();
    let unrelated_schema = eg_lake::LakeSchema::new(vec![LakeField::new("v", LakeType::Double)]);
    let before = mgr
        .create_table(
            &s,
            DEFAULT_NAMESPACE,
            "precreation-table",
            unrelated_schema.clone(),
            None,
        )
        .expect("create table before the target exists");
    let precreation_lsn = before["metadata"]["current-snapshot-id"]
        .as_u64()
        .expect("precreation global LSN");
    let first = mgr
        .drain_series(&s, &tsdb, series_id)
        .expect("first drain")
        .expect("first drain materializes");

    let before_target_creation = mgr
        .load_table_as_of(
            DEFAULT_NAMESPACE,
            series_id,
            precreation_lsn,
            &LakeVisibility::Unfiltered,
        )
        .expect("the precreation global LSN is committed");
    assert!(
        before_target_creation.is_none(),
        "a table is absent before its first emitted snapshot"
    );

    let other = mgr
        .create_table(
            &s,
            DEFAULT_NAMESPACE,
            "unrelated-table",
            unrelated_schema,
            None,
        )
        .expect("create unrelated table");
    let unrelated_lsn = other["metadata"]["current-snapshot-id"]
        .as_u64()
        .expect("unrelated table LSN");
    let at_unrelated_lsn = mgr
        .load_table_as_of(
            DEFAULT_NAMESPACE,
            series_id,
            unrelated_lsn,
            &LakeVisibility::Unfiltered,
        )
        .expect("the requested global LSN is committed")
        .expect("original table remains visible");
    assert_eq!(
        at_unrelated_lsn["metadata"]["current-snapshot-id"], first.lsn as i64,
        "another table's committed LSN resolves to this table's latest emitted snapshot"
    );
    assert_eq!(total_data_files(&at_unrelated_lsn), "1");

    tsdb.append_batch(
        series_id,
        1,
        TEST_BUCKET_NS,
        &["v".to_string()],
        &points(3, 3),
    )
    .expect("append second points");
    let second = mgr
        .drain_series(&s, &tsdb, series_id)
        .expect("second drain")
        .expect("second drain materializes");
    assert!(second.lsn > first.lsn, "writes receive increasing LSNs");

    let table = series_id;
    let current = mgr
        .load_table(DEFAULT_NAMESPACE, table)
        .expect("current table exists");
    assert_eq!(
        total_data_files(&current),
        "2",
        "current view has both appends"
    );

    let current_as_of = mgr
        .load_table_as_of(
            DEFAULT_NAMESPACE,
            table,
            second.lsn,
            &LakeVisibility::Unfiltered,
        )
        .expect("second LSN was committed")
        .expect("table is visible");
    assert_eq!(total_data_files(&current_as_of), "2");

    let compacted = mgr
        .compact(&s, DEFAULT_NAMESPACE, table)
        .expect("compact")
        .expect("compact rewrites the two live files");
    assert!(compacted.lsn > second.lsn);
    let compacted_current = mgr
        .load_table_as_of(
            DEFAULT_NAMESPACE,
            table,
            compacted.lsn,
            &LakeVisibility::Unfiltered,
        )
        .expect("compaction LSN was committed")
        .expect("table is visible");
    assert_eq!(
        total_data_files(&compacted_current),
        "1",
        "current rewrite is compacted"
    );
    let table_snapshot_ids: Vec<u64> = compacted_current["metadata"]["snapshots"]
        .as_array()
        .expect("Iceberg snapshots array")
        .iter()
        .map(|snapshot| snapshot["snapshot-id"].as_u64().unwrap())
        .collect();
    assert_eq!(
        table_snapshot_ids,
        vec![first.lsn, second.lsn, compacted.lsn],
        "history contains only this table's emitted materializations: no unrelated global LSN or rewrite tombstone"
    );
    assert!(!table_snapshot_ids.contains(&precreation_lsn));
    assert!(!table_snapshot_ids.contains(&unrelated_lsn));

    let historical = mgr
        .load_table_as_of(
            DEFAULT_NAMESPACE,
            table,
            second.lsn,
            &LakeVisibility::Unfiltered,
        )
        .expect("historical LSN was committed")
        .expect("table is visible");
    assert_eq!(
        total_data_files(&historical),
        "2",
        "historical read retains both files"
    );
    assert_eq!(
        historical["metadata"]["current-snapshot-id"],
        second.lsn as i64
    );

    let empty_history = mgr
        .load_table_as_of(DEFAULT_NAMESPACE, table, 0, &LakeVisibility::Unfiltered)
        .expect("LSN zero is a valid global boundary");
    assert!(
        empty_history.is_none(),
        "the table does not exist at the empty-history boundary"
    );

    let out_of_range = compacted.lsn + 1;
    assert!(matches!(
        mgr.load_table_as_of(
            DEFAULT_NAMESPACE,
            table,
            out_of_range,
            &LakeVisibility::Unfiltered,
        ),
        Err(LoadTableAsOfError::LsnUnavailable {
            requested,
            current_lsn: _,
        }) if requested == out_of_range
    ));
    assert!(matches!(
        mgr.load_table_as_of(
            DEFAULT_NAMESPACE,
            table,
            u64::MAX,
            &LakeVisibility::Unfiltered,
        ),
        Err(LoadTableAsOfError::LsnUnavailable {
            requested: u64::MAX,
            current_lsn: _,
        })
    ));
}

#[test]
fn as_of_applies_owner_visibility_before_lsn_validation() {
    let s = store();
    let mgr = LakeManager::new();
    let schema = eg_lake::LakeSchema::new(vec![LakeField::new("v", LakeType::Double)]);
    let table = "adopt-asof-owner";
    let created = mgr
        .create_table(&s, DEFAULT_NAMESPACE, table, schema, Some("tenant-a"))
        .expect("create owner-scoped table");
    let owner_lsn = created["metadata"]["current-snapshot-id"]
        .as_i64()
        .expect("created table snapshot LSN") as u64;

    let owner_view = mgr
        .load_table_as_of(
            DEFAULT_NAMESPACE,
            table,
            owner_lsn,
            &LakeVisibility::Owner("tenant-a".to_string()),
        )
        .expect("owner LSN was committed")
        .expect("owner can read the table");
    assert_eq!(
        total_data_files(&owner_view),
        "1",
        "create_table materializes one zero-row file"
    );
    assert_eq!(
        current_snapshot(&owner_view)["summary"]["total-records"],
        "0",
        "new table has no rows"
    );

    let other_owner = mgr
        .load_table_as_of(
            DEFAULT_NAMESPACE,
            table,
            owner_lsn,
            &LakeVisibility::Owner("tenant-b".to_string()),
        )
        .expect("visibility denial is not an LSN error");
    assert!(other_owner.is_none(), "cross-owner read is hidden");

    let admin_view = mgr
        .load_table_as_of(
            DEFAULT_NAMESPACE,
            table,
            owner_lsn,
            &LakeVisibility::Unfiltered,
        )
        .expect("owner LSN was committed")
        .expect("unfiltered read can see the table");
    assert_eq!(total_data_files(&admin_view), "1");

    let hidden_invalid = mgr
        .load_table_as_of(
            DEFAULT_NAMESPACE,
            table,
            u64::MAX,
            &LakeVisibility::Owner("tenant-b".to_string()),
        )
        .expect("wrong-owner invalid LSN remains an indistinguishable denial");
    assert!(hidden_invalid.is_none());

    assert!(mgr
        .load_table_as_of(
            DEFAULT_NAMESPACE,
            "does-not-exist",
            u64::MAX,
            &LakeVisibility::Unfiltered,
        )
        .expect("unknown table is not an LSN error")
        .is_none());
}
