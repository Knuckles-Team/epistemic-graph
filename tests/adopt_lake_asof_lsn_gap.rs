//! NE-033/NE-049 acceptance tests for the lake as-of read seam.
//!
//! These are real integration tests over the public `LakeManager` API.  They
//! prove that a caller can read the current and historical projections, that
//! the valid empty-history boundary remains available, and that nonexistent or
//! out-of-range LSNs are denied.  The same API applies `LakeVisibility` before
//! validating an LSN, so a cross-owner request is indistinguishable from a
//! missing table even when it supplies an invalid LSN.
//!
//! The established Iceberg REST `LoadTable` route in `src/server/lake/rest.rs`
//! has no as-of/snapshot-id query field.  This acceptance surface therefore
//! exercises the owning manager directly; it does not invent an unauthenticated
//! REST side channel.  If the protocol later gains an authenticated as-of
//! field, its adapter must call this same scoped API.

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

fn total_data_files(snapshot: &serde_json::Value) -> &str {
    snapshot["metadata"]["snapshots"][0]["summary"]["total-data-files"]
        .as_str()
        .expect("Iceberg total-data-files summary")
}

#[test]
fn as_of_reads_current_historical_and_empty_history_but_denies_uncommitted_lsns() {
    let s = store();
    let tsdb = SeriesStore::open_in_dir(&tsdb_dir("history")).expect("open series store");
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
    let first = mgr
        .drain_series(&s, &tsdb, series_id)
        .expect("first drain")
        .expect("first drain materializes");
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
        .expect("LSN zero is the valid empty history")
        .expect("table is visible");
    assert_eq!(total_data_files(&empty_history), "0");
    assert_eq!(empty_history["metadata"]["current-snapshot-id"], 0);

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
        owner_view["metadata"]["snapshots"][0]["summary"]["total-records"], "0",
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
