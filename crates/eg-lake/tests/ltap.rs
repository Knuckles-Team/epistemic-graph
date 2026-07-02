//! LTAP lakehouse-interop tests (CONCEPT:EG-317).
//!
//! Covers the four seams: Parquet materialize→read-back (behind `lake`), the Delta
//! `_delta_log` + Iceberg `metadata.json` written & parsed back, the LSN as-of
//! snapshot, and the Iceberg-REST catalog listing.

use eg_lake::catalog::IcebergRestCatalog;
use eg_lake::schema::{CellValue, LakeBatch, LakeField, LakeSchema, LakeType};
use eg_lake::snapshot::Lsn;
use eg_lake::{delta, iceberg, LakeTable};

/// A 3-column schema exercising every LakeType (CONCEPT:EG-317).
fn sample_schema() -> LakeSchema {
    LakeSchema::new(vec![
        LakeField::required("id", LakeType::Long),
        LakeField::new("price", LakeType::Double),
        LakeField::new("symbol", LakeType::String),
        LakeField::new("active", LakeType::Bool),
        LakeField::new("ts", LakeType::Timestamp),
    ])
}

fn sample_batch() -> LakeBatch {
    let rows = vec![
        vec![
            CellValue::Long(1),
            CellValue::Double(101.5),
            CellValue::String("AAPL".into()),
            CellValue::Bool(true),
            CellValue::Timestamp(1_700_000_000_000_000),
        ],
        vec![
            CellValue::Long(2),
            CellValue::Null, // null price — exercises the null bitmap
            CellValue::String("MSFT".into()),
            CellValue::Bool(false),
            CellValue::Timestamp(1_700_000_060_000_000),
        ],
        vec![
            CellValue::Long(3),
            CellValue::Double(-3.25),
            CellValue::Null, // null string
            CellValue::Bool(true),
            CellValue::Timestamp(1_700_000_120_000_000),
        ],
    ];
    LakeBatch::new(sample_schema(), rows).expect("valid batch")
}

/// CONCEPT:EG-317 — a wrong-arity row is rejected.
#[test]
fn eg_317_batch_arity_is_validated() {
    let bad = LakeBatch::new(sample_schema(), vec![vec![CellValue::Long(1)]]);
    assert!(bad.is_err(), "short row must be rejected");
}

/// CONCEPT:EG-317 — rows materialize to Parquet and read back byte-for-cell correct,
/// nulls included.
#[cfg(feature = "lake")]
#[test]
fn eg_317_parquet_materialize_roundtrip_is_correct() {
    use eg_lake::parquet_io::{materialize_batch, read_parquet};

    let batch = sample_batch();
    let bytes = materialize_batch(&batch).expect("materialize");
    // A real Parquet file starts and ends with the "PAR1" magic.
    assert_eq!(&bytes[..4], b"PAR1", "not a parquet file");
    assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");

    let back = read_parquet(&bytes).expect("read back");
    assert_eq!(back.schema, batch.schema, "schema must round-trip");
    assert_eq!(back.rows, batch.rows, "rows must round-trip incl. nulls");
}

/// CONCEPT:EG-317 — Parquet stats (size + row count) are surfaced for the table log.
#[cfg(feature = "lake")]
#[test]
fn eg_317_parquet_stats_report_rows() {
    use eg_lake::parquet_io::materialize_with_stats;
    let (bytes, stats) = materialize_with_stats(&sample_batch()).expect("materialize");
    assert_eq!(stats.num_rows, 3);
    assert_eq!(stats.size_bytes, bytes.len() as u64);
    assert!(stats.size_bytes > 4);
}

/// CONCEPT:EG-317 — the Delta `_delta_log` is written and replays to the live files.
#[test]
fn eg_317_delta_log_written_and_parseable() {
    let mut table = LakeTable::new("market", "quotes", sample_schema(), "s3://lake/quotes");
    table.record_file("data/part-0.parquet", 512, 3, Lsn(10));
    table.record_file("data/part-1.parquet", 640, 4, Lsn(20));

    let log = table.delta_log(1_700_000_000_000);
    // Two distinct LSNs → two contiguous Delta versions, 0-padded to 20 digits.
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].path, "_delta_log/00000000000000000000.json");
    assert_eq!(log[1].path, "_delta_log/00000000000000000001.json");

    // Version 0 carries protocol + metaData + one add.
    let v0 = delta::parse_delta_actions(&log[0].content).expect("parse v0");
    assert!(v0.iter().any(|a| a.get("protocol").is_some()));
    let meta = v0
        .iter()
        .find_map(|a| a.get("metaData"))
        .expect("metaData present");
    // schemaString is a JSON-encoded struct with our 5 fields.
    let schema_str = meta["schemaString"].as_str().unwrap();
    let sv: serde_json::Value = serde_json::from_str(schema_str).unwrap();
    assert_eq!(sv["type"], "struct");
    assert_eq!(sv["fields"].as_array().unwrap().len(), 5);
    assert_eq!(sv["fields"][0]["type"], "long");

    // Replaying both commits yields both live files.
    let live = delta::live_paths(&log).expect("replay");
    assert_eq!(live.len(), 2);
    assert!(live.contains(&"data/part-0.parquet".to_string()));
    assert!(live.contains(&"data/part-1.parquet".to_string()));
}

/// CONCEPT:EG-317 — a Delta `remove` tombstone drops a file from the replayed live set.
#[test]
fn eg_317_delta_remove_tombstones_file() {
    let mut table = LakeTable::new("market", "quotes", sample_schema(), "s3://lake/quotes");
    table.record_file("data/part-0.parquet", 512, 3, Lsn(10));
    table.snapshot.remove_file("data/part-0.parquet", Lsn(30));
    table.record_file("data/part-1.parquet", 640, 4, Lsn(30));

    let log = table.delta_log(1);
    let live = delta::live_paths(&log).expect("replay");
    assert_eq!(live, vec!["data/part-1.parquet".to_string()]);
}

/// CONCEPT:EG-317 — the LSN as-of snapshot returns a consistent point-in-time file set.
#[test]
fn eg_317_lsn_as_of_snapshot_is_consistent() {
    let mut table = LakeTable::new("market", "quotes", sample_schema(), "s3://lake/quotes");
    table.record_file("data/part-0.parquet", 100, 1, Lsn(10));
    table.record_file("data/part-1.parquet", 200, 2, Lsn(20));
    table.snapshot.remove_file("data/part-0.parquet", Lsn(20));

    assert_eq!(table.current_lsn(), Lsn(20));
    // As of LSN 10: only part-0 is live.
    let at10 = table.snapshot.files_as_of(Lsn(10));
    assert_eq!(at10.len(), 1);
    assert_eq!(at10[0].path, "data/part-0.parquet");
    // As of LSN 20 (now): only part-1 (part-0 tombstoned).
    let at20 = table.snapshot.files_as_of(Lsn(20));
    assert_eq!(at20.len(), 1);
    assert_eq!(at20[0].path, "data/part-1.parquet");
    // Earlier than any commit: empty.
    assert!(table.snapshot.files_as_of(Lsn(0)).is_empty());
}

/// CONCEPT:EG-317 — the Iceberg metadata.json is real & parseable; the manifest is a
/// documented JSON stub.
#[test]
fn eg_317_iceberg_metadata_written_and_parseable() {
    let mut table = LakeTable::new("market", "quotes", sample_schema(), "s3://lake/quotes");
    table.record_file("data/part-0.parquet", 512, 3, Lsn(42));

    let ib = table.iceberg(1_700_000_000_000);
    let meta = iceberg::parse_metadata(&ib.metadata_json).expect("parse metadata");
    assert_eq!(meta["format-version"], 2);
    assert_eq!(meta["current-snapshot-id"], 42);
    assert_eq!(meta["schemas"][0]["fields"].as_array().unwrap().len(), 5);
    assert_eq!(meta["snapshots"][0]["summary"]["total-records"], "3");
    // The manifest is the documented Avro stub.
    let man: serde_json::Value = serde_json::from_str(&ib.manifest_json).unwrap();
    assert!(man["_stub"].as_str().unwrap().contains("Avro"));
    assert_eq!(man["entries"].as_array().unwrap().len(), 1);
}

/// CONCEPT:EG-317 — the Iceberg-REST catalog lists and loads a registered table.
#[test]
fn eg_317_catalog_lists_and_loads_table() {
    let mut table = LakeTable::new("market", "quotes", sample_schema(), "s3://lake/quotes");
    table.record_file("data/part-0.parquet", 512, 3, Lsn(7));

    let mut cat = IcebergRestCatalog::new();
    table.register_in(&mut cat, 1_700_000_000_000);
    assert_eq!(cat.len(), 1);

    // list_namespaces → [["market"]]
    let ns = cat.list_namespaces();
    assert_eq!(ns["namespaces"][0][0], "market");

    // list_tables → identifiers with our table.
    let tables = cat.list_tables("market");
    let ids = tables["identifiers"].as_array().unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0]["name"], "quotes");

    // load_table → metadata-location present, inline metadata parses.
    let loaded = cat.load_table("market", "quotes").expect("load");
    let loc = loaded["metadata-location"].as_str().unwrap();
    assert!(loc.ends_with(".metadata.json"));
    assert_eq!(loaded["metadata"]["current-snapshot-id"], 7);

    // Unknown table → None.
    assert!(cat.load_table("market", "nope").is_none());
}

/// CONCEPT:EG-317 — the full materialize path: rows → Parquet bytes recorded → Delta +
/// Iceberg + catalog all describe the same file, under one LSN.
#[cfg(feature = "lake")]
#[test]
fn eg_317_end_to_end_materialize_and_export() {
    let mut table = LakeTable::new("market", "quotes", sample_schema(), "s3://lake/quotes");
    let (path, bytes) = table
        .materialize(&sample_batch(), Lsn(100))
        .expect("materialize");
    assert_eq!(&bytes[..4], b"PAR1");
    assert!(path.ends_with(".parquet"));
    assert_eq!(table.current_lsn(), Lsn(100));

    // Delta references the materialized part.
    let log = table.delta_log(1);
    let live = delta::live_paths(&log).expect("replay");
    assert_eq!(live, vec![path.clone()]);

    // Iceberg + catalog agree on the same snapshot.
    let mut cat = IcebergRestCatalog::new();
    table.register_in(&mut cat, 1);
    let loaded = cat.load_table("market", "quotes").expect("load");
    assert_eq!(loaded["metadata"]["current-snapshot-id"], 100);
}
