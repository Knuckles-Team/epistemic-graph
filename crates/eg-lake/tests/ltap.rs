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

/// CONCEPT:EG-317 — the Iceberg metadata.json is real & parseable and references the
/// real Avro manifest paths (the Avro itself is round-tripped in the EG-333 test).
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
    // The metadata references the real Avro manifest-list path; the JSON preview mirrors
    // the entries (the real Avro is asserted in eg_333_iceberg_avro_manifest_roundtrip).
    let man: serde_json::Value = serde_json::from_str(&ib.manifest_json).unwrap();
    assert!(man["manifest_list"]
        .as_str()
        .unwrap()
        .ends_with("-manifest-list.avro"));
    assert!(man["manifest_file"].as_str().unwrap().ends_with("-m0.avro"));
    assert_eq!(man["entries"].as_array().unwrap().len(), 1);
}

/// CONCEPT:EG-333/EG-334 — the Iceberg **Avro** manifest + manifest-list are written as
/// real Avro containers, parse back, reference each other, and the manifest entries
/// match the live data files. Also asserts the `metadata.json` snapshot's `manifest-list`
/// points at the exact manifest-list path we materialized.
#[cfg(feature = "lake")]
#[test]
fn eg_333_iceberg_avro_manifest_roundtrip() {
    use apache_avro::types::Value as AvroValue;
    use eg_lake::iceberg_avro::read_avro_records;

    let mut table = LakeTable::new("market", "quotes", sample_schema(), "s3://lake/quotes");
    table.record_file("data/part-0.parquet", 512, 3, Lsn(41));
    table.record_file("data/part-1.parquet", 640, 4, Lsn(42));

    let m = table.iceberg_manifests().expect("build avro manifests");
    assert_eq!(m.snapshot_id, 42);
    assert_eq!(m.added_files, 2);
    assert_eq!(m.added_rows, 7);
    assert!(m.manifest_path.ends_with("-m0.avro"));
    assert!(m.manifest_list_path.ends_with("-manifest-list.avro"));

    // Both blobs are real Avro containers (magic "Obj\x01").
    assert_eq!(&m.manifest_avro[..4], b"Obj\x01", "not an avro manifest");
    assert_eq!(
        &m.manifest_list_avro[..4],
        b"Obj\x01",
        "not an avro manifest-list"
    );

    // metadata.json's snapshot references the exact manifest-list path we wrote.
    let ib = table.iceberg(1_700_000_000_000);
    let meta = iceberg::parse_metadata(&ib.metadata_json).expect("parse metadata");
    assert_eq!(
        meta["snapshots"][0]["manifest-list"].as_str().unwrap(),
        m.manifest_list_path
    );

    // Manifest list parses to one manifest_file record pointing at our manifest.
    let list = read_avro_records(&m.manifest_list_avro).expect("read manifest list");
    assert_eq!(list.len(), 1);
    let fields = match &list[0] {
        AvroValue::Record(f) => f,
        other => panic!("expected record, got {other:?}"),
    };
    let get = |name: &str| fields.iter().find(|(k, _)| k == name).map(|(_, v)| v);
    match get("manifest_path").unwrap() {
        AvroValue::String(s) => assert_eq!(s, &m.manifest_path),
        other => panic!("manifest_path not a string: {other:?}"),
    }
    assert!(matches!(
        get("added_files_count").unwrap(),
        AvroValue::Int(2)
    ));
    assert!(matches!(
        get("added_rows_count").unwrap(),
        AvroValue::Long(7)
    ));
    assert!(matches!(get("content").unwrap(), AvroValue::Int(0)));

    // Manifest parses to one entry per live data file, matching path/rows/size.
    let entries = read_avro_records(&m.manifest_avro).expect("read manifest");
    assert_eq!(entries.len(), 2);
    let mut seen: Vec<(String, i64, i64)> = Vec::new();
    for e in &entries {
        let ef = match e {
            AvroValue::Record(f) => f,
            other => panic!("entry not a record: {other:?}"),
        };
        // status == ADDED (1)
        let status = ef
            .iter()
            .find(|(k, _)| k == "status")
            .map(|(_, v)| v)
            .unwrap();
        assert!(matches!(status, AvroValue::Int(1)), "status must be ADDED");
        let data_file = ef
            .iter()
            .find(|(k, _)| k == "data_file")
            .map(|(_, v)| v)
            .unwrap();
        let dff = match data_file {
            AvroValue::Record(f) => f,
            other => panic!("data_file not a record: {other:?}"),
        };
        let dget = |name: &str| dff.iter().find(|(k, _)| k == name).map(|(_, v)| v).unwrap();
        // content == DATA (0), format PARQUET
        assert!(matches!(dget("content"), AvroValue::Int(0)));
        match dget("file_format") {
            AvroValue::String(s) => assert_eq!(s, "PARQUET"),
            other => panic!("file_format: {other:?}"),
        }
        let path = match dget("file_path") {
            AvroValue::String(s) => s.clone(),
            other => panic!("file_path: {other:?}"),
        };
        let rows = match dget("record_count") {
            AvroValue::Long(n) => *n,
            other => panic!("record_count: {other:?}"),
        };
        let size = match dget("file_size_in_bytes") {
            AvroValue::Long(n) => *n,
            other => panic!("file_size_in_bytes: {other:?}"),
        };
        seen.push((path, rows, size));
    }
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ("s3://lake/quotes/data/part-0.parquet".to_string(), 3, 512),
            ("s3://lake/quotes/data/part-1.parquet".to_string(), 4, 640),
        ]
    );

    // The manifest embeds the required Iceberg metadata (format-version 2, data content).
    let reader = apache_avro::Reader::new(&m.manifest_avro[..]).expect("open manifest");
    let md = reader.user_metadata();
    assert_eq!(
        md.get("format-version").map(|v| v.as_slice()),
        Some(&b"2"[..])
    );
    assert_eq!(md.get("content").map(|v| v.as_slice()), Some(&b"data"[..]));
    assert!(
        md.contains_key("schema"),
        "manifest must embed the iceberg schema"
    );
}

/// CONCEPT:EG-350 — the Iceberg Avro manifest carries per-column stats (value/null/nan
/// counts, column sizes, and typed lower/upper bounds) gathered as the file is
/// materialized, so an external reader can skip files by predicate. Asserts the stats
/// maps are present and correct (min/max/null-count) for the known `sample_batch`.
#[cfg(feature = "lake")]
#[test]
fn eg_350_iceberg_manifest_carries_column_stats() {
    use apache_avro::types::Value as AvroValue;
    use eg_lake::iceberg_avro::read_avro_records;

    let mut table = LakeTable::new("market", "quotes", sample_schema(), "s3://lake/quotes");
    // Materialize the known 3-row batch — this is the path that gathers column stats.
    table
        .materialize(&sample_batch(), Lsn(77))
        .expect("materialize");

    let m = table.iceberg_manifests().expect("build avro manifests");
    let entries = read_avro_records(&m.manifest_avro).expect("read manifest");
    assert_eq!(entries.len(), 1, "one data file");

    // Drill into the single data_file record.
    let ef = match &entries[0] {
        AvroValue::Record(f) => f,
        other => panic!("entry not a record: {other:?}"),
    };
    let data_file = ef
        .iter()
        .find(|(k, _)| k == "data_file")
        .map(|(_, v)| v)
        .unwrap();
    let dff = match data_file {
        AvroValue::Record(f) => f,
        other => panic!("data_file not a record: {other:?}"),
    };
    let dget = |name: &str| dff.iter().find(|(k, _)| k == name).map(|(_, v)| v).unwrap();

    // A stats map field is `["null", array<{key:int, value:X}>]`. Decode it into a
    // field-id → value map. Panics if the union is null (stat absent).
    let as_map = |field: &str| -> std::collections::HashMap<i32, AvroValue> {
        let arr = match dget(field) {
            AvroValue::Union(_, boxed) => match boxed.as_ref() {
                AvroValue::Array(items) => items.clone(),
                other => panic!("{field} not an array: {other:?}"),
            },
            other => panic!("{field} not a union: {other:?}"),
        };
        let mut out = std::collections::HashMap::new();
        for it in arr {
            let kv = match it {
                AvroValue::Record(f) => f,
                other => panic!("map entry not a record: {other:?}"),
            };
            let key = match kv.iter().find(|(k, _)| k == "key").map(|(_, v)| v).unwrap() {
                AvroValue::Int(n) => *n,
                other => panic!("key not int: {other:?}"),
            };
            let val = kv
                .iter()
                .find(|(k, _)| k == "value")
                .map(|(_, v)| v.clone())
                .unwrap();
            out.insert(key, val);
        }
        out
    };
    let long_of = |v: &AvroValue| match v {
        AvroValue::Long(n) => *n,
        other => panic!("expected long: {other:?}"),
    };
    let bytes_of = |v: &AvroValue| match v {
        AvroValue::Bytes(b) => b.clone(),
        other => panic!("expected bytes: {other:?}"),
    };

    // value_counts: every column has 3 values (rows), keyed by 1-based field-id.
    let values = as_map("value_counts");
    for fid in 1..=5 {
        assert_eq!(long_of(&values[&fid]), 3, "value_count for field {fid}");
    }

    // null_value_counts: price (field 2) and symbol (field 3) each have one null; the
    // rest zero. (Iceberg only needs the columns it tracks; we list all 5.)
    let nulls = as_map("null_value_counts");
    assert_eq!(long_of(&nulls[&1]), 0, "id nulls");
    assert_eq!(long_of(&nulls[&2]), 1, "price null");
    assert_eq!(long_of(&nulls[&3]), 1, "symbol null");
    assert_eq!(long_of(&nulls[&4]), 0, "active nulls");
    assert_eq!(long_of(&nulls[&5]), 0, "ts nulls");

    // nan_value_counts: no NaNs anywhere.
    let nans = as_map("nan_value_counts");
    assert_eq!(long_of(&nans[&2]), 0, "price nans");

    // column_sizes: present for every column, each a positive on-disk byte size.
    let sizes = as_map("column_sizes");
    for fid in 1..=5 {
        assert!(long_of(&sizes[&fid]) > 0, "column_size for field {fid}");
    }

    // lower/upper bounds are Iceberg single-value binary. Decode + assert the known
    // min/max of the sample batch.
    let lowers = as_map("lower_bounds");
    let uppers = as_map("upper_bounds");

    // id (field 1, Long): min 1, max 3.
    assert_eq!(
        i64::from_le_bytes(bytes_of(&lowers[&1]).try_into().unwrap()),
        1
    );
    assert_eq!(
        i64::from_le_bytes(bytes_of(&uppers[&1]).try_into().unwrap()),
        3
    );
    // price (field 2, Double): min -3.25, max 101.5 (the null is excluded).
    assert_eq!(
        f64::from_le_bytes(bytes_of(&lowers[&2]).try_into().unwrap()),
        -3.25
    );
    assert_eq!(
        f64::from_le_bytes(bytes_of(&uppers[&2]).try_into().unwrap()),
        101.5
    );
    // symbol (field 3, String): min "AAPL", max "MSFT" (the null is excluded).
    assert_eq!(
        String::from_utf8(bytes_of(&lowers[&3])).unwrap(),
        "AAPL".to_string()
    );
    assert_eq!(
        String::from_utf8(bytes_of(&uppers[&3])).unwrap(),
        "MSFT".to_string()
    );
    // active (field 4, Bool): min false (0x00), max true (0x01).
    assert_eq!(bytes_of(&lowers[&4]), vec![0u8]);
    assert_eq!(bytes_of(&uppers[&4]), vec![1u8]);
    // ts (field 5, Timestamp): min/max of the three micros values.
    assert_eq!(
        i64::from_le_bytes(bytes_of(&lowers[&5]).try_into().unwrap()),
        1_700_000_000_000_000
    );
    assert_eq!(
        i64::from_le_bytes(bytes_of(&uppers[&5]).try_into().unwrap()),
        1_700_000_120_000_000
    );
}

/// CONCEPT:EG-350 — a file recorded WITHOUT stats (the bare `record_file` seam) emits
/// the stats maps as null unions, not garbage — the maps are genuinely optional.
#[cfg(feature = "lake")]
#[test]
fn eg_350_stats_absent_when_not_gathered() {
    use apache_avro::types::Value as AvroValue;
    use eg_lake::iceberg_avro::read_avro_records;

    let mut table = LakeTable::new("market", "quotes", sample_schema(), "s3://lake/quotes");
    table.record_file("data/part-0.parquet", 512, 3, Lsn(9)); // no stats path

    let m = table.iceberg_manifests().expect("build avro manifests");
    let entries = read_avro_records(&m.manifest_avro).expect("read manifest");
    let ef = match &entries[0] {
        AvroValue::Record(f) => f,
        other => panic!("{other:?}"),
    };
    let dff = match ef.iter().find(|(k, _)| k == "data_file").map(|(_, v)| v) {
        Some(AvroValue::Record(f)) => f,
        other => panic!("{other:?}"),
    };
    for field in [
        "column_sizes",
        "value_counts",
        "null_value_counts",
        "nan_value_counts",
        "lower_bounds",
        "upper_bounds",
    ] {
        let v = dff
            .iter()
            .find(|(k, _)| k == field)
            .map(|(_, v)| v)
            .unwrap();
        assert!(
            matches!(v, AvroValue::Union(0, b) if matches!(b.as_ref(), AvroValue::Null)),
            "{field} must be a null union when no stats were gathered"
        );
    }
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
