//! `lake-fixture-export` — write a small, deterministic fixture table's REAL Delta +
//! Iceberg artifacts (Parquet data files, `_delta_log`, `metadata.json`, Avro
//! manifests) straight to a plain local directory, via the SAME `eg-lake` production
//! write calls `src/server/lake::LakeManager::materialize_batch` makes against the
//! blob CAS (CONCEPT:EG-317, W4.8).
//!
//! Exists so the Python read-parity test suite
//! (`tests/test_lake_iceberg_delta_parity.py`) can point a real, unmodified
//! pyiceberg/deltalake reader at genuinely engine-written files, without a live
//! server, blob CAS, or HTTP round trip in between — this binary IS the documented
//! engine-side seam (`LakeTable::materialize`/`delta_log`/`iceberg`/
//! `iceberg_manifests`), it just writes to `std::fs` instead of the content-addressed
//! blob store. Two commits (CREATE then APPEND) are written so a reader must also
//! replay Delta/Iceberg version history correctly, not just open one metadata
//! generation.
//!
//! ```text
//! lake-fixture-export <output-dir>
//! ```
//! Prints one line of JSON to stdout: the table's real filesystem `location`, the
//! CURRENT `metadata_location`, and the exact row values written — the single source
//! of truth the Python test asserts pyiceberg/deltalake read back correctly.

use std::fs;
use std::path::Path;

use eg_lake::schema::{CellValue, LakeBatch, LakeField, LakeSchema, LakeType};
use eg_lake::snapshot::Lsn;
use eg_lake::LakeTable;

const NAMESPACE: &str = "lake_parity_test";
const TABLE: &str = "fixture_table";

/// (id, price, symbol, active, ts-micros); `None` is a NULL cell.
type FixtureRow = (
    i64,
    Option<f64>,
    Option<&'static str>,
    Option<bool>,
    Option<i64>,
);

const BATCH_1: &[FixtureRow] = &[
    (
        1,
        Some(101.5),
        Some("AAPL"),
        Some(true),
        Some(1_700_000_000_000_000),
    ),
    (
        2,
        None,
        Some("MSFT"),
        Some(false),
        Some(1_700_000_060_000_000),
    ),
    (
        3,
        Some(-3.25),
        None,
        Some(true),
        Some(1_700_000_120_000_000),
    ),
];
const BATCH_2: &[FixtureRow] = &[
    (
        4,
        Some(55.0),
        Some("GOOG"),
        Some(true),
        Some(1_700_000_180_000_000),
    ),
    (
        5,
        Some(0.0),
        Some("AMZN"),
        Some(false),
        Some(1_700_000_240_000_000),
    ),
];

fn schema() -> LakeSchema {
    LakeSchema::new(vec![
        LakeField::required("id", LakeType::Long),
        LakeField::new("price", LakeType::Double),
        LakeField::new("symbol", LakeType::String),
        LakeField::new("active", LakeType::Bool),
        LakeField::new("ts", LakeType::Timestamp),
    ])
}

fn to_batch(rows: &[FixtureRow]) -> LakeBatch {
    let cells = rows
        .iter()
        .map(|(id, price, symbol, active, ts)| {
            vec![
                CellValue::Long(*id),
                price.map(CellValue::Double).unwrap_or(CellValue::Null),
                symbol
                    .map(|s| CellValue::String(s.to_string()))
                    .unwrap_or(CellValue::Null),
                active.map(CellValue::Bool).unwrap_or(CellValue::Null),
                ts.map(CellValue::Timestamp).unwrap_or(CellValue::Null),
            ]
        })
        .collect();
    LakeBatch::new(schema(), cells).expect("fixture batch is well-formed")
}

/// `rel` is relative to `location` (the shape `LakeTable::materialize`/`delta_log`
/// return) — mirrors `LakeManager::materialize_batch`'s
/// `format!("{location}/{rel_path}")` prefixing exactly.
fn write_rel(location: &str, rel: &str, bytes: &[u8]) {
    write_abs(&format!("{location}/{rel}"), bytes);
}

/// `abs` already carries the `location` prefix (the shape `LakeTable::iceberg`'s
/// `metadata_location` and `LakeTable::iceberg_manifests`'s `manifest_path`/
/// `manifest_list_path` return) — used as-is, exactly like
/// `LakeManager::materialize_batch` does.
fn write_abs(abs: &str, bytes: &[u8]) {
    let full = Path::new(abs);
    if let Some(parent) = full.parent() {
        fs::create_dir_all(parent).unwrap_or_else(|e| {
            panic!("create parent dir for {abs}: {e}");
        });
    }
    fs::write(full, bytes).unwrap_or_else(|e| panic!("write {abs}: {e}"));
}

/// One commit: materialize `batch` at `lsn`, then write the Parquet data file + the
/// FULL (re-rendered) Delta log + Iceberg metadata.json + Avro manifest/manifest-list
/// to real files under `location` — the identical sequence
/// `LakeManager::materialize_batch` runs against the blob CAS. Returns the just-written
/// `metadata_location` (absolute).
fn commit(
    table: &mut LakeTable,
    location: &str,
    batch: &LakeBatch,
    lsn: Lsn,
    ts_ms: i64,
) -> String {
    let (rel_path, bytes) = table.materialize(batch, lsn).expect("materialize batch");
    write_rel(location, &rel_path, &bytes);

    for f in table.delta_log(ts_ms) {
        write_rel(location, &f.path, f.content.as_bytes());
    }

    let ib = table.iceberg(ts_ms);
    write_abs(&ib.metadata_location, ib.metadata_json.as_bytes());
    let manifests = table
        .iceberg_manifests()
        .expect("build iceberg avro manifests");
    write_abs(&manifests.manifest_path, &manifests.manifest_avro);
    write_abs(&manifests.manifest_list_path, &manifests.manifest_list_avro);

    ib.metadata_location
}

fn row_json((id, price, symbol, active, ts): &FixtureRow) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "price": price,
        "symbol": symbol,
        "active": active,
        "ts_micros": ts,
    })
}

fn main() {
    let out_dir = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: lake-fixture-export <output-dir>");
        std::process::exit(2);
    });
    let location = Path::new(&out_dir)
        .join(NAMESPACE)
        .join(TABLE)
        .to_string_lossy()
        .into_owned();
    fs::create_dir_all(&location).unwrap_or_else(|e| panic!("create table root: {e}"));

    let mut table = LakeTable::new(NAMESPACE, TABLE, schema(), location.clone());

    let batch1 = to_batch(BATCH_1);
    commit(&mut table, &location, &batch1, Lsn(1), 1_700_000_000_000);

    let batch2 = to_batch(BATCH_2);
    let metadata_location = commit(&mut table, &location, &batch2, Lsn(2), 1_700_000_300_000);

    let rows: Vec<serde_json::Value> = BATCH_1.iter().chain(BATCH_2.iter()).map(row_json).collect();
    let summary = serde_json::json!({
        "namespace": NAMESPACE,
        "table": TABLE,
        "location": location,
        "metadata_location": metadata_location,
        "row_count": rows.len(),
        "rows": rows,
    });
    println!("{summary}");
}
