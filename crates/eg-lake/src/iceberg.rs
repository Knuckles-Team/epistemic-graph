//! Apache Iceberg table metadata (CONCEPT:EG-317).
//!
//! Iceberg describes a table with a chain of JSON `metadata.json` files, each pointing
//! at a *manifest list* which points at *manifest files* which list the data
//! (Parquet) files. The `metadata.json` layer is pure JSON and is written FULLY here —
//! format-version 2, a typed schema, a partition spec, a sort order, and a snapshot
//! whose `manifest-list` locates the data files.
//!
//! ## What is a stub (documented, per CONCEPT:EG-317)
//! The manifest-list and manifest files themselves are, in the Iceberg spec, **Avro**
//! containers. Emitting real Avro would pull an Avro codec dep; to keep `eg-lake`
//! lean, [`build_iceberg`] emits a JSON *representation* of the manifest entries
//! ([`IcebergTable::manifest_json`]) alongside the real `metadata.json`. So the
//! `metadata.json` is spec-correct and machine-parseable, but a stock Iceberg reader
//! that follows `manifest-list` will look for an Avro file — that half is a STUB. The
//! read-consistent, fully-external-readable format in this crate is **Delta**
//! (`crate::delta`); Iceberg here gives the metadata/catalog seam plus a clearly
//! marked manifest stub. Real Avro manifests are a documented follow-up.

use serde_json::{json, Value};

use crate::schema::LakeSchema;
use crate::snapshot::{Lsn, SnapshotLog};

/// The Iceberg artifacts for a table snapshot (CONCEPT:EG-317).
#[derive(Clone, Debug, PartialEq)]
pub struct IcebergTable {
    /// The spec-correct `metadata.json` content (format-version 2).
    pub metadata_json: String,
    /// A JSON *representation* of the manifest entries (data files). NOTE: the Iceberg
    /// spec mandates Avro here — this is a documented STUB (see module docs).
    pub manifest_json: String,
    /// Where the metadata.json should live (object-store-relative).
    pub metadata_location: String,
}

/// The Iceberg typed schema for a [`LakeSchema`] with 1-based field ids
/// (CONCEPT:EG-317).
fn iceberg_schema(schema: &LakeSchema) -> Value {
    let fields: Vec<Value> = schema
        .fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            json!({
                "id": i + 1,
                "name": f.name,
                "required": !f.nullable,
                "type": f.ty.iceberg_type_name(),
            })
        })
        .collect();
    json!({ "type": "struct", "schema-id": 0, "fields": fields })
}

/// Build the Iceberg `metadata.json` (real) + a manifest JSON stub for the file set
/// live as of the snapshot's current LSN (CONCEPT:EG-317).
///
/// `table_uuid` is the stable Iceberg table id; `location` is the table root on the
/// object store (the Parquet files + `metadata/` live under it); `timestamp_ms` stamps
/// the snapshot deterministically. The Iceberg snapshot id is derived from the engine
/// LSN so the two version lines stay correlated.
pub fn build_iceberg(
    schema: &LakeSchema,
    snapshot: &SnapshotLog,
    table_uuid: &str,
    location: &str,
    timestamp_ms: i64,
) -> IcebergTable {
    let lsn: Lsn = snapshot.current_lsn();
    let snapshot_id: i64 = lsn.value() as i64;
    let last_column_id = schema.len() as i64;

    let manifest_list = format!("{location}/metadata/snap-{snapshot_id}-manifest-list.avro");
    let metadata_location = format!("{location}/metadata/v{snapshot_id}.metadata.json");

    // Data-file entries live as of the current LSN — the manifest content (stubbed to
    // JSON; real Iceberg is Avro).
    let live = snapshot.live_files();
    let total_rows: u64 = live.iter().map(|f| f.num_rows).sum();
    let total_size: u64 = live.iter().map(|f| f.size_bytes).sum();

    let data_files: Vec<Value> = live
        .iter()
        .map(|f| {
            json!({
                "status": 1, // ADDED
                "data_file": {
                    "content": 0, // DATA
                    "file_path": format!("{location}/{}", f.path),
                    "file_format": "PARQUET",
                    "record_count": f.num_rows,
                    "file_size_in_bytes": f.size_bytes,
                    "partition": {},
                }
            })
        })
        .collect();

    let manifest_json = json!({
        "_stub": "JSON representation; the Iceberg spec mandates Avro (CONCEPT:EG-317)",
        "manifest_list": manifest_list,
        "schema-id": 0,
        "snapshot-id": snapshot_id,
        "entries": data_files,
    })
    .to_string();

    let snapshot_obj = json!({
        "snapshot-id": snapshot_id,
        "timestamp-ms": timestamp_ms,
        "summary": {
            "operation": "append",
            "total-records": total_rows.to_string(),
            "total-files-size": total_size.to_string(),
            "total-data-files": live.len().to_string(),
            "epistemic-graph-lsn": lsn.value().to_string(),
        },
        "manifest-list": manifest_list,
        "schema-id": 0,
    });

    let metadata = json!({
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": location,
        "last-sequence-number": snapshot_id,
        "last-updated-ms": timestamp_ms,
        "last-column-id": last_column_id,
        "current-schema-id": 0,
        "schemas": [iceberg_schema(schema)],
        "default-spec-id": 0,
        "partition-specs": [ { "spec-id": 0, "fields": [] } ],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [ { "order-id": 0, "fields": [] } ],
        "properties": {
            "engine": "epistemic-graph/eg-lake",
            "concept": "EG-317",
        },
        "current-snapshot-id": snapshot_id,
        "snapshots": [snapshot_obj],
        "snapshot-log": [ { "snapshot-id": snapshot_id, "timestamp-ms": timestamp_ms } ],
        "metadata-log": [],
    });

    IcebergTable {
        metadata_json: serde_json::to_string_pretty(&metadata).unwrap_or_else(|_| "{}".into()),
        manifest_json,
        metadata_location,
    }
}

/// Parse an Iceberg `metadata.json` back to a value (CONCEPT:EG-317) — used by the
/// round-trip test and the catalog to read `current-snapshot-id` etc.
pub fn parse_metadata(metadata_json: &str) -> Result<Value, String> {
    serde_json::from_str(metadata_json).map_err(|e| format!("iceberg metadata json: {e}"))
}
