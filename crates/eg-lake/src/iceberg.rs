//! Apache Iceberg table metadata (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
//!
//! Iceberg describes a table with a chain of JSON `metadata.json` files, each pointing
//! at a *manifest list* which points at *manifest files* which list the data
//! (Parquet) files. The `metadata.json` layer is pure JSON and is written FULLY here —
//! format-version 2, a typed schema, a partition spec, a sort order, and a snapshot
//! whose `manifest-list` locates the data files.
//!
//! ## Metadata (here) vs. the real Avro manifests (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest/EG-334)
//! The manifest-list and manifest files themselves are, in the Iceberg spec, **Avro**
//! containers. This module owns the pure-JSON `metadata.json` (format-version 2) plus
//! a convenience JSON *preview* of the manifest entries ([`IcebergTable::manifest_json`],
//! handy for tests / debugging and dependency-free). The **real, spec-compliant Avro
//! manifest + manifest-list writer** lives in [`crate::iceberg_avro`] behind the `lake`
//! feature (it needs the `apache-avro` codec dep) — the `metadata.json` written here
//! points its `manifest-list` at the exact object-store path
//! ([`manifest_list_path`]) that [`crate::iceberg_avro::build_iceberg_manifests`]
//! materializes, so a committed snapshot resolves to a real Avro manifest chain a stock
//! Iceberg reader (Spark/Trino/DuckDB) follows. Delta (`crate::delta`) remains a second,
//! fully-external-readable format; Iceberg now has both metadata AND real manifests.

use serde_json::{json, Value};

use crate::schema::LakeSchema;
use crate::snapshot::{Lsn, SnapshotLog};

/// The Iceberg artifacts for a table snapshot (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
#[derive(Clone, Debug, PartialEq)]
pub struct IcebergTable {
    /// The spec-correct `metadata.json` content (format-version 2).
    pub metadata_json: String,
    /// A JSON *preview* of the manifest entries (data files) — dependency-free and
    /// handy for tests/debugging. The spec-mandated **Avro** manifest is written by
    /// [`crate::iceberg_avro`] (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest) at [`manifest_file_path`]; this preview
    /// mirrors its entries.
    pub manifest_json: String,
    /// Where the metadata.json should live (object-store-relative).
    pub metadata_location: String,
}

/// Object-store path of the Iceberg **manifest list** Avro file for a snapshot
/// (CONCEPT:EG-KG.storage.iceberg-manifest-list). Shared by [`build_iceberg`] (which references it from the
/// snapshot's `manifest-list`) and [`crate::iceberg_avro`] (which writes it), so the
/// metadata always resolves to the real Avro file.
pub fn manifest_list_path(location: &str, snapshot_id: i64) -> String {
    format!("{location}/metadata/snap-{snapshot_id}-manifest-list.avro")
}

/// Object-store path of the Iceberg **manifest** Avro file for a snapshot
/// (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest) — the single data manifest the manifest list points at.
pub fn manifest_file_path(location: &str, snapshot_id: i64) -> String {
    format!("{location}/metadata/snap-{snapshot_id}-m0.avro")
}

/// The Iceberg typed schema for a [`LakeSchema`] with 1-based field ids
/// (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). `pub(crate)` so the Avro manifest writer embeds the identical
/// schema JSON in the manifest file's metadata (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest).
pub(crate) fn iceberg_schema(schema: &LakeSchema) -> Value {
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
/// live as of the snapshot's current LSN (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
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

    let manifest_list = manifest_list_path(location, snapshot_id);
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
        "_note": "JSON preview; the real Avro manifest is written by iceberg_avro (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest)",
        "manifest_list": manifest_list,
        "manifest_file": manifest_file_path(location, snapshot_id),
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
            "concept": "EG-KG.storage.lsn-as-snapshot-returns",
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

/// Parse an Iceberg `metadata.json` back to a value (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns) — used by the
/// round-trip test and the catalog to read `current-snapshot-id` etc.
pub fn parse_metadata(metadata_json: &str) -> Result<Value, String> {
    serde_json::from_str(metadata_json).map_err(|e| format!("iceberg metadata json: {e}"))
}
