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

/// The Iceberg typed schema for a [`LakeSchema`] with 1-based field ids and the given
/// `schema-id` (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns, CONCEPT:EG-KG.storage.iceberg-per-file-schema-id). `pub(crate)` so the Avro
/// manifest writer embeds the identical schema JSON in the manifest file's metadata
/// (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest).
pub(crate) fn iceberg_schema(schema: &LakeSchema, schema_id: i32) -> Value {
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
    json!({ "type": "struct", "schema-id": schema_id, "fields": fields })
}

/// Build the Iceberg `metadata.json` (real) + a manifest JSON stub for the file set
/// live as of the snapshot's current LSN (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
///
/// `schema_versions` is EVERY schema version the table has ever used, oldest first
/// (CONCEPT:EG-KG.storage.iceberg-per-file-schema-id, INT-P2-4) — rendered into `schemas[]` in full, so an external
/// reader sees the whole schema-evolution history, not just today's shape.
/// `current_schema_id` is the id in effect for NEW writes (`schema_versions`'s last
/// entry); each LIVE data-file's manifest-preview entry carries the schema-id it was
/// ACTUALLY written under ([`crate::snapshot::FileEntry::schema_id`]) rather than
/// always the current one, so a rewrite that lands new files under a newer schema
/// never relabels the older, still-live files' history.
///
/// `table_uuid` is the stable Iceberg table id; `location` is the table root on the
/// object store (the Parquet files + `metadata/` live under it); `timestamp_ms` stamps
/// the snapshot deterministically. The Iceberg snapshot id is derived from the engine
/// LSN so the two version lines stay correlated.
pub fn build_iceberg(
    schema_versions: &[(i32, LakeSchema)],
    current_schema_id: i32,
    snapshot: &SnapshotLog,
    table_uuid: &str,
    location: &str,
    timestamp_ms: i64,
) -> IcebergTable {
    build_iceberg_as_of(
        schema_versions,
        current_schema_id,
        snapshot,
        snapshot.current_lsn(),
        table_uuid,
        location,
        timestamp_ms,
    )
}

/// Build the Iceberg `metadata.json` (+ manifest JSON stub) for the file set live as
/// of an EXPLICIT `lsn` — the time-travel / `Op::AsOf` seam (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns): the
/// engine-side caller resolves a query-time as-of request (a timestamp or an explicit
/// snapshot/LSN) down to this `Lsn` and gets back the metadata a consistent historical
/// reader needs, exactly the shape [`build_iceberg`] returns for "now" (`lsn ==
/// snapshot.current_lsn()` reproduces [`build_iceberg`] byte-for-byte, since that is
/// this function with `lsn` defaulted to current). `lsn` need not be `<=
/// snapshot.current_lsn()` — a value beyond current simply includes every file live at
/// "now" (the same clamping [`crate::snapshot::FileEntry::visible_at`] already gives a
/// forward LSN).
pub fn build_iceberg_as_of(
    schema_versions: &[(i32, LakeSchema)],
    current_schema_id: i32,
    snapshot: &SnapshotLog,
    lsn: Lsn,
    table_uuid: &str,
    location: &str,
    timestamp_ms: i64,
) -> IcebergTable {
    let snapshot_id: i64 = lsn.value() as i64;
    let current_schema = schema_versions
        .iter()
        .find(|(id, _)| *id == current_schema_id)
        .map(|(_, s)| s)
        .or_else(|| schema_versions.last().map(|(_, s)| s));
    let last_column_id = current_schema.map(|s| s.len() as i64).unwrap_or(0);

    let manifest_list = manifest_list_path(location, snapshot_id);
    let metadata_location = format!("{location}/metadata/v{snapshot_id}.metadata.json");

    // Data-file entries live as of the REQUESTED lsn (not necessarily current) — the
    // manifest content (stubbed to JSON; real Iceberg is Avro).
    let live = snapshot.files_as_of(lsn);
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
                    // The schema-id THIS file was actually written under (CONCEPT:EG-KG.storage.iceberg-per-file-schema-id)
                    // -- may be OLDER than `current_schema_id` for a live file that
                    // predates a later evolve_add_column and hasn't been rewritten yet.
                    "schema-id": f.schema_id,
                }
            })
        })
        .collect();

    let manifest_json = json!({
        "_note": "JSON preview; the real Avro manifest is written by iceberg_avro (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest)",
        "manifest_list": manifest_list,
        "manifest_file": manifest_file_path(location, snapshot_id),
        "schema-id": current_schema_id,
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
        // The schema-id in effect for THIS commit (CONCEPT:EG-KG.storage.iceberg-per-file-schema-id) -- new commits
        // always write under the CURRENT schema; older, not-yet-rewritten live files
        // keep their own (possibly older) schema-id on their manifest entry above.
        "schema-id": current_schema_id,
    });

    // Full schema-evolution history (CONCEPT:EG-KG.storage.iceberg-per-file-schema-id) -- every version this table has
    // ever used, each correctly tagged with ITS OWN schema-id (not always 0).
    let schemas: Vec<Value> = schema_versions
        .iter()
        .map(|(id, s)| iceberg_schema(s, *id))
        .collect();

    // `schema.name-mapping.default` (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns): eg-lake's Parquet writer
    // (Polars, `parquet_io.rs`) does not embed Iceberg field-ids on Parquet columns the
    // way a native Iceberg writer does, so a spec-compliant reader (pyiceberg's
    // `pyarrow_to_schema`, and any other reader that enforces
    // https://iceberg.apache.org/spec/#column-projection) refuses to open the file
    // without EITHER embedded field-ids OR this table-property fallback: a JSON array
    // (itself embedded as a STRING value, per the spec's Name Mapping Serialization)
    // mapping each column NAME to its Iceberg field-id, so the reader resolves by name.
    // Built from the CURRENT schema (evolution is additive-only, CONCEPT:EG-KG.storage.iceberg-per-file-schema-id) —
    // matches the SAME 1-based, declaration-order ids `iceberg_schema` assigns.
    let name_mapping: Vec<Value> = current_schema
        .map(|s| {
            s.fields
                .iter()
                .enumerate()
                .map(|(i, f)| json!({ "field-id": i + 1, "names": [f.name] }))
                .collect()
        })
        .unwrap_or_default();
    let name_mapping_json =
        serde_json::to_string(&name_mapping).unwrap_or_else(|_| "[]".to_string());

    let metadata = json!({
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": location,
        "last-sequence-number": snapshot_id,
        "last-updated-ms": timestamp_ms,
        "last-column-id": last_column_id,
        "current-schema-id": current_schema_id,
        "schemas": schemas,
        "default-spec-id": 0,
        "partition-specs": [ { "spec-id": 0, "fields": [] } ],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [ { "order-id": 0, "fields": [] } ],
        "properties": {
            "engine": "epistemic-graph/eg-lake",
            "concept": "EG-KG.storage.lsn-as-snapshot-returns",
            "schema.name-mapping.default": name_mapping_json,
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
