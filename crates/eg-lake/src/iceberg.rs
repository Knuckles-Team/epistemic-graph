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

/// One Iceberg metadata generation retained by [`crate::LakeTable`]. Keeping this
/// separate from file-addition LSNs matters: a snapshot is valid only after its
/// metadata and manifest list have actually been emitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IcebergCommit {
    pub lsn: Lsn,
    pub timestamp_ms: i64,
    pub schema_id: i32,
}

pub(crate) struct IcebergBuildContext<'a> {
    pub schema_versions: &'a [(i32, LakeSchema)],
    pub current_schema_id: i32,
    pub snapshot: &'a SnapshotLog,
    pub lsn: Lsn,
    pub table_uuid: &'a str,
    pub location: &'a str,
    pub timestamp_ms: i64,
    pub history: &'a [IcebergCommit],
}

fn projected_commit(
    snapshot: &SnapshotLog,
    lsn: Lsn,
    schema_id: i32,
    timestamp_ms: i64,
) -> Option<IcebergCommit> {
    snapshot
        .all_files()
        .iter()
        .any(|file| file.added_at <= lsn)
        .then_some(IcebergCommit {
            lsn,
            timestamp_ms,
            schema_id,
        })
}

fn current_schema<'a>(ctx: &IcebergBuildContext<'a>) -> Option<&'a LakeSchema> {
    ctx.schema_versions
        .iter()
        .find(|(id, _)| *id == ctx.current_schema_id)
        .map(|(_, schema)| schema)
        .or_else(|| ctx.schema_versions.last().map(|(_, schema)| schema))
}

fn manifest_preview(ctx: &IcebergBuildContext<'_>, commit: Option<IcebergCommit>) -> String {
    let Some(commit) = commit else {
        return json!({
            "_note": "JSON preview; no Iceberg snapshot had been emitted as of this LSN",
            "manifest_list": Value::Null,
            "manifest_file": Value::Null,
            "schema-id": ctx.current_schema_id,
            "snapshot-id": Value::Null,
            "entries": [],
        })
        .to_string();
    };
    let snapshot_id = commit.lsn.value() as i64;
    let data_files: Vec<Value> = ctx
        .snapshot
        .files_as_of(commit.lsn)
        .iter()
        .map(|file| {
            json!({
                "status": 1,
                "data_file": {
                    "content": 0,
                    "file_path": format!("{}/{}", ctx.location, file.path),
                    "file_format": "PARQUET",
                    "record_count": file.num_rows,
                    "file_size_in_bytes": file.size_bytes,
                    "partition": {},
                    "schema-id": file.schema_id,
                }
            })
        })
        .collect();
    json!({
        "_note": "JSON preview; the real Avro manifest is written by iceberg_avro (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest)",
        "manifest_list": manifest_list_path(ctx.location, snapshot_id),
        "manifest_file": manifest_file_path(ctx.location, snapshot_id),
        "schema-id": commit.schema_id,
        "snapshot-id": snapshot_id,
        "entries": data_files,
    })
    .to_string()
}

fn commits_as_of(ctx: &IcebergBuildContext<'_>) -> Vec<IcebergCommit> {
    ctx.history
        .iter()
        .filter(|commit| commit.lsn <= ctx.lsn)
        .copied()
        .collect()
}

fn snapshot_value(
    ctx: &IcebergBuildContext<'_>,
    commit: IcebergCommit,
    parent: Option<Lsn>,
) -> Value {
    let live = ctx.snapshot.files_as_of(commit.lsn);
    let mut value = json!({
        "snapshot-id": commit.lsn.value() as i64,
        "sequence-number": commit.lsn.value() as i64,
        "timestamp-ms": commit.timestamp_ms,
        "summary": {
            "operation": "append",
            "total-records": live.iter().map(|file| file.num_rows).sum::<u64>().to_string(),
            "total-files-size": live.iter().map(|file| file.size_bytes).sum::<u64>().to_string(),
            "total-data-files": live.len().to_string(),
            "epistemic-graph-lsn": commit.lsn.value().to_string(),
        },
        "manifest-list": manifest_list_path(ctx.location, commit.lsn.value() as i64),
        "schema-id": commit.schema_id,
    });
    if let Some(parent_lsn) = parent {
        value["parent-snapshot-id"] = json!(parent_lsn.value() as i64);
    }
    value
}

fn snapshots(ctx: &IcebergBuildContext<'_>, commits: &[IcebergCommit]) -> Vec<Value> {
    commits
        .iter()
        .enumerate()
        .map(|(index, commit)| {
            snapshot_value(ctx, *commit, index.checked_sub(1).map(|i| commits[i].lsn))
        })
        .collect()
}

fn snapshot_log(commits: &[IcebergCommit]) -> Vec<Value> {
    commits
        .iter()
        .map(|commit| {
            json!({
                "snapshot-id": commit.lsn.value() as i64,
                "timestamp-ms": commit.timestamp_ms,
            })
        })
        .collect()
}

fn metadata_log(location: &str, commits: &[IcebergCommit]) -> Vec<Value> {
    commits
        .iter()
        .take(commits.len().saturating_sub(1))
        .map(|commit| {
            json!({
                "metadata-file": format!(
                    "{location}/metadata/v{}.metadata.json",
                    commit.lsn.value()
                ),
                "timestamp-ms": commit.timestamp_ms,
            })
        })
        .collect()
}

fn name_mapping(schema: Option<&LakeSchema>) -> String {
    let mapping: Vec<Value> = schema
        .map(|schema| {
            schema
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| json!({ "field-id": index + 1, "names": [field.name] }))
                .collect()
        })
        .unwrap_or_default();
    serde_json::to_string(&mapping).unwrap_or_else(|_| "[]".to_string())
}

fn metadata_document(
    ctx: &IcebergBuildContext<'_>,
    schema: Option<&LakeSchema>,
    commits: &[IcebergCommit],
) -> Value {
    let current = commits.last().copied();
    let snapshot_id = current
        .map(|commit| commit.lsn.value() as i64)
        .unwrap_or(-1);
    let last_sequence_number = current.map(|commit| commit.lsn.value() as i64).unwrap_or(0);
    let current_schema_id = current
        .map(|commit| commit.schema_id)
        .unwrap_or(ctx.current_schema_id);
    let last_updated_ms = current
        .map(|commit| commit.timestamp_ms)
        .unwrap_or(ctx.timestamp_ms);
    let schemas: Vec<Value> = ctx
        .schema_versions
        .iter()
        .filter(|(id, _)| *id <= current_schema_id)
        .map(|(id, schema)| iceberg_schema(schema, *id))
        .collect();
    json!({
        "format-version": 2,
        "table-uuid": ctx.table_uuid,
        "location": ctx.location,
        "last-sequence-number": last_sequence_number,
        "last-updated-ms": last_updated_ms,
        "last-column-id": schema.map(|schema| schema.len() as i64).unwrap_or(0),
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
            "schema.name-mapping.default": name_mapping(schema),
        },
        "current-snapshot-id": snapshot_id,
        "snapshots": snapshots(ctx, commits),
        "snapshot-log": snapshot_log(commits),
        "metadata-log": metadata_log(ctx.location, commits),
    })
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
    let lsn = snapshot.current_lsn();
    let commit = projected_commit(snapshot, lsn, current_schema_id, timestamp_ms);
    let history = commit.as_ref().map(std::slice::from_ref).unwrap_or(&[]);
    build_iceberg_as_of_with_history(IcebergBuildContext {
        schema_versions,
        current_schema_id,
        snapshot,
        lsn,
        table_uuid,
        location,
        timestamp_ms,
        history,
    })
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
    let commit = projected_commit(snapshot, lsn, current_schema_id, timestamp_ms);
    let history = commit.as_ref().map(std::slice::from_ref).unwrap_or(&[]);
    build_iceberg_as_of_with_history(IcebergBuildContext {
        schema_versions,
        current_schema_id,
        snapshot,
        lsn,
        table_uuid,
        location,
        timestamp_ms,
        history,
    })
}

/// Render metadata with the Iceberg generations this table instance has actually
/// emitted. File-addition LSNs alone are insufficient: advertising an LSN whose
/// manifest list was never written would create a dangling, invalid snapshot.
pub(crate) fn build_iceberg_as_of_with_history(ctx: IcebergBuildContext<'_>) -> IcebergTable {
    let commits = commits_as_of(&ctx);
    let current = commits.last().copied();
    let snapshot_id = current.map(|commit| commit.lsn.value() as i64).unwrap_or(0);
    let current_schema_id = current
        .map(|commit| commit.schema_id)
        .unwrap_or(ctx.current_schema_id);
    let schema = ctx
        .schema_versions
        .iter()
        .find(|(id, _)| *id == current_schema_id)
        .map(|(_, schema)| schema)
        .or_else(|| current_schema(&ctx));
    let metadata = metadata_document(&ctx, schema, &commits);
    IcebergTable {
        metadata_json: serde_json::to_string_pretty(&metadata).unwrap_or_else(|_| "{}".into()),
        manifest_json: manifest_preview(&ctx, current),
        metadata_location: format!("{}/metadata/v{snapshot_id}.metadata.json", ctx.location),
    }
}

/// Parse an Iceberg `metadata.json` back to a value (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns) — used by the
/// round-trip test and the catalog to read `current-snapshot-id` etc.
pub fn parse_metadata(metadata_json: &str) -> Result<Value, String> {
    serde_json::from_str(metadata_json).map_err(|e| format!("iceberg metadata json: {e}"))
}
