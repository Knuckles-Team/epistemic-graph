//! Real, spec-compliant Iceberg v2 **Avro** manifest + manifest-list writer
//! (CONCEPT:EG-333 manifest / CONCEPT:EG-334 manifest-list).
//!
//! The Iceberg table spec stores the file-list layer as **Avro** containers, not JSON:
//! a snapshot's `manifest-list` (an Avro file, one `manifest_file` record per manifest)
//! points at one or more **manifest** files (Avro, one `manifest_entry` record per data
//! file). [`crate::iceberg`] writes the pure-JSON `metadata.json` and points its
//! snapshot at [`crate::iceberg::manifest_list_path`]; THIS module materializes the real
//! Avro bytes at that exact path (+ the manifest file it references), so a stock Iceberg
//! reader (Spark / Trino / DuckDB) that follows `manifest-list` finds real, parseable
//! Avro and can read the engine's Iceberg tables end-to-end — no longer a stub.
//!
//! Gated behind the `lake` feature (needs the pure-Rust `apache-avro` codec), the same
//! gate the Parquet transcode lives behind, so the Pi build links no Avro (EG-317).
//!
//! ## Spec scope (honest)
//! * Format-version **2** manifests: `manifest_entry` carries `status`, `snapshot_id`,
//!   the (null-inherited) `sequence_number` / `file_sequence_number`, and a `data_file`
//!   with `content`, `file_path`, `file_format`, `partition`, `record_count`,
//!   `file_size_in_bytes`. Iceberg **field-ids** are embedded on every Avro field, and
//!   the required manifest metadata keys (`schema`, `schema-id`, `partition-spec`,
//!   `partition-spec-id`, `format-version`, `content`) sit in the Avro file header.
//! * The `manifest_file` records carry the v2 `content` / `sequence_number` /
//!   `min_sequence_number` / added·existing·deleted file & row counts.
//! * **Deferred (documented):** per-column stats (`column_sizes`, `value_counts`,
//!   `null_value_counts`, `lower_bounds`, `upper_bounds`) and partition `field_summary`
//!   bounds are OMITTED — the engine's `SnapshotLog` (EG-317) tracks only per-file
//!   `record_count` + `file_size`, and the crate materializes an UNPARTITIONED spec
//!   (`partition-spec` = `[]`), so these optional fields are legitimately absent rather
//!   than wrong. Container codec is Avro **null** (uncompressed) — universally readable.

use apache_avro::types::Value as AvroValue;
use apache_avro::{Reader, Schema, Writer};

use crate::iceberg::{iceberg_schema, manifest_file_path, manifest_list_path};
use crate::schema::LakeSchema;
use crate::snapshot::{FileEntry, Lsn, SnapshotLog};

/// Iceberg v2 `manifest_entry` Avro schema (CONCEPT:EG-333). Field-ids per the spec;
/// the `data_file` here is the unpartitioned, stats-free projection eg-lake tracks.
const MANIFEST_ENTRY_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int", "field-id": 0},
    {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
    {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3},
    {"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "r2",
      "fields": [
        {"name": "content", "type": "int", "field-id": 134},
        {"name": "file_path", "type": "string", "field-id": 100},
        {"name": "file_format", "type": "string", "field-id": 101},
        {"name": "partition", "type": {"type": "record", "name": "r102", "fields": []}, "field-id": 102},
        {"name": "record_count", "type": "long", "field-id": 103},
        {"name": "file_size_in_bytes", "type": "long", "field-id": 104}
      ]
    }, "field-id": 2}
  ]
}"#;

/// Iceberg v2 `manifest_file` Avro schema for the manifest list (CONCEPT:EG-334).
const MANIFEST_FILE_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string", "field-id": 500},
    {"name": "manifest_length", "type": "long", "field-id": 501},
    {"name": "partition_spec_id", "type": "int", "field-id": 502},
    {"name": "content", "type": "int", "field-id": 517},
    {"name": "sequence_number", "type": "long", "field-id": 515},
    {"name": "min_sequence_number", "type": "long", "field-id": 516},
    {"name": "added_snapshot_id", "type": "long", "field-id": 503},
    {"name": "added_files_count", "type": "int", "field-id": 504},
    {"name": "existing_files_count", "type": "int", "field-id": 505},
    {"name": "deleted_files_count", "type": "int", "field-id": 506},
    {"name": "added_rows_count", "type": "long", "field-id": 512},
    {"name": "existing_rows_count", "type": "long", "field-id": 513},
    {"name": "deleted_rows_count", "type": "long", "field-id": 514},
    {"name": "partitions", "type": ["null", {"type": "array", "items": {
      "type": "record",
      "name": "field_summary",
      "fields": [
        {"name": "contains_null", "type": "boolean", "field-id": 509},
        {"name": "contains_nan", "type": ["null", "boolean"], "default": null, "field-id": 518},
        {"name": "lower_bound", "type": ["null", "bytes"], "default": null, "field-id": 510},
        {"name": "upper_bound", "type": ["null", "bytes"], "default": null, "field-id": 511}
      ]
    }}], "default": null, "field-id": 507}
  ]
}"#;

/// The real Avro Iceberg manifest artifacts for one committed snapshot
/// (CONCEPT:EG-333/EG-334). The caller persists both byte blobs to the object store at
/// their respective paths (already the paths `metadata.json` references).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergManifests {
    /// Object-store path of the manifest Avro file (matches the manifest-list entry).
    pub manifest_path: String,
    /// Spec-compliant Avro bytes of the manifest file (one entry per live data file).
    pub manifest_avro: Vec<u8>,
    /// Object-store path of the manifest-list Avro file (matches `metadata.json`'s
    /// snapshot `manifest-list`).
    pub manifest_list_path: String,
    /// Spec-compliant Avro bytes of the manifest list (one `manifest_file` record).
    pub manifest_list_avro: Vec<u8>,
    /// The Iceberg snapshot id these manifests describe (derived from the LSN).
    pub snapshot_id: i64,
    /// Number of ADDED data files listed.
    pub added_files: usize,
    /// Total record count across the listed data files.
    pub added_rows: u64,
}

/// Build one `manifest_entry` Avro value for a live data file (CONCEPT:EG-333). A
/// newly-added file leaves `sequence_number` / `file_sequence_number` null so the reader
/// inherits the manifest's sequence number, per the v2 inheritance rule.
fn manifest_entry_value(location: &str, f: &FileEntry, snapshot_id: i64) -> AvroValue {
    let data_file = AvroValue::Record(vec![
        ("content".into(), AvroValue::Int(0)), // 0 = DATA
        (
            "file_path".into(),
            AvroValue::String(format!("{location}/{}", f.path)),
        ),
        ("file_format".into(), AvroValue::String("PARQUET".into())),
        ("partition".into(), AvroValue::Record(vec![])), // unpartitioned spec
        ("record_count".into(), AvroValue::Long(f.num_rows as i64)),
        (
            "file_size_in_bytes".into(),
            AvroValue::Long(f.size_bytes as i64),
        ),
    ]);
    AvroValue::Record(vec![
        ("status".into(), AvroValue::Int(1)), // 1 = ADDED
        (
            "snapshot_id".into(),
            AvroValue::Union(1, Box::new(AvroValue::Long(snapshot_id))),
        ),
        // null → inherit the manifest sequence number (v2 rule for ADDED files).
        (
            "sequence_number".into(),
            AvroValue::Union(0, Box::new(AvroValue::Null)),
        ),
        (
            "file_sequence_number".into(),
            AvroValue::Union(0, Box::new(AvroValue::Null)),
        ),
        ("data_file".into(), data_file),
    ])
}

/// Write the Avro **manifest** file listing every live data file (CONCEPT:EG-333),
/// with the required Iceberg manifest metadata in the container header.
fn write_manifest(
    schema: &LakeSchema,
    location: &str,
    live: &[&FileEntry],
    snapshot_id: i64,
) -> Result<Vec<u8>, String> {
    let avro_schema =
        Schema::parse_str(MANIFEST_ENTRY_SCHEMA).map_err(|e| format!("manifest schema: {e}"))?;
    let mut writer = Writer::new(&avro_schema, Vec::new());

    // Required Iceberg manifest metadata (lands in the Avro file header). A reader uses
    // these to bind field-ids and inherit sequence numbers.
    let ib_schema = iceberg_schema(schema).to_string();
    let meta: [(&str, &str); 6] = [
        ("schema", ib_schema.as_str()),
        ("schema-id", "0"),
        ("partition-spec", "[]"),
        ("partition-spec-id", "0"),
        ("format-version", "2"),
        ("content", "data"),
    ];
    for (k, v) in meta {
        writer
            .add_user_metadata(k.to_string(), v)
            .map_err(|e| format!("manifest metadata {k}: {e}"))?;
    }

    for f in live {
        writer
            .append(manifest_entry_value(location, f, snapshot_id))
            .map_err(|e| format!("append manifest_entry: {e}"))?;
    }
    writer
        .into_inner()
        .map_err(|e| format!("manifest avro: {e}"))
}

/// Write the Avro **manifest list** with one `manifest_file` record pointing at the
/// manifest just written (CONCEPT:EG-334).
fn write_manifest_list(
    manifest_path: &str,
    manifest_len: i64,
    snapshot_id: i64,
    added_files: usize,
    added_rows: u64,
) -> Result<Vec<u8>, String> {
    let avro_schema = Schema::parse_str(MANIFEST_FILE_SCHEMA)
        .map_err(|e| format!("manifest_file schema: {e}"))?;
    let mut writer = Writer::new(&avro_schema, Vec::new());

    let record = AvroValue::Record(vec![
        (
            "manifest_path".into(),
            AvroValue::String(manifest_path.to_string()),
        ),
        ("manifest_length".into(), AvroValue::Long(manifest_len)),
        ("partition_spec_id".into(), AvroValue::Int(0)),
        ("content".into(), AvroValue::Int(0)), // 0 = data manifest
        ("sequence_number".into(), AvroValue::Long(snapshot_id)),
        ("min_sequence_number".into(), AvroValue::Long(snapshot_id)),
        ("added_snapshot_id".into(), AvroValue::Long(snapshot_id)),
        (
            "added_files_count".into(),
            AvroValue::Int(added_files as i32),
        ),
        ("existing_files_count".into(), AvroValue::Int(0)),
        ("deleted_files_count".into(), AvroValue::Int(0)),
        (
            "added_rows_count".into(),
            AvroValue::Long(added_rows as i64),
        ),
        ("existing_rows_count".into(), AvroValue::Long(0)),
        ("deleted_rows_count".into(), AvroValue::Long(0)),
        // Unpartitioned spec → no per-partition field summaries.
        (
            "partitions".into(),
            AvroValue::Union(0, Box::new(AvroValue::Null)),
        ),
    ]);
    writer
        .append(record)
        .map_err(|e| format!("append manifest_file: {e}"))?;
    writer
        .into_inner()
        .map_err(|e| format!("manifest_list avro: {e}"))
}

/// Materialize the real Iceberg Avro manifest + manifest list for the current snapshot
/// (CONCEPT:EG-333/EG-334). The paths match exactly what [`crate::iceberg::build_iceberg`]
/// references from `metadata.json`, so a committed snapshot resolves to real Avro. The
/// caller persists both byte blobs to the object store.
pub fn build_iceberg_manifests(
    schema: &LakeSchema,
    snapshot: &SnapshotLog,
    location: &str,
) -> Result<IcebergManifests, String> {
    let lsn: Lsn = snapshot.current_lsn();
    let snapshot_id: i64 = lsn.value() as i64;
    let live = snapshot.live_files();
    let added_files = live.len();
    let added_rows: u64 = live.iter().map(|f| f.num_rows).sum();

    let manifest_path = manifest_file_path(location, snapshot_id);
    let manifest_list_path = manifest_list_path(location, snapshot_id);

    let manifest_avro = write_manifest(schema, location, &live, snapshot_id)?;
    let manifest_list_avro = write_manifest_list(
        &manifest_path,
        manifest_avro.len() as i64,
        snapshot_id,
        added_files,
        added_rows,
    )?;

    Ok(IcebergManifests {
        manifest_path,
        manifest_avro,
        manifest_list_path,
        manifest_list_avro,
        snapshot_id,
        added_files,
        added_rows,
    })
}

/// Parse an Avro container's records back to values (CONCEPT:EG-333) — used by the
/// round-trip test and any reader that wants to verify manifest bytes.
pub fn read_avro_records(bytes: &[u8]) -> Result<Vec<AvroValue>, String> {
    let reader = Reader::new(bytes).map_err(|e| format!("avro open: {e}"))?;
    let mut out = Vec::new();
    for rec in reader {
        out.push(rec.map_err(|e| format!("avro record: {e}"))?);
    }
    Ok(out)
}
