//! Real, spec-compliant Iceberg v2 **Avro** manifest + manifest-list writer
//! (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest manifest / CONCEPT:EG-KG.storage.iceberg-manifest-list manifest-list).
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
//! * **Per-column stats (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries):** the `data_file` now carries the Iceberg
//!   stats maps — `column_sizes` (108), `value_counts` (109), `null_value_counts`
//!   (110), `nan_value_counts` (137), `lower_bounds` (125) and `upper_bounds` (128),
//!   each an Avro `logicalType: map` (array of key/value records) keyed by the column's
//!   Iceberg field-id. Bounds are the column min/max serialized to Iceberg single-value
//!   **binary** (little-endian scalars; raw UTF-8 for strings). Gathered as the file is
//!   materialized ([`crate::parquet_io::materialize_with_column_stats`]) and carried on
//!   the [`FileEntry`]; a reader (Spark/Trino) uses the bounds to **skip whole files**.
//!   A file recorded without stats (bare `record_file`) emits the maps as null.
//! * **Partition `field_summary`:** eg-lake materializes an UNPARTITIONED spec
//!   (`partition-spec` = `[]`), so the `manifest_file.partitions` list has ZERO
//!   entries (one `field_summary` per partition-spec field, of which there are none) —
//!   it is correctly emitted as null, NOT stubbed. Data-file lower/upper bounds are the
//!   file-skipping mechanism that actually applies to an unpartitioned table.
//! * Container codec is Avro **null** (uncompressed) — universally readable.

use apache_avro::types::Value as AvroValue;
use apache_avro::{Reader, Schema, Writer};

use crate::iceberg::{iceberg_schema, manifest_file_path, manifest_list_path};
use crate::schema::{CellValue, LakeSchema};
use crate::snapshot::{ColumnStat, FileEntry, Lsn, SnapshotLog};

/// Iceberg v2 `manifest_entry` Avro schema (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest + per-column stats EG-350).
/// Field-ids per the spec; the `data_file` carries the unpartitioned projection eg-lake
/// tracks PLUS the six Iceberg stats maps (`logicalType: map`, keyed by column field-id)
/// that drive external predicate pushdown / file skipping.
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
        {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
        {"name": "column_sizes", "type": ["null", {"type": "array", "logicalType": "map", "items": {
          "type": "record", "name": "k117_v118", "fields": [
            {"name": "key", "type": "int", "field-id": 117},
            {"name": "value", "type": "long", "field-id": 118}
          ]}}], "default": null, "field-id": 108},
        {"name": "value_counts", "type": ["null", {"type": "array", "logicalType": "map", "items": {
          "type": "record", "name": "k119_v120", "fields": [
            {"name": "key", "type": "int", "field-id": 119},
            {"name": "value", "type": "long", "field-id": 120}
          ]}}], "default": null, "field-id": 109},
        {"name": "null_value_counts", "type": ["null", {"type": "array", "logicalType": "map", "items": {
          "type": "record", "name": "k121_v122", "fields": [
            {"name": "key", "type": "int", "field-id": 121},
            {"name": "value", "type": "long", "field-id": 122}
          ]}}], "default": null, "field-id": 110},
        {"name": "nan_value_counts", "type": ["null", {"type": "array", "logicalType": "map", "items": {
          "type": "record", "name": "k138_v139", "fields": [
            {"name": "key", "type": "int", "field-id": 138},
            {"name": "value", "type": "long", "field-id": 139}
          ]}}], "default": null, "field-id": 137},
        {"name": "lower_bounds", "type": ["null", {"type": "array", "logicalType": "map", "items": {
          "type": "record", "name": "k126_v127", "fields": [
            {"name": "key", "type": "int", "field-id": 126},
            {"name": "value", "type": "bytes", "field-id": 127}
          ]}}], "default": null, "field-id": 125},
        {"name": "upper_bounds", "type": ["null", {"type": "array", "logicalType": "map", "items": {
          "type": "record", "name": "k129_v130", "fields": [
            {"name": "key", "type": "int", "field-id": 129},
            {"name": "value", "type": "bytes", "field-id": 130}
          ]}}], "default": null, "field-id": 128}
      ]
    }, "field-id": 2}
  ]
}"#;

/// Iceberg v2 `manifest_file` Avro schema for the manifest list (CONCEPT:EG-KG.storage.iceberg-manifest-list).
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
/// (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest/EG-334). The caller persists both byte blobs to the object store at
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

/// Serialize a scalar to Iceberg **single-value binary** (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries) — the encoding
/// `lower_bounds` / `upper_bounds` values carry. Per the Iceberg spec: `long` / `timestamptz`
/// as little-endian 8-byte, `double` as little-endian IEEE-754 8-byte, `boolean` as one
/// byte (`0x00`/`0x01`), `string` as raw UTF-8 (no length prefix). Bounds are emitted
/// untruncated, which is always spec-valid (truncation is only a size optimization).
fn iceberg_binary(v: &CellValue) -> Vec<u8> {
    match v {
        CellValue::Long(x) => x.to_le_bytes().to_vec(),
        CellValue::Timestamp(x) => x.to_le_bytes().to_vec(),
        CellValue::Double(x) => x.to_le_bytes().to_vec(),
        CellValue::Bool(b) => vec![u8::from(*b)],
        CellValue::String(s) => s.as_bytes().to_vec(),
        CellValue::Null => Vec::new(),
    }
}

/// One entry of an Iceberg `logicalType: map` field: an Avro `{key, value}` record with
/// an `int` key (the column field-id) and the given value (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries).
fn map_entry(field_id: i32, value: AvroValue) -> AvroValue {
    AvroValue::Record(vec![
        ("key".into(), AvroValue::Int(field_id)),
        ("value".into(), value),
    ])
}

/// Wrap a built map as the `["null", array]` union value the schema declares
/// (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries): union branch 1 (the array) when any entry exists, else branch 0
/// (null) so a reader sees the optional stat as genuinely absent.
fn map_field(entries: Vec<AvroValue>) -> AvroValue {
    if entries.is_empty() {
        AvroValue::Union(0, Box::new(AvroValue::Null))
    } else {
        AvroValue::Union(1, Box::new(AvroValue::Array(entries)))
    }
}

/// Build the six Iceberg stats-map field values for a `data_file` record from a file's
/// per-column stats (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries). Emits `column_sizes` / `value_counts` /
/// `null_value_counts` / `nan_value_counts` (int→long maps) and `lower_bounds` /
/// `upper_bounds` (int→bytes maps), each keyed by the column's Iceberg field-id. A file
/// with no gathered stats yields six null unions.
fn stats_fields(stats: Option<&Vec<ColumnStat>>) -> [(String, AvroValue); 6] {
    let (mut sizes, mut values, mut nulls, mut nans, mut lowers, mut uppers) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    if let Some(cols) = stats {
        for c in cols {
            if let Some(sz) = c.column_size {
                sizes.push(map_entry(c.field_id, AvroValue::Long(sz)));
            }
            values.push(map_entry(c.field_id, AvroValue::Long(c.value_count)));
            nulls.push(map_entry(c.field_id, AvroValue::Long(c.null_count)));
            nans.push(map_entry(c.field_id, AvroValue::Long(c.nan_count)));
            if let Some(lo) = &c.lower {
                lowers.push(map_entry(c.field_id, AvroValue::Bytes(iceberg_binary(lo))));
            }
            if let Some(hi) = &c.upper {
                uppers.push(map_entry(c.field_id, AvroValue::Bytes(iceberg_binary(hi))));
            }
        }
    }
    [
        ("column_sizes".into(), map_field(sizes)),
        ("value_counts".into(), map_field(values)),
        ("null_value_counts".into(), map_field(nulls)),
        ("nan_value_counts".into(), map_field(nans)),
        ("lower_bounds".into(), map_field(lowers)),
        ("upper_bounds".into(), map_field(uppers)),
    ]
}

/// Build one `manifest_entry` Avro value for a live data file (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest). A
/// newly-added file leaves `sequence_number` / `file_sequence_number` null so the reader
/// inherits the manifest's sequence number, per the v2 inheritance rule. The `data_file`
/// carries the per-column Iceberg stats maps (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries) when they were gathered.
fn manifest_entry_value(location: &str, f: &FileEntry, snapshot_id: i64) -> AvroValue {
    let mut data_file = vec![
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
    ];
    data_file.extend(stats_fields(f.column_stats.as_ref()));
    let data_file = AvroValue::Record(data_file);
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

/// Write the Avro **manifest** file listing every live data file (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest),
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
/// manifest just written (CONCEPT:EG-KG.storage.iceberg-manifest-list).
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
        // Unpartitioned spec → the `partitions` list has one `field_summary` per
        // partition-spec field, of which there are none, so it is correctly null (NOT
        // stubbed) (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries). Data-file lower/upper bounds (EG-350) are the
        // file-skipping mechanism that applies to an unpartitioned table.
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
/// (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest/EG-334). The paths match exactly what [`crate::iceberg::build_iceberg`]
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

/// Parse an Avro container's records back to values (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest) — used by the
/// round-trip test and any reader that wants to verify manifest bytes.
pub fn read_avro_records(bytes: &[u8]) -> Result<Vec<AvroValue>, String> {
    let reader = Reader::new(bytes).map_err(|e| format!("avro open: {e}"))?;
    let mut out = Vec::new();
    for rec in reader {
        out.push(rec.map_err(|e| format!("avro record: {e}"))?);
    }
    Ok(out)
}
