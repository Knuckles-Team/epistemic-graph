//! Minimal Delta Lake `_delta_log` writer (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
//!
//! Delta Lake's transaction log is a directory `_delta_log/` of zero-padded,
//! newline-delimited-JSON commit files (`00000000000000000000.json`, `...001.json`,
//! …). Each line is one *action* object. A reader replays the actions in version
//! order to reconstruct the live set of Parquet files. Because it is pure JSON — no
//! Avro, unlike Iceberg manifests — this is the format `eg-lake` implements FULLY and
//! read-consistently: an external engine (Spark/Trino/DuckDB with delta support) can
//! open the table directly.
//!
//! This is a WRITE-ONLY, read-consistent snapshot export: it emits `protocol` +
//! `metaData` + `add`/`remove` actions describing the Parquet files a
//! [`SnapshotLog`](crate::snapshot::SnapshotLog) already recorded, mapping each
//! distinct engine LSN to one Delta version. It does NOT implement Delta's
//! optimistic-concurrency writer, checkpoints, deletion vectors, column mapping or
//! partition pruning — those are documented follow-ups; the emitted log is a valid
//! read target for the snapshot as-of an LSN.

use serde_json::{json, Value};

use crate::schema::LakeSchema;
use crate::snapshot::{Lsn, SnapshotLog};

/// The Delta protocol versions we write to (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). Reader v1 / writer v2 is
/// the classic, widely-supported baseline (no deletion vectors / column mapping),
/// which every Delta reader understands.
const MIN_READER_VERSION: i64 = 1;
const MIN_WRITER_VERSION: i64 = 2;

/// One emitted `_delta_log` file: its object-store-relative path and its
/// newline-delimited-JSON content (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
#[derive(Clone, Debug, PartialEq)]
pub struct DeltaLogFile {
    /// e.g. `_delta_log/00000000000000000000.json`.
    pub path: String,
    /// Newline-delimited JSON: one action object per line.
    pub content: String,
}

/// Encode a [`LakeSchema`] as Delta's `schemaString` (a JSON-encoded struct)
/// (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
fn delta_schema_string(schema: &LakeSchema) -> String {
    let fields: Vec<Value> = schema
        .fields
        .iter()
        .map(|f| {
            json!({
                "name": f.name,
                "type": f.ty.delta_type_name(),
                "nullable": f.nullable,
                "metadata": {},
            })
        })
        .collect();
    let struct_json = json!({ "type": "struct", "fields": fields });
    struct_json.to_string()
}

/// The zero-padded Delta log filename for a version (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
fn version_filename(version: u64) -> String {
    format!("_delta_log/{version:020}.json")
}

/// Build the full `_delta_log` for a table from its schema and the durable
/// [`SnapshotLog`] (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
///
/// Each distinct LSN that added or removed a file becomes one contiguous Delta version
/// (0, 1, 2, …), in LSN order — so the log an external reader replays reproduces the
/// engine's as-of history. Version 0 additionally carries the `protocol` + `metaData`
/// actions. `table_id` is the stable Delta table UUID; `created_time_ms` stamps
/// `metaData`/`add` deterministically (pass a real epoch-millis in production).
pub fn build_delta_log(
    schema: &LakeSchema,
    snapshot: &SnapshotLog,
    table_id: &str,
    created_time_ms: i64,
) -> Vec<DeltaLogFile> {
    // Collect the distinct commit LSNs (both adds and removes) in sorted order; their
    // position is the contiguous Delta version number.
    let mut lsns: Vec<Lsn> = Vec::new();
    for f in snapshot.all_files() {
        lsns.push(f.added_at);
        if let Some(r) = f.removed_at {
            lsns.push(r);
        }
    }
    lsns.sort();
    lsns.dedup();

    let mut out = Vec::with_capacity(lsns.len());
    for (version, lsn) in lsns.iter().enumerate() {
        let mut lines: Vec<String> = Vec::new();

        // commitInfo (informational, first line by convention).
        lines.push(
            json!({
                "commitInfo": {
                    "timestamp": created_time_ms,
                    "operation": if version == 0 { "CREATE TABLE" } else { "WRITE" },
                    "operationParameters": {},
                    "engineInfo": "epistemic-graph/eg-lake CONCEPT:EG-KG.storage.lsn-as-snapshot-returns",
                    // Expose the engine LSN so a reader can correlate a Delta version
                    // with our as-of snapshot version.
                    "readVersion": version.saturating_sub(1),
                    "epistemicGraphLsn": lsn.value(),
                }
            })
            .to_string(),
        );

        // Version 0 declares the protocol + table metadata.
        if version == 0 {
            lines.push(
                json!({
                    "protocol": {
                        "minReaderVersion": MIN_READER_VERSION,
                        "minWriterVersion": MIN_WRITER_VERSION,
                    }
                })
                .to_string(),
            );
            lines.push(
                json!({
                    "metaData": {
                        "id": table_id,
                        "format": { "provider": "parquet", "options": {} },
                        "schemaString": delta_schema_string(schema),
                        "partitionColumns": [],
                        "configuration": {},
                        "createdTime": created_time_ms,
                    }
                })
                .to_string(),
            );
        }

        // `add` actions for files introduced at this LSN.
        for f in snapshot.all_files().iter().filter(|f| f.added_at == *lsn) {
            let stats = json!({ "numRecords": f.num_rows }).to_string();
            lines.push(
                json!({
                    "add": {
                        "path": f.path,
                        "partitionValues": {},
                        "size": f.size_bytes,
                        "modificationTime": created_time_ms,
                        "dataChange": true,
                        "stats": stats,
                    }
                })
                .to_string(),
            );
        }

        // `remove` (tombstone) actions for files retired at this LSN.
        for f in snapshot
            .all_files()
            .iter()
            .filter(|f| f.removed_at == Some(*lsn))
        {
            lines.push(
                json!({
                    "remove": {
                        "path": f.path,
                        "deletionTimestamp": created_time_ms,
                        "dataChange": true,
                        "size": f.size_bytes,
                    }
                })
                .to_string(),
            );
        }

        out.push(DeltaLogFile {
            path: version_filename(version as u64),
            content: lines.join("\n"),
        });
    }

    out
}

/// Parse a `_delta_log` commit file's newline-delimited JSON back into action values
/// (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). Used by the round-trip test and any consumer replaying the log.
pub fn parse_delta_actions(content: &str) -> Result<Vec<Value>, String> {
    content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<Value>(l).map_err(|e| format!("delta action json: {e}")))
        .collect()
}

/// Replay a set of `_delta_log` files (in version order) to the live set of Parquet
/// file paths (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns) — the reconstruction an external Delta reader performs:
/// every `add` path minus every `remove` path.
pub fn live_paths(files: &[DeltaLogFile]) -> Result<Vec<String>, String> {
    let mut live: Vec<String> = Vec::new();
    for file in files {
        for action in parse_delta_actions(&file.content)? {
            if let Some(add) = action.get("add").and_then(|a| a.get("path")) {
                if let Some(p) = add.as_str() {
                    live.push(p.to_string());
                }
            }
            if let Some(rem) = action.get("remove").and_then(|a| a.get("path")) {
                if let Some(p) = rem.as_str() {
                    live.retain(|x| x != p);
                }
            }
        }
    }
    Ok(live)
}
