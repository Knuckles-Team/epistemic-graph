//! Parquet columnar log segments on the blob CAS (CONCEPT:EG-KG.retrieval.observability-search) — the
//! cheap-object-store substrate under the observability ingest front door
//! (CONCEPT:AU-KG.ingest.self-ingest).
//!
//! ## The storage thesis (surpass OpenObserve)
//!
//! OpenObserve's "140× cheaper storage" architecture is: buffer ingested log
//! records, roll them into **Parquet columnar segments**, and land those on an
//! object store (S3/MinIO) with a small manifest so a query can prune by
//! time/stream BEFORE scanning. The engine already has every piece:
//!
//!  * Arrow `RecordBatch` (the same `(ts, …)` shape [`eg_tsdb::arrow_seg`] hands
//!    DataFusion, CONCEPT:EG-KG.temporal.columnar-schema-inference), and
//!  * a content-addressed blob CAS ([`crate::server::blob`], CONCEPT:EG-KG.storage.blob-namespace)
//!    that, with `blob-s3` on, lands chunks on S3.
//!
//! So a log segment is: `Vec<LogRecord>` → typed column frame → Parquet bytes →
//! stored in the CAS (one content-addressed blob) → a [`SegmentManifest`]
//! recording `(stream, [min_ts, max_ts], row_count, schema)`. The manifest is the
//! prune index: a later time/stream-bounded search (EG-162/164, a follow-up Phase T
//! item) reads only the segments whose `[min_ts, max_ts]` overlaps the query window.
//!
//! ## Schema (schema-on-read)
//!
//! Five columns: `ts: Int64` (epoch-ns), `stream: Utf8`, `severity: Utf8`,
//! `body: Utf8`, and `attrs: Utf8` — the dynamic attribute map serialized as a JSON
//! object so the columnar segment stays a FIXED schema while the log attributes stay
//! schema-on-read (parsed out of the `attrs` JSON at query time). This is the
//! tractable first cut EG-161 calls for; a per-attribute column promotion is a later
//! Phase T refinement.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use polars_core::prelude::{Column, DataFrame};
use polars_io::prelude::{ParquetCompression, ParquetReader, ParquetWriter, SerReader};
use serde::{Deserialize, Serialize};

use crate::server::blob::store::{hex_digest, BlobManifest, ChunkStore, DEFAULT_CHUNK_SIZE};

use super::LogRecord;

/// The prune index for one flushed Parquet log segment (CONCEPT:EG-KG.retrieval.observability-search). Records
/// where the segment's bytes live in the CAS (`blob_digest`) plus the coordinates a
/// query prunes on WITHOUT opening the Parquet: the stream, the closed time range
/// `[min_ts, max_ts]` (epoch-ns), the row count and the column schema.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentManifest {
    /// The org/stream namespace this segment belongs to.
    pub stream: String,
    /// Content address of the Parquet bytes in the blob CAS (its blob digest).
    pub blob_digest: String,
    /// Smallest record timestamp in the segment (epoch-ns) — the prune lower bound.
    pub min_ts: i64,
    /// Largest record timestamp in the segment (epoch-ns) — the prune upper bound.
    pub max_ts: i64,
    /// Number of log records (Parquet rows) in the segment.
    pub row_count: usize,
    /// Ordered column names of the Parquet schema (schema-on-read record of shape).
    pub schema_fields: Vec<String>,
    /// Total Parquet byte length (observability / object-store cost accounting).
    pub bytes_len: u64,
}

/// The FIXED Arrow schema of a log segment (attrs collapsed to a JSON `Utf8` column).
pub fn log_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("stream", DataType::Utf8, false),
        Field::new("severity", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
        Field::new("attrs", DataType::Utf8, false),
    ]))
}

/// The column names of [`log_arrow_schema`], in order.
pub fn schema_field_names() -> Vec<String> {
    log_arrow_schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// Serialize a record's dynamic attribute map to a stable JSON object string.
fn attrs_json(rec: &LogRecord) -> String {
    let map: serde_json::Map<String, serde_json::Value> = rec
        .attrs
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    serde_json::Value::Object(map).to_string()
}

/// Build an Arrow `RecordBatch` (the [`log_arrow_schema`]) from log records. Public
/// to the obs module so the search surface (CONCEPT:EG-KG.query.concept-4) can register the scanned
/// hot + cold records as a DataFusion `logs` table.
pub(super) fn records_to_batch(records: &[LogRecord]) -> Result<RecordBatch, String> {
    let ts: Int64Array = records.iter().map(|r| Some(r.ts)).collect();
    let stream: StringArray = records.iter().map(|r| Some(r.stream.as_str())).collect();
    let severity: StringArray = records.iter().map(|r| Some(r.severity.as_str())).collect();
    let body: StringArray = records.iter().map(|r| Some(r.body.as_str())).collect();
    let attrs: StringArray = records.iter().map(|r| Some(attrs_json(r))).collect();
    RecordBatch::try_new(
        log_arrow_schema(),
        vec![
            Arc::new(ts),
            Arc::new(stream),
            Arc::new(severity),
            Arc::new(body),
            Arc::new(attrs),
        ],
    )
    .map_err(|e| format!("build log RecordBatch: {e}"))
}

/// Encode log records into Parquet bytes (Arrow → Parquet, deliberately
/// uncompressed to keep segment CPU predictable). Returns the serialized blob.
pub fn records_to_parquet(records: &[LogRecord]) -> Result<Vec<u8>, String> {
    let attrs_values: Vec<String> = records.iter().map(attrs_json).collect();
    let mut frame = DataFrame::new(
        records.len(),
        vec![
            Column::new(
                "ts".into(),
                records.iter().map(|record| record.ts).collect::<Vec<_>>(),
            ),
            Column::new(
                "stream".into(),
                records
                    .iter()
                    .map(|record| record.stream.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "severity".into(),
                records
                    .iter()
                    .map(|record| record.severity.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "body".into(),
                records
                    .iter()
                    .map(|record| record.body.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "attrs".into(),
                attrs_values.iter().map(String::as_str).collect::<Vec<_>>(),
            ),
        ],
    )
    .map_err(|e| format!("build parquet frame: {e}"))?;
    let mut buf: Vec<u8> = Vec::new();
    ParquetWriter::new(&mut buf)
        .with_compression(ParquetCompression::Uncompressed)
        .set_parallel(false)
        .finish(&mut frame)
        .map_err(|e| format!("write parquet frame: {e}"))?;
    Ok(buf)
}

/// Decode Parquet segment bytes back into log records (the round-trip read).
pub fn parquet_to_records(bytes: &[u8]) -> Result<Vec<LogRecord>, String> {
    let frame = ParquetReader::new(std::io::Cursor::new(bytes))
        .finish()
        .map_err(|e| format!("read parquet frame: {e}"))?;
    let ts = frame
        .column("ts")
        .and_then(Column::i64)
        .map_err(|e| format!("column ts: {e}"))?;
    let stream = frame
        .column("stream")
        .and_then(Column::str)
        .map_err(|e| format!("column stream: {e}"))?;
    let severity = frame
        .column("severity")
        .and_then(Column::str)
        .map_err(|e| format!("column severity: {e}"))?;
    let body = frame
        .column("body")
        .and_then(Column::str)
        .map_err(|e| format!("column body: {e}"))?;
    let attrs = frame
        .column("attrs")
        .and_then(Column::str)
        .map_err(|e| format!("column attrs: {e}"))?;

    let mut out = Vec::with_capacity(frame.height());
    for row in 0..frame.height() {
        let attr_json = attrs
            .get(row)
            .ok_or_else(|| format!("null attrs at row {row}"))?;
        out.push(LogRecord {
            ts: ts.get(row).ok_or_else(|| format!("null ts at row {row}"))?,
            stream: stream
                .get(row)
                .ok_or_else(|| format!("null stream at row {row}"))?
                .to_string(),
            severity: severity
                .get(row)
                .ok_or_else(|| format!("null severity at row {row}"))?
                .to_string(),
            body: body
                .get(row)
                .ok_or_else(|| format!("null body at row {row}"))?
                .to_string(),
            attrs: parse_attrs_json(attr_json),
        });
    }
    Ok(out)
}

/// Parse the `attrs` JSON-object column back into the dynamic attribute map.
fn parse_attrs_json(s: &str) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    if let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(s) {
        for (k, v) in map {
            let sv = match v {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            out.insert(k, sv);
        }
    }
    out
}

/// Store Parquet segment bytes in the blob CAS as one content-addressed blob and
/// return its blob digest. Chunks the bytes through the streaming CAS
/// (bounded-memory, dedup) exactly as a media upload does, then commits the
/// manifest and `incref`s it so the segment is retained (a sweep never reclaims a
/// referenced blob).
pub fn store_segment_bytes(store: &dyn ChunkStore, bytes: &[u8]) -> Result<String, String> {
    let mut chunks = Vec::new();
    let mut chunk_lens = Vec::new();
    for part in bytes.chunks(DEFAULT_CHUNK_SIZE) {
        let (digest, _was_new) = store.put_chunk(part)?;
        chunks.push(digest);
        chunk_lens.push(part.len() as u32);
    }
    let manifest = BlobManifest {
        schema_version: crate::server::blob::BLOB_MANIFEST_VERSION,
        owner_scope: crate::server::blob::ENGINE_BLOB_OWNER_SCOPE.to_string(),
        chunks,
        chunk_lens,
        len: bytes.len() as u64,
        // Native content-defined boundaries are recorded explicitly in chunk_lens.
        chunk_size: 0,
    };
    let mbytes = rmp_serde::to_vec_named(&manifest).map_err(|e| e.to_string())?;
    let blob_digest = hex_digest(&mbytes);
    store.put_manifest(&blob_digest, &manifest)?;
    store.incref(&blob_digest)?;
    Ok(blob_digest)
}

/// Read Parquet segment bytes back out of the blob CAS by blob digest.
pub fn read_segment_bytes(store: &dyn ChunkStore, blob_digest: &str) -> Result<Vec<u8>, String> {
    let manifest = store
        .get_manifest(blob_digest)?
        .ok_or_else(|| format!("segment manifest {blob_digest} missing"))?;
    let mut out = Vec::with_capacity(manifest.len as usize);
    for c in &manifest.chunks {
        let chunk = store
            .get_chunk(c)?
            .ok_or_else(|| format!("segment chunk {c} missing"))?;
        out.extend(chunk);
    }
    Ok(out)
}

/// Roll a buffer of log records into a Parquet segment: encode → store in the CAS →
/// return the [`SegmentManifest`] (the prune index). `stream` labels the manifest.
/// Empty input yields `None` (nothing to flush).
pub fn flush_records_to_segment(
    store: &dyn ChunkStore,
    stream: &str,
    records: &[LogRecord],
) -> Result<Option<SegmentManifest>, String> {
    if records.is_empty() {
        return Ok(None);
    }
    let bytes = records_to_parquet(records)?;
    let blob_digest = store_segment_bytes(store, &bytes)?;
    let min_ts = records.iter().map(|r| r.ts).min().unwrap_or(0);
    let max_ts = records.iter().map(|r| r.ts).max().unwrap_or(0);
    Ok(Some(SegmentManifest {
        stream: stream.to_string(),
        blob_digest,
        min_ts,
        max_ts,
        row_count: records.len(),
        schema_fields: schema_field_names(),
        bytes_len: bytes.len() as u64,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::blob::store::RedbChunkStore;
    use std::collections::BTreeMap;

    fn rec(ts: i64, stream: &str, sev: &str, body: &str, k: &str, v: &str) -> LogRecord {
        let mut attrs = BTreeMap::new();
        attrs.insert(k.to_string(), v.to_string());
        LogRecord {
            ts,
            stream: stream.to_string(),
            severity: sev.to_string(),
            body: body.to_string(),
            attrs,
        }
    }

    #[test]
    fn parquet_round_trips_records() {
        let records = vec![
            rec(100, "app", "INFO", "started up", "host", "a1"),
            rec(200, "app", "ERROR", "disk full", "host", "a2"),
        ];
        let bytes = records_to_parquet(&records).unwrap();
        let back = parquet_to_records(&bytes).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].ts, 100);
        assert_eq!(back[1].severity, "ERROR");
        assert_eq!(back[1].body, "disk full");
        assert_eq!(back[0].attrs.get("host").unwrap(), "a1");
    }

    #[test]
    fn segment_flush_stores_and_reads_back_with_manifest() {
        let store = RedbChunkStore::open_temp().unwrap();
        let records = vec![
            rec(500, "web", "INFO", "GET /", "code", "200"),
            rec(900, "web", "WARN", "slow query", "code", "200"),
            rec(700, "web", "INFO", "GET /health", "code", "204"),
        ];
        // Flush → manifest records the prune coordinates without opening parquet.
        let manifest = flush_records_to_segment(&store, "web", &records)
            .unwrap()
            .expect("non-empty flush yields a segment");
        assert_eq!(manifest.stream, "web");
        assert_eq!(manifest.row_count, 3);
        assert_eq!(manifest.min_ts, 500);
        assert_eq!(manifest.max_ts, 900);
        assert_eq!(
            manifest.schema_fields,
            vec!["ts", "stream", "severity", "body", "attrs"]
        );
        assert!(manifest.bytes_len > 0);

        // Read the parquet bytes back out of the CAS by digest and decode them.
        let bytes = read_segment_bytes(&store, &manifest.blob_digest).unwrap();
        let back = parquet_to_records(&bytes).unwrap();
        assert_eq!(back.len(), 3);
        let bodies: Vec<&str> = back.iter().map(|r| r.body.as_str()).collect();
        assert!(bodies.contains(&"slow query"));
        assert!(bodies.contains(&"GET /health"));
    }

    #[test]
    fn empty_flush_is_none() {
        let store = RedbChunkStore::open_temp().unwrap();
        assert!(flush_records_to_segment(&store, "x", &[])
            .unwrap()
            .is_none());
    }
}
