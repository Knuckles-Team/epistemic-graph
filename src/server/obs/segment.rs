//! Parquet columnar log segments on the blob CAS (CONCEPT:EG-161) — the
//! cheap-object-store substrate under the observability ingest front door
//! (CONCEPT:EG-160).
//!
//! ## The storage thesis (surpass OpenObserve)
//!
//! OpenObserve's "140× cheaper storage" architecture is: buffer ingested log
//! records, roll them into **Parquet columnar segments**, and land those on an
//! object store (S3/MinIO) with a small manifest so a query can prune by
//! time/stream BEFORE scanning. The engine already has every piece:
//!
//!  * Arrow `RecordBatch` (the same `(ts, …)` shape [`eg_tsdb::arrow_seg`] hands
//!    DataFusion, CONCEPT:EG-089), and
//!  * a content-addressed blob CAS ([`crate::server::blob`], CONCEPT:KG-2.206)
//!    that, with `blob-s3` on, lands chunks on S3.
//!
//! So a log segment is: `Vec<LogRecord>` → Arrow `RecordBatch` → Parquet bytes →
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

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};

use crate::server::blob::store::{hex_digest, BlobManifest, ChunkStore, DEFAULT_CHUNK_SIZE};

use super::LogRecord;

/// The prune index for one flushed Parquet log segment (CONCEPT:EG-161). Records
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
/// to the obs module so the search surface (CONCEPT:EG-162) can register the scanned
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

/// Encode log records into Parquet bytes (Arrow → Parquet, UNCOMPRESSED — the
/// codec crates stay out of the tree). Returns the serialized segment blob.
pub fn records_to_parquet(records: &[LogRecord]) -> Result<Vec<u8>, String> {
    let batch = records_to_batch(records)?;
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, log_arrow_schema(), None)
            .map_err(|e| format!("open parquet writer: {e}"))?;
        writer
            .write(&batch)
            .map_err(|e| format!("write parquet batch: {e}"))?;
        writer
            .close()
            .map_err(|e| format!("close parquet writer: {e}"))?;
    }
    Ok(buf)
}

/// Decode Parquet segment bytes back into log records (the round-trip read).
pub fn parquet_to_records(bytes: &[u8]) -> Result<Vec<LogRecord>, String> {
    let data = bytes::Bytes::copy_from_slice(bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(data)
        .map_err(|e| format!("open parquet reader: {e}"))?;
    let reader = builder
        .build()
        .map_err(|e| format!("build parquet reader: {e}"))?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| format!("read parquet batch: {e}"))?;
        let col_i64 = |name: &str| -> Result<Int64Array, String> {
            let idx = batch
                .schema()
                .index_of(name)
                .map_err(|e| format!("column {name}: {e}"))?;
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .cloned()
                .ok_or_else(|| format!("column {name} not Int64"))
        };
        let col_str = |name: &str| -> Result<StringArray, String> {
            let idx = batch
                .schema()
                .index_of(name)
                .map_err(|e| format!("column {name}: {e}"))?;
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .cloned()
                .ok_or_else(|| format!("column {name} not Utf8"))
        };
        let ts = col_i64("ts")?;
        let stream = col_str("stream")?;
        let severity = col_str("severity")?;
        let body = col_str("body")?;
        let attrs = col_str("attrs")?;
        for i in 0..batch.num_rows() {
            let attr_map = parse_attrs_json(attrs.value(i));
            out.push(LogRecord {
                ts: ts.value(i),
                stream: stream.value(i).to_string(),
                severity: severity.value(i).to_string(),
                body: body.value(i).to_string(),
                attrs: attr_map,
            });
        }
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
        chunks,
        chunk_lens,
        len: bytes.len() as u64,
        // Content-defined-shaped manifest (variable chunk_lens recorded); 0 marks it
        // non-legacy per CONCEPT:EG-071.
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
