//! Observability log ingestion front door (CONCEPT:AU-KG.ingest.self-ingest) — the first slice of
//! Phase T ("surpass OpenObserve").
//!
//! ## Why this exists
//!
//! The engine already IS OpenObserve's stack: Rust + redb + Arrow/DataFusion + a
//! Tantivy full-text index (`eg-text`) + a content-addressed blob store on S3
//! (`blob-s3`). OpenObserve (O2) is: ingest logs/metrics/traces over standard
//! wire protocols → land them as time-series + full-text → roll them into
//! Parquet-on-object-store → search with SQL. This module lands the **log
//! ingestion front door**; [`segment`] lands the **Parquet segment substrate**
//! (CONCEPT:EG-KG.retrieval.observability-search). Search / PromQL / dashboards are later Phase T items.
//!
//! ## The listener
//!
//! A HAND-ROLLED HTTP/1.1 listener over `tokio::net::TcpListener` — the SAME
//! dependency-free idiom as [`crate::metrics::serve`] and
//! [`crate::server::sparql_http`] (NO axum/hyper/warp, so the Pi contract holds).
//! Bound via `EPISTEMIC_GRAPH_OBS_ADDR`. It accepts log records over three
//! interchange shapes so EXISTING agents/collectors point at it unchanged:
//!
//!  * `POST /v1/logs`            — OTLP/HTTP JSON (`ExportLogsServiceRequest`)
//!  * `POST /_bulk`              — Elasticsearch `_bulk` NDJSON (action/doc pairs)
//!  * `POST /<stream>/_doc`      — Elasticsearch single-doc index
//!  * `POST /` or `/api/logs`    — plain JSON-lines (one JSON object per line)
//!
//! Every shape is normalized into a common [`LogRecord`] and stored into (a) an
//! `eg-tsdb` series keyed by stream (time-range + retention) and (b) a per-stream
//! `eg-text` Tantivy index (full-text search) — schema-on-read: attributes are a
//! dynamic map, never a fixed column. Multi-tenant by **stream**: an org/stream
//! name gets its own tsdb series id + its own text-index namespace.

pub mod search;
pub mod segment;

/// CONCEPT:EG-OS.observability.prometheus-ingest — the observability EGRESS half: OTLP/HTTP-JSON export of the engine's
/// OWN Prometheus metrics + its stored distributed-trace spans to an external
/// OpenTelemetry collector (closing the loop the engine already ingests). Behind the
/// `otel-export` feature.
#[cfg(feature = "otel-export")]
pub mod otel_export;

/// CONCEPT:EG-OS.observability.prometheus-ingest — the Prometheus `remote_write` receiver: land snappy-compressed
/// protobuf `WriteRequest` POSTs (`/api/v1/write`) into the durable eg-tsdb SeriesStore
/// so an external Prometheus can PUSH to the engine. Behind the `otel-export` feature.
#[cfg(feature = "otel-export")]
pub mod remote_write;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::server::blob::store::{ChunkStore, RedbChunkStore};
use eg_text::TextIndex;
use eg_tsdb::point::Point;
use eg_tsdb::store::SeriesStore;

pub use search::{LogQuery, DEFAULT_SEARCH_SIZE};
pub use segment::SegmentManifest;

/// Env var carrying the observability-ingest listener bind address (`host:port`),
/// e.g. `127.0.0.1:5080` (O2's default log-ingest port). Unset ⇒ no listener.
pub const OBS_ADDR_ENV: &str = "EPISTEMIC_GRAPH_OBS_ADDR";
/// Env var overriding the per-stream buffered-record count that triggers a Parquet
/// segment flush (CONCEPT:EG-KG.retrieval.observability-search). Default [`DEFAULT_FLUSH_RECORDS`].
pub const OBS_FLUSH_RECORDS_ENV: &str = "EPISTEMIC_GRAPH_OBS_FLUSH_RECORDS";

/// Default flush window: buffer this many records per stream, then roll a segment.
pub const DEFAULT_FLUSH_RECORDS: usize = 1024;
/// TSDB time-partition width for a log series: 1 hour of wall-clock per chunk.
const SERIES_BUCKET_NS: u64 = 3_600_000_000_000;

/// One normalized log record — the common shape every wire format is parsed into.
/// `attrs` is a dynamic string map (schema-on-read); the fixed fields are the ones
/// every observability format carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    /// Timestamp, epoch nanoseconds (the tsdb series key).
    pub ts: i64,
    /// Org/stream namespace (the tenancy key → its own series + text namespace).
    pub stream: String,
    /// Severity text (e.g. `INFO`/`WARN`/`ERROR`); empty if the source omitted it.
    pub severity: String,
    /// The log message body.
    pub body: String,
    /// Dynamic attributes (schema-on-read), sorted for a stable serialization.
    pub attrs: BTreeMap<String, String>,
}

/// The ingest counts an ingest call landed, shaped into the wire response.
#[derive(Clone, Copy, Debug, Default)]
pub struct IngestOutcome {
    pub accepted: usize,
    pub segments_flushed: usize,
}

/// Wall-clock now in epoch nanoseconds.
fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Map a severity text onto a numeric level (OTLP-ish) for the tsdb series value, so
/// the series is a meaningful "severity over time" signal, not just a count.
fn severity_number(sev: &str) -> f64 {
    match sev.trim().to_ascii_uppercase().as_str() {
        "TRACE" => 1.0,
        "DEBUG" => 5.0,
        "INFO" | "INFORMATION" => 9.0,
        "WARN" | "WARNING" => 13.0,
        "ERROR" | "ERR" => 17.0,
        "FATAL" | "CRITICAL" | "CRIT" => 21.0,
        _ => 0.0,
    }
}

/// Sanitize a stream name into a filesystem-safe directory component (per-stream
/// text index dir). Non-alphanumerics collapse to `_`.
fn sanitize(stream: &str) -> String {
    stream
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// The self-contained ingest state: a tsdb series store + per-stream text indices +
/// the blob CAS for Parquet segments + per-stream flush buffers + the recorded
/// segment manifests. NOT tied to the graph `ServerState` — it is the observability
/// tier's own substrate.
pub struct ObsState {
    /// Time-series store (series id = `obs:logs:<stream>`).
    series: Arc<SeriesStore>,
    /// Blob CAS the Parquet segments land in (S3-backed when `blob-s3` is on).
    blob: Arc<dyn ChunkStore>,
    /// Per-stream Tantivy full-text indices, created lazily.
    indices: Mutex<HashMap<String, TextIndex>>,
    /// Per-stream buffers of records awaiting a Parquet flush.
    buffers: Mutex<HashMap<String, Vec<LogRecord>>>,
    /// Recorded segment manifests (the prune index), newest last.
    segments: Mutex<Vec<SegmentManifest>>,
    /// Base dir for persistent text indices; `None` ⇒ in-memory indices (tests).
    text_dir: Option<std::path::PathBuf>,
    /// Roll a segment once a stream buffers this many records.
    flush_threshold: usize,
    /// Monotonic doc-id sequence for text-index document ids.
    next_doc: AtomicU64,
    /// CONCEPT:EG-OS.observability.trace-assembly — the distributed-trace span store (in-memory; a durable tier
    /// mirroring logs is a follow-up). Present only under the `traces` sub-feature.
    #[cfg(feature = "traces")]
    traces: Arc<eg_tsdb::traces::SpanStore>,
}

impl ObsState {
    /// Open the ingest substrate under a persist dir (durable series + blob CAS +
    /// on-disk text indices under `{persist_dir}/obs/…`). With `persist_dir = None`
    /// everything is in a temp dir / in-memory (tests / ephemeral).
    pub fn open(persist_dir: Option<&str>, flush_threshold: usize) -> Result<Self, String> {
        let (series, blob, text_dir) = match persist_dir {
            Some(dir) => {
                let base = std::path::Path::new(dir).join("obs");
                let series = SeriesStore::open_in_dir(&base).map_err(|e| e.to_string())?;
                let blob = RedbChunkStore::open(&base.join("blob").to_string_lossy())?;
                (series, blob, Some(base.join("text")))
            }
            None => {
                let base = std::env::temp_dir().join(format!(
                    "eg-obs-{}-{}",
                    std::process::id(),
                    now_ns()
                ));
                let series = SeriesStore::open_in_dir(&base).map_err(|e| e.to_string())?;
                let blob = RedbChunkStore::open(&base.join("blob").to_string_lossy())?;
                (series, blob, None)
            }
        };
        Ok(Self {
            series: Arc::new(series),
            blob: Arc::new(blob),
            indices: Mutex::new(HashMap::new()),
            buffers: Mutex::new(HashMap::new()),
            segments: Mutex::new(Vec::new()),
            text_dir,
            flush_threshold: flush_threshold.max(1),
            next_doc: AtomicU64::new(1),
            #[cfg(feature = "traces")]
            traces: Arc::new(eg_tsdb::traces::SpanStore::new()),
        })
    }

    /// CONCEPT:EG-OS.observability.trace-assembly — the distributed-trace span store handle, used by the trace
    /// facade (`src/server/traces`) to ingest spans and serve trace search/assembly.
    #[cfg(feature = "traces")]
    pub fn trace_store(&self) -> Arc<eg_tsdb::traces::SpanStore> {
        self.traces.clone()
    }

    /// In-memory ingest state (temp series/blob, RAM text indices) — for tests.
    pub fn in_memory(flush_threshold: usize) -> Result<Self, String> {
        Self::open(None, flush_threshold)
    }

    /// Ingest a batch of normalized records: append each stream's points to its tsdb
    /// series, upsert each record into its stream's text index, buffer it for a
    /// Parquet flush, and roll a segment for any stream that crossed the threshold.
    pub fn ingest(&self, records: Vec<LogRecord>) -> Result<IngestOutcome, String> {
        if records.is_empty() {
            return Ok(IngestOutcome::default());
        }
        // Group by stream so each series append + index commit is one batch.
        let mut by_stream: HashMap<String, Vec<LogRecord>> = HashMap::new();
        for r in records {
            by_stream.entry(r.stream.clone()).or_default().push(r);
        }

        let mut accepted = 0usize;
        let mut segments_flushed = 0usize;
        for (stream, recs) in by_stream {
            accepted += recs.len();

            // (a) tsdb series — time-range + retention.
            let series_id = format!("obs:logs:{stream}");
            let points: Vec<Point> = recs
                .iter()
                .map(|r| Point {
                    ts: r.ts,
                    values: vec![severity_number(&r.severity)],
                })
                .collect();
            self.series
                .append_batch(
                    &series_id,
                    1,
                    SERIES_BUCKET_NS,
                    &["severity".to_string()],
                    &points,
                )
                .map_err(|e| e.to_string())?;

            // (b) full-text index — schema-on-read (attrs flattened into the text).
            self.with_index(&stream, |ix| {
                for r in &recs {
                    let seq = self.next_doc.fetch_add(1, Ordering::Relaxed);
                    let doc_id = format!("{stream}:{seq}");
                    ix.upsert(&doc_id, &text_body(r));
                }
                ix.commit().map_err(|e| e.to_string())
            })?;

            // (c) buffer for Parquet; flush a segment past the threshold.
            let drained = {
                let mut buffers = self.buffers.lock();
                let buf = buffers.entry(stream.clone()).or_default();
                buf.extend(recs);
                if buf.len() >= self.flush_threshold {
                    Some(std::mem::take(buf))
                } else {
                    None
                }
            };
            if let Some(batch) = drained {
                if let Some(manifest) =
                    segment::flush_records_to_segment(self.blob.as_ref(), &stream, &batch)?
                {
                    self.segments.lock().push(manifest);
                    segments_flushed += 1;
                }
            }
        }
        Ok(IngestOutcome {
            accepted,
            segments_flushed,
        })
    }

    /// Force-flush a stream's buffered records into a Parquet segment NOW (used at
    /// shutdown / tests / a small stream that never crosses the threshold). Returns
    /// the manifest, or `None` if the buffer was empty.
    pub fn flush_stream(&self, stream: &str) -> Result<Option<SegmentManifest>, String> {
        let batch = {
            let mut buffers = self.buffers.lock();
            match buffers.get_mut(stream) {
                Some(buf) if !buf.is_empty() => std::mem::take(buf),
                _ => return Ok(None),
            }
        };
        let manifest = segment::flush_records_to_segment(self.blob.as_ref(), stream, &batch)?;
        if let Some(m) = &manifest {
            self.segments.lock().push(m.clone());
        }
        Ok(manifest)
    }

    /// BM25 search a stream's text index — returns `(doc_id, score)` hits. The
    /// query-back surface tests + a future EG-162 search endpoint use.
    pub fn search(&self, stream: &str, query: &str, k: usize) -> Vec<eg_text::TextHit> {
        let mut indices = self.indices.lock();
        match indices.get_mut(stream) {
            Some(ix) => ix.search(query, k),
            None => Vec::new(),
        }
    }

    /// The durable time-series store handle — used by the PromQL facade
    /// (CONCEPT:EG-KG.query.prometheus-http-query-api) to resolve selectors / enumerate labels over the same series
    /// the log ingest lands in.
    pub fn series_store(&self) -> Arc<SeriesStore> {
        self.series.clone()
    }

    /// Read a stream's tsdb series points over `[from, to)` (epoch-ns) — proves the
    /// records landed in the time-series tier.
    pub fn series_range(&self, stream: &str, from: i64, to: i64) -> Result<Vec<Point>, String> {
        self.series
            .range(&format!("obs:logs:{stream}"), from, to)
            .map_err(|e| e.to_string())
    }

    /// Segment manifests recorded for a stream (the prune index).
    pub fn segments_for(&self, stream: &str) -> Vec<SegmentManifest> {
        self.segments
            .lock()
            .iter()
            .filter(|m| m.stream == stream)
            .cloned()
            .collect()
    }

    /// Read a flushed Parquet segment's records back out of the blob CAS.
    pub fn read_segment(&self, manifest: &SegmentManifest) -> Result<Vec<LogRecord>, String> {
        let bytes = segment::read_segment_bytes(self.blob.as_ref(), &manifest.blob_digest)?;
        segment::parquet_to_records(&bytes)
    }

    /// Run `f` with a mutable handle to `stream`'s text index, creating it lazily
    /// (on-disk under `text_dir` when persistent, else in-RAM).
    fn with_index<T>(
        &self,
        stream: &str,
        f: impl FnOnce(&mut TextIndex) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut indices = self.indices.lock();
        if !indices.contains_key(stream) {
            let ix = match &self.text_dir {
                Some(base) => {
                    let dir = base.join(sanitize(stream));
                    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
                    TextIndex::open(&dir).map_err(|e| e.to_string())?
                }
                None => TextIndex::in_memory().map_err(|e| e.to_string())?,
            };
            indices.insert(stream.to_string(), ix);
        }
        let ix = indices.get_mut(stream).expect("just inserted");
        f(ix)
    }
}

/// The searchable text for a record: the body plus its flattened attributes, so a
/// full-text query can match on either (schema-on-read).
fn text_body(r: &LogRecord) -> String {
    let mut s = r.body.clone();
    if !r.severity.is_empty() {
        s.push(' ');
        s.push_str(&r.severity);
    }
    for (k, v) in &r.attrs {
        s.push(' ');
        s.push_str(k);
        s.push('=');
        s.push_str(v);
    }
    s
}

// ── wire-format parsers ───────────────────────────────────────────────────────

/// Extract a timestamp (epoch-ns) from a generic doc, tolerating the common shapes:
/// OTLP `timeUnixNano` (ns string/number), `@timestamp`/`timestamp`/`time` as epoch
/// ms or an ISO-8601-ish number. Falls back to `now`.
fn extract_ts(doc: &serde_json::Value) -> i64 {
    // OTLP nano timestamp (string or number of nanoseconds).
    for key in ["timeUnixNano", "observedTimeUnixNano"] {
        if let Some(v) = doc.get(key) {
            if let Some(n) = v.as_str().and_then(|s| s.parse::<i64>().ok()) {
                return n;
            }
            if let Some(n) = v.as_i64() {
                return n;
            }
        }
    }
    // Epoch-millis style fields → ns. A plain integer is treated as milliseconds
    // (the Elastic/O2 convention); a non-numeric (ISO string) falls back to now.
    for key in ["@timestamp", "timestamp", "time", "_timestamp"] {
        if let Some(v) = doc.get(key) {
            if let Some(ms) = v.as_i64() {
                return ms.saturating_mul(1_000_000);
            }
            if let Some(ms) = v.as_f64() {
                return (ms * 1_000_000.0) as i64;
            }
        }
    }
    now_ns()
}

/// Extract the severity text from the common field names.
fn extract_severity(doc: &serde_json::Value) -> String {
    for key in ["severityText", "severity", "level", "log.level", "loglevel"] {
        if let Some(s) = doc.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    String::new()
}

/// Extract the message body from the common field names (OTLP `body.stringValue`,
/// or `message`/`msg`/`body`).
fn extract_body(doc: &serde_json::Value) -> String {
    if let Some(b) = doc.get("body") {
        if let Some(s) = b.get("stringValue").and_then(|v| v.as_str()) {
            return s.to_string();
        }
        if let Some(s) = b.as_str() {
            return s.to_string();
        }
    }
    for key in ["message", "msg", "log", "_message"] {
        if let Some(s) = doc.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    String::new()
}

/// The set of doc keys consumed into fixed [`LogRecord`] fields (so they are not
/// ALSO duplicated into `attrs`).
const RESERVED_KEYS: &[&str] = &[
    "timeUnixNano",
    "observedTimeUnixNano",
    "@timestamp",
    "timestamp",
    "time",
    "_timestamp",
    "severityText",
    "severity",
    "level",
    "log.level",
    "loglevel",
    "body",
    "message",
    "msg",
    "log",
    "_message",
    "stream",
    "_stream",
    "attributes",
];

/// Collect the remaining scalar fields of a doc into the dynamic attribute map, plus
/// any OTLP `attributes` array (`[{key,value:{stringValue}}]`).
fn extract_attrs(doc: &serde_json::Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(obj) = doc.as_object() {
        for (k, v) in obj {
            if RESERVED_KEYS.contains(&k.as_str()) {
                continue;
            }
            out.insert(k.clone(), scalar_to_string(v));
        }
    }
    // OTLP-style attributes array.
    if let Some(arr) = doc.get("attributes").and_then(|v| v.as_array()) {
        for a in arr {
            if let (Some(k), Some(val)) = (a.get("key").and_then(|v| v.as_str()), a.get("value")) {
                out.insert(k.to_string(), otlp_anyvalue(val));
            }
        }
    }
    out
}

/// Render a JSON scalar (or nested value) as a compact attribute string.
fn scalar_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Unwrap an OTLP `AnyValue` (`{stringValue|intValue|doubleValue|boolValue}`).
fn otlp_anyvalue(v: &serde_json::Value) -> String {
    for key in ["stringValue", "intValue", "doubleValue", "boolValue"] {
        if let Some(inner) = v.get(key) {
            return scalar_to_string(inner);
        }
    }
    scalar_to_string(v)
}

/// Resolve the stream for a doc: an explicit `stream`/`_stream` field wins, else the
/// caller's default (path/query-derived).
fn extract_stream(doc: &serde_json::Value, default_stream: &str) -> String {
    for key in ["stream", "_stream"] {
        if let Some(s) = doc.get(key).and_then(|v| v.as_str()) {
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    default_stream.to_string()
}

/// Normalize a single generic doc into a [`LogRecord`] with the given default stream.
fn doc_to_record(doc: &serde_json::Value, default_stream: &str) -> LogRecord {
    LogRecord {
        ts: extract_ts(doc),
        stream: extract_stream(doc, default_stream),
        severity: extract_severity(doc),
        body: extract_body(doc),
        attrs: extract_attrs(doc),
    }
}

/// Parse an OTLP/HTTP JSON `ExportLogsServiceRequest`
/// (`resourceLogs[].scopeLogs[].logRecords[]`) into records. The stream is derived
/// from the resource's `service.name` attribute, else `default_stream`.
pub fn parse_otlp_logs(body: &str, default_stream: &str) -> Result<Vec<LogRecord>, String> {
    let root: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("parse OTLP JSON: {e}"))?;
    let mut out = Vec::new();
    let resource_logs = root
        .get("resourceLogs")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for rl in &resource_logs {
        // Resource service.name → stream.
        let mut stream = default_stream.to_string();
        if let Some(attrs) = rl
            .get("resource")
            .and_then(|r| r.get("attributes"))
            .and_then(|v| v.as_array())
        {
            for a in attrs {
                if a.get("key").and_then(|v| v.as_str()) == Some("service.name") {
                    if let Some(val) = a.get("value") {
                        let s = otlp_anyvalue(val);
                        if !s.is_empty() {
                            stream = s;
                        }
                    }
                }
            }
        }
        for sl in rl
            .get("scopeLogs")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            for lr in sl
                .get("logRecords")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
            {
                out.push(doc_to_record(lr, &stream));
            }
        }
    }
    Ok(out)
}

/// Parse an Elasticsearch `_bulk` NDJSON body: alternating action/source lines
/// (`{"index":{"_index":"logs"}}\n{doc}\n`). The `_index` names the stream (else
/// `default_stream`). `create`/`index` actions carry a following source line;
/// `delete` actions (no source) are skipped.
pub fn parse_es_bulk(body: &str, default_stream: &str) -> Vec<LogRecord> {
    let mut out = Vec::new();
    let mut lines = body.lines().filter(|l| !l.trim().is_empty());
    while let Some(action_line) = lines.next() {
        let action: serde_json::Value = match serde_json::from_str(action_line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        // The action object has ONE key: index|create|update|delete.
        let (op, meta) = match action.as_object().and_then(|o| o.iter().next()) {
            Some((k, v)) => (k.as_str(), v),
            None => continue,
        };
        if op == "delete" {
            continue; // no source line follows
        }
        let Some(source_line) = lines.next() else {
            break;
        };
        let doc: serde_json::Value = match serde_json::from_str(source_line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let stream = meta
            .get("_index")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(default_stream);
        out.push(doc_to_record(&doc, stream));
    }
    out
}

/// Parse a plain JSON-lines body (one JSON object per line) into records.
pub fn parse_json_lines(body: &str, default_stream: &str) -> Vec<LogRecord> {
    let trimmed = body.trim_start();
    // Tolerate a single JSON array too (`[ {...}, {...} ]`).
    if trimmed.starts_with('[') {
        if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(body) {
            return arr
                .iter()
                .map(|d| doc_to_record(d, default_stream))
                .collect();
        }
    }
    body.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .map(|d| doc_to_record(&d, default_stream))
        .collect()
}

// ── HTTP listener (hand-rolled, no axum/hyper — the Pi contract) ────────────────

/// A parsed HTTP request: method, raw target, content type, body.
struct HttpRequest {
    method: String,
    target: String,
    content_type: String,
    body: String,
    /// CONCEPT:EG-OS.observability.prometheus-ingest — the ORIGINAL (un-lossy-UTF-8'd) request bytes, kept for the
    /// Prometheus `remote_write` receiver whose body is snappy-compressed BINARY (the
    /// `body` String would corrupt it). Only populated/read under `otel-export`.
    #[cfg(feature = "otel-export")]
    body_bytes: Vec<u8>,
}

/// Read one HTTP/1.1 request: headers to the blank line, then `Content-Length` body.
/// Mirrors [`crate::server::sparql_http`]'s reader (bounded header flood guard).
async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<HttpRequest> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 16 * 1024 * 1024 {
            return None;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();

    let mut content_length = 0usize;
    let mut content_type = String::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            match k.trim().to_ascii_lowercase().as_str() {
                "content-length" => content_length = v.trim().parse().unwrap_or(0),
                "content-type" => content_type = v.trim().to_ascii_lowercase(),
                _ => {}
            }
        }
    }
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    if content_length > 0 && body.len() > content_length {
        body.truncate(content_length);
    }
    Some(HttpRequest {
        method,
        target,
        content_type,
        body: String::from_utf8_lossy(&body).to_string(),
        #[cfg(feature = "otel-export")]
        body_bytes: body,
    })
}

/// Serve the observability log-ingestion HTTP surface on `listener`, backed by
/// `state`. One task per connection, one response per request, connection: close —
/// the SAME dependency-free idiom as the SPARQL / metrics listeners.
pub async fn serve(listener: TcpListener, state: Arc<ObsState>) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let state = state.clone();
        tokio::spawn(async move {
            let (status, ctype, body) = match read_request(&mut stream).await {
                Some(req) => handle(&state, req).await,
                None => (
                    "400 Bad Request",
                    "text/plain",
                    "malformed HTTP request".to_string(),
                ),
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}

/// Route + execute an ingest request → `(status, content_type, body)`.
async fn handle(state: &Arc<ObsState>, req: HttpRequest) -> (&'static str, &'static str, String) {
    let (path, query) = match req.target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (req.target.as_str(), ""),
    };
    if req.method == "OPTIONS" {
        return ("204 No Content", "text/plain", String::new());
    }
    // Health probe.
    if path == "/healthz" || path == "/" && req.method == "GET" {
        return ("200 OK", "text/plain", "ok".to_string());
    }

    // CONCEPT:EG-KG.query.prometheus-http-query-api — the Prometheus HTTP query API (GET or POST), routed BEFORE the
    // POST-only ingest guard (instant queries are typically GET). Gated on `promql`,
    // which implies `obs`; absent that feature these paths fall through to 404.
    #[cfg(feature = "promql")]
    if path.starts_with("/api/v1/query")
        || path == "/api/v1/labels"
        || path.starts_with("/api/v1/label/")
    {
        return crate::server::promql::handle(state, &req.method, path, query, &req.body).await;
    }

    // CONCEPT:EG-OS.observability.trace-assembly — distributed-trace surface (GET or POST), routed BEFORE the
    // POST-only ingest guard (trace SEARCH / assembly / dependency-graph are GET).
    // Gated on `traces`, which implies `obs`; absent that feature these paths fall
    // through to 404. Covers OTLP-JSON ingest (`POST /v1/traces`), trace search
    // (`/api/traces`), single-trace assembly (`/api/traces/<id>`) and the
    // service-dependency graph (`/api/dependencies`).
    #[cfg(feature = "traces")]
    if path == "/v1/traces"
        || path == "/api/traces"
        || path.starts_with("/api/traces/")
        || path == "/api/dependencies"
        || path == "/api/services/dependencies"
    {
        return crate::server::traces::handle(state, &req.method, path, query, &req.body).await;
    }

    // CONCEPT:EG-OS.observability.prometheus-ingest — the Prometheus `remote_write` receiver (`POST /api/v1/write`):
    // decode the snappy-compressed protobuf WriteRequest from the RAW body bytes (the
    // lossy-UTF-8 `body` String would corrupt the binary) and land its samples in the
    // durable eg-tsdb SeriesStore. Gated on `otel-export`; absent the feature this path
    // falls through to the unknown-ingest 404.
    #[cfg(feature = "otel-export")]
    if path == "/api/v1/write" {
        return remote_write::handle(state, &req.method, &req.body_bytes).await;
    }

    if req.method != "POST" {
        return (
            "405 Method Not Allowed",
            "text/plain",
            "POST only".to_string(),
        );
    }

    // EG-162 search surface: O2/Elasticsearch `_search`-shaped query API. Routed
    // BEFORE ingest (no ingest path ends with `_search`).
    if path == "/api/_search" || path == "/_search" || path.ends_with("/_search") {
        return handle_search(state, path, query, &req.body).await;
    }

    // The `stream` query param is the default stream for shapes that don't name one.
    let default_stream = query_param(query, "stream").unwrap_or_else(|| "default".to_string());

    // Route by path → parse into records + choose the response shape.
    enum Shape {
        Otlp,
        EsBulk,
        EsDoc,
        Lines,
    }
    let (records, shape): (Result<Vec<LogRecord>, String>, Shape) = if path == "/v1/logs" {
        (parse_otlp_logs(&req.body, &default_stream), Shape::Otlp)
    } else if path == "/_bulk" || path.ends_with("/_bulk") {
        (Ok(parse_es_bulk(&req.body, &default_stream)), Shape::EsBulk)
    } else if let Some(stream) = es_doc_stream(path) {
        // `/<stream>/_doc` — a single ES document.
        let doc: Result<serde_json::Value, String> =
            serde_json::from_str(&req.body).map_err(|e| format!("parse _doc JSON: {e}"));
        (doc.map(|d| vec![doc_to_record(&d, &stream)]), Shape::EsDoc)
    } else if path == "/" || path == "/api/logs" || path == "/logs" {
        (
            Ok(parse_json_lines(&req.body, &default_stream)),
            Shape::Lines,
        )
    } else {
        return (
            "404 Not Found",
            "text/plain",
            "unknown ingest path".to_string(),
        );
    };

    let records = match records {
        Ok(r) => r,
        Err(e) => return ("400 Bad Request", "text/plain", e),
    };

    // Ingest OFF the reactor (redb + Tantivy commit are blocking).
    let st = state.clone();
    let outcome = tokio::task::spawn_blocking(move || st.ingest(records)).await;
    match outcome {
        Ok(Ok(o)) => {
            let (status, ctype, body) = match shape {
                Shape::Otlp => (
                    "200 OK",
                    "application/json",
                    "{\"partialSuccess\":{}}".to_string(),
                ),
                Shape::EsBulk => ("200 OK", "application/json", es_bulk_response(o.accepted)),
                Shape::EsDoc => (
                    "201 Created",
                    "application/json",
                    "{\"result\":\"created\",\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}}"
                        .to_string(),
                ),
                Shape::Lines => (
                    "200 OK",
                    "application/json",
                    format!(
                        "{{\"successful\":{},\"failed\":0,\"segments\":{}}}",
                        o.accepted, o.segments_flushed
                    ),
                ),
            };
            (status, ctype, body)
        }
        Ok(Err(e)) => (
            "500 Internal Server Error",
            "text/plain",
            format!("ingest failed: {e}"),
        ),
        Err(e) => (
            "500 Internal Server Error",
            "text/plain",
            format!("ingest task failed: {e}"),
        ),
    }
}

// ── EG-162 search surface (O2 / Elasticsearch `_search`) ────────────────────────

/// Route + execute a `_search` request → `(status, content_type, body)`.
///
/// Two modes on the SAME endpoint, discriminated by the JSON body:
///  * a raw SQL query — `{"sql": "SELECT …"}` (or `{"query":{"sql":"…"}}`, the O2
///    shape) — runs DataFusion over the `logs` table (segments + hot buffers);
///  * a structured log search — `{stream, start_time, end_time, query, size, …}` —
///    returns O2/ES-shaped hits UNIONed across the hot + cold tiers.
///
/// The stream may come from the path (`/api/<org>/<stream>/_search`) or the body.
async fn handle_search(
    state: &Arc<ObsState>,
    path: &str,
    query: &str,
    body: &str,
) -> (&'static str, &'static str, String) {
    let val: serde_json::Value = if body.trim().is_empty() {
        serde_json::json!({})
    } else {
        match serde_json::from_str(body) {
            Ok(v) => v,
            Err(e) => {
                return (
                    "400 Bad Request",
                    "text/plain",
                    format!("parse _search JSON: {e}"),
                )
            }
        }
    };

    // SQL mode: top-level `sql`, or the O2 `{"query":{"sql":…}}` nesting.
    let sql = val.get("sql").and_then(|v| v.as_str()).or_else(|| {
        val.get("query")
            .and_then(|q| q.get("sql"))
            .and_then(|v| v.as_str())
    });
    if let Some(sql) = sql {
        let sql = sql.to_string();
        let st = state.clone();
        return match tokio::task::spawn_blocking(move || st.search_sql(&sql)).await {
            Ok(Ok(res)) => ("200 OK", "application/json", sql_search_response(&res)),
            Ok(Err(e)) => (
                "400 Bad Request",
                "text/plain",
                format!("sql search failed: {e}"),
            ),
            Err(e) => (
                "500 Internal Server Error",
                "text/plain",
                format!("sql search task failed: {e}"),
            ),
        };
    }

    // Structured search mode: resolve the stream (path wins, then body, then `?stream`).
    let stream = search_stream_from_path(path)
        .or_else(|| {
            for key in ["stream", "_stream", "index", "_index"] {
                if let Some(s) = val
                    .get(key)
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                {
                    return Some(s.to_string());
                }
            }
            None
        })
        .or_else(|| query_param(query, "stream"));
    let stream = match stream {
        Some(s) => s,
        None => {
            return (
                "400 Bad Request",
                "text/plain",
                "search requires a stream (path /api/<org>/<stream>/_search or body `stream`)"
                    .to_string(),
            )
        }
    };

    let q = parse_log_query(&val, stream);
    let st = state.clone();
    match tokio::task::spawn_blocking(move || st.search_logs(&q)).await {
        Ok(Ok(hits)) => ("200 OK", "application/json", es_search_response(&hits)),
        Ok(Err(e)) => (
            "400 Bad Request",
            "text/plain",
            format!("search failed: {e}"),
        ),
        Err(e) => (
            "500 Internal Server Error",
            "text/plain",
            format!("search task failed: {e}"),
        ),
    }
}

/// If `path` is `/api/<org>/<stream>/_search`, return `<stream>`; else `None`
/// (`/api/_search` and `/_search` carry the stream in the body).
fn search_stream_from_path(path: &str) -> Option<String> {
    let trimmed = path.trim_start_matches('/');
    let parts: Vec<&str> = trimmed.split('/').collect();
    // /api/<org>/<stream>/_search  → ["api", org, stream, "_search"]
    if parts.len() == 4 && parts[0] == "api" && parts[3] == "_search" && !parts[2].is_empty() {
        return Some(parts[2].to_string());
    }
    // /<stream>/_search → ["stream", "_search"] (but NOT the org-less "/api/_search").
    if parts.len() == 2 && parts[1] == "_search" && !parts[0].is_empty() && parts[0] != "api" {
        return Some(parts[0].to_string());
    }
    None
}

/// Build a [`LogQuery`] from the parsed `_search` JSON body. Tolerates the common
/// shapes: `start_time`/`end_time` (O2) or `from`/`to` for the window; a full-text
/// `query` string (bare, or the ES `{"query_string":{"query":…}}` nesting); a
/// `filters` object of attribute equalities + a `severity`; and `size`.
fn parse_log_query(val: &serde_json::Value, stream: String) -> LogQuery {
    let ts = |keys: &[&str]| -> Option<i64> {
        for k in keys {
            if let Some(n) = val.get(*k).and_then(|v| v.as_i64()) {
                return Some(n);
            }
        }
        None
    };
    let from = ts(&["start_time", "from_ts", "from"]).unwrap_or(i64::MIN);
    let to = ts(&["end_time", "to_ts", "to"]).unwrap_or(i64::MAX);

    // Full-text terms: bare `query`/`q` string, or ES `query.query_string.query`.
    let terms = val
        .get("query")
        .and_then(|q| q.as_str())
        .map(str::to_string)
        .or_else(|| val.get("q").and_then(|v| v.as_str()).map(str::to_string))
        .or_else(|| {
            val.get("query")
                .and_then(|q| q.get("query_string"))
                .and_then(|qs| qs.get("query"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .filter(|s| !s.trim().is_empty());

    let severity = val
        .get("severity")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let mut filters = Vec::new();
    if let Some(obj) = val.get("filters").and_then(|v| v.as_object()) {
        for (k, v) in obj {
            filters.push((k.clone(), scalar_to_string(v)));
        }
    }

    let size = val
        .get("size")
        .and_then(|v| v.as_u64())
        .map(|n| n as usize)
        .unwrap_or(DEFAULT_SEARCH_SIZE);

    LogQuery {
        stream,
        from,
        to,
        terms,
        filters,
        severity,
        size,
    }
}

/// Render one log record as an Elasticsearch/O2 `_source` object.
fn record_source(r: &LogRecord) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("_timestamp".into(), serde_json::json!(r.ts));
    obj.insert("stream".into(), serde_json::json!(r.stream));
    obj.insert("severity".into(), serde_json::json!(r.severity));
    obj.insert("message".into(), serde_json::json!(r.body));
    for (k, v) in &r.attrs {
        // Never let an attribute clobber a reserved field.
        if !obj.contains_key(k) {
            obj.insert(k.clone(), serde_json::json!(v));
        }
    }
    serde_json::Value::Object(obj)
}

/// The Elasticsearch/O2 `_search` response envelope over the matched records.
fn es_search_response(hits: &[LogRecord]) -> String {
    let items: Vec<serde_json::Value> = hits
        .iter()
        .map(|r| {
            serde_json::json!({
                "_index": r.stream,
                "_score": 1.0,
                "_source": record_source(r),
            })
        })
        .collect();
    serde_json::json!({
        "took": 0,
        "timed_out": false,
        "hits": {
            "total": { "value": hits.len(), "relation": "eq" },
            "hits": items,
        }
    })
    .to_string()
}

/// The SQL `_search` response: columns + rows, plus row objects (`hits`) keyed by
/// column name (the O2 SQL result shape).
fn sql_search_response(res: &eg_query::TypedQueryResult) -> String {
    let cols: Vec<&str> = res.columns.iter().map(|c| c.name.as_str()).collect();
    let hits: Vec<serde_json::Value> = res
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (i, cell) in row.iter().enumerate() {
                if let Some(name) = cols.get(i) {
                    obj.insert((*name).to_string(), cell.clone());
                }
            }
            serde_json::Value::Object(obj)
        })
        .collect();
    serde_json::json!({
        "took": 0,
        "total": res.rows.len(),
        "columns": cols,
        "hits": hits,
    })
    .to_string()
}

/// An ES `_bulk` response: `errors:false` with one `index`/`created` item per doc.
fn es_bulk_response(n: usize) -> String {
    let items: Vec<serde_json::Value> = (0..n)
        .map(|_| serde_json::json!({"index":{"status":201,"result":"created"}}))
        .collect();
    serde_json::json!({ "took": 0, "errors": false, "items": items }).to_string()
}

/// If `path` is `/<stream>/_doc` (optionally `/<stream>/_doc/<id>`), return `<stream>`.
fn es_doc_stream(path: &str) -> Option<String> {
    let trimmed = path.trim_start_matches('/');
    let mut parts = trimmed.split('/');
    let stream = parts.next()?;
    let doc_marker = parts.next()?;
    if stream.is_empty() || doc_marker != "_doc" {
        return None;
    }
    Some(stream.to_string())
}

/// Extract a query-string parameter value (no percent-decoding needed for a bare
/// stream name; kept minimal).
fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key && !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn otlp_parses_and_derives_stream_from_service_name() {
        let body = r#"{
          "resourceLogs":[{
            "resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},
            "scopeLogs":[{"logRecords":[
              {"timeUnixNano":"1700000000000000000","severityText":"ERROR",
               "body":{"stringValue":"payment declined"},
               "attributes":[{"key":"order","value":{"stringValue":"o-42"}}]},
              {"timeUnixNano":"1700000000000000001","severityText":"INFO",
               "body":{"stringValue":"cart viewed"}}
            ]}]
          }]
        }"#;
        let recs = parse_otlp_logs(body, "default").unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].stream, "checkout");
        assert_eq!(recs[0].severity, "ERROR");
        assert_eq!(recs[0].body, "payment declined");
        assert_eq!(recs[0].ts, 1_700_000_000_000_000_000);
        assert_eq!(recs[0].attrs.get("order").unwrap(), "o-42");
    }

    #[test]
    fn es_bulk_parses_action_doc_pairs() {
        let body = "{\"index\":{\"_index\":\"weblogs\"}}\n\
                    {\"@timestamp\":1700,\"level\":\"WARN\",\"message\":\"slow\"}\n\
                    {\"create\":{\"_index\":\"weblogs\"}}\n\
                    {\"message\":\"ok\",\"level\":\"INFO\"}\n\
                    {\"delete\":{\"_index\":\"weblogs\",\"_id\":\"x\"}}\n";
        let recs = parse_es_bulk(body, "default");
        assert_eq!(recs.len(), 2, "delete carries no source line");
        assert_eq!(recs[0].stream, "weblogs");
        assert_eq!(recs[0].severity, "WARN");
        assert_eq!(recs[0].body, "slow");
        assert_eq!(recs[0].ts, 1700 * 1_000_000, "epoch-ms → ns");
        assert_eq!(recs[1].body, "ok");
    }

    #[test]
    fn json_lines_parse_with_default_and_explicit_stream() {
        let body = "{\"message\":\"a\",\"level\":\"INFO\"}\n\
                    {\"message\":\"b\",\"stream\":\"other\",\"level\":\"ERROR\"}\n";
        let recs = parse_json_lines(body, "svc");
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].stream, "svc");
        assert_eq!(recs[1].stream, "other");
    }

    #[test]
    fn es_doc_path_extracts_stream() {
        assert_eq!(es_doc_stream("/logs/_doc").as_deref(), Some("logs"));
        assert_eq!(es_doc_stream("/logs/_doc/abc").as_deref(), Some("logs"));
        assert_eq!(es_doc_stream("/_bulk"), None);
        assert_eq!(es_doc_stream("/v1/logs"), None);
    }

    #[test]
    fn ingest_lands_in_tsdb_series_and_text_index() {
        let obs = ObsState::in_memory(1024).unwrap();
        let recs = vec![
            LogRecord {
                ts: 1000,
                stream: "app".into(),
                severity: "ERROR".into(),
                body: "database connection refused".into(),
                attrs: BTreeMap::new(),
            },
            LogRecord {
                ts: 2000,
                stream: "app".into(),
                severity: "INFO".into(),
                body: "server listening on port".into(),
                attrs: BTreeMap::new(),
            },
        ];
        let out = obs.ingest(recs).unwrap();
        assert_eq!(out.accepted, 2);

        // (a) tsdb: both points land in the series, ts-ordered.
        let points = obs.series_range("app", 0, 10_000).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].ts, 1000);
        assert_eq!(points[0].values[0], severity_number("ERROR"));

        // (b) text: a BM25 query retrieves the matching record.
        let hits = obs.search("app", "database connection", 10);
        assert_eq!(hits.len(), 1, "one doc matches 'database connection'");
        assert!(hits[0].id.starts_with("app:"));
        // A term only in the OTHER doc matches that one, not the first.
        let listening = obs.search("app", "listening port", 10);
        assert_eq!(listening.len(), 1);
    }

    #[test]
    fn threshold_flush_rolls_a_parquet_segment_that_round_trips() {
        // flush_threshold=2 → the 2-record ingest rolls one segment immediately.
        let obs = ObsState::in_memory(2).unwrap();
        let recs = vec![
            LogRecord {
                ts: 10,
                stream: "s".into(),
                severity: "INFO".into(),
                body: "one".into(),
                attrs: BTreeMap::new(),
            },
            LogRecord {
                ts: 20,
                stream: "s".into(),
                severity: "WARN".into(),
                body: "two".into(),
                attrs: BTreeMap::new(),
            },
        ];
        let out = obs.ingest(recs).unwrap();
        assert_eq!(out.segments_flushed, 1);

        let segs = obs.segments_for("s");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].row_count, 2);
        assert_eq!(segs[0].min_ts, 10);
        assert_eq!(segs[0].max_ts, 20);

        // The Parquet segment round-trips out of the CAS.
        let back = obs.read_segment(&segs[0]).unwrap();
        assert_eq!(back.len(), 2);
        let bodies: Vec<&str> = back.iter().map(|r| r.body.as_str()).collect();
        assert!(bodies.contains(&"one") && bodies.contains(&"two"));
    }

    #[test]
    fn small_stream_force_flush_via_flush_stream() {
        // Below the threshold: nothing auto-flushes, but flush_stream forces it.
        let obs = ObsState::in_memory(1000).unwrap();
        obs.ingest(vec![LogRecord {
            ts: 5,
            stream: "tiny".into(),
            severity: "INFO".into(),
            body: "hello".into(),
            attrs: BTreeMap::new(),
        }])
        .unwrap();
        assert!(obs.segments_for("tiny").is_empty());
        let m = obs.flush_stream("tiny").unwrap().expect("forced segment");
        assert_eq!(m.row_count, 1);
        assert!(
            obs.flush_stream("tiny").unwrap().is_none(),
            "buffer drained"
        );
    }

    #[tokio::test]
    async fn http_otlp_ingest_end_to_end() {
        let obs = Arc::new(ObsState::in_memory(1024).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = obs.clone();
        tokio::spawn(async move { serve(listener, served).await });

        let body = r#"{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"4200","severityText":"INFO","body":{"stringValue":"unique_marker_token served"}}]}]}]}"#;
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST /v1/logs HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut resp = Vec::new();
        sock.read_to_end(&mut resp).await.unwrap();
        let text = String::from_utf8_lossy(&resp);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        assert!(text.contains("partialSuccess"));

        // The record landed: tsdb series + searchable text.
        let points = obs.series_range("api", 0, 10_000).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].ts, 4200);
        let hits = obs.search("api", "unique_marker_token", 10);
        assert_eq!(hits.len(), 1);
    }

    #[tokio::test]
    async fn http_es_bulk_ingest_end_to_end() {
        let obs = Arc::new(ObsState::in_memory(1024).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = obs.clone();
        tokio::spawn(async move { serve(listener, served).await });

        let body = "{\"index\":{\"_index\":\"esstream\"}}\n\
                    {\"@timestamp\":7,\"level\":\"ERROR\",\"message\":\"es_bulk_marker boom\"}\n";
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST /_bulk HTTP/1.1\r\nHost: x\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut resp = Vec::new();
        sock.read_to_end(&mut resp).await.unwrap();
        let text = String::from_utf8_lossy(&resp);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        assert!(text.contains("\"errors\":false"));

        let hits = obs.search("esstream", "es_bulk_marker", 10);
        assert_eq!(hits.len(), 1);
        let points = obs.series_range("esstream", 0, 100_000_000).unwrap();
        assert_eq!(points.len(), 1);
    }

    /// Send a POST to `addr` and return the response text.
    #[cfg(test)]
    async fn post(addr: std::net::SocketAddr, path: &str, body: &str) -> String {
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST {path} HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut resp = Vec::new();
        sock.read_to_end(&mut resp).await.unwrap();
        String::from_utf8_lossy(&resp).to_string()
    }

    /// The `_search` HTTP surface: a structured search returns ES-shaped hits, and a
    /// `{"sql":…}` body aggregates over the log segments (CONCEPT:EG-KG.query.concept-4).
    #[tokio::test]
    async fn http_search_end_to_end() {
        // flush_threshold=2 → two records roll a segment, one stays hot.
        let obs = Arc::new(ObsState::in_memory(2).unwrap());
        obs.ingest(vec![
            LogRecord {
                ts: 100,
                stream: "web".into(),
                severity: "ERROR".into(),
                body: "search_marker cold".into(),
                attrs: BTreeMap::new(),
            },
            LogRecord {
                ts: 200,
                stream: "web".into(),
                severity: "INFO".into(),
                body: "quiet cold".into(),
                attrs: BTreeMap::new(),
            },
        ])
        .unwrap();
        obs.ingest(vec![LogRecord {
            ts: 300,
            stream: "web".into(),
            severity: "WARN".into(),
            body: "search_marker hot".into(),
            attrs: BTreeMap::new(),
        }])
        .unwrap();
        assert_eq!(obs.segments_for("web").len(), 1);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = obs.clone();
        tokio::spawn(async move { serve(listener, served).await });

        // Structured search: time window spanning both tiers, path-derived stream.
        let text = post(
            addr,
            "/api/default/web/_search",
            r#"{"start_time":0,"end_time":1000}"#,
        )
        .await;
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        let json_body = text.split("\r\n\r\n").nth(1).unwrap();
        let v: serde_json::Value = serde_json::from_str(json_body).unwrap();
        assert_eq!(v["hits"]["total"]["value"], 3, "both tiers: {json_body}");

        // Full-text term via the `query` string → only the two "search_marker" records.
        let text = post(
            addr,
            "/api/_search",
            r#"{"stream":"web","query":"search_marker"}"#,
        )
        .await;
        let v: serde_json::Value =
            serde_json::from_str(text.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(v["hits"]["total"]["value"], 2);

        // SQL mode: count by severity over the `logs` table (segments + hot).
        let text = post(
            addr,
            "/api/_search",
            r#"{"sql":"SELECT severity, count(*) AS n FROM logs GROUP BY severity ORDER BY severity"}"#,
        )
        .await;
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        let v: serde_json::Value =
            serde_json::from_str(text.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(v["total"], 3, "ERROR/INFO/WARN buckets: {text}");
        assert_eq!(v["hits"][0]["severity"], "ERROR");
        assert_eq!(v["hits"][0]["n"], 1);
    }
}
