//! Arrow dataset-handle export + signed result writeback (CONCEPT:INT-P2-2 — the Arrow
//! Flight / dataset-handle seam for external heavy compute, feature `dataset-handle`).
//!
//! Codex's ask: heavy model training/compute should stay OUTSIDE the engine (in
//! data-science-mcp or an accelerator), and the scalable seam is
//!
//! ```text
//! engine snapshot/dataset handle → Arrow → external job → signed result artifact → transactional writeback
//! ```
//!
//! never marshalling millions of rows through Python/JSON. This module is the engine
//! side of that seam, served on its OWN hand-rolled HTTP/1.1 listener (the SAME
//! dep-free idiom as `--metrics-addr`/`--obs-addr`/`--sparql-addr` — no
//! axum/hyper/tonic, so the Pi-contract discipline holds):
//!
//!   * `POST /dataset/export` `{"graph": "...", "sql": "..."}` — runs `sql` over
//!     `graph`'s CURRENT snapshot via [`eg_query::exec_sql_arrow`] (the SAME DataFusion
//!     path `Method::Sql`/pgwire use) and materializes the result ONCE into a NEW,
//!     IMMUTABLE, id-addressed [`DatasetHandleMeta`] (JSON response: id, schema, row
//!     count, snapshot version).
//!   * `GET /dataset/<id>` — streams the handle's Arrow `RecordBatch`es back as a real
//!     Arrow IPC stream (`application/vnd.apache.arrow.stream`) — typed columns OUT,
//!     never JSON rows. Any Arrow IPC reader (`pyarrow.ipc.open_stream`, polars,
//!     DataFusion) can pull it with no Flight client.
//!   * `POST /dataset/<id>/result` — the WRITEBACK seam: an external job POSTs its
//!     result bytes with `X-Eg-Job-Id` + `X-Eg-Signature` headers. The signature is
//!     `hex(HMAC-SHA256(secret, "dataset-result:<id>:<job-id>:<sha256-hex-of-body>"))`
//!     — the SAME `hex(HMAC-SHA256(secret, …))` construction the pgwire/mysql-wire/
//!     mssql-wire SCRAM-style password schemes already use, binding the dataset id AND
//!     job id into the signed message (so a signature can't be replayed against a
//!     different dataset/job) — verified in CONSTANT time
//!     ([`hmac::Mac::verify_slice`], never `==`). A verified artifact is written ONCE
//!     into the blob CAS (content-addressed by its own sha256, [`super::blob::store::hex_digest`]),
//!     ref-counted, and committed as a `:DatasetResult` node in the SAME graph the
//!     dataset was exported from — one `add_node` + `mark_dirty()`, the SAME
//!     durability discipline the SPARQL Graph Store HTTP surface
//!     (`src/server/sparql_http.rs::handle_graph_store`) already uses for a
//!     non-wire-dispatch write. The node is immediately queryable through the normal
//!     `SELECT * FROM nodes` surface — the engine owns the job/snapshot/output
//!     registration, not the external caller.
//!
//! ## Why Arrow-IPC-over-HTTP, not a real Arrow Flight (gRPC) server
//!
//! A real Arrow Flight server (the `arrow-flight` crate) was evaluated and rejected:
//! it is built on `tonic` (gRPC), which pulls `prost`/`h2`/`hyper`/`tower` — a heavy
//! async-gRPC stack this codebase deliberately avoids for EVERY other auxiliary
//! listener (`metrics`/`sparql-http`/`obs`/`federation-search` all hand-roll HTTP
//! instead of axum/hyper/tonic, precisely to keep the Pi-tier build dep-light). Arrow
//! IPC streaming is the wire format Flight uses internally anyway (a `DoGet` response
//! IS an Arrow IPC stream over gRPC framing) — serving it directly over plain HTTP
//! keeps the exact same typed-Arrow-out contract while adding zero new crates beyond
//! the `arrow` dependency `query`/`obs`/`knowledge-batch` already carry.
//!
//! ## Immutability + bounded memory
//!
//! A [`DatasetHandleRegistry`] entry is materialized ONCE at export time and never
//! mutated afterward — re-running the same SQL creates a NEW handle with a NEW id, so a
//! puller always sees a consistent snapshot no matter how many times it re-fetches.
//! The registry is bounded (`EPISTEMIC_GRAPH_DATASET_HANDLE_MAX`, default 64): the
//! OLDEST handle is evicted once the cap is exceeded, the same "bounded resident
//! memory over an unbounded surface" discipline the blob CAS / matview stores already
//! apply — an unbounded stream of exports can never leak memory.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use serde::Serialize;
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::server::blob::store::hex_digest;
use crate::server::ServerState;

type HmacSha256 = Hmac<Sha256>;

/// Env var naming the bind address (`host:port`) when `--dataset-addr` is not passed.
pub const DATASET_ADDR_ENV: &str = "EPISTEMIC_GRAPH_DATASET_ADDR";

/// Env var overriding the bounded handle-registry cap (default 64).
pub const DATASET_HANDLE_MAX_ENV: &str = "EPISTEMIC_GRAPH_DATASET_HANDLE_MAX";
const DEFAULT_MAX_HANDLES: usize = 64;

/// One immutable, materialized query result — the dataset a handle id addresses.
struct DatasetEntry {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    graph: String,
    sql: String,
    row_count: usize,
    created_at_unix: u64,
    snapshot_version: u64,
}

/// One Arrow field's JSON-facing description (the export response's schema summary —
/// the AUTHORITATIVE schema is the Arrow IPC stream itself; this is a convenience
/// preview so a caller can decide whether to pull the full stream at all).
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
pub struct DatasetFieldMeta {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// JSON metadata returned by `POST /dataset/export` (CONCEPT:INT-P2-2) — the stable id
/// + Arrow schema description of a freshly minted, immutable dataset handle.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
pub struct DatasetHandleMeta {
    pub id: String,
    pub graph: String,
    pub row_count: usize,
    pub created_at_unix: u64,
    pub snapshot_version: u64,
    pub schema: Vec<DatasetFieldMeta>,
}

fn schema_meta(schema: &SchemaRef) -> Vec<DatasetFieldMeta> {
    schema
        .fields()
        .iter()
        .map(|f| DatasetFieldMeta {
            name: f.name().clone(),
            data_type: format!("{:?}", f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect()
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Bounded in-memory registry of exported dataset handles (CONCEPT:INT-P2-2). Every
/// entry is IMMUTABLE once created — a fresh export always mints a NEW id rather than
/// mutating an existing one — and materialized exactly once, so a pull never
/// re-executes the query or re-marshals rows. See the module docs for the eviction
/// discipline.
pub struct DatasetHandleRegistry {
    entries: Mutex<HashMap<String, Arc<DatasetEntry>>>,
    order: Mutex<VecDeque<String>>,
    next_id: AtomicU64,
    max_handles: usize,
}

impl Default for DatasetHandleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DatasetHandleRegistry {
    pub fn new() -> Self {
        let max_handles = std::env::var(DATASET_HANDLE_MAX_ENV)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_MAX_HANDLES);
        Self {
            entries: Mutex::new(HashMap::new()),
            order: Mutex::new(VecDeque::new()),
            next_id: AtomicU64::new(1),
            max_handles,
        }
    }

    /// Materialize `sql` over `graph`'s CURRENT snapshot into a NEW, immutable dataset
    /// handle. Runs the exact same DataFusion path `Method::Sql`/pgwire use
    /// ([`eg_query::exec_sql_arrow`]), so the result is real typed Arrow — never JSON
    /// rows. Returns the handle metadata; the batches themselves are pulled separately
    /// via [`Self::get`] (streamed as Arrow IPC by the HTTP handler).
    pub fn export(&self, core: &Arc<GraphCore>, graph: &str, sql: &str) -> Result<DatasetHandleMeta, String> {
        let (snap, version) = core.analysis_snapshot_versioned();
        let (schema, batches) = eg_query::exec_sql_arrow(&snap, sql)?;
        let row_count = batches.iter().map(|b| b.num_rows()).sum();
        let id = format!(
            "ds-{}-{}",
            now_unix(),
            self.next_id.fetch_add(1, Ordering::Relaxed)
        );
        let meta = DatasetHandleMeta {
            id: id.clone(),
            graph: graph.to_string(),
            row_count,
            created_at_unix: now_unix(),
            snapshot_version: version,
            schema: schema_meta(&schema),
        };
        let entry = Arc::new(DatasetEntry {
            schema,
            batches,
            graph: graph.to_string(),
            sql: sql.to_string(),
            row_count,
            created_at_unix: meta.created_at_unix,
            snapshot_version: version,
        });
        {
            let mut entries = self.entries.lock();
            let mut order = self.order.lock();
            entries.insert(id.clone(), entry);
            order.push_back(id);
            while order.len() > self.max_handles {
                if let Some(oldest) = order.pop_front() {
                    entries.remove(&oldest);
                }
            }
        }
        Ok(meta)
    }

    fn get(&self, id: &str) -> Option<Arc<DatasetEntry>> {
        self.entries.lock().get(id).cloned()
    }
}

/// Encode `schema` + `batches` as a real Arrow IPC STREAM (`application/vnd.apache.arrow.stream`):
/// a schema message followed by one message per record batch, then the end-of-stream
/// marker — the format any Arrow IPC reader (`pyarrow.ipc.open_stream`, polars,
/// DataFusion) understands with no Flight client involved.
fn encode_arrow_ipc_stream(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut buf, schema).map_err(|e| format!("ipc writer: {e}"))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| format!("ipc write batch: {e}"))?;
        }
        writer.finish().map_err(|e| format!("ipc finish: {e}"))?;
    }
    Ok(buf)
}

/// Serve the dataset-handle HTTP surface on `listener` (CONCEPT:INT-P2-2). Every
/// connection gets one request/response then closes — a minimal HTTP/1.1 idiom
/// matching `sparql_http`/`obs`, not a persistent connection pool.
pub async fn serve(listener: TcpListener, state: Arc<RwLock<ServerState>>) {
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
                    "text/plain".to_string(),
                    b"malformed HTTP request".to_vec(),
                ),
            };
            let resp_head = format!(
                "HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp_head.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            let _ = stream.shutdown().await;
        });
    }
}

/// A parsed HTTP request: method, raw target path, headers (lowercased keys), raw body
/// bytes. Bodies are NOT assumed to be UTF-8 (a result-writeback POST body is
/// arbitrary binary).
struct HttpRequest {
    method: String,
    target: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

/// Read one HTTP/1.1 request: headers up to the blank line, then `Content-Length`
/// bytes of body. Mirrors `sparql_http::read_request`'s framing but keeps the body as
/// raw bytes (never `String`) since a result artifact is arbitrary binary.
async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<HttpRequest> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
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
            return None; // header flood guard
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();

    let mut content_length = 0usize;
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim().to_ascii_lowercase();
            let val = v.trim().to_string();
            if key == "content-length" {
                content_length = val.parse().unwrap_or(0);
            }
            headers.insert(key, val);
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
        headers,
        body,
    })
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Route + execute a request → `(status, content-type, body-bytes)`.
async fn handle(
    state: &Arc<RwLock<ServerState>>,
    req: HttpRequest,
) -> (&'static str, String, Vec<u8>) {
    let path = req.target.split('?').next().unwrap_or(&req.target);

    if req.method == "POST" && path == "/dataset/export" {
        return handle_export(state, &req.body).await;
    }
    if req.method == "GET" {
        if let Some(id) = path.strip_prefix("/dataset/") {
            if !id.is_empty() && !id.contains('/') {
                return handle_pull(state, id).await;
            }
        }
    }
    if req.method == "POST" {
        if let Some(rest) = path.strip_prefix("/dataset/") {
            if let Some(id) = rest.strip_suffix("/result") {
                if !id.is_empty() {
                    return handle_result(state, id, &req.headers, &req.body).await;
                }
            }
        }
    }
    (
        "404 Not Found",
        "text/plain".to_string(),
        b"not found".to_vec(),
    )
}

#[derive(serde::Deserialize)]
struct ExportRequest {
    graph: String,
    sql: String,
}

async fn handle_export(
    state: &Arc<RwLock<ServerState>>,
    body: &[u8],
) -> (&'static str, String, Vec<u8>) {
    let req: ExportRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            return (
                "400 Bad Request",
                "text/plain".to_string(),
                format!("invalid request body: {e}").into_bytes(),
            )
        }
    };
    let core = {
        let s = state.read().await;
        s.registry.get(&req.graph).map(|e| e.core.clone())
    };
    let Some(core) = core else {
        return (
            "404 Not Found",
            "text/plain".to_string(),
            format!("no such graph: {}", req.graph).into_bytes(),
        );
    };
    let registry = state.read().await.dataset_handles.clone();
    let sql = req.sql.clone();
    let graph = req.graph.clone();
    let result = tokio::task::spawn_blocking(move || registry.export(&core, &graph, &sql)).await;
    match result {
        Ok(Ok(meta)) => (
            "200 OK",
            "application/json".to_string(),
            serde_json::to_vec(&meta).unwrap_or_default(),
        ),
        Ok(Err(e)) => (
            "400 Bad Request",
            "text/plain".to_string(),
            format!("query failed: {e}").into_bytes(),
        ),
        Err(e) => (
            "500 Internal Server Error",
            "text/plain".to_string(),
            format!("export task failed: {e}").into_bytes(),
        ),
    }
}

async fn handle_pull(state: &Arc<RwLock<ServerState>>, id: &str) -> (&'static str, String, Vec<u8>) {
    let registry = state.read().await.dataset_handles.clone();
    let Some(entry) = registry.get(id) else {
        return (
            "404 Not Found",
            "text/plain".to_string(),
            format!("no such dataset handle: {id}").into_bytes(),
        );
    };
    match encode_arrow_ipc_stream(&entry.schema, &entry.batches) {
        Ok(bytes) => (
            "200 OK",
            "application/vnd.apache.arrow.stream".to_string(),
            bytes,
        ),
        Err(e) => (
            "500 Internal Server Error",
            "text/plain".to_string(),
            format!("arrow encode: {e}").into_bytes(),
        ),
    }
}

/// Verify `hex(HMAC-SHA256(secret, "dataset-result:<id>:<job_id>:<digest>"))` in
/// CONSTANT time — the same construction (and the same [`hmac::Mac::verify_slice`]
/// discipline) `src/server/auth.rs` documents for the wire auth envelopes.
fn verify_result_signature(
    secret: &str,
    dataset_id: &str,
    job_id: &str,
    digest: &str,
    signature_hex: &str,
) -> bool {
    let Ok(sig_bytes) = hex::decode(signature_hex) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(format!("dataset-result:{dataset_id}:{job_id}:{digest}").as_bytes());
    mac.verify_slice(&sig_bytes).is_ok()
}

async fn handle_result(
    state: &Arc<RwLock<ServerState>>,
    id: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> (&'static str, String, Vec<u8>) {
    let registry = state.read().await.dataset_handles.clone();
    let Some(entry) = registry.get(id) else {
        return (
            "404 Not Found",
            "text/plain".to_string(),
            format!("no such dataset handle: {id}").into_bytes(),
        );
    };
    let Some(job_id) = headers.get("x-eg-job-id") else {
        return (
            "400 Bad Request",
            "text/plain".to_string(),
            b"missing X-Eg-Job-Id header".to_vec(),
        );
    };
    let Some(signature) = headers.get("x-eg-signature") else {
        return (
            "400 Bad Request",
            "text/plain".to_string(),
            b"missing X-Eg-Signature header".to_vec(),
        );
    };

    let digest = hex_digest(body);
    let secret = state.read().await.auth_secret.clone();
    if !verify_result_signature(&secret, id, job_id, &digest, signature) {
        return (
            "401 Unauthorized",
            "text/plain".to_string(),
            b"signature verification failed".to_vec(),
        );
    }

    let store = {
        let s = state.read().await;
        s.blob.as_ref().map(|b| b.store.clone())
    };
    let Some(store) = store else {
        return (
            "503 Service Unavailable",
            "text/plain".to_string(),
            b"blob substrate unavailable: no persist dir configured".to_vec(),
        );
    };

    let core = {
        let s = state.read().await;
        s.registry.get(&entry.graph).map(|e| e.core.clone())
    };
    let Some(core) = core else {
        return (
            "500 Internal Server Error",
            "text/plain".to_string(),
            format!("dataset handle's graph '{}' no longer exists", entry.graph).into_bytes(),
        );
    };

    let job_id = job_id.clone();
    let signature = signature.clone();
    let id_owned = id.to_string();
    let body_owned = body.to_vec();
    let committed = tokio::task::spawn_blocking(move || -> Result<(String, String, u64), String> {
        let (blob_digest, _was_new) = store.put_chunk(&body_owned)?;
        store.incref(&blob_digest)?;
        let committed_at = now_unix();
        let node_id = format!("dataset-result:{id_owned}:{job_id}");
        let blob = rmp_serde::to_vec_named(&serde_json::json!({
            "node_type": "DatasetResult",
            "dataset_handle_id": id_owned,
            "job_id": job_id,
            "blob_digest": blob_digest,
            "signature": signature,
            "committed_at": committed_at,
        }))
        .map_err(|e| format!("encode result node: {e}"))?;
        core.add_node(node_id.clone(), blob);
        core.mark_dirty();
        Ok((node_id, blob_digest, committed_at))
    })
    .await;

    match committed {
        Ok(Ok((node_id, blob_digest, committed_at))) => (
            "200 OK",
            "application/json".to_string(),
            serde_json::to_vec(&serde_json::json!({
                "node_id": node_id,
                "blob_digest": blob_digest,
                "committed_at": committed_at,
            }))
            .unwrap_or_default(),
        ),
        Ok(Err(e)) => (
            "500 Internal Server Error",
            "text/plain".to_string(),
            format!("writeback failed: {e}").into_bytes(),
        ),
        Err(e) => (
            "500 Internal Server Error",
            "text/plain".to_string(),
            format!("writeback task failed: {e}").into_bytes(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphCore;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn agents_graph() -> Arc<GraphCore> {
        let core = Arc::new(GraphCore::new());
        for (id, score) in [("a1", 10i64), ("a2", 50)] {
            core.add_node(
                id.into(),
                rmp_serde::to_vec_named(&serde_json::json!({"kind": "Agent", "score": score}))
                    .unwrap(),
            );
        }
        core
    }

    #[test]
    fn export_materializes_a_new_immutable_handle_each_call() {
        let registry = DatasetHandleRegistry::new();
        let core = agents_graph();
        let meta1 = registry
            .export(&core, "g", "SELECT id, score FROM nodes ORDER BY id")
            .expect("export 1");
        let meta2 = registry
            .export(&core, "g", "SELECT id, score FROM nodes ORDER BY id")
            .expect("export 2");
        assert_ne!(meta1.id, meta2.id, "each export mints a NEW id");
        assert_eq!(meta1.row_count, 2);
        assert!(meta1.schema.iter().any(|f| f.name == "score"));

        let entry1 = registry.get(&meta1.id).expect("handle 1 still present");
        assert_eq!(entry1.row_count, 2);
    }

    #[test]
    fn eviction_bounds_registry_size() {
        let registry = DatasetHandleRegistry {
            entries: Mutex::new(HashMap::new()),
            order: Mutex::new(VecDeque::new()),
            next_id: AtomicU64::new(1),
            max_handles: 2,
        };
        let core = agents_graph();
        let m1 = registry.export(&core, "g", "SELECT id FROM nodes").unwrap();
        let m2 = registry.export(&core, "g", "SELECT id FROM nodes").unwrap();
        let m3 = registry.export(&core, "g", "SELECT id FROM nodes").unwrap();
        assert!(registry.get(&m1.id).is_none(), "oldest handle evicted");
        assert!(registry.get(&m2.id).is_some());
        assert!(registry.get(&m3.id).is_some());
    }

    #[test]
    fn arrow_ipc_stream_round_trips_schema_and_values() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 50]))],
        )
        .unwrap();
        let bytes = encode_arrow_ipc_stream(&schema, &[batch]).expect("encode");

        let cursor = std::io::Cursor::new(bytes);
        let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).expect("reader");
        assert_eq!(reader.schema().as_ref(), schema.as_ref());
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.values(), &[10, 50]);
    }

    #[test]
    fn signature_verification_is_bound_to_dataset_and_job_and_content() {
        let secret = "test-secret";
        let digest = hex_digest(b"result bytes");
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(format!("dataset-result:ds-1:job-1:{digest}").as_bytes());
        let sig = hex::encode(mac.finalize().into_bytes());

        assert!(verify_result_signature(secret, "ds-1", "job-1", &digest, &sig));
        // Wrong dataset id, wrong job id, wrong digest, and a wrong secret must each
        // independently fail — the signature is bound to ALL of them, not just one.
        assert!(!verify_result_signature(secret, "ds-2", "job-1", &digest, &sig));
        assert!(!verify_result_signature(secret, "ds-1", "job-2", &digest, &sig));
        assert!(!verify_result_signature(
            secret,
            "ds-1",
            "job-1",
            &hex_digest(b"other bytes"),
            &sig
        ));
        assert!(!verify_result_signature("wrong-secret", "ds-1", "job-1", &digest, &sig));
    }
}
