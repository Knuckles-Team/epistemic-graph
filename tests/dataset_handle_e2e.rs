//! Arrow dataset-handle end-to-end test (CONCEPT:INT-P2-2 — the Arrow Flight /
//! dataset-handle seam for external heavy compute).
//!
//! Starts the real `dataset_handle::serve` HTTP listener over an in-process
//! `ServerState` and drives it with a hand-rolled HTTP client (no new HTTP client dep
//! — mirrors `tests/pgwire_roundtrip.rs`/`tests/mysql_roundtrip.rs`'s own hand-rolled
//! wire clients), proving the FULL seam end-to-end:
//!
//!   * `POST /dataset/export` materializes a query over a graph snapshot into an
//!     immutable dataset handle;
//!   * `GET /dataset/<id>` streams it back as a REAL Arrow IPC stream — decoded here
//!     with `arrow::ipc::reader::StreamReader` and asserted against the native Arrow
//!     array API, never a JSON cell;
//!   * `POST /dataset/<id>/result` accepts a correctly-signed result artifact,
//!     lands it in the blob CAS, and commits a `:DatasetResult` node that is
//!     immediately queryable via the normal SQL `nodes` surface;
//!   * an incorrectly-signed result artifact is rejected (401) and never committed.
//!
//! Only compiled with `--features dataset-handle`.

#![cfg(feature = "dataset-handle")]

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::isolation::IsolationLayer;
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::blob::{BlobCursors, RedbChunkStore};
use epistemic_graph::server::dataset_handle::{self, DatasetHandleMeta};
use epistemic_graph::server::txn::TxnIdGen;
use epistemic_graph::server::ServerState;

const SECRET: &str = "dataset-handle-test-secret";

/// A `ServerState` seeded with three `:Agent` nodes in `__commons__` (a `name`/`score`
/// property each) and a real (temp-dir) blob CAS, so the export/pull/writeback seam is
/// exercised against real data + a real content-addressed store, not a mock.
fn seeded_state(blob_dir: &std::path::Path) -> Arc<RwLock<ServerState>> {
    let registry = GraphRegistry::new();
    {
        let core = registry.get("__commons__").unwrap().core.clone();
        for (id, name, score) in [("a1", "alice", 10i64), ("a2", "bob", 50), ("a3", "carol", 90)] {
            let blob = rmp_serde::to_vec_named(
                &serde_json::json!({"kind": "Agent", "name": name, "score": score}),
            )
            .unwrap();
            core.add_node(id.to_string(), blob);
        }
    }
    let store = Arc::new(RedbChunkStore::open(&blob_dir.to_string_lossy()).unwrap());
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry,
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: SECRET.to_string(),
        #[cfg(feature = "kv")]
        kv: None,
        persist_dir: Some(blob_dir.to_string_lossy().to_string()),
        persistence: None,
        redb_authoritative: false,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(
            epistemic_graph::write_coalescer::WriteCoalescerRegistry::from_env(),
        ),
        open_txns: Arc::new(DashMap::new()),
        txn_id_gen: Arc::new(TxnIdGen::default()),
        txn_ttl_secs: 300,
        txn_max_per_graph: 256,
        txn_max_per_agent: 256,
        blob: Some(Arc::new(BlobCursors::new(store))),
        blob_cursor_ttl_secs: 300,
        #[cfg(feature = "raft")]
        raft: None,
        #[cfg(feature = "raft")]
        multi_raft: None,
        #[cfg(feature = "tsdb")]
        tsdb_store: None,
        #[cfg(feature = "rdf-redb")]
        rdf_quads: None,
        #[cfg(feature = "streaming")]
        cdc: Some(Arc::new(epistemic_graph::server::cdc::CdcHub::new())),
        #[cfg(feature = "wasm-udf")]
        udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: Arc::new(parking_lot::Mutex::new(
            epistemic_graph::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: Arc::new(DashMap::new()),
        dataset_handles: Arc::new(dataset_handle::DatasetHandleRegistry::new()),
    }))
}

/// Bind an ephemeral port and serve the dataset-handle listener.
async fn spawn_listener(state: Arc<RwLock<ServerState>>) -> String {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap().to_string();
    drop(probe);
    let serve_addr = addr.clone();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&serve_addr).await.unwrap();
        dataset_handle::serve(listener, state).await;
    });
    for _ in 0..50 {
        if TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    addr
}

struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

/// A minimal one-shot HTTP/1.1 client: send a request, read until the peer closes the
/// connection (the server always answers `connection: close`), parse status/headers/
/// body. No HTTP client crate — mirrors the wire-protocol tests' own hand-rolled
/// clients.
async fn http_request(
    addr: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> HttpResponse {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nhost: {addr}\r\ncontent-length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let _ = stream.shutdown().await;

    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();

    let head_end = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has a header terminator");
    let head = String::from_utf8_lossy(&resp[..head_end]).to_string();
    let body = resp[head_end + 4..].to_vec();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut resp_headers = HashMap::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            resp_headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    HttpResponse {
        status,
        headers: resp_headers,
        body,
    }
}

fn sign(dataset_id: &str, job_id: &str, digest: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(format!("dataset-result:{dataset_id}:{job_id}:{digest}").as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[tokio::test(flavor = "multi_thread")]
async fn export_pull_arrow_and_signed_writeback_round_trip() {
    let dir = std::env::temp_dir().join(format!(
        "eg-dataset-handle-e2e-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let state = seeded_state(&dir);
    let addr = spawn_listener(state.clone()).await;

    // ── 1. export a dataset handle for a query ──────────────────────────────
    let export_body =
        serde_json::json!({"graph": "__commons__", "sql": "SELECT id, name, score FROM nodes ORDER BY id"});
    let resp = http_request(
        &addr,
        "POST",
        "/dataset/export",
        &[("content-type", "application/json")],
        export_body.to_string().as_bytes(),
    )
    .await;
    assert_eq!(resp.status, 200, "export failed: {:?}", String::from_utf8_lossy(&resp.body));
    let meta: DatasetHandleMeta = serde_json::from_slice(&resp.body).expect("parse meta");
    assert_eq!(meta.row_count, 3);
    assert_eq!(meta.graph, "__commons__");
    assert!(meta.schema.iter().any(|f| f.name == "score" && f.data_type.contains("Int64")));
    assert!(meta.schema.iter().any(|f| f.name == "name" && f.data_type.contains("Utf8")));

    // ── 2. pull it as real Arrow record batches — NOT per-row JSON ──────────
    let resp = http_request(&addr, "GET", &format!("/dataset/{}", meta.id), &[], &[]).await;
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.headers.get("content-type").map(String::as_str),
        Some("application/vnd.apache.arrow.stream")
    );
    let cursor = std::io::Cursor::new(resp.body);
    let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).expect("arrow ipc reader");
    let schema = reader.schema();
    assert!(schema.field_with_name("name").is_ok());
    assert_eq!(
        schema.field_with_name("score").unwrap().data_type(),
        &arrow::datatypes::DataType::Int64
    );
    let batches: Vec<arrow::record_batch::RecordBatch> =
        reader.map(|b| b.expect("decode batch")).collect();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
    let mut names = Vec::new();
    let mut scores = Vec::new();
    for batch in &batches {
        let name_col = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("real Arrow StringArray, not a JSON cell");
        let score_col = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("real Arrow Int64Array, not a JSON cell");
        for i in 0..batch.num_rows() {
            names.push(name_col.value(i).to_string());
            scores.push(score_col.value(i));
        }
    }
    assert_eq!(names, vec!["alice", "bob", "carol"]);
    assert_eq!(scores, vec![10, 50, 90]);

    // ── 3. a WRONG signature is rejected and commits nothing ────────────────
    let artifact = b"a fitted model blob produced by an external heavy-compute job";
    let bad_resp = http_request(
        &addr,
        "POST",
        &format!("/dataset/{}/result", meta.id),
        &[("x-eg-job-id", "job-42"), ("x-eg-signature", "deadbeef")],
        artifact,
    )
    .await;
    assert_eq!(bad_resp.status, 401);

    // ── 4. a correctly-signed result artifact commits transactionally ───────
    let digest = epistemic_graph::server::blob::store::hex_digest(artifact);
    let sig = sign(&meta.id, "job-42", &digest);
    let ok_resp = http_request(
        &addr,
        "POST",
        &format!("/dataset/{}/result", meta.id),
        &[("x-eg-job-id", "job-42"), ("x-eg-signature", sig.as_str())],
        artifact,
    )
    .await;
    assert_eq!(
        ok_resp.status,
        200,
        "signed writeback failed: {:?}",
        String::from_utf8_lossy(&ok_resp.body)
    );
    let committed: serde_json::Value = serde_json::from_slice(&ok_resp.body).unwrap();
    let node_id = committed["node_id"].as_str().unwrap().to_string();
    assert_eq!(node_id, format!("dataset-result:{}:job-42", meta.id));
    assert_eq!(committed["blob_digest"].as_str().unwrap(), digest);

    // ── 5. the committed result is immediately QUERYABLE over the normal SQL surface ──
    let core = {
        let s = state.read().await;
        s.registry.get("__commons__").unwrap().core.clone()
    };
    let view = core.analysis_snapshot();
    // `exec_sql_typed` builds + drives its own current-thread runtime, so it must run
    // on the blocking pool (never on a reactor worker) — the same discipline the wire
    // handlers use.
    let query = format!("SELECT id FROM nodes WHERE id = '{node_id}'");
    let result = tokio::task::spawn_blocking(move || eg_query::exec_sql_typed(&view, &query))
        .await
        .expect("query task")
        .expect("query result node");
    assert_eq!(result.rows.len(), 1, "the DatasetResult node is queryable");

    let _ = std::fs::remove_dir_all(&dir);
}
