//! MSSQL TDS wire round-trip smoke test (CONCEPT:EG-077).
//!
//! Starts the real hand-rolled TDS listener over an in-process `ServerState`, then
//! drives the raw protocol from a plain `TcpStream` (no `tiberius` — the server side
//! is hand-rolled, so the test hand-rolls the client too): PRELOGIN → LOGIN7 (trust)
//! → a hand-built `SQLBatch` (`SELECT id FROM nodes …`), and asserts a correct
//! COLMETADATA + ROW* + DONE token stream comes back over the seeded graph — proving
//! the adapter reuses the SAME eg-query DataFusion path the shared `WireSession` runs.
//!
//! Only compiled with `--features mssql-wire`.

#![cfg(feature = "mssql-wire")]

use std::sync::Arc;

use dashmap::DashMap;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::isolation::IsolationLayer;
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::mssql_wire::{self, protocol};
use epistemic_graph::server::txn::TxnIdGen;
use epistemic_graph::server::ServerState;

use protocol::{
    frame_message, parse_header, utf16le_bytes, utf16le_to_string, TdsType, HEADER_LEN, PKT_LOGIN7,
    PKT_PRELOGIN, PKT_SQLBATCH, TOKEN_COLMETADATA, TOKEN_DONE, TOKEN_ROW, TYPE_BITN, TYPE_FLTN,
    TYPE_INTN, TYPE_NVARCHAR,
};

/// Build a minimal TRUST-mode `ServerState` (empty auth secret) seeded with three
/// nodes so a wire SELECT returns rows. `__commons__` is pre-created by the registry.
fn seeded_state() -> Arc<RwLock<ServerState>> {
    let registry = GraphRegistry::new();
    {
        let core = registry.get("__commons__").unwrap().core.clone();
        for (id, ty, rank) in [("n1", "Agent", 1i64), ("n2", "Agent", 2), ("n3", "Tool", 3)] {
            let blob =
                rmp_serde::to_vec_named(&serde_json::json!({"type": ty, "rank": rank})).unwrap();
            core.add_node(id.to_string(), blob);
        }
    }
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry,
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: String::new(), // trust mode
        #[cfg(feature = "kv")]
        kv: None,
        persist_dir: None,
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
        #[cfg(feature = "blob")]
        blob: None,
        #[cfg(feature = "blob")]
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
    }))
}

/// Bind an ephemeral port, serve the TDS listener there, and return the address.
async fn spawn_listener(state: Arc<RwLock<ServerState>>) -> String {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap().to_string();
    drop(probe);
    let serve_addr = addr.clone();
    tokio::spawn(async move {
        let _ = mssql_wire::serve(&serve_addr, state).await;
    });
    // Give the listener a moment to bind.
    for _ in 0..50 {
        if TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    addr
}

/// Build a minimal LOGIN7 record: the 94-byte fixed header with all variable fields
/// empty (offset/length zero) — trust mode needs no credentials.
fn empty_login7() -> Vec<u8> {
    let mut rec = vec![0u8; 94];
    let len = rec.len() as u32;
    rec[0..4].copy_from_slice(&len.to_le_bytes());
    rec
}

/// Read one complete TDS message (reassembling until the EOM bit), returning the body.
async fn read_message(stream: &mut TcpStream) -> Vec<u8> {
    let mut payload = Vec::new();
    loop {
        let mut hdr = [0u8; HEADER_LEN];
        stream.read_exact(&mut hdr).await.unwrap();
        let h = parse_header(&hdr);
        let blen = h.body_len();
        if blen > 0 {
            let start = payload.len();
            payload.resize(start + blen, 0);
            stream.read_exact(&mut payload[start..]).await.unwrap();
        }
        if h.is_eom() {
            break;
        }
    }
    payload
}

/// Walk a COLMETADATA + ROW* + DONE token stream into (columns, rows, done_status).
#[allow(clippy::type_complexity)]
fn walk_result(s: &[u8]) -> (Vec<(String, TdsType)>, Vec<Vec<Value>>, u16) {
    let mut i = 0usize;
    let mut cols: Vec<(String, TdsType)> = Vec::new();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut status = 0u16;
    while i < s.len() {
        match s[i] {
            TOKEN_COLMETADATA => {
                i += 1;
                let count = u16::from_le_bytes([s[i], s[i + 1]]) as usize;
                i += 2;
                for _ in 0..count {
                    i += 6; // UserType(4) + Flags(2)
                    let ty = match s[i] {
                        TYPE_INTN => {
                            i += 2;
                            TdsType::IntN
                        }
                        TYPE_FLTN => {
                            i += 2;
                            TdsType::FloatN
                        }
                        TYPE_BITN => {
                            i += 2;
                            TdsType::BitN
                        }
                        TYPE_NVARCHAR => {
                            i += 1 + 2 + 5; // token + max-len + collation
                            TdsType::NVarchar
                        }
                        other => panic!("unexpected TYPE_INFO {other:#x}"),
                    };
                    let units = s[i] as usize;
                    i += 1;
                    let name = utf16le_to_string(&s[i..i + units * 2]);
                    i += units * 2;
                    cols.push((name, ty));
                }
            }
            TOKEN_ROW => {
                i += 1;
                let mut row = Vec::new();
                for (_, ty) in &cols {
                    match ty {
                        TdsType::NVarchar => {
                            let len = u16::from_le_bytes([s[i], s[i + 1]]) as usize;
                            i += 2;
                            if len == 0xFFFF {
                                row.push(Value::Null);
                            } else {
                                row.push(Value::String(utf16le_to_string(&s[i..i + len])));
                                i += len;
                            }
                        }
                        _ => {
                            let len = s[i] as usize;
                            i += 1 + len;
                            row.push(Value::Null); // value bytes not needed for this test
                        }
                    }
                }
                rows.push(row);
            }
            TOKEN_DONE => {
                status = u16::from_le_bytes([s[i + 1], s[i + 2]]);
                i += 13;
            }
            other => panic!("unexpected token {other:#x} at {i}"),
        }
    }
    (cols, rows, status)
}

#[tokio::test]
async fn tds_select_returns_colmetadata_rows_done() {
    let addr = spawn_listener(seeded_state()).await;
    let mut stream = TcpStream::connect(&addr).await.unwrap();

    // 1. PRELOGIN (a bare option terminator is enough for the server to reply).
    stream
        .write_all(&frame_message(PKT_PRELOGIN, &[0xFF]))
        .await
        .unwrap();
    let _prelogin_reply = read_message(&mut stream).await;

    // 2. LOGIN7 (trust — empty credentials). Server replies LOGINACK + DONE.
    stream
        .write_all(&frame_message(PKT_LOGIN7, &empty_login7()))
        .await
        .unwrap();
    let login_reply = read_message(&mut stream).await;
    assert_eq!(
        login_reply.first().copied(),
        Some(protocol::TOKEN_LOGINACK),
        "login succeeds with a LOGINACK token"
    );

    // 3. SQLBatch — the UCS-2 SQL text (no ALL_HEADERS block).
    let sql = utf16le_bytes("SELECT id FROM nodes ORDER BY id");
    stream
        .write_all(&frame_message(PKT_SQLBATCH, &sql))
        .await
        .unwrap();
    let result = read_message(&mut stream).await;

    assert_eq!(
        result.first().copied(),
        Some(TOKEN_COLMETADATA),
        "the result stream opens with COLMETADATA (not an ERROR token)"
    );
    let (cols, rows, status) = walk_result(&result);
    assert_eq!(cols.len(), 1, "one projected column");
    assert_eq!(cols[0].0, "id");
    assert_eq!(cols[0].1, TdsType::NVarchar, "text id → NVARCHAR");
    assert_eq!(rows.len(), 3, "three seeded nodes returned");
    let ids: Vec<String> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected string id, got {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec!["n1", "n2", "n3"]);
    assert_eq!(status & protocol::DONE_ERROR, 0, "DONE has no error flag");
}
