//! MSSQL TDS wire round-trip smoke test (CONCEPT:EG-KG.query.hand-rolled-tds-server).
//!
//! Starts the real hand-rolled TDS listener over an in-process `ServerState`, then
//! drives the raw protocol from a plain `TcpStream` (no `tiberius` — the server side
//! is hand-rolled, so the test hand-rolls the client too): PRELOGIN → authenticated LOGIN7
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
use epistemic_graph::server::mssql_wire::{self, derive_mssql_password, protocol};
use epistemic_graph::server::txn::TxnIdGen;
use epistemic_graph::server::ServerState;

use protocol::{
    frame_message, parse_header, utf16le_bytes, utf16le_to_string, TdsType, HEADER_LEN, PKT_LOGIN7,
    PKT_PRELOGIN, PKT_SQLBATCH, TOKEN_COLMETADATA, TOKEN_DONE, TOKEN_ROW, TYPE_BITN, TYPE_FLTN,
    TYPE_INTN, TYPE_NVARCHAR,
};

const TEST_SECRET: &str = "test";
const TEST_USER: &str = "tester";

fn sql_test_persist_dir() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
    std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-test");
    std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
    std::env::temp_dir()
        .join(format!(
            "epistemic-graph-mssql-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
        .to_string_lossy()
        .into_owned()
}

/// Build a minimal authenticated `ServerState` seeded with three
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
        auth_secret: TEST_SECRET.to_string(),
        #[cfg(feature = "kv")]
        kv: None,
        persist_dir: Some(sql_test_persist_dir()),
        persistence: None,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
        routed_write_coalescer: Arc::new(epistemic_graph::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new()),
        open_txns: Arc::new(DashMap::new()),
        txn_id_gen: Arc::new(TxnIdGen),
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
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(epistemic_graph::server::lake::LakeManager::new()),
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

/// Build a minimal authenticated LOGIN7 record with user/password/database fields.
fn login7(user: &str, password: &str, database: &str) -> Vec<u8> {
    const FIXED_LEN: usize = 94;
    let user_b = utf16le_bytes(user);
    let pw_enc: Vec<u8> = utf16le_bytes(password)
        .into_iter()
        .map(|b| (b ^ 0xA5).rotate_left(4))
        .collect();
    let db_b = utf16le_bytes(database);

    let mut rec = vec![0u8; FIXED_LEN];
    let mut data = Vec::new();
    let put = |rec: &mut Vec<u8>,
               data: &mut Vec<u8>,
               offset_pos: usize,
               count_pos: usize,
               bytes: &[u8]| {
        let offset = (FIXED_LEN + data.len()) as u16;
        let units = (bytes.len() / 2) as u16;
        rec[offset_pos..offset_pos + 2].copy_from_slice(&offset.to_le_bytes());
        rec[count_pos..count_pos + 2].copy_from_slice(&units.to_le_bytes());
        data.extend_from_slice(bytes);
    };
    put(&mut rec, &mut data, 40, 42, &user_b);
    put(&mut rec, &mut data, 44, 46, &pw_enc);
    put(&mut rec, &mut data, 68, 70, &db_b);
    rec.extend_from_slice(&data);
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

    // 2. Authenticated LOGIN7. Server replies LOGINACK + DONE.
    let password = derive_mssql_password(TEST_SECRET, TEST_USER);
    stream
        .write_all(&frame_message(
            PKT_LOGIN7,
            &login7(TEST_USER, &password, "__commons__"),
        ))
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

// ───────────────────────────────────────────────────────────────────────────
// Cross-modal transaction round-trip (CONCEPT:EG-KG.query.per-surface-parity) — per-surface parity.
//
// The in-txn cross-modal seam routes through the SHARED `WireSession`
// (CONCEPT:EG-KG.compute.subsystems-reference/EG-372), so the TDS wire INHERITS the pgwire seam. These tests
// MIRROR the pgwire cross-modal cases (`tests/pgwire_roundtrip.rs`) over the TDS
// SQLBatch protocol — every batch is handed verbatim to `WireSession::execute`, the
// SAME core the pgwire shim runs:
//   * `wire_txn_update_then_cross_modal_read` — a staged in-txn graph UPDATE + vector
//     `SET EMBEDDING` are BOTH read back by an in-txn cross-modal `UQL` read, and
//   * `wire_txn_crossmodal_ryow_isolated_until_commit` — `BEGIN; SET EMBEDDING …;
//     INSERT INTO series …; <UQL cross-modal read>; COMMIT` reads its own writes while
//     a SECOND connection sees NONE of them until COMMIT.
// ───────────────────────────────────────────────────────────────────────────

/// Complete PRELOGIN + authenticated LOGIN7 and return the connected stream ready for the
/// command phase. Panics if the login is not acknowledged.
async fn connect(addr: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(&frame_message(PKT_PRELOGIN, &[0xFF]))
        .await
        .unwrap();
    let _ = read_message(&mut stream).await;
    let password = derive_mssql_password(TEST_SECRET, TEST_USER);
    stream
        .write_all(&frame_message(
            PKT_LOGIN7,
            &login7(TEST_USER, &password, "__commons__"),
        ))
        .await
        .unwrap();
    let login_reply = read_message(&mut stream).await;
    assert_eq!(
        login_reply.first().copied(),
        Some(protocol::TOKEN_LOGINACK),
        "login succeeds with a LOGINACK token"
    );
    stream
}

/// Decode a TDS ERROR token's message text (US_VARCHAR at a fixed offset) for a loud panic.
fn decode_error(s: &[u8]) -> String {
    // [0xAA][len u16][number i32][state u8][class u8][msglen u16 code-units][utf16 msg]…
    let msg_units = u16::from_le_bytes([s[9], s[10]]) as usize;
    utf16le_to_string(&s[11..11 + msg_units * 2])
}

/// Send a `SQLBatch` and walk the returned token stream into `(cols, rows, status)`.
/// Panics loudly on an ERROR token so a rejected cross-modal verb fails visibly.
#[allow(clippy::type_complexity)]
async fn batch(
    stream: &mut TcpStream,
    sql: &str,
) -> (Vec<(String, TdsType)>, Vec<Vec<Value>>, u16) {
    let bytes = utf16le_bytes(sql);
    stream
        .write_all(&frame_message(PKT_SQLBATCH, &bytes))
        .await
        .unwrap();
    let result = read_message(stream).await;
    if result.first().copied() == Some(protocol::TOKEN_ERROR) {
        panic!(
            "batch `{sql}` returned an ERROR token: {}",
            decode_error(&result)
        );
    }
    walk_result(&result)
}

/// The first column of every returned row (mirrors pgwire's `simple_ids`).
fn first_col(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected a string id, got {other:?}"),
        })
        .collect()
}

/// Mirrors pgwire `wire_txn_update_then_cross_modal_read` (CONCEPT:EG-KG.query.per-surface-parity / EG-372):
/// inside an open txn, a staged graph UPDATE AND a staged vector `SET EMBEDDING` are BOTH
/// read back by an in-txn cross-modal `UQL` read over the TDS wire (read-your-own-writes
/// across the graph + vector modalities before COMMIT).
#[tokio::test]
async fn tds_txn_update_then_cross_modal_read() {
    let addr = spawn_listener(seeded_state()).await;
    let mut c = connect(&addr).await;

    batch(&mut c, "BEGIN").await;

    // Stage a graph write (UPDATE) AND a vector write (SET EMBEDDING) inside the txn.
    batch(&mut c, "UPDATE nodes SET rank = 99 WHERE id = 'n1'").await;
    batch(&mut c, "SET EMBEDDING FOR 'n1' = '[1.0, 0.0, 0.0]'").await;

    // Plain-SQL RYOW sees the staged write (buffered write path).
    let (_c, rows, _s) = batch(&mut c, "SELECT id FROM nodes WHERE rank = 99").await;
    assert_eq!(
        first_col(&rows),
        vec!["n1".to_string()],
        "plain SQL RYOW works"
    );

    // THE SEAM: a CROSS-MODAL / UQL read over the TDS wire ALSO reflects the staged
    // writes — a unified filter→rank over the staged row returns `n1` (RYOW across the
    // graph AND vector modalities before COMMIT).
    let (_c, rows, _s) = batch(
        &mut c,
        "UQL MATCH (:Agent) WHERE rank = 99 |> RANK BY ~[1.0,0.0,0.0] |> LIMIT 5",
    )
    .await;
    assert_eq!(
        first_col(&rows),
        vec!["n1".to_string()],
        "the cross-modal read must see the same-txn staged UPDATE + embedding (RYOW)"
    );

    batch(&mut c, "COMMIT").await;
}

/// Mirrors pgwire `wire_txn_crossmodal_ryow_isolated_until_commit` (CONCEPT:EG-KG.query.per-surface-parity /
/// EG-372) — the headline per-surface parity proof over TDS. A `BEGIN; INSERT node;
/// SET EMBEDDING; INSERT INTO series; <UQL join over graph+vector>; COMMIT` reads its OWN
/// writes inside the txn, while a SECOND connection sees NONE of them until COMMIT. After
/// COMMIT the second connection reads the committed cross-modal state.
#[tokio::test]
async fn tds_txn_crossmodal_ryow_isolated_until_commit() {
    let addr = spawn_listener(seeded_state()).await;
    let mut writer = connect(&addr).await;
    let mut reader = connect(&addr).await;

    // The UQL cross-modal join used by both connections: a Widget node filtered on the
    // graph modality and ranked by the query embedding (graph + vector legs).
    let uql = "UQL MATCH (:Widget) WHERE rank = 5 |> RANK BY ~[1.0,0.0,0.0] |> LIMIT 5";

    batch(&mut writer, "BEGIN").await;
    // Stage a graph node, its embedding, and a measurement — all inside the txn.
    batch(
        &mut writer,
        "INSERT INTO nodes (id, type, rank) VALUES ('vv1', 'Widget', 5)",
    )
    .await;
    batch(&mut writer, "SET EMBEDDING FOR 'vv1' = '[1.0, 0.0, 0.0]'").await;
    batch(
        &mut writer,
        "INSERT INTO series (id, ts, value) VALUES ('vv1', 1000, 9.0)",
    )
    .await;

    // RYOW: the writer's OWN in-txn UQL join sees its staged node + embedding.
    let (_c, rows, _s) = batch(&mut writer, uql).await;
    assert_eq!(
        first_col(&rows),
        vec!["vv1".to_string()],
        "the writer reads its own staged cross-modal writes (RYOW)"
    );

    // Isolation: a SECOND connection (no open txn) sees NONE of the staged writes.
    let (_c, rows, _s) = batch(&mut reader, uql).await;
    assert!(
        rows.is_empty(),
        "a second connection must see none of the uncommitted cross-modal writes, got {rows:?}"
    );

    batch(&mut writer, "COMMIT").await;

    // After COMMIT the committed cross-modal state is visible to the second connection.
    let (_c, rows, _s) = batch(&mut reader, uql).await;
    assert_eq!(
        first_col(&rows),
        vec!["vv1".to_string()],
        "after COMMIT the committed node + embedding are visible off-txn"
    );
}
